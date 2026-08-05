// Package producer implements the doorman: the thin, verified HTTP front door
// that turns webhooks into Kafka records.
//
// It does four things and nothing else — read the raw body, verify the
// signature over those exact bytes, publish them keyed for ordering, and
// answer. No database, no OpenMeter lookups, no invoice logic. Keeping it
// hollow is what lets it stay fast enough that Stripe never times out, and
// what makes it a security boundary worth trusting: everything downstream can
// assume the bytes on the billing topic were signed by someone holding the
// secret.
package producer

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/metrics"
	"github.com/pymthouse/settlement/internal/webhook"
)

// Publisher is the Kafka write path, narrowed so tests can substitute it.
type Publisher interface {
	Publish(ctx context.Context, msg kafka.Message) error
}

// Server routes and verifies inbound webhooks.
type Server struct {
	cfg       config.Producer
	log       *slog.Logger
	publisher Publisher
	stripe    *webhook.StripeVerifier
	standard  *webhook.StandardVerifier
	now       func() time.Time
}

// New builds the doorman.
func New(cfg config.Producer, log *slog.Logger, publisher Publisher) *Server {
	return &Server{
		cfg:       cfg,
		log:       log,
		publisher: publisher,
		stripe:    webhook.NewStripeVerifier(cfg.StripeWebhookSecrets, cfg.StripeToleranceSeconds),
		standard:  webhook.NewStandardVerifier(cfg.OpenMeterWebhookSecrets, cfg.OpenMeterToleranceSecs),
		now:       time.Now,
	}
}

// Routes returns the HTTP surface: two webhook endpoints, liveness, readiness
// and metrics.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /webhooks/stripe", s.handleStripe)
	mux.HandleFunc("POST /webhooks/openmeter", s.handleOpenMeter)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		// Readiness is signature-config readiness. Broker reachability is
		// deliberately not probed here: a reachability check on every probe
		// would add the very latency the doorman exists to avoid, and a
		// publish failure already surfaces as a 5xx that Stripe will retry.
		if !s.stripe.Enabled() && !s.standard.Enabled() {
			http.Error(w, "no webhook secrets configured", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ready")
	})
	mux.Handle("GET /metrics", metrics.Handler())
	return mux
}

func (s *Server) handleStripe(w http.ResponseWriter, r *http.Request) {
	start := s.now()
	defer func() {
		metrics.WebhookDuration.WithLabelValues(events.SourceStripe).Observe(time.Since(start).Seconds())
	}()

	if !s.stripe.Enabled() {
		s.reject(w, events.SourceStripe, "not_configured", http.StatusServiceUnavailable,
			"stripe webhooks are not configured", nil)
		return
	}

	body, ok := s.readBody(w, r, events.SourceStripe)
	if !ok {
		return
	}

	if err := s.stripe.Verify(r.Header.Get("Stripe-Signature"), body); err != nil {
		s.reject(w, events.SourceStripe, "bad_signature", http.StatusBadRequest, "invalid signature", err)
		return
	}

	s.accept(w, r, events.SourceStripe, s.cfg.Kafka.TopicStripe, body)
}

func (s *Server) handleOpenMeter(w http.ResponseWriter, r *http.Request) {
	start := s.now()
	defer func() {
		metrics.WebhookDuration.WithLabelValues(events.SourceOpenMeter).Observe(time.Since(start).Seconds())
	}()

	if !s.standard.Enabled() {
		s.reject(w, events.SourceOpenMeter, "not_configured", http.StatusServiceUnavailable,
			"openmeter webhooks are not configured", nil)
		return
	}

	body, ok := s.readBody(w, r, events.SourceOpenMeter)
	if !ok {
		return
	}

	// OpenMeter delivers Standard Webhooks headers; Svix-hosted channels send
	// the same triple under svix-* names.
	id := headerAny(r, "webhook-id", "svix-id")
	ts := headerAny(r, "webhook-timestamp", "svix-timestamp")
	sig := headerAny(r, "webhook-signature", "svix-signature")

	if err := s.standard.Verify(id, ts, sig, body); err != nil {
		s.reject(w, events.SourceOpenMeter, "bad_signature", http.StatusBadRequest, "invalid signature", err)
		return
	}

	s.accept(w, r, events.SourceOpenMeter, s.cfg.Kafka.TopicOpenMeter, body)
}

// readBody reads at most MaxBodyBytes of the request. The cap is a defence
// against a body large enough to stall the process before the signature has
// even been checked.
func (s *Server) readBody(w http.ResponseWriter, r *http.Request, source string) ([]byte, bool) {
	limited := http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.reject(w, source, "too_large", http.StatusRequestEntityTooLarge, "body too large", err)
			return nil, false
		}
		s.reject(w, source, "read_error", http.StatusBadRequest, "could not read body", err)
		return nil, false
	}
	if len(body) == 0 {
		s.reject(w, source, "empty_body", http.StatusBadRequest, "empty body", nil)
		return nil, false
	}
	return body, true
}

// accept describes, publishes and acknowledges a verified body.
func (s *Server) accept(w http.ResponseWriter, r *http.Request, source, topic string, body []byte) {
	desc, err := events.Describe(source, body)
	if err != nil {
		// Signed but unrecognisable. Rejecting keeps the billing log clean; a
		// 400 also makes the mismatch visible in the provider's dashboard
		// instead of hiding it in our own logs.
		s.reject(w, source, "bad_request", http.StatusBadRequest, "unrecognised event body", err)
		return
	}

	msg := kafka.Message{
		Topic: topic,
		Key:   []byte(desc.PartitionKey),
		Value: body, // raw and unmodified: this is the audit record
		Headers: []kafka.Header{
			{Key: events.HeaderSource, Value: []byte(desc.Source)},
			{Key: events.HeaderEventID, Value: []byte(desc.EventID)},
			{Key: events.HeaderEventType, Value: []byte(desc.EventType)},
			{Key: events.HeaderKey, Value: []byte(desc.PartitionKey)},
			{Key: events.HeaderAccount, Value: []byte(desc.Account)},
			{Key: events.HeaderLivemode, Value: []byte(strconv.FormatBool(desc.Livemode))},
			{Key: events.HeaderReceivedAt, Value: []byte(s.now().UTC().Format(time.RFC3339Nano))},
			{Key: events.HeaderProducer, Value: []byte("settlement-producer")},
		},
		Time: s.now().UTC(),
	}

	// Bound the publish independently of the client's context so a hung caller
	// cannot leave a half-written record, and so we fail fast enough for the
	// provider to retry rather than time out.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(r.Context()), s.cfg.Kafka.WriteTimeout)
	defer cancel()

	if err := s.publisher.Publish(ctx, msg); err != nil {
		// 500 means "not persisted, please redeliver". Answering 200 here
		// would drop a financial event permanently.
		s.reject(w, source, "publish_error", http.StatusInternalServerError, "could not enqueue event", err)
		return
	}

	metrics.WebhooksReceived.WithLabelValues(source, "accepted").Inc()
	s.log.Info("webhook accepted",
		"source", source,
		"event_id", desc.EventID,
		"event_type", desc.EventType,
		"partition_key", desc.PartitionKey,
		"topic", topic,
		"livemode", desc.Livemode,
	)
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"received":true}`)
}

// reject records the outcome and answers with a short, non-revealing message.
func (s *Server) reject(w http.ResponseWriter, source, outcome string, status int, message string, err error) {
	metrics.WebhooksReceived.WithLabelValues(source, outcome).Inc()
	level := slog.LevelWarn
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	}
	s.log.Log(context.Background(), level, "webhook rejected",
		"source", source, "outcome", outcome, "status", status, "error", err)
	http.Error(w, message, status)
}

func headerAny(r *http.Request, names ...string) string {
	for _, n := range names {
		if v := r.Header.Get(n); v != "" {
			return v
		}
	}
	return ""
}
