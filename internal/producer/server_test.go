package producer

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
)

const (
	testStripeSecret = "whsec_producer_test"
	// Synthetic fixture only — not a real webhook secret.
	testStandardSecret = "whsec_c2V0dGxlbWVudC10ZXN0LWhtYWMta2V5LXYx"
)

// recordingPublisher captures what the doorman would have written to Kafka.
type recordingPublisher struct {
	mu       sync.Mutex
	messages []kafka.Message
	err      error
}

func (p *recordingPublisher) Publish(_ context.Context, msg kafka.Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.messages = append(p.messages, msg)
	return nil
}

func (p *recordingPublisher) only(t *testing.T) kafka.Message {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.messages) != 1 {
		t.Fatalf("expected exactly one published message, got %d", len(p.messages))
	}
	return p.messages[0]
}

func (p *recordingPublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func newTestServer(t *testing.T, publisher Publisher) http.Handler {
	t.Helper()

	cfg := config.Producer{
		MaxBodyBytes:            1 << 20,
		StripeWebhookSecrets:    []string{testStripeSecret},
		StripeToleranceSeconds:  300,
		OpenMeterWebhookSecrets: []string{testStandardSecret},
		OpenMeterToleranceSecs:  300,
		Kafka: config.Kafka{
			TopicStripe:    "billing.stripe.events.v1",
			TopicOpenMeter: "billing.openmeter.invoices.v1",
			WriteTimeout:   5 * time.Second,
		},
	}
	return New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), publisher).Routes()
}

func stripeRequest(t *testing.T, body string) *http.Request {
	t.Helper()

	ts := strconv.FormatInt(time.Now().Unix(), 10)
	mac := hmac.New(sha256.New, []byte(testStripeSecret))
	mac.Write([]byte(ts + "." + body))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(body))
	req.Header.Set("Stripe-Signature", "t="+ts+",v1="+hex.EncodeToString(mac.Sum(nil)))
	return req
}

func openMeterRequest(t *testing.T, body string) *http.Request {
	t.Helper()

	id := "msg_test_1"
	ts := strconv.FormatInt(time.Now().Unix(), 10)
	key, err := base64.StdEncoding.DecodeString(testStandardSecret[len("whsec_"):])
	if err != nil {
		t.Fatal(err)
	}
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(id + "." + ts + "." + body))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/openmeter", strings.NewReader(body))
	req.Header.Set("webhook-id", id)
	req.Header.Set("webhook-timestamp", ts)
	req.Header.Set("webhook-signature", "v1,"+base64.StdEncoding.EncodeToString(mac.Sum(nil)))
	return req
}

func TestStripeWebhookIsVerifiedAndPublishedVerbatim(t *testing.T) {
	publisher := &recordingPublisher{}
	server := newTestServer(t, publisher)

	// Deliberately untidy JSON: the bytes on Kafka must match exactly.
	body := `{ "id":"evt_1",  "type":"invoice.payment_succeeded", "account":"acct_dev_1",
		"livemode":true, "data":{"object":{"id":"in_1","customer":"cus_1"}} }`

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, stripeRequest(t, body))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	msg := publisher.only(t)
	if string(msg.Value) != body {
		t.Errorf("published body was modified\n got: %s\nwant: %s", msg.Value, body)
	}
	if msg.Topic != "billing.stripe.events.v1" {
		t.Errorf("topic = %q", msg.Topic)
	}
	if string(msg.Key) != "acct_dev_1" {
		t.Errorf("partition key = %q, want the Connect account", msg.Key)
	}

	headers := map[string]string{}
	for _, h := range msg.Headers {
		headers[h.Key] = string(h.Value)
	}
	if headers[events.HeaderSource] != events.SourceStripe {
		t.Errorf("source header = %q", headers[events.HeaderSource])
	}
	if headers[events.HeaderEventID] != "evt_1" {
		t.Errorf("event id header = %q", headers[events.HeaderEventID])
	}
	if headers[events.HeaderEventType] != "invoice.payment_succeeded" {
		t.Errorf("event type header = %q", headers[events.HeaderEventType])
	}
	if headers[events.HeaderLivemode] != "true" {
		t.Errorf("livemode header = %q", headers[events.HeaderLivemode])
	}
	if headers[events.HeaderReceivedAt] == "" {
		t.Error("received-at header is missing")
	}
}

func TestOpenMeterWebhookIsVerifiedAndPublished(t *testing.T) {
	publisher := &recordingPublisher{}
	server := newTestServer(t, publisher)

	body := `{"id":"01J2KNP","type":"invoice.updated","data":{"id":"inv_1","customer":{"id":"cus_om_1"}}}`

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, openMeterRequest(t, body))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}

	msg := publisher.only(t)
	if msg.Topic != "billing.openmeter.invoices.v1" {
		t.Errorf("topic = %q", msg.Topic)
	}
	if string(msg.Key) != "cus_om_1" {
		t.Errorf("partition key = %q, want the OpenMeter customer id", msg.Key)
	}
}

// Svix-hosted channels send the same triple under svix-* names.
func TestOpenMeterWebhookAcceptsSvixHeaderNames(t *testing.T) {
	publisher := &recordingPublisher{}
	server := newTestServer(t, publisher)

	body := `{"id":"01J2KNP","type":"invoice.created","data":{"id":"inv_1","customer":{"id":"cus_om_1"}}}`
	req := openMeterRequest(t, body)

	for _, name := range []string{"webhook-id", "webhook-timestamp", "webhook-signature"} {
		value := req.Header.Get(name)
		req.Header.Del(name)
		req.Header.Set(strings.Replace(name, "webhook", "svix", 1), value)
	}

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	if publisher.count() != 1 {
		t.Error("nothing was published")
	}
}

// The security boundary: an unsigned or forged body must never reach Kafka.
func TestUnverifiedRequestsAreRejectedAndNotPublished(t *testing.T) {
	cases := []struct {
		name    string
		build   func(t *testing.T) *http.Request
		wantMin int
		wantMax int
	}{
		{
			name: "no signature header",
			build: func(t *testing.T) *http.Request {
				return httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(`{"id":"evt_1","type":"x"}`))
			},
			wantMin: 400, wantMax: 400,
		},
		{
			name: "signature over a different body",
			build: func(t *testing.T) *http.Request {
				req := stripeRequest(t, `{"id":"evt_1","type":"invoice.paid","amount":100}`)
				req.Body = io.NopCloser(strings.NewReader(`{"id":"evt_1","type":"invoice.paid","amount":900}`))
				return req
			},
			wantMin: 400, wantMax: 400,
		},
		{
			name: "stale timestamp",
			build: func(t *testing.T) *http.Request {
				body := `{"id":"evt_1","type":"invoice.paid"}`
				ts := strconv.FormatInt(time.Now().Add(-time.Hour).Unix(), 10)
				mac := hmac.New(sha256.New, []byte(testStripeSecret))
				mac.Write([]byte(ts + "." + body))
				req := httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(body))
				req.Header.Set("Stripe-Signature", "t="+ts+",v1="+hex.EncodeToString(mac.Sum(nil)))
				return req
			},
			wantMin: 400, wantMax: 400,
		},
		{
			name: "empty body",
			build: func(t *testing.T) *http.Request {
				return stripeRequest(t, "")
			},
			wantMin: 400, wantMax: 400,
		},
		{
			name: "openmeter signature on the stripe endpoint",
			build: func(t *testing.T) *http.Request {
				req := openMeterRequest(t, `{"id":"n1","type":"invoice.updated"}`)
				req.URL.Path = "/webhooks/stripe"
				return req
			},
			wantMin: 400, wantMax: 400,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &recordingPublisher{}
			server := newTestServer(t, publisher)

			rec := httptest.NewRecorder()
			server.ServeHTTP(rec, tc.build(t))

			if rec.Code < tc.wantMin || rec.Code > tc.wantMax {
				t.Errorf("status = %d, want %d..%d", rec.Code, tc.wantMin, tc.wantMax)
			}
			if publisher.count() != 0 {
				t.Error("an unverified request reached the billing topic")
			}
		})
	}
}

// A signed body we cannot route is rejected rather than parked on the topic.
func TestVerifiedButUnrecognisableBodyIsRejected(t *testing.T) {
	publisher := &recordingPublisher{}
	server := newTestServer(t, publisher)

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, stripeRequest(t, `{"not":"an event"}`))

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if publisher.count() != 0 {
		t.Error("an unroutable body was published")
	}
}

// Answering 200 for an event we failed to persist would lose it permanently:
// Stripe treats 2xx as delivered and never retries.
func TestPublishFailureReturns500SoTheProviderRetries(t *testing.T) {
	publisher := &recordingPublisher{err: errors.New("all brokers down")}
	server := newTestServer(t, publisher)

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, stripeRequest(t, `{"id":"evt_1","type":"invoice.paid","data":{"object":{"id":"in_1"}}}`))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 so the event is redelivered", rec.Code)
	}
}

func TestOversizedBodyIsRejected(t *testing.T) {
	publisher := &recordingPublisher{}
	cfg := config.Producer{
		MaxBodyBytes:           64,
		StripeWebhookSecrets:   []string{testStripeSecret},
		StripeToleranceSeconds: 300,
		Kafka:                  config.Kafka{TopicStripe: "t", WriteTimeout: time.Second},
	}
	server := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), publisher).Routes()

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, stripeRequest(t, `{"id":"evt_1","padding":"`+strings.Repeat("x", 200)+`"}`))

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if publisher.count() != 0 {
		t.Error("an oversized body was published")
	}
}

func TestHealthAndReadiness(t *testing.T) {
	server := newTestServer(t, &recordingPublisher{})

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		rec := httptest.NewRecorder()
		server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("GET %s = %d, want 200", path, rec.Code)
		}
	}
}

// Without secrets there is no boundary, so the doorman must refuse traffic
// rather than forward unverified bodies.
func TestUnconfiguredEndpointRefusesTraffic(t *testing.T) {
	publisher := &recordingPublisher{}
	cfg := config.Producer{
		MaxBodyBytes:           1 << 20,
		StripeToleranceSeconds: 300,
		Kafka:                  config.Kafka{TopicStripe: "t", WriteTimeout: time.Second},
	}
	server := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), publisher).Routes()

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/webhooks/stripe", strings.NewReader(`{}`)))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503", rec.Code)
	}

	rec = httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("readyz = %d, want 503 with no secrets configured", rec.Code)
	}
}

func TestGetOnAWebhookEndpointIsRejected(t *testing.T) {
	server := newTestServer(t, &recordingPublisher{})

	rec := httptest.NewRecorder()
	server.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/webhooks/stripe", nil))
	if rec.Code == http.StatusOK {
		t.Fatal("GET on a webhook endpoint returned 200")
	}
}
