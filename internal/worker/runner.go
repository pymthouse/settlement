// Package worker is the settlement worker's plumbing: it consumes the billing
// topics, guarantees each event is handled once and in order per customer,
// retries what is worth retrying, and parks what is not.
//
// Everything that decides *what an event means* lives in the lifecycle
// package. This one only decides when it runs, how often it is retried, and
// when its offset may be committed.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand/v2"
	"strconv"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/dedupe"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/kafkax"
	"github.com/pymthouse/settlement/internal/lifecycle"
	"github.com/pymthouse/settlement/internal/metrics"
)

// Settler turns a raw event body into invoice progress. It is an interface so
// the retry, dedupe and dead-letter policies can be exercised without an
// OpenMeter or a Stripe on the other end.
type Settler interface {
	// HandleOpenMeterNotification processes an OpenMeter notification and
	// returns the handler that ran, for metrics.
	HandleOpenMeterNotification(ctx context.Context, body []byte) (string, error)
	// HandleStripeEvent processes a Stripe webhook body.
	HandleStripeEvent(ctx context.Context, body []byte) (string, error)
	// HandleCollectRequest processes a pymthouse-originated collect-request
	// body, raising the customer's pending gathering lines.
	HandleCollectRequest(ctx context.Context, body []byte) (string, error)
}

// DLQPublisher parks messages that could not be settled.
type DLQPublisher interface {
	Publish(ctx context.Context, msg kafka.Message) error
}

// Runner consumes the billing topics and settles invoices.
type Runner struct {
	cfg     config.Worker
	log     *slog.Logger
	settler Settler
	claims  dedupe.Store
	dlq     DLQPublisher
	readers map[string]*kafka.Reader
	tracker *offsetTracker

	// halt cancels the run when the retry policy says to stop rather than
	// dead-letter. Set while Run is executing.
	halt func()
}

// New builds a Runner over readers for each billing topic.
func New(cfg config.Worker, log *slog.Logger, settler Settler, claims dedupe.Store, dlq DLQPublisher, readers map[string]*kafka.Reader) *Runner {
	return &Runner{
		cfg:     cfg,
		log:     log,
		settler: settler,
		claims:  claims,
		dlq:     dlq,
		readers: readers,
		tracker: newOffsetTracker(),
	}
}

// Run consumes until ctx is cancelled, then drains cleanly.
//
// Shutdown order matters: stop fetching, let in-flight work finish, commit the
// contiguous run, and only then close the readers. Committing before the work
// drains would claim offsets whose invoices were never settled.
func (r *Runner) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	r.halt = cancel

	pool := newLanePool(runCtx, r.cfg.Lanes, r.cfg.LaneBuffer)

	var consumers sync.WaitGroup
	for topic, reader := range r.readers {
		consumers.Add(1)
		go func(topic string, reader *kafka.Reader) {
			defer consumers.Done()
			r.consume(runCtx, topic, reader, pool)
		}(topic, reader)
	}

	committerDone := make(chan struct{})
	go func() {
		defer close(committerDone)
		r.commitLoop(runCtx)
	}()

	r.log.Info("worker running",
		"lanes", r.cfg.Lanes,
		"topics", topicNames(r.readers),
		"group", r.cfg.Kafka.ConsumerGroup,
		"on_retry_exhausted", r.cfg.OnRetryExhausted,
	)

	consumers.Wait()
	// Closing the pool waits for every lane to finish the work it accepted.
	pool.close()
	<-committerDone

	// Final commit outside the cancelled context, so the offsets earned before
	// shutdown are not replayed on the next boot.
	commitCtx, commitCancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.ShutdownTimeout)
	defer commitCancel()
	r.commit(commitCtx)

	r.log.Info("worker drained", "in_flight", r.tracker.inFlight())
	return nil
}

// consume pulls messages from one topic and hands them to ordered lanes.
func (r *Runner) consume(ctx context.Context, topic string, reader *kafka.Reader, pool *lanePool) {
	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				r.log.Info("consumer stopping", "topic", topic)
				return
			}
			// A fetch error is nearly always a rebalance or a broker blip.
			// Pause briefly so a persistent failure cannot spin the CPU.
			r.log.Error("fetch failed", "topic", topic, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}

		metrics.EventsConsumed.WithLabelValues(topic).Inc()
		r.tracker.track(msg)

		message := msg
		submitErr := pool.submit(ctx, job{
			key: partitionKeyOf(message),
			run: func(jobCtx context.Context) {
				metrics.InFlight.Inc()
				defer metrics.InFlight.Dec()
				if r.process(jobCtx, message) {
					r.tracker.complete(message)
				}
			},
		})
		if submitErr != nil {
			// Only happens on shutdown; the message stays uncommitted and is
			// re-delivered to whoever picks up the partition next.
			r.log.Info("dropping unstarted message on shutdown",
				"topic", message.Topic, "partition", message.Partition, "offset", message.Offset)
			return
		}
	}
}

// process runs one message through dedupe, the handler, and the retry policy.
// It returns true when the message is resolved (success, duplicate, or parked
// on the DLQ) and false when it must stay uncommitted for redelivery.
func (r *Runner) process(ctx context.Context, msg kafka.Message) bool {
	source := kafkax.Header(msg, events.HeaderSource)
	eventID := kafkax.Header(msg, events.HeaderEventID)
	eventType := kafkax.Header(msg, events.HeaderEventType)

	log := r.log.With(
		"source", source,
		"event_id", eventID,
		"event_type", eventType,
		"topic", msg.Topic,
		"partition", msg.Partition,
		"offset", msg.Offset,
	)

	if source == "" || eventID == "" {
		// Headers are written by the doorman; their absence means the message
		// came from somewhere else entirely and cannot be trusted to route.
		return r.deadLetter(ctx, msg, "missing_headers",
			errors.New("message has no settlement source/event-id headers"), 0)
	}

	claimKey := claimKeyOf(msg, source, eventID)
	claimed, err := r.claims.Claim(ctx, claimKey)
	if err != nil {
		// The dedupe store is unavailable. Refusing to proceed is the only
		// safe option: processing blind risks charging twice.
		log.Error("dedupe store unavailable; leaving message uncommitted", "error", err)
		r.stopForSafety("dedupe store unavailable")
		return false
	}
	if !claimed {
		metrics.DuplicatesDropped.WithLabelValues(source).Inc()
		log.Info("duplicate event dropped")
		return true
	}

	handler, err := r.attempt(ctx, log, source, msg)
	if err == nil {
		return true
	}

	// Shutdown interrupted a retryable attempt: leave the claim held and the
	// offset uncommitted so the message is redelivered after restart rather
	// than released into a DLQ as if it had permanently failed.
	if ctx.Err() != nil {
		log.Info("shutdown during processing; leaving message uncommitted", "error", err)
		return false
	}

	// Release the claim so a redrive or redelivery can try again; a failed
	// event that stays claimed would be silently skipped forever.
	if releaseErr := r.claims.Release(context.WithoutCancel(ctx), claimKey); releaseErr != nil {
		log.Error("could not release dedupe claim", "error", releaseErr)
	}

	metrics.EventsProcessed.WithLabelValues(source, handler, "failed").Inc()
	return r.deadLetter(ctx, msg, faults.Reason(err), err, r.cfg.MaxAttempts)
}

// attempt runs the handler, retrying transient failures with backoff.
func (r *Runner) attempt(ctx context.Context, log *slog.Logger, source string, msg kafka.Message) (string, error) {
	handler := lifecycle.HandlerNoop

	for tries := 1; ; tries++ {
		start := time.Now()
		var err error
		handler, err = r.handle(ctx, source, msg.Value)
		metrics.EventDuration.WithLabelValues(handler).Observe(time.Since(start).Seconds())

		if err == nil {
			metrics.EventsProcessed.WithLabelValues(source, handler, "ok").Inc()
			if tries > 1 {
				log.Info("event settled after retries", "handler", handler, "attempts", tries)
			} else {
				log.Debug("event settled", "handler", handler)
			}
			return handler, nil
		}

		if ctx.Err() != nil {
			return handler, fmt.Errorf("shutdown before completion: %w", err)
		}

		if faults.IsPermanent(err) {
			log.Error("permanent failure",
				"handler", handler, "reason", faults.Reason(err), "error", err)
			return handler, err
		}

		if tries >= r.cfg.MaxAttempts {
			log.Error("retries exhausted",
				"handler", handler, "attempts", tries, "error", err)
			return handler, err
		}

		delay := r.backoff(tries)
		metrics.EventRetries.WithLabelValues(handler).Inc()
		log.Warn("retrying event",
			"handler", handler, "attempt", tries, "next_in", delay.String(), "error", err)

		select {
		case <-ctx.Done():
			return handler, fmt.Errorf("shutdown before retry: %w", err)
		case <-time.After(delay):
		}
	}
}

// handle dispatches to the settler by source.
func (r *Runner) handle(ctx context.Context, source string, body []byte) (string, error) {
	switch source {
	case events.SourceOpenMeter:
		return r.settler.HandleOpenMeterNotification(ctx, body)
	case events.SourceStripe:
		return r.settler.HandleStripeEvent(ctx, body)
	case events.SourceCollectRequest:
		return r.settler.HandleCollectRequest(ctx, body)
	default:
		return lifecycle.HandlerNoop, faults.Permanentf("unknown_source", "no handler for source %q", source)
	}
}

// backoff returns the delay before attempt n+1: exponential, capped, and
// jittered so a broker-wide outage does not produce a synchronised thundering
// herd when it recovers.
func (r *Runner) backoff(attempt int) time.Duration {
	delay := float64(r.cfg.RetryBaseDelay) * math.Pow(2, float64(attempt-1))
	if max := float64(r.cfg.RetryMaxDelay); delay > max {
		delay = max
	}
	if r.cfg.RetryJitter > 0 {
		// Jitter downward only, so the cap remains a real bound.
		delay *= 1 - rand.Float64()*r.cfg.RetryJitter
	}
	return time.Duration(delay)
}

// deadLetter parks a message, or halts, according to the configured policy.
// It returns true when the message was successfully parked (resolved) and
// false when the worker halted without parking it.
func (r *Runner) deadLetter(ctx context.Context, msg kafka.Message, reason string, cause error, attempts int) bool {
	source := kafkax.Header(msg, events.HeaderSource)

	if r.cfg.OnRetryExhausted == config.PolicyHalt {
		r.log.Error("halting: message could not be settled",
			"reason", reason, "error", cause,
			"topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		r.stopForSafety(reason)
		return false
	}

	headers := append([]kafka.Header{}, msg.Headers...)
	headers = append(headers,
		kafka.Header{Key: events.HeaderDLQReason, Value: []byte(reason)},
		kafka.Header{Key: events.HeaderDLQError, Value: []byte(truncate(cause.Error(), 1024))},
		kafka.Header{Key: events.HeaderDLQTopic, Value: []byte(msg.Topic)},
		kafka.Header{Key: events.HeaderDLQPartition, Value: []byte(strconv.Itoa(msg.Partition))},
		kafka.Header{Key: events.HeaderDLQOffset, Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		kafka.Header{Key: events.HeaderDLQAttempts, Value: []byte(strconv.Itoa(attempts))},
		kafka.Header{Key: events.HeaderDLQAt, Value: []byte(time.Now().UTC().Format(time.RFC3339Nano))},
	)

	// Use a context detached from shutdown: parking the message is the last
	// chance to preserve it before its offset is committed.
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), r.cfg.Kafka.WriteTimeout)
	defer cancel()

	err := r.dlq.Publish(publishCtx, kafka.Message{
		Topic:   r.cfg.Kafka.TopicDLQ,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: headers,
	})
	if err != nil {
		// The event exists nowhere else. Halting keeps the offset uncommitted
		// so it is redelivered rather than lost.
		r.log.Error("could not dead-letter message; halting to avoid data loss",
			"error", err, "topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
		r.stopForSafety("dlq publish failed")
		return false
	}

	metrics.DeadLettered.WithLabelValues(source, reason).Inc()
	r.log.Error("message dead-lettered",
		"reason", reason, "error", cause, "dlq_topic", r.cfg.Kafka.TopicDLQ,
		"source_topic", msg.Topic, "partition", msg.Partition, "offset", msg.Offset)
	return true
}

// stopForSafety cancels the run so nothing further is committed.
func (r *Runner) stopForSafety(reason string) {
	r.log.Error("stopping worker for safety", "reason", reason)
	if r.halt != nil {
		r.halt()
	}
}

// commitLoop commits the contiguous completed run at a steady cadence.
func (r *Runner) commitLoop(ctx context.Context) {
	ticker := time.NewTicker(r.cfg.CommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.commit(ctx)
		}
	}
}

// commit hands the safe offsets to their readers.
func (r *Runner) commit(ctx context.Context) {
	ready := r.tracker.commitable()
	if len(ready) == 0 {
		return
	}

	byTopic := make(map[string][]kafka.Message, len(r.readers))
	for _, msg := range ready {
		byTopic[msg.Topic] = append(byTopic[msg.Topic], msg)
	}

	for topic, msgs := range byTopic {
		reader, ok := r.readers[topic]
		if !ok {
			continue
		}
		if err := reader.CommitMessages(ctx, msgs...); err != nil {
			// Re-arm so the next tick retries these watermarks rather than
			// losing them after commitable() cleared the slot. Worst case the
			// messages are re-delivered and dropped by dedupe.
			r.tracker.rearm(msgs)
			if ctx.Err() == nil {
				r.log.Error("commit failed", "topic", topic, "error", err)
			}
			continue
		}
		for _, msg := range msgs {
			metrics.CommittedOffset.
				WithLabelValues(msg.Topic, strconv.Itoa(msg.Partition)).
				Set(float64(msg.Offset + 1))
		}
	}
}

// claimKeyOf builds the idempotency key for a message.
//
// A deliberate replay carries a batch id, which is folded into the key so the
// event is processed again. That is the whole point of replaying after a
// mapping bug: the events were handled before, incorrectly, and must be
// handled once more with the fix in place. Ordinary redeliveries carry no
// batch id and stay suppressed.
func claimKeyOf(msg kafka.Message, source, eventID string) string {
	key := source + ":" + eventID
	if batch := kafkax.Header(msg, events.HeaderReplayOf); batch != "" {
		return key + ":replay:" + batch
	}
	return key
}

// partitionKeyOf returns the ordering key for a message, preferring the header
// the doorman wrote and falling back to the Kafka key.
func partitionKeyOf(msg kafka.Message) string {
	if key := kafkax.Header(msg, events.HeaderKey); key != "" {
		return key
	}
	return string(msg.Key)
}

func topicNames(readers map[string]*kafka.Reader) []string {
	out := make([]string, 0, len(readers))
	for topic := range readers {
		out = append(out, topic)
	}
	return out
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
