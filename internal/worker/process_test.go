package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/dedupe"
	"github.com/pymthouse/settlement/internal/events"
	"github.com/pymthouse/settlement/internal/faults"
	"github.com/pymthouse/settlement/internal/lifecycle"
)

// fakeSettler records calls and returns a scripted sequence of results.
type fakeSettler struct {
	mu      sync.Mutex
	calls   int
	results []error
}

func (f *fakeSettler) next() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.calls++
	if len(f.results) == 0 {
		return nil
	}
	result := f.results[0]
	if len(f.results) > 1 {
		f.results = f.results[1:]
	}
	return result
}

func (f *fakeSettler) HandleOpenMeterNotification(context.Context, []byte) (string, error) {
	return lifecycle.HandlerDraftSync, f.next()
}

func (f *fakeSettler) HandleStripeEvent(context.Context, []byte) (string, error) {
	return lifecycle.HandlerPaymentStatus, f.next()
}

func (f *fakeSettler) HandleCollectRequest(context.Context, []byte) (string, error) {
	return lifecycle.HandlerCollectRequest, f.next()
}

func (f *fakeSettler) attempts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// fakeDLQ captures parked messages.
type fakeDLQ struct {
	mu       sync.Mutex
	messages []kafka.Message
	err      error
}

func (f *fakeDLQ) Publish(_ context.Context, msg kafka.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.messages = append(f.messages, msg)
	return nil
}

func (f *fakeDLQ) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.messages)
}

func (f *fakeDLQ) only(t *testing.T) kafka.Message {
	t.Helper()
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.messages) != 1 {
		t.Fatalf("expected one dead-lettered message, got %d", len(f.messages))
	}
	return f.messages[0]
}

// brokenStore fails every claim, standing in for a Redis outage.
type brokenStore struct{}

func (brokenStore) Claim(context.Context, string) (bool, error) {
	return false, errors.New("redis unreachable")
}
func (brokenStore) Release(context.Context, string) error { return nil }
func (brokenStore) Close() error                          { return nil }

func testRunner(t *testing.T, settler Settler, claims dedupe.Store, dlq DLQPublisher, mutate func(*config.Worker)) (*Runner, *bool) {
	t.Helper()

	cfg := config.Worker{
		MaxAttempts:      3,
		RetryBaseDelay:   time.Millisecond,
		RetryMaxDelay:    2 * time.Millisecond,
		OnRetryExhausted: config.PolicyDLQ,
		Kafka: config.Kafka{
			TopicDLQ:     "billing.settlement.dlq.v1",
			WriteTimeout: time.Second,
		},
	}
	if mutate != nil {
		mutate(&cfg)
	}

	runner := New(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), settler, claims, dlq, nil)

	halted := false
	runner.halt = func() { halted = true }
	return runner, &halted
}

func eventMessage(source, eventID string, extraHeaders ...kafka.Header) kafka.Message {
	headers := []kafka.Header{
		{Key: events.HeaderSource, Value: []byte(source)},
		{Key: events.HeaderEventID, Value: []byte(eventID)},
		{Key: events.HeaderEventType, Value: []byte("invoice.updated")},
		{Key: events.HeaderKey, Value: []byte("cus_1")},
	}
	return kafka.Message{
		Topic:     "billing.openmeter.invoices.v1",
		Partition: 0,
		Offset:    42,
		Key:       []byte("cus_1"),
		Value:     []byte(`{"id":"n1"}`),
		Headers:   append(headers, extraHeaders...),
	}
}

func TestProcessSettlesAndKeepsTheClaim(t *testing.T) {
	settler := &fakeSettler{}
	claims := dedupe.NewMemory(time.Hour)
	dlq := &fakeDLQ{}
	runner, halted := testRunner(t, settler, claims, dlq, nil)

	runner.process(context.Background(), eventMessage(events.SourceOpenMeter, "evt_1"))

	if settler.attempts() != 1 {
		t.Errorf("handler ran %d times, want 1", settler.attempts())
	}
	if dlq.count() != 0 {
		t.Error("a successful event was dead-lettered")
	}
	if *halted {
		t.Error("the worker halted on a successful event")
	}

	// The claim must survive success, so a redelivery is suppressed.
	if won, _ := claims.Claim(context.Background(), "openmeter:evt_1"); won {
		t.Error("the claim was released after success; a redelivery would reprocess the event")
	}
}

// A pymthouse collect request must reach HandleCollectRequest through the
// same dispatch, dedupe and DLQ machinery every other source gets — it has
// no bespoke path.
func TestProcessDispatchesCollectRequest(t *testing.T) {
	settler := &fakeSettler{}
	claims := dedupe.NewMemory(time.Hour)
	dlq := &fakeDLQ{}
	runner, halted := testRunner(t, settler, claims, dlq, nil)

	runner.process(context.Background(), eventMessage(events.SourceCollectRequest, "req_1"))

	if settler.attempts() != 1 {
		t.Errorf("handler ran %d times, want 1", settler.attempts())
	}
	if dlq.count() != 0 {
		t.Error("a successful collect request was dead-lettered")
	}
	if *halted {
		t.Error("the worker halted on a successful collect request")
	}
}

func TestProcessDropsDuplicates(t *testing.T) {
	settler := &fakeSettler{}
	claims := dedupe.NewMemory(time.Hour)
	runner, _ := testRunner(t, settler, claims, &fakeDLQ{}, nil)

	msg := eventMessage(events.SourceStripe, "evt_dup")
	runner.process(context.Background(), msg)
	runner.process(context.Background(), msg)

	if settler.attempts() != 1 {
		t.Fatalf("handler ran %d times for a duplicate delivery, want 1", settler.attempts())
	}
}

// A deliberate replay carries a batch id and must be processed again.
func TestProcessRunsReplayedMessagesAgain(t *testing.T) {
	settler := &fakeSettler{}
	claims := dedupe.NewMemory(time.Hour)
	runner, _ := testRunner(t, settler, claims, &fakeDLQ{}, nil)

	runner.process(context.Background(), eventMessage(events.SourceStripe, "evt_1"))
	runner.process(context.Background(), eventMessage(events.SourceStripe, "evt_1",
		kafka.Header{Key: events.HeaderReplayOf, Value: []byte("batch-1")}))

	if settler.attempts() != 2 {
		t.Fatalf("handler ran %d times, want 2 — the replay was suppressed", settler.attempts())
	}
}

func TestProcessRetriesTransientFailures(t *testing.T) {
	settler := &fakeSettler{results: []error{
		errors.New("openmeter 503"),
		errors.New("openmeter 503"),
		nil,
	}}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	runner.process(context.Background(), eventMessage(events.SourceOpenMeter, "evt_1"))

	if settler.attempts() != 3 {
		t.Errorf("handler ran %d times, want 3", settler.attempts())
	}
	if dlq.count() != 0 {
		t.Error("an event that eventually succeeded was dead-lettered")
	}
}

// Retrying a validation error would occupy the lane for hours and change
// nothing, so permanent failures skip straight to the DLQ.
func TestPermanentFailuresSkipRetries(t *testing.T) {
	settler := &fakeSettler{results: []error{
		faults.Permanentf("missing_connect_account", "no account configured"),
	}}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	runner.process(context.Background(), eventMessage(events.SourceOpenMeter, "evt_1"))

	if settler.attempts() != 1 {
		t.Errorf("a permanent failure was retried %d times", settler.attempts())
	}

	parked := dlq.only(t)
	headers := headerMap(parked)
	if headers[events.HeaderDLQReason] != "missing_connect_account" {
		t.Errorf("DLQ reason = %q", headers[events.HeaderDLQReason])
	}
	if headers[events.HeaderDLQTopic] != "billing.openmeter.invoices.v1" {
		t.Errorf("source topic header = %q; a redrive needs it to route back", headers[events.HeaderDLQTopic])
	}
	if headers[events.HeaderDLQOffset] != "42" {
		t.Errorf("source offset header = %q", headers[events.HeaderDLQOffset])
	}
	if string(parked.Value) != `{"id":"n1"}` {
		t.Errorf("the parked body was modified: %s", parked.Value)
	}
	if headers[events.HeaderEventID] != "evt_1" {
		t.Error("the original event headers were not preserved")
	}
}

func TestExhaustedRetriesAreDeadLettered(t *testing.T) {
	settler := &fakeSettler{results: []error{errors.New("stripe 502")}}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	runner.process(context.Background(), eventMessage(events.SourceStripe, "evt_1"))

	if settler.attempts() != 3 {
		t.Errorf("handler ran %d times, want MaxAttempts=3", settler.attempts())
	}
	if headers := headerMap(dlq.only(t)); headers[events.HeaderDLQReason] != "retry_exhausted" {
		t.Errorf("DLQ reason = %q, want retry_exhausted", headers[events.HeaderDLQReason])
	}
}

// A failed event must be reclaimable, or a redrive would be silently skipped.
func TestFailureReleasesTheClaim(t *testing.T) {
	settler := &fakeSettler{results: []error{faults.Permanentf("bad_line_amount", "nope")}}
	claims := dedupe.NewMemory(time.Hour)
	runner, _ := testRunner(t, settler, claims, &fakeDLQ{}, nil)

	runner.process(context.Background(), eventMessage(events.SourceStripe, "evt_1"))

	won, err := claims.Claim(context.Background(), "stripe:evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if !won {
		t.Fatal("the claim was not released; the event could never be redriven")
	}
}

// halt keeps the offset uncommitted so nothing is lost, at the cost of the
// partition stopping — the safest choice for money.
func TestHaltPolicyStopsInsteadOfParking(t *testing.T) {
	settler := &fakeSettler{results: []error{faults.Permanentf("total_mismatch", "off by a lot")}}
	dlq := &fakeDLQ{}
	runner, halted := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, func(c *config.Worker) {
		c.OnRetryExhausted = config.PolicyHalt
	})

	if runner.process(context.Background(), eventMessage(events.SourceOpenMeter, "evt_1")) {
		t.Error("unresolved halt was marked complete")
	}

	if !*halted {
		t.Error("the worker did not halt")
	}
	if dlq.count() != 0 {
		t.Error("the message was parked despite the halt policy")
	}
}

// If the DLQ write fails the event exists nowhere else, so halting is the only
// way to avoid losing it.
func TestDeadLetterFailureHalts(t *testing.T) {
	settler := &fakeSettler{results: []error{faults.Permanentf("bad_line_amount", "nope")}}
	dlq := &fakeDLQ{err: errors.New("brokers unreachable")}
	runner, halted := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	if runner.process(context.Background(), eventMessage(events.SourceOpenMeter, "evt_1")) {
		t.Error("unresolved DLQ failure was marked complete")
	}

	if !*halted {
		t.Error("a failed DLQ publish did not halt the worker; the event would be lost")
	}
}

// Processing blind while the claim store is down risks charging twice.
func TestDedupeOutageStopsProcessing(t *testing.T) {
	settler := &fakeSettler{}
	dlq := &fakeDLQ{}
	runner, halted := testRunner(t, settler, brokenStore{}, dlq, nil)

	if runner.process(context.Background(), eventMessage(events.SourceStripe, "evt_1")) {
		t.Error("unresolved dedupe outage was marked complete")
	}

	if settler.attempts() != 0 {
		t.Error("an event was handled without a successful claim")
	}
	if !*halted {
		t.Error("the worker kept running with an unavailable dedupe store")
	}
	if dlq.count() != 0 {
		t.Error("the message was parked; it should stay uncommitted for redelivery")
	}
}

// Messages without the doorman's headers did not come through the verified
// front door and cannot be trusted to route.
func TestMessagesWithoutHeadersAreParked(t *testing.T) {
	settler := &fakeSettler{}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	runner.process(context.Background(), kafka.Message{
		Topic: "billing.stripe.events.v1", Value: []byte(`{"id":"evt_1"}`),
	})

	if settler.attempts() != 0 {
		t.Error("an unheadered message was handled")
	}
	if headers := headerMap(dlq.only(t)); headers[events.HeaderDLQReason] != "missing_headers" {
		t.Errorf("DLQ reason = %q", headers[events.HeaderDLQReason])
	}
}

func TestUnknownSourceIsPermanentlyRejected(t *testing.T) {
	settler := &fakeSettler{}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, nil)

	runner.process(context.Background(), eventMessage("paypal", "evt_1"))

	if settler.attempts() != 0 {
		t.Error("an unknown source reached a handler")
	}
	if headers := headerMap(dlq.only(t)); headers[events.HeaderDLQReason] != "unknown_source" {
		t.Errorf("DLQ reason = %q", headers[events.HeaderDLQReason])
	}
}

// Shutdown mid-retry must leave the offset uncommitted rather than parking a
// message that was only interrupted.
func TestShutdownDuringRetryDoesNotDiscardTheMessage(t *testing.T) {
	settler := &fakeSettler{results: []error{errors.New("openmeter 503")}}
	dlq := &fakeDLQ{}
	runner, _ := testRunner(t, settler, dedupe.NewMemory(time.Hour), dlq, func(c *config.Worker) {
		c.MaxAttempts = 100
		c.RetryBaseDelay = 50 * time.Millisecond
	})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	if runner.process(ctx, eventMessage(events.SourceOpenMeter, "evt_1")) {
		t.Error("shutdown during retry was marked complete")
	}

	if settler.attempts() >= 100 {
		t.Error("the retry loop ignored the cancelled context")
	}
	if dlq.count() != 0 {
		t.Error("a shutdown interruption was dead-lettered; the message should be redelivered")
	}
}

func headerMap(msg kafka.Message) map[string]string {
	out := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		out[h.Key] = string(h.Value)
	}
	return out
}
