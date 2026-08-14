package worker

import (
	"testing"
	"time"

	"github.com/segmentio/kafka-go"

	"github.com/pymthouse/settlement/internal/config"
	"github.com/pymthouse/settlement/internal/events"
)

func TestClaimKeyNamespacesBySource(t *testing.T) {
	msg := kafka.Message{}

	if got := claimKeyOf(msg, "stripe", "evt_1"); got != "stripe:evt_1" {
		t.Errorf("claim key = %q", got)
	}
	// Provider ids are only unique within their own system.
	if claimKeyOf(msg, "stripe", "evt_1") == claimKeyOf(msg, "openmeter", "evt_1") {
		t.Error("two sources with the same event id share a claim key")
	}
}

// A deliberate replay must be processed again — that is the entire point of
// replaying after a mapping bug ships.
func TestReplayBatchBypassesDeduplication(t *testing.T) {
	plain := kafka.Message{}
	replayed := kafka.Message{Headers: []kafka.Header{
		{Key: events.HeaderReplayOf, Value: []byte("20260804T120000Z")},
	}}

	normal := claimKeyOf(plain, "stripe", "evt_1")
	replay := claimKeyOf(replayed, "stripe", "evt_1")

	if normal == replay {
		t.Fatal("a replayed message would be suppressed as a duplicate")
	}

	// Two replays of the same batch must still deduplicate against each other.
	if claimKeyOf(replayed, "stripe", "evt_1") != replay {
		t.Error("claim keys are not stable within a replay batch")
	}
}

func TestPartitionKeyPrefersTheHeader(t *testing.T) {
	withHeader := kafka.Message{
		Key:     []byte("fallback"),
		Headers: []kafka.Header{{Key: events.HeaderKey, Value: []byte("cus_1")}},
	}
	if got := partitionKeyOf(withHeader); got != "cus_1" {
		t.Errorf("partition key = %q, want the header value", got)
	}

	withoutHeader := kafka.Message{Key: []byte("fallback")}
	if got := partitionKeyOf(withoutHeader); got != "fallback" {
		t.Errorf("partition key = %q, want the Kafka key", got)
	}
}

func TestBackoffGrowsAndStaysBounded(t *testing.T) {
	runner := &Runner{cfg: config.Worker{
		RetryBaseDelay: 100 * time.Millisecond,
		RetryMaxDelay:  2 * time.Second,
		RetryJitter:    0,
	}}

	want := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		400 * time.Millisecond,
		800 * time.Millisecond,
		1600 * time.Millisecond,
		2 * time.Second, // capped
		2 * time.Second,
	}
	for attempt, expected := range want {
		if got := runner.backoff(attempt + 1); got != expected {
			t.Errorf("backoff(%d) = %s, want %s", attempt+1, got, expected)
		}
	}
}

// Jitter must only shorten the delay, so the configured cap stays a real bound.
func TestBackoffJitterStaysWithinTheCap(t *testing.T) {
	runner := &Runner{cfg: config.Worker{
		RetryBaseDelay: time.Second,
		RetryMaxDelay:  4 * time.Second,
		RetryJitter:    0.5,
	}}

	for i := 0; i < 200; i++ {
		got := runner.backoff(10) // well past the cap
		if got > 4*time.Second {
			t.Fatalf("backoff exceeded the cap: %s", got)
		}
		if got < 2*time.Second {
			t.Fatalf("jitter reduced the delay below 50%% of the cap: %s", got)
		}
	}
}

func TestTruncateBoundsDLQErrorText(t *testing.T) {
	long := make([]byte, 3000)
	for i := range long {
		long[i] = 'x'
	}
	got := truncate(string(long), 1024)
	if len(got) > 1024+len("…") {
		t.Fatalf("truncate returned %d bytes", len(got))
	}
	if short := truncate("fine", 1024); short != "fine" {
		t.Errorf("truncate altered a short string: %q", short)
	}
}

func TestTopicNames(t *testing.T) {
	readers := map[string]*kafka.Reader{"a": nil, "b": nil}
	if got := topicNames(readers); len(got) != 2 {
		t.Fatalf("topicNames = %v", got)
	}
}
