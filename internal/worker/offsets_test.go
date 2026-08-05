package worker

import (
	"testing"

	"github.com/segmentio/kafka-go"
)

func msg(topic string, partition int, offset int64) kafka.Message {
	return kafka.Message{Topic: topic, Partition: partition, Offset: offset}
}

func TestOffsetTrackerCommitsInOrder(t *testing.T) {
	tracker := newOffsetTracker()

	for i := int64(0); i < 3; i++ {
		tracker.track(msg("billing", 0, i))
	}
	for i := int64(0); i < 3; i++ {
		tracker.complete(msg("billing", 0, i))
	}

	ready := tracker.commitable()
	if len(ready) != 1 {
		t.Fatalf("expected one commit, got %d", len(ready))
	}
	if ready[0].Offset != 2 {
		t.Fatalf("committed offset %d, want the highest contiguous offset 2", ready[0].Offset)
	}
}

// The core safety property: a message that finishes early must not carry the
// watermark past one still in flight behind it.
func TestOffsetTrackerHoldsBackOnAGap(t *testing.T) {
	tracker := newOffsetTracker()
	for i := int64(0); i < 4; i++ {
		tracker.track(msg("billing", 0, i))
	}

	// Offsets 1..3 finish; offset 0 is still working.
	tracker.complete(msg("billing", 0, 1))
	tracker.complete(msg("billing", 0, 3))
	tracker.complete(msg("billing", 0, 2))

	if ready := tracker.commitable(); len(ready) != 0 {
		t.Fatalf("committed %d message(s) while offset 0 was in flight", len(ready))
	}

	// Once the straggler lands the whole run becomes committable at once.
	tracker.complete(msg("billing", 0, 0))

	ready := tracker.commitable()
	if len(ready) != 1 || ready[0].Offset != 3 {
		t.Fatalf("expected a single commit at offset 3, got %+v", ready)
	}
}

func TestOffsetTrackerIsPerPartition(t *testing.T) {
	tracker := newOffsetTracker()

	tracker.track(msg("billing", 0, 10))
	tracker.track(msg("billing", 1, 5))
	tracker.track(msg("stripe", 0, 99))

	tracker.complete(msg("billing", 1, 5))
	tracker.complete(msg("stripe", 0, 99))

	ready := tracker.commitable()
	if len(ready) != 2 {
		t.Fatalf("expected two independent commits, got %d", len(ready))
	}

	// Deterministic ordering: billing/1 before stripe/0.
	if ready[0].Topic != "billing" || ready[0].Partition != 1 {
		t.Errorf("first commit = %s/%d, want billing/1", ready[0].Topic, ready[0].Partition)
	}
	if ready[1].Topic != "stripe" {
		t.Errorf("second commit topic = %s, want stripe", ready[1].Topic)
	}

	// billing/0 is still blocked by its in-flight message.
	if remaining := tracker.commitable(); len(remaining) != 0 {
		t.Fatalf("commitable did not clear after being read: %+v", remaining)
	}
}

func TestOffsetTrackerClearsAfterRead(t *testing.T) {
	tracker := newOffsetTracker()
	tracker.track(msg("billing", 0, 7))
	tracker.complete(msg("billing", 0, 7))

	if got := len(tracker.commitable()); got != 1 {
		t.Fatalf("first read returned %d, want 1", got)
	}
	if got := len(tracker.commitable()); got != 0 {
		t.Fatalf("second read returned %d, want 0 — offsets would be committed twice", got)
	}
}

func TestOffsetTrackerCountsInFlight(t *testing.T) {
	tracker := newOffsetTracker()
	tracker.track(msg("billing", 0, 0))
	tracker.track(msg("billing", 0, 1))

	if got := tracker.inFlight(); got != 2 {
		t.Fatalf("inFlight = %d, want 2", got)
	}
	tracker.complete(msg("billing", 0, 0))
	if got := tracker.inFlight(); got != 1 {
		t.Fatalf("inFlight = %d, want 1", got)
	}
}

// A completion for a partition we no longer own must not resurrect state or
// panic; the new owner's watermark is the authoritative one.
func TestOffsetTrackerIgnoresCompletionAfterForget(t *testing.T) {
	tracker := newOffsetTracker()
	tracker.track(msg("billing", 0, 3))
	tracker.forget("billing", 0)
	tracker.complete(msg("billing", 0, 3))

	if ready := tracker.commitable(); len(ready) != 0 {
		t.Fatalf("expected nothing commitable after forget, got %+v", ready)
	}
}

// A partition that starts mid-log (a fresh group joining at LastOffset) must
// commit from its first seen offset, not from zero.
func TestOffsetTrackerStartsAtFirstSeenOffset(t *testing.T) {
	tracker := newOffsetTracker()
	tracker.track(msg("billing", 0, 5_000))
	tracker.complete(msg("billing", 0, 5_000))

	ready := tracker.commitable()
	if len(ready) != 1 || ready[0].Offset != 5_000 {
		t.Fatalf("expected a commit at offset 5000, got %+v", ready)
	}
}
