package worker

import (
	"sort"
	"sync"

	"github.com/segmentio/kafka-go"
)

// offsetTracker decides which Kafka offsets are safe to commit.
//
// Messages are handed to ordered lanes and therefore finish out of order
// across partitions. Committing the offset of whichever message finished last
// would silently skip anything still in flight behind it — an invoice event
// lost on the next restart. So the tracker commits only the highest
// *contiguous* run of completed offsets per partition: everything below the
// watermark is genuinely done, and anything not yet finished is re-delivered
// after a crash and caught by the dedupe store.
type offsetTracker struct {
	mu         sync.Mutex
	partitions map[partitionKey]*partitionState
}

type partitionKey struct {
	topic     string
	partition int
}

type partitionState struct {
	// next is the offset the watermark is waiting on.
	next int64
	// done holds completed offsets above next, waiting for the gap to close.
	done map[int64]kafka.Message
	// commitable is the highest contiguous completed message, if any.
	commitable *kafka.Message
	// inFlight counts messages handed out but not yet completed.
	inFlight int
}

func newOffsetTracker() *offsetTracker {
	return &offsetTracker{partitions: make(map[partitionKey]*partitionState)}
}

// track registers a message that is about to be processed.
func (t *offsetTracker) track(msg kafka.Message) {
	key := partitionKey{topic: msg.Topic, partition: msg.Partition}

	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.partitions[key]
	if !ok {
		state = &partitionState{next: msg.Offset, done: make(map[int64]kafka.Message)}
		t.partitions[key] = state
	}
	state.inFlight++
}

// complete marks a message finished and advances the watermark as far as the
// contiguous run allows.
func (t *offsetTracker) complete(msg kafka.Message) {
	key := partitionKey{topic: msg.Topic, partition: msg.Partition}

	t.mu.Lock()
	defer t.mu.Unlock()

	state, ok := t.partitions[key]
	if !ok {
		// A completion without a matching track means the partition was
		// revoked and re-assigned mid-flight; the new owner's watermark is
		// authoritative, so drop this one.
		return
	}
	state.inFlight--
	// Stale redeliveries below the watermark must not re-enter done: they
	// would never become contiguously advanceable and would pin memory.
	if msg.Offset < state.next {
		return
	}
	state.done[msg.Offset] = msg

	for {
		candidate, ok := state.done[state.next]
		if !ok {
			break
		}
		delete(state.done, state.next)
		state.commitable = &candidate
		state.next++
	}
}

// rearm restores messages whose commit failed so the next tick retries them,
// unless a newer committable offset for the same partition already exists.
func (t *offsetTracker) rearm(msgs []kafka.Message) {
	t.mu.Lock()
	defer t.mu.Unlock()

	for i := range msgs {
		msg := msgs[i]
		key := partitionKey{topic: msg.Topic, partition: msg.Partition}
		state, ok := t.partitions[key]
		if !ok {
			continue
		}
		if state.commitable != nil && state.commitable.Offset > msg.Offset {
			continue
		}
		m := msg
		state.commitable = &m
	}
}

// commitable returns, and clears, the messages whose offsets are now safe to
// commit. kafka-go commits offset+1 for each message handed to it, so
// returning the highest contiguous message per partition commits the whole run.
func (t *offsetTracker) commitable() []kafka.Message {
	t.mu.Lock()
	defer t.mu.Unlock()

	var out []kafka.Message
	for _, state := range t.partitions {
		if state.commitable != nil {
			out = append(out, *state.commitable)
			state.commitable = nil
		}
	}

	// Deterministic order keeps commit batches reproducible in tests and logs.
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// inFlight reports how many tracked messages have not completed.
func (t *offsetTracker) inFlight() int {
	t.mu.Lock()
	defer t.mu.Unlock()

	total := 0
	for _, state := range t.partitions {
		total += state.inFlight
	}
	return total
}

// forget drops all state for a partition, used when an assignment is revoked.
func (t *offsetTracker) forget(topic string, partition int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.partitions, partitionKey{topic: topic, partition: partition})
}
