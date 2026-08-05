package worker

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// The pool's reason for existing: work for one key must run in submission
// order, even while other keys run concurrently.
func TestLanePoolPreservesOrderWithinAKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newLanePool(ctx, 8, 4)

	var mu sync.Mutex
	seen := map[string][]int{}

	const keys, perKey = 5, 20
	for k := 0; k < keys; k++ {
		key := fmt.Sprintf("cus_%d", k)
		for i := 0; i < perKey; i++ {
			index := i
			if err := pool.submit(ctx, job{key: key, run: func(context.Context) {
				// Vary the work so an unordered pool would interleave visibly.
				if index%3 == 0 {
					time.Sleep(time.Millisecond)
				}
				mu.Lock()
				seen[key] = append(seen[key], index)
				mu.Unlock()
			}}); err != nil {
				t.Fatalf("submit: %v", err)
			}
		}
	}
	pool.close()

	for key, order := range seen {
		if len(order) != perKey {
			t.Fatalf("key %s ran %d jobs, want %d", key, len(order), perKey)
		}
		for i, got := range order {
			if got != i {
				t.Fatalf("key %s ran out of order at position %d: %v", key, i, order)
			}
		}
	}
}

func TestLanePoolRunsDistinctKeysConcurrently(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := newLanePool(ctx, 4, 4)

	// Two jobs on lanes that must differ; if they were serialized the second
	// would never start before the first returns.
	started := make(chan string, 2)
	release := make(chan struct{})

	keyA, keyB := distinctLaneKeys(t, 4)
	for _, key := range []string{keyA, keyB} {
		k := key
		if err := pool.submit(ctx, job{key: k, run: func(context.Context) {
			started <- k
			<-release
		}}); err != nil {
			t.Fatalf("submit: %v", err)
		}
	}

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("second key did not start; lanes are not running concurrently")
		}
	}
	close(release)
	pool.close()
}

func TestLanePoolSubmitReturnsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pool := newLanePool(ctx, 1, 1)

	block := make(chan struct{})
	// Occupy the single lane and fill its buffer.
	_ = pool.submit(ctx, job{key: "k", run: func(context.Context) { <-block }})
	_ = pool.submit(ctx, job{key: "k", run: func(context.Context) {}})

	cancel()
	if err := pool.submit(ctx, job{key: "k", run: func(context.Context) {}}); err == nil {
		t.Fatal("submit should fail once the context is cancelled")
	}

	close(block)
	pool.close()
}

func TestLaneIndexIsStableAndBounded(t *testing.T) {
	const lanes = 16
	for _, key := range []string{"cus_1", "acct_42", "", "a very long partition key value"} {
		first := laneIndex(key, lanes)
		if first < 0 || first >= lanes {
			t.Fatalf("laneIndex(%q) = %d, out of range", key, first)
		}
		if second := laneIndex(key, lanes); second != first {
			t.Fatalf("laneIndex(%q) is not stable: %d then %d", key, first, second)
		}
	}
	if got := laneIndex("", 16); got != 0 {
		t.Errorf("empty keys should share lane 0, got %d", got)
	}
	if got := laneIndex("anything", 1); got != 0 {
		t.Errorf("single-lane pool must always return 0, got %d", got)
	}
}

// distinctLaneKeys finds two keys that hash to different lanes.
func distinctLaneKeys(t *testing.T, lanes int) (string, string) {
	t.Helper()
	base := laneIndex("cus_0", lanes)
	for i := 1; i < 1000; i++ {
		candidate := fmt.Sprintf("cus_%d", i)
		if laneIndex(candidate, lanes) != base {
			return "cus_0", candidate
		}
	}
	t.Fatal("could not find two keys on different lanes")
	return "", ""
}
