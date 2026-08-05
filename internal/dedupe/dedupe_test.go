package dedupe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pymthouse/settlement/internal/config"
)

func TestMemoryClaimsOnlyOnce(t *testing.T) {
	store := NewMemory(time.Hour)
	ctx := context.Background()

	won, err := store.Claim(ctx, "stripe:evt_1")
	if err != nil || !won {
		t.Fatalf("first claim: won=%v err=%v", won, err)
	}

	won, err = store.Claim(ctx, "stripe:evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if won {
		t.Fatal("the same event id was claimed twice; a duplicate would be processed")
	}
}

func TestMemoryClaimsAreIndependentPerKey(t *testing.T) {
	store := NewMemory(time.Hour)
	ctx := context.Background()

	// The same provider id from two sources must not collide.
	for _, key := range []string{"stripe:evt_1", "openmeter:evt_1"} {
		won, err := store.Claim(ctx, key)
		if err != nil || !won {
			t.Fatalf("claim %s: won=%v err=%v", key, won, err)
		}
	}
}

// Releasing on failure is what lets a redelivery retry a transient error.
func TestMemoryReleaseAllowsReprocessing(t *testing.T) {
	store := NewMemory(time.Hour)
	ctx := context.Background()

	if _, err := store.Claim(ctx, "stripe:evt_1"); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, "stripe:evt_1"); err != nil {
		t.Fatal(err)
	}

	won, err := store.Claim(ctx, "stripe:evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if !won {
		t.Fatal("a released event could not be reclaimed; it would be skipped forever")
	}
}

func TestMemoryExpiresAndEvictsClaims(t *testing.T) {
	store := NewMemory(time.Minute)
	now := time.Date(2026, 8, 4, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	ctx := context.Background()

	if _, err := store.Claim(ctx, "stripe:evt_1"); err != nil {
		t.Fatal(err)
	}
	if got := store.Len(); got != 1 {
		t.Fatalf("Len = %d, want 1", got)
	}

	now = now.Add(2 * time.Minute)

	won, err := store.Claim(ctx, "stripe:evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if !won {
		t.Fatal("an expired claim still suppressed the event")
	}

	// Expired entries must be evicted, not retained forever.
	now = now.Add(2 * time.Minute)
	if _, err := store.Claim(ctx, "stripe:evt_2"); err != nil {
		t.Fatal(err)
	}
	if got := store.Len(); got != 1 {
		t.Fatalf("Len = %d after eviction, want 1", got)
	}
}

// Concurrent lanes may race on the same id; exactly one must win.
func TestMemoryClaimIsRaceFree(t *testing.T) {
	store := NewMemory(time.Hour)
	ctx := context.Background()

	const racers = 50
	var wg sync.WaitGroup
	wins := make(chan bool, racers)

	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			won, err := store.Claim(ctx, "stripe:evt_hot")
			if err != nil {
				t.Error(err)
				return
			}
			wins <- won
		}()
	}
	wg.Wait()
	close(wins)

	total := 0
	for won := range wins {
		if won {
			total++
		}
	}
	if total != 1 {
		t.Fatalf("%d goroutines claimed the same event, want exactly 1", total)
	}
}

func TestNewFallsBackToMemoryWithoutRedis(t *testing.T) {
	store, err := New(config.Dedupe{TTL: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if _, ok := store.(*Memory); !ok {
		t.Fatalf("store type = %T, want the in-process Memory store", store)
	}
}

func TestNewRejectsAMalformedRedisURL(t *testing.T) {
	if _, err := New(config.Dedupe{RedisURL: "not-a-url", TTL: time.Hour}); err == nil {
		t.Fatal("a malformed Redis URL was accepted; the worker would start without a shared claim store")
	}
}
