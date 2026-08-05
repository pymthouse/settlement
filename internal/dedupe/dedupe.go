// Package dedupe provides the idempotency store the worker consults before it
// does anything.
//
// A message arriving on Kafka is not proof it is new. Stripe redelivers events
// it believes were not acknowledged, the doorman can publish the same event
// twice if a broker ack is lost, and a replay deliberately re-reads history.
// Any of those, applied twice, means charging a customer twice — so the first
// action on every event is to claim its id.
package dedupe

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/pymthouse/settlement/internal/config"
)

// Store claims event ids exactly once.
type Store interface {
	// Claim records the key and reports whether this caller won it. A false
	// return means the event was already processed and must be skipped.
	Claim(ctx context.Context, key string) (bool, error)
	// Release drops a claim so the event can be retried. It is called when
	// processing fails, otherwise a transient error would permanently suppress
	// the redelivery that would have fixed it.
	Release(ctx context.Context, key string) error
	// Close releases resources.
	Close() error
}

// New builds the configured store: Redis when a URL is set, otherwise an
// in-process store.
func New(cfg config.Dedupe) (Store, error) {
	ttl := cfg.TTL
	if ttl <= 0 {
		ttl = time.Hour // same clamp as NewMemory
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 3 * time.Second // same default as LoadWorker
	}

	if cfg.RedisURL == "" {
		return NewMemory(ttl), nil
	}
	opts, err := redis.ParseURL(cfg.RedisURL)
	if err != nil {
		return nil, fmt.Errorf("parse SETTLEMENT_REDIS_URL: %w", err)
	}
	return &redisStore{
		client:  redis.NewClient(opts),
		ttl:     ttl,
		prefix:  cfg.KeyPrefix,
		timeout: timeout,
	}, nil
}

type redisStore struct {
	client  *redis.Client
	ttl     time.Duration
	prefix  string
	timeout time.Duration
}

// Claim uses SET NX so the claim is atomic across worker replicas.
func (s *redisStore) Claim(ctx context.Context, key string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	ok, err := s.client.SetNX(ctx, s.prefix+key, time.Now().UTC().Format(time.RFC3339), s.ttl).Result()
	if err != nil {
		// Fail closed. Treating a Redis outage as "not a duplicate" would let
		// a redelivery storm double-charge; the message stays uncommitted and
		// is retried once Redis is back.
		return false, fmt.Errorf("dedupe claim: %w", err)
	}
	return ok, nil
}

func (s *redisStore) Release(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	if err := s.client.Del(ctx, s.prefix+key).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return fmt.Errorf("dedupe release: %w", err)
	}
	return nil
}

func (s *redisStore) Close() error { return s.client.Close() }

// Memory is an in-process store.
//
// It is correct only for a single replica: two workers each keep their own
// map and would both win the same claim. Production deployments set
// SETTLEMENT_REDIS_URL; this exists for local development and tests.
type Memory struct {
	mu      sync.Mutex
	entries map[string]time.Time
	ttl     time.Duration
	now     func() time.Time
}

// NewMemory builds an in-process store with the given retention.
func NewMemory(ttl time.Duration) *Memory {
	if ttl <= 0 {
		ttl = time.Hour
	}
	return &Memory{entries: make(map[string]time.Time), ttl: ttl, now: time.Now}
}

// Claim records key when it is absent or expired.
func (m *Memory) Claim(_ context.Context, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := m.now()
	m.evictLocked(now)

	if expiry, seen := m.entries[key]; seen && expiry.After(now) {
		return false, nil
	}
	m.entries[key] = now.Add(m.ttl)
	return true, nil
}

// Release removes a claim.
func (m *Memory) Release(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.entries, key)
	return nil
}

// Close is a no-op.
func (m *Memory) Close() error { return nil }

// Len reports the number of live claims. Used by tests.
func (m *Memory) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.evictLocked(m.now())
	return len(m.entries)
}

// evictLocked drops expired entries so a long-running worker's memory tracks
// the retention window rather than total event volume.
func (m *Memory) evictLocked(now time.Time) {
	for key, expiry := range m.entries {
		if !expiry.After(now) {
			delete(m.entries, key)
		}
	}
}
