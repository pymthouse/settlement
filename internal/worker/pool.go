package worker

import (
	"context"
	"hash/fnv"
	"sync"
)

// lanePool executes jobs concurrently while preserving order within a key.
//
// A plain worker pool would let a customer's issuing.sync overtake their
// draft.sync, and the second call would fail against an invoice that had not
// advanced yet. Hashing the partition key to a fixed lane means all work for
// one payer runs serially in arrival order, while unrelated payers proceed in
// parallel. Lanes are shared across topics on purpose: a Stripe payment event
// and an OpenMeter invoice event for the same customer must not run at once.
type lanePool struct {
	lanes []chan job
	wg    sync.WaitGroup
}

type job struct {
	key string
	run func(context.Context)
}

// newLanePool starts count lanes, each buffering up to buffer jobs.
func newLanePool(ctx context.Context, count, buffer int) *lanePool {
	if count < 1 {
		count = 1
	}
	if buffer < 1 {
		buffer = 1
	}

	p := &lanePool{lanes: make([]chan job, count)}
	for i := range p.lanes {
		lane := make(chan job, buffer)
		p.lanes[i] = lane
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for j := range lane {
				j.run(ctx)
			}
		}()
	}
	return p
}

// submit enqueues a job on the lane owning its key.
//
// It blocks when the lane is full, which is the backpressure that stops the
// consumer from reading faster than the handlers can settle invoices.
func (p *lanePool) submit(ctx context.Context, j job) error {
	lane := p.lanes[laneIndex(j.key, len(p.lanes))]
	select {
	case lane <- j:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// close stops accepting work and waits for every lane to drain.
func (p *lanePool) close() {
	for _, lane := range p.lanes {
		close(lane)
	}
	p.wg.Wait()
}

// laneIndex maps a key to a lane. An empty key lands on lane 0 rather than
// being spread around, so unkeyed messages stay serialized with each other.
func laneIndex(key string, lanes int) int {
	if lanes <= 1 || key == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(lanes))
}
