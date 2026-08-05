# Architecture

How `settlement` is put together, and why each boundary sits where it does.

The [README](../README.md) gives the shape; this document gives the mechanics —
what happens to a single event from the moment it arrives to the moment its
offset is committed, and what happens when each step fails.

---

## Table of contents

- [The problem](#the-problem)
- [Component boundaries](#component-boundaries)
- [The Kafka contract](#the-kafka-contract)
- [The life of an event](#the-life-of-an-event)
- [Ordering and concurrency](#ordering-and-concurrency)
- [Offset commits](#offset-commits)
- [Idempotency](#idempotency)
- [Failure taxonomy](#failure-taxonomy)
- [Reconciliation](#reconciliation)
- [Money handling](#money-handling)
- [Security boundaries](#security-boundaries)
- [Scaling](#scaling)
- [Deployment topology](#deployment-topology)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Implementation tasks](#implementation-tasks)

---

## The problem

Two systems hold half the truth each.

**OpenMeter** knows what a customer owes. It meters usage, applies plans and
discounts, and runs an invoice through a state machine. With the Custom
Invoicing app it stops at three states and waits for an integration to do the
external work.

**Stripe** knows whether the customer paid. It holds the payment methods, the
connected accounts and the money.

They never speak. OpenMeter does not consume Stripe webhooks, and Stripe knows
nothing about OpenMeter invoices. Without an integration an invoice reaches
`payment_processing` and stays there forever, no matter how much money changed
hands.

The integration must therefore do three things reliably: **receive** both event
streams without losing any, **join** them, and **never act twice** on the same
event. Everything below follows from those three.

---

## Component boundaries

### producer — the security and latency boundary

```
   HTTPS ──▶ read raw body ──▶ verify signature ──▶ publish keyed ──▶ 200
                (capped)         (over raw bytes)     (acks=all)
```

The producer exists so that everything downstream can assume one thing: **the
bytes on the billing topic were signed by someone holding the secret.**

It holds no database handle, makes no OpenMeter call, and contains no invoice
logic. That is not minimalism for its own sake — it is what keeps the handler
fast enough that Stripe never times out, and small enough that its correctness
can be read in one sitting.

The one piece of parsing it does is extracting the routing descriptor
(`internal/events`): the event id, type, and the partition key. That decision
has to be made before the write to Kafka, and getting the key wrong silently
breaks per-customer ordering. It reads the body; it never rewrites it.

**Failure behaviour:** a publish failure returns `500`. Answering `200` for an
event we did not persist would lose it permanently, because Stripe treats any
`2xx` as delivered and never redelivers.

### Kafka — the durable, replayable record

Three topics, keyed and long-retained:

| Topic | Contents |
|---|---|
| `billing.openmeter.invoices.v1` | verified OpenMeter notification bodies |
| `billing.stripe.events.v1` | verified Stripe webhook bodies |
| `billing.settlement.dlq.v1` | messages that could not be settled |

They belong on separate topics from OpenMeter's usage-ingest topics, and ideally
on a separate cluster. The two workloads have opposite characteristics:

| | Usage ingest | Billing |
|---|---|---|
| Volume | very high | low |
| Loss tolerance | some | none |
| Ordering | not required | per customer |
| Retention | weeks | years (audit) |
| Consequence of delay | a metric is late | an invoice is late |

Sharing a cluster couples them in both directions: a usage spike delays an
invoice, and a billing incident stalls metering.

### worker — the correctness boundary

Long-lived, so OpenMeter and Stripe connections stay warm and an invoice burst
meets no cold starts. Internally split in two:

- `internal/worker` — plumbing. Consume, dedupe, order, retry, park, commit.
  Knows nothing about invoices.
- `internal/lifecycle` — domain. Knows what a `draft.sync` means and what to do
  about it. Knows nothing about Kafka.

The seam is a two-method `Settler` interface. It exists so the retry, dedupe and
dead-letter policies can be tested exhaustively without a Stripe on the other
end — and those policies are exactly the code that is hardest to exercise in
production and worst to get wrong.

---

## The Kafka contract

**The message value is the raw, unmodified webhook body.** It is the audit
record. Re-serialising it would destroy the ability to re-verify a signature
months later, and JSON round-tripping is not byte-stable anyway.

Routing metadata rides in headers, so the worker never has to re-parse the body
to decide what to do with it:

| Header | Purpose |
|---|---|
| `settlement-source` | `stripe` or `openmeter` — selects the handler |
| `settlement-event-id` | the deduplication key |
| `settlement-event-type` | provider event type, for logs and metrics |
| `settlement-key` | the partition key, echoed for the lane hash |
| `settlement-account` | Stripe Connect account, when present |
| `settlement-livemode` | `false` for Stripe test-mode events |
| `settlement-received-at` | RFC 3339, when the doorman accepted it |
| `settlement-producer` | which component wrote the record |

Dead-lettered messages gain `settlement-dlq-*` headers recording the reason,
the error text, and the source topic/partition/offset so a redrive can route
them back. Replayed messages gain `settlement-replay-of`, carrying a batch id.

### Partition keys

The key decides ordering, so it must be stable for one payer across every event
about them.

- **Stripe:** the Connect account, falling back to the customer, then the
  object, then the event id.
- **OpenMeter:** the customer id, falling back to the customer key, then the
  invoice id, then the event id.

Every fallback still produces a stable key for the same logical payer.

---

## The life of an event

```
FetchMessage
    │
    ├─▶ tracker.track(msg)              register the offset as in flight
    │
    ├─▶ pool.submit(hash(key))          queue on the lane owning this payer
    │                                   (blocks when full — backpressure)
    ▼
lane goroutine
    │
    ├─▶ claim(source:event-id)          fails closed if the store is down
    │       └─ already claimed? ──▶ drop, complete, done
    │
    ├─▶ handler                         OpenMeter notification or Stripe event
    │       ├─ ok ──────────────▶ keep the claim, complete, done
    │       ├─ permanent ───────▶ release claim, park, complete
    │       └─ transient ───────▶ backoff, retry, up to MaxAttempts
    │                                then release claim, park, complete
    ▼
tracker.complete(msg)                   advance the contiguous watermark

commit loop (every CommitInterval)
    └─▶ commit the highest contiguous completed offset per partition
```

The OpenMeter handler re-fetches the invoice rather than trusting the copy
embedded in the notification. Notifications can arrive out of order relative to
the invoice's real state, and acting on a stale draft is how a duplicate Stripe
invoice gets created.

---

## Ordering and concurrency

Kafka guarantees order **within a partition**. That guarantee is worthless if
the consumer then hands messages to a plain worker pool: a customer's
`issuing.sync` would race their `draft.sync`, and the issuing call would fail
against an invoice that had not advanced.

So the worker uses **ordered lanes**. A fixed set of goroutines, each with its
own queue; a message goes to `hash(partition key) % lanes`. Within a lane, work
is strictly serial in arrival order. Across lanes, it is fully parallel.

Two properties fall out of this:

- **Lanes are shared across topics.** The same customer's Stripe payment event
  and OpenMeter invoice event land on the same lane, so they can never run
  concurrently against the same invoice.
- **Submission blocks when a lane is full.** That backpressure is what stops
  the consumer from reading faster than invoices can actually be settled.

The trade-off: one slow customer backs up its lane, including unrelated
customers that hash to it. Raising `SETTLEMENT_LANES` reduces collisions.

---

## Offset commits

Because lanes finish out of order, committing the offset of whichever message
finished last would silently skip anything still in flight behind it — an
invoice event lost at the next restart.

The tracker therefore commits only the **highest contiguous run** of completed
offsets per partition:

```
offsets:   0    1    2    3    4
state:     ▓    ✓    ✓    ✓    ·        ▓ in flight  ✓ done  · not started
commit:    nothing — 0 is still working

offsets:   0    1    2    3    4
state:     ✓    ✓    ✓    ✓    ▓
commit:    offset 3  (kafka-go commits 3+1)
```

Everything below the watermark is genuinely done. Anything above it is
re-delivered after a crash and caught by the dedupe store. The cost is a little
bookkeeping; the benefit is a restart that neither loses nor duplicates work.

On shutdown: stop fetching, drain the lanes, commit, then close the readers —
in that order. Committing before the drain would claim offsets whose invoices
were never settled.

---

## Idempotency

Three defences, at three different layers.

**1. The claim store.** The first action on every message is claiming
`source:event-id` (Redis `SET NX`, 30-day TTL by default). A message on Kafka is
not proof of uniqueness: Stripe redelivers events it believes were not
acknowledged, the doorman can double-publish on a lost broker ack, and replays
re-read history deliberately.

The store **fails closed**. If Redis is unreachable the worker stops rather than
processing blind, because processing blind risks charging a customer twice. It
trades availability for correctness — the right trade for money.

A failed event **releases** its claim, or a transient error would permanently
suppress the redelivery that would have fixed it.

**2. Stripe idempotency keys.** Every create carries a deterministic key
(`settlement-invoice-{openmeter invoice id}`, `settlement-item-{invoice}-{line}-{amount}`).
Stripe remembers keys for 24 hours, so a retry inside that window is a no-op
rather than a second invoice. The item key includes the amount so a *corrected*
line is a genuinely new request, not a silent replay of the old figure.

**3. Lookup before create.** Beyond the 24-hour window, settlement recovers by
reading: the external id OpenMeter recorded, then a Stripe search on our own
metadata. Search lags writes by up to a minute, so keys and lookups cover each
other's gaps.

Replays are the deliberate exception. `settlementctl replay` stamps a batch id
that is folded into the claim key, so the event is processed again — which is
the entire point of replaying after a mapping bug.

---

## Failure taxonomy

The only distinction that matters is whether retrying could ever work.

| Class | Examples | Treatment |
|---|---|---|
| **Transient** | 5xx, timeouts, connection resets, `429`, Stripe `lock_timeout`, OpenMeter `409` on read paths | Exponential backoff with jitter, up to `MaxAttempts` |
| **Permanent** | validation errors, missing Connect account, unparseable body, total mismatch, unknown source | Straight to the DLQ; retrying would occupy the lane and change nothing |

`internal/faults` carries the classification, and it survives wrapping — a
handler adding context on the way up must not turn a permanent fault back into
a retryable one.

Backoff is exponential from `RetryBaseDelay`, capped at `RetryMaxDelay`, with
jitter applied **downward only** so the cap remains a real bound and a recovered
outage does not produce a synchronised thundering herd.

### When retries run out

| Policy | Behaviour | Cost |
|---|---|---|
| `dlq` (default) | Park the message with full provenance, commit the offset, keep the partition flowing | Requires alerting on `settlement_dead_lettered_total` |
| `halt` | Cancel the run without committing | The partition stops; nothing is lost, nothing proceeds |

Two situations halt regardless of policy, because they mean the event would
otherwise be **lost** rather than merely delayed:

- the dedupe store is unavailable, and
- the DLQ publish itself fails.

---

## Reconciliation

Every event-driven pipeline eventually drops something — a notification channel
misconfigured for an afternoon, an endpoint returning 500s past its retry
budget, a topic recreated. Without a sweep those invoices sit at a pause point
indefinitely, and nobody finds out until a customer asks why they were never
billed.

The sweep asks OpenMeter directly for invoices in `draft`, `issuing` or
`payment_processing`, skips anything updated within `ReconcileMinAge` (so it
cannot race a live notification), and pushes the rest through
`DriveInvoice` — **the same entry point a live event uses**. That shared path is
what makes the sweep trustworthy: a re-driven invoice takes exactly the route it
would have taken anyway.

A sustained `settlement_reconcile_redriven_total` is a signal in its own right:
the live pipeline is dropping events and needs investigation.

---

## Money handling

`internal/money` exists because `float64` cannot represent `0.10`, and a few
thousand such lines is how an invoice ends up a cent out.

- Amounts arrive from OpenMeter as decimal strings and are parsed into
  `math/big.Rat` — exact, no accumulated error.
- Conversion to minor units happens **once per amount**, at the Stripe
  boundary, rounding half away from zero. That is the rounding a customer
  expects to see, and it makes a credit note the exact mirror of the invoice it
  reverses.
- Currency exponents follow Stripe's zero-decimal (JPY, KRW, …) and
  three-decimal (KWD, BHD, …) lists; three-decimal amounts are floored to a
  multiple of ten, which Stripe requires.
- Application fees truncate downward. Rounding a fee up would take a cent the
  platform did not earn, and the connected account is the one who notices.
- The invoice total is asserted, not assumed. Per-line rounding residue becomes
  an explicit adjustment item; a gap larger than one minor unit per line is a
  permanent `total_mismatch` fault.

---

## Security boundaries

**Signature verification happens once, at the edge, over raw bytes.** Nothing
downstream re-verifies, because nothing downstream has the original bytes in a
verifiable form — which is precisely why the value on Kafka is never rewritten.

- **Stripe:** HMAC-SHA256 over `"{timestamp}.{raw body}"`, hex-encoded in `v1=`
  fields of `Stripe-Signature`, with a tolerance window bounding replay.
- **OpenMeter:** [Standard Webhooks](https://www.standardwebhooks.com/) —
  HMAC-SHA256 over `"{id}.{timestamp}.{raw body}"`, base64, in
  `webhook-signature` (or `svix-signature`). The message id is part of the
  signed content, so a valid body cannot be replayed under a different id.

Both accept a **list** of secrets, so rotation is: add the new secret, roll the
endpoint, drop the old — with no delivery gap.

Other properties:

- Comparisons are constant-time (`hmac.Equal`).
- Bodies are capped (`SETTLEMENT_MAX_BODY_BYTES`) before verification, so a
  huge body cannot stall the process before its signature is even checked.
- Rejections return a short, non-revealing message; the reason goes to the log
  and the metric, not to the caller.
- Logs carry identifiers, never raw webhook bodies.
- The container runs as a non-root user with no shell entrypoint.

---

## Scaling

**Producer:** stateless. Scale horizontally behind a load balancer. The only
shared state is the Kafka connection pool.

**Worker:** scale by adding replicas within the consumer group, up to the
partition count. Beyond that, add partitions — but note that re-partitioning
re-shards the keyspace and can break ordering for in-flight invoices, which is
why `settlementctl topics ensure` deliberately never widens an existing topic.
Provision enough partitions up front (12 is a reasonable default).

**More than one replica requires Redis.** Without `SETTLEMENT_REDIS_URL` each
replica keeps its own in-process claim map, and two replicas would both win the
same claim.

**Vertical knobs:** `SETTLEMENT_LANES` (concurrency within a replica) and
`SETTLEMENT_LANE_BUFFER` (how much a lane will queue before applying
backpressure).

---

## Deployment topology

```
                    ┌───────────────┐
   Stripe ─────────▶│               │
                    │   producer    │  N replicas, stateless
   OpenMeter ──────▶│   (:8080)     │  behind a load balancer
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐
                    │  Kafka        │  billing cluster
                    │  billing.*    │  12 partitions, RF 3, min ISR 2
                    └───────┬───────┘
                            │
                    ┌───────▼───────┐        ┌──────────┐
                    │   worker      │───────▶│  Redis   │  claim store
                    │   (:8081)     │        └──────────┘  noeviction
                    └───┬───────┬───┘
                        │       │
                 OpenMeter     Stripe
```

Redis must run with `maxmemory-policy noeviction`. Silently evicting a dedupe
claim under memory pressure would let a redelivered event charge a customer
twice — the exact failure the store exists to prevent.

---

## Design decisions and trade-offs

| Decision | Rationale | Cost |
|---|---|---|
| Thin verified producer in front of Kafka | REST proxies and HTTP source connectors cannot verify a Stripe signature | One more service to deploy |
| Raw body as the Kafka value | Signatures stay re-verifiable; the log is the audit record | Routing metadata must live in headers |
| Separate billing topics/cluster | Financial events must not queue behind usage spikes | A second cluster to operate |
| Ordered lanes | Preserves per-customer ordering under concurrency | A slow customer backs up its lane |
| Contiguous-offset commits | A restart neither loses nor duplicates work | More bookkeeping than committing the newest completion |
| Fail-closed dedupe | A double charge is worse than an outage | Redis becomes a hard dependency |
| Permanent/transient split | Keeps a doomed event from occupying a lane for hours | Every error site must classify correctly |
| DLQ by default | Keeps the partition flowing | Requires someone watching the DLQ |
| No settlement database | The join lives in Stripe metadata and OpenMeter external ids | A Stripe lookup where a local table would have answered |
| Hand-rolled Stripe client | Precise control over `Stripe-Account` and `Idempotency-Key`, auditable in one file | Request shapes are ours to maintain |
| `big.Rat` throughout | Invoices must be exact | Slower than float, irrelevant at this volume |
| Reconciliation reuses the live path | A re-driven invoice behaves identically to a live one | The sweep costs OpenMeter list calls |

---

## Implementation tasks

- [ ] Provision the production billing cluster: 12 partitions, RF 3, min ISR 2,
      retention set for audit
- [ ] Deploy Redis with `noeviction` and persistence enabled
- [ ] Decide where the producer runs (Vercel function vs this container) and
      wire the Stripe and OpenMeter endpoints to it
- [ ] Set `SETTLEMENT_ON_RETRY_EXHAUSTED` per environment — consider `halt` for
      the first production billing run, `dlq` once the DLQ alert is live
- [ ] Add the alert rules from the [README](../README.md#observability)
- [ ] Load-test an invoice burst; confirm per-customer ordering holds and the
      commit watermark keeps up
- [ ] Document the on-call escalation path for a non-empty DLQ in
      [operations.md](operations.md)
