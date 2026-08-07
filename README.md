# settlement

Custom Invoice Settlement — a Go worker and Kafka pipeline that reconciles
OpenMeter invoices against Stripe.

OpenMeter meters usage and owns the invoice lifecycle. Stripe moves the money.
Neither talks to the other. `settlement` is the integration that closes the
loop: it verifies inbound webhooks, durably records them, and drives every
OpenMeter invoice through the three points where the Custom Invoicing app stops
and waits for an external system to answer.

---

## Table of contents

- [Why this service exists](#why-this-service-exists)
- [Architecture](#architecture)
- [The invoice lifecycle](#the-invoice-lifecycle)
- [Charge models](#charge-models)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Operating the service](#operating-the-service)
- [Observability](#observability)
- [Design decisions and trade-offs](#design-decisions-and-trade-offs)
- [Testing](#testing)
- [Implementation tasks](#implementation-tasks)
- [Further reading](#further-reading)

---

## Why this service exists

OpenMeter's built-in Stripe app cannot express Stripe Connect: it assumes one
merchant, one set of books. Switching OpenMeter to the **Custom Invoicing app**
removes that assumption — but the Custom Invoicing app is, by design, inert. It
pauses the invoice at three states and waits for an integration to do the
external work and report back.

Critically, **OpenMeter never sees a Stripe webhook**. An invoice sits in
`payment_processing` indefinitely unless something tells OpenMeter the money
arrived. Joining those two event streams is this service's whole job:

| Stream | Source | Tells us |
|---|---|---|
| A | OpenMeter notifications | an invoice reached a state and is waiting |
| B | Stripe webhooks | money moved (or failed to) |

Neither stream is sufficient alone. The worker joins them.

---

## Architecture

Ingestion is separated from processing by a durable log.

```
   Stripe                OpenMeter
  webhooks              notifications
      │                       │
      │  HTTPS POST           │  HTTPS POST
      ▼                       ▼
┌─────────────────────────────────────────┐
│  producer — the doorman                 │   no DB, no business logic
│  1. read the RAW body                   │   no OpenMeter lookups
│  2. verify the signature over it        │   ~1ms, no cold starts
│  3. publish it unmodified, keyed        │
│  4. 200                                 │
└─────────────────────────────────────────┘
      │ keyed by Connect account / customer id
      ▼
┌─────────────────────────────────────────┐
│  Kafka — the billing lane               │   separate topics, ideally a
│  billing.openmeter.invoices.v1          │   separate cluster from
│  billing.stripe.events.v1               │   usage ingest
│  billing.settlement.dlq.v1              │   years of retention, replayable
└─────────────────────────────────────────┘
      │
      ▼
┌─────────────────────────────────────────┐
│  worker — owns correctness              │   long-lived, warm connections
│  1. dedupe on the event id              │   ordered lanes per customer
│  2. route by state / event type         │   retry with backoff
│  3. drive the lifecycle                 │   DLQ or halt on exhaustion
│  4. commit only contiguous offsets      │   periodic reconciliation sweep
└─────────────────────────────────────────┘
      │                       │
      ▼                       ▼
 OpenMeter Custom        Stripe Connect
 Invoicing API           (invoices, fees)
```

### The producer is deliberately hollow

Four responsibilities, and nothing else:

1. **Read the raw body.** Once, into a byte slice, under a size cap.
2. **Verify the signature against those exact bytes.** Before any parsing.
   JSON round-tripping reorders keys, rewrites numbers, and drops whitespace —
   any one of which turns a valid signature into a rejected one. Failures get
   `400`.
3. **Publish the unmodified payload,** keyed by a stable value so per-customer
   ordering holds. The Kafka value *is* the audit record; routing metadata
   rides in Kafka headers instead.
4. **Answer `200` as fast as possible.**

The doorman blocks on the Kafka acknowledgement before answering. A `200` for
an event we failed to persist would lose it permanently — Stripe treats `2xx`
as delivered and never retries. A `500` gets a redelivery.

> Kafka cannot receive HTTP directly. Confluent REST Proxy and Kafka Connect
> HTTP source connectors exist, but neither verifies a Stripe signature, which
> would put unverified junk on a financial topic. Hence the thin verified
> producer in front.

### The worker owns correctness

Long-lived, so connections to OpenMeter and Stripe stay warm and there are no
cold starts during an invoice burst. Work runs in **ordered lanes**: messages
hash by partition key onto a fixed set of serial lanes, so one customer's
`draft.sync` can never overtake their `issuing.sync` while unrelated customers
proceed in parallel. Lanes are shared across both topics, so a Stripe payment
event and an OpenMeter invoice event for the same customer never run at once.

Offsets are committed **only for the highest contiguous completed run** per
partition. Committing whichever message finished last would silently skip
anything still in flight behind it.

---

## The invoice lifecycle

```
gathering ──▶ draft.created ──▶ [ draft.sync ] ──▶ issuing ──▶ [ issuing.sync ]
                                      ▲                              ▲
                                      │ PAUSE                        │ PAUSE
                                 draft synchronized           invoicing synchronized

  ──▶ issued ──▶ [ payment_processing.pending ] ──▶ paid
                              ▲
                              │ PAUSE
                        payment status (trigger)

  overdue / uncollectible / voided are alternate terminal branches
```

| Pause state | What the worker does | Completion call |
|---|---|---|
| `draft.sync` | Ensure the Stripe customer; create the Stripe **draft** invoice; add one item per OpenMeter line; reconcile totals | `POST /api/v1/apps/custom-invoicing/{id}/draft/synchronized` with the invoice external id, line mappings and discount mappings |
| `issuing.sync` | Set the application fee against the final total; **finalize** the Stripe invoice | `POST .../issuing/synchronized` with `invoiceNumber`, `sentToCustomerAt` and `payment.externalId` |
| `payment_processing.pending` | Wait for the Stripe webhook; if it never came, read Stripe directly | `POST .../payment/status` with a `trigger` |

Two details of the real API differ from how the lifecycle is often described,
and both are load-bearing — see
[docs/openmeter-custom-invoicing.md](docs/openmeter-custom-invoicing.md):

- **Line mappings can only be supplied at draft sync.** The issuing call
  accepts no line ids. Everything the later stages need to reference in Stripe
  must therefore exist by the end of draft sync, which is why the Stripe draft
  invoice and its items are created there rather than at issuing.
- **The payment call carries no external id** — only a trigger. The Stripe
  payment reference is stamped during the *issuing* call.

The worker branches on `statusDetails.extendedStatus` and reads
`statusDetails.availableActions` rather than hard-coding the state graph, as
OpenMeter's own documentation recommends. The recognised status strings are
configurable, because `extendedStatus` is a free-form string whose vocabulary
has shifted across releases.

---

## Charge models

All three are supported and selectable per customer, per invoice, or globally.

| Model | Where the invoice lives | Merchant of record | Platform revenue |
|---|---|---|---|
| `platform` | Platform account | Platform | The whole invoice |
| `direct` | Connected account (`Stripe-Account`) | The developer | `application_fee_amount` |
| `destination` | Platform account, `transfer_data[destination]` | Platform | `application_fee_amount` |

`platform` is the default: a developer owner with a payment method pays for
their own end users' usage, with no Connect onboarding at all. This is the
lowest-friction path and the one that maximises the integrator base.

`direct` and `destination` unlock marketplace monetization — developer app
owners are connected accounts, their end users are customers underneath them,
and the platform takes a cut via application fees.

Resolution order, highest first:

1. Invoice metadata (`stripe_charge_model`, `stripe_connect_account_id`)
2. OpenMeter customer metadata (same keys)
3. `SETTLEMENT_STRIPE_CHARGE_MODEL`

The invoice wins because OpenMeter freezes an invoice at creation: one raised
under the platform model must keep settling that way even if the developer
switches to Connect afterwards. A Connect model with no account configured is a
**hard failure**, never a silent fallback to the platform account.

---

## Repository layout

```
cmd/
  producer/          the doorman
  worker/            the settlement worker
  settlementctl/     operator CLI: topics, replay, DLQ redrive, inspect
  stripefake/        test-mode Stripe emulator and webhook pusher
  e2e/               Konnect + fake Stripe e2e driver
internal/
  config/            environment configuration and validation
  webhook/           raw-body signature verification (Stripe, Standard Webhooks)
  events/            Kafka message contract: headers, descriptors, keys
  kafkax/            Kafka clients, topic provisioning
  producer/          HTTP surface for the doorman
  worker/            consume loop, ordered lanes, offset tracker, retries, DLQ
  lifecycle/         the three sync handlers — the domain logic
  openmeter/         OpenMeter + Custom Invoicing client (pinned to beta.228)
  stripe/            focused Stripe REST client
  stripefake/        in-memory Stripe emulator used by tests and e2e
  money/             exact decimal → minor-unit conversion
  dedupe/            idempotency store (Redis or in-process)
  faults/            permanent vs retryable classification
  metrics/           Prometheus instruments
deploy/              Dockerfile and local + e2e compose stacks
docs/                architecture, OpenMeter contract, operations runbook, e2e runbook
```

---

## Getting started

### Prerequisites

- Go 1.25+
- Docker (for the local stack)
- A running OpenMeter with the Custom Invoicing app installed
- A Stripe account (test mode is fine)

### Run the local stack

```bash
cp .env.example .env      # fill in the Stripe key and webhook secret
make up                   # Redpanda, Redis, producer, worker
make topics               # provision the billing topics
make logs                 # follow the worker
```

Forward Stripe events to the doorman:

```bash
stripe listen --forward-to localhost:8080/webhooks/stripe
```

Point an OpenMeter notification channel at
`http://<host>:8080/webhooks/openmeter` and put its signing secret in
`SETTLEMENT_OPENMETER_WEBHOOK_SECRETS`.

### Build and test locally

```bash
make build     # bin/producer, bin/worker, bin/settlementctl, bin/stripefake, bin/e2e
make test      # unit and integration tests
make check     # what CI runs: vet, gofmt, go mod tidy, tests under -race
```

---

## Configuration

Every knob is an environment variable, validated at startup — a misconfigured
billing service refuses to boot rather than discovering the problem halfway
through an invoice. See [.env.example](.env.example) for the annotated set.

The variables worth understanding before a production deploy:

| Variable | Why it matters |
|---|---|
| `SETTLEMENT_STRIPE_WEBHOOK_SECRETS` | Comma-separated, so a secret rotates without a delivery gap |
| `SETTLEMENT_KAFKA_REQUIRED_ACKS` | `-1` (all) is the only durable setting for financial events |
| `SETTLEMENT_REDIS_URL` | Required for more than one worker replica; without it each replica keeps its own claim map |
| `SETTLEMENT_ON_RETRY_EXHAUSTED` | `dlq` keeps the partition flowing and needs alerting; `halt` stops without committing |
| `SETTLEMENT_STRIPE_CHARGE_MODEL` | `platform`, `direct` or `destination` |
| `SETTLEMENT_APPLICATION_FEE_BPS` | The platform's cut on Connect invoices; opt-in, defaults to zero |
| `SETTLEMENT_OPENMETER_*_STATUSES` | The `extendedStatus` values that mean "waiting on us" — verify against your deployment |

---

## Operating the service

`settlementctl` reads the same `SETTLEMENT_KAFKA_*` variables as the services,
so it can never operate on a different cluster than the one it is fixing.

```bash
# Provision the billing topics (existing topics are left untouched)
settlementctl topics ensure --partitions 12 --replication 3 --retention-days 3650

# Look at what is on a topic
settlementctl inspect -topic billing.stripe.events.v1 -count 20 -full

# Replay history after shipping a mapping fix
settlementctl replay -topic billing.openmeter.invoices.v1 -partition 0 \
  -since 2026-08-01T00:00:00Z -dry-run

# Redrive everything parked for one reason
settlementctl dlq redrive -reason total_mismatch
```

Replayed messages carry a batch id that deliberately bypasses deduplication —
that is the point of a replay: the events *were* handled, incorrectly, and must
be handled once more with the fix in place.

Full procedures, including the reconciliation sweep and what each DLQ reason
means, are in [docs/operations.md](docs/operations.md).

---

## Observability

Prometheus metrics on `/metrics` (`:8080` producer, `:8081` worker), plus
`/healthz` and `/readyz`. Structured JSON logs carry identifiers, never raw
webhook bodies.

The series to alert on:

| Metric | Alert when |
|---|---|
| `settlement_dead_lettered_total` | Any increase — an event needs a human |
| `settlement_invoices_needing_attention_total` | Any increase — failed, overdue or uncollectible |
| `settlement_webhooks_received_total{outcome="bad_signature"}` | Sustained — misconfigured secret or an attack |
| `settlement_events_processed_total{outcome="failed"}` | Rate above baseline |
| `settlement_reconcile_redriven_total` | Sustained — the live pipeline is dropping events |
| `settlement_committed_offset` | Flat while the topic advances — the worker is stuck |

---

## Design decisions and trade-offs

**Raw bodies on Kafka, metadata in headers.** The message value is the exact
byte sequence the provider signed, so a signature can be re-verified months
later during an audit. Routing metadata rides in headers so the worker can
dispatch without trusting a re-parse.

**A separate billing lane.** Usage ingest is high-volume and loss-tolerant with
weeks of retention; billing is financial, ordered per customer, with years of
retention. Sharing a cluster means a usage spike can delay an invoice and a
billing incident can stall metering. The trade-off is a second cluster to run.

**Dedupe first, always.** Kafka delivery is not proof of uniqueness — Stripe
redelivers, the doorman can double-publish on a lost ack, and replays re-read
history on purpose. Claiming the event id is the first action on every message.
The store **fails closed**: if Redis is unreachable the worker stops rather
than risking a double charge, trading availability for correctness.

**`big.Rat`, never `float64`.** OpenMeter emits decimal strings precisely so
amounts survive transport exactly. Binary floating point cannot represent
`0.10`; a few thousand such lines is how an invoice ends up a cent out. Each
amount is rounded exactly once, half away from zero, at the Stripe boundary.

**Rounding residue is posted, not absorbed.** Rounding each line independently
can leave the sum a minor unit from the invoice total. That gap is real money,
so it becomes an explicit "Rounding adjustment" item. A gap larger than one
unit per line is not rounding — it is a mapping bug, and the worker refuses to
issue the invoice.

**Top-level lines only.** A usage-based line's children are OpenMeter's
internal breakdown of the same charge; billing both would double the invoice.
Children and discounts map to the Stripe item that contains them, so any
OpenMeter id can still be traced into Stripe.

**Ordered lanes over a plain worker pool.** A plain pool would let a customer's
`issuing.sync` overtake their `draft.sync`. The trade-off is that one slow
customer can back up its lane; raising `SETTLEMENT_LANES` reduces collisions.

**Contiguous-offset commits.** Slightly more bookkeeping than committing the
newest completion, and the only way a restart neither loses nor duplicates work.

**Idempotency keys plus lookup.** Stripe remembers idempotency keys for 24
hours; anything older is recovered by searching on our own metadata. Search
lags writes by up to a minute, so the two mechanisms cover each other's gaps.

**DLQ by default, halt available.** `dlq` keeps the partition flowing but
requires someone to watch it. `halt` never loses an event and never proceeds
past one — the safest choice for money, the loudest for uptime. Both are one
environment variable apart.

**A hand-rolled Stripe client.** Settlement touches four Stripe resources, and
every call needs precise control over `Stripe-Account` and `Idempotency-Key`.
A focused client keeps those visible and auditable in one file; the cost is
maintaining the request shapes ourselves.

**No settlement database.** The join between the two systems lives in Stripe
metadata (`openmeter_invoice_id`, `openmeter_line_id`) and in the OpenMeter
invoice's external ids. One less stateful component to operate and back up; the
cost is a Stripe lookup where a local table would have answered.

---

## Testing

```bash
make test    # unit and integration tests
make race    # under the race detector — the worker runs lanes, an offset
             # tracker and a commit loop concurrently
make cover   # coverage summary
```

The lifecycle tests run the full draft → issuing → payment flow against
in-memory fakes of both APIs. The fakes are strict on purpose: the Stripe fake
recomputes invoice totals from its items, rejects an application fee applied
after finalization, and records every `Stripe-Account` header — so mistakes in
ordering or routing fail the test rather than the production ledger.

---

## Implementation tasks

Sequenced roughly by dependency. Items marked complete ship in this repository;
the rest are the deployment and integration work around it.

### Phase 0 — Foundations

- [x] Provision billing topics separate from usage ingest, keyed by Connect
      account / customer id, with audit-length retention (`settlementctl topics ensure`)
- [ ] Install and configure the OpenMeter Custom Invoicing app; enable the
      draft sync, issuing sync and payment-pending hooks
- [ ] Fetch the OpenAPI spec from the **running** OpenMeter and confirm the
      three request schemas and the notification envelope against
      [docs/openmeter-custom-invoicing.md](docs/openmeter-custom-invoicing.md)
- [ ] Confirm the `extendedStatus` strings the deployment actually emits and
      set `SETTLEMENT_OPENMETER_*_STATUSES` accordingly

### Phase 1 — Producer

- [x] Raw-body signature verification for Stripe and OpenMeter, `400` on failure
- [x] Keyed publish of the unmodified payload, `200` only after the broker ack
- [x] Separate topics per source, with a type tag in the headers
- [ ] Decide whether the doorman stays on Vercel or runs as this container

### Phase 2 — Worker skeleton

- [x] Long-lived consumer with bounded, ordered lanes
- [x] Idempotency store keyed by event id, failing closed
- [x] Router by event type and invoice state, reading `availableActions`
- [x] Retry with backoff, DLQ or halt, and offset replay

### Phase 3 — Lifecycle handlers

- [x] `draft.sync`: map lines and discounts, call draft synchronized
- [x] `issuing.sync`: application fee, finalize, call invoicing synchronized
- [x] `payment_processing.pending`: trigger from the Stripe webhook, or from
      Stripe's own state when the webhook was lost
### Phase 4 — Connect and monetization

- [x] Charge-model resolution: platform, direct, destination
- [x] Application fees in basis points plus a flat component
- [ ] End-user account linking and checkout that saves a default payment
      method, mirroring app-owner onboarding (lives in the pymthouse app)
- [ ] Decide the default charge model per product tier and encode it in
      customer metadata at provisioning time

### Phase 5 — Hardening

- [x] Reconciliation sweep for invoices stranded by a dropped event
- [x] Metrics for failed, overdue and uncollectible invoices
- [x] Immutability respected — corrections go on the invoice, never upstream
- [ ] Wire the alert rules into the Prometheus stack
- [ ] Load-test an invoice burst and confirm per-customer ordering holds
- [x] Run a full test-mode settlement end to end and add it to the automated
      suite (Konnect + fake Stripe e2e)

---

## Further reading

- [docs/architecture.md](docs/architecture.md) — component and data-flow detail
- [docs/openmeter-custom-invoicing.md](docs/openmeter-custom-invoicing.md) — the
  pinned API contract and where it differs from the summary docs
- [docs/e2e-konnect-fake-stripe.md](docs/e2e-konnect-fake-stripe.md) — the
  Railway e2e environment and fake Stripe driver runbook
- [docs/operations.md](docs/operations.md) — runbook: deploying, replaying,
  redriving, and what each failure reason means

## Standards

- Webhook signatures follow [Standard Webhooks](https://www.standardwebhooks.com/)
  (OpenMeter) and Stripe's HMAC-SHA256 scheme, both built on
  [RFC 2104](https://datatracker.ietf.org/doc/html/rfc2104) HMAC and
  [RFC 6234](https://datatracker.ietf.org/doc/html/rfc6234) SHA-256.
- Timestamps are [RFC 3339](https://datatracker.ietf.org/doc/html/rfc3339).
- HTTP semantics follow [RFC 9110](https://datatracker.ietf.org/doc/html/rfc9110):
  `400` for an unverifiable request, `413` for an oversized body, `500` when an
  event could not be persisted and should be redelivered.

## License

MIT — see [LICENSE](LICENSE).
