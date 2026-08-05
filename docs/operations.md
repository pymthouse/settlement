# Operations runbook

Deploying, watching, and repairing the settlement billing lane.

This is written for whoever is on call. Every procedure states what it changes
and what it costs, because most of these commands move money or decide whether
money moves.

---

## Table of contents

- [Before the first production run](#before-the-first-production-run)
- [Deploying](#deploying)
- [What to watch](#what-to-watch)
- [Diagnosing a stuck invoice](#diagnosing-a-stuck-invoice)
- [Dead-letter reasons](#dead-letter-reasons)
- [Redriving the DLQ](#redriving-the-dlq)
- [Replaying after a mapping bug](#replaying-after-a-mapping-bug)
- [Rotating secrets](#rotating-secrets)
- [Common incidents](#common-incidents)
- [Auditing](#auditing)
- [Implementation tasks](#implementation-tasks)

---

## Before the first production run

A checklist, in order. Items above the line are launch-blocking.

- [ ] The Custom Invoicing app is installed with all three sync hooks enabled.
- [ ] `SETTLEMENT_OPENMETER_*_STATUSES` match the `extendedStatus` strings the
      deployment actually emits. **If these are wrong the worker treats every
      invoice as "not at a pause point" and silently does nothing.** Verify per
      [docs/openmeter-custom-invoicing.md](openmeter-custom-invoicing.md#verifying-against-a-live-instance).
- [ ] Billing topics exist with the intended partition count. Widening them
      later re-shards the keyspace and breaks ordering for in-flight invoices.
- [ ] `SETTLEMENT_REDIS_URL` is set, and Redis runs with
      `maxmemory-policy noeviction`.
- [ ] `SETTLEMENT_KAFKA_REQUIRED_ACKS=-1`.
- [ ] Webhook secrets are configured for both sources, and a signed test
      delivery from each reaches the doorman with a `200`.
- [ ] Alerts are wired for `settlement_dead_lettered_total` and
      `settlement_invoices_needing_attention_total`.

---

- [ ] One test-mode invoice has gone draft → issuing → paid end to end.
- [ ] The charge model is correct for each customer segment, and every customer
      on `direct` or `destination` has `stripe_connect_account_id` in their
      OpenMeter metadata.
- [ ] `SETTLEMENT_APPLICATION_FEE_BPS` matches the agreed fee schedule.

For the first live billing run, consider `SETTLEMENT_ON_RETRY_EXHAUSTED=halt`.
It stops the partition on the first thing it cannot settle, which is loud and
inconvenient — and much better than discovering a hundred parked invoices the
next morning. Switch to `dlq` once the alert has proven itself.

---

## Deploying

```bash
make docker                       # build the image
docker run --rm --env-file .env pymthouse/settlement producer
docker run --rm --env-file .env pymthouse/settlement worker
docker run --rm --env-file .env pymthouse/settlement settlementctl topics ensure
```

One image ships all three binaries, so the producer, the worker and the CLI can
never drift to different versions of the same event contract.

### Provisioning topics

```bash
settlementctl topics ensure \
  --partitions 12 --replication 3 --min-isr 2 --retention-days 3650
```

Existing topics are **left untouched**, deliberately: changing a partition count
re-shards the keyspace and would break per-customer ordering for any invoice
mid-lifecycle.

### Rolling the worker

The worker drains on `SIGTERM`: it stops fetching, finishes in-flight work,
commits the contiguous run, then exits. Give it at least
`SETTLEMENT_SHUTDOWN_TIMEOUT` (default 30s) before `SIGKILL`, or in-flight
invoices will be re-delivered to the next replica — correct, but noisier than it
needs to be.

Rolling replicas triggers a consumer-group rebalance. Uncommitted messages are
redelivered and dropped by the dedupe store; no action needed.

---

## What to watch

| Signal | Meaning | First response |
|---|---|---|
| `settlement_dead_lettered_total` rising | An event could not be settled | [Dead-letter reasons](#dead-letter-reasons) |
| `settlement_invoices_needing_attention_total` rising | Failed, overdue or uncollectible invoices | Inspect the invoice in OpenMeter |
| `settlement_webhooks_received_total{outcome="bad_signature"}` sustained | Wrong secret, or someone probing | [Rotating secrets](#rotating-secrets) |
| `settlement_committed_offset` flat while the topic grows | The worker is stuck or halted | Check worker logs for `stopping worker for safety` |
| `settlement_reconcile_redriven_total` sustained | The live pipeline is dropping events | Check the notification channel and the doorman's error rate |
| `settlement_events_in_flight` pinned at the lane ceiling | Handlers are slower than arrivals | Raise `SETTLEMENT_LANES`, or check upstream latency |
| `settlement_upstream_duration_seconds` p99 climbing | OpenMeter or Stripe is degraded | Expect retries; confirm they are succeeding |

A **halted** worker is the loudest state the system has. It logs
`stopping worker for safety` with a reason and stops committing. It halts only
when continuing would risk losing or double-processing an event:

- the dedupe store is unavailable,
- a DLQ publish failed, or
- `SETTLEMENT_ON_RETRY_EXHAUSTED=halt` and retries ran out.

Nothing is lost — the offsets are uncommitted and will be redelivered. Fix the
cause, then restart.

---

## Diagnosing a stuck invoice

Start from the invoice, not from the logs.

```bash
# 1. What does OpenMeter think?
curl -s -H "Authorization: Bearer $OPENMETER_API_KEY" \
  "$OPENMETER_URL/api/v1/billing/invoices/$INVOICE_ID?expand=lines" \
  | jq '{status, extended: .statusDetails.extendedStatus,
         failed: .statusDetails.failed, immutable: .statusDetails.immutable,
         actions: .statusDetails.availableActions | keys,
         external: .externalIds, issues: .validationIssues}'
```

- **`extendedStatus` is not in your configured lists** → the worker never
  considered it a pause point. Fix `SETTLEMENT_OPENMETER_*_STATUSES`.
- **`failed: true`** → OpenMeter recorded a problem; read `validationIssues`.
- **`externalIds.invoicing` is empty past draft** → draft sync never completed.

```bash
# 2. What does Stripe think?
stripe invoices list --limit 10 \
  --expand 'data.payment_intent' \
  | jq '.data[] | select(.metadata.openmeter_invoice_id == "'"$INVOICE_ID"'")'
```

```bash
# 3. What did the worker see?
kubectl logs deploy/settlement-worker | grep "$INVOICE_ID"
# or: docker compose -f deploy/docker-compose.yml logs worker | grep "$INVOICE_ID"
```

```bash
# 4. Is the event on the topic at all?
settlementctl inspect -topic billing.openmeter.invoices.v1 -count 50 \
  | jq 'select(.key == "'"$CUSTOMER_ID"'")'
```

**If the event never arrived**, the notification channel or the doorman is the
problem — check `settlement_webhooks_received_total` by outcome. The
reconciliation sweep will pick the invoice up within
`SETTLEMENT_RECONCILE_INTERVAL` regardless; that is what it is for.

---

## Dead-letter reasons

Every parked message carries `settlement-dlq-reason`. What each means, and
whether a redrive will help:

| Reason | Meaning | Redrive helps? |
|---|---|---|
| `missing_headers` | The message did not come through the doorman | No — investigate who wrote it |
| `unknown_source` | No handler for that `settlement-source` | No |
| `unparseable_notification` / `unparseable_stripe_event` | Body is not the expected JSON | No — likely a schema change |
| `missing_invoice_id` | Notification carried no invoice | No |
| `invoice_deleted` | The invoice is gone from OpenMeter | No — expected for deleted drafts |
| `invalid_charge_model` | Metadata names a model that does not exist | After fixing the metadata |
| `missing_connect_account` | `direct`/`destination` with no `acct_` id | After adding the account to customer metadata |
| `invalid_connect_account` | The value is not a Stripe account id | After fixing the metadata |
| `bad_line_amount` / `bad_invoice_total` | An amount would not parse | No — investigate the invoice |
| `total_mismatch` | Stripe items do not sum to the OpenMeter total | Only after fixing the mapping bug |
| `stripe_customer_rejected` / `stripe_invoice_rejected` / `stripe_items_rejected` | Stripe refused the request permanently | After fixing the cause |
| `stripe_finalize_rejected` | Stripe refused to finalize | After fixing the cause |
| `stripe_invoice_void` | The Stripe invoice was voided | No — resolve manually |
| `missing_stripe_invoice` | Reached issuing with no Stripe invoice | Yes — re-drive the draft sync first |
| `retry_exhausted` | Transient failures outlasted the retry budget | Yes, once the upstream recovers |

Read the parked messages before acting:

```bash
settlementctl inspect -topic billing.settlement.dlq.v1 -offset 0 -count 100 \
  | jq '{offset, reason: .dlq_reason, error: .dlq_error, event_id, event_type}'
```

---

## Redriving the DLQ

A redrive republishes parked messages to their original topic so the worker
handles them again. It stamps a batch id, which bypasses deduplication — the
messages are meant to be reprocessed.

**Always dry-run first.**

```bash
settlementctl dlq redrive -reason retry_exhausted -dry-run
settlementctl dlq redrive -reason retry_exhausted
```

Redrive one class of failure at a time. Mixing reasons makes it impossible to
tell which fix worked, and a reason that is still broken will simply park again.

The DLQ has multiple partitions; redrive each one (`-partition N`).

A redrive stops at the offsets that existed when it started. That bound is
deliberate: if it tailed the log, messages that failed again would land back on
the DLQ behind the reader and be redriven a second time, and a third —
amplifying two parked events into hundreds. Run the command again once you have
confirmed the re-parked messages are genuinely fixed.

---

## Replaying after a mapping bug

The scenario the Kafka log exists for: events were processed, incorrectly, and
must be processed again with the fix deployed.

```bash
# 1. Deploy the fix and confirm it is running.

# 2. Find where to start.
settlementctl inspect -topic billing.openmeter.invoices.v1 -offset 0 -count 5

# 3. Dry-run the range.
settlementctl replay -topic billing.openmeter.invoices.v1 -partition 0 \
  -since 2026-08-01T00:00:00Z -dry-run

# 4. Replay, one partition at a time.
settlementctl replay -topic billing.openmeter.invoices.v1 -partition 0 \
  -since 2026-08-01T00:00:00Z -batch fix-1234
```

Use a meaningful `-batch` value. It appears in the claim key and in the message
headers, so a later audit can tell replayed traffic from live traffic.

**Before replaying, understand what re-running will do.** Handlers are
idempotent against Stripe — an existing invoice is reused, existing items are
not duplicated — but a replay that spans `issuing.sync` events will attempt to
finalize invoices, and finalization is irreversible. Replay the narrowest range
that covers the bug.

---

## Rotating secrets

Both verifiers accept a comma-separated list, so rotation has no delivery gap.

```bash
# 1. Add the new secret alongside the old.
SETTLEMENT_STRIPE_WEBHOOK_SECRETS=whsec_old,whsec_new

# 2. Roll the producer. Both secrets now verify.
# 3. Switch the endpoint in the Stripe dashboard to the new secret.
# 4. Confirm settlement_webhooks_received_total{outcome="bad_signature"} is flat.
# 5. Drop the old secret and roll again.
SETTLEMENT_STRIPE_WEBHOOK_SECRETS=whsec_new
```

The same procedure applies to `SETTLEMENT_OPENMETER_WEBHOOK_SECRETS`.

Rotating the **Stripe secret key** or the **OpenMeter API key** is a plain
restart of the worker; nothing is in-flight that depends on the old credential
beyond the current request.

---

## Common incidents

### The doorman is returning 500s

Kafka is unreachable or refusing writes. Stripe will retry for up to three days
and OpenMeter's notification channel has its own retry schedule, so events are
not lost yet — but the clock is running.

Check broker health and `min.insync.replicas`. With `acks=-1`, losing enough
replicas makes every write fail. That is the intended behaviour: it is better to
reject an event the provider will redeliver than to accept one that is not
durably stored.

### Invoices are settling on the wrong account

Check the resolved charge model in the worker logs (`charge_model`,
`connect_account` on the `draft synchronized` line). Remember the resolution
order: invoice metadata, then customer metadata, then the environment default.

Invoices already issued cannot be re-routed — OpenMeter invoices are immutable
and Stripe invoices are finalized. Fix the metadata, then void and re-raise if
the amounts justify it.

### Every invoice is a no-op

Almost always the `extendedStatus` lists. Confirm what the deployment emits, set
the three `SETTLEMENT_OPENMETER_*_STATUSES` variables, restart, and let the
reconciliation sweep pick up everything that was skipped.

### The DLQ is filling with `total_mismatch`

A mapping bug: the Stripe items do not sum to the OpenMeter total. Do **not**
redrive until the mapping is fixed — the guard is doing its job, and every
redrive will park again. Inspect one parked message, reproduce the invoice
locally, fix, deploy, then redrive.

### Duplicate Stripe invoices for one OpenMeter invoice

This should not happen; the idempotency key plus the metadata search prevent it.
If it does: void the extra invoice in Stripe, confirm which id OpenMeter
recorded in `externalIds.invoicing`, and capture the worker logs for both
creations before anything is cleaned up.

---

## Auditing

The billing topics are retained for years precisely so questions like "what did
Stripe actually tell us on 3 August?" have an answer.

```bash
# Every event for one payer
settlementctl inspect -topic billing.stripe.events.v1 -offset 0 -count 1000 \
  | jq 'select(.key == "acct_dev_1")'

# The full body of a specific event
settlementctl inspect -topic billing.openmeter.invoices.v1 -offset 0 \
  -count 1000 -full | jq 'select(.event_id == "01J2KNP…")'
```

The stored body is byte-identical to what the provider signed, so a signature
can be re-verified against it long after the fact.

---

## Implementation tasks

- [ ] Wire the alert rules and page routing for the DLQ and attention metrics
- [ ] Add a dashboard covering the seven signals in
      [What to watch](#what-to-watch)
- [ ] Document the escalation path for a halted worker
- [ ] Rehearse a DLQ redrive and a replay in staging, and record how long each
      takes at production volume
- [ ] Schedule a quarterly secret rotation and confirm the no-gap procedure
      works
- [ ] Re-run the [pre-launch checklist](#before-the-first-production-run) after
      every OpenMeter upgrade
