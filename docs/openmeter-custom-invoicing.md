# The OpenMeter Custom Invoicing contract

This document pins the exact API surface `settlement` depends on, and records
where the deployed contract differs from how the integration is usually
summarised. Those differences are not pedantry — two of them determine where in
the lifecycle Stripe objects must be created.

**Pinned against OpenMeter `v1.0.0-beta.228`**, the release pymthouse runs
(`docker-compose.openmeter.yml`), verified against `@openmeter/sdk` of the same
version. Re-verify after any OpenMeter upgrade; see
[Verifying against a live instance](#verifying-against-a-live-instance).

---

## Table of contents

- [Installing the app](#installing-the-app)
- [The three completion endpoints](#the-three-completion-endpoints)
- [Request schemas](#request-schemas)
- [Corrections to the common summary](#corrections-to-the-common-summary)
- [The notification envelope](#the-notification-envelope)
- [Invoice state and available actions](#invoice-state-and-available-actions)
- [Immutability](#immutability)
- [How settlement maps lines to Stripe](#how-settlement-maps-lines-to-stripe)
- [Verifying against a live instance](#verifying-against-a-live-instance)
- [Implementation tasks](#implementation-tasks)

---

## Installing the app

The Custom Invoicing app replaces OpenMeter's built-in Stripe app. The built-in
app assumes a single merchant and a default payment method already linked to
the billing profile, which is precisely what Stripe Connect breaks: with
Connect, the merchant varies per invoice.

- **OpenMeter Cloud:** install from the App Marketplace.
- **Self-hosted:** install through the Marketplace API.

The app appears in the OpenAPI spec under the tag `App: Custom Invoicing`
("Interface third party invoicing and payment systems"), alongside `App: Stripe`.

Enable all three sync hooks. Without them the invoice never pauses, and
settlement never gets the chance to build the Stripe side.

---

## The three completion endpoints

| Hook | Endpoint | Operation id |
|---|---|---|
| Draft sync | `POST /api/v1/apps/custom-invoicing/{invoiceId}/draft/synchronized` | `appCustomInvoicingDraftSynchronized` |
| Issuing sync | `POST /api/v1/apps/custom-invoicing/{invoiceId}/issuing/synchronized` | `appCustomInvoicingIssuingSynchronized` |
| Payment | `POST /api/v1/apps/custom-invoicing/{invoiceId}/payment/status` | `appCustomInvoicingUpdatePaymentStatus` |

All three authenticate with `Authorization: Bearer <api key>` and answer `2xx`
with no meaningful body.

Settlement treats **`409 Conflict` as success**. It means the invoice already
advanced past the state we were completing — a duplicate delivery, or the
reconciliation sweep arriving first. The desired state holds either way, and
retrying would eventually dead-letter a perfectly settled invoice.

---

## Request schemas

### Draft synchronized — `CustomInvoicingDraftSynchronizedRequest`

```jsonc
{
  "invoicing": {                          // CustomInvoicingSyncResult
    "invoiceNumber": "…",                 // optional
    "externalId": "in_1ABC…",             // the Stripe invoice id
    "lineExternalIds": [                  // CustomInvoicingLineExternalIdMapping[]
      { "lineId": "01G65Z…", "externalId": "ii_1ABC…" }
    ],
    "lineDiscountExternalIds": [          // CustomInvoicingLineDiscountExternalIdMapping[]
      { "lineDiscountId": "01G65Z…", "externalId": "ii_1ABC…" }
    ]
  }
}
```

Every field is optional. **This is the only call that accepts line and discount
mappings.**

### Issuing synchronized — `CustomInvoicingFinalizedRequest`

```jsonc
{
  "invoicing": {                          // CustomInvoicingFinalizedInvoicingRequest
    "invoiceNumber": "STRIPE-0001",       // 1–256 chars; omit to let OpenMeter
                                          // generate an INV- prefixed number
    "sentToCustomerAt": "2026-08-04T12:00:00.000Z"   // RFC 3339
  },
  "payment": {                            // CustomInvoicingFinalizedPaymentRequest
    "externalId": "pi_1ABC…"              // the Stripe payment reference
  }
}
```

Note there is **no line mapping here**. Whatever was not reported at draft sync
cannot be reported at all.

### Payment status — `CustomInvoicingUpdatePaymentStatusRequest`

```jsonc
{ "trigger": "paid" }
```

`trigger` is required and is the **only** field. Valid values
(`CustomInvoicingPaymentTrigger`):

`paid` · `payment_failed` · `payment_uncollectible` · `payment_overdue` ·
`action_required` · `void`

Settlement maps Stripe events to triggers as follows:

| Stripe event | Trigger |
|---|---|
| `invoice.payment_succeeded`, `invoice.paid` | `paid` |
| `invoice.payment_failed` | `payment_failed` |
| `invoice.marked_uncollectible` | `payment_uncollectible` |
| `invoice.voided` | `void` |
| `invoice.payment_action_required` | `action_required` |
| `invoice.overdue` | `payment_overdue` |

Everything else — `invoice.created`, `invoice.updated`, `customer.*`,
`charge.*` — is Stripe narrating its own bookkeeping and must not move the
OpenMeter invoice.

---

## Corrections to the common summary

Three points where the deployed contract differs from the way the integration
is usually described. Each one changed a design decision.

### 1. The payment call takes no external id

The summary describes a "payment synchronized" call whose body carries a Stripe
payment reference. There is no such call. The endpoint is
`.../payment/status`, and its body is a bare `{ "trigger": … }`.

**Consequence:** the Stripe payment reference must be stamped during the
*issuing* call, via `payment.externalId` on `CustomInvoicingFinalizedRequest`.
Settlement reads the PaymentIntent that Stripe creates at finalization (falling
back to the charge) and sends it then.

### 2. Line mappings exist only at draft sync

Since `CustomInvoicingFinalizedRequest` accepts no line ids, everything the
lifecycle needs to reference in Stripe must already exist when draft sync
completes.

**Consequence:** settlement creates the Stripe **draft invoice and all of its
items during draft sync**, and merely *finalizes* that invoice at issuing. This
turns out to line up cleanly with Stripe's own model — a Stripe draft invoice
mirrors an OpenMeter draft, and Stripe finalization mirrors OpenMeter issuing.

### 3. Notification event types are `invoice.created` / `invoice.updated`

`NotificationEventType` is exactly:

`entitlements.balance.threshold` · `entitlements.reset` · `invoice.created` ·
`invoice.updated`

There is no `invoicing.` prefix in the deployed enum. Settlement accepts the
prefixed form defensively (`NormalizedType()` strips it) so a documentation
variant cannot silently drop events, but the canonical values are the two
above.

> Gathering-invoice notifications are **not** emitted. An invoice becomes
> visible to settlement at `draft.created` and no earlier.

---

## The notification envelope

`NotificationEventInvoiceCreatedPayload` / `NotificationEventInvoiceUpdatedPayload`:

```jsonc
{
  "id": "01J2KNP1YTXQRXHTDJ4KPR7PZ0",     // the deduplication key
  "type": "invoice.updated",
  "timestamp": "2026-08-04T12:00:00.001Z",
  "data": { /* the fully expanded Invoice */ }
}
```

Notification channels sign with [Standard Webhooks](https://www.standardwebhooks.com/)
headers — `webhook-id`, `webhook-timestamp`, `webhook-signature` (Svix-hosted
channels use `svix-*`). The signed content is
`"{id}.{timestamp}.{raw body}"`, HMAC-SHA256 with the base64 payload of the
`whsec_`-prefixed secret. Settlement accepts both header spellings.

**Settlement does not trust the embedded invoice for decisions.** It reads the
id and re-fetches. Notifications can arrive out of order relative to the
invoice's real state, and acting on a stale draft is how a duplicate Stripe
invoice gets created.

---

## Invoice state and available actions

`InvoiceStatus` (the coarse status):

`gathering` · `draft` · `issuing` · `issued` · `payment_processing` ·
`overdue` · `paid` · `uncollectible` · `voided`

`InvoiceStatusDetails` carries the fields OpenMeter asks integrators to branch
on:

```jsonc
{
  "immutable": false,
  "failed": false,
  "extendedStatus": "draft.sync",
  "availableActions": {
    "approve": { "resultingState": "payment_processing.pending" },
    "delete":  { "resultingState": "deleted" }
    // also: advance, retry, snapshotQuantities, void, invoice
  }
}
```

The OpenAPI description is explicit: *"API users are encouraged to rely on the
immutable/failed/availableActions fields to determine the next steps of the
invoice instead of the extendedStatus field."*

Settlement follows this. It uses `extendedStatus` only to recognise which of
the three pause points an invoice is sitting at — and even that comes from
configurable lists (`SETTLEMENT_OPENMETER_*_STATUSES`), because the string
vocabulary has changed across releases. `failed` and the coarse status drive
the "needs attention" alerting.

---

## Immutability

An OpenMeter invoice is **immutable once created**: it clones the billing
profile and customer data at creation time. Correcting a customer name upstream
does nothing to invoices already raised.

Two consequences settlement respects:

1. Corrections belong on the invoice, not on the customer.
2. Routing frozen into an invoice must be honoured. An invoice raised while a
   developer was on the platform model settles that way even if they have since
   moved to Connect — which is why invoice metadata outranks customer metadata
   when resolving the charge model.

---

## How settlement maps lines to Stripe

One **top-level** OpenMeter line becomes one Stripe invoice item, priced at the
line's settled `totals.total`.

- **Children are not billed.** A usage-based line's `children` are OpenMeter's
  internal breakdown of the same charge; billing them too would double the
  invoice. They are *mapped* to the containing Stripe item so any OpenMeter id
  can be traced into Stripe.
- **Discounts are not separate Stripe objects.** `totals.total` is already net
  of discounts. Each `lineDiscountId` maps to the item that contains it, which
  keeps the Stripe total exactly equal to the OpenMeter total — the property
  that makes reconciliation possible at all.
- **Rounding residue is posted explicitly.** Per-line rounding can leave the
  sum a minor unit away from the invoice total; the difference becomes a
  "Rounding adjustment" item. A gap larger than one minor unit per line is
  treated as a mapping bug: the invoice is refused with a permanent
  `total_mismatch` fault rather than issued at the wrong amount.

Amounts are parsed as exact rationals (`math/big.Rat`) and rounded once, half
away from zero, at the Stripe boundary. Currency exponents follow Stripe's
zero-decimal and three-decimal lists.

Metadata written onto Stripe objects — the join between the two systems, and
the reason settlement needs no database of its own:

| Key | On | Value |
|---|---|---|
| `openmeter_invoice_id` | invoice, item | OpenMeter invoice id |
| `openmeter_customer_id` | customer, invoice | OpenMeter customer id |
| `openmeter_customer_key` | customer | OpenMeter customer key |
| `openmeter_line_id` | item | OpenMeter line id, or `rounding-adjustment` |
| `pymthouse_source` | all | `pymthouse-settlement` |

---

## Verifying against a live instance

The schemas here were read from the pinned SDK, which matches the deployed
image version. Before going to production, confirm against the **running**
instance — a reference copy can lag what is actually deployed.

```bash
# Dump the live spec
curl -s http://127.0.0.1:48888/api/v1/openapi.json > /tmp/openmeter.json

# The three request schemas
jq '.components.schemas.CustomInvoicingDraftSynchronizedRequest,
    .components.schemas.CustomInvoicingSyncResult,
    .components.schemas.CustomInvoicingFinalizedRequest,
    .components.schemas.CustomInvoicingUpdatePaymentStatusRequest' /tmp/openmeter.json

# The endpoints
jq '.paths | keys[] | select(contains("custom-invoicing"))' /tmp/openmeter.json

# The notification event types
jq '.components.schemas.NotificationEventType' /tmp/openmeter.json
```

To confirm the `extendedStatus` strings your deployment actually emits, take a
real invoice through a billing run and read them back:

```bash
curl -s -H "Authorization: Bearer $OPENMETER_API_KEY" \
  "$OPENMETER_URL/api/v1/billing/invoices?expand=lines" \
  | jq '.items[] | {id, status, extended: .statusDetails.extendedStatus}'
```

Set `SETTLEMENT_OPENMETER_DRAFT_SYNC_STATUSES`,
`SETTLEMENT_OPENMETER_ISSUING_SYNC_STATUSES` and
`SETTLEMENT_OPENMETER_PAYMENT_PENDING_STATUSES` from what you observe. If they
are wrong, the worker will treat every invoice as "not at a pause point" and
quietly do nothing — which is why this is a launch-blocking check.

---

## Implementation tasks

- [ ] Install the Custom Invoicing app and enable all three sync hooks
- [ ] Dump the live OpenAPI spec and diff the three request schemas against
      [`internal/openmeter/types.go`](../internal/openmeter/types.go)
- [ ] Observe the real `extendedStatus` values and pin the three status lists
- [ ] Create the notification channel, record its signing secret in
      `SETTLEMENT_OPENMETER_WEBHOOK_SECRETS`, and confirm a signed delivery
      reaches the doorman
- [ ] Take one test-mode invoice through draft → issuing → paid and confirm
      each completion call is accepted
- [ ] Confirm the Stripe invoice total equals the OpenMeter invoice total on an
      invoice with discounts and a usage-based line with children
- [ ] Re-run this checklist after every OpenMeter upgrade
