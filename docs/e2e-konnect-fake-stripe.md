# Konnect + Fake Stripe E2E

This runbook covers the test-mode settlement path that uses the real worker and producer binaries, a Railway mirror of production, and `stripefake` instead of Stripe's API.

## Safety rules

- Fake Stripe only flows through `SETTLEMENT_STRIPE_API_BASE`.
- Production Railway must never point at `stripefake`.
- The environment check script refuses any production Stripe base override.

## What runs in the e2e stack

- `producer` handles signed Stripe webhooks.
- `worker` drives OpenMeter invoices through the Stripe lifecycle.
- `stripefake` emulates Stripe and emits signed payment webhooks back to the producer.
- `redpanda` and `redis` match the local billing lane layout.

## Required environment variables

- `SETTLEMENT_OPENMETER_URL`
- `SETTLEMENT_OPENMETER_API_KEY`
- `SETTLEMENT_E2E_BILLING_PROFILE_ID`
- `SETTLEMENT_E2E_PRODUCER_URL`
- `SETTLEMENT_E2E_WORKER_URL`
- `SETTLEMENT_E2E_STRIPEFAKE_URL`
- `SETTLEMENT_STRIPE_API_BASE` pointing at `stripefake`
- `SETTLEMENT_STRIPE_SECRET_KEY`
- `SETTLEMENT_STRIPE_WEBHOOK_SECRETS`

Optional:

- `SETTLEMENT_E2E_CONNECT_ACCOUNT_ID` defaults to `acct_e2e_settlement`
- `SETTLEMENT_RAILWAY_ENVIRONMENT` or `RAILWAY_ENVIRONMENT` for the guard script

## Railway setup

1. Mirror the production service layout into a dedicated Railway e2e environment.
2. Point the worker at OpenMeter and `stripefake`.
3. Point the producer at the Stripe webhook secret used by `stripefake`.
4. Run `scripts/railway-e2e-env-check.sh` before deploying or testing.

## Running the driver

```bash
export SETTLEMENT_OPENMETER_URL=...
export SETTLEMENT_OPENMETER_API_KEY=...
export SETTLEMENT_E2E_BILLING_PROFILE_ID=...
export SETTLEMENT_E2E_PRODUCER_URL=...
export SETTLEMENT_E2E_WORKER_URL=...
export SETTLEMENT_E2E_STRIPEFAKE_URL=...
export SETTLEMENT_STRIPE_API_BASE=http://stripefake:12111
export SETTLEMENT_STRIPE_SECRET_KEY=sk_test_e2e
export SETTLEMENT_STRIPE_WEBHOOK_SECRETS=whsec_test

./scripts/railway-e2e-env-check.sh
./bin/e2e
```

The driver health-checks the producer, worker and `stripefake`, refuses to run against the production Stripe API, creates a Konnect test customer, applies the billing-profile override, adds pending lines, advances the invoice, waits for the invoice to reach `paid`, and then verifies the fake Stripe state saw the expected connect account and total.
