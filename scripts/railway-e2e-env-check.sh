#!/usr/bin/env bash
set -euo pipefail

railway_env=${RAILWAY_ENVIRONMENT:-${SETTLEMENT_RAILWAY_ENVIRONMENT:-}}
case "${railway_env,,}" in
  production|prod)
    echo "refusing to touch production Railway from the e2e environment check" >&2
    exit 1
    ;;
  *)
    # Any other environment (including unset) is a non-production target.
    ;;
esac

stripe_base=${SETTLEMENT_STRIPE_API_BASE:-}
if [[ -z "$stripe_base" ]]; then
  echo "SETTLEMENT_STRIPE_API_BASE is required" >&2
  exit 1
fi
if [[ "$stripe_base" == *"api.stripe.com"* ]]; then
  echo "refusing production Stripe base override: $stripe_base" >&2
  exit 1
fi

required=(
  SETTLEMENT_OPENMETER_URL
  SETTLEMENT_OPENMETER_API_KEY
  SETTLEMENT_E2E_BILLING_PROFILE_ID
  SETTLEMENT_E2E_PRODUCER_URL
  SETTLEMENT_E2E_WORKER_URL
  SETTLEMENT_E2E_STRIPEFAKE_URL
)

missing=()
for name in "${required[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    missing+=("$name")
  fi
done

if (( ${#missing[@]} > 0 )); then
  printf 'missing required env vars: %s\n' "${missing[*]}" >&2
  exit 1
fi

echo "Railway e2e environment looks safe."
