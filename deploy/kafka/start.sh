#!/bin/sh
# Settlement billing Kafka (Redpanda) entrypoint.
#
# Admin API cannot be fully disabled on Redpanda v24 (`enable_admin_api` was
# removed). Bind it to loopback by default so it is not exposed on the
# private network. Set REDPANDA_ADMIN_ADDR=0.0.0.0 only when remote Admin
# API access is intentional (and prefer admin_api_require_auth too).
set -eu

ADMIN_ADDR="${REDPANDA_ADMIN_ADDR:-127.0.0.1}"

exec rpk redpanda start \
  --kafka-addr "internal://0.0.0.0:9092" \
  --advertise-kafka-addr "internal://settlement-kafka.railway.internal:9092" \
  --smp 1 \
  --memory 1G \
  --reserve-memory 0M \
  --overprovisioned \
  --check=false \
  --default-log-level warn \
  --set redpanda.auto_create_topics_enabled=false \
  --set "redpanda.admin=[{address:${ADMIN_ADDR},port:9644}]"
