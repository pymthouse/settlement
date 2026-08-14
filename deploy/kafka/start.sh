#!/bin/sh
# Settlement billing Kafka (Redpanda) entrypoint.
#
# Admin API defaults to loopback. Settlement never calls it; binding
# 127.0.0.1 keeps a mis-enabled listener off the private network.
# Set REDPANDA_ADMIN_ADDR=0.0.0.0 only when you intentionally need
# remote Admin API access (and prefer admin_api_require_auth too).
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
  --set redpanda.enable_admin_api=false \
  --set "redpanda.admin=[{address:${ADMIN_ADDR},port:9644}]"
