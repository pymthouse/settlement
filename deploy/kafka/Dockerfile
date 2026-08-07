# Settlement billing Kafka (Redpanda) for Railway private networking.
# Separate from pymthouse usage-ingest kafka: billing is financial, ordered
# per customer, multi-year retention.
#
# The broker runs as the image's unprivileged redpanda user (uid 101), so the
# Railway volume mounted at /var/lib/redpanda/data must be writable by uid 101.
# If startup fails with a permission error on the data dir, chown the volume to
# 101:101 — do not put the broker back on root to work around it.
#
# Prefer startCommand on Railway that invokes `rpk redpanda start ...`
# (Railway image deploys replace ENTRYPOINT; bare `redpanda` hits the C++
# binary which rejects --kafka-addr).
FROM redpandadata/redpanda:v24.2.4

USER redpanda

CMD ["rpk", "redpanda", "start", \
  "--kafka-addr", "internal://0.0.0.0:9092", \
  "--advertise-kafka-addr", "internal://settlement-kafka.railway.internal:9092", \
  "--smp", "1", \
  "--memory", "1G", \
  "--reserve-memory", "0M", \
  "--overprovisioned", \
  "--check=false", \
  "--default-log-level=warn", \
  "--set", "redpanda.auto_create_topics_enabled=false"]
