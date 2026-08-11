# Settlement billing Kafka (Redpanda) for Railway private networking.
# Separate from pymthouse usage-ingest kafka: billing is financial, ordered
# per customer, multi-year retention.
#
# Railway volume mounts are root-owned. The official image's uid 101 cannot
# create files under the mount, so this image starts as root. Prefer a
# startCommand that invokes `rpk redpanda start ...` (Railway image deploys
# replace ENTRYPOINT; bare `redpanda` hits the C++ binary which rejects
# --kafka-addr).
FROM redpandadata/redpanda:v24.2.4

USER root

CMD ["rpk", "redpanda", "start", \
  "--kafka-addr", "internal://0.0.0.0:9092", \
  "--advertise-kafka-addr", "internal://settlement-kafka.railway.internal:9092", \
  "--smp", "1", \
  "--memory", "1G", \
  "--reserve-memory", "0M", \
  "--overprovisioned", \
  "--check=false", \
  "--default-log-level", "warn", \
  "--set", "redpanda.auto_create_topics_enabled=false"]
