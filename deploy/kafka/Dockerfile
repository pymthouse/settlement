# Settlement billing Kafka (Redpanda) for Railway private networking.
# Separate from pymthouse usage-ingest kafka: billing is financial, ordered
# per customer, multi-year retention.
#
# Railway volume mounts are root-owned. entrypoint.sh chowns the data dir and
# drops to uid 101 before redpanda starts. Prefer a startCommand that invokes
# `rpk redpanda start ...` (Railway image deploys replace ENTRYPOINT; bare
# `redpanda` hits the C++ binary which rejects --kafka-addr).
FROM redpandadata/redpanda:v24.2.4

# Railway builds this file with repo-root context (dockerfilePath
# deploy/kafka/Dockerfile, no rootDirectory). COPY paths are repo-root relative.
# The base image USER is 101, so switch to root before COPY/chmod.
USER root

COPY deploy/kafka/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Settlement only uses the Kafka API (:9092). The Admin API (:9644) is unused
# by producer/worker/settlementctl, so disable it rather than leave an
# unauthenticated listener on 0.0.0.0.
ENTRYPOINT ["/entrypoint.sh"]
CMD ["rpk", "redpanda", "start", \
  "--kafka-addr", "internal://0.0.0.0:9092", \
  "--advertise-kafka-addr", "internal://settlement-kafka.railway.internal:9092", \
  "--smp", "1", \
  "--memory", "1G", \
  "--reserve-memory", "0M", \
  "--overprovisioned", \
  "--check=false", \
  "--default-log-level", "warn", \
  "--set", "redpanda.auto_create_topics_enabled=false", \
  "--set", "redpanda.enable_admin_api=false"]
