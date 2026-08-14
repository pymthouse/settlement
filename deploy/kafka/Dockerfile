# Settlement billing Kafka (Redpanda) for Railway private networking.
# Separate from pymthouse usage-ingest kafka: billing is financial, ordered
# per customer, multi-year retention.
#
# Railway volume mounts are root-owned. The official image's uid 101 cannot
# create files under the mount, so this image starts as root. Prefer a
# startCommand that invokes `rpk redpanda start ...` (Railway image deploys
# replace ENTRYPOINT; bare `redpanda` hits the C++ binary which rejects
# --kafka-addr).
#
# Railway builds this file with repo-root context (dockerfilePath
# deploy/kafka/Dockerfile, no rootDirectory). COPY paths are repo-root relative.
FROM redpandadata/redpanda:v24.2.4

USER root

COPY deploy/kafka/start.sh /usr/local/bin/settlement-kafka-start.sh
RUN chmod +x /usr/local/bin/settlement-kafka-start.sh

# Admin API binds to 127.0.0.1 by default (see start.sh). Override with
# REDPANDA_ADMIN_ADDR=0.0.0.0 when remote Admin API access is required.
CMD ["/usr/local/bin/settlement-kafka-start.sh"]
