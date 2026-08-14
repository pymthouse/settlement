---
name: railway-kafka-dockerfile
description: >-
  Settlement Kafka (Redpanda) Railway image build rules for deploy/kafka/Dockerfile.
  Use when editing deploy/kafka/*, entrypoint.sh, Kafka Dockerfile COPY/USER layers,
  or debugging Railway settlement-kafka build failures.
---

# Railway Kafka Dockerfile

## Hard-won failure note (do not regress)

The failed build was from cd544f8 adding entrypoint.sh. Railway builds deploy/kafka/Dockerfile with the repo root as context, and the Redpanda image’s default user is uid 101. That caused two build failures:

1. `COPY entrypoint.sh` looked at the repo root → `/entrypoint.sh: not found`
2. After the path fix, `chmod +x /entrypoint.sh` ran as uid 101 → `Operation not permitted`

## Rules

When changing `deploy/kafka/Dockerfile`:

1. **COPY paths are repo-root relative** — Railway sets `dockerfilePath=deploy/kafka/Dockerfile` with **no** `rootDirectory`. Never `COPY entrypoint.sh`; always `COPY deploy/kafka/entrypoint.sh`.
2. **`USER root` before COPY/chmod** — the `redpandadata/redpanda` base image defaults to uid 101. Switch to root before any `COPY`/`RUN chmod` on files you add. Do **not** “fix” this by removing `USER root` — that breaks the image build. Sonar `docker:S6471` is intentionally accepted (`sonar-project.properties` ignore + SonarCloud ACCEPTED).
3. Prefer a Railway `startCommand` that runs `rpk redpanda start ...`. Railway image deploys replace `ENTRYPOINT`; bare `redpanda` hits the C++ binary and rejects `--kafka-addr`.

## Known-good pattern

```dockerfile
FROM redpandadata/redpanda:v24.2.4

USER root

COPY deploy/kafka/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["rpk", "redpanda", "start", ...]
```

## Related history

- `e07c354` — same COPY-path lesson for a prior `start.sh`
- `63ffbd6` — fix COPY to `deploy/kafka/entrypoint.sh`
- `0b0d513` — move `USER root` above COPY/chmod
