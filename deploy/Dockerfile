# Multi-stage build for the settlement binaries.
#
# One image ships all three commands; the entrypoint selects which to run, so
# the producer, the worker and settlementctl can never drift to different
# versions of the same event contract.
#
#   docker build -f deploy/Dockerfile -t pymthouse/settlement .
#   docker run --rm pymthouse/settlement worker

FROM golang:1.25-alpine AS build

WORKDIR /src

# Dependencies first: they change far less often than the source.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

# CGO off keeps the binaries static so they run on a distroless-style base.
# -trimpath keeps build paths out of the artefact.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/producer ./cmd/producer && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/worker ./cmd/worker && \
    CGO_ENABLED=0 GOOS=linux go build -trimpath \
      -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
      -o /out/settlementctl ./cmd/settlementctl

FROM alpine:3.22

# ca-certificates is required to reach api.stripe.com over TLS; wget serves the
# container healthcheck.
RUN apk add --no-cache ca-certificates tzdata wget && \
    adduser -D -u 10001 settlement

COPY --from=build /out/producer /usr/local/bin/producer
COPY --from=build /out/worker /usr/local/bin/worker
COPY --from=build /out/settlementctl /usr/local/bin/settlementctl

USER settlement

# Money-handling processes should never run as root and never need a shell.
ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/\"$0\" \"$@\""]
CMD ["worker"]
