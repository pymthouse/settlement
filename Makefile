# settlement — build, test and operate the billing lane.

GO           ?= go
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
LDFLAGS      := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)
COMPOSE      := docker compose -f deploy/docker-compose.yml
IMAGE        ?= pymthouse/settlement

.PHONY: help
help: ## Show this help
	@awk 'BEGIN {FS = ":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

.PHONY: build
build: ## Build all service binaries into bin/
	@mkdir -p bin
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/producer ./cmd/producer
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/worker ./cmd/worker
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/settlementctl ./cmd/settlementctl
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/stripefake ./cmd/stripefake
	$(GO) build -trimpath -ldflags="$(LDFLAGS)" -o bin/e2e ./cmd/e2e

.PHONY: test
test: ## Run the test suite
	$(GO) test ./...

.PHONY: race
race: ## Run the test suite under the race detector
	$(GO) test -race ./...

.PHONY: cover
cover: ## Run tests with a coverage report
	$(GO) test -coverprofile=coverage.out -covermode=atomic ./...
	$(GO) tool cover -func=coverage.out | tail -1

.PHONY: cover-html
cover-html: cover ## Open the coverage report in a browser
	$(GO) tool cover -html=coverage.out

.PHONY: lint
lint: ## Vet, format check and module tidiness
	$(GO) vet ./...
	@unformatted=$$(find . -name '*.go' -not -path './vendor/*' -print0 | xargs -0 -r gofmt -l || true); \
	if [ -n "$$unformatted" ]; then echo "gofmt needed:"; echo "$$unformatted"; exit 1; fi
	$(GO) mod tidy -diff

.PHONY: check
check: lint race ## Everything CI runs

.PHONY: docker
docker: ## Build the container image
	docker build -f deploy/Dockerfile -t $(IMAGE):$(VERSION) -t $(IMAGE):latest \
		--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) .

.PHONY: up
up: ## Start the local stack (Redpanda, Redis, producer, worker)
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop the local stack and remove its volumes
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Follow the worker logs
	$(COMPOSE) logs -f worker

.PHONY: topics
topics: ## Provision the billing topics on the local stack
	$(COMPOSE) run --rm settlementctl topics ensure --partitions 3 --replication 1

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf bin coverage.out
