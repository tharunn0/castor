.DEFAULT_GOAL := help

# Go parameters
GOCMD      := go
GOBUILD    := $(GOCMD) build
GOTEST     := $(GOCMD) test
GOFMT      := gofmt
GOCLEAN    := $(GOCMD) clean

# Versioning & images
SERVICES     ?= metadata-svc data-svc gateway-svc auth-svc
VERSION      ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
COMMIT       ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
IMAGE_TAG    ?= $(patsubst v%,%,$(VERSION))
REGISTRY     ?= ghcr.io/tharunn0
PLATFORMS    ?= linux/amd64,linux/arm64
DOCKERFILE   := deploy/docker/Dockerfile
VERSION_PKG  := github.com/tharunn0/castor/internal/version
LDFLAGS      := -s -w -X $(VERSION_PKG).Version=$(VERSION) -X $(VERSION_PKG).Commit=$(COMMIT)

# Test configurations
TEST_TIMEOUT   := 120s
RACE_TIMEOUT   := 300s
INTEG_TIMEOUT  := 180s

# Code Quality & Formatting
fmt: ## Format Go source files using gofmt
	@echo "==> Formatting Go files..."
	@$(GOFMT) -s -w .
fmt-check: ## Check code formatting without applying changes
	@echo "==> Checking Go formatting..."
	@test -z "$$($(GOFMT) -s -l .)" || (echo "Unformatted files exist. Run 'make fmt'"; exit 1)

vet: ## Run go vet analysis
	@echo "==> Running go vet..."
	@$(GOCMD) vet ./...

lint: fmt-check vet ## Run formatting and vet linters



# Testing Suites
test: ## Run all unit tests across the entire repository
	@echo "==> Running all unit tests..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./internal/...
test-race: ## Run all unit tests with data race detector enabled (-race)
	@echo "==> Running tests with data race detector..."
	@$(GOTEST) -v -race -count=1 -timeout $(RACE_TIMEOUT) ./internal/...
test-full: ## Run all unit and integration tests with fresh cache (-count=1)
	@echo "==> Running full test suite (unit + integration)..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./...
test-integration: ## Run live end-to-end integration tests only
	@echo "==> Running integration tests..."
	@$(GOTEST) -v -count=1 -timeout $(INTEG_TIMEOUT) ./tests/integration/...



# Per-Service Unit Tests
test-gateway: ## Run gateway-svc tests 
	@echo "==> Testing gateway-svc..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./internal/gateway/...
test-metadata: ## Run metadata-svc tests 
	@echo "==> Testing metadata-svc..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./internal/metadata/...
test-data: ## Run data-svc tests
	@echo "==> Testing data-svc..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./internal/data/...
test-auth: ## Run auth-svc tests
	@echo "==> Testing auth-svc..."
	@$(GOTEST) -v -count=1 -timeout $(TEST_TIMEOUT) ./internal/auth/...


# Build & Release
version: ## Print the version that builds and images will be stamped with
	@echo "version=$(VERSION) commit=$(COMMIT) image_tag=$(IMAGE_TAG)"
build: ## Build service binaries into bin/ (SERVICES=... to limit)
	@for svc in $(SERVICES); do \
		echo "==> Building $$svc $(VERSION)..."; \
		CGO_ENABLED=0 $(GOBUILD) -trimpath -ldflags "$(LDFLAGS)" -o bin/$$svc ./cmd/$$svc || exit 1; \
	done
images: ## Build service images for the host platform into the local Docker daemon
	@for svc in $(SERVICES); do \
		echo "==> Building image $(REGISTRY)/castor-$$svc:$(IMAGE_TAG)..."; \
		docker buildx build -f $(DOCKERFILE) --target $$svc \
			--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
			-t $(REGISTRY)/castor-$$svc:$(IMAGE_TAG) --load . || exit 1; \
	done
push: ## Build multi-arch service images and push them to $(REGISTRY)
	@for svc in $(SERVICES); do \
		echo "==> Pushing image $(REGISTRY)/castor-$$svc:$(IMAGE_TAG)..."; \
		docker buildx build -f $(DOCKERFILE) --target $$svc --platform $(PLATFORMS) \
			--build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) \
			-t $(REGISTRY)/castor-$$svc:$(IMAGE_TAG) --push . || exit 1; \
	done
