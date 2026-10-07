.DEFAULT_GOAL := help

# Go parameters
GOCMD      := go
GOBUILD    := $(GOCMD) build
GOTEST     := $(GOCMD) test
GOFMT      := gofmt
GOCLEAN    := $(GOCMD) clean

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