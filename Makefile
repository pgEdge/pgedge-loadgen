# pgedge-loadgen Makefile

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=gofmt
GOVET=$(GOCMD) vet

# Binary name and output directory
BINARY_NAME=pgedge-loadgen
BINARY_DIR=bin
BINARY_PATH=./cmd/pgedge-loadgen

# Build information
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_DATE ?= $(shell date -u '+%Y-%m-%dT%H:%M:%SZ')

# Linker flags
LDFLAGS=-ldflags "-s -w \
	-X github.com/pgEdge/pgedge-loadgen/pkg/version.Version=$(VERSION) \
	-X github.com/pgEdge/pgedge-loadgen/pkg/version.Commit=$(COMMIT) \
	-X github.com/pgEdge/pgedge-loadgen/pkg/version.BuildDate=$(BUILD_DATE)"

# Default target
.PHONY: all
all: lint test build

# Build the binary
.PHONY: build
build:
	@mkdir -p $(BINARY_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME) $(BINARY_PATH)

# Build for all platforms
.PHONY: build-all
build-all:
	@mkdir -p $(BINARY_DIR)
	GOOS=linux GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-linux-amd64 $(BINARY_PATH)
	GOOS=linux GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-linux-arm64 $(BINARY_PATH)
	GOOS=darwin GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-darwin-amd64 $(BINARY_PATH)
	GOOS=darwin GOARCH=arm64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-darwin-arm64 $(BINARY_PATH)
	GOOS=windows GOARCH=amd64 $(GOBUILD) $(LDFLAGS) -o $(BINARY_DIR)/$(BINARY_NAME)-windows-amd64.exe $(BINARY_PATH)

# Run tests
.PHONY: test
test:
	$(GOTEST) -v -race -coverprofile=coverage.out ./...

# Run tests with coverage report
.PHONY: test-coverage
test-coverage: test
	$(GOCMD) tool cover -html=coverage.out -o coverage.html

# Run linting
.PHONY: lint
lint: fmt vet

# Format code
.PHONY: fmt
fmt:
	$(GOFMT) -s -w .

# Run go vet
.PHONY: vet
vet:
	$(GOVET) ./...

# Install staticcheck if needed and run it
.PHONY: staticcheck
staticcheck:
	@which staticcheck > /dev/null || go install honnef.co/go/tools/cmd/staticcheck@latest
	staticcheck ./...

# Tidy dependencies
.PHONY: tidy
tidy:
	$(GOMOD) tidy

# Download dependencies
.PHONY: deps
deps:
	$(GOMOD) download

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf $(BINARY_DIR)
	rm -f coverage.out
	rm -f coverage.html

# Install the binary
.PHONY: install
install: build
	cp $(BINARY_DIR)/$(BINARY_NAME) $(GOPATH)/bin/

# Run the application (for development)
.PHONY: run
run: build
	./$(BINARY_DIR)/$(BINARY_NAME) $(ARGS)

# Show help
.PHONY: help
help:
	@echo "pgedge-loadgen Makefile"
	@echo ""
	@echo "Usage:"
	@echo "  make              Build after running lint and tests"
	@echo "  make build        Build the binary"
	@echo "  make build-all    Build for all platforms"
	@echo "  make test         Run tests"
	@echo "  make test-coverage Run tests with coverage report"
	@echo "  make lint         Run linting (fmt + vet)"
	@echo "  make fmt          Format code with gofmt"
	@echo "  make vet          Run go vet"
	@echo "  make staticcheck  Run staticcheck"
	@echo "  make tidy         Tidy go.mod"
	@echo "  make deps         Download dependencies"
	@echo "  make clean        Remove build artifacts"
	@echo "  make install      Install binary to GOPATH/bin"
	@echo "  make run ARGS='<args>' Run the application"
	@echo "  make help         Show this help"
