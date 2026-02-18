# Makefile for Backup Service

# Backup-specific variables
BACKUP_IMAGE_NAME ?= menta2l/backup-service
VERSION ?= 1.0.0
BACKUP_IMAGE_TAG ?= $(VERSION)
DOCKER_REGISTRY ?=

LDFLAGS ?= -X main.version=$(VERSION)
GOFLAGS ?=

# Build the server binary
.PHONY: build-server
build-server:
	@echo "Building Backup server..."
	@go build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o ./bin/backup-server ./cmd/server

# Build Docker image for Backup service
.PHONY: docker
docker:
	@echo "Building Docker image $(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG)..."
	@docker build \
		-t $(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG) \
		-t $(BACKUP_IMAGE_NAME):latest \
		--build-arg APP_VERSION=$(VERSION) \
		-f ./Dockerfile \
		.

# Build Docker image with custom registry
.PHONY: docker-tag
docker-tag: docker
ifdef DOCKER_REGISTRY
	@echo "Tagging image for registry $(DOCKER_REGISTRY)..."
	@docker tag $(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG) $(DOCKER_REGISTRY)/$(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG)
	@docker tag $(BACKUP_IMAGE_NAME):latest $(DOCKER_REGISTRY)/$(BACKUP_IMAGE_NAME):latest
endif

# Push Docker image to registry
.PHONY: docker-push
docker-push: docker-tag
ifdef DOCKER_REGISTRY
	@echo "Pushing image to $(DOCKER_REGISTRY)..."
	@docker push $(DOCKER_REGISTRY)/$(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG)
	@docker push $(DOCKER_REGISTRY)/$(BACKUP_IMAGE_NAME):latest
else
	@echo "Pushing image to Docker Hub..."
	@docker push $(BACKUP_IMAGE_NAME):$(BACKUP_IMAGE_TAG)
	@docker push $(BACKUP_IMAGE_NAME):latest
endif

# Run the server locally
.PHONY: run-server
run-server:
	@go run ./cmd/server -c ./configs

# Generate wire dependencies
.PHONY: wire
wire:
	@cd ./cmd/server && wire

# Run tests
.PHONY: test
test:
	@go test -v ./...

# Run tests with coverage
.PHONY: test-cover
test-cover:
	@go test -v -coverprofile=coverage.out ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

# Clean build artifacts
.PHONY: clean
clean:
	@rm -rf ./bin
	@rm -f coverage.out coverage.html
	@echo "Clean complete!"

# Generate proto code
.PHONY: api
api:
	@buf generate
	@buf build -o cmd/server/assets/descriptor.bin --exclude-source-info

# Generate all (wire + proto)
.PHONY: generate
generate: api wire
	@echo "Generation complete!"
