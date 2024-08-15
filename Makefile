APP_NAME = bunny
APP_VSN ?= $(shell git rev-parse --short HEAD)

.PHONY: help
help: #: Show this help message
	@echo "$(APP_NAME):$(APP_VSN)"
	@awk '/^[A-Za-z_ -]*:.*#:/ {printf("%c[1;32m%-15s%c[0m", 27, $$1, 27); for(i=3; i<=NF; i++) { printf("%s ", $$i); } printf("\n"); }' Makefile* | sort

CGO_ENABLED ?= 0
GO = CGO_ENABLED=$(CGO_ENABLED) go
GO_BUILD_FLAGS = -ldflags "-X main.Version=${APP_VSN}"

#---------#
# - Dev - #
#---------#

.PHONY: code-check
code-check: #: Run the linter
	golangci-lint run

.PHONY: generate
generate: ## Run generate for non-vendor packages only
	go list ./... | xargs go generate
	go fmt ./fakes/...

#-----------#
# - Build - #
#-----------#

.PHONY: build
build: #: Build the app locally
build: clean 
	$(GO) build $(GO_BUILD_FLAGS) -o ./$(APP_NAME)

.PHONY: clean
clean: #: Clean up build artifacts
clean:
	$(RM) ./$(APP_NAME)

#----------#
# - Test - #
#----------#

TEST_PACKAGES := $(shell go list ./... | grep -v mocks | grep -v fakes )

.PHONY: test
test: #: Run Go unit tests
test:
	GO111MODULE=on $(GO) test $(TEST_PACKAGES) -coverprofile=coverage.out

.PHONY: testv
testv: #: Run Go unit tests verbose
testv:
	GO111MODULE=on $(GO) test -v $(TEST_PACKAGES) -coverprofile=coverage.out

.PHONY: cover
cover: #: Open coverage report in a browser
	go test $(TEST_PACKAGES) -coverprofile=coverage.out && go tool cover -html=coverage.out

#------------#
# - Docker - #
#------------#

.PHONY: run-infra
run-infra: #: Run the infrastructure we use to test/develop the app
	@./ci/scripts/start-deps.sh

.PHONY: stop-infra
stop-infra: #: Stop the infrastructure we started
	@docker-compose --file ./ci/docker-compose.yml stop

