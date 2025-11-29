.PHONY: all build test coverage clean run run-pg run-http install dev fmt vet benchmark integration-test help test-all test-unit test-client concurrent-validate concurrent-validate-short concurrent-validate-long concurrent-clean

# 变量定义
BINARY_NAME=pglitedb-server
BUILD_DIR=bin
GO=go
GOFLAGS=-v
DB_PATH=/tmp/pglitedb
HTTP_PORT=8080
PG_PORT=5666
REGRESS_DIR=/Users/gl/agentwork/postgresql-18.1/src/test/regress
PWD := $(shell pwd)

# 版本信息
VERSION?=0.1.0
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME=$(shell date -u +"%Y-%m-%dT%H:%M:%SZ")
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.GitCommit=$(GIT_COMMIT) -X main.BuildTime=$(BUILD_TIME)"

## help: 显示帮助信息
help:
	@echo "PGLiteDB Makefile 命令:"
	@echo ""
	@echo "构建相关:"
	@echo "  make build          - 编译服务器二进制文件"
	@echo "  make install        - 安装到 GOPATH/bin"
	@echo "  make clean          - 清理构建产物"
	@echo ""
	@echo "开发相关:"
	@echo "  make dev            - 开发模式（格式化+检查+测试）"
	@echo "  make fmt            - 格式化代码"
	@echo "  make vet            - 静态分析"
	@echo ""
	@echo "测试相关:"
	@echo "  make test           - 运行所有单元测试"
	@echo "  make test-verbose   - 运行测试（详细模式）"
	@echo "  make test-unit      - 运行单元测试"
	@echo "  make test-client    - 运行客户端兼容性测试（GORM、TypeScript）"
	@echo "  make test-all       - 运行所有测试（单元、集成、客户端）"
	@echo "  make coverage       - 生成测试覆盖率报告"
	@echo "  make integration-test - 运行集成测试"
	@echo "  make benchmark      - 运行性能测试"
	@echo "  make pgbench        - 运行 pgbench 性能测试"
	@echo "  make concurrent-validate - 运行并发访问验证"
	@echo "  make concurrent-validate-short - 运行短时间并发访问验证"
	@echo "  make concurrent-validate-long - 运行长时间并发访问验证"
	@echo "  make concurrent-clean - 清理并发验证数据"
	@echo ""
	@echo "运行相关:"
	@echo "  make run            - 运行 HTTP 服务器"
	@echo "  make run-http       - 运行 HTTP REST API 服务器"
	@echo "  make run-pg         - 运行 PostgreSQL 协议服务器"
	@echo ""
	@echo "其他:"
	@echo "  make deps           - 下载并整理依赖"
	@echo "  make docker-build   - 构建 Docker 镜像"

## all: 默认目标 - 格式化、检查、测试、构建
all: fmt vet test build

## build: 编译服务器二进制文件
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p $(BUILD_DIR)
	$(GO) build $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY_NAME) ./cmd/server
	@echo "Build complete: $(BUILD_DIR)/$(BINARY_NAME)"

## install: 安装到 GOPATH/bin
install:
	@echo "Installing $(BINARY_NAME)..."
	$(GO) install $(LDFLAGS) ./cmd/server

## clean: 清理构建产物和测试数据
clean:
	@echo "Cleaning..."
	@rm -rf $(BUILD_DIR)
	@rm -f coverage.out coverage.html
	@rm -rf /tmp/pglitedb*
	@rm -rf test_logs
	@echo "Clean complete"

## deps: 下载并整理依赖
deps:
	@echo "Downloading dependencies..."
	$(GO) mod download
	$(GO) mod tidy
	$(GO) mod verify

## fmt: 格式化代码
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...

## vet: 运行 go vet 静态分析
vet:
	@echo "Running go vet..."
	$(GO) vet ./...

## dev: 开发模式 - 格式化、检查、测试
dev: fmt vet test
	@echo "Development checks passed"

## test: 运行所有单元测试
test:
	@echo "Running tests..."
	$(GO) test ./... -short

## test-unit: 运行单元测试
test-unit:
	@echo "Running unit tests..."
	$(GO) test -v ./...

## test-verbose: 运行测试（详细模式）
test-verbose:
	@echo "Running tests (verbose)..."
	$(GO) test ./... -v -short

## coverage: 生成测试覆盖率报告
coverage:
	@echo "Generating coverage report..."
	$(GO) test ./... -coverprofile=coverage.out -covermode=atomic
	$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"
	@echo "Opening coverage report in browser..."
	@which open > /dev/null && open coverage.html || echo "Please open coverage.html manually"

## integration-test: 运行集成测试
integration-test:
	@echo "Running integration tests..."
	@cd examples/integration_test && $(GO) mod tidy && $(GO) test -v
	@echo "Integration tests complete"

## benchmark: 运行性能测试
benchmark:
	@echo "Running benchmark tests..."
	@cd examples/benchmark && $(GO) mod tidy && $(GO) run benchmark.go
	@echo "Benchmark tests complete"

## pgbench: 运行 pgbench 性能测试
pgbench:
	@echo "Running pgbench tests..."
	@(make run-pg > /tmp/pglitedb-pgbench.log 2>&1 &) && sleep 5
	@scripts/run_pgbench.sh
	@echo "pgbench tests complete"

## test-client: 运行客户端兼容性测试（需要启动服务器）
test-client:
	@echo "Running client compatibility tests..."
	@echo "Starting PostgreSQL server on port 5433..."
	@PG_PORT=5433 $(GO) run cmd/server/main.go pg > /tmp/pglitedb-test-server.log 2>&1 & \
	SERVER_PID=$$!; \
	echo "Server PID: $$SERVER_PID"; \
	sleep 3; \
	echo "Running GORM test..."; \
	cd examples/gorm_test && go run main.go; \
	GORM_EXIT=$$?; \
	echo "Running TypeScript test..."; \
	cd ../typescript_test && pnpm test; \
	TS_EXIT=$$?; \
	echo "Stopping server..."; \
	kill $$SERVER_PID 2>/dev/null || true; \
	if [ $$GORM_EXIT -ne 0 ] || [ $$TS_EXIT -ne 0 ]; then \
		echo "Client tests failed"; \
		exit 1; \
	fi

## test-all: 运行所有测试（单元测试、集成测试、客户端测试）
test-all:
	@echo "Running all tests..."
	@bash scripts/run_all_tests.sh

## run: 运行 HTTP REST API 服务器
run: run-http

## run-http: 运行 HTTP REST API 服务器
run-http:
	@echo "Starting HTTP server on port $(HTTP_PORT)..."
	@mkdir -p $(DB_PATH)-http
	PORT=$(HTTP_PORT) $(GO) run ./cmd/server $(DB_PATH)

## run-pg: 运行 PostgreSQL 协议服务器
run-pg:
	@echo "Starting PostgreSQL server on port $(PG_PORT)..."
	@mkdir -p $(DB_PATH)-postgres
	PG_PORT=$(PG_PORT) $(GO) run ./cmd/server $(DB_PATH) pg

## regress_bench: 运行回归测试和性能测试
regress_bench:
	@echo "Starting regression and benchmark tests..."
	@(make run-pg > /tmp/pglitedb-regress.log 2>&1 &) && sleep 10
	@TIMESTAMP=$$(date +%s); \
	echo "Running regression tests..."; \
	scripts/run_regress.sh; \
	echo "Running pgbench tests..."; \
	mkdir -p bench && scripts/run_pgbench.sh | tee $(PWD)/bench/$$TIMESTAMP.json; \
	echo "Benchmark test results saved to bench/$$TIMESTAMP.json"; \
	echo "Tests completed. Results saved to regress/ and bench/ directories."

## run-both: 同时运行 HTTP 和 PostgreSQL 服务器（需要多个终端）
run-both:
	@echo "Start HTTP server in one terminal with: make run-http"
	@echo "Start PostgreSQL server in another terminal with: make run-pg"

## docker-build: 构建 Docker 镜像
docker-build:
	@echo "Building Docker image..."
	docker build -t pglitedb:$(VERSION) .
	docker tag pglitedb:$(VERSION) pglitedb:latest
	@echo "Docker image built: pglitedb:$(VERSION)"

## docker-run-http: 运行 HTTP 服务器 Docker 容器
docker-run-http:
	docker run -p $(HTTP_PORT):8080 -v $(DB_PATH):/data pglitedb:latest

## docker-run-pg: 运行 PostgreSQL 服务器 Docker 容器
docker-run-pg:
	docker run -p $(PG_PORT):5432 -v $(DB_PATH):/data pglitedb:latest pg

## example-embedded: 运行嵌入式客户端示例
example-embedded:
	@echo "Running embedded client example..."
	cd examples/embedded_client && $(GO) run main.go

## example-compat: 运行兼容性测试示例
example-compat:
	@echo "Running compatibility test example..."
	cd examples/compatibility_test && $(GO) mod tidy && $(GO) test -v

## ts-test: 运行 TypeScript 客户端测试（需要服务器运行）
ts-test:
	@echo "Running TypeScript tests..."
	@echo "Make sure PostgreSQL server is running: make run-pg"
	cd examples/typescript_test && npm install && npm test

## watch: 使用 air 进行热重载开发（需要安装 air）
watch:
	@which air > /dev/null || (echo "Please install air: go install github.com/air-verse/air@latest" && exit 1)
	air

## concurrent-validate: Run concurrent access validation with default settings
concurrent-validate:
	go run concurrent_access_validation.go

## concurrent-validate-short: Run concurrent access validation for 1 minute per level
concurrent-validate-short:
	go run concurrent_access_validation.go -duration=1m

## concurrent-validate-long: Run concurrent access validation for 5 minutes per level
concurrent-validate-long:
	go run concurrent_access_validation.go -duration=5m

## concurrent-clean: Clean up concurrent validation data and profiles
concurrent-clean:
	rm -rf /tmp/pglitedb-concurrent-validation*
	rm -rf concurrent_profiles/

## lint: 运行 golangci-lint（需要安装 golangci-lint）
lint:
	@which golangci-lint > /dev/null || (echo "Please install golangci-lint: https://golangci-lint.run/welcome/install/" && exit 1)
	golangci-lint run

## mod-graph: 显示模块依赖图
mod-graph:
	$(GO) mod graph

## mod-why: 解释为什么需要某个包（使用: make mod-why PKG=package-name）
mod-why:
	$(GO) mod why $(PKG)

## version: 显示版本信息
version:
	@echo "Version: $(VERSION)"
	@echo "Git Commit: $(GIT_COMMIT)"
	@echo "Build Time: $(BUILD_TIME)"

## tree: 显示项目目录结构
tree:
	tree -L 3 -I 'node_modules|dist|.git|bin|vendor|coverage.*|*.out'

## init-examples: 初始化所有示例项目的依赖
init-examples:
	@echo "Initializing example dependencies..."
	@cd examples/integration_test && $(GO) mod tidy
	@cd examples/benchmark && $(GO) mod tidy
	@cd examples/compatibility_test && $(GO) mod tidy
	@cd examples/typescript_test && npm install
	@cd examples/pgbench_test && $(GO) mod tidy
	@echo "Example dependencies initialized"

## start-dev: 开发环境快速启动
start-dev: clean deps build
	@echo "Development environment ready"
	@echo "Run 'make run-http' to start HTTP server"
	@echo "Run 'make run-pg' to start PostgreSQL server"