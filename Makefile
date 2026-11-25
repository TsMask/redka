.PHONY: setup vet build build-deb build-rpm run \
	postgres-start postgres-stop postgres-shell \
	mysql-start mysql-stop mysql-shell

# 默认版本号，打包时可用 VERSION=xxx 覆盖
DEFAULT_VERSION := 1.5.0

has_git := $(shell command -v git 2>/dev/null)

ifdef has_git
build_rev := $(shell git rev-parse --short HEAD 2>/dev/null || echo unknown)
else
build_rev := unknown
endif

# VERSION 可以从命令行覆盖，未指定时用默认版本号
VERSION ?= $(DEFAULT_VERSION)

build_date := $(shell date -u '+%Y-%m-%dT%H:%M:%S')

setup:
	@go mod download

vet:
	@echo "> running vet..."
	@go vet ./...
	@echo "✓ finished vet"

build:
	@echo "> running build (version: $(VERSION))..."
	@mkdir -p build
	@go build -ldflags "-s -w \
		-X github.com/tsmask/redka/config.Version=$(VERSION) \
		-X github.com/tsmask/redka/config.Commit=$(build_rev) \
		-X github.com/tsmask/redka/config.Date=$(build_date)" \
		-trimpath -o build/redka cmd/redka/main.go
	@echo "✓ finished build"

build-deb: build
	@echo "> running build-deb (version: $(VERSION))..."
	@bash scripts/build/build-deb.sh -v "$(VERSION)"
	@echo "✓ finished build-deb"

build-rpm: build
	@echo "> running build-rpm (version: $(VERSION))..."
	@bash scripts/build/build-rpm.sh -v "$(VERSION)"
	@echo "✓ finished build-rpm"

run:
	@chmod +x ./redka && ./redka

postgres-start:
	@echo "> starting postgres..."
	@docker run --rm --detach --name=redka-postgres \
		--env=POSTGRES_DB=redka \
		--env=POSTGRES_USER=redka \
		--env=POSTGRES_PASSWORD=redka \
		--publish=5432:5432 \
		--tmpfs /var/lib/postgresql/data \
		postgres:17-alpine
	@until docker exec redka-postgres \
		pg_isready --username=redka --dbname=redka --quiet --quiet; \
		do sleep 1; done
	@echo "✓ started postgres"

postgres-stop:
	@echo "> stopping postgres..."
	@docker stop redka-postgres
	@echo "✓ stopped postgres"

postgres-shell:
	@docker exec -it redka-postgres psql --username=redka --dbname=redka

mysql-start:
	@echo "> starting mysql..."
	@docker run --rm --detach --name=redka-mysql \
		--env=MYSQL_ROOT_PASSWORD=redka \
		--env=MYSQL_DATABASE=redka \
		--env=MYSQL_USER=redka \
		--env=MYSQL_PASSWORD=redka \
		--publish=3306:3306 \
		--tmpfs /var/lib/mysql \
		mysql:8.0
	@until docker exec redka-mysql \
		mysqladmin ping --silent --host=localhost --user=root --password=redka 2>/dev/null; \
		do sleep 1; done
	@echo "✓ started mysql"

mysql-stop:
	@echo "> stopping mysql..."
	@docker stop redka-mysql
	@echo "✓ stopped mysql"

mysql-shell:
	@docker exec -it redka-mysql mysql --user=redka --password=redka --database=redka

# Example:
# make build VERSION=1.5.2
# make build-deb VERSION=1.5.2
# make build-rpm VERSION=1.5.2
