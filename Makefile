.PHONY: build

has_git := $(shell command -v git 2>/dev/null)

ifdef has_git
build_rev := $(shell git rev-parse --short HEAD)
git_tag := $(shell git describe --tags --exact-match 2>/dev/null)
else
build_rev := unknown
endif

ifdef git_tag
build_ver := $(git_tag)
else
build_ver := main
endif

build_date := $(shell date -u '+%Y-%m-%dT%H:%M:%S')

setup:
	@go mod download

vet:
	@echo "> running vet..."
	@go vet ./...
	@echo "✓ finished vet"

build:
	@echo "> running build..."
	@CGO_ENABLED=1 go build -ldflags "-s -w -X github.com/tsmask/redka/config.Version=$(build_ver) -X github.com/tsmask/redka/config.Commit=$(build_rev) -X github.com/tsmask/redka/config.Date=$(build_date)" -trimpath -o redka -v main.go
	@echo "✓ finished build"

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
	