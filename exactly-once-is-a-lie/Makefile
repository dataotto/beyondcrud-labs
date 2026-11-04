# exactly-once-is-a-lie lab. Native Postgres 17 (Homebrew), no Docker needed.
#   brew install postgresql@17     (once)
#   make demo                      (~30s, ~300 MB RAM peak)

PG_BIN  ?= $(shell brew --prefix postgresql@17 2>/dev/null)/bin
PG_PORT ?= 55433
PGDATA  ?= $(CURDIR)/.pgdata
DSN     ?= postgres://postgres@127.0.0.1:$(PG_PORT)/postgres?sslmode=disable
ADDR    ?= 127.0.0.1:8089

# client knobs (see `./bin/client -h`)
KEYS ?= 500
DUPS ?= 20
CLIENT_FLAGS ?= -keys $(KEYS) -dups $(DUPS)

export PG_BIN PG_PORT PGDATA DSN ADDR CLIENT_FLAGS

.PHONY: demo build db-up db-down clean vet

demo: build
	@./scripts/demo.sh

build:
	@mkdir -p bin
	go build -o bin/server ./cmd/server
	go build -o bin/client ./cmd/client

vet:
	go vet ./...

db-up:
	@test -x "$(PG_BIN)/pg_ctl" || { echo "pg_ctl not found in $(PG_BIN). brew install postgresql@17, or set PG_BIN"; exit 1; }
	@test -f "$(PGDATA)/PG_VERSION" || "$(PG_BIN)/initdb" -D "$(PGDATA)" -U postgres --auth=trust -E UTF8 >/dev/null
	@"$(PG_BIN)/pg_ctl" -D "$(PGDATA)" -l "$(PGDATA)/server.log" -w status >/dev/null 2>&1 || \
	  "$(PG_BIN)/pg_ctl" -D "$(PGDATA)" -l "$(PGDATA)/server.log" -w \
	  -o "-p $(PG_PORT) -c listen_addresses=127.0.0.1 -k '' -c shared_buffers=32MB -c max_connections=30" start

db-down:
	@test -f "$(PGDATA)/postmaster.pid" && "$(PG_BIN)/pg_ctl" -D "$(PGDATA)" -m fast -w stop || true

clean: db-down
	rm -rf bin .pgdata .server.pid
