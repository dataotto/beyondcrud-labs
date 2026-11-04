# Native Postgres 17 (no Docker). Data dir lives inside this folder; port 55432.
# Override the binaries with: make demo PG_BIN=/path/to/postgresql@17/bin
PG_BIN ?= $(shell brew --prefix postgresql@17 2>/dev/null)/bin
PORT   ?= 55432
PGDATA := $(CURDIR)/pgdata
BIN    := $(CURDIR)/bin/lab
DSN    := postgres://postgres@127.0.0.1:$(PORT)/lab?sslmode=disable

.PHONY: demo build pg-up pg-down staging naive fixed clean

demo: build pg-up
	@echo "== staging: no traffic ==";            $(BIN) -dsn '$(DSN)' -mode staging
	@echo; echo "== naive: plain ALTER ==";       $(BIN) -dsn '$(DSN)' -mode naive
	@echo; echo "== fixed: lock_timeout + retry =="; $(BIN) -dsn '$(DSN)' -mode fixed
	@$(MAKE) --no-print-directory pg-down
	@echo; echo "raw output in results/"

build:
	@mkdir -p bin && go build -o $(BIN) .

# Small, light server: 64 MB shared_buffers, 50 connections, no fsync (throwaway lab data).
pg-up:
	@test -x "$(PG_BIN)/pg_ctl" || { echo "pg_ctl not found in $(PG_BIN). brew install postgresql@17 or set PG_BIN"; exit 1; }
	@mkdir -p results; test -d $(PGDATA) || $(PG_BIN)/initdb -D $(PGDATA) -U postgres --auth=trust >/dev/null
	@$(PG_BIN)/pg_ctl -D $(PGDATA) -l $(PGDATA)/server.log -w \
	  -o "-p $(PORT) -k /tmp -c listen_addresses=127.0.0.1 -c shared_buffers=64MB -c max_connections=50 -c fsync=off -c synchronous_commit=off" start >/dev/null
	@$(PG_BIN)/psql -h 127.0.0.1 -p $(PORT) -U postgres -tAc "SELECT 1 FROM pg_database WHERE datname='lab'" | grep -q 1 || \
	  $(PG_BIN)/createdb -h 127.0.0.1 -p $(PORT) -U postgres lab
	@$(PG_BIN)/psql -h 127.0.0.1 -p $(PORT) -U postgres -tAc "SELECT version()" > results/version.txt

pg-down:
	@$(PG_BIN)/pg_ctl -D $(PGDATA) -m fast stop >/dev/null 2>&1 || true

staging naive fixed: build pg-up
	$(BIN) -dsn '$(DSN)' -mode $@ $(ARGS)
	@$(MAKE) --no-print-directory pg-down

clean: pg-down
	rm -rf $(PGDATA) bin
