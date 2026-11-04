# Exactly-once is a lie: the lab

Companion lab for [Exactly-once is a lie: idempotency is a database design problem](https://beyondcrud.dev/system-design/exactly-once-is-a-lie/).

A small Go payment API with four ways to handle `POST /charges`, and a client that sends the duplicates real clients send. The server fakes a payment provider in-process and counts how many times money actually moved.

| Mode | What it does |
| --- | --- |
| `none` | Charges on every request. |
| `naive` | `SELECT` the idempotency key. If it isn't there, charge, then `INSERT` it. No unique constraint. |
| `correct` | Claims the key with `INSERT ... ON CONFLICT DO NOTHING` on a primary key, stores the response, replays it. Returns `409` while the first request is still running and `422` if the key comes back with a different body. Passes the key on to the provider. |
| `outbox` | `correct` plus a transactional outbox. A relay publishes events to two consumers and "crashes" 20% of the time after publishing but before marking the batch sent. One consumer sends a receipt per delivery. The other dedupes on `event_id`. |

Scenarios, run for every mode:

- **burst**: 500 keys × 20 identical requests released at the same moment (10 keys in flight, so 200 requests at once).
- **timeouts**: 200 keys, one sender each, client timeout 25 ms, provider latency 10–40 ms, up to 3 retries with the same key. The server keeps working after the client hangs up.
- **mismatch**: 50 keys, each reused with a different amount.

## Run it

```bash
brew install postgresql@17   # once
make demo                    # ~30s
make clean                   # stop Postgres, delete the data dir
```

Postgres runs natively on port 55433, with its data dir in `.pgdata/` inside this folder (32 MB shared_buffers, 30 connections). `make demo` stops it again when it finishes. If `pg_ctl` lives somewhere else, run `make demo PG_BIN=/path/to/bin`.

Smaller or bigger runs: `make demo KEYS=200 DUPS=10`, or pass any client flag with `CLIENT_FLAGS="..."` (see `./bin/client -h`).

## Output

`make demo` writes:

- `results/demo.txt`: the client's raw output
- `results/summary.csv`: one row per mode × scenario
- `results/machine.txt`: CPU, RAM, OS, Go and Postgres versions
- `results/server.log`: server log
- `results/make-demo.log`, `results/time.txt`: full `make demo` output and `/usr/bin/time -l` of the run

"Duplicate charges" means the number of times the fake provider moved money, minus the number of distinct idempotency keys it moved money for.

## Machine

Results in `results/` and in the post come from one run on: Intel Core i5-8257U @ 1.40GHz (4 cores), 8 GB RAM, macOS 15.8.2, Go 1.22.6, PostgreSQL 17.11 (Homebrew). `scripts/demo.sh` sets `GOMAXPROCS=2` for the server and client. The client finished all four modes in 20.6 s (23.4 s wall clock for `make demo`, including build and Postgres start/stop).

| mode | burst dupes (of 9,500 possible) | timeout dupes | mismatch |
| --- | --- | --- | --- |
| none | 9500 | 176 | charged again (50) |
| naive | 9474 | 48 | replayed old response (50) |
| correct | 0 | 0 | 422 (50) |
| outbox | 0 | 0 | 422 (50) |

Outbox, burst: 500 events published 613 times (34 relay crashes). Naive consumer sent 613 receipts, dedupe consumer 500.

## Files

- `cmd/server/schema.sql`: tables for every mode (recreated on server start)
- `cmd/server/main.go`: the four handlers, fake provider, outbox relay, consumers
- `cmd/client/main.go`: load generator and report
