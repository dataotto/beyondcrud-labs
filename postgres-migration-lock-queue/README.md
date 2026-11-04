# postgres-migration-lock-queue

Lab for the Beyond CRUD post [A 1 ms migration can take your API down](https://beyondcrud.dev/system-design/postgres-migration-lock-queue/).

An `ALTER TABLE ... ADD COLUMN ... DEFAULT ''` is instant on Postgres 11+. But it needs an ACCESS EXCLUSIVE lock. If one session holds even a weak lock on the table (an open transaction that ran a `SELECT`), the ALTER waits. While it waits, every new query on that table queues behind it. This lab shows that pile-up, then the fix.

## Run it

```bash
brew install postgresql@17   # once; the lab runs its own server on port 55432
make demo                    # about 30 s
make clean                   # stop the server, delete pgdata/ and bin/
```

No Docker. `make demo` starts a small native Postgres 17 (data dir `./pgdata`, port 55432, 64 MB shared_buffers), runs three scenarios, and stops it. Peak memory is around 300 MB.

## What it does

A Go program (`main.go`, pgx v5) plays three parts:

| t (s) | who | what |
|---|---|---|
| 0 | `api` | 20 workers, 80% `SELECT ... WHERE id = $1`, 20% `UPDATE`, 10 ms pause between requests |
| 2 | `report` | `BEGIN; SELECT count(*) FROM accounts;` then idle in transaction for 8 s |
| 3 | `migration` | `ALTER TABLE accounts ADD COLUMN note text NOT NULL DEFAULT ''` |
| 5 | `snapshot` | `pg_stat_activity` + `pg_blocking_pids()` + `pg_locks` saved to results/ |
| 10 | `report` | `COMMIT` |
| 14 | | stop, write per-second CSV |

Modes:

- `staging`: no traffic, no report session. Times the ALTER alone.
- `naive`: the ALTER waits as long as it needs to.
- `fixed`: `SET lock_timeout = '200ms'`, retry on SQLSTATE `55P03` with backoff (250 ms doubling, cap 2 s, plus jitter).

## Output (results/)

| file | what |
|---|---|
| `{naive,fixed}-api.csv` | per second: ok requests, errors, p50/p99/max latency (ms), max API sessions waiting on a lock |
| `{naive,fixed}-pg_stat_activity.txt` | snapshot at t=5 s |
| `{staging,naive,fixed}-timeline.log` | what each session did and when |
| `version.txt` | `SELECT version()` |

Latency is bucketed by the second the request finished, so a request that waited 7 s shows up in the second it was released.

## Machine

Numbers in the post come from: Intel Core i5-8257U @ 1.40 GHz (4 cores), 8 GB RAM, macOS 15.8.2, Go 1.22.6, PostgreSQL 17.11 (Homebrew, x86_64-apple-darwin24.6.0; see `results/version.txt`). The machine was shared with other work, so treat absolute throughput as rough. The shape of the curves is the point.
