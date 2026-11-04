# The p99 tax of fan-out

Lab for the Beyond CRUD post [The p99 tax: why fan-out makes everything slow](https://beyondcrud.dev/system-design/the-p99-tax-of-fan-out/).

One user request fans out to N backend calls and has to wait for the slowest one. Each backend call is usually fast, but about 1 in 100 is slow. This lab measures what that does to the user, and what hedged requests, retries and deadlines do about it.

## Run it

```bash
make demo   # about 30 s, writes results/
make full   # about 3 minutes, 15 s per run, steadier p99s
```

Needs Go 1.22+. No Docker, no dependencies outside the standard library. Everything runs on 127.0.0.1.

## What is in here

| Piece | What it does |
|---|---|
| `cmd/backend` | One process, 4 ports, each port is a "replica". A normal request sleeps a log-normal time (median 3 ms). 1% of requests hit a spike of 50 to 150 ms, standing in for a GC pause, a noisy neighbour or a cold cache. Spikes are independent per request. Cancelled requests stop early. |
| `cmd/aggregator` | `GET /fanout?n=N&mode=...` sends N calls in parallel and waits for all of them. Modes: `none`, `retry` (timeout then try another replica), `hedge` (send a backup to another replica after a delay, first answer wins, loser cancelled), `deadline` (answer with whatever arrived in time). |
| `cmd/loadgen` | Open-loop load at a fixed rate. Writes every user request to `results/users_*.csv`, every call to `results/calls_*.csv`, one summary line per run to `results/summary.csv`, and prints p50/p95/p99. |
| `run.sh` | Runs N = 1, 10, 50, 100 with no protection, then hedged (backup sent after the backend p95 measured in the N=1 run), then retry and deadline at N=100. |

Knobs: `DUR` and `RATE` env vars for `run.sh`; backend flags `-slow-p`, `-median-ms`, `-slow-min-ms`, `-slow-max-ms`.

## What the columns mean

- `per call`: latency of one logical call as the aggregator saw it. In `none` mode this is the backend latency. In `hedge` mode it is the faster of the two tries.
- `user request`: what the user waited for, measured by the load generator.
- `users that waited on a slow call`: share of user requests where at least one answer came from a spike.
- `extra backend load`: backend requests sent divided by calls needed, minus 1. Cancelled hedges count, because the backend already started on them.

## Machine

Results in `results/` came from:

- Intel Core i5-8257U @ 1.40 GHz, 8 GB RAM, macOS 15.8.2
- go1.22.6 darwin/amd64
- The laptop was shared with other work, so absolute numbers are noisy. The shape is what matters.

## Limits

- Spikes here are random and independent. Real spikes are often correlated (a GC pause slows every request on that host at once), which makes hedging to another replica more important, and hedging to the same host useless.
- Short runs give few user requests, so the user p99 rests on a handful of samples. Use `make full` for steadier numbers.
- Everything is on one machine, so the aggregator's own CPU adds some queueing at N=100.
