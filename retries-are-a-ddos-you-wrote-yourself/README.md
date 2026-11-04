# Retries are a DDoS you wrote yourself

Lab for the beyondcrud.dev post of the same name. A three-tier HTTP chain on loopback:

```
load generator -> edge -> api -> db-service
```

`db-service` behaves like a small database: 4 workers, 20 ms per request, at most 400
requests waiting or running (more get a fast 503). In the storm scenario its work time
jumps to 500 ms for 3 seconds (capacity drops from about 200 to 8 requests a second).

## Run it

```bash
make demo        # amp + waves + storm, about a minute in total
```

Needs Go 1.22+. No Docker, no dependencies. Outputs land in `results/`.

| scenario | what it shows | output |
|---|---|---|
| `amp` | db fails every request; how many db calls one client request turns into | `results/amp-*.txt` |
| `waves` | 400 requests fail together for 1 s; retry arrivals at db per 10 ms, exponential backoff (100 ms base, 1 s cap, 8 tries, no budget, no breaker) without vs with full jitter | `results/waves-*.txt`, `results/waves-*.csv` |
| `storm` | 20 req/s for 20 s, db slows down at t=6s for 3s; per-tier requests per second | `results/storm-*.txt`, `results/storm-*.csv` |

## The two modes

**naive**: client, edge and api each try 3 times with a fixed 100 ms pause, every layer
uses the same 1 s timeout, downstream calls use `context.Background()` so work keeps going
after the caller gave up, db ignores cancellation, writes have no idempotency key.

**fixed**: only api retries (max 3 tries), retry budget of 10% of requests (token bucket),
exponential backoff with full jitter (25 ms base, 250 ms cap), circuit breaker in api
(opens at 50% failures over 20+ calls in a second, 1 s open, 3 half-open probes), the
client's 1 s deadline is passed down in `X-Deadline-Unix-Ms` and db drops work whose
deadline has passed, writes carry an `Idempotency-Key` that db de-duplicates.

## Notes

- All tiers run in one process on separate loopback ports and talk real HTTP/1.1. One
  process means one clock for the per-second CSV.
- Client sockets use `SO_LINGER 0` so cancelled requests reset instead of piling up in
  TIME_WAIT. Without it a naive storm runs macOS out of ephemeral ports, which is a
  different failure from the one this lab is about.
- A request's attempt number travels as `&a=N` in the query string so db can count
  retries apart from first tries.
- Numbers depend on the machine. Ratios (amplification, success rate) are the point.

## Machine

Intel Core i5-8257U @ 1.40GHz (4 cores / 8 threads), 8 GB RAM, macOS 15.8.2, Go 1.22.6,
shared with other workloads while running. Runs use `GOMAXPROCS=2`.
