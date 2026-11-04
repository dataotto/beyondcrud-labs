# Noisy neighbours: one tenant's spike vs everyone's latency

Lab for the Beyond CRUD post [Noisy neighbours: when one customer's spike takes everyone down](https://beyondcrud.dev/writing/noisy-neighbours-multi-tenant).

One Go process plays a multi-tenant service. It has a fixed pool of 8 workers, and each job takes 20 ms, so it can do about 400 jobs per second.

Six tenants send requests (Poisson arrivals, fixed seed):

| tenant | normal rate | during the burst (second 10 to 20) |
| --- | --- | --- |
| A, B, C, D, E | 30 req/s each | 30 req/s each |
| X | 40 req/s | 400 req/s (10x) |

Normal load is 190 req/s. During the burst it is 550 req/s, which is more than the 400 req/s the pool can do.

The same traffic goes through four modes:

| mode | what it does |
| --- | --- |
| `fifo` | one shared queue, first in first out, no limits |
| `global` | one token bucket for the whole service (360 req/s, burst 40), then the shared queue. Over the limit = 429 |
| `pertenant` | one token bucket per tenant (100 req/s, burst 20), then the shared queue. Over the limit = 429 |
| `fair` | one queue per tenant (max 100 jobs each), workers take one job from each tenant in turn (deficit round robin, weight 1). Full queue = 429 |

Latency is measured from arrival to the end of the job. Each request is counted in the second it arrived.

## Run it

```bash
make demo        # all four modes, about 30 s each (2 min total), then the shuffle-sharding maths
make fair        # one mode
make shuffle     # only the overlap maths, no traffic
go run . -h      # every knob: workers, job time, rates, burst size, limits, X's weight
```

It needs Go 1.22+ and nothing else. Each run uses a few MB of RAM and very little CPU, since the workers only sleep.

## Output

- `results/<mode>.csv` has one row per second per tenant: `mode,second,tenant,offered,served,rejected,reject_pct,p50_ms,p99_ms`
- `results/summary.txt` has p50, p99 and rejection rate for A to E together and for X, before, during and after the burst
- `results/shuffle.txt`: for one other customer, the chance they share 0..k of your k workers, for a few (n, k) pairs

## Results from my run

Machine: MacBook, Intel Core i5-8257U @ 1.40 GHz (4 cores / 8 threads), 8 GB RAM, macOS 15.8.2, Go 1.22.6 darwin/amd64. Other work was running on the machine at the same time, so treat a few ms of jitter as noise.

```
mode=fifo workers=8 job=20ms
window   tenants   offered    p50_ms    p99_ms  rejected
before   A-E          1458      20.3      28.2      0.0%
before   X             434      20.3      25.6      0.0%
burst    A-E          1559    1969.2    4133.6      0.0%
burst    X            4006    1969.8    4117.8      0.0%
after    A-E          1509    1635.1    4127.4      0.0%
after    X             393    1601.6    4109.1      0.0%

mode=global workers=8 job=20ms
window   tenants   offered    p50_ms    p99_ms  rejected
before   A-E          1458      20.2      28.4      0.0%
before   X             434      20.2      26.3      0.0%
burst    A-E          1559      20.5      87.9     34.0%
burst    X            4006      20.4      87.1     35.1%
after    A-E          1509      20.2      24.6      0.1%
after    X             393      20.2      24.4      0.0%

mode=pertenant workers=8 job=20ms
window   tenants   offered    p50_ms    p99_ms  rejected
before   A-E          1458      20.2      27.9      0.0%
before   X             434      20.2      25.5      0.0%
burst    A-E          1559      20.2      31.6      0.0%
burst    X            4006      20.2      42.9     74.7%
after    A-E          1509      20.3      24.6      0.0%
after    X             393      20.3      23.9      0.3%

mode=fair workers=8 job=20ms
window   tenants   offered    p50_ms    p99_ms  rejected
before   A-E          1458      20.3      27.4      0.0%
before   X             434      20.3      28.8      0.0%
burst    A-E          1559      30.1      49.0      0.0%
burst    X            4006     423.6     552.1     38.7%
after    A-E          1509      20.2      35.1      0.0%
after    X             393      20.3     321.6      0.0%
```

## What this lab does not model

- Real CPU work. Jobs sleep, so tenants compete only for worker slots, not for CPU, memory or a database.
- Retries. Rejected clients here just go away. Real clients retry, which adds load unless they honour `Retry-After` and back off with jitter.
- Shuffle sharding is maths only (`-shuffle`). The traffic runs use one pool.
