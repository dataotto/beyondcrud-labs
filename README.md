# Beyond CRUD labs

Runnable labs behind the [Break it on purpose](https://beyondcrud.dev/series/break-it-on-purpose/) series on beyondcrud.dev. Each folder rebuilds one production failure mode on a laptop: break it, measure it, fix it, measure again. Every number in the posts comes from the `results/` folder next to the code that produced it.

| Lab | Post | Needs |
|---|---|---|
| [retries-are-a-ddos-you-wrote-yourself](retries-are-a-ddos-you-wrote-yourself) | Retries are a DDoS you wrote yourself | Go |
| [the-p99-tax-of-fan-out](the-p99-tax-of-fan-out) | The p99 tax: why fan-out makes everything slow | Go |
| [noisy-neighbours-multi-tenant](noisy-neighbours-multi-tenant) | Noisy neighbours: when one customer's spike takes everyone down | Go |
| [exactly-once-is-a-lie](exactly-once-is-a-lie) | Exactly-once is a lie: idempotency is a database design problem | Go, PostgreSQL 17 |
| [postgres-migration-lock-queue](postgres-migration-lock-queue) | The 4 ms migration that stopped every query on the table | Go, PostgreSQL 17 |

Run any lab with `cd <lab> && make demo`. Each README lists the machine and versions the published numbers came from (Intel i5-8257U, 8 GB RAM, Go 1.22.6, PostgreSQL 17.11). Your numbers will differ; the shape should not.
