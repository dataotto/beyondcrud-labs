// Lab: one "instant" ALTER TABLE takes an API down through the Postgres lock queue.
//
// Timeline of one run (seconds from start):
//
//	0   API workers start: steady SELECT / UPDATE on accounts
//	2   "report" session: BEGIN; SELECT count(*) FROM accounts; then sits idle in transaction
//	3   migration: ALTER TABLE accounts ADD COLUMN ... (needs ACCESS EXCLUSIVE)
//	5   snapshot of pg_stat_activity, pg_blocking_pids and pg_locks
//	10  report session commits
//	14  stop, write CSV
//
// -mode naive    the ALTER waits as long as it takes, and every new query queues behind it
// -mode fixed    SET lock_timeout + retry with backoff, so the API never waits longer than lock_timeout
// -mode staging  no traffic, no report session: just time the ALTER on a quiet database
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	dsn         = flag.String("dsn", "postgres://postgres@127.0.0.1:55432/lab?sslmode=disable", "connection string")
	mode        = flag.String("mode", "naive", "naive | fixed | staging")
	workers     = flag.Int("workers", 20, "API workers (one pooled connection each)")
	think       = flag.Duration("think", 10*time.Millisecond, "pause between requests per worker")
	rows        = flag.Int("rows", 10000, "rows in accounts")
	runFor      = flag.Duration("duration", 14*time.Second, "total run time")
	holdFor     = flag.Duration("hold", 8*time.Second, "how long the report session stays idle in transaction")
	lockTimeout = flag.Duration("lock-timeout", 200*time.Millisecond, "lock_timeout for -mode fixed")
	outDir      = flag.String("out", "results", "output directory")
)

const migrationSQL = `ALTER TABLE accounts ADD COLUMN note text NOT NULL DEFAULT ''`

// at most one bucket per second of the run
type bucket struct {
	mu      sync.Mutex
	lat     []time.Duration
	errs    int
	waiting int64 // max API sessions seen waiting on a lock during this second
}

type run struct {
	start   time.Time
	buckets []*bucket
	logf    func(format string, a ...any)
}

func (r *run) sec() int {
	s := int(time.Since(r.start) / time.Second)
	if s >= len(r.buckets) {
		s = len(r.buckets) - 1
	}
	return s
}

func (r *run) record(d time.Duration, err error) {
	b := r.buckets[r.sec()]
	b.mu.Lock()
	if err != nil {
		b.errs++
	} else {
		b.lat = append(b.lat, d)
	}
	b.mu.Unlock()
}

func connCfg(app string) *pgx.ConnConfig {
	c, err := pgx.ParseConfig(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	c.RuntimeParams["application_name"] = app
	return c
}

func connect(ctx context.Context, app string) *pgx.Conn {
	c, err := pgx.ConnectConfig(ctx, connCfg(app))
	if err != nil {
		log.Fatalf("connect %s: %v", app, err)
	}
	return c
}

func setup(ctx context.Context) {
	c := connect(ctx, "setup")
	defer c.Close(ctx)
	_, err := c.Exec(ctx, fmt.Sprintf(`
DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (id int PRIMARY KEY, balance bigint NOT NULL);
INSERT INTO accounts SELECT g, 1000 FROM generate_series(1, %d) g;
ANALYZE accounts;`, *rows))
	if err != nil {
		log.Fatalf("setup: %v", err)
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	ctx := context.Background()
	if err := os.MkdirAll(*outDir, 0o755); err != nil {
		log.Fatal(err)
	}
	setup(ctx)

	logFile, err := os.Create(filepath.Join(*outDir, *mode+"-timeline.log"))
	if err != nil {
		log.Fatal(err)
	}
	defer logFile.Close()

	if *mode == "staging" {
		staging(ctx, logFile)
		return
	}
	if *mode != "naive" && *mode != "fixed" {
		log.Fatalf("unknown -mode %q", *mode)
	}

	n := int(*runFor/time.Second) + 1
	r := &run{start: time.Now(), buckets: make([]*bucket, n)}
	for i := range r.buckets {
		r.buckets[i] = &bucket{}
	}
	var logMu sync.Mutex
	r.logf = func(format string, a ...any) {
		logMu.Lock()
		defer logMu.Unlock()
		line := fmt.Sprintf("t=%6.3fs  %s", time.Since(r.start).Seconds(), fmt.Sprintf(format, a...))
		fmt.Println(line)
		fmt.Fprintln(logFile, line)
	}

	pcfg, err := pgxpool.ParseConfig(*dsn)
	if err != nil {
		log.Fatal(err)
	}
	pcfg.MaxConns = int32(*workers)
	pcfg.MinConns = int32(*workers)
	pcfg.ConnConfig.RuntimeParams["application_name"] = "api"
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	runCtx, cancel := context.WithTimeout(ctx, *runFor)
	defer cancel()
	var wg sync.WaitGroup

	// API workers: 80% reads by primary key, 20% small updates.
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func(seed int64) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(seed))
			for runCtx.Err() == nil {
				id := rng.Intn(*rows) + 1
				t0 := time.Now()
				var err error
				if rng.Intn(5) == 0 {
					_, err = pool.Exec(runCtx, `UPDATE accounts SET balance = balance + 1 WHERE id = $1`, id)
				} else {
					var bal int64
					err = pool.QueryRow(runCtx, `SELECT balance FROM accounts WHERE id = $1`, id).Scan(&bal)
				}
				if runCtx.Err() != nil {
					return // cut off by the end of the run, not a real error
				}
				r.record(time.Since(t0), err)
				time.Sleep(*think)
			}
		}(int64(w + 1))
	}

	// Monitor: every 100 ms, count API sessions waiting on a heavyweight lock.
	wg.Add(1)
	go func() {
		defer wg.Done()
		mon := connect(ctx, "monitor")
		defer mon.Close(ctx)
		tk := time.NewTicker(100 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-runCtx.Done():
				return
			case <-tk.C:
				var cnt int64
				if err := mon.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity
					WHERE application_name = 'api' AND wait_event_type = 'Lock'`).Scan(&cnt); err != nil {
					continue
				}
				b := r.buckets[r.sec()]
				if cnt > atomic.LoadInt64(&b.waiting) {
					atomic.StoreInt64(&b.waiting, cnt)
				}
			}
		}
	}()

	// The slow "report": takes ACCESS SHARE on accounts and then sits idle in transaction.
	wg.Add(1)
	go func() {
		defer wg.Done()
		sleepUntil(r.start.Add(2 * time.Second))
		c := connect(ctx, "report")
		defer c.Close(ctx)
		tx, err := c.Begin(ctx)
		if err != nil {
			log.Fatal(err)
		}
		var cnt int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM accounts`).Scan(&cnt); err != nil {
			log.Fatal(err)
		}
		r.logf("report    BEGIN; SELECT count(*) FROM accounts -> %d rows; now idle in transaction for %s", cnt, *holdFor)
		time.Sleep(*holdFor)
		if err := tx.Commit(ctx); err != nil {
			log.Fatal(err)
		}
		r.logf("report    COMMIT (ACCESS SHARE released)")
	}()

	// The migration.
	var migDone atomic.Bool
	wg.Add(1)
	go func() {
		defer wg.Done()
		sleepUntil(r.start.Add(3 * time.Second))
		c := connect(ctx, "migration")
		defer c.Close(ctx)
		if *mode == "naive" {
			r.logf("migration %s", migrationSQL)
			t0 := time.Now()
			if _, err := c.Exec(ctx, migrationSQL); err != nil {
				log.Fatalf("migration: %v", err)
			}
			r.logf("migration done after %s (waiting for the lock, the ALTER itself is instant)", time.Since(t0).Round(time.Millisecond))
		} else {
			migrateWithRetry(ctx, c, r)
		}
		migDone.Store(true)
	}()

	// Snapshot of the pile-up two seconds after the migration started.
	wg.Add(1)
	go func() {
		defer wg.Done()
		sleepUntil(r.start.Add(5 * time.Second))
		snapshot(ctx, r)
	}()

	wg.Wait()
	writeCSV(r)
}

func migrateWithRetry(ctx context.Context, c *pgx.Conn, r *run) {
	if _, err := c.Exec(ctx, fmt.Sprintf("SET lock_timeout = '%dms'", lockTimeout.Milliseconds())); err != nil {
		log.Fatal(err)
	}
	r.logf("migration SET lock_timeout = '%dms'", lockTimeout.Milliseconds())
	backoff := 250 * time.Millisecond
	t0 := time.Now()
	for attempt := 1; attempt <= 40; attempt++ {
		ta := time.Now()
		_, err := c.Exec(ctx, migrationSQL)
		if err == nil {
			r.logf("migration attempt %d: done in %s. total %s", attempt, time.Since(ta).Round(time.Millisecond), time.Since(t0).Round(time.Millisecond))
			return
		}
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "55P03" { // 55P03 = lock_not_available
			log.Fatalf("migration: %v", err)
		}
		sleep := backoff + time.Duration(rand.Int63n(int64(100*time.Millisecond)))
		r.logf("migration attempt %d: %s (%s). retry in %s", attempt, pgErr.Message, pgErr.Code, sleep.Round(time.Millisecond))
		time.Sleep(sleep)
		backoff *= 2
		if backoff > 2*time.Second {
			backoff = 2 * time.Second
		}
	}
	log.Fatal("migration: gave up after 40 attempts")
}

func snapshot(ctx context.Context, r *run) {
	c := connect(ctx, "snapshot")
	defer c.Close(ctx)
	f, err := os.Create(filepath.Join(*outDir, *mode+"-pg_stat_activity.txt"))
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintf(f, "-- snapshot at t=%.1fs, mode=%s\n\n", time.Since(r.start).Seconds(), *mode)

	q1 := `SELECT pid, application_name AS app, state, coalesce(wait_event_type, '') AS wait,
       pg_blocking_pids(pid)::text AS blocked_by,
       coalesce(round(extract(epoch FROM now() - xact_start) * 1000)::text, '') AS xact_ms,
       left(regexp_replace(query, '\s+', ' ', 'g'), 60) AS query
FROM pg_stat_activity
WHERE datname = current_database() AND application_name IN ('api', 'report', 'migration')
ORDER BY xact_start NULLS LAST, pid`
	fmt.Fprintf(f, "%s;\n\n", q1)
	writeTable(ctx, c, f, q1, 12)

	q2 := `SELECT mode, granted, count(*)
FROM pg_locks
WHERE relation = 'accounts'::regclass
GROUP BY mode, granted
ORDER BY granted DESC, mode`
	fmt.Fprintf(f, "\n%s;\n\n", q2)
	writeTable(ctx, c, f, q2, 0)

	q3 := `SELECT count(*) FILTER (WHERE wait_event_type = 'Lock') AS waiting_on_lock,
       count(*) AS api_sessions
FROM pg_stat_activity WHERE application_name = 'api'`
	fmt.Fprintf(f, "\n%s;\n\n", q3)
	writeTable(ctx, c, f, q3, 0)
	r.logf("snapshot  wrote %s", f.Name())
}

// writeTable prints a query result as an aligned table. limit > 0 keeps the first rows and counts the rest.
func writeTable(ctx context.Context, c *pgx.Conn, f *os.File, q string, limit int) {
	rs, err := c.Query(ctx, q)
	if err != nil {
		fmt.Fprintf(f, "error: %v\n", err)
		return
	}
	defer rs.Close()
	tw := tabwriter.NewWriter(f, 0, 0, 2, ' ', 0)
	var head []string
	for _, fd := range rs.FieldDescriptions() {
		head = append(head, fd.Name)
	}
	fmt.Fprintln(tw, strings.Join(head, "\t"))
	n := 0
	for rs.Next() {
		n++
		if limit > 0 && n > limit {
			continue
		}
		vals, _ := rs.Values()
		cells := make([]string, len(vals))
		for i, v := range vals {
			cells[i] = fmt.Sprint(v)
		}
		fmt.Fprintln(tw, strings.Join(cells, "\t"))
	}
	tw.Flush()
	if limit > 0 && n > limit {
		fmt.Fprintf(f, "... %d more rows (%d total)\n", n-limit, n)
	}
}

func writeCSV(r *run) {
	path := filepath.Join(*outDir, *mode+"-api.csv")
	f, err := os.Create(path)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()
	fmt.Fprintln(f, "second,ok,errors,p50_ms,p99_ms,max_ms,waiting_on_lock")
	var total, worstSec int
	worstOK := -1
	var worstMax time.Duration
	for i, b := range r.buckets {
		if i == len(r.buckets)-1 {
			break // partial last second
		}
		sort.Slice(b.lat, func(a, c int) bool { return b.lat[a] < b.lat[c] })
		ok := len(b.lat)
		total += ok
		if worstOK < 0 || ok < worstOK {
			worstOK, worstSec = ok, i
		}
		var mx time.Duration
		if ok > 0 {
			mx = b.lat[ok-1]
		}
		if mx > worstMax {
			worstMax = mx
		}
		fmt.Fprintf(f, "%d,%d,%d,%.1f,%.1f,%.1f,%d\n", i, ok, b.errs, ms(pct(b.lat, 50)), ms(pct(b.lat, 99)), ms(mx), atomic.LoadInt64(&b.waiting))
	}
	summary := fmt.Sprintf("summary   mode=%s  requests=%d  worst second=t%d with %d ok  slowest request=%s  csv=%s",
		*mode, total, worstSec, worstOK, worstMax.Round(time.Millisecond), path)
	r.logf("%s", summary)
}

func staging(ctx context.Context, logFile *os.File) {
	c := connect(ctx, "migration")
	defer c.Close(ctx)
	t0 := time.Now()
	if _, err := c.Exec(ctx, migrationSQL); err != nil {
		log.Fatal(err)
	}
	line := fmt.Sprintf("staging   %s on %d rows, no traffic: %s", migrationSQL, *rows, time.Since(t0).Round(10*time.Microsecond))
	fmt.Println(line)
	fmt.Fprintln(logFile, line)
}

func pct(s []time.Duration, p int) time.Duration {
	if len(s) == 0 {
		return 0
	}
	i := (len(s)*p + 99) / 100
	if i < 1 {
		i = 1
	}
	return s[i-1]
}

func ms(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }

func sleepUntil(t time.Time) { time.Sleep(time.Until(t)) }
