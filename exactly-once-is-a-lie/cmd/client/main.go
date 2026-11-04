// Command client hammers the server with the duplicates real clients produce:
//
//	burst     N identical requests for the same key, fired at the same moment
//	          (double clicks, a load balancer retry, two app instances)
//	timeouts  one sender per key with a short client timeout. When it gives up,
//	          it retries with the same key, while the server is still working.
//	mismatch  a key reused with a different amount (a client bug)
//
// After each scenario it asks the server how many times money actually moved.
package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type stats struct {
	Postgres         string `json:"postgres"`
	ProviderCalls    int64  `json:"provider_calls"`
	ProviderUnique   int64  `json:"provider_unique_requests"`
	ProviderReplayed int64  `json:"provider_replayed"`
	ChargeRows       int64  `json:"charge_rows"`
	ChargeUniqueRefs int64  `json:"charge_unique_requests"`
	OutboxRows       int64  `json:"outbox_rows"`
	OutboxUnsent     int64  `json:"outbox_unsent"`
	Published        int64  `json:"events_published"`
	RelayCrashes     int64  `json:"relay_crashes"`
	ReceiptsNaive    int64  `json:"receipts_naive_consumer"`
	ReceiptsDedupe   int64  `json:"receipts_dedupe_consumer"`
	ConsumerInflight int64  `json:"consumer_inflight"`
}

type counts struct {
	sent, c201, c200, c409, c422, c5xx, timeouts, other atomic.Int64
}

func (c *counts) String() string {
	return fmt.Sprintf("sent=%d new=%d replayed=%d 409=%d 422=%d 5xx=%d client_timeouts=%d other=%d",
		c.sent.Load(), c.c201.Load(), c.c200.Load(), c.c409.Load(), c.c422.Load(), c.c5xx.Load(), c.timeouts.Load(), c.other.Load())
}

var (
	base      = flag.String("base", "http://127.0.0.1:8089", "server base URL")
	modesFlag = flag.String("modes", "none,naive,correct,outbox", "modes to run")
	keys      = flag.Int("keys", 500, "burst: distinct idempotency keys")
	dups      = flag.Int("dups", 20, "burst: identical concurrent requests per key")
	parKeys   = flag.Int("parallel-keys", 10, "burst: keys in flight at once (in-flight requests = parallel-keys x dups)")
	tKeys     = flag.Int("timeout-keys", 200, "timeouts: distinct keys")
	tPar      = flag.Int("timeout-parallel", 20, "timeouts: keys in flight at once")
	tTimeout  = flag.Duration("client-timeout", 25*time.Millisecond, "timeouts: client gives up after this")
	tRetries  = flag.Int("retries", 3, "timeouts: retries after a client timeout")
	mKeys     = flag.Int("mismatch-keys", 50, "mismatch: keys reused with a different amount")
	csvPath   = flag.String("csv", "results/summary.csv", "summary CSV output")
)

// One shared client for every request. Bodies are always drained before Close,
// so connections go back to the idle pool instead of being thrown away.
// Connections that do get closed (client timeouts abort them) close with
// SO_LINGER 0, so they don't sit in TIME_WAIT and use up local ports on macOS.
var httpc = &http.Client{
	Timeout: 5 * time.Second,
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout: 2 * time.Second,
			Control: func(network, address string, c syscall.RawConn) error {
				var serr error
				err := c.Control(func(fd uintptr) {
					serr = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 0})
				})
				if err != nil {
					return err
				}
				return serr
			},
		}).DialContext,
		MaxIdleConns:        1024,
		MaxIdleConnsPerHost: 1024, // >= peak concurrency (parallel-keys x dups)
		IdleConnTimeout:     30 * time.Second,
	},
}

func drain(resp *http.Response) {
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
}

// send posts one charge. On 409 (key in progress) it waits and asks again,
// which is what a well-behaved client does with Stripe-style APIs.
func send(ctx context.Context, mode, key string, amount int, c *counts) error {
	body := fmt.Sprintf(`{"account":"acct_lab","amount_cents":%d}`, amount)
	for attempt := 0; attempt < 50; attempt++ {
		req, _ := http.NewRequestWithContext(ctx, "POST", *base+"/"+mode+"/charges", strings.NewReader(body))
		req.Header.Set("Idempotency-Key", key)
		req.Header.Set("Content-Type", "application/json")
		c.sent.Add(1)
		resp, err := httpc.Do(req)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				c.timeouts.Add(1)
			} else {
				c.other.Add(1)
			}
			return err
		}
		drain(resp)
		switch {
		case resp.Header.Get("Idempotent-Replayed") == "true":
			c.c200.Add(1)
			return nil
		case resp.StatusCode == 201:
			c.c201.Add(1)
			return nil
		case resp.StatusCode == 409:
			c.c409.Add(1)
			time.Sleep(15 * time.Millisecond)
			continue
		case resp.StatusCode == 422:
			c.c422.Add(1)
			return nil
		case resp.StatusCode >= 500:
			c.c5xx.Add(1)
			return nil
		default:
			c.other.Add(1)
			return nil
		}
	}
	return nil
}

func post(path string) {
	resp, err := httpc.Post(*base+path, "application/json", nil)
	if err != nil {
		fatal(err)
	}
	drain(resp)
}

func getStats() stats {
	resp, err := httpc.Get(*base + "/admin/stats")
	if err != nil {
		fatal(err)
	}
	defer drain(resp)
	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		fatal(err)
	}
	return s
}

// settle waits until the server has nothing left in flight (outbox drained, consumers idle).
func settle() stats {
	deadline := time.Now().Add(15 * time.Second)
	var s stats
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		s = getStats()
		if s.OutboxUnsent == 0 && s.ConsumerInflight == 0 {
			// give in-flight server handlers (from timed-out clients) a moment to finish
			time.Sleep(150 * time.Millisecond)
			s2 := getStats()
			if s2.ProviderCalls == s.ProviderCalls && s2.ChargeRows == s.ChargeRows && s2.OutboxUnsent == 0 {
				return s2
			}
		}
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "client:", err)
	os.Exit(1)
}

type row struct {
	mode, scenario string
	logical        int
	c              *counts
	s              stats
	elapsed        time.Duration
}

func (r row) duplicates() int64 { return r.s.ProviderCalls - r.s.ProviderUnique }

func burst(mode string) row {
	post("/admin/reset")
	c := &counts{}
	start := time.Now()
	sem := make(chan struct{}, *parKeys)
	var wg sync.WaitGroup
	for k := 0; k < *keys; k++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(k int) {
			defer func() { <-sem; wg.Done() }()
			key := fmt.Sprintf("%s-burst-%05d", mode, k)
			gate := make(chan struct{})
			var inner sync.WaitGroup
			for d := 0; d < *dups; d++ {
				inner.Add(1)
				go func() {
					defer inner.Done()
					<-gate // release all duplicates at the same instant
					_ = send(context.Background(), mode, key, 1999, c)
				}()
			}
			close(gate)
			inner.Wait()
		}(k)
	}
	wg.Wait()
	el := time.Since(start)
	return row{mode, "burst", *keys, c, settle(), el}
}

func timeouts(mode string) row {
	post("/admin/reset")
	c := &counts{}
	start := time.Now()
	sem := make(chan struct{}, *tPar)
	var wg sync.WaitGroup
	for k := 0; k < *tKeys; k++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(k int) {
			defer func() { <-sem; wg.Done() }()
			key := fmt.Sprintf("%s-timeout-%05d", mode, k)
			for try := 0; try <= *tRetries; try++ {
				timeout := *tTimeout
				if try == *tRetries {
					timeout = 2 * time.Second // last try waits for the answer
				}
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				err := send(ctx, mode, key, 1999, c)
				cancel()
				if err == nil {
					return
				}
				time.Sleep(time.Duration(10*(try+1)) * time.Millisecond) // small backoff
			}
		}(k)
	}
	wg.Wait()
	el := time.Since(start)
	return row{mode, "timeouts", *tKeys, c, settle(), el}
}

func mismatch(mode string) row {
	post("/admin/reset")
	c := &counts{}
	first := &counts{}
	start := time.Now()
	for k := 0; k < *mKeys; k++ {
		key := fmt.Sprintf("%s-mismatch-%05d", mode, k)
		_ = send(context.Background(), mode, key, 1999, first)
		_ = send(context.Background(), mode, key, 2999, c) // same key, different amount
	}
	el := time.Since(start)
	return row{mode, "mismatch", *mKeys, c, settle(), el}
}

func main() {
	flag.Parse()
	modes := strings.Split(*modesFlag, ",")
	s0 := getStats()
	fmt.Printf("exactly-once-is-a-lie lab  go=%s  postgres=%s  GOMAXPROCS=%d\n", runtime.Version(), s0.Postgres, runtime.GOMAXPROCS(0))
	fmt.Printf("burst: %d keys x %d concurrent duplicates (%d keys in flight)\n", *keys, *dups, *parKeys)
	fmt.Printf("timeouts: %d keys, client timeout %s, up to %d retries\n", *tKeys, *tTimeout, *tRetries)
	fmt.Printf("mismatch: %d keys reused with a different amount\n\n", *mKeys)

	var rows []row
	total := time.Now()
	for _, m := range modes {
		for _, f := range []func(string) row{burst, timeouts, mismatch} {
			r := f(m)
			rows = append(rows, r)
			fmt.Printf("[%-7s %-8s] %5.2fs  %s\n", r.mode, r.scenario, r.elapsed.Seconds(), r.c)
			fmt.Printf("                    money moved=%d for %d logical charges -> duplicate charges=%d  (db rows=%d, provider replays=%d)\n",
				r.s.ProviderCalls, r.s.ProviderUnique, r.duplicates(), r.s.ChargeRows, r.s.ProviderReplayed)
			if m == "outbox" && r.s.OutboxRows > 0 {
				fmt.Printf("                    outbox events=%d published=%d (relay crashes=%d) receipts: naive consumer=%d, dedupe consumer=%d\n",
					r.s.OutboxRows, r.s.Published, r.s.RelayCrashes, r.s.ReceiptsNaive, r.s.ReceiptsDedupe)
			}
		}
		fmt.Println()
	}

	fmt.Println("SUMMARY (duplicate charges = times money moved minus logical charges)")
	fmt.Printf("%-8s %-9s %10s %12s %10s %10s %14s %14s\n", "mode", "scenario", "requests", "money_moved", "dupes", "rejected", "receipts_naive", "receipts_dedup")
	for _, r := range rows {
		fmt.Printf("%-8s %-9s %10d %12d %10d %10d %14d %14d\n", r.mode, r.scenario, r.c.sent.Load(), r.s.ProviderCalls, r.duplicates(), r.c.c422.Load(), r.s.ReceiptsNaive, r.s.ReceiptsDedupe)
	}
	fmt.Printf("\ntotal %.1fs\n", time.Since(total).Seconds())

	if *csvPath != "" {
		writeCSV(rows)
	}
}

func writeCSV(rows []row) {
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	_ = w.Write([]string{"mode", "scenario", "logical_charges", "requests_sent", "status_201", "replayed", "status_409", "status_422", "status_5xx", "client_timeouts",
		"money_moved", "duplicate_charges", "db_charge_rows", "provider_replays", "outbox_events", "events_published", "relay_crashes", "receipts_naive", "receipts_dedupe", "seconds"})
	i := func(n int64) string { return strconv.FormatInt(n, 10) }
	for _, r := range rows {
		_ = w.Write([]string{r.mode, r.scenario, strconv.Itoa(r.logical), i(r.c.sent.Load()), i(r.c.c201.Load()), i(r.c.c200.Load()), i(r.c.c409.Load()), i(r.c.c422.Load()), i(r.c.c5xx.Load()), i(r.c.timeouts.Load()),
			i(r.s.ProviderCalls), i(r.duplicates()), i(r.s.ChargeRows), i(r.s.ProviderReplayed), i(r.s.OutboxRows), i(r.s.Published), i(r.s.RelayCrashes), i(r.s.ReceiptsNaive), i(r.s.ReceiptsDedupe), fmt.Sprintf("%.2f", r.elapsed.Seconds())})
	}
	w.Flush()
	if err := os.WriteFile(*csvPath, buf.Bytes(), 0o644); err != nil {
		fatal(err)
	}
}
