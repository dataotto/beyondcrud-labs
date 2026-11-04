// retrylab: a three-tier HTTP chain (edge -> api -> db) on loopback, plus an
// open-loop load generator, to measure what retries do to the bottom tier.
//
// Scenarios:
//
//	storm  steady load, a brief slowdown injected into db, per-tier counts per second
//	amp    db fails every request; count how many attempts one request turns into
//	waves  400 requests fail at the same moment; backoff with and without jitter
//
// Modes:
//
//	naive  every layer retries 3 times, fixed 100ms delay, same 1s timeout at every
//	       layer, downstream calls use context.Background() (work keeps running
//	       after the caller gives up), no idempotency keys
//	fixed  only api retries, retry budget (10%), exponential backoff with full
//	       jitter, circuit breaker, deadline propagated in a header and honoured by
//	       db, idempotency keys on writes
//
// All three tiers run in one process on separate loopback ports and talk real
// HTTP/1.1 to each other. One process keeps one clock for the CSV.
package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var (
	mode     = flag.String("mode", "naive", "naive | fixed")
	scenario = flag.String("scenario", "storm", "storm | amp | waves")
	jitter   = flag.String("jitter", "none", "waves only: none | full")
	rate     = flag.Int("rate", 20, "client requests per second (open loop)")
	loadFor  = flag.Duration("load", 20*time.Second, "how long the client sends load")
	drain    = flag.Duration("drain", 3*time.Second, "wait after load stops before writing results")
	slowAt   = flag.Duration("slow-at", 6*time.Second, "storm: when db slows down")
	slowFor  = flag.Duration("slow-for", 3*time.Second, "storm: how long db stays slow")
	slowWork = flag.Duration("slow-work", 500*time.Millisecond, "storm: db work time per request while slow")
	outDir   = flag.String("out", "results", "output directory")
)

// db-service shape: like a database with a small worker pool and a connection limit.
const (
	dbWorkers   = 4
	dbMaxQueue  = 400 // requests waiting or running; beyond this db answers 503 at once
	dbWorkFast  = 20 * time.Millisecond
	timeout     = time.Second // naive: the same at every layer
	naiveTries  = 3
	naiveDelay  = 100 * time.Millisecond
	fixedTries  = 3
	backoffBase = 25 * time.Millisecond
	backoffCap  = 250 * time.Millisecond
	budgetRatio = 0.10
	writeEvery  = 5 // every 5th request is a write
)

var start time.Time

func since() time.Duration { return time.Since(start) }

// ---------- counters ----------

type counter struct {
	width time.Duration
	b     []atomic.Int64
}

func newCounter(width, span time.Duration) *counter {
	return &counter{width: width, b: make([]atomic.Int64, int(span/width)+1)}
}

func (c *counter) inc() {
	i := int(since() / c.width)
	if i >= 0 && i < len(c.b) {
		c.b[i].Add(1)
	}
}

func (c *counter) total() (n int64) {
	for i := range c.b {
		n += c.b[i].Load()
	}
	return
}

// ---------- http plumbing ----------

func newClient() *http.Client {
	d := &net.Dialer{Timeout: time.Second}
	return &http.Client{Transport: &http.Transport{
		// Linger 0: a cancelled request resets the socket instead of leaving it in
		// TIME_WAIT, so a retry storm does not run the laptop out of ports.
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c, err := d.DialContext(ctx, network, addr)
			if tc, ok := c.(*net.TCPConn); ok {
				tc.SetLinger(0)
			}
			return c, err
		},
		MaxIdleConns:        4000,
		MaxIdleConnsPerHost: 4000,
		IdleConnTimeout:     30 * time.Second,
	}}
}

func serve(h http.Handler) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	go http.Serve(ln, h)
	return "http://" + ln.Addr().String()
}

const hdrDeadline = "X-Deadline-Unix-Ms"

func call(ctx context.Context, c *http.Client, method, url, id string, attempt int) int {
	req, _ := http.NewRequestWithContext(ctx, method, url+"?id="+id+"&a="+strconv.Itoa(attempt), nil)
	if *mode == "fixed" {
		if dl, ok := ctx.Deadline(); ok {
			req.Header.Set(hdrDeadline, strconv.FormatInt(dl.UnixMilli(), 10))
		}
		if method == http.MethodPost {
			req.Header.Set("Idempotency-Key", id)
		}
	}
	resp, err := c.Do(req)
	if err != nil {
		return 0 // timeout or connection error
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

// deadline reads the propagated deadline, or falls back to now+timeout.
func deadline(r *http.Request) time.Time {
	if v := r.Header.Get(hdrDeadline); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			return time.UnixMilli(ms)
		}
	}
	return time.Now().Add(timeout)
}

// ---------- db-service ----------

type db struct {
	sem                  chan struct{}
	queued               atomic.Int64
	recv, done, retries  *counter // retries: arrivals with attempt > 0
	wasted, shed, reject atomic.Int64
	failAll              bool
	failUntil            time.Duration
	mu                   sync.Mutex
	applies              map[string]int
	keys                 map[string]bool
}

func (d *db) work() time.Duration {
	t := since()
	if *scenario == "storm" && t >= *slowAt && t < *slowAt+*slowFor {
		return *slowWork
	}
	return dbWorkFast
}

func (d *db) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	d.recv.inc()
	if r.URL.Query().Get("a") != "0" {
		d.retries.inc()
	}
	if d.failAll || since() < d.failUntil {
		w.WriteHeader(500)
		return
	}
	if d.queued.Add(1) > dbMaxQueue {
		d.queued.Add(-1)
		d.reject.Add(1)
		w.WriteHeader(503) // "too many connections"
		return
	}
	defer d.queued.Add(-1)

	id, write := r.URL.Query().Get("id"), r.Method == http.MethodPost
	ctx := r.Context()
	fixed := *mode == "fixed"

	if fixed {
		ctx2, cancel := context.WithDeadline(ctx, deadline(r))
		defer cancel()
		ctx = ctx2
		select { // wait for a worker, but not past the caller's deadline
		case d.sem <- struct{}{}:
		case <-ctx.Done():
			d.shed.Add(1)
			w.WriteHeader(504)
			return
		}
	} else {
		d.sem <- struct{}{} // naive: wait no matter what
	}
	defer func() { <-d.sem }()

	if fixed {
		select {
		case <-time.After(d.work()):
		case <-ctx.Done():
			d.shed.Add(1)
			w.WriteHeader(504)
			return
		}
	} else {
		time.Sleep(d.work())
	}

	if write {
		d.mu.Lock()
		key := r.Header.Get("Idempotency-Key")
		if key == "" || !d.keys[key] {
			d.applies[id]++
			if key != "" {
				d.keys[key] = true
			}
		}
		d.mu.Unlock()
	}
	d.done.inc()
	if r.Context().Err() != nil {
		d.wasted.Add(1) // finished work nobody is waiting for
	}
	w.WriteHeader(200)
}

func (d *db) duplicates() (n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, c := range d.applies {
		if c > 1 {
			n += c - 1
		}
	}
	return
}

// ---------- retry budget and circuit breaker ----------

type budget struct {
	mu     sync.Mutex
	tokens float64
}

func (b *budget) deposit() {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.tokens = min(b.tokens+budgetRatio, 10)
	b.mu.Unlock()
}

func (b *budget) withdraw() bool {
	if b == nil {
		return true // no budget: unlimited retries
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.tokens >= 1 {
		b.tokens--
		return true
	}
	return false
}

type breaker struct {
	mu          sync.Mutex
	win         int64 // current 1s window
	total, fail int
	openUntil   time.Time
	probes      int
	opened      atomic.Int64
	fastFail    atomic.Int64
}

func (b *breaker) allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if now.Before(b.openUntil) {
		b.fastFail.Add(1)
		return false
	}
	if !b.openUntil.IsZero() { // half-open: a few probes only
		if b.probes >= 3 {
			b.fastFail.Add(1)
			return false
		}
		b.probes++
	}
	return true
}

func (b *breaker) record(ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if !b.openUntil.IsZero() && !now.Before(b.openUntil) { // half-open result
		b.probes--
		if ok {
			b.openUntil, b.total, b.fail = time.Time{}, 0, 0
		} else {
			b.openUntil = now.Add(time.Second)
			b.opened.Add(1)
		}
		return
	}
	if w := now.Unix(); w != b.win {
		b.win, b.total, b.fail = w, 0, 0
	}
	b.total++
	if !ok {
		b.fail++
	}
	if b.total >= 20 && b.fail*2 >= b.total {
		b.openUntil = now.Add(time.Second)
		b.probes = 0
		b.opened.Add(1)
	}
}

// ---------- edge and api tiers ----------

type tier struct {
	down    string
	c       *http.Client
	recv    *counter
	retries bool // fixed mode: does this layer retry?
	bud     *budget
	brk     *breaker
	jitter  string // "full" or "none"
	tries   int
	base    time.Duration
	cap     time.Duration
	retried atomic.Int64
}

func (t *tier) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t.recv.inc()
	id := r.URL.Query().Get("id")
	if *mode == "naive" {
		for a := 0; a < naiveTries; a++ {
			if a > 0 {
				t.retried.Add(1)
				time.Sleep(naiveDelay)
			}
			// fresh 1s timeout, detached from the caller: keeps going after the caller left
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			s := call(ctx, t.c, r.Method, t.down, id, a)
			cancel()
			if s == 200 {
				w.WriteHeader(200)
				return
			}
		}
		w.WriteHeader(502)
		return
	}

	dl := deadline(r)
	ctx, cancel := context.WithDeadline(r.Context(), dl)
	defer cancel()
	tries := 1
	if t.retries {
		tries = t.tries
		t.bud.deposit()
	}
	status := 502
	for a := 0; a < tries; a++ {
		if a > 0 {
			if !t.bud.withdraw() {
				break
			}
			d := backoff(a, t.jitter, t.base, t.cap)
			if time.Until(dl) < d+dbWorkFast {
				break // no time left for another try
			}
			select {
			case <-time.After(d):
			case <-ctx.Done():
				w.WriteHeader(504)
				return
			}
			t.retried.Add(1)
		}
		if t.brk != nil && !t.brk.allow() {
			status = 503
			break
		}
		s := call(ctx, t.c, r.Method, t.down, id, a)
		if t.brk != nil {
			t.brk.record(s == 200)
		}
		if s == 200 {
			w.WriteHeader(200)
			return
		}
		if s >= 400 && s < 500 {
			status = s
			break
		}
	}
	w.WriteHeader(status)
}

func backoff(attempt int, j string, base, cap time.Duration) time.Duration {
	d := base << (attempt - 1)
	if d > cap {
		d = cap
	}
	if j == "full" {
		return time.Duration(rand.Int63n(int64(d) + 1))
	}
	return d
}

// ---------- client ----------

type result struct {
	issued time.Duration
	ok     bool
	lat    time.Duration
}

func client(edge string, c *http.Client, attempts *counter, logical int, every time.Duration, out chan<- result) {
	var wg sync.WaitGroup
	tick := time.NewTicker(every)
	defer tick.Stop()
	for i := 0; i < logical; i++ {
		<-tick.C
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "o" + strconv.Itoa(i)
			method := http.MethodGet
			if i%writeEvery == 0 {
				method = http.MethodPost
			}
			t0 := time.Now()
			issued := since()
			tries := 1
			if *mode == "naive" {
				tries = naiveTries
			}
			ok := false
			for a := 0; a < tries && !ok; a++ {
				if a > 0 {
					time.Sleep(naiveDelay)
				}
				attempts.inc()
				ctx, cancel := context.WithTimeout(context.Background(), timeout)
				ok = call(ctx, c, method, edge, id, a) == 200
				cancel()
			}
			out <- result{issued, ok, time.Since(t0)}
		}(i)
	}
	wg.Wait()
	close(out)
}

// ---------- scenarios ----------

func main() {
	flag.Parse()
	os.MkdirAll(*outDir, 0o755)
	switch *scenario {
	case "storm":
		storm()
	case "amp":
		amp()
	case "waves":
		waves()
	default:
		fmt.Println("unknown scenario")
		os.Exit(2)
	}
}

func chain(span, width time.Duration) (*db, *tier, *tier, string) {
	d := &db{sem: make(chan struct{}, dbWorkers), recv: newCounter(width, span), done: newCounter(width, span),
		retries: newCounter(width, span), applies: map[string]int{}, keys: map[string]bool{}}
	dbURL := serve(d)
	api := &tier{down: dbURL, c: newClient(), recv: newCounter(width, span), retries: true,
		bud: &budget{}, brk: &breaker{}, jitter: "full", tries: fixedTries, base: backoffBase, cap: backoffCap}
	apiURL := serve(api)
	edge := &tier{down: apiURL, c: newClient(), recv: newCounter(width, span)}
	edgeURL := serve(edge)
	return d, api, edge, edgeURL
}

func pct(xs []time.Duration, p float64) time.Duration {
	if len(xs) == 0 {
		return 0
	}
	sort.Slice(xs, func(i, j int) bool { return xs[i] < xs[j] })
	return xs[int(float64(len(xs)-1)*p)]
}

func ms(d time.Duration) string { return strconv.FormatInt(d.Milliseconds(), 10) }

func storm() {
	span := *loadFor + *drain + time.Second
	start = time.Now()
	d, api, edge, edgeURL := chain(span, time.Second)
	attempts := newCounter(time.Second, span)
	logical := int(*loadFor/time.Second) * *rate
	out := make(chan result, logical)
	go client(edgeURL, newClient(), attempts, logical, time.Second/time.Duration(*rate), out)

	secs := int(span / time.Second)
	sent, okN := make([]int, secs+1), make([]int, secs+1)
	var lats, okLats []time.Duration
	ok := 0
	for r := range out {
		s := int(r.issued / time.Second)
		sent[s]++
		lats = append(lats, r.lat)
		if r.ok {
			ok++
			okN[s]++
			okLats = append(okLats, r.lat)
		}
	}
	time.Sleep(*drain)

	f, _ := os.Create(filepath.Join(*outDir, "storm-"+*mode+".csv"))
	cw := csv.NewWriter(f)
	cw.Write([]string{"second", "client_requests", "client_ok", "client_attempts", "edge_rps", "api_rps", "db_rps", "db_done"})
	for s := 0; s < secs; s++ {
		cw.Write([]string{strconv.Itoa(s), strconv.Itoa(sent[s]), strconv.Itoa(okN[s]),
			strconv.FormatInt(attempts.b[s].Load(), 10), strconv.FormatInt(edge.recv.b[s].Load(), 10),
			strconv.FormatInt(api.recv.b[s].Load(), 10), strconv.FormatInt(d.recv.b[s].Load(), 10),
			strconv.FormatInt(d.done.b[s].Load(), 10)})
	}
	cw.Flush()
	f.Close()

	// recovery: first second after the slowdown ends with >=99% success for 3 seconds in a row
	end := int((*slowAt + *slowFor) / time.Second)
	recovery := "did not recover before load stopped"
	last := int(*loadFor / time.Second)
	for s := end; s+2 < last; s++ {
		good := true
		for k := s; k < s+3; k++ {
			if sent[k] == 0 || float64(okN[k]) < 0.99*float64(sent[k]) {
				good = false
			}
		}
		if good {
			recovery = fmt.Sprintf("%ds after the slowdown ended", s-end)
			break
		}
	}

	dbTotal := d.recv.total()
	fmt.Printf("mode=%s rate=%d/s load=%s slowdown=%s..%s (db work %s -> %s)\n", *mode, *rate, *loadFor,
		*slowAt, *slowAt+*slowFor, dbWorkFast, *slowWork)
	fmt.Printf("client requests      %d\n", logical)
	fmt.Printf("client attempts      %d\n", attempts.total())
	fmt.Printf("edge received        %d\n", edge.recv.total())
	fmt.Printf("api received         %d\n", api.recv.total())
	fmt.Printf("db received          %d\n", dbTotal)
	fmt.Printf("amplification at db  %.1fx\n", float64(dbTotal)/float64(logical))
	fmt.Printf("success rate         %.1f%%\n", 100*float64(ok)/float64(logical))
	fmt.Printf("latency all   p50 %sms  p99 %sms\n", ms(pct(lats, .5)), ms(pct(lats, .99)))
	fmt.Printf("latency ok    p50 %sms  p99 %sms\n", ms(pct(okLats, .5)), ms(pct(okLats, .99)))
	fmt.Printf("peak db rps          %d\n", peak(d.recv))
	fmt.Printf("db work finished for a caller that had left  %d\n", d.wasted.Load())
	fmt.Printf("db rejected (queue full)                     %d\n", d.reject.Load())
	fmt.Printf("db dropped (deadline passed)                 %d\n", d.shed.Load())
	fmt.Printf("duplicate writes applied                     %d\n", d.duplicates())
	fmt.Printf("circuit breaker opened                       %d times, %d fast fails\n", api.brk.opened.Load(), api.brk.fastFail.Load())
	fmt.Printf("recovery             %s\n", recovery)
}

func peak(c *counter) (p int64) {
	for i := range c.b {
		p = max(p, c.b[i].Load())
	}
	return
}

// amp: db fails every request. How many db attempts does one client request become?
func amp() {
	const n = 20
	span := 30 * time.Second
	start = time.Now()
	d, api, edge, edgeURL := chain(span, time.Second)
	d.failAll = true
	attempts := newCounter(time.Second, span)
	out := make(chan result, n)
	go client(edgeURL, newClient(), attempts, n, 100*time.Millisecond, out)
	for range out {
	}
	time.Sleep(500 * time.Millisecond)
	fmt.Printf("mode=%s, db returns 500 to everything, %d client requests\n", *mode, n)
	fmt.Printf("client attempts  %d\n", attempts.total())
	fmt.Printf("edge received    %d\n", edge.recv.total())
	fmt.Printf("api received     %d\n", api.recv.total())
	fmt.Printf("db received      %d\n", d.recv.total())
	fmt.Printf("db per request   %.1fx\n", float64(d.recv.total())/n)
}

// waves: 400 requests hit api at the same instant, db fails for the first second.
// api retries with exponential backoff (100ms base, 1s cap), with or without jitter,
// no budget and no breaker, so jitter is the only difference. Count db arrivals per 10ms.
func waves() {
	const n = 400
	const failFor = time.Second
	span := 6 * time.Second
	start = time.Now()
	w := 10 * time.Millisecond
	d := &db{sem: make(chan struct{}, 64), recv: newCounter(w, span), done: newCounter(w, span),
		retries: newCounter(w, span), applies: map[string]int{}, keys: map[string]bool{}}
	d.failUntil = failFor
	api := &tier{down: serve(d), c: newClient(), recv: newCounter(w, span), retries: true,
		jitter: *jitter, tries: 8, base: 100 * time.Millisecond, cap: time.Second}
	apiURL := serve(api)
	*mode = "fixed" // the fixed retry loop, minus budget and breaker
	var wg sync.WaitGroup
	var mu sync.Mutex
	var okAt []time.Duration
	c := newClient()
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			if call(ctx, c, http.MethodGet, apiURL, "w"+strconv.Itoa(i), 0) == 200 {
				mu.Lock()
				okAt = append(okAt, since())
				mu.Unlock()
			}
			cancel()
		}(i)
	}
	wg.Wait()
	f, _ := os.Create(filepath.Join(*outDir, "waves-"+*jitter+".csv"))
	cw := csv.NewWriter(f)
	cw.Write([]string{"ms", "db_arrivals", "db_retry_arrivals"})
	last := 0
	for i := range d.recv.b {
		if d.recv.b[i].Load() > 0 {
			last = i
		}
	}
	for i := 0; i <= last; i++ {
		cw.Write([]string{strconv.Itoa(i * 10), strconv.FormatInt(d.recv.b[i].Load(), 10), strconv.FormatInt(d.retries.b[i].Load(), 10)})
	}
	cw.Flush()
	f.Close()
	fmt.Printf("jitter=%s, %d requests at once, db fails for the first %s, backoff 100ms base, 1s cap\n", *jitter, n, failFor)
	fmt.Printf("db received %d (%d retries)\n", d.recv.total(), d.retries.total())
	fmt.Printf("peak retry arrivals in one 10ms slot  %d\n", peak(d.retries))
	fmt.Printf("succeeded %d/%d, p50 done at %sms, last done at %sms\n", len(okAt), n, ms(pct(okAt, .5)), ms(pct(okAt, 1)))
}
