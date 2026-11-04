// Noisy neighbours lab.
//
// One service, a fixed pool of workers, six tenants. A to E send steady small
// traffic. X sends a little more and, for ten seconds in the middle of the
// run, ten times as much. We run the same traffic through four admission and
// scheduling modes and record per-tenant latency and rejections per second.
//
//	go run . -mode fifo       shared FIFO queue, no limits
//	go run . -mode global     one global token bucket in front of the FIFO queue
//	go run . -mode pertenant  one token bucket per tenant in front of the FIFO queue
//	go run . -mode fair       one queue per tenant, workers take round-robin (DRR)
//	go run . -shuffle         shuffle-sharding overlap maths, no traffic
package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type tenant struct {
	name   string
	rate   float64 // requests per second, normal
	weight int     // fair mode: jobs taken per round
}

var tenants = []tenant{
	{"A", 30, 1}, {"B", 30, 1}, {"C", 30, 1}, {"D", 30, 1}, {"E", 30, 1},
	{"X", 40, 1},
}

const xIndex = 5

var (
	workers     = flag.Int("workers", 8, "worker pool size")
	jobMs       = flag.Int("job-ms", 20, "time each job takes (ms)")
	duration    = flag.Int("seconds", 30, "how long traffic runs")
	burstFrom   = flag.Int("burst-from", 10, "second X starts bursting")
	burstTo     = flag.Int("burst-to", 20, "second X stops bursting")
	burstMul    = flag.Float64("burst-mul", 10, "X's rate multiplier during the burst")
	mode        = flag.String("mode", "fifo", "fifo | global | pertenant | fair")
	globalRate  = flag.Float64("global-rate", 360, "global mode: requests/s allowed in total")
	globalBurst = flag.Float64("global-burst", 40, "global mode: bucket size")
	tenantRate  = flag.Float64("tenant-rate", 100, "pertenant mode: requests/s per tenant")
	tenantBurst = flag.Float64("tenant-burst", 20, "pertenant mode: bucket size per tenant")
	queueCap    = flag.Int("queue-cap", 100, "fair mode: max queued jobs per tenant")
	weightX     = flag.Int("weight-x", 1, "fair mode: X's weight (others are 1)")
	out         = flag.String("out", "", "CSV output path (default results/<mode>.csv)")
	seed        = flag.Uint64("seed", 42, "random seed for arrivals")
	shuffle     = flag.Bool("shuffle", false, "print shuffle-sharding maths and exit")
)

// ---------- token bucket ----------

type bucket struct {
	mu     sync.Mutex
	rate   float64
	size   float64
	tokens float64
	last   time.Time
}

func newBucket(rate, size float64) *bucket {
	return &bucket{rate: rate, size: size, tokens: size, last: time.Now()}
}

func (b *bucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens = math.Min(b.size, b.tokens+now.Sub(b.last).Seconds()*b.rate)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// ---------- queues ----------

type job struct {
	tenant  int
	arrived time.Time
	sec     int
}

// queues is either one shared FIFO (fair=false) or one FIFO per tenant
// drained with deficit round robin (fair=true).
type queues struct {
	mu      sync.Mutex
	cond    *sync.Cond
	q       [][]job
	cap     int
	fair    bool
	weights []int
	rr      int
	taken   int
	n       int
	closed  bool
}

func newQueues(fair bool, capacity int, weights []int) *queues {
	nq := 1
	if fair {
		nq = len(weights)
	}
	qs := &queues{q: make([][]job, nq), cap: capacity, fair: fair, weights: weights}
	qs.cond = sync.NewCond(&qs.mu)
	return qs
}

func (qs *queues) push(j job) bool {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	i := 0
	if qs.fair {
		i = j.tenant
	}
	if len(qs.q[i]) >= qs.cap {
		return false // queue full: 429
	}
	qs.q[i] = append(qs.q[i], j)
	qs.n++
	qs.cond.Signal()
	return true
}

func (qs *queues) pop() (job, bool) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	for qs.n == 0 {
		if qs.closed {
			return job{}, false
		}
		qs.cond.Wait()
	}
	if !qs.fair {
		j := qs.q[0][0]
		qs.q[0] = qs.q[0][1:]
		qs.n--
		return j, true
	}
	for {
		if len(qs.q[qs.rr]) > 0 && qs.taken < qs.weights[qs.rr] {
			j := qs.q[qs.rr][0]
			qs.q[qs.rr] = qs.q[qs.rr][1:]
			qs.n--
			qs.taken++
			return j, true
		}
		qs.rr = (qs.rr + 1) % len(qs.q)
		qs.taken = 0
	}
}

func (qs *queues) close() {
	qs.mu.Lock()
	qs.closed = true
	qs.cond.Broadcast()
	qs.mu.Unlock()
}

// ---------- stats ----------

type cell struct {
	offered, rejected int
	lat               []float64 // ms
}

type stats struct {
	mu    sync.Mutex
	cells [][]cell // [second][tenant]
}

func (s *stats) offer(sec, t int, rejected bool) {
	s.mu.Lock()
	s.cells[sec][t].offered++
	if rejected {
		s.cells[sec][t].rejected++
	}
	s.mu.Unlock()
}

func (s *stats) done(sec, t int, ms float64) {
	s.mu.Lock()
	s.cells[sec][t].lat = append(s.cells[sec][t].lat, ms)
	s.mu.Unlock()
}

func pct(v []float64, p float64) float64 {
	if len(v) == 0 {
		return math.NaN()
	}
	c := append([]float64(nil), v...)
	sort.Float64s(c)
	i := int(math.Ceil(p/100*float64(len(c)))) - 1
	if i < 0 {
		i = 0
	}
	return c[i]
}

// ---------- run ----------

func run() {
	var admit func(t int, now time.Time) bool
	qcap := 1 << 20 // effectively unbounded
	fair := false
	switch *mode {
	case "fifo":
		admit = func(int, time.Time) bool { return true }
	case "global":
		b := newBucket(*globalRate, *globalBurst)
		admit = func(_ int, now time.Time) bool { return b.allow(now) }
	case "pertenant":
		bs := make([]*bucket, len(tenants))
		for i := range bs {
			bs[i] = newBucket(*tenantRate, *tenantBurst)
		}
		admit = func(t int, now time.Time) bool { return bs[t].allow(now) }
	case "fair":
		admit = func(int, time.Time) bool { return true }
		fair = true
		qcap = *queueCap
	default:
		fmt.Fprintln(os.Stderr, "unknown mode", *mode)
		os.Exit(2)
	}

	weights := make([]int, len(tenants))
	for i, t := range tenants {
		weights[i] = t.weight
	}
	weights[xIndex] = *weightX

	qs := newQueues(fair, qcap, weights)
	st := &stats{cells: make([][]cell, *duration)}
	for i := range st.cells {
		st.cells[i] = make([]cell, len(tenants))
	}

	var wg sync.WaitGroup
	for w := 0; w < *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				j, ok := qs.pop()
				if !ok {
					return
				}
				time.Sleep(time.Duration(*jobMs) * time.Millisecond)
				st.done(j.sec, j.tenant, float64(time.Since(j.arrived).Microseconds())/1000)
			}
		}()
	}

	start := time.Now()
	end := start.Add(time.Duration(*duration) * time.Second)
	var gen sync.WaitGroup
	for ti := range tenants {
		gen.Add(1)
		go func(ti int) {
			defer gen.Done()
			rng := rand.New(rand.NewPCG(*seed, uint64(ti)))
			next := start
			for {
				sec := int(next.Sub(start).Seconds())
				rate := tenants[ti].rate
				if ti == xIndex && sec >= *burstFrom && sec < *burstTo {
					rate *= *burstMul
				}
				next = next.Add(time.Duration(rng.ExpFloat64() / rate * float64(time.Second)))
				if !next.Before(end) {
					return
				}
				time.Sleep(time.Until(next))
				now := time.Now()
				sec = int(now.Sub(start).Seconds())
				if sec >= *duration {
					return
				}
				ok := admit(ti, now) && qs.push(job{ti, now, sec})
				st.offer(sec, ti, !ok)
			}
		}(ti)
	}
	gen.Wait()
	qs.close() // workers drain what is left, then exit
	wg.Wait()

	path := *out
	if path == "" {
		path = "results/" + *mode + ".csv"
	}
	writeCSV(path, st)
	summary(st)
}

func writeCSV(path string, st *stats) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	w.Write([]string{"mode", "second", "tenant", "offered", "served", "rejected", "reject_pct", "p50_ms", "p99_ms"})
	for sec, row := range st.cells {
		for ti, c := range row {
			rej := 0.0
			if c.offered > 0 {
				rej = 100 * float64(c.rejected) / float64(c.offered)
			}
			w.Write([]string{*mode, strconv.Itoa(sec), tenants[ti].name,
				strconv.Itoa(c.offered), strconv.Itoa(len(c.lat)), strconv.Itoa(c.rejected),
				f1(rej), f1(pct(c.lat, 50)), f1(pct(c.lat, 99))})
		}
	}
	w.Flush()
}

func f1(v float64) string {
	if math.IsNaN(v) {
		return ""
	}
	return strconv.FormatFloat(v, 'f', 1, 64)
}

func summary(st *stats) {
	type win struct {
		name     string
		from, to int
	}
	wins := []win{{"before", 0, *burstFrom}, {"burst", *burstFrom, *burstTo}, {"after", *burstTo, *duration}}
	fmt.Printf("mode=%s workers=%d job=%dms\n", *mode, *workers, *jobMs)
	fmt.Printf("%-8s %-8s %8s %9s %9s %9s\n", "window", "tenants", "offered", "p50_ms", "p99_ms", "rejected")
	for _, wn := range wins {
		for _, g := range []struct {
			name string
			ids  []int
		}{{"A-E", []int{0, 1, 2, 3, 4}}, {"X", []int{xIndex}}} {
			var lat []float64
			off, rej := 0, 0
			for s := wn.from; s < wn.to; s++ {
				for _, ti := range g.ids {
					c := st.cells[s][ti]
					lat = append(lat, c.lat...)
					off += c.offered
					rej += c.rejected
				}
			}
			fmt.Printf("%-8s %-8s %8d %9s %9s %8s%%\n", wn.name, g.name, off,
				f1(pct(lat, 50)), f1(pct(lat, 99)), f1(100*float64(rej)/math.Max(1, float64(off))))
		}
	}
	fmt.Println()
}

// ---------- shuffle sharding ----------

func choose(n, k int) float64 {
	r := 1.0
	for i := 0; i < k; i++ {
		r = r * float64(n-i) / float64(i+1)
	}
	return r
}

func shuffleMaths() {
	fmt.Println("Shuffle sharding: each customer gets k of n workers, picked at random.")
	fmt.Println("For one other customer, how many of your k workers do they also have?")
	fmt.Println()
	for _, c := range [][2]int{{8, 2}, {8, 4}, {100, 5}, {2048, 4}} {
		n, k := c[0], c[1]
		total := choose(n, k)
		fmt.Printf("n=%d k=%d  shards=%.0f\n", n, k, total)
		for j := 0; j <= k; j++ {
			p := choose(k, j) * choose(n-k, k-j) / total
			fmt.Printf("  overlap %d of %d: %.6g%%\n", j, k, 100*p)
		}
		fmt.Println()
	}
}

func main() {
	flag.Parse()
	if *shuffle {
		shuffleMaths()
		return
	}
	run()
}
