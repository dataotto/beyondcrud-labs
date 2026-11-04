// loadgen: sends user requests to the aggregator at a fixed rate (open loop),
// records every user request and every backend call to CSV, prints a summary.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type userRow struct {
	id                    int
	ms                    float64
	sent, missing, slowed int
	calls                 []string
}

func pct(xs []float64, p float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	s := append([]float64(nil), xs...)
	sort.Float64s(s)
	i := int(p/100*float64(len(s))+0.5) - 1
	if i < 0 {
		i = 0
	}
	if i >= len(s) {
		i = len(s) - 1
	}
	return s[i]
}

func main() {
	target := flag.String("target", "http://127.0.0.1:9000", "aggregator")
	n := flag.Int("n", 10, "fan-out")
	mode := flag.String("mode", "none", "none|retry|hedge|deadline")
	after := flag.Float64("after-ms", 0, "hedge delay / retry timeout / deadline in ms")
	rate := flag.Int("rate", 40, "user requests per second")
	dur := flag.Duration("dur", 20*time.Second, "how long to send")
	warm := flag.Int("warmup", 10, "requests to send and ignore first")
	out := flag.String("out", "results", "output dir")
	summary := flag.String("summary", "results/summary.csv", "append one line here")
	flag.Parse()

	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{MaxIdleConnsPerHost: 200}}
	url := fmt.Sprintf("%s/fanout?n=%d&mode=%s&after_ms=%g", *target, *n, *mode, *after)

	do := func(id int) (userRow, error) {
		t0 := time.Now()
		resp, err := client.Get(url)
		if err != nil {
			return userRow{}, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		ms := float64(time.Since(t0).Microseconds()) / 1000
		lines := strings.Split(strings.TrimSpace(string(body)), "\n")
		r := userRow{id: id, ms: ms, calls: lines[1:]}
		fmt.Sscanf(lines[0], "sent=%d missing=%d slow=%d", &r.sent, &r.missing, &r.slowed)
		return r, nil
	}

	for i := 0; i < *warm; i++ { // warm connections, not recorded
		do(-1)
	}

	total := int(dur.Seconds() * float64(*rate))
	rows := make([]userRow, 0, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	tick := time.NewTicker(time.Second / time.Duration(*rate))
	for i := 0; i < total; i++ {
		<-tick.C
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			r, err := do(id)
			if err != nil {
				log.Printf("request %d: %v", id, err)
				return
			}
			mu.Lock()
			rows = append(rows, r)
			mu.Unlock()
		}(i)
	}
	tick.Stop()
	wg.Wait()
	sort.Slice(rows, func(a, b int) bool { return rows[a].id < rows[b].id })

	tag := fmt.Sprintf("n%03d_%s", *n, *mode)
	uf, _ := os.Create(filepath.Join(*out, "users_"+tag+".csv"))
	cf, _ := os.Create(filepath.Join(*out, "calls_"+tag+".csv"))
	uw, cw := bufio.NewWriter(uf), bufio.NewWriter(cf)
	fmt.Fprintln(uw, "request_id,user_ms,backend_requests,missing,slow_calls")
	fmt.Fprintln(cw, "request_id,call_ms,slow,ok")

	var user, backend []float64
	var sent, logical, missing, partial, hitSlow int
	for _, r := range rows {
		fmt.Fprintf(uw, "%d,%.3f,%d,%d,%d\n", r.id, r.ms, r.sent, r.missing, r.slowed)
		user = append(user, r.ms)
		sent += r.sent
		logical += *n
		missing += r.missing
		if r.missing > 0 {
			partial++
		}
		if r.slowed > 0 {
			hitSlow++
		}
		for _, c := range r.calls {
			fmt.Fprintf(cw, "%d,%s\n", r.id, c)
			if f := strings.SplitN(c, ",", 2); len(f) > 0 {
				if v, err := strconv.ParseFloat(f[0], 64); err == nil {
					backend = append(backend, v)
				}
			}
		}
	}
	uw.Flush()
	cw.Flush()
	uf.Close()
	cf.Close()

	extra := 100 * float64(sent-logical) / float64(logical)
	fmt.Printf("N=%-3d mode=%-8s after=%-5g users=%d calls=%d\n", *n, *mode, *after, len(rows), len(backend))
	fmt.Printf("  per call      p50=%7.2fms  p95=%7.2fms  p99=%7.2fms  p99.9=%7.2fms\n",
		pct(backend, 50), pct(backend, 95), pct(backend, 99), pct(backend, 99.9))
	fmt.Printf("  user request  p50=%7.2fms  p95=%7.2fms  p99=%7.2fms  max=%7.2fms\n",
		pct(user, 50), pct(user, 95), pct(user, 99), pct(user, 100))
	fmt.Printf("  users that waited on a slow call: %.1f%%   partial answers: %.1f%%   extra backend load: %+.1f%%\n",
		100*float64(hitSlow)/float64(len(rows)), 100*float64(partial)/float64(len(rows)), extra)

	newFile := false
	if _, err := os.Stat(*summary); os.IsNotExist(err) {
		newFile = true
	}
	sf, _ := os.OpenFile(*summary, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if newFile {
		fmt.Fprintln(sf, "n,mode,after_ms,users,backend_p50,backend_p95,backend_p99,user_p50,user_p95,user_p99,user_max,pct_users_slow,pct_partial,extra_load_pct")
	}
	fmt.Fprintf(sf, "%d,%s,%g,%d,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.1f,%.1f,%.1f\n", *n, *mode, *after, len(rows),
		pct(backend, 50), pct(backend, 95), pct(backend, 99), pct(user, 50), pct(user, 95), pct(user, 99), pct(user, 100),
		100*float64(hitSlow)/float64(len(rows)), 100*float64(partial)/float64(len(rows)), extra)
	sf.Close()
}
