// aggregator: one user request fans out to N backend calls and waits for them.
//
// GET /fanout?n=100&mode=none|retry|hedge|deadline&after_ms=8
//
//	none      wait for every call
//	retry     per-call timeout of after_ms, then cancel and try another replica once
//	hedge     if a call has not answered after after_ms, send a backup to another
//	          replica; first answer wins, the loser is cancelled
//	deadline  wait at most after_ms for everything, answer with whatever came back
//
// The response is a small text report the load generator logs to CSV.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	replicas []string
	client   *http.Client
)

type result struct {
	ms   float64 // latency of this logical call as the aggregator saw it
	slow bool    // the answer that won came from a spike
	ok   bool
}

// one HTTP call to one replica
func call(ctx context.Context, replica string) (bool, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", replica+"/work", nil)
	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.Header.Get("X-Slow") == "1", nil
}

func other(i int) int { return (i + 1 + rand.Intn(len(replicas)-1)) % len(replicas) }

// logical call with the chosen tail-latency strategy; sent counts backend requests
func logical(ctx context.Context, mode string, after time.Duration, sent *int64) result {
	start := time.Now()
	first := rand.Intn(len(replicas))
	since := func() float64 { return float64(time.Since(start).Microseconds()) / 1000 }

	switch mode {
	case "retry":
		atomic.AddInt64(sent, 1)
		c1, cancel := context.WithTimeout(ctx, after)
		slow, err := call(c1, replicas[first])
		cancel()
		if err == nil {
			return result{since(), slow, true}
		}
		atomic.AddInt64(sent, 1)
		slow, err = call(ctx, replicas[other(first)])
		return result{since(), slow, err == nil}

	case "hedge":
		c, cancel := context.WithCancel(ctx)
		defer cancel() // cancels the loser
		type ans struct {
			slow bool
			err  error
		}
		out := make(chan ans, 2)
		launch := func(i int) {
			atomic.AddInt64(sent, 1)
			go func() { s, e := call(c, replicas[i]); out <- ans{s, e} }()
		}
		launch(first)
		timer := time.NewTimer(after)
		defer timer.Stop()
		pending := 1
		hedged := false
		for {
			select {
			case a := <-out:
				pending--
				if a.err == nil {
					return result{since(), a.slow, true}
				}
				if pending == 0 && hedged {
					return result{since(), false, false}
				}
				if !hedged { // first failed fast: hedge now
					hedged = true
					pending++
					launch(other(first))
				}
			case <-timer.C:
				if !hedged {
					hedged = true
					pending++
					launch(other(first))
				}
			}
		}

	default: // none, deadline (deadline is applied by the caller's ctx)
		atomic.AddInt64(sent, 1)
		slow, err := call(ctx, replicas[first])
		return result{since(), slow, err == nil}
	}
}

func fanout(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	n, _ := strconv.Atoi(q.Get("n"))
	if n < 1 {
		n = 1
	}
	mode := q.Get("mode")
	afterMs, _ := strconv.ParseFloat(q.Get("after_ms"), 64)
	after := time.Duration(afterMs * float64(time.Millisecond))

	ctx := r.Context()
	if mode == "deadline" {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, after)
		defer cancel()
	}

	res := make([]result, n)
	var sent int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); res[i] = logical(ctx, mode, after, &sent) }(i)
	}
	wg.Wait()

	// body: sent=<k> missing=<m> slow=<s>\n then one latency per line ("ms,slow,ok")
	missing, slow := 0, 0
	var b strings.Builder
	for _, x := range res {
		if !x.ok {
			missing++
		}
		if x.slow {
			slow++
		}
		fmt.Fprintf(&b, "%.3f,%d,%d\n", x.ms, btoi(x.slow), btoi(x.ok))
	}
	fmt.Fprintf(w, "sent=%d missing=%d slow=%d\n%s", sent, missing, slow, b.String())
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

func main() {
	addr := flag.String("addr", "127.0.0.1:9000", "listen address")
	ports := flag.String("backends", "9001,9002,9003,9004", "backend replica ports")
	flag.Parse()
	for _, p := range strings.Split(*ports, ",") {
		replicas = append(replicas, "http://127.0.0.1:"+strings.TrimSpace(p))
	}
	client = &http.Client{Transport: &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 500,
		IdleConnTimeout:     30 * time.Second,
	}}
	http.HandleFunc("/fanout", fanout)
	log.Printf("aggregator on %s -> %d replicas", *addr, len(replicas))
	log.Fatal(http.ListenAndServe(*addr, nil))
}
