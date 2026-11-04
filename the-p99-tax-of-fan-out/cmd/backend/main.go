// backend: a fake service with a realistic latency shape.
//
// Most requests take a few milliseconds (log-normal, median ~3 ms).
// About 1 in 100 requests hits a "spike" of 50-150 ms, standing in for a GC
// pause, a noisy neighbour, a cold cache or a queue behind a slow request.
// Spikes are drawn independently per request.
//
// One process listens on several ports. Each port is a "replica".
package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"
)

func main() {
	ports := flag.String("ports", "9001,9002,9003,9004", "comma-separated ports, one per replica")
	median := flag.Float64("median-ms", 3, "median latency of a normal request")
	sigma := flag.Float64("sigma", 0.4, "log-normal spread of normal requests")
	slowP := flag.Float64("slow-p", 0.01, "probability a request hits a spike")
	slowMin := flag.Float64("slow-min-ms", 50, "shortest spike")
	slowMax := flag.Float64("slow-max-ms", 150, "longest spike")
	flag.Parse()

	for _, p := range strings.Split(*ports, ",") {
		p := strings.TrimSpace(p)
		mux := http.NewServeMux()
		mux.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
			ms := *median * math.Exp(*sigma*rand.NormFloat64())
			slow := rand.Float64() < *slowP
			if slow {
				ms = *slowMin + rand.Float64()*(*slowMax-*slowMin)
			}
			t := time.NewTimer(time.Duration(ms * float64(time.Millisecond)))
			select {
			case <-t.C:
			case <-r.Context().Done(): // caller gave up (hedge won elsewhere)
				t.Stop()
				return
			}
			s := "0"
			if slow {
				s = "1"
			}
			w.Header().Set("X-Slow", s)
			fmt.Fprint(w, "ok")
		})
		srv := &http.Server{Addr: "127.0.0.1:" + p, Handler: mux}
		go func() { log.Fatal(srv.ListenAndServe()) }()
	}
	log.Printf("backend replicas on %s (slow-p=%.3f)", *ports, *slowP)
	select {}
}
