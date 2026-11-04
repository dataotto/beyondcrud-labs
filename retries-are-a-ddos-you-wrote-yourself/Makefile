# Each run is one Go process, under 30s, GOMAXPROCS=2 to stay light on a shared laptop.
BIN := bin/retrylab
RUN := GOMAXPROCS=2 ./$(BIN)

.PHONY: demo build amp waves storm clean chartdata

demo: build amp waves storm

build:
	go vet ./...
	go build -o $(BIN) .

# db fails everything: count how many db calls one request becomes (~2s each)
amp: build
	$(RUN) -scenario amp -mode naive | tee results/amp-naive.txt
	$(RUN) -scenario amp -mode fixed | tee results/amp-fixed.txt

# 400 requests fail at once for 1s, backoff without and with jitter (~3s each)
waves: build
	$(RUN) -scenario waves -jitter none | tee results/waves-none.txt
	$(RUN) -scenario waves -jitter full | tee results/waves-full.txt

# same load, db 25x slower (20ms -> 500ms) for 3s at t=6s, naive vs fixed (~24s each)
storm: build
	$(RUN) -scenario storm -mode naive | tee results/storm-naive.txt
	$(RUN) -scenario storm -mode fixed | tee results/storm-fixed.txt

clean:
	rm -rf bin results/*

# Print the CSVs as TypeScript arrays for the post's charts (src/components/explainers/retries.tsx)
chartdata:
	@for m in naive fixed; do echo "// storm-$$m: [second, client_requests, edge_rps, api_rps, db_rps]"; \
	  awk -F, 'NR>1 {printf "[%s,%s,%s,%s,%s],", $$1,$$2,$$5,$$6,$$7} END {print ""}' results/storm-$$m.csv; done
	@for j in none full; do echo "// waves-$$j: db retry arrivals per 10ms"; \
	  awk -F, 'NR>1 {printf "%s,", $$3} END {print ""}' results/waves-$$j.csv; done
