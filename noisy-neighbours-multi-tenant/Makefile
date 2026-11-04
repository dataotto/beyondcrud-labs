.PHONY: demo build fifo global pertenant fair shuffle clean

BIN := ./noisy

build:
	go build -o $(BIN) .

# Runs the four modes back to back (about 30s each) and the shuffle-sharding maths.
demo: build
	@mkdir -p results
	@: > results/summary.txt
	@for m in fifo global pertenant fair; do \
		$(BIN) -mode $$m | tee -a results/summary.txt; \
	done
	$(BIN) -shuffle | tee results/shuffle.txt

fifo global pertenant fair: build
	@mkdir -p results
	$(BIN) -mode $@

shuffle: build
	$(BIN) -shuffle

clean:
	rm -f $(BIN)
