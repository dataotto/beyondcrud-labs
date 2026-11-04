.PHONY: build demo full clean

build:
	go vet ./...
	go build -o bin/ ./cmd/...

# about 30 s, light enough for a shared laptop
demo: build
	./run.sh

# longer runs for steadier p99s (about 3 minutes, each run 15 s)
full: build
	DUR=15s RATE=40 ./run.sh

clean:
	rm -rf bin results/*.csv results/demo-output.txt
