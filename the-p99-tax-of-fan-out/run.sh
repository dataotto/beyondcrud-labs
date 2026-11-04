#!/usr/bin/env bash
# Runs the whole experiment against local processes. Light by default:
# DUR=2s RATE=60 keeps `make demo` near 30 s of wall time.
set -euo pipefail
cd "$(dirname "$0")"
DUR=${DUR:-2s}
RATE=${RATE:-60}
OUT=${OUT:-results}
mkdir -p "$OUT"
rm -f "$OUT"/*.csv "$OUT"/demo-output.txt

./bin/backend >/dev/null 2>&1 &
B=$!
./bin/aggregator >/dev/null 2>&1 &
A=$!
trap 'kill $B $A 2>/dev/null || true' EXIT
sleep 1

run() { ./bin/loadgen -rate "$RATE" -dur "$DUR" -out "$OUT" -summary "$OUT/summary.csv" "$@" | tee -a "$OUT/demo-output.txt"; }

echo "# go: $(go version)  machine: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m)  rate=$RATE/s dur=$DUR" | tee "$OUT/demo-output.txt"

echo "## 1. no protection" | tee -a "$OUT/demo-output.txt"
for n in 1 10 50 100; do run -n "$n" -mode none; done

# hedge after the backend p95, measured from the N=1 run
P95=$(awk -F, '$1==1 && $2=="none" {print $6}' "$OUT/summary.csv")
echo "## 2. hedged requests (backup sent after backend p95 = ${P95} ms)" | tee -a "$OUT/demo-output.txt"
for n in 1 10 50 100; do run -n "$n" -mode hedge -after-ms "$P95"; done

echo "## 3. the naive fix and a deadline, N=100" | tee -a "$OUT/demo-output.txt"
run -n 100 -mode retry -after-ms 25
run -n 100 -mode deadline -after-ms 25
