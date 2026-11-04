#!/usr/bin/env bash
# Starts Postgres and the server, runs the client, saves raw output to results/, stops everything.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p results

cleanup() {
  [[ -f .server.pid ]] && kill "$(cat .server.pid)" 2>/dev/null || true
  rm -f .server.pid
  make -s db-down
}
trap cleanup EXIT

{
  echo "date:     $(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "cpu:      $(sysctl -n machdep.cpu.brand_string 2>/dev/null || grep -m1 'model name' /proc/cpuinfo | cut -d: -f2)"
  echo "ram:      $(( $(sysctl -n hw.memsize 2>/dev/null || echo 0) / 1073741824 )) GB"
  echo "os:       $(sw_vers -productVersion 2>/dev/null || uname -sr)"
  echo "go:       $(go version | awk '{print $3}')"
  echo "postgres: $("$PG_BIN/postgres" --version)"
} > results/machine.txt

export GOMAXPROCS=2   # keep the lab light on a shared 4-core laptop
make -s db-up
./bin/server -addr "$ADDR" -dsn "$DSN" > results/server.log 2>&1 &
echo $! > .server.pid

for _ in $(seq 1 50); do
  curl -sf "http://$ADDR/admin/stats" >/dev/null && break
  sleep 0.1
done

cat results/machine.txt
echo
# shellcheck disable=SC2086
./bin/client -base "http://$ADDR" -csv results/summary.csv $CLIENT_FLAGS | tee results/demo.txt
