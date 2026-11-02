#!/usr/bin/env bash
# Fan-out benchmark matrix (in-process loopback; run on an idle machine). Raw JSON per run in
# bench/results/fanout-<stamp>/. Live aggregate rate for reference: ~75 consolidated updates/s.
set -euo pipefail
BIN="${BIN:-bin/mmbench}"
OUT="bench/results/fanout-$(date +%Y%m%d-%H%M%S)"; mkdir -p "$OUT"
DUR="${DUR:-12s}"; RUNS="${RUNS:-3}"
TO="gtimeout 600"; command -v gtimeout >/dev/null || TO="timeout 600"
{ date -u +%FT%TZ; sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | grep 'Model name'; go version
  pmset -g therm 2>/dev/null | tr '\n' ' '; echo; memory_pressure 2>/dev/null | tail -1; uptime; } > "$OUT/machine.txt"
run() { local name="$1"; shift
  for i in $(seq 1 "$RUNS"); do echo "[$name] $i/$RUNS" >&2
    $TO "$BIN" -duration "$DUR" -warmup 3s "$@" -out "$OUT/$name.run$i.json" > /dev/null; done; }
# Sustainable loads (delivered msgs/s well under the loopback syscall ceiling).
run ws_conflate_c10_r20000   -clients 10   -policy conflate -rate 20000
run ws_conflate_c100_r2000   -clients 100  -policy conflate -rate 2000
run ws_conflate_c1000_r200   -clients 1000 -policy conflate -rate 200
run ws_queue_c100_r2000      -clients 100  -policy queue -queue 8192 -rate 2000
run grpc_conflate_c100_r2000 -transport grpc -clients 100 -policy conflate -rate 2000
run grpc_conflate_c1000_r200 -transport grpc -clients 1000 -policy conflate -rate 200
# Saturation and throttled conflation.
run ws_conflate_c1000_r5000            -clients 1000 -policy conflate -rate 5000
run ws_conflate_c1000_r5000_int10ms    -clients 1000 -policy conflate -rate 5000 -interval 10ms
# Slow-consumer isolation: 10% of clients stall (connect, never read); hub_totals show the policy.
RUNS=1 run ws_conflate_c1000_r200_slow10   -clients 1000 -policy conflate -rate 200 -slow 0.1 -slow-sleep 0 -duration 20s
RUNS=1 run ws_queue_c1000_r200_slow10      -clients 1000 -policy queue -queue 256 -rate 200 -slow 0.1 -slow-sleep 0 -duration 20s
RUNS=1 run ws_disconnect_c1000_r200_slow10 -clients 1000 -policy disconnect -queue 256 -rate 200 -slow 0.1 -slow-sleep 0 -duration 20s
if [[ -n "${CAPTURE:-}" ]]; then
  run capture_ws_conflate_c100_r2000 -capture "$CAPTURE" -clients 100 -policy conflate -rate 2000
fi
echo "end: $(uptime)" >> "$OUT/machine.txt"
python3 bench/summarize_fanout.py "$OUT" | tee "$OUT/SUMMARY.md"
