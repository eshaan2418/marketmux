#!/usr/bin/env python3
"""Summarise mmbench JSON runs (median across runs)."""
import glob, json, os, re, statistics, sys
from collections import defaultdict
d = sys.argv[1]
g = defaultdict(list)
for f in sorted(glob.glob(os.path.join(d, "*.run*.json"))):
    g[re.sub(r"\.run\d+\.json$", "", os.path.basename(f))].append(json.load(open(f)))
med = lambda xs: statistics.median(xs)
print(f"# Fan-out benchmark ({os.path.basename(d)})\n")
print("| config | runs | frames/s | updates/s | delivered msgs/s (fast clients) | p50 µs | p99 µs | p99.9 µs | max µs | gaps (fast) | slow: delivered/s, gaps, closed, p50/p99 ms | process alloc B/update | shard drops |")
print("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
for k, rs in g.items():
    lat = lambda q: med([r["fast_clients"]["latency_ns"][q] for r in rs]) / 1e3
    slow = rs[0]["slow"]
    sl = (f'{slow["msgs_per_sec"]:.0f}, {slow["seq_gaps_observed"]}, {slow["closed_by_server"]}, '
          f'{slow["latency_ns"]["p50"]/1e6:.0f}/{slow["latency_ns"]["p99"]/1e6:.0f}') if rs[0]["slow_clients"] else "-"
    print(f'| {k} | {len(rs)} | {med([r["frames_per_sec"] for r in rs]):,.0f} | {med([r["updates_per_sec"] for r in rs]):,.0f} | '
          f'{med([r["fast_clients"]["msgs_per_sec"] for r in rs]):,.0f} | {lat("p50"):.1f} | {lat("p99"):.1f} | {lat("p99_9"):.1f} | {lat("max"):.0f} | '
          f'{max(r["fast_clients"]["seq_gaps_observed"] for r in rs)} | {sl} | {med([r["alloc_bytes_per_update"] for r in rs]):.0f} | {max(r["shard_drops"] for r in rs)} |')

stall = {k: v for k, v in g.items() if v[0].get("slow_clients")}
if stall:
    print("\n## Stalled-consumer runs (server-side policy counters, cumulative)\n")
    print("| config | stalled clients | conflated | dropped | disconnected by policy | write-timeout disconnects | fast clients p50/p99 µs | fast gaps |")
    print("|---|---|---|---|---|---|---|---|")
    for k, rs in stall.items():
        r = rs[0]; t = r.get("hub_totals", {}); f = r["fast_clients"]
        print(f'| {k} | {r["slow_clients"]} | {t.get("Conflated",0):,} | {t.get("Dropped",0):,} | {t.get("Disconnected",0)} | {t.get("WriteTimeouts",0)} | '
              f'{f["latency_ns"]["p50"]/1e3:.0f}/{f["latency_ns"]["p99"]/1e3:.0f} | {f["seq_gaps_observed"]} |')
