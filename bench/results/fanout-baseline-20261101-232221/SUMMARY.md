# Fan-out benchmark (fanout-baseline-20261101-232221)

| config | runs | frames/s | updates/s | delivered msgs/s (fast clients) | p50 µs | p99 µs | p99.9 µs | max µs | gaps (fast) | slow: delivered/s, gaps, closed | alloc B/update | shard drops |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| ws_conflate_c1000_r5000 | 3 | 5,000 | 4,980 | 376,302 | 559939.6 | 7688159.2 | 10586423.3 | 12222202 | 1452848 | - | 130794 | 0 |
| ws_conflate_c100_r5000 | 3 | 5,000 | 4,980 | 466,564 | 4603.9 | 100204.5 | 142999.6 | 187564 | 285681 | - | 160361 | 0 |
