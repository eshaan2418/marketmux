# Performance log

All measurements: Apple M5 Pro laptop, Go 1.27.3, in-process loopback unless noted. Raw JSON for
every number is under `bench/results/` (directory named in each section).

## 1. Fan-out: per-client encoding and one message per update (baseline)

`bench/results/fanout-baseline-20261101-232221/`, conflate policy, 5,000 consolidated updates/s
(synthetic source, ~66× the live aggregate rate):

| clients | delivered msgs/s needed | delivered msgs/s | p50 latency | p99 |
|---|---|---|---|---|
| 100 | 498k | 467k | 4.6 ms | 100 ms |
| 1000 | 4.98M | 376k | 560 ms | 7.7 s |

Each client JSON-encoded every update itself, and each update was its own WebSocket message.

## 2. Batched writes + encode once per shard

`bench/results/fanout-opt-20261101-232638/`:

- **Batching**: all updates ready for a client are written as one WebSocket message (a JSON
  array). For a conflating client, that is the latest value of every dirty symbol.
- **Encode once**: the shard worker encodes each update once and shares the immutable bytes with
  all of its clients, instead of every client encoding it.

| clients | config | delivered msgs/s | p50 | p99 |
|---|---|---|---|---|
| 100 | batching only | 488k | 0.88 ms | 23 ms |
| 100 | batching + encode once | 490k | 0.94 ms | 27 ms |
| 1000 | batching only | 636k | 498 ms | 5.2 s |
| 1000 | batching + encode once | 531k | 508 ms | 7.9 s |

Batching is the large win at 100 clients: p50 dropped **4.9×** and p99 4×. Encode-once makes no
measurable difference at this scale, so encoding was not the bottleneck. Both 1000-client
configurations are still saturated.

## 3. Profiling the saturated case

A CPU profile of 1000 clients at 5,000 updates/s (`mmbench -cpuprofile`) shows **72% of samples
in `syscall.rawsyscalln`**, almost all from `internal/poll.(*FD).Write`. Fan-out at this rate is
bound by one `write(2)` per client per batch on the macOS loopback stack, not by Go code:
`fanout.(*Client).offer` is 5% and the hub workers 6%. Two consequences:

1. Lighter benchmark clients that scan three fields instead of fully parsing JSON did not change
   the result (`-full-parse` is kept as an option). The client side was not the limit either.
2. The fix is fewer syscalls per client: **throttled conflation**. `/ws?interval=10ms` gives each
   client at most one write per interval, carrying the latest state of every changed symbol, which
   bounds the syscall rate at clients/interval. Production systems get the same effect from
   multicast or kernel-bypass transports for co-located consumers.

## 4. Fan-out matrix after the changes

`bench/results/fanout-20261101-235809/SUMMARY.md` and README §Performance. Measuring stalled
consumers needed two benchmark fixes. (1) A server-side `SO_SNDBUF` (`server.BufferedListener`).
(2) Counting the policy server-side (`Hub.Totals`), because macOS auto-tunes the loopback
*receive* buffer up to 4 MB regardless of `SO_RCVBUF`. A merely slow reader therefore buffers tens
of seconds of updates in the kernel and the server never sees back-pressure. Stalled clients
(never read) do trigger every policy, and the fast clients' latency is unchanged.

## 4b. Allocation removal on the engine path

`go build -gcflags=-m` showed three per-frame heap escapes:

| source | fix | effect (`bench/results/micro.txt`) |
|---|---|---|
| a 40-byte stack buffer passed to fastjson's non-inlined `MarshalTo` (Kraken numbers) | decoder-owned scratch buffer | Kraken decode 192 B/op → 0 |
| the `Update` passed through the `Publisher` interface | engine-owned reusable `Update`; publishers must copy | 398 B/op → 16 |
| method value `e.onEvent` created per `Handle` call | bind once at construction | 16 B/op → 0 |

`TestEngineHotPathAllocFree` locks in 0 allocations per frame.

## 4c. Determinism bug found by the test suite

`TestReplayDeterminism` became flaky after gap handling went connection-wide: `failVenue` and
`invalidateVenue` iterated a Go map, whose order is randomised, so after a gap the per-symbol
updates came out in a different order. The live captures had no gaps, which is why live and
replay hashes still matched. Books are now iterated in creation order; 50 repeated runs pass.

## 5. Engine and replay

`bench/results/replay/`: replaying the 30-minute live capture (694,941 frames) through decoders,
books, checksums and consolidation runs at a median of **289k frames/s** over 5 runs of the final
code (range 280-301k; the machine was under unrelated load), against 386 frames/s live (~750× headroom). The output hash was identical
every run. Engine time per frame on replay: p50 1.4 µs, p99 44-47 µs. The p99 and max are dominated by Bitstamp's REST snapshot
frames (~7k levels each) and Kraken checksum recomputation.

## 6. Not done (yet)

- `c.Read` in the WebSocket library still allocates a fresh buffer per received message. A `c.Reader` with pooled
  buffers would remove the main per-frame allocation on the receive path.
- The Bitstamp snapshot could be decoded off the engine goroutine and handed over pre-built, to
  remove the multi-millisecond max in engine time per frame.
