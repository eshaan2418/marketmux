# marketmux design

## 1. Process structure

```
 venue sessions (1 goroutine each + reader)          engine goroutine                 fan-out
 ┌───────────────┐                                  ┌──────────────────────┐      ┌──────────────┐
 │ kraken  WS v2 │──┐                               │ decoder[venue]       │      │ shard 0..W-1 │
 │ coinbase WS   │──┤  Frame{kind,venue,recvNs,     │ per-venue books      │      │  inbox (cap) │
 │ bitstamp WS+  │──┼─ payload} bounded chan ──────▶│ integrity checks     │─────▶│  clients     │──▶ WS / gRPC
 │   REST snap   │  │  (8192)                       │ consolidated BBO     │      │  (policy)    │    writers
 │ gemini  WS v2 │──┘          │                    │ analytics, hash      │      └──────────────┘
 └───────────────┘             ▼                    └──────────────────────┘
        ▲               capture writer (gzip, raw frames)        │ resync requests
        └────────────────────────────────────────────────────────┘
```

- **Sessions** own I/O only: dial, subscribe, read, reconnect with exponential backoff and jitter
  (1 s doubling to 30 s, reset after a healthy minute), idle watchdog, and per-venue resync actions
  (Kraken/Coinbase resubscribe, Bitstamp REST snapshot, Gemini/synthetic reconnect), rate-limited
  to one per symbol per 5 s. Every observation becomes a `Frame`: WS payloads, REST snapshots, and
  connect/disconnect markers. The connect marker carries metadata such as Kraken precisions.
- **The engine** is a single goroutine that owns every book: no locks on the data path. It decodes,
  applies, validates, consolidates and publishes. Because its input is exactly the frame sequence,
  it is a deterministic function of the capture.
- **Fan-out** is sharded: `Publish` copies the update into each shard's bounded inbox and never
  blocks. Each shard worker offers to its clients under their policy, and each client has its own
  writer goroutine.

## 2. Why one engine goroutine

Per-symbol parallelism is tempting, but the work per frame is a few microseconds of JSON decoding
plus sub-microsecond book work (`bench/results/micro.txt`). The aggregate live rate across four
venues and five symbols is well below 1k frames/s. One goroutine handles more than 100× that
(replay benchmark). A single owner gives determinism (replay = live), lock-free books, trivially
consistent consolidated state across venues, and one output hash. Scaling beyond one core would
shard by symbol, with one engine per shard, not add locks.

## 3. Exact decimals

`model.Fixed` is an int64 with 8 implied decimals. `ParseFixed` reads the decimal text directly:
JSON strings from Coinbase/Bitstamp/Gemini, JSON *numbers* from Kraken, whose raw text fastjson
preserves. It rejects digits beyond the 8th decimal instead of truncating, and accepts exponent
notation. A fuzz test checks round-trip exactness and agreement with float parsing within float
precision. Exactness is required by Kraken's checksum and by cross-venue price equality.

## 4. Books

`book.Book` keeps each side as a slice sorted best-first. An update does a linear probe of the
first 8 levels, then binary search, then an in-place insert or delete. L2 updates concentrate at
the touch, so the probe usually resolves in the first cache line. The book is allocation-free
once at capacity (`TestApplyDeltaNoAllocsAtCapacity`) and differential-tested against a map-based
reference over 600k random updates at three depths, including Kraken-style truncation.

## 5. Integrity state machine (per venue, symbol)

```
            snapshot                    delta (valid)
  ┌──────┐ ─────────▶ ┌──────┐ ◀───────────────────────┐
  │ DEAD │            │ LIVE │ ─────────────────────────┘
  └──────┘ ◀───────── └──────┘
     ▲   checksum fail / seq gap / own book crossed / out-of-order / disconnect
     │   (book cleared, removed from consolidated view, resync requested)
     └── deltas ignored (Bitstamp: buffered up to N, then resync)
```

A DEAD book contributes nothing to the consolidated BBO, so consumers never see a price from a
replica known to be wrong. Every transition is counted by reason (`ResyncByReason`).

## 6. Consolidated BBO

Per symbol, the engine keeps each venue's top `{valid, bid, ask}`. When a venue's top changes, it
recomputes best bid (max) and best ask (min) across valid venues. Ties go to the larger size, then
the lower venue id, which keeps the result deterministic. It then sets `Crossed`, increments the
per-symbol `Seq` and publishes. Recomputation costs O(venues) = 4, so no heap is needed. A property
test checks the result against brute force under random snapshots and disconnects.

## 7. Fan-out and back-pressure

| policy | per-client state | when the client is slow | right for |
|---|---|---|---|
| conflate | map symbol → latest + dirty list + notify chan (cap 1) | intermediate updates overwritten (counted) | BBO/state consumers |
| queue | bounded chan | newest dropped (counted); `Seq` reveals gaps | consumers that tolerate gaps |
| disconnect | bounded chan | connection closed with a policy-violation status | consumers that require completeness |

Writers also have a per-write timeout (5 s), so a half-dead TCP peer is cut off rather than
pinning a goroutine forever. The shard inbox is a second safety valve: if a shard falls behind
entirely, updates for that shard are dropped and counted (`shard_drops`), and the engine still
never blocks.

## 8. Transports
- **WebSocket**: JSON produced by an append-based encoder (no reflection), one message per update.
- **gRPC**: server-streaming `marketmux.v1.MarketData/StreamBBO`. The service descriptor is
  registered by hand and uses a fixed-layout binary codec (`mmbin`) selected via the content
  subtype, so there is no protoc step in the build. Batches carry the updates that were ready
  together. The codec is fuzzed (decode → re-encode → decode).

## 9. Capture and replay
Raw frames are stored, not normalised events: `kind | venue | recv_ns | len | payload` in gzip
files, rotated at 256 MB and capped in total size. Replaying a capture exercises the real decoders,
checksums and resync logic. A truncated tail, e.g. after a crash mid-write, ends the replay cleanly.
`TestReplayDeterminism` and the capture replay benchmark (identical `OutputHash` across runs)
verify determinism.

## 10. Observability
- Prometheus (`/metrics`): engine counters, per-venue session counters, fan-out clients and
  drops, Go runtime and process metrics.
- `/v1/status`: JSON snapshot. `/v1/bbo?symbol=`: latest consolidated update.
- `/debug/pprof/`: CPU, heap and trace profiling.
- Structured logs (`log/slog`).
