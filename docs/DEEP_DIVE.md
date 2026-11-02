# marketmux: deep-dive interview questions

### 1. Walk me through a Kraken book update from socket to client.

The session's reader goroutine reads one WebSocket message and stamps it with the monotonic
receive time. It wraps it as `Frame{FrameWS, Kraken, recvNs, payload}`, sends it on the bounded
frame channel, and the capture writer also records it. The engine goroutine dequeues it and
`KrakenDecoder` parses it with a reused `fastjson.Parser`. Prices and quantities are read from the
JSON number text straight into `model.Fixed`, and the result is an `Event{KindDelta, "BTC-USD",
bids, asks, checksum}` backed by reusable slices. The engine applies the levels to the
(Kraken, BTC-USD) book, truncates to depth 10, computes Kraken's CRC32 over the top 10 per side
and compares it with the venue's. On a match it updates Kraken's top for the symbol. If the top
changed, it recomputes the consolidated BBO over all valid venues, increments the symbol's `Seq`,
folds the update into the output hash and publishes. The hub copies the update into each shard's
inbox. Each shard worker offers it to its clients, conflating per symbol or queueing. A client's
writer goroutine encodes JSON with an append encoder and writes the WebSocket frame.

### 2. Why fixed-point and not float64?

Three reasons. (1) Kraken's checksum is computed over the decimal strings: a float such as
77825.899999 formats differently and the checksum fails. (2) Equality across venues: two venues
quoting 0.1 + 0.2 must compare equal, and prices are map keys and sort keys. (3) Accumulated sizes
must not drift. `ParseFixed` reads the decimal text directly, rejects excess precision instead of
silently truncating, and is fuzzed for round-trip exactness.

### 3. Kraken sends prices as JSON numbers. How do you avoid float parsing?

`fastjson` keeps a number's original text. `fixedOf` calls `MarshalTo`, which re-emits that raw
text, and parses it with `ParseFixed`. Exponent forms (`1e-05`) are handled exactly.

### 4. What exactly does the Kraken checksum protect against, and what must the client do?

It detects any divergence in the top 10 levels per side after each update: a missed or
misapplied update, wrong truncation, or wrong decimal handling. The client must keep exactly the
subscribed depth, because Kraken does not send deletes for levels pushed out of the top N, and it
must format with the pair's precision from `AssetPairs`. On mismatch, marketmux marks the book
DEAD (removed from the consolidated view), counts the failure and unsubscribes and resubscribes
for a fresh snapshot, rate-limited to once per 5 s per symbol. In the 30-minute live session all
259,164 checksums matched.

### 5. Explain Bitstamp snapshot/diff bridging and its failure modes.

Diffs are buffered from the moment of subscription. The REST snapshot carries a microtimestamp.
Buffered diffs at or before it are discarded and later ones applied in order. Failure modes:

- the snapshot is older than the first buffered diff (a gap in coverage): undetectable with
  timestamps alone, so the session waits 1 s after subscribing before fetching, to make overlap
  likely;
- a diff older than the last applied one (reordering): treated as inconsistent, resync;
- the snapshot never arrives: the buffer is bounded at 10k diffs, overflow forces a resync;
- duplicate timestamps: ignored.

With sequence numbers instead of timestamps (Binance, most exchange recovery feeds) the first
case becomes detectable: the first applied diff must satisfy first ≤ snapshot+1 ≤ last.

### 6. Coinbase and Gemini give you no checksum or sequence on the book channel. How do you detect divergence?

By self-consistency: a correct replica of a single venue's book can never be crossed, because the
venue would have matched those orders. A crossed own book means a missed delete or a misapplied
update, so marketmux resyncs. Heartbeats give liveness: the idle watchdog reconnects after 20-60 s
of silence. That catches the common failures but not every one. A missed size change on a
non-top level is invisible until it matters. I'd add periodic snapshot reconciliation (re-subscribe
every N minutes and diff the new snapshot against the replica) to measure the real error rate.

### 7. Why is a sequence gap a connection-wide event?

A per-connection sequence number orders every message on the connection, across all symbols. A
gap means some message was lost, and from the gap alone you can't know which symbol it belonged
to. So every book carried on that connection is suspect, and the engine invalidates all of them.
`TestSyntheticGapTriggersResync` pins this behaviour. My first implementation tracked sequences
per symbol and the test caught it.

### 8. The consolidated book was crossed 97-99.99% of the time for BTC, ETH, SOL and XRP. Bug?

No. Each venue's own book was never crossed (0 `venue_crossed` resyncs) and all Kraken checksums
matched. The cross comes from Gemini quoting a persistent premium. In the capture, Gemini's mid
sits a median 1.3-2.9 bps above the other venues' mid, and Gemini's trades print 1.3-2.8 bps above
it too (`research/results/venue_premium.json`). A stale replica would show quotes that don't trade;
here trades print at the premium. Crypto venues have no trade-through rule, and a 2 bps cross is
far below taker fees on two legs, so it persists. That is why the consolidated BBO carries a
`Crossed` flag instead of being "fixed".

### 9. Why a single engine goroutine?

Determinism, simplicity and enough throughput. One owner means lock-free books, a consistent
cross-venue view, and a pure function from frame sequence to output, which is what makes
replay == live provable (identical output hash). Throughput: replay processes about 290k frames/s
against a live aggregate of about 390 frames/s. To scale, I'd shard by symbol (one engine per
shard, the consolidated view stays per symbol) rather than introduce locks.

### 10. How do you prove replay equals live?

The engine folds every published update's deterministic fields (symbol, seq, prices, sizes,
venues, cause) into an FNV-1a hash. The live session reported hash 5921621457644666170, and
replaying its capture 5 times produced the same hash every time (`bench/results/replay/`). Frames
include connect/disconnect markers and REST snapshots, so resync paths replay identically too.

### 11. What does the hub do when a client is slow?

It depends on the client's policy:

- **Conflate**: one slot per symbol. Later updates overwrite earlier undelivered ones (counted),
  and the client always gets the latest state.
- **Queue**: a bounded channel. Overflow drops the update, counted, and the client sees a gap in
  `Seq`.
- **Disconnect**: overflow closes the client with a policy-violation status.

In every case `Publish` never blocks: it copies into per-shard inboxes. If an inbox is full, the
update is dropped for that shard and counted. There is also a 5 s write timeout per write against
half-dead TCP peers. `TestPublishNeverBlocks` and `TestSlowClientIsolation` pin these. The fan-out
benchmark runs 10% of clients with a 50 ms sleep per read and measures the fast clients'
latency.

### 12. Why shard the hub instead of one goroutine per client receiving from a channel?

Per-client channels written by the engine would put O(clients) channel operations on the engine
goroutine, and one full channel would force a choice between blocking and dropping on the hot
path. Sharding moves the O(clients) loop off the engine: the engine does W non-blocking channel
sends, and W workers do the per-client offers in parallel.

### 13. Conflation loses data. When is that wrong?

It is wrong for consumers that need every event: trade tape, audit, PnL from fills. For those, use
queue or disconnect, or better, a separate lossless channel backed by the capture (replayable by
sequence). For BBO consumers, conflation is right. A strategy wants the current best price, not a
backlog of stale ones.

### 14. How did you measure latency, and what are the caveats?

Every frame gets `RecvNs` (monotonic) right after `c.Read` returns, which is after TLS decryption
and WebSocket deframing in the library, so kernel and TLS time are excluded. The engine records
receive → dequeue (goroutine hand-off) and frame start → publish. The analytics record receive →
publish per update. The fan-out benchmark clients run in-process on the same clock and record
injection → client receive over loopback WebSocket, so that figure includes JSON encode, the write
syscall, the loopback TCP stack, the client read and the JSON parse. Caveats: a laptop, Go GC and
scheduler, no core pinning, and the WAN latency to the venues (by far the dominant term in
practice) isn't measured.

### 15. Where are the allocations?

The engine path (decode, book, consolidate, publish) is allocation-free: `TestEngineHotPathAllocFree`
and 0 allocs/op on every decoder benchmark. Getting there took `-gcflags=-m`, which showed three
escapes: a stack buffer passed to a non-inlined fastjson method, the `Update` escaping through the
`Publisher` interface, and a method value created per call (`docs/PERF_LOG.md` §4b). The remaining
allocations are off the engine:

- the WebSocket library's per-message read buffer (`c.Read`; `c.Reader` with pooled buffers would
  fix it);
- Bitstamp's pre-snapshot diff copies;
- the per-shard encoded bytes of each update, deliberately: one allocation per update per shard
  instead of one per client.

### 16. What breaks at 10× venues or symbols?

- The engine at ~3 µs/frame (replay) handles ~300k frames/s. Extrapolating the live rate
  linearly (386 frames/s for 20 venue-symbols) gives 100 symbols × 10 venues ≈ 20k frames/s:
  fine on one core, but at about 15× less headroom it is the point where sharding by symbol
  becomes worth doing.
- Fan-out cost grows as updates × clients: 1000 clients × 10k updates/s means 10M JSON encodes/s.
  Fixes: encode once per update and share the bytes across clients (today each client encodes),
  binary encoding, and multicast to co-located consumers.
- The Bitstamp REST snapshot of the full book (~7k levels) costs milliseconds to decode, which
  shows as the max engine time per frame. Fetching snapshots off the engine goroutine and handing
  over a pre-decoded book would remove that spike.

### 17. How would you take this to production?

- One process per region near the venues' matching engines (AWS ap-northeast-1 for some, us-east
  for others).
- A sequence-numbered internal multicast or Aeron bus instead of WebSocket for internal consumers.
- Per-venue snapshot reconciliation.
- Alerting on resync rate and stale venues: a venue with no update for X seconds is flagged and
  excluded from the consolidated view.
- Persisted captures for TCA and backtests.
- Authentication and entitlements on the client API.

### 18. Why not protoc for gRPC?

protoc wasn't available in this toolchain. The hand-registered `ServiceDesc` with a fixed-layout
codec is about 150 lines, fuzzed, and avoids reflection. In a team setting I'd use protobuf for
schema evolution and cross-language clients. The trade-off is explicit in the code.

### 19. How do you keep the venues happy (rate limits, ToS)?

Public market-data channels only, no keys, no order entry. One connection per venue. Exponential
backoff with jitter on reconnect. Resyncs rate-limited per symbol. Bitstamp REST snapshots fetched
sequentially and only on (re)connect or resync. Live sessions bounded (30 min) with a capped
capture. Captured data is not redistributed.

### 20. What would a backtester or SOR do with this?

The consolidated BBO with per-venue tops is the input to a smart order router: route to the venue
with the best price net of fees and expected fill probability. The capture plus deterministic replay
gives a backtester exact historical consolidated state. The per-venue premium and best-quote time
shares are first-order inputs to venue-selection and lead-lag models.
