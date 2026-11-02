# Market microstructure behind marketmux

## 1. Fragmentation without a consolidated tape

The same instrument trades on many venues at once. In US equities, fragmentation is tied together
by regulation. Reg NMS defines a National Best Bid and Offer (NBBO) computed by the SIPs from
every exchange's top of book, and the Order Protection Rule forbids trading through a better
displayed price elsewhere. A crossed NBBO is rare and short-lived because routers must sweep the
better quotes first.

Crypto spot markets have no such linkage. There is no SIP, no trade-through rule and no
obligation for Kraken to route to Coinbase. Each venue is an independent order book with its own
participants, fee schedule, custody and settlement. A "consolidated BBO" has to be computed by the
consumer, and it can be, and often is, **crossed**: the best bid on venue A is above the best ask
on venue B. That is not an error. The cross persists because capturing it requires:

- taker fees on both legs (roughly 5-60 bps per side depending on volume tier, per the venues'
  public fee schedules);
- pre-positioned inventory on both venues, since there is no instant cross-venue settlement;
- latency between venues, so the quote may be gone by the time an order arrives.

A desk therefore treats the cross-venue book as information (lead-lag, where price discovery
happens, which venue is stale) rather than as free money. marketmux measures how often and by
how much the consolidated book is crossed, and which venue sets the best bid or offer, as
time-weighted shares. See README §Real data.

## 2. L2 feeds and their consistency mechanisms

All four venues publish L2 data (aggregated size per price) as **absolute** updates: "price p now
has total quantity q", where q = 0 deletes the level. Absolute updates are idempotent per level,
so re-applying one is harmless, but a missed update silently leaves a wrong level in the book.
Each venue gives the consumer a different tool to detect that:

| venue | channel(s) | snapshot | integrity mechanism in marketmux |
|---|---|---|---|
| Kraken (WS v2) | `book` depth 10, `trade` | first message after subscribe | **CRC32 checksum** of the top 10 levels per side, sent with every update; verified after each update; mismatch ⇒ unsubscribe/resubscribe for a fresh snapshot |
| Coinbase Exchange | `level2_batch` (served as `level2_50`), `heartbeat`, `matches` | first message | TCP ordering plus snapshot on (re)subscribe; crossed own book ⇒ resubscribe; heartbeats for liveness |
| Bitstamp | `diff_order_book_<pair>`, `live_trades_<pair>` | **REST** `order_book` | snapshot/diff **bridging by microtimestamp**; out-of-order diff or buffer overflow ⇒ fetch a new snapshot |
| Gemini (v2) | `l2` (book + trades) | first `l2_updates` (carries `trades`/`auction_events`) | TCP ordering; crossed own book ⇒ reconnect |

### Kraken checksum
For the top 10 asks (best first) and then the top 10 bids (best first), each price and quantity is
formatted with the pair's precision (`pair_decimals` / `lot_decimals` from REST `AssetPairs`).
The decimal point is removed and leading zeros stripped, the strings are concatenated, and the
CRC32 (IEEE) is taken. Two consequences for the client:

1. It must keep **exactly** the subscribed depth. Levels that fall out of the top 10 must be
   truncated after each update, because Kraken does not send deletes for them.
2. Prices must be handled as exact decimals. A float that prints as `77825.89999` would break the
   checksum. marketmux parses every price from its JSON text into a fixed-point int64 (1e-8 units),
   never via float64.

### Snapshot/diff bridging (Bitstamp)
The diff stream and the REST snapshot are separate channels. The correct procedure:

1. Subscribe to diffs and buffer them.
2. Fetch the REST snapshot, which carries a `microtimestamp`.
3. Discard buffered diffs with timestamp ≤ the snapshot's, then apply the rest in order.
4. From then on, a diff older than the last applied one means the stream is inconsistent: resync.

The same pattern (sequence numbers instead of timestamps) applies to Binance depth streams,
equities "recovery" feeds, and ITCH snapshot plus MoldUDP64 replay.

### Self-consistency for venues without checksums
A single venue's own book can never be crossed in a correct replica, because the venue's matching
engine would have executed the crossing orders. If a Coinbase or Gemini replica shows best bid ≥
best ask, the replica has diverged: resync. This catches missed deletes, the most common failure.

## 3. Sequence numbers and gaps

Exchange feeds with per-connection sequence numbers (ITCH over MoldUDP64, most FIX market-data
sessions, the synthetic venue here) allow exact gap detection: message n+2 after n means n+1 was
lost. The gap invalidates every book carried on that connection, not just the symbol of the late
message, because the lost message could have been for any of them. marketmux applies this rule.

## 4. Slow consumers

A market-data platform's consumers differ by orders of magnitude in speed: a co-located strategy,
a risk engine, a browser UI. Three policies are standard:

- **Conflation**: keep only the latest value per key (symbol). Consumers always see current state,
  memory is bounded by the number of keys, and nobody blocks. Right for BBO and state streams.
- **Bounded queue with drop**: consumers see every update until they fall behind, then miss some.
  The per-symbol `Seq` lets them detect it.
- **Disconnect**: a consumer that cannot keep up is cut off and must reconnect and resnapshot.
  This suits consumers that require completeness (an audit tap) and would rather fail loudly than
  silently.

The one design rule is that **the producer never blocks on a consumer**. One stuck TCP peer must
not delay data to everyone else.

## 5. Timestamps

Each venue timestamps differently: Kraken and Coinbase send RFC 3339 with microseconds, Bitstamp
a microsecond epoch, Gemini milliseconds. Venue clocks are not synchronised with ours. marketmux
therefore stamps every frame with a **local monotonic receive time**. All latency figures and all
time-weighted statistics use that clock. Venue timestamps are carried through for analysis only.
