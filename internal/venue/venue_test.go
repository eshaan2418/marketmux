package venue

import (
	"errors"
	"hash/crc32"
	"testing"

	"github.com/eshaan2418/marketmux/internal/model"
)

// Fixtures below are hand-written in each venue's documented message shape (no captured
// third-party data is committed).

func table(t testing.TB) *SymbolTable {
	st, err := NewSymbolTable([]string{"BTC-USD", "ETH-USD"})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func collect(t testing.TB, d Decoder, kind FrameKind, payload string) []model.Event {
	var out []model.Event
	err := d.Decode(&Frame{Kind: kind, Venue: d.Venue(), Payload: []byte(payload)}, func(e *model.Event) {
		c := *e
		c.Bids = append([]model.Level(nil), e.Bids...)
		c.Asks = append([]model.Level(nil), e.Asks...)
		out = append(out, c)
	})
	if err != nil && !errors.Is(err, ErrIgnored) {
		t.Fatalf("decode %s: %v", payload, err)
	}
	return out
}

func lv(p, q string) model.Level {
	return model.Level{Price: model.MustFixed(p), Qty: model.MustFixed(q)}
}

func TestKrakenBookAndTrade(t *testing.T) {
	d := NewKrakenDecoder(table(t))
	evs := collect(t, d, FrameWS, `{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":77825.9,"qty":0.41857559}],"asks":[{"price":77826.0,"qty":1.5}],"checksum":123}]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindSnapshot || evs[0].Symbol != "BTC-USD" ||
		evs[0].Bids[0] != lv("77825.9", "0.41857559") || evs[0].Asks[0] != lv("77826", "1.5") ||
		!evs[0].HasCRC || evs[0].Checksum != 123 {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[],"asks":[{"price":77833.0,"qty":0}],"checksum":1352553919,"timestamp":"2026-11-01T21:35:35.212793Z"}]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindDelta || evs[0].Asks[0].Qty != 0 || evs[0].ExchTsUs != 1793568935212793 {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"channel":"trade","type":"update","data":[{"symbol":"ETH/USD","side":"sell","price":2262.5,"qty":0.25,"ord_type":"market","trade_id":1,"timestamp":"2026-11-01T21:35:35Z"}]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindTrade || evs[0].TradeSide != model.Ask || evs[0].TradePx != model.MustFixed("2262.5") {
		t.Fatalf("%+v", evs)
	}
	if evs := collect(t, d, FrameWS, `{"channel":"status","type":"update","data":[]}`); len(evs) != 0 {
		t.Fatal("status should be ignored")
	}
	if evs := collect(t, d, FrameWS, `{"channel":"book","type":"snapshot","data":[{"symbol":"DOGE/USD","bids":[],"asks":[]}]}`); len(evs) != 0 {
		t.Fatal("unknown symbol should be skipped")
	}
}

// The checksum string for a level is price and qty at the pair's precision with the decimal point
// and leading zeros removed, asks (best first) then bids (best first), CRC32-IEEE.
func TestKrakenChecksumConstruction(t *testing.T) {
	p := KrakenPrecision{Price: 1, Qty: 8}
	bids := []model.Level{lv("77825.9", "0.41857559"), lv("77824.2", "0.008")}
	asks := []model.Level{lv("77826", "1.5")}
	sum, s := KrakenChecksum(bids, asks, p, nil)
	want := "778260" + "150000000" + "778259" + "41857559" + "778242" + "800000"
	if string(s) != want {
		t.Fatalf("checksum string\n got %s\nwant %s", s, want)
	}
	if sum != crc32.ChecksumIEEE([]byte(want)) {
		t.Fatal("crc mismatch")
	}
	// Only the top 10 levels per side participate.
	var many []model.Level
	for i := 0; i < 15; i++ {
		many = append(many, model.Level{Price: model.Fixed(int64(100+i) * model.Scale), Qty: model.Scale})
	}
	a, _ := KrakenChecksum(nil, many, p, nil)
	b, _ := KrakenChecksum(nil, many[:10], p, nil)
	if a != b {
		t.Fatal("levels beyond 10 must not affect the checksum")
	}
}

func TestCoinbase(t *testing.T) {
	d := NewCoinbaseDecoder(table(t))
	evs := collect(t, d, FrameWS, `{"type":"snapshot","product_id":"BTC-USD","asks":[["77822.19","0.08262049"]],"bids":[["77820.00","1.00000000"]]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindSnapshot || evs[0].Asks[0] != lv("77822.19", "0.08262049") {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"type":"l2update","product_id":"BTC-USD","changes":[["buy","68094.41","0.01071465"],["sell","77837.31","0.00000000"]],"time":"2026-11-01T21:35:35.610341Z"}`)
	if len(evs) != 1 || len(evs[0].Bids) != 1 || len(evs[0].Asks) != 1 || evs[0].Asks[0].Qty != 0 {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"type":"heartbeat","last_trade_id":1,"product_id":"ETH-USD","sequence":987654321,"time":"2026-11-01T21:35:35.6Z"}`)
	if len(evs) != 1 || evs[0].Kind != model.KindHeartbeat || evs[0].Seq != 987654321 {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"type":"match","trade_id":5,"sequence":7,"side":"sell","size":"0.5","price":"77800.01","product_id":"BTC-USD","time":"2026-11-01T21:35:35Z"}`)
	if len(evs) != 1 || evs[0].Kind != model.KindTrade || evs[0].TradeSide != model.Bid {
		t.Fatalf("maker sell => taker buy: %+v", evs)
	}
}

func TestBitstamp(t *testing.T) {
	d := NewBitstampDecoder(table(t))
	snap := BitstampSnapshotPayload("btcusd", []byte(`{"timestamp":"1793568945","microtimestamp":"1793568945806550","bids":[["77826.06","0.63598338"]],"asks":[["77830.00","0.1"]]}`))
	evs := collect(t, d, FrameSnapshot, string(snap))
	if len(evs) != 1 || evs[0].Kind != model.KindSnapshot || evs[0].ExchTsUs != 1793568945806550 || evs[0].Symbol != "BTC-USD" {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"data":{"timestamp":"1793568945","microtimestamp":"1793568945921930","bids":[["77826.06","0.63598338"],["77803.90","0.00000000"]],"asks":[]},"channel":"diff_order_book_btcusd","event":"data"}`)
	if len(evs) != 1 || evs[0].Kind != model.KindDelta || len(evs[0].Bids) != 2 || evs[0].ExchTsUs != 1793568945921930 {
		t.Fatalf("%+v", evs)
	}
	// Trades arrive with event "trade" (shape as published by Bitstamp's live_trades channel).
	evs = collect(t, d, FrameWS, `{"data":{"id":1,"amount":0.5,"amount_str":"0.50000000","price":2262.1,"price_str":"2262.1","type":1,"microtimestamp":"1793568945921931"},"channel":"live_trades_ethusd","event":"trade"}`)
	if len(evs) != 1 || evs[0].Kind != model.KindTrade || evs[0].TradeSide != model.Ask || evs[0].TradePx != model.MustFixed("2262.1") {
		t.Fatalf("%+v", evs)
	}
	if evs := collect(t, d, FrameWS, `{"event":"bts:subscription_succeeded","channel":"diff_order_book_btcusd","data":{}}`); len(evs) != 0 {
		t.Fatal("ack should be ignored")
	}
}

func TestGemini(t *testing.T) {
	d := NewGeminiDecoder(table(t))
	evs := collect(t, d, FrameWS, `{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","77847.54","0.02569376"],["sell","77850","1.0"]],"trades":[],"auction_events":[]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindSnapshot {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","77827.62","0.0"]]}`)
	if len(evs) != 1 || evs[0].Kind != model.KindDelta || evs[0].Bids[0].Qty != 0 {
		t.Fatalf("%+v", evs)
	}
	evs = collect(t, d, FrameWS, `{"type":"trade","symbol":"ETHUSD","event_id":1,"timestamp":1793568945921,"price":"2262.11","quantity":"0.3","side":"buy"}`)
	if len(evs) != 1 || evs[0].ExchTsUs != 1793568945921000 {
		t.Fatalf("%+v", evs)
	}
}

func TestRFC3339Micros(t *testing.T) {
	if got := parseRFC3339Micros([]byte("1970-01-01T00:00:01.5Z")); got != 1_500_000 {
		t.Fatal(got)
	}
	if got := parseRFC3339Micros([]byte("2000-03-01T00:00:00Z")); got != 951868800_000000 {
		t.Fatal(got)
	}
	if parseRFC3339Micros([]byte("garbage")) != 0 {
		t.Fatal("garbage")
	}
}

// Fuzz every decoder: arbitrary bytes must never panic, and any decoded level must be valid.
func FuzzDecoders(f *testing.F) {
	for _, s := range []string{
		`{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":1.5,"qty":2}],"asks":[],"checksum":1}]}`,
		`{"type":"l2update","product_id":"BTC-USD","changes":[["buy","1.0","2.0"]]}`,
		`{"data":{"microtimestamp":"1","bids":[["1","2"]],"asks":[]},"channel":"diff_order_book_btcusd","event":"data"}`,
		`{"type":"l2_updates","symbol":"BTCUSD","changes":[["sell","1","1"]]}`,
		`{"t":"d","s":"BTC-USD","b":[["1","1"]],"a":[],"q":3}`,
	} {
		f.Add([]byte(s))
	}
	st := table(f)
	decs := []Decoder{NewKrakenDecoder(st), NewCoinbaseDecoder(st), NewBitstampDecoder(st), NewGeminiDecoder(st), NewSyntheticDecoder(st)}
	f.Fuzz(func(t *testing.T, b []byte) {
		for _, d := range decs {
			for _, k := range []FrameKind{FrameWS, FrameSnapshot, FrameConnected} {
				_ = d.Decode(&Frame{Kind: k, Venue: d.Venue(), Payload: b}, func(e *model.Event) {
					for _, l := range append(append([]model.Level(nil), e.Bids...), e.Asks...) {
						if l.Price < 0 || l.Qty < 0 {
							t.Fatalf("negative level from %q", b)
						}
					}
				})
			}
		}
	})
}

func BenchmarkDecodeKrakenUpdate(b *testing.B) {
	d := NewKrakenDecoder(table(b))
	fr := &Frame{Kind: FrameWS, Venue: model.Kraken, Payload: []byte(`{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":77825.9,"qty":0.41857559}],"asks":[{"price":77833.0,"qty":0.14392755}],"checksum":1352553919,"timestamp":"2026-11-01T21:35:35.212793Z"}]}`)}
	b.ReportAllocs()
	b.SetBytes(int64(len(fr.Payload)))
	for i := 0; i < b.N; i++ {
		_ = d.Decode(fr, func(*model.Event) {})
	}
}

func BenchmarkDecodeCoinbaseL2Update(b *testing.B) {
	d := NewCoinbaseDecoder(table(b))
	fr := &Frame{Kind: FrameWS, Venue: model.Coinbase, Payload: []byte(`{"type":"l2update","product_id":"BTC-USD","changes":[["buy","77810.62","0.08262049"],["sell","77837.31","0.00000000"]],"time":"2026-11-01T21:35:35.630945Z"}`)}
	b.ReportAllocs()
	b.SetBytes(int64(len(fr.Payload)))
	for i := 0; i < b.N; i++ {
		_ = d.Decode(fr, func(*model.Event) {})
	}
}

func BenchmarkDecodeBitstampDiff(b *testing.B) {
	d := NewBitstampDecoder(table(b))
	fr := &Frame{Kind: FrameWS, Venue: model.Bitstamp, Payload: []byte(`{"data":{"timestamp":"1793568945","microtimestamp":"1793568945921930","bids":[["77826.06","0.63598338"],["77803.90","0.13764480"],["77795.95","0.00000000"]],"asks":[["77838.58","0.00000000"],["77838.71","0.64239055"]]},"channel":"diff_order_book_btcusd","event":"data"}`)}
	b.ReportAllocs()
	b.SetBytes(int64(len(fr.Payload)))
	for i := 0; i < b.N; i++ {
		_ = d.Decode(fr, func(*model.Event) {})
	}
}

func BenchmarkDecodeGeminiUpdate(b *testing.B) {
	d := NewGeminiDecoder(table(b))
	fr := &Frame{Kind: FrameWS, Venue: model.Gemini, Payload: []byte(`{"changes":[["buy","77827.61","0.03372305"]],"symbol":"BTCUSD","type":"l2_updates"}`)}
	b.ReportAllocs()
	b.SetBytes(int64(len(fr.Payload)))
	for i := 0; i < b.N; i++ {
		_ = d.Decode(fr, func(*model.Event) {})
	}
}
