package pipeline

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/eshaan2418/marketmux/internal/fakevenue"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/venue"
)

type recorder struct{ ups []Update }

func (r *recorder) Publish(u *Update) { r.ups = append(r.ups, *u) }

type resyncLog struct{ reasons []string }

func (r *resyncLog) Resync(v model.Venue, sym, reason string) {
	r.reasons = append(r.reasons, v.String()+"/"+sym+"/"+reason)
}

func newEngine(t testing.TB, syms ...string) (*Engine, *recorder, *resyncLog) {
	st, err := venue.NewSymbolTable(syms)
	if err != nil {
		t.Fatal(err)
	}
	rec, rs := &recorder{}, &resyncLog{}
	return New(st, Options{Publisher: rec, Resyncer: rs, MaxPendingDiffs: 4}), rec, rs
}

func ws(v model.Venue, s string) *venue.Frame {
	return &venue.Frame{Kind: venue.FrameWS, Venue: v, Payload: []byte(s)}
}

func TestSyntheticGapTriggersResync(t *testing.T) {
	e, rec, rs := newEngine(t, "BTC-USD", "ETH-USD")
	g := fakevenue.NewGenerator(1, []string{"BTC-USD", "ETH-USD"})
	e.Handle(ws(model.Synthetic, string(g.Snapshot(0))))
	e.Handle(ws(model.Synthetic, string(g.Snapshot(1))))
	for i := 0; i < 100; i++ {
		e.Handle(ws(model.Synthetic, string(g.Delta())))
	}
	if st := e.Stats(); st.SeqGaps != 0 || st.Resyncs != 0 || st.DecodeErrors != 0 {
		t.Fatalf("%+v", st)
	}
	g.SkipSeq()
	e.Handle(ws(model.Synthetic, string(g.Delta())))
	st := e.Stats()
	if st.SeqGaps != 1 || st.Resyncs != 1 || len(rs.reasons) != 1 {
		t.Fatalf("gap not detected: %+v %v", st, rs.reasons)
	}
	// Every book of the venue is now untrusted and removed from the consolidated view.
	for _, s := range []string{"BTC-USD", "ETH-USD"} {
		if u, _ := e.Last(s); u.Venues[model.Synthetic].Valid {
			t.Fatalf("%s still valid after gap", s)
		}
	}
	last := rec.ups[len(rec.ups)-1]
	// A snapshot restores it.
	sym := last.Symbol
	idx := 0
	if sym == "ETH-USD" {
		idx = 1
	}
	e.Handle(ws(model.Synthetic, string(g.Snapshot(idx))))
	if u, _ := e.Last(sym); !u.Venues[model.Synthetic].Valid {
		t.Fatal("snapshot did not restore the book")
	}
}

func TestBitstampSnapshotDiffSync(t *testing.T) {
	e, _, rs := newEngine(t, "BTC-USD")
	diff := func(us int64, bid string) *venue.Frame {
		return ws(model.Bitstamp, fmt.Sprintf(`{"data":{"microtimestamp":"%d","bids":[["%s","1"]],"asks":[]},"channel":"diff_order_book_btcusd","event":"data"}`, us, bid))
	}
	snap := func(us int64) *venue.Frame {
		body := fmt.Sprintf(`{"microtimestamp":"%d","bids":[["100","1"]],"asks":[["200","1"]]}`, us)
		return &venue.Frame{Kind: venue.FrameSnapshot, Venue: model.Bitstamp, Payload: venue.BitstampSnapshotPayload("btcusd", []byte(body))}
	}
	// Diffs before the snapshot are buffered; those at/before snapshot time are discarded.
	e.Handle(diff(10, "101"))
	e.Handle(diff(20, "102"))
	e.Handle(diff(30, "103"))
	e.Handle(snap(20))
	b, live := e.Book(model.Bitstamp, "BTC-USD")
	if !live || b.Bids[0].Price != model.MustFixed("103") || len(b.Bids) != 2 {
		t.Fatalf("live=%v bids=%+v", live, b.Bids)
	}
	st := e.Stats()
	if st.BitstampBuffer != 3 || st.BitstampStale != 2 {
		t.Fatalf("%+v", st)
	}
	// An equal timestamp is a duplicate (ignored); an older one means inconsistency => resync.
	e.Handle(diff(30, "104"))
	if _, live := e.Book(model.Bitstamp, "BTC-USD"); !live {
		t.Fatal("duplicate timestamp should be ignored")
	}
	e.Handle(diff(25, "105"))
	if _, live := e.Book(model.Bitstamp, "BTC-USD"); live || len(rs.reasons) != 1 || rs.reasons[0] != "bitstamp/BTC-USD/bitstamp_out_of_order" {
		t.Fatalf("out-of-order not handled: %v", rs.reasons)
	}
	// Buffer overflow while waiting for a snapshot also forces a resync.
	for i := 0; i < 5; i++ {
		e.Handle(diff(int64(40+i), "106"))
	}
	if e.Stats().ResyncByReason["bitstamp_buffer_overflow"] != 1 {
		t.Fatalf("%+v", e.Stats().ResyncByReason)
	}
}

func TestKrakenChecksumFailureResyncs(t *testing.T) {
	e, _, rs := newEngine(t, "BTC-USD")
	e.Handle(&venue.Frame{Kind: venue.FrameConnected, Venue: model.Kraken, Payload: []byte(`{"precision":{"BTC-USD":{"price":1,"qty":8}}}`)})
	bids := []model.Level{{Price: model.MustFixed("100.5"), Qty: model.MustFixed("2")}}
	asks := []model.Level{{Price: model.MustFixed("100.6"), Qty: model.MustFixed("1")}}
	good, _ := venue.KrakenChecksum(bids, asks, venue.KrakenPrecision{Price: 1, Qty: 8}, nil)
	e.Handle(ws(model.Kraken, fmt.Sprintf(`{"channel":"book","type":"snapshot","data":[{"symbol":"BTC/USD","bids":[{"price":100.5,"qty":2}],"asks":[{"price":100.6,"qty":1}],"checksum":%d}]}`, good)))
	if st := e.Stats(); st.ChecksumOK != 1 || st.ChecksumFail != 0 {
		t.Fatalf("%+v", st)
	}
	e.Handle(ws(model.Kraken, `{"channel":"book","type":"update","data":[{"symbol":"BTC/USD","bids":[{"price":100.4,"qty":1}],"asks":[],"checksum":1}]}`))
	st := e.Stats()
	if st.ChecksumFail != 1 || st.Resyncs != 1 || len(rs.reasons) != 1 {
		t.Fatalf("%+v %v", st, rs.reasons)
	}
	if _, live := e.Book(model.Kraken, "BTC-USD"); live {
		t.Fatal("book should be untrusted after checksum failure")
	}
}

func TestDisconnectInvalidatesVenue(t *testing.T) {
	e, _, _ := newEngine(t, "BTC-USD")
	e.Handle(ws(model.Coinbase, `{"type":"snapshot","product_id":"BTC-USD","bids":[["100","1"]],"asks":[["101","1"]]}`))
	if u, _ := e.Last("BTC-USD"); !u.Venues[model.Coinbase].Valid || u.BidVenue != model.Coinbase {
		t.Fatalf("%+v", u)
	}
	e.Handle(&venue.Frame{Kind: venue.FrameDisconnected, Venue: model.Coinbase})
	if u, _ := e.Last("BTC-USD"); u.Venues[model.Coinbase].Valid || u.BidVenue != 0 {
		t.Fatalf("%+v", u)
	}
	// Deltas before a fresh snapshot are ignored.
	e.Handle(ws(model.Coinbase, `{"type":"l2update","product_id":"BTC-USD","changes":[["buy","100.5","1"]]}`))
	if u, _ := e.Last("BTC-USD"); u.Venues[model.Coinbase].Valid {
		t.Fatal("delta without snapshot accepted")
	}
}

func TestVenueSelfCrossed(t *testing.T) {
	e, _, rs := newEngine(t, "BTC-USD")
	e.Handle(ws(model.Gemini, `{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","100","1"],["sell","101","1"]],"trades":[]}`))
	// A transient cross: excluded from the consolidated view, then healed by the next update.
	e.Handle(ws(model.Gemini, `{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","102","1"]]}`))
	if u, _ := e.Last("BTC-USD"); u.Venues[model.Gemini].Valid {
		t.Fatal("crossed venue must be excluded")
	}
	e.Handle(ws(model.Gemini, `{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","102","0"]]}`))
	if u, _ := e.Last("BTC-USD"); !u.Venues[model.Gemini].Valid || u.BidPx != model.MustFixed("100") {
		t.Fatalf("healed venue must return: %+v", u)
	}
	if st := e.Stats(); st.CrossedHealed != 1 || len(rs.reasons) != 0 {
		t.Fatalf("%+v %v", st, rs.reasons)
	}
	// A persistent cross (5 consecutive updates) is divergence: resync.
	for i := 0; i < 5; i++ {
		e.Handle(ws(model.Gemini, fmt.Sprintf(`{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","%d","1"]]}`, 103+i)))
	}
	if len(rs.reasons) != 1 || rs.reasons[0] != "gemini/BTC-USD/venue_crossed" {
		t.Fatalf("%v", rs.reasons)
	}
}

// Property: the consolidated BBO equals a brute-force max/min over valid venue tops, under random
// snapshots, deltas and disconnects across venues.
func TestConsolidatedMatchesBruteForce(t *testing.T) {
	e, rec, _ := newEngine(t, "BTC-USD")
	rng := rand.New(rand.NewSource(7))
	venues := []model.Venue{model.Coinbase, model.Gemini}
	snap := func(v model.Venue, mid int) *venue.Frame {
		switch v {
		case model.Coinbase:
			return ws(v, fmt.Sprintf(`{"type":"snapshot","product_id":"BTC-USD","bids":[["%d","1"]],"asks":[["%d","1"]]}`, mid-1, mid+1))
		default:
			return ws(v, fmt.Sprintf(`{"type":"l2_updates","symbol":"BTCUSD","changes":[["buy","%d","1"],["sell","%d","1"]],"trades":[]}`, mid-1, mid+1))
		}
	}
	for i := 0; i < 5000; i++ {
		v := venues[rng.Intn(2)]
		switch rng.Intn(10) {
		case 0:
			e.Handle(&venue.Frame{Kind: venue.FrameDisconnected, Venue: v})
		default:
			e.Handle(snap(v, 1000+rng.Intn(20)))
		}
		if len(rec.ups) == 0 {
			continue
		}
		u := rec.ups[len(rec.ups)-1]
		var bb, ba model.Fixed
		for vi := range u.Venues {
			tp := u.Venues[vi]
			if !tp.Valid {
				continue
			}
			if tp.HasBid && tp.Bid.Price > bb {
				bb = tp.Bid.Price
			}
			if tp.HasAsk && (ba == 0 || tp.Ask.Price < ba) {
				ba = tp.Ask.Price
			}
		}
		if u.BidPx != bb || u.AskPx != ba {
			t.Fatalf("step %d: consolidated %v/%v brute %v/%v", i, u.BidPx, u.AskPx, bb, ba)
		}
		if u.Crossed != (bb != 0 && ba != 0 && bb >= ba) {
			t.Fatalf("step %d: crossed flag wrong", i)
		}
	}
}

// Determinism: the same frames produce the same output hash and update stream.
func TestReplayDeterminism(t *testing.T) {
	g := fakevenue.NewGenerator(3, []string{"BTC-USD", "ETH-USD"})
	frames := []*venue.Frame{ws(model.Synthetic, string(g.Snapshot(0))), ws(model.Synthetic, string(g.Snapshot(1)))}
	for i := 0; i < 20000; i++ {
		if i == 5000 {
			g.SkipSeq()
		}
		frames = append(frames, ws(model.Synthetic, string(g.Delta())))
		if i == 7000 {
			frames = append(frames, ws(model.Synthetic, string(g.Snapshot(0))), ws(model.Synthetic, string(g.Snapshot(1))))
		}
	}
	run := func() (uint64, int) {
		e, rec, _ := newEngine(t, "BTC-USD", "ETH-USD")
		for _, f := range frames {
			e.Handle(f)
		}
		return e.Stats().OutputHash, len(rec.ups)
	}
	h1, n1 := run()
	h2, n2 := run()
	if h1 != h2 || n1 != n2 || n1 == 0 {
		t.Fatalf("non-deterministic: %x/%d vs %x/%d", h1, n1, h2, n2)
	}
}

func BenchmarkEngineSyntheticDelta(b *testing.B) {
	st, _ := venue.NewSymbolTable([]string{"BTC-USD", "ETH-USD", "SOL-USD"})
	e := New(st, Options{})
	g := fakevenue.NewGenerator(1, []string{"BTC-USD", "ETH-USD", "SOL-USD"})
	for i := 0; i < 3; i++ {
		e.Handle(ws(model.Synthetic, string(g.Snapshot(i))))
	}
	msgs := make([]*venue.Frame, 4096)
	for i := range msgs {
		msgs[i] = ws(model.Synthetic, string(g.Delta()))
	}
	// Re-sequence: the benchmark loops over the same deltas, so rewrite them to stay gap-free by
	// resetting the engine's view with a snapshot each lap.
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%4096 == 0 && i > 0 {
			b.StopTimer()
			e = New(st, Options{})
			g2 := fakevenue.NewGenerator(1, []string{"BTC-USD", "ETH-USD", "SOL-USD"})
			for k := 0; k < 3; k++ {
				e.Handle(ws(model.Synthetic, string(g2.Snapshot(k))))
			}
			b.StartTimer()
		}
		e.Handle(msgs[i%4096])
	}
}

// The per-frame hot path (decode -> book -> consolidate -> publish) must not allocate once the
// books are at working capacity.
func TestEngineHotPathAllocFree(t *testing.T) {
	st, _ := venue.NewSymbolTable([]string{"BTC-USD"})
	rec := &countPub{}
	e := New(st, Options{Publisher: rec})
	g := fakevenue.NewGenerator(1, []string{"BTC-USD"})
	e.Handle(ws(model.Synthetic, string(g.Snapshot(0))))
	frames := make([]*venue.Frame, 2000)
	for i := range frames {
		frames[i] = ws(model.Synthetic, string(g.Delta()))
	}
	for _, f := range frames[:1000] { // warm up
		e.Handle(f)
	}
	i := 1000
	allocs := testing.AllocsPerRun(900, func() {
		e.Handle(frames[i])
		i++
	})
	if allocs != 0 {
		t.Fatalf("allocs per frame = %v", allocs)
	}
	if rec.n == 0 {
		t.Fatal("nothing published")
	}
}

type countPub struct{ n int }

func (c *countPub) Publish(*Update) { c.n++ }
