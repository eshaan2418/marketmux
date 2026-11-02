// Package pipeline contains the single-goroutine engine that turns raw venue frames into
// validated per-venue books and a consolidated cross-venue BBO.
//
// Determinism: the engine is a pure function of the frame sequence. Sessions (live) or a capture
// reader (replay) produce frames; the engine never reads the wall clock for decisions, so a replay
// of a capture reproduces every book state, resync decision and published update exactly. The
// output hash (Stats.OutputHash) makes that checkable.
package pipeline

import (
	"errors"
	"hash/fnv"
	"math"

	"github.com/eshaan2418/marketmux/internal/book"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/venue"
)

// Top is one venue's best bid/ask for a symbol.
type Top struct {
	Valid  bool
	Bid    model.Level
	Ask    model.Level
	HasBid bool
	HasAsk bool
}

// Update is a consolidated BBO update for one symbol. It is a value type (no pointers besides
// the immutable Symbol string) so it can be copied into per-client queues without allocation.
type Update struct {
	Symbol   string
	Seq      uint64 // per-symbol update sequence (gap-free for a given engine run)
	BidPx    model.Fixed
	BidQty   model.Fixed
	BidVenue model.Venue
	AskPx    model.Fixed
	AskQty   model.Fixed
	AskVenue model.Venue
	Crossed  bool // best bid >= best ask across venues (stale venue or latency arbitrage)
	Cause    model.Venue
	Venues   [model.NumVenues]Top
	RecvNs   int64 // when the causing frame was received (capture time base under replay)
	EngNs    int64 // when the engine started processing the causing frame (engine clock)
	PubNs    int64 // when the engine published (engine clock)
}

// Resyncer is notified when a venue book is found inconsistent. Live sessions resubscribe or
// refetch a snapshot; replay ignores it (the capture already contains what happened).
type Resyncer interface {
	Resync(v model.Venue, symbol string, reason string)
}

// Publisher receives consolidated updates. The pointer is only valid during the call: the
// engine reuses the Update, so implementations must copy whatever they keep.
type Publisher interface {
	Publish(u *Update)
}

// Clock returns monotonic nanoseconds; injected so replay and tests control it.
type Clock func() int64

// Stats are the engine's integrity and throughput counters.
type Stats struct {
	Frames         uint64
	Events         uint64
	DecodeErrors   uint64
	Ignored        uint64
	Snapshots      uint64
	Deltas         uint64
	Trades         uint64
	ChecksumOK     uint64
	ChecksumFail   uint64
	SeqGaps        uint64
	VenueCrossed   uint64 // updates after which a single venue's own book was crossed
	CrossedHealed  uint64 // venue-crossed episodes that resolved without a resync
	BitstampStale  uint64 // diffs dropped because they predate the snapshot
	BitstampBuffer uint64 // diffs buffered while waiting for a snapshot
	Resyncs        uint64
	Disconnects    uint64
	Published      uint64
	CrossedUpdates uint64
	OutputHash     uint64
	ResyncByReason map[string]uint64
}

type bookKey struct {
	v   model.Venue
	sym string
}

type vstate struct {
	book    *book.Book
	live    bool  // book is synchronised and trusted
	lastUs  int64 // Bitstamp: last applied microtimestamp
	pending []pendingDiff
	// crossedRun counts consecutive updates after which this venue's own book was crossed.
	crossedRun int
}

type pendingDiff struct {
	us         int64
	bids, asks []model.Level
}

type symState struct {
	tops [model.NumVenues]Top
	last Update
	seq  uint64
}

// Engine owns all books. Not safe for concurrent use.
type Engine struct {
	syms     *venue.SymbolTable
	decoders [model.NumVenues]venue.Decoder
	books    map[bookKey]*vstate
	order    []bookKey // creation order: iteration must be deterministic (map order is random)
	symbols  map[string]*symState
	pub      Publisher
	resync   Resyncer
	clock    Clock
	stats    Stats
	hash     uint64
	scratch  []byte
	cur      *venue.Frame
	frameNs  int64
	upd      Update     // reused for every publish (keeps the hot path allocation-free)
	emit     venue.Emit // e.onEvent bound once: a method value per call would allocate
	seq      [model.NumVenues]uint64
	seqLive  [model.NumVenues]bool
	maxPend  int
	crossTol int
	OnEvent  func(*model.Event) // optional tap (capture stats, tests)
}

// Options configure an Engine.
type Options struct {
	Publisher Publisher
	Resyncer  Resyncer
	Clock     Clock
	// MaxPendingDiffs bounds the Bitstamp pre-snapshot buffer per symbol.
	MaxPendingDiffs int
	// CrossedTolerance is how many consecutive updates a venue's own book may stay crossed
	// before it is declared divergent and resynced (default 5). While crossed it is excluded
	// from the consolidated view. 1 = resync on the first crossed update.
	CrossedTolerance int
}

// New creates an engine for the given symbols.
func New(t *venue.SymbolTable, o Options) *Engine {
	e := &Engine{syms: t, books: map[bookKey]*vstate{}, symbols: map[string]*symState{},
		pub: o.Publisher, resync: o.Resyncer, clock: o.Clock, maxPend: o.MaxPendingDiffs, crossTol: o.CrossedTolerance,
		scratch: make([]byte, 0, 512)}
	if e.maxPend == 0 {
		e.maxPend = 10_000
	}
	if e.crossTol == 0 {
		e.crossTol = 5
	}
	if e.clock == nil {
		e.clock = func() int64 { return 0 }
	}
	for v := model.Venue(1); int(v) < model.NumVenues; v++ {
		e.decoders[v] = venue.NewDecoder(v, t)
	}
	for _, s := range t.Symbols() {
		e.symbols[s.Canonical] = &symState{}
	}
	e.stats.ResyncByReason = map[string]uint64{}
	e.emit = e.onEvent
	h := fnv.New64a()
	e.hash = h.Sum64()
	return e
}

// Stats returns a copy of the counters.
func (e *Engine) Stats() Stats {
	s := e.stats
	s.OutputHash = e.hash
	s.ResyncByReason = make(map[string]uint64, len(e.stats.ResyncByReason))
	for k, v := range e.stats.ResyncByReason {
		s.ResyncByReason[k] = v
	}
	return s
}

// Book returns the venue book for inspection (nil if absent). The returned book must not be
// mutated and is only valid on the engine goroutine.
func (e *Engine) Book(v model.Venue, sym string) (*book.Book, bool) {
	st, ok := e.books[bookKey{v, sym}]
	if !ok {
		return nil, false
	}
	return st.book, st.live
}

// Last returns the latest consolidated update for a symbol.
func (e *Engine) Last(sym string) (Update, bool) {
	s, ok := e.symbols[sym]
	if !ok || s.seq == 0 {
		return Update{}, false
	}
	return s.last, true
}

// Handle processes one frame.
func (e *Engine) Handle(f *venue.Frame) {
	e.stats.Frames++
	if int(f.Venue) <= 0 || int(f.Venue) >= model.NumVenues {
		e.stats.DecodeErrors++
		return
	}
	if f.Kind == venue.FrameDisconnected {
		e.stats.Disconnects++
		e.invalidateVenue(f.Venue, f.RecvNs)
		return
	}
	if f.Kind == venue.FrameConnected {
		// A new connection starts from snapshots; nothing is trusted until they arrive.
		e.invalidateVenue(f.Venue, f.RecvNs)
	}
	e.cur = f
	e.frameNs = e.clock()
	err := e.decoders[f.Venue].Decode(f, e.emit)
	switch {
	case err == nil:
	case errors.Is(err, venue.ErrIgnored):
		e.stats.Ignored++
	default:
		e.stats.DecodeErrors++
	}
}

func (e *Engine) state(v model.Venue, sym string) *vstate {
	k := bookKey{v, sym}
	st, ok := e.books[k]
	if !ok {
		depth := 0
		if v == model.Kraken {
			depth = venue.KrakenDepth
		}
		st = &vstate{book: book.New(depth)}
		e.books[k] = st
		e.order = append(e.order, k)
	}
	return st
}

func (e *Engine) onEvent(ev *model.Event) {
	e.stats.Events++
	if e.OnEvent != nil {
		e.OnEvent(ev)
	}
	switch ev.Kind {
	case model.KindTrade:
		e.stats.Trades++
		return
	case model.KindHeartbeat:
		return
	}
	st := e.state(ev.Venue, ev.Symbol)
	switch ev.Venue {
	case model.Bitstamp:
		e.bitstamp(st, ev)
		return
	case model.Synthetic:
		// The sequence number is per connection (as on MoldUDP64/ITCH or exchange FIX feeds), so a
		// gap invalidates every book of the venue, not just the symbol of the late message.
		if ev.HasSeq {
			switch {
			case ev.Kind == model.KindSnapshot:
				e.seq[ev.Venue], e.seqLive[ev.Venue] = ev.Seq, true
			case e.seqLive[ev.Venue] && ev.Seq != e.seq[ev.Venue]+1:
				e.stats.SeqGaps++
				e.seqLive[ev.Venue] = false
				e.failVenue(ev, "seq_gap")
				return
			default:
				e.seq[ev.Venue] = ev.Seq
			}
		}
	}
	if ev.Kind == model.KindSnapshot {
		e.stats.Snapshots++
		st.book.ApplySnapshot(ev.Bids, ev.Asks)
		st.live = true
	} else {
		e.stats.Deltas++
		if !st.live {
			return // deltas before the first snapshot are meaningless on these venues
		}
		st.book.ApplyDelta(ev.Bids, ev.Asks)
	}
	if ev.Venue == model.Kraken && ev.HasCRC {
		p, ok := e.decoders[model.Kraken].(*venue.KrakenDecoder).Precision(ev.Symbol)
		if ok {
			var sum uint32
			sum, e.scratch = venue.KrakenChecksum(st.book.Bids, st.book.Asks, p, e.scratch)
			if sum != ev.Checksum {
				e.stats.ChecksumFail++
				e.fail(st, ev, "checksum")
				return
			}
			e.stats.ChecksumOK++
		}
	}
	e.afterApply(st, ev)
}

// bitstamp implements snapshot/diff synchronisation: diffs are buffered until the REST snapshot
// arrives; diffs at or before the snapshot's microtimestamp are discarded; the rest are applied
// in order. A diff older than the last applied one means the stream is inconsistent.
func (e *Engine) bitstamp(st *vstate, ev *model.Event) {
	if ev.Kind == model.KindSnapshot {
		e.stats.Snapshots++
		st.book.ApplySnapshot(ev.Bids, ev.Asks)
		st.live, st.lastUs = true, ev.ExchTsUs
		for _, d := range st.pending {
			if d.us <= st.lastUs {
				e.stats.BitstampStale++
				continue
			}
			st.book.ApplyDelta(d.bids, d.asks)
			st.lastUs = d.us
		}
		st.pending = st.pending[:0]
		e.afterApply(st, ev)
		return
	}
	e.stats.Deltas++
	if !st.live {
		if len(st.pending) >= e.maxPend {
			e.fail(st, ev, "bitstamp_buffer_overflow")
			return
		}
		e.stats.BitstampBuffer++
		st.pending = append(st.pending, pendingDiff{us: ev.ExchTsUs,
			bids: append([]model.Level(nil), ev.Bids...), asks: append([]model.Level(nil), ev.Asks...)})
		return
	}
	if ev.ExchTsUs <= st.lastUs {
		if ev.ExchTsUs < st.lastUs {
			e.fail(st, ev, "bitstamp_out_of_order")
			return
		}
		e.stats.BitstampStale++
		return
	}
	st.book.ApplyDelta(ev.Bids, ev.Asks)
	st.lastUs = ev.ExchTsUs
	e.afterApply(st, ev)
}

// afterApply checks self-consistency. A venue's own book should never be crossed for long: its
// matching engine would have executed the crossing orders. Some derived feeds (Bitstamp's batched
// diff channel) do publish transient crossed states, so a crossed book is excluded from the
// consolidated view at once and declared divergent only if it stays crossed for crossTol
// consecutive updates.
func (e *Engine) afterApply(st *vstate, ev *model.Event) {
	if st.book.Crossed() {
		e.stats.VenueCrossed++
		st.crossedRun++
		if st.crossedRun >= e.crossTol {
			st.crossedRun = 0
			e.fail(st, ev, "venue_crossed")
			return
		}
		e.updateTop(ev.Venue, ev.Symbol, st, ev.RecvNs)
		return
	}
	if st.crossedRun > 0 {
		e.stats.CrossedHealed++
		st.crossedRun = 0
	}
	e.updateTop(ev.Venue, ev.Symbol, st, ev.RecvNs)
}

func (e *Engine) fail(st *vstate, ev *model.Event, reason string) {
	st.live = false
	st.book.Reset()
	st.pending = st.pending[:0]
	e.stats.Resyncs++
	e.stats.ResyncByReason[reason]++
	e.updateTop(ev.Venue, ev.Symbol, st, ev.RecvNs)
	if e.resync != nil {
		e.resync.Resync(ev.Venue, ev.Symbol, reason)
	}
}

// failVenue marks every book of the event's venue untrusted and requests one resync.
func (e *Engine) failVenue(ev *model.Event, reason string) {
	e.stats.Resyncs++
	e.stats.ResyncByReason[reason]++
	for _, k := range e.order {
		st := e.books[k]
		if k.v != ev.Venue {
			continue
		}
		st.live = false
		st.book.Reset()
		st.pending = st.pending[:0]
		e.updateTop(k.v, k.sym, st, ev.RecvNs)
	}
	if e.resync != nil {
		e.resync.Resync(ev.Venue, ev.Symbol, reason)
	}
}

func (e *Engine) invalidateVenue(v model.Venue, recvNs int64) {
	for _, k := range e.order {
		st := e.books[k]
		if k.v != v {
			continue
		}
		st.live = false
		st.book.Reset()
		st.pending = st.pending[:0]
		e.updateTop(v, k.sym, st, recvNs)
	}
}

func (e *Engine) updateTop(v model.Venue, sym string, st *vstate, recvNs int64) {
	s, ok := e.symbols[sym]
	if !ok {
		return
	}
	var t Top
	if st.live && st.crossedRun == 0 {
		bid, ask, okB, okA := st.book.Top()
		t = Top{Valid: true, Bid: bid, Ask: ask, HasBid: okB, HasAsk: okA}
	}
	if s.tops[v] == t {
		return
	}
	s.tops[v] = t
	e.upd = Update{Symbol: sym, Venues: s.tops, Cause: v, RecvNs: recvNs, AskPx: math.MaxInt64}
	u := &e.upd
	for vi := range s.tops {
		tp := s.tops[vi]
		if !tp.Valid {
			continue
		}
		// Ties on price go to the larger size, then the lower venue id (deterministic).
		if tp.HasBid && (tp.Bid.Price > u.BidPx || (tp.Bid.Price == u.BidPx && tp.Bid.Qty > u.BidQty)) {
			u.BidPx, u.BidQty, u.BidVenue = tp.Bid.Price, tp.Bid.Qty, model.Venue(vi)
		}
		if tp.HasAsk && (tp.Ask.Price < u.AskPx || (tp.Ask.Price == u.AskPx && tp.Ask.Qty > u.AskQty)) {
			u.AskPx, u.AskQty, u.AskVenue = tp.Ask.Price, tp.Ask.Qty, model.Venue(vi)
		}
	}
	if u.AskVenue == 0 {
		u.AskPx = 0
	}
	u.Crossed = u.BidVenue != 0 && u.AskVenue != 0 && u.BidPx >= u.AskPx
	s.seq++
	u.Seq = s.seq
	if u.Crossed {
		e.stats.CrossedUpdates++
	}
	e.hashUpdate(u)
	u.EngNs = e.frameNs
	u.PubNs = e.clock()
	s.last = *u
	e.stats.Published++
	if e.pub != nil {
		e.pub.Publish(u)
	}
}

// hashUpdate folds the deterministic fields of an update into the running FNV-1a output hash.
func (e *Engine) hashUpdate(u *Update) {
	h := e.hash
	mix := func(x uint64) {
		for i := 0; i < 8; i++ {
			h ^= x & 0xff
			h *= 1099511628211
			x >>= 8
		}
	}
	for i := 0; i < len(u.Symbol); i++ {
		h ^= uint64(u.Symbol[i])
		h *= 1099511628211
	}
	mix(u.Seq)
	mix(uint64(u.BidPx))
	mix(uint64(u.BidQty))
	mix(uint64(u.BidVenue))
	mix(uint64(u.AskPx))
	mix(uint64(u.AskQty))
	mix(uint64(u.AskVenue))
	mix(uint64(u.Cause))
	e.hash = h
}
