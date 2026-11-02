// Package book implements a per-venue L2 (price-aggregated) order book.
//
// Each side is a slice of levels kept sorted best-first (bids descending, asks ascending).
// Crypto L2 feeds send absolute level updates ("price p now has qty q", q = 0 deletes), and most
// updates land near the top of the book, so a sorted slice with binary search and in-place
// insert/delete is cache-friendly and allocation-free once the slices reach working capacity.
package book

import (
	"sort"

	"github.com/eshaan2418/marketmux/internal/model"
)

// Book is an L2 book for one (venue, symbol). Not safe for concurrent use; the pipeline owns
// each book from a single goroutine.
type Book struct {
	Bids []model.Level // descending price
	Asks []model.Level // ascending price
	// MaxDepth truncates each side after every update (0 = unlimited). Kraken v2 requires the
	// client to keep exactly the subscribed depth for checksums to match.
	MaxDepth int
}

// New returns a book with pre-sized sides.
func New(maxDepth int) *Book {
	c := 64
	if maxDepth > 0 {
		c = maxDepth + 1
	}
	return &Book{Bids: make([]model.Level, 0, c), Asks: make([]model.Level, 0, c), MaxDepth: maxDepth}
}

// Reset empties the book, keeping capacity.
func (b *Book) Reset() {
	b.Bids = b.Bids[:0]
	b.Asks = b.Asks[:0]
}

// ApplySnapshot replaces the book. Levels may arrive in any order; zero-qty levels are skipped.
func (b *Book) ApplySnapshot(bids, asks []model.Level) {
	b.Reset()
	for _, l := range bids {
		b.Set(model.Bid, l.Price, l.Qty)
	}
	for _, l := range asks {
		b.Set(model.Ask, l.Price, l.Qty)
	}
	b.truncate()
}

// ApplyDelta applies absolute level updates in order.
func (b *Book) ApplyDelta(bids, asks []model.Level) {
	for _, l := range bids {
		b.Set(model.Bid, l.Price, l.Qty)
	}
	for _, l := range asks {
		b.Set(model.Ask, l.Price, l.Qty)
	}
	b.truncate()
}

// Set sets the quantity at a price (qty 0 removes the level).
func (b *Book) Set(s model.Side, px, qty model.Fixed) {
	lv := &b.Bids
	if s == model.Ask {
		lv = &b.Asks
	}
	v := *lv
	i := search(v, s, px)
	found := i < len(v) && v[i].Price == px
	switch {
	case qty == 0 && found:
		copy(v[i:], v[i+1:])
		*lv = v[:len(v)-1]
	case qty == 0:
		// delete of an unknown level: legal on L2 feeds (e.g. outside our truncated depth)
	case found:
		v[i].Qty = qty
	default:
		v = append(v, model.Level{})
		copy(v[i+1:], v[i:])
		v[i] = model.Level{Price: px, Qty: qty}
		*lv = v
	}
}

// search returns the index of the first level not better than px.
func search(v []model.Level, s model.Side, px model.Fixed) int {
	// Fast path: most updates hit the first few levels.
	n := len(v)
	lim := n
	if lim > 8 {
		lim = 8
	}
	for i := 0; i < lim; i++ {
		if !better(s, v[i].Price, px) {
			return i
		}
	}
	if lim == n {
		return n
	}
	return lim + sort.Search(n-lim, func(j int) bool { return !better(s, v[lim+j].Price, px) })
}

func better(s model.Side, a, b model.Fixed) bool {
	if s == model.Bid {
		return a > b
	}
	return a < b
}

func (b *Book) truncate() {
	if b.MaxDepth <= 0 {
		return
	}
	if len(b.Bids) > b.MaxDepth {
		b.Bids = b.Bids[:b.MaxDepth]
	}
	if len(b.Asks) > b.MaxDepth {
		b.Asks = b.Asks[:b.MaxDepth]
	}
}

// Top returns the best bid and ask (ok=false for an empty side).
func (b *Book) Top() (bid, ask model.Level, okBid, okAsk bool) {
	if len(b.Bids) > 0 {
		bid, okBid = b.Bids[0], true
	}
	if len(b.Asks) > 0 {
		ask, okAsk = b.Asks[0], true
	}
	return
}

// Crossed reports best bid >= best ask.
func (b *Book) Crossed() bool {
	return len(b.Bids) > 0 && len(b.Asks) > 0 && b.Bids[0].Price >= b.Asks[0].Price
}

// Valid checks the structural invariants (strict ordering, positive quantities, depth bound).
func (b *Book) Valid() bool {
	check := func(v []model.Level, s model.Side) bool {
		for i := range v {
			if v[i].Qty <= 0 {
				return false
			}
			if i > 0 && !better(s, v[i-1].Price, v[i].Price) {
				return false
			}
		}
		return b.MaxDepth <= 0 || len(v) <= b.MaxDepth
	}
	return check(b.Bids, model.Bid) && check(b.Asks, model.Ask)
}
