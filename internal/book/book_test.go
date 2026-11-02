package book

import (
	"math/rand"
	"sort"
	"testing"

	"github.com/eshaan2418/marketmux/internal/model"
)

// refBook is the obviously-correct reference: a map per side, sorted on read.
type refBook struct{ bids, asks map[model.Fixed]model.Fixed }

func newRef() *refBook { return &refBook{map[model.Fixed]model.Fixed{}, map[model.Fixed]model.Fixed{}} }

func (r *refBook) set(s model.Side, px, q model.Fixed) {
	m := r.bids
	if s == model.Ask {
		m = r.asks
	}
	if q == 0 {
		delete(m, px)
	} else {
		m[px] = q
	}
}

func (r *refBook) side(s model.Side, depth int) []model.Level {
	m := r.bids
	if s == model.Ask {
		m = r.asks
	}
	out := make([]model.Level, 0, len(m))
	for p, q := range m {
		out = append(out, model.Level{Price: p, Qty: q})
	}
	sort.Slice(out, func(i, j int) bool {
		if s == model.Bid {
			return out[i].Price > out[j].Price
		}
		return out[i].Price < out[j].Price
	})
	if depth > 0 && len(out) > depth {
		// Mirror truncation: levels beyond depth are forgotten.
		for _, l := range out[depth:] {
			delete(m, l.Price)
		}
		out = out[:depth]
	}
	return out
}

func equal(a, b []model.Level) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// Differential test: random absolute updates near a drifting mid, against the map reference.
func TestBookMatchesReference(t *testing.T) {
	for _, depth := range []int{0, 10, 25} {
		rng := rand.New(rand.NewSource(int64(depth) + 1))
		b, r := New(depth), newRef()
		mid := int64(1_000_000)
		for i := 0; i < 60_000; i++ {
			mid += int64(rng.Intn(3) - 1)
			s := model.Side(rng.Intn(2))
			off := int64(rng.ExpFloat64()*20) + 1
			px := mid - off
			if s == model.Ask {
				px = mid + off
			}
			q := model.Fixed(0)
			if rng.Intn(3) > 0 {
				q = model.Fixed(rng.Intn(1000) + 1)
			}
			b.ApplyDelta(nil, nil) // no-op must not disturb
			if s == model.Bid {
				b.ApplyDelta([]model.Level{{Price: model.Fixed(px), Qty: q}}, nil)
			} else {
				b.ApplyDelta(nil, []model.Level{{Price: model.Fixed(px), Qty: q}})
			}
			r.set(s, model.Fixed(px), q)
			if !b.Valid() {
				t.Fatalf("depth %d step %d: invalid book", depth, i)
			}
			if !equal(b.Bids, r.side(model.Bid, depth)) || !equal(b.Asks, r.side(model.Ask, depth)) {
				t.Fatalf("depth %d step %d: mismatch", depth, i)
			}
		}
	}
}

func TestSnapshotUnsortedInput(t *testing.T) {
	b := New(0)
	b.ApplySnapshot(
		[]model.Level{{Price: 5, Qty: 1}, {Price: 9, Qty: 2}, {Price: 7, Qty: 0}, {Price: 6, Qty: 3}},
		[]model.Level{{Price: 12, Qty: 1}, {Price: 10, Qty: 1}})
	if b.Bids[0].Price != 9 || len(b.Bids) != 3 || b.Asks[0].Price != 10 || !b.Valid() {
		t.Fatalf("%+v %+v", b.Bids, b.Asks)
	}
	if b.Crossed() {
		t.Fatal("not crossed")
	}
}

func TestApplyDeltaNoAllocsAtCapacity(t *testing.T) {
	b := New(50)
	lv := []model.Level{{}}
	for i := 0; i < 50; i++ {
		lv[0] = model.Level{Price: model.Fixed(1000 - i), Qty: 1}
		b.ApplyDelta(lv, nil)
	}
	allocs := testing.AllocsPerRun(1000, func() {
		lv[0] = model.Level{Price: 990, Qty: 0}
		b.ApplyDelta(lv, nil)
		lv[0] = model.Level{Price: 990, Qty: 7}
		b.ApplyDelta(lv, nil)
	})
	if allocs != 0 {
		t.Fatalf("allocs per run = %v", allocs)
	}
}

func BenchmarkApplyDeltaNearTop(b *testing.B) {
	bk := New(0)
	for i := 0; i < 500; i++ {
		bk.Set(model.Bid, model.Fixed(100000-i), 1)
		bk.Set(model.Ask, model.Fixed(100001+i), 1)
	}
	rng := rand.New(rand.NewSource(1))
	ups := make([]model.Level, 4096)
	for i := range ups {
		ups[i] = model.Level{Price: model.Fixed(100000 - int(rng.ExpFloat64()*5)), Qty: model.Fixed(rng.Intn(3))}
	}
	one := []model.Level{{}}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		one[0] = ups[i&4095]
		bk.ApplyDelta(one, nil)
	}
}

func BenchmarkReferenceMapNearTop(b *testing.B) {
	r := newRef()
	for i := 0; i < 500; i++ {
		r.set(model.Bid, model.Fixed(100000-i), 1)
	}
	rng := rand.New(rand.NewSource(1))
	ups := make([]model.Level, 4096)
	for i := range ups {
		ups[i] = model.Level{Price: model.Fixed(100000 - int(rng.ExpFloat64()*5)), Qty: model.Fixed(rng.Intn(3))}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		u := ups[i&4095]
		r.set(model.Bid, u.Price, u.Qty)
		if i&63 == 0 { // reading the top requires a full sort with a map; amortise like a consumer would
			_ = r.side(model.Bid, 0)[0]
		}
	}
}
