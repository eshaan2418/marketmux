// Package fakevenue generates synthetic venue traffic and serves it over WebSocket with fault
// injection (sequence gaps, disconnects, delays). It is used by integration tests and by the
// fan-out benchmark, so neither depends on live venues.
package fakevenue

import (
	"context"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Generator produces the synthetic protocol: a snapshot per symbol, then deltas carrying a
// gap-free per-connection sequence number. Prices random-walk around a mid with a 1-tick spread.
type Generator struct {
	rng     *rand.Rand
	symbols []string
	mids    []int64 // in ticks
	seq     uint64
	buf     []byte
}

// NewGenerator returns a deterministic generator.
func NewGenerator(seed int64, symbols []string) *Generator {
	g := &Generator{rng: rand.New(rand.NewSource(seed)), symbols: symbols}
	for i := range symbols {
		g.mids = append(g.mids, int64(10_000+1_000*i))
	}
	return g
}

func appendLevel(b []byte, ticks int64, qty int64) []byte {
	b = append(b, `["`...)
	b = strconv.AppendInt(b, ticks/100, 10)
	b = append(b, '.')
	c := ticks % 100
	b = append(b, byte('0'+c/10), byte('0'+c%10))
	b = append(b, `","`...)
	b = strconv.AppendInt(b, qty, 10)
	return append(b, `"]`...)
}

// Snapshot returns a snapshot message (10 levels per side) for symbol i and resets the sequence.
func (g *Generator) Snapshot(i int) []byte {
	m := g.mids[i]
	b := append(g.buf[:0], `{"t":"s","s":"`...)
	b = append(b, g.symbols[i]...)
	b = append(b, `","b":[`...)
	for k := int64(0); k < 10; k++ {
		if k > 0 {
			b = append(b, ',')
		}
		b = appendLevel(b, m-1-k, 1+k)
	}
	b = append(b, `],"a":[`...)
	for k := int64(0); k < 10; k++ {
		if k > 0 {
			b = append(b, ',')
		}
		b = appendLevel(b, m+1+k, 1+k)
	}
	b = append(b, `],"q":`...)
	b = strconv.AppendUint(b, g.seq, 10)
	b = append(b, '}')
	g.buf = b
	return append([]byte(nil), b...)
}

// Delta returns the next delta message (round-robin symbol) with seq+1. Each delta moves the mid
// by -1/0/+1 tick and rewrites the touch levels so the book stays uncrossed.
func (g *Generator) Delta() []byte {
	g.seq++
	i := int(g.seq % uint64(len(g.symbols)))
	old := g.mids[i]
	g.mids[i] += int64(g.rng.Intn(3) - 1)
	m := g.mids[i]
	b := append(g.buf[:0], `{"t":"d","s":"`...)
	b = append(b, g.symbols[i]...)
	b = append(b, `","b":[`...)
	// Remove levels that would cross, then set the new touch.
	b = appendLevel(b, old, 0)
	b = append(b, ',')
	b = appendLevel(b, m, 0)
	b = append(b, ',')
	b = appendLevel(b, m-1, 1+int64(g.rng.Intn(9)))
	b = append(b, `],"a":[`...)
	b = appendLevel(b, old, 0)
	b = append(b, ',')
	b = appendLevel(b, m, 0)
	b = append(b, ',')
	b = appendLevel(b, m+1, 1+int64(g.rng.Intn(9)))
	b = append(b, `],"q":`...)
	b = strconv.AppendUint(b, g.seq, 10)
	b = append(b, `,"x":`...)
	b = strconv.AppendInt(b, time.Now().UnixMicro(), 10)
	b = append(b, '}')
	g.buf = b
	return append([]byte(nil), b...)
}

// SkipSeq advances the sequence without emitting (injects a gap).
func (g *Generator) SkipSeq() { g.seq++ }

// Server serves the generator over WebSocket at "/" with controllable faults.
type Server struct {
	Symbols []string
	Seed    int64
	Rate    time.Duration // delay between deltas (0 = as fast as possible)
	Limit   int           // deltas per connection before closing (0 = unlimited)

	gapAfter atomic.Int64 // inject a gap after this many deltas on the next connection (0 = none)
	dropNext atomic.Bool  // close the next connection after the snapshot
	Conns    atomic.Int64
	Sent     atomic.Int64
	mu       sync.Mutex
}

// InjectGap makes the next connection skip one sequence number after n deltas.
func (s *Server) InjectGap(n int64) { s.gapAfter.Store(n) }

// DropNext closes the next connection right after its snapshots.
func (s *Server) DropNext() { s.dropNext.Store(true) }

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer c.CloseNow()
	s.Conns.Add(1)
	ctx := c.CloseRead(r.Context())
	g := NewGenerator(s.Seed, s.Symbols)
	for i := range s.Symbols {
		if err := c.Write(ctx, websocket.MessageText, g.Snapshot(i)); err != nil {
			return
		}
	}
	if s.dropNext.Swap(false) {
		return
	}
	gap := s.gapAfter.Swap(0)
	for n := int64(1); s.Limit == 0 || n <= int64(s.Limit); n++ {
		if gap > 0 && n == gap {
			g.SkipSeq()
		}
		if err := c.Write(ctx, websocket.MessageText, g.Delta()); err != nil {
			return
		}
		s.Sent.Add(1)
		if s.Rate > 0 {
			select {
			case <-time.After(s.Rate):
			case <-ctx.Done():
				return
			}
		}
	}
	<-ctx.Done()
}

// Wait blocks until cond() is true or the timeout elapses.
func Wait(ctx context.Context, timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		select {
		case <-time.After(5 * time.Millisecond):
		case <-ctx.Done():
			return false
		}
	}
	return cond()
}
