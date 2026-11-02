package fanout

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/eshaan2418/marketmux/internal/pipeline"
)

func upd(sym string, seq uint64) *pipeline.Update { return &pipeline.Update{Symbol: sym, Seq: seq} }

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timeout")
		}
		time.Sleep(time.Millisecond)
	}
}

// A conflating client that never reads receives only the latest update per symbol.
func TestConflateDeliversLatest(t *testing.T) {
	h := NewHub(1, 1024)
	defer h.Close()
	c := h.Subscribe(Conflate, nil, 0)
	for i := uint64(1); i <= 100; i++ {
		h.Publish(upd("BTC-USD", i))
		h.Publish(upd("ETH-USD", i))
	}
	waitFor(t, func() bool { return c.Conflated.Load() == 198 })
	got, ok := c.Next(nil, nil)
	if !ok || len(got) != 2 || got[0].U.Seq != 100 || got[1].U.Seq != 100 {
		t.Fatalf("%+v", got)
	}
}

func TestSymbolFilter(t *testing.T) {
	h := NewHub(2, 1024)
	defer h.Close()
	c := h.Subscribe(Queue, []string{"ETH-USD"}, 16)
	h.Publish(upd("BTC-USD", 1))
	h.Publish(upd("ETH-USD", 1))
	got, _ := c.Next(make([]Item, 0, 8), nil)
	if len(got) != 1 || got[0].U.Symbol != "ETH-USD" {
		t.Fatalf("%+v", got)
	}
}

func TestQueueDropsWhenFull(t *testing.T) {
	h := NewHub(1, 1024)
	defer h.Close()
	c := h.Subscribe(Queue, nil, 10)
	for i := uint64(1); i <= 50; i++ {
		h.Publish(upd("BTC-USD", i))
	}
	waitFor(t, func() bool { return c.Dropped.Load() == 40 })
	got, _ := c.Next(make([]Item, 0, 64), nil)
	if len(got) != 10 || got[0].U.Seq != 1 || got[9].U.Seq != 10 {
		t.Fatalf("queue keeps the oldest 10: %+v", got)
	}
}

func TestDisconnectPolicy(t *testing.T) {
	h := NewHub(1, 1024)
	defer h.Close()
	c := h.Subscribe(Disconnect, nil, 4)
	for i := uint64(1); i <= 5; i++ {
		h.Publish(upd("BTC-USD", i))
	}
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("slow client not disconnected")
	}
	if !c.Disconnect.Load() {
		t.Fatal("flag not set")
	}
}

// Publish never blocks, even with stuck clients and a full shard inbox.
func TestPublishNeverBlocks(t *testing.T) {
	h := NewHub(2, 8)
	defer h.Close()
	for i := 0; i < 50; i++ {
		h.Subscribe(Queue, nil, 1)
	}
	done := make(chan struct{})
	go func() {
		for i := uint64(0); i < 200_000; i++ {
			h.Publish(upd("BTC-USD", i))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Publish blocked")
	}
}

// A slow conflating client does not delay a fast one.
func TestSlowClientIsolation(t *testing.T) {
	h := NewHub(4, 65536)
	defer h.Close()
	slow := h.Subscribe(Conflate, nil, 0)
	fast := h.Subscribe(Queue, nil, 1<<17)
	_ = slow // never read
	const n = 100_000
	go func() {
		// Pace on inbox depth so the test measures isolation, not whether a producer can outrun
		// the shard workers (that case drops at the inbox by design; see TestPublishNeverBlocks).
		for i := uint64(1); i <= n; i++ {
			for i%1000 == 0 && len(h.shards[0].inbox)+len(h.shards[1].inbox) > 1<<14 {
				time.Sleep(100 * time.Microsecond)
			}
			h.Publish(upd("BTC-USD", i))
		}
	}()
	var seen uint64
	buf := make([]Item, 0, 1024)
	deadline := time.Now().Add(10 * time.Second)
	for seen < n && time.Now().Before(deadline) {
		var ok bool
		buf, ok = fast.Next(buf[:0], nil)
		if !ok {
			t.Fatal("closed")
		}
		for _, u := range buf {
			if u.U.Seq != seen+1 {
				t.Fatalf("fast client gap: got %d want %d", u.U.Seq, seen+1)
			}
			seen = u.U.Seq
		}
	}
	if seen != n || fast.Dropped.Load() != 0 || h.ShardDrops() != 0 {
		t.Fatalf("seen %d dropped %d shard drops %d", seen, fast.Dropped.Load(), h.ShardDrops())
	}
}

func TestEncodeOncePerShard(t *testing.T) {
	h := NewHub(1, 1024)
	defer h.Close()
	var calls atomic.Int64
	h.Encode = func(u *pipeline.Update) []byte { calls.Add(1); return []byte(u.Symbol) }
	a := h.Subscribe(Queue, nil, 16)
	b := h.Subscribe(Queue, nil, 16)
	h.Publish(upd("BTC-USD", 1))
	ga, _ := a.Next(make([]Item, 0, 4), nil)
	gb, _ := b.Next(make([]Item, 0, 4), nil)
	if calls.Load() != 1 || string(ga[0].Wire) != "BTC-USD" || &ga[0].Wire[0] != &gb[0].Wire[0] {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func BenchmarkPublish1000Conflate(b *testing.B) {
	h := NewHub(4, 1<<16)
	defer h.Close()
	for i := 0; i < 1000; i++ {
		h.Subscribe(Conflate, nil, 0)
	}
	u := upd("BTC-USD", 1)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		u.Seq = uint64(i)
		h.Publish(u)
	}
}
