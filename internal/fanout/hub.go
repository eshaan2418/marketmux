// Package fanout distributes consolidated updates to many clients with per-client back-pressure.
//
// The engine calls Hub.Publish on its own goroutine, so Publish must never block on a client.
// Clients are sharded across W worker goroutines; Publish copies the update (a value type) into
// each shard's bounded inbox. A shard worker offers the update to each of its clients according to
// the client's policy:
//
//   - Conflate: one slot per symbol holding the latest update plus a dirty list. A slow client
//     skips intermediate updates but always receives the latest state; memory per client is
//     bounded by the number of symbols. Best default for BBO consumers.
//   - Queue: a bounded FIFO; when full the update is dropped and counted (gap visible to the
//     client through the per-symbol Seq).
//   - Disconnect: a bounded FIFO; when full the client is closed (it must reconnect and resync).
//
// Encoding happens once per update per shard (Hub.Encode), not once per client: the shard worker
// attaches the encoded bytes to the item it offers, and every client of the shard shares them
// (the bytes are immutable once created).
//
// If a shard inbox itself is full (all of a shard's clients are being served too slowly to keep
// up with offers), the update is dropped for that shard and counted: the engine is never stalled.
package fanout

import (
	"sync"
	"sync/atomic"

	"github.com/eshaan2418/marketmux/internal/pipeline"
)

// Policy is a slow-consumer policy.
type Policy uint8

// Policies.
const (
	Conflate Policy = iota
	Queue
	Disconnect
)

// ParsePolicy maps a name to a Policy.
func ParsePolicy(s string) (Policy, bool) {
	switch s {
	case "", "conflate":
		return Conflate, true
	case "queue", "drop":
		return Queue, true
	case "disconnect":
		return Disconnect, true
	}
	return 0, false
}

func (p Policy) String() string {
	return [...]string{"conflate", "queue", "disconnect"}[p]
}

// Item is what clients receive: the update plus its shared, immutable wire encoding (nil if the
// hub has no encoder).
type Item struct {
	U    pipeline.Update
	Wire []byte
}

// Client is one subscriber. The transport goroutine calls Next to obtain updates to send.
type Client struct {
	policy  Policy
	symbols map[string]bool // nil = all

	mu     sync.Mutex
	slots  map[string]Item
	dirty  []string
	queue  chan Item
	notify chan struct{}
	closed chan struct{}
	once   sync.Once

	Delivered  atomic.Uint64
	Conflated  atomic.Uint64 // updates overwritten before delivery
	Dropped    atomic.Uint64
	Disconnect atomic.Bool
}

// Done is closed when the hub disconnects the client (Disconnect policy) or the client is removed.
func (c *Client) Done() <-chan struct{} { return c.closed }

func (c *Client) close() { c.once.Do(func() { close(c.closed) }) }

func (c *Client) wants(sym string) bool { return c.symbols == nil || c.symbols[sym] }

// offer is called by the client's shard worker only.
func (c *Client) offer(it *Item) {
	if !c.wants(it.U.Symbol) {
		return
	}
	switch c.policy {
	case Conflate:
		c.mu.Lock()
		if _, pending := c.slots[it.U.Symbol]; pending {
			c.Conflated.Add(1)
		} else {
			c.dirty = append(c.dirty, it.U.Symbol)
		}
		c.slots[it.U.Symbol] = *it
		c.mu.Unlock()
		select {
		case c.notify <- struct{}{}:
		default:
		}
	case Queue, Disconnect:
		select {
		case c.queue <- *it:
		default:
			c.Dropped.Add(1)
			if c.policy == Disconnect {
				c.Disconnect.Store(true)
				c.close()
			}
		}
	}
}

// Next blocks until updates are available (or the client is closed / stop fires) and appends
// them to dst. For Conflate it returns the latest update of every dirty symbol.
func (c *Client) Next(dst []Item, stop <-chan struct{}) ([]Item, bool) {
	if c.policy == Conflate {
		for {
			c.mu.Lock()
			if len(c.dirty) > 0 {
				for _, s := range c.dirty {
					dst = append(dst, c.slots[s])
					delete(c.slots, s)
				}
				c.dirty = c.dirty[:0]
				c.mu.Unlock()
				c.Delivered.Add(uint64(len(dst)))
				return dst, true
			}
			c.mu.Unlock()
			select {
			case <-c.notify:
			case <-c.closed:
				return dst, false
			case <-stop:
				return dst, false
			}
		}
	}
	select {
	case u := <-c.queue:
		dst = append(dst, u)
	case <-c.closed:
		return dst, false
	case <-stop:
		return dst, false
	}
	// Drain whatever else is ready, to batch writes.
	for len(dst) < cap(dst) {
		select {
		case u := <-c.queue:
			dst = append(dst, u)
		default:
			c.Delivered.Add(uint64(len(dst)))
			return dst, true
		}
	}
	c.Delivered.Add(uint64(len(dst)))
	return dst, true
}

type shard struct {
	inbox   chan pipeline.Update
	mu      sync.RWMutex
	clients map[*Client]struct{}
	dropped atomic.Uint64
}

// Hub fans updates out to clients.
type Hub struct {
	// Encode, if set, produces the wire bytes for an update once per shard. It must return a
	// fresh slice (the result is shared by clients and never mutated).
	Encode func(*pipeline.Update) []byte

	shards []*shard
	next   atomic.Uint64
	stop   chan struct{}
	wg     sync.WaitGroup

	Published atomic.Uint64
	Clients   atomic.Int64
	// Cumulative counters of clients that have already left (so totals survive disconnects).
	goneDelivered, goneConflated, goneDropped, goneDisconnected atomic.Uint64
	// WriteTimeouts counts clients cut off because a single write blocked past the deadline.
	WriteTimeouts atomic.Uint64
}

// NewHub starts `workers` shard workers with inboxes of `inbox` updates each.
func NewHub(workers, inbox int) *Hub {
	if workers < 1 {
		workers = 1
	}
	h := &Hub{stop: make(chan struct{})}
	for i := 0; i < workers; i++ {
		s := &shard{inbox: make(chan pipeline.Update, inbox), clients: map[*Client]struct{}{}}
		h.shards = append(h.shards, s)
		h.wg.Add(1)
		go h.run(s)
	}
	return h
}

func (h *Hub) run(s *shard) {
	defer h.wg.Done()
	for {
		select {
		case u := <-s.inbox:
			it := Item{U: u}
			s.mu.RLock()
			if len(s.clients) > 0 && h.Encode != nil {
				it.Wire = h.Encode(&it.U)
			}
			for c := range s.clients {
				c.offer(&it)
			}
			s.mu.RUnlock()
		case <-h.stop:
			return
		}
	}
}

// Publish implements pipeline.Publisher. Never blocks.
func (h *Hub) Publish(u *pipeline.Update) {
	h.Published.Add(1)
	for _, s := range h.shards {
		select {
		case s.inbox <- *u:
		default:
			s.dropped.Add(1)
		}
	}
}

// ShardDrops is the number of updates dropped because a shard inbox was full.
func (h *Hub) ShardDrops() uint64 {
	var n uint64
	for _, s := range h.shards {
		n += s.dropped.Load()
	}
	return n
}

// Totals sums the per-client policy counters, including clients that have disconnected.
type Totals struct {
	Clients, Delivered, Conflated, Dropped, Disconnected, WriteTimeouts uint64
	// MaxPending is the largest number of undelivered items held for any single client: bounded
	// by the symbol count under conflation, by the queue length otherwise.
	MaxPending int
}

// Totals returns current policy counters (for metrics and benchmarks).
func (h *Hub) Totals() Totals {
	t := Totals{Delivered: h.goneDelivered.Load(), Conflated: h.goneConflated.Load(),
		Dropped: h.goneDropped.Load(), Disconnected: h.goneDisconnected.Load(), WriteTimeouts: h.WriteTimeouts.Load()}
	for _, s := range h.shards {
		s.mu.RLock()
		for c := range s.clients {
			t.Clients++
			t.Delivered += c.Delivered.Load()
			t.Conflated += c.Conflated.Load()
			t.Dropped += c.Dropped.Load()
			if c.Disconnect.Load() {
				t.Disconnected++
			}
			if p := c.pending(); p > t.MaxPending {
				t.MaxPending = p
			}
		}
		s.mu.RUnlock()
	}
	return t
}

func (c *Client) pending() int {
	if c.policy == Conflate {
		c.mu.Lock()
		defer c.mu.Unlock()
		return len(c.dirty)
	}
	return len(c.queue)
}

// Subscribe registers a client. queueLen applies to Queue/Disconnect.
func (h *Hub) Subscribe(p Policy, symbols []string, queueLen int) *Client {
	c := &Client{policy: p, notify: make(chan struct{}, 1), closed: make(chan struct{})}
	if len(symbols) > 0 {
		c.symbols = map[string]bool{}
		for _, s := range symbols {
			c.symbols[s] = true
		}
	}
	if p == Conflate {
		c.slots = map[string]Item{}
	} else {
		if queueLen <= 0 {
			queueLen = 1024
		}
		c.queue = make(chan Item, queueLen)
	}
	s := h.shards[h.next.Add(1)%uint64(len(h.shards))]
	s.mu.Lock()
	s.clients[c] = struct{}{}
	s.mu.Unlock()
	h.Clients.Add(1)
	return c
}

// Unsubscribe removes a client.
func (h *Hub) Unsubscribe(c *Client) {
	for _, s := range h.shards {
		s.mu.Lock()
		if _, ok := s.clients[c]; ok {
			delete(s.clients, c)
			h.Clients.Add(-1)
			h.goneDelivered.Add(c.Delivered.Load())
			h.goneConflated.Add(c.Conflated.Load())
			h.goneDropped.Add(c.Dropped.Load())
			if c.Disconnect.Load() {
				h.goneDisconnected.Add(1)
			}
		}
		s.mu.Unlock()
	}
	c.close()
}

// Close stops the workers.
func (h *Hub) Close() {
	close(h.stop)
	h.wg.Wait()
}
