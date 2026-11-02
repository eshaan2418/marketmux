package venue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/eshaan2418/marketmux/internal/model"
)

// Clock returns monotonic nanoseconds since process start.
type Clock func() int64

// Spec describes how to talk to one venue.
type Spec struct {
	Venue model.Venue
	URL   string
	// Meta runs before each connection; its result is recorded in the FrameConnected payload.
	Meta func(ctx context.Context, syms []Symbol) ([]byte, error)
	// Subscribe returns the messages to send after connecting.
	Subscribe func(syms []Symbol) [][]byte
	// AfterSubscribe runs after subscribing (Bitstamp fetches REST snapshots here).
	AfterSubscribe func(ctx context.Context, s *Session, syms []Symbol) error
	// Resync handles a book-level resync for one symbol. nil => reconnect the session.
	Resync func(ctx context.Context, s *Session, sym Symbol) error
	// IdleTimeout: reconnect if nothing is received for this long.
	IdleTimeout time.Duration
}

// Session maintains one WebSocket connection to a venue with reconnect backoff and emits every
// observation as a Frame.
type Session struct {
	spec  Spec
	syms  []Symbol
	out   chan<- Frame
	clock Clock
	log   *slog.Logger
	http  *http.Client

	mu   sync.Mutex
	conn *websocket.Conn

	resyncCh   chan string
	lastResync map[string]time.Time
	reconnect  chan struct{}

	// Counters (atomic; read by metrics).
	Connects   atomic.Uint64
	Frames     atomic.Uint64
	Bytes      atomic.Uint64
	ResyncsReq atomic.Uint64
	ResyncsRL  atomic.Uint64 // resync requests suppressed by rate limiting
}

// NewSession creates a session; call Run to start it.
func NewSession(spec Spec, syms []Symbol, out chan<- Frame, clock Clock, log *slog.Logger) *Session {
	if spec.IdleTimeout == 0 {
		spec.IdleTimeout = 30 * time.Second
	}
	return &Session{spec: spec, syms: syms, out: out, clock: clock, log: log.With("venue", spec.Venue.String()),
		http: &http.Client{Timeout: 15 * time.Second}, resyncCh: make(chan string, 64),
		lastResync: map[string]time.Time{}, reconnect: make(chan struct{}, 1)}
}

// Venue returns the session's venue.
func (s *Session) Venue() model.Venue { return s.spec.Venue }

// RequestResync asks for a resync of one symbol (non-blocking; coalesced and rate-limited).
func (s *Session) RequestResync(sym string) {
	select {
	case s.resyncCh <- sym:
	default:
	}
}

// Send writes a message on the current connection.
func (s *Session) Send(ctx context.Context, msg []byte) error {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	if c == nil {
		return errors.New("not connected")
	}
	return c.Write(ctx, websocket.MessageText, msg)
}

// Emit pushes a frame to the engine (blocking: TCP back-pressure is preferable to dropping
// market data; the engine is orders of magnitude faster than any venue feed).
func (s *Session) Emit(ctx context.Context, kind FrameKind, payload []byte) {
	f := Frame{Kind: kind, Venue: s.spec.Venue, RecvNs: s.clock(), Payload: payload}
	select {
	case s.out <- f:
	case <-ctx.Done():
	}
}

// HTTPGet fetches a URL (for snapshots / metadata).
func (s *Session) HTTPGet(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "marketmux/1.0 (market-data research)")
	resp, err := s.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s: %s", url, resp.Status)
	}
	return io.ReadAll(io.LimitReader(resp.Body, 32<<20))
}

// Run connects and reconnects until ctx is cancelled.
func (s *Session) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := s.runOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		s.Emit(ctx, FrameDisconnected, nil)
		if time.Since(start) > time.Minute {
			backoff = time.Second // the connection was healthy; reset backoff
		}
		jitter := time.Duration(rand.Int63n(int64(backoff / 2)))
		s.log.Warn("session ended; reconnecting", "err", err, "in", backoff+jitter)
		select {
		case <-time.After(backoff + jitter):
		case <-ctx.Done():
			return
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (s *Session) runOnce(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var meta []byte
	if s.spec.Meta != nil {
		m, err := s.spec.Meta(ctx, s.syms)
		if err != nil {
			return fmt.Errorf("meta: %w", err)
		}
		meta = m
	}
	dctx, dcancel := context.WithTimeout(ctx, 15*time.Second)
	c, _, err := websocket.Dial(dctx, s.spec.URL, nil)
	dcancel()
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	c.SetReadLimit(32 << 20)
	s.mu.Lock()
	s.conn = c
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.conn = nil
		s.mu.Unlock()
		_ = c.CloseNow()
	}()
	s.Connects.Add(1)
	s.Emit(ctx, FrameConnected, meta)
	for _, m := range s.spec.Subscribe(s.syms) {
		if err := c.Write(ctx, websocket.MessageText, m); err != nil {
			return fmt.Errorf("subscribe: %w", err)
		}
	}
	errc := make(chan error, 2)
	go func() { errc <- s.readLoop(ctx, c) }()
	if s.spec.AfterSubscribe != nil {
		go func() {
			if err := s.spec.AfterSubscribe(ctx, s, s.syms); err != nil {
				errc <- fmt.Errorf("after subscribe: %w", err)
			}
		}()
	}
	for {
		select {
		case err := <-errc:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-s.reconnect:
			return errors.New("reconnect requested")
		case sym := <-s.resyncCh:
			s.handleResync(ctx, sym)
		}
	}
}

func (s *Session) handleResync(ctx context.Context, canon string) {
	// Rate limit: at most one resync per symbol per 5 s, so a persistently inconsistent feed
	// cannot turn into a request storm against the venue.
	if t, ok := s.lastResync[canon]; ok && time.Since(t) < 5*time.Second {
		s.ResyncsRL.Add(1)
		return
	}
	s.lastResync[canon] = time.Now()
	s.ResyncsReq.Add(1)
	var sym Symbol
	for _, x := range s.syms {
		if x.Canonical == canon {
			sym = x
		}
	}
	if s.spec.Resync == nil || sym.Canonical == "" {
		select {
		case s.reconnect <- struct{}{}:
		default:
		}
		return
	}
	go func() {
		if err := s.spec.Resync(ctx, s, sym); err != nil {
			s.log.Warn("resync failed; reconnecting", "symbol", canon, "err", err)
			select {
			case s.reconnect <- struct{}{}:
			default:
			}
		}
	}()
}

func (s *Session) readLoop(ctx context.Context, c *websocket.Conn) error {
	for {
		rctx, cancel := context.WithTimeout(ctx, s.spec.IdleTimeout)
		_, b, err := c.Read(rctx)
		cancel()
		if err != nil {
			return err
		}
		s.Frames.Add(1)
		s.Bytes.Add(uint64(len(b)))
		s.Emit(ctx, FrameWS, b)
	}
}

// ---------------------------------------------------------------------------------------------
// Venue specs.

func jsonMsg(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// KrakenSpec subscribes to book (depth 10, with checksums) and trades on Kraken WS v2.
func KrakenSpec() Spec {
	return Spec{
		Venue: model.Kraken,
		URL:   "wss://ws.kraken.com/v2",
		Meta: func(ctx context.Context, syms []Symbol) ([]byte, error) {
			// Precisions for checksum strings come from REST AssetPairs (pair_decimals/lot_decimals).
			cl := &http.Client{Timeout: 15 * time.Second}
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.kraken.com/0/public/AssetPairs", nil)
			resp, err := cl.Do(req)
			if err != nil {
				return nil, err
			}
			defer func() { _ = resp.Body.Close() }()
			var body struct {
				Result map[string]struct {
					WSName       string `json:"wsname"`
					PairDecimals int    `json:"pair_decimals"`
					LotDecimals  int    `json:"lot_decimals"`
				} `json:"result"`
			}
			if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
				return nil, err
			}
			meta := KrakenMeta{Precision: map[string]KrakenPrecision{}}
			for _, p := range body.Result {
				ws := strings.NewReplacer("XBT/", "BTC/", "XDG/", "DOGE/").Replace(p.WSName)
				for _, s := range syms {
					if s.Kraken == ws {
						meta.Precision[s.Canonical] = KrakenPrecision{Price: p.PairDecimals, Qty: p.LotDecimals}
					}
				}
			}
			return json.Marshal(meta)
		},
		Subscribe: func(syms []Symbol) [][]byte {
			names := make([]string, len(syms))
			for i, s := range syms {
				names[i] = s.Kraken
			}
			return [][]byte{
				jsonMsg(map[string]any{"method": "subscribe", "params": map[string]any{"channel": "book", "symbol": names, "depth": KrakenDepth}}),
				jsonMsg(map[string]any{"method": "subscribe", "params": map[string]any{"channel": "trade", "symbol": names, "snapshot": false}}),
			}
		},
		Resync: func(ctx context.Context, s *Session, sym Symbol) error {
			// Unsubscribe + subscribe the one symbol: Kraken sends a fresh snapshot.
			p := map[string]any{"channel": "book", "symbol": []string{sym.Kraken}, "depth": KrakenDepth}
			if err := s.Send(ctx, jsonMsg(map[string]any{"method": "unsubscribe", "params": p})); err != nil {
				return err
			}
			return s.Send(ctx, jsonMsg(map[string]any{"method": "subscribe", "params": p}))
		},
		IdleTimeout: 20 * time.Second, // heartbeats arrive every ~1 s
	}
}

// CoinbaseSpec subscribes to level2_batch (served as level2_50), heartbeat and matches.
func CoinbaseSpec() Spec {
	return Spec{
		Venue: model.Coinbase,
		URL:   "wss://ws-feed.exchange.coinbase.com",
		Subscribe: func(syms []Symbol) [][]byte {
			ids := make([]string, len(syms))
			for i, s := range syms {
				ids[i] = s.Coinbase
			}
			return [][]byte{jsonMsg(map[string]any{"type": "subscribe", "product_ids": ids,
				"channels": []string{"level2_batch", "heartbeat", "matches"}})}
		},
		Resync: func(ctx context.Context, s *Session, sym Symbol) error {
			msg := map[string]any{"product_ids": []string{sym.Coinbase}, "channels": []string{"level2_batch"}}
			msg["type"] = "unsubscribe"
			if err := s.Send(ctx, jsonMsg(msg)); err != nil {
				return err
			}
			msg["type"] = "subscribe"
			return s.Send(ctx, jsonMsg(msg))
		},
		IdleTimeout: 20 * time.Second,
	}
}

// BitstampSpec subscribes to diff order book + live trades, then fetches REST snapshots.
func BitstampSpec() Spec {
	snapshot := func(ctx context.Context, s *Session, sym Symbol) error {
		body, err := s.HTTPGet(ctx, "https://www.bitstamp.net/api/v2/order_book/"+sym.Bitstamp+"/")
		if err != nil {
			return err
		}
		s.Emit(ctx, FrameSnapshot, BitstampSnapshotPayload(sym.Bitstamp, body))
		return nil
	}
	return Spec{
		Venue: model.Bitstamp,
		URL:   "wss://ws.bitstamp.net",
		Subscribe: func(syms []Symbol) [][]byte {
			out := make([][]byte, 0, 2*len(syms))
			for _, s := range syms {
				out = append(out,
					jsonMsg(map[string]any{"event": "bts:subscribe", "data": map[string]string{"channel": "diff_order_book_" + s.Bitstamp}}),
					jsonMsg(map[string]any{"event": "bts:subscribe", "data": map[string]string{"channel": "live_trades_" + s.Bitstamp}}))
			}
			return out
		},
		AfterSubscribe: func(ctx context.Context, s *Session, syms []Symbol) error {
			// Let diffs start buffering so the snapshot can be bridged, then fetch snapshots
			// sequentially (polite to the REST API).
			select {
			case <-time.After(time.Second):
			case <-ctx.Done():
				return nil
			}
			for _, sym := range syms {
				if err := snapshot(ctx, s, sym); err != nil {
					return err
				}
			}
			return nil
		},
		Resync:      snapshot,
		IdleTimeout: 60 * time.Second, // quiet pairs can be silent for a while
	}
}

// GeminiSpec subscribes to l2 (book + trades) on Gemini market data v2.
func GeminiSpec() Spec {
	return Spec{
		Venue: model.Gemini,
		URL:   "wss://api.gemini.com/v2/marketdata",
		Subscribe: func(syms []Symbol) [][]byte {
			names := make([]string, len(syms))
			for i, s := range syms {
				names[i] = s.Gemini
			}
			return [][]byte{jsonMsg(map[string]any{"type": "subscribe",
				"subscriptions": []map[string]any{{"name": "l2", "symbols": names}}})}
		},
		IdleTimeout: 60 * time.Second,
	}
}

// SpecFor returns the spec for a venue name.
func SpecFor(v model.Venue) (Spec, bool) {
	switch v {
	case model.Kraken:
		return KrakenSpec(), true
	case model.Coinbase:
		return CoinbaseSpec(), true
	case model.Bitstamp:
		return BitstampSpec(), true
	case model.Gemini:
		return GeminiSpec(), true
	}
	return Spec{}, false
}

// SyntheticSpec connects to a fake venue (tests, benchmarks). No per-symbol resync: a sequence
// gap reconnects the session, which re-sends snapshots.
func SyntheticSpec(url string) Spec {
	return Spec{Venue: model.Synthetic, URL: url, Subscribe: func([]Symbol) [][]byte { return nil },
		IdleTimeout: 10 * time.Second}
}
