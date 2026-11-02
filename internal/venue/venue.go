// Package venue contains per-venue protocol decoders (pure functions over captured bytes, so
// they are deterministic, fuzzable and replayable) and the WebSocket sessions that feed them.
package venue

import (
	"errors"
	"fmt"
	"strings"

	"github.com/eshaan2418/marketmux/internal/model"
)

// FrameKind tags what a captured frame contains.
type FrameKind uint8

// Frame kinds. Everything a venue session observes is recorded as a frame, including connection
// boundaries and REST snapshots, so replay reproduces resync behaviour exactly.
const (
	FrameWS           FrameKind = iota + 1 // WebSocket message payload
	FrameSnapshot                          // REST snapshot payload (Bitstamp)
	FrameConnected                         // session (re)connected; payload = meta JSON (e.g. precisions)
	FrameDisconnected                      // session lost; books for the venue are now invalid
)

func (k FrameKind) String() string {
	switch k {
	case FrameWS:
		return "ws"
	case FrameSnapshot:
		return "snapshot"
	case FrameConnected:
		return "connected"
	case FrameDisconnected:
		return "disconnected"
	}
	return fmt.Sprintf("kind(%d)", k)
}

// Frame is one raw input to the pipeline.
type Frame struct {
	Kind    FrameKind
	Venue   model.Venue
	RecvNs  int64 // monotonic ns since process start when received
	Payload []byte
}

// Emit receives decoded events. The event (and its slices) is only valid during the call.
type Emit func(*model.Event)

// Decoder turns frames of one venue into normalised events.
type Decoder interface {
	Venue() model.Venue
	Decode(f *Frame, emit Emit) error
}

// ErrIgnored is returned for frames that are valid but carry no market data (acks, status).
var ErrIgnored = errors.New("ignored frame")

// Symbol maps a canonical symbol ("BTC-USD") to venue-specific spellings.
type Symbol struct {
	Canonical string
	Kraken    string // "BTC/USD"
	Coinbase  string // "BTC-USD"
	Bitstamp  string // "btcusd"
	Gemini    string // "BTCUSD"
}

// NewSymbol derives venue spellings from "BASE-QUOTE".
func NewSymbol(canonical string) (Symbol, error) {
	parts := strings.Split(strings.ToUpper(canonical), "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Symbol{}, fmt.Errorf("symbol %q: want BASE-QUOTE", canonical)
	}
	b, q := parts[0], parts[1]
	return Symbol{
		Canonical: b + "-" + q,
		Kraken:    b + "/" + q,
		Coinbase:  b + "-" + q,
		Bitstamp:  strings.ToLower(b + q),
		Gemini:    b + q,
	}, nil
}

// SymbolTable resolves venue spellings back to canonical names without allocating.
type SymbolTable struct {
	byVenue [model.NumVenues]map[string]string
	list    []Symbol
}

// NewSymbolTable builds the lookup for the given canonical symbols.
func NewSymbolTable(canon []string) (*SymbolTable, error) {
	t := &SymbolTable{}
	for v := range t.byVenue {
		t.byVenue[v] = map[string]string{}
	}
	for _, c := range canon {
		s, err := NewSymbol(c)
		if err != nil {
			return nil, err
		}
		t.list = append(t.list, s)
		t.byVenue[model.Kraken][s.Kraken] = s.Canonical
		t.byVenue[model.Coinbase][s.Coinbase] = s.Canonical
		t.byVenue[model.Bitstamp][s.Bitstamp] = s.Canonical
		t.byVenue[model.Gemini][s.Gemini] = s.Canonical
		t.byVenue[model.Synthetic][s.Canonical] = s.Canonical
	}
	return t, nil
}

// Lookup returns the canonical symbol for a venue spelling. The []byte->string conversion in a
// map index expression does not allocate.
func (t *SymbolTable) Lookup(v model.Venue, venueSym []byte) (string, bool) {
	if int(v) >= len(t.byVenue) {
		return "", false
	}
	c, ok := t.byVenue[v][string(venueSym)]
	return c, ok
}

// Symbols returns the configured symbols.
func (t *SymbolTable) Symbols() []Symbol { return t.list }
