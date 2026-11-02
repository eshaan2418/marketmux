package venue

import (
	"errors"
	"strconv"

	"github.com/valyala/fastjson"

	"github.com/eshaan2418/marketmux/internal/model"
)

var errShape = errors.New("unexpected message shape")

// appendPairLevels parses [["price","qty"], ...] (strings or numbers).
func appendPairLevels(dst []model.Level, arr []*fastjson.Value) ([]model.Level, error) {
	for _, l := range arr {
		a := l.GetArray()
		if len(a) < 2 {
			return dst, errShape
		}
		px, err := fixedOf(a[0])
		if err != nil {
			return dst, err
		}
		q, err := fixedOf(a[1])
		if err != nil {
			return dst, err
		}
		dst = append(dst, model.Level{Price: px, Qty: q})
	}
	return dst, nil
}

// appendSideChanges parses [["buy"|"sell","price","qty"], ...] into bid/ask deltas.
func appendSideChanges(bids, asks []model.Level, arr []*fastjson.Value) ([]model.Level, []model.Level, error) {
	for _, c := range arr {
		a := c.GetArray()
		if len(a) < 3 {
			return bids, asks, errShape
		}
		px, err := fixedOf(a[1])
		if err != nil {
			return bids, asks, err
		}
		q, err := fixedOf(a[2])
		if err != nil {
			return bids, asks, err
		}
		switch string(a[0].GetStringBytes()) {
		case "buy", "bid":
			bids = append(bids, model.Level{Price: px, Qty: q})
		case "sell", "ask", "offer":
			asks = append(asks, model.Level{Price: px, Qty: q})
		default:
			return bids, asks, errShape
		}
	}
	return bids, asks, nil
}

// ---------------------------------------------------------------------------------------------
// Coinbase Exchange: channels level2_batch (served as level2_50), heartbeat, matches.

// CoinbaseDepth is the book depth served on the public level2 channel ("level2_50").
const CoinbaseDepth = 50

// CoinbaseDecoder decodes Coinbase Exchange public WebSocket messages.
type CoinbaseDecoder struct {
	syms       *SymbolTable
	p          fastjson.Parser
	ev         model.Event
	bids, asks []model.Level
}

// NewCoinbaseDecoder returns a decoder for the given symbols.
func NewCoinbaseDecoder(t *SymbolTable) *CoinbaseDecoder {
	return &CoinbaseDecoder{syms: t, bids: make([]model.Level, 0, 64), asks: make([]model.Level, 0, 64)}
}

// Venue implements Decoder.
func (d *CoinbaseDecoder) Venue() model.Venue { return model.Coinbase }

// Decode implements Decoder.
func (d *CoinbaseDecoder) Decode(f *Frame, emit Emit) error {
	if f.Kind != FrameWS {
		return ErrIgnored
	}
	v, err := d.p.ParseBytes(f.Payload)
	if err != nil {
		return err
	}
	typ := string(v.GetStringBytes("type"))
	sym, ok := d.syms.Lookup(model.Coinbase, v.GetStringBytes("product_id"))
	if !ok && typ != "subscriptions" && typ != "error" {
		return ErrIgnored
	}
	switch typ {
	case "snapshot":
		if d.bids, err = appendPairLevels(d.bids[:0], v.GetArray("bids")); err != nil {
			return err
		}
		if d.asks, err = appendPairLevels(d.asks[:0], v.GetArray("asks")); err != nil {
			return err
		}
		d.ev = model.Event{Venue: model.Coinbase, Kind: model.KindSnapshot, Symbol: sym,
			Bids: d.bids, Asks: d.asks, RecvNs: f.RecvNs}
	case "l2update":
		d.bids, d.asks, err = appendSideChanges(d.bids[:0], d.asks[:0], v.GetArray("changes"))
		if err != nil {
			return err
		}
		d.ev = model.Event{Venue: model.Coinbase, Kind: model.KindDelta, Symbol: sym,
			Bids: d.bids, Asks: d.asks, RecvNs: f.RecvNs,
			ExchTsUs: parseRFC3339Micros(v.GetStringBytes("time"))}
	case "heartbeat":
		d.ev = model.Event{Venue: model.Coinbase, Kind: model.KindHeartbeat, Symbol: sym,
			Seq: v.GetUint64("sequence"), HasSeq: true, RecvNs: f.RecvNs,
			ExchTsUs: parseRFC3339Micros(v.GetStringBytes("time"))}
	case "match", "last_match":
		px, err := fixedOf(v.Get("price"))
		if err != nil {
			return err
		}
		q, err := fixedOf(v.Get("size"))
		if err != nil {
			return err
		}
		// Coinbase "side" is the maker side; the taker is the opposite.
		side := model.Ask
		if string(v.GetStringBytes("side")) == "sell" {
			side = model.Bid
		}
		d.ev = model.Event{Venue: model.Coinbase, Kind: model.KindTrade, Symbol: sym, TradePx: px,
			TradeQty: q, TradeSide: side, Seq: v.GetUint64("sequence"), HasSeq: true,
			RecvNs: f.RecvNs, ExchTsUs: parseRFC3339Micros(v.GetStringBytes("time"))}
	default:
		return ErrIgnored
	}
	emit(&d.ev)
	return nil
}

// ---------------------------------------------------------------------------------------------
// Bitstamp: diff_order_book_<pair> (absolute updates, microtimestamp) + REST order_book snapshot.

// BitstampDecoder decodes Bitstamp diff and snapshot frames. Synchronisation (dropping diffs
// that predate the snapshot) is the pipeline's job; the decoder exposes the microtimestamp as
// ExchTsUs and the snapshot's symbol via the frame payload.
type BitstampDecoder struct {
	syms       *SymbolTable
	p          fastjson.Parser
	ev         model.Event
	bids, asks []model.Level
}

// NewBitstampDecoder returns a decoder for the given symbols.
func NewBitstampDecoder(t *SymbolTable) *BitstampDecoder {
	return &BitstampDecoder{syms: t, bids: make([]model.Level, 0, 4096), asks: make([]model.Level, 0, 4096)}
}

// Venue implements Decoder.
func (d *BitstampDecoder) Venue() model.Venue { return model.Bitstamp }

// BitstampSnapshotPayload wraps a REST snapshot with its pair so the frame is self-describing:
// {"pair":"btcusd","book":{...REST body...}}.
func BitstampSnapshotPayload(pair string, body []byte) []byte {
	out := make([]byte, 0, len(body)+32)
	out = append(out, `{"pair":`...)
	out = strconv.AppendQuote(out, pair)
	out = append(out, `,"book":`...)
	out = append(out, body...)
	return append(out, '}')
}

// Decode implements Decoder.
func (d *BitstampDecoder) Decode(f *Frame, emit Emit) error {
	switch f.Kind {
	case FrameSnapshot:
		v, err := d.p.ParseBytes(f.Payload)
		if err != nil {
			return err
		}
		sym, ok := d.syms.Lookup(model.Bitstamp, v.GetStringBytes("pair"))
		if !ok {
			return ErrIgnored
		}
		bk := v.Get("book")
		if bk == nil {
			return errShape
		}
		if d.bids, err = appendPairLevels(d.bids[:0], bk.GetArray("bids")); err != nil {
			return err
		}
		if d.asks, err = appendPairLevels(d.asks[:0], bk.GetArray("asks")); err != nil {
			return err
		}
		us, _ := strconv.ParseInt(string(bk.GetStringBytes("microtimestamp")), 10, 64)
		d.ev = model.Event{Venue: model.Bitstamp, Kind: model.KindSnapshot, Symbol: sym,
			Bids: d.bids, Asks: d.asks, ExchTsUs: us, RecvNs: f.RecvNs}
		emit(&d.ev)
		return nil
	case FrameWS:
	default:
		return ErrIgnored
	}
	v, err := d.p.ParseBytes(f.Payload)
	if err != nil {
		return err
	}
	// Book diffs arrive as event "data"; trades on live_trades_<pair> as event "trade".
	if ev := string(v.GetStringBytes("event")); ev != "data" && ev != "trade" {
		return ErrIgnored
	}
	ch := v.GetStringBytes("channel")
	const diffPrefix, tradePrefix = "diff_order_book_", "live_trades_"
	data := v.Get("data")
	if data == nil {
		return errShape
	}
	switch {
	case len(ch) > len(diffPrefix) && string(ch[:len(diffPrefix)]) == diffPrefix:
		sym, ok := d.syms.Lookup(model.Bitstamp, ch[len(diffPrefix):])
		if !ok {
			return ErrIgnored
		}
		if d.bids, err = appendPairLevels(d.bids[:0], data.GetArray("bids")); err != nil {
			return err
		}
		if d.asks, err = appendPairLevels(d.asks[:0], data.GetArray("asks")); err != nil {
			return err
		}
		us, _ := strconv.ParseInt(string(data.GetStringBytes("microtimestamp")), 10, 64)
		d.ev = model.Event{Venue: model.Bitstamp, Kind: model.KindDelta, Symbol: sym,
			Bids: d.bids, Asks: d.asks, ExchTsUs: us, RecvNs: f.RecvNs}
	case len(ch) > len(tradePrefix) && string(ch[:len(tradePrefix)]) == tradePrefix:
		sym, ok := d.syms.Lookup(model.Bitstamp, ch[len(tradePrefix):])
		if !ok {
			return ErrIgnored
		}
		px, err := fixedOf(data.Get("price_str"))
		if err != nil {
			return err
		}
		q, err := fixedOf(data.Get("amount_str"))
		if err != nil {
			return err
		}
		side := model.Bid // type 0 = buy (taker bought)
		if data.GetInt("type") == 1 {
			side = model.Ask
		}
		us, _ := strconv.ParseInt(string(data.GetStringBytes("microtimestamp")), 10, 64)
		d.ev = model.Event{Venue: model.Bitstamp, Kind: model.KindTrade, Symbol: sym, TradePx: px,
			TradeQty: q, TradeSide: side, ExchTsUs: us, RecvNs: f.RecvNs}
	default:
		return ErrIgnored
	}
	emit(&d.ev)
	return nil
}

// ---------------------------------------------------------------------------------------------
// Gemini market data v2: l2_updates (first per symbol after subscribe is the full book), trade.

// GeminiDecoder decodes Gemini v2 market-data messages.
type GeminiDecoder struct {
	syms       *SymbolTable
	p          fastjson.Parser
	ev         model.Event
	bids, asks []model.Level
}

// NewGeminiDecoder returns a decoder for the given symbols.
func NewGeminiDecoder(t *SymbolTable) *GeminiDecoder {
	return &GeminiDecoder{syms: t, bids: make([]model.Level, 0, 4096), asks: make([]model.Level, 0, 4096)}
}

// Venue implements Decoder.
func (d *GeminiDecoder) Venue() model.Venue { return model.Gemini }

// Decode implements Decoder.
func (d *GeminiDecoder) Decode(f *Frame, emit Emit) error {
	if f.Kind != FrameWS {
		return ErrIgnored
	}
	v, err := d.p.ParseBytes(f.Payload)
	if err != nil {
		return err
	}
	sym, ok := d.syms.Lookup(model.Gemini, v.GetStringBytes("symbol"))
	if !ok {
		return ErrIgnored
	}
	switch string(v.GetStringBytes("type")) {
	case "l2_updates":
		d.bids, d.asks, err = appendSideChanges(d.bids[:0], d.asks[:0], v.GetArray("changes"))
		if err != nil {
			return err
		}
		kind := model.KindDelta
		// The initial message per subscription carries the full book plus recent trades and
		// auction events; later updates carry only "changes".
		if v.Exists("trades") || v.Exists("auction_events") {
			kind = model.KindSnapshot
		}
		d.ev = model.Event{Venue: model.Gemini, Kind: kind, Symbol: sym, Bids: d.bids, Asks: d.asks,
			RecvNs: f.RecvNs}
	case "trade":
		px, err := fixedOf(v.Get("price"))
		if err != nil {
			return err
		}
		q, err := fixedOf(v.Get("quantity"))
		if err != nil {
			return err
		}
		side := model.Bid
		if string(v.GetStringBytes("side")) == "sell" {
			side = model.Ask
		}
		d.ev = model.Event{Venue: model.Gemini, Kind: model.KindTrade, Symbol: sym, TradePx: px,
			TradeQty: q, TradeSide: side, RecvNs: f.RecvNs, ExchTsUs: v.GetInt64("timestamp") * 1000}
	default:
		return ErrIgnored
	}
	emit(&d.ev)
	return nil
}

// ---------------------------------------------------------------------------------------------
// Synthetic venue (tests, benchmarks, fake-venue server). Compact JSON:
//   {"t":"s"|"d","s":"BTC-USD","b":[["p","q"]],"a":[...],"q":seq}

// SyntheticDecoder decodes the synthetic venue protocol, which carries an explicit sequence
// number per message so the pipeline's gap detection can be exercised deterministically.
type SyntheticDecoder struct {
	syms       *SymbolTable
	p          fastjson.Parser
	ev         model.Event
	bids, asks []model.Level
}

// NewSyntheticDecoder returns a decoder for the given symbols.
func NewSyntheticDecoder(t *SymbolTable) *SyntheticDecoder {
	return &SyntheticDecoder{syms: t, bids: make([]model.Level, 0, 64), asks: make([]model.Level, 0, 64)}
}

// Venue implements Decoder.
func (d *SyntheticDecoder) Venue() model.Venue { return model.Synthetic }

// Decode implements Decoder.
func (d *SyntheticDecoder) Decode(f *Frame, emit Emit) error {
	if f.Kind != FrameWS {
		return ErrIgnored
	}
	v, err := d.p.ParseBytes(f.Payload)
	if err != nil {
		return err
	}
	sym, ok := d.syms.Lookup(model.Synthetic, v.GetStringBytes("s"))
	if !ok {
		return ErrIgnored
	}
	kind := model.KindDelta
	switch string(v.GetStringBytes("t")) {
	case "s":
		kind = model.KindSnapshot
	case "d":
	default:
		return errShape
	}
	if d.bids, err = appendPairLevels(d.bids[:0], v.GetArray("b")); err != nil {
		return err
	}
	if d.asks, err = appendPairLevels(d.asks[:0], v.GetArray("a")); err != nil {
		return err
	}
	d.ev = model.Event{Venue: model.Synthetic, Kind: kind, Symbol: sym, Bids: d.bids, Asks: d.asks,
		Seq: v.GetUint64("q"), HasSeq: true, RecvNs: f.RecvNs, ExchTsUs: v.GetInt64("x")}
	emit(&d.ev)
	return nil
}

// NewDecoder returns the decoder for a venue.
func NewDecoder(v model.Venue, t *SymbolTable) Decoder {
	switch v {
	case model.Kraken:
		return NewKrakenDecoder(t)
	case model.Coinbase:
		return NewCoinbaseDecoder(t)
	case model.Bitstamp:
		return NewBitstampDecoder(t)
	case model.Gemini:
		return NewGeminiDecoder(t)
	case model.Synthetic:
		return NewSyntheticDecoder(t)
	}
	return nil
}
