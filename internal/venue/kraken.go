package venue

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/valyala/fastjson"

	"github.com/eshaan2418/marketmux/internal/model"
)

// KrakenDepth is the subscribed book depth. The v2 checksum covers the top 10 levels per side and
// the client must truncate its book to the subscribed depth after every update.
const KrakenDepth = 10

// KrakenPrecision holds per-symbol decimal precisions needed to reproduce Kraken's checksum
// strings (from REST AssetPairs: pair_decimals, lot_decimals).
type KrakenPrecision struct {
	Price int `json:"price"`
	Qty   int `json:"qty"`
}

// KrakenMeta is the payload of a Kraken FrameConnected frame.
type KrakenMeta struct {
	Precision map[string]KrakenPrecision `json:"precision"` // canonical symbol -> precision
}

// KrakenDecoder decodes Kraken WebSocket v2 book and trade messages.
type KrakenDecoder struct {
	syms *SymbolTable
	prec map[string]KrakenPrecision
	p    fastjson.Parser
	ev   model.Event
	bids []model.Level
	asks []model.Level
	num  numBuf
}

// NewKrakenDecoder returns a decoder for the given symbols.
func NewKrakenDecoder(t *SymbolTable) *KrakenDecoder {
	return &KrakenDecoder{syms: t, prec: map[string]KrakenPrecision{},
		bids: make([]model.Level, 0, 64), asks: make([]model.Level, 0, 64)}
}

// Venue implements Decoder.
func (d *KrakenDecoder) Venue() model.Venue { return model.Kraken }

// Precision returns the checksum precision for a canonical symbol.
func (d *KrakenDecoder) Precision(sym string) (KrakenPrecision, bool) {
	p, ok := d.prec[sym]
	return p, ok
}

var errKrakenShape = errors.New("kraken: unexpected message shape")

// Decode implements Decoder.
func (d *KrakenDecoder) Decode(f *Frame, emit Emit) error {
	if f.Kind == FrameConnected {
		if len(f.Payload) > 0 {
			var m KrakenMeta
			if err := json.Unmarshal(f.Payload, &m); err != nil {
				return fmt.Errorf("kraken meta: %w", err)
			}
			for k, v := range m.Precision {
				d.prec[k] = v
			}
		}
		return ErrIgnored
	}
	if f.Kind != FrameWS {
		return ErrIgnored
	}
	v, err := d.p.ParseBytes(f.Payload)
	if err != nil {
		return err
	}
	ch := v.GetStringBytes("channel")
	switch string(ch) {
	case "book":
	case "trade":
		return d.decodeTrades(v, f, emit)
	case "heartbeat":
		d.ev = model.Event{Venue: model.Kraken, Kind: model.KindHeartbeat, RecvNs: f.RecvNs}
		emit(&d.ev)
		return nil
	default:
		return ErrIgnored // status, subscribe acks, ...
	}
	typ := v.GetStringBytes("type")
	kind := model.KindDelta
	switch string(typ) {
	case "snapshot":
		kind = model.KindSnapshot
	case "update":
	default:
		return errKrakenShape
	}
	data := v.GetArray("data")
	for _, item := range data {
		sym, ok := d.syms.Lookup(model.Kraken, item.GetStringBytes("symbol"))
		if !ok {
			continue
		}
		d.bids, err = appendObjLevels(d.bids[:0], item.GetArray("bids"), &d.num)
		if err != nil {
			return err
		}
		d.asks, err = appendObjLevels(d.asks[:0], item.GetArray("asks"), &d.num)
		if err != nil {
			return err
		}
		d.ev = model.Event{Venue: model.Kraken, Kind: kind, Symbol: sym, Bids: d.bids, Asks: d.asks,
			RecvNs: f.RecvNs}
		if cs := item.Get("checksum"); cs != nil {
			u, err := cs.Uint64()
			if err != nil {
				return err
			}
			d.ev.Checksum, d.ev.HasCRC = uint32(u), true
		}
		if ts := item.GetStringBytes("timestamp"); len(ts) > 0 {
			d.ev.ExchTsUs = parseRFC3339Micros(ts)
		}
		emit(&d.ev)
	}
	return nil
}

func (d *KrakenDecoder) decodeTrades(v *fastjson.Value, f *Frame, emit Emit) error {
	for _, item := range v.GetArray("data") {
		sym, ok := d.syms.Lookup(model.Kraken, item.GetStringBytes("symbol"))
		if !ok {
			continue
		}
		px, err := d.num.fixed(item.Get("price"))
		if err != nil {
			return err
		}
		q, err := d.num.fixed(item.Get("qty"))
		if err != nil {
			return err
		}
		side := model.Bid
		if string(item.GetStringBytes("side")) == "sell" {
			side = model.Ask
		}
		d.ev = model.Event{Venue: model.Kraken, Kind: model.KindTrade, Symbol: sym, TradePx: px,
			TradeQty: q, TradeSide: side, RecvNs: f.RecvNs,
			ExchTsUs: parseRFC3339Micros(item.GetStringBytes("timestamp"))}
		emit(&d.ev)
	}
	return nil
}

// appendObjLevels parses [{"price":p,"qty":q},...] where p/q are JSON numbers.
func appendObjLevels(dst []model.Level, arr []*fastjson.Value, nb *numBuf) ([]model.Level, error) {
	for _, l := range arr {
		px, err := nb.fixed(l.Get("price"))
		if err != nil {
			return dst, err
		}
		q, err := nb.fixed(l.Get("qty"))
		if err != nil {
			return dst, err
		}
		dst = append(dst, model.Level{Price: px, Qty: q})
	}
	return dst, nil
}

// numBuf is a decoder-owned scratch buffer for re-emitting JSON number text (a stack array
// passed to fastjson's non-inlined MarshalTo would escape and allocate per call).
type numBuf struct{ b []byte }

func (n *numBuf) fixed(v *fastjson.Value) (model.Fixed, error) {
	if v != nil && v.Type() == fastjson.TypeNumber {
		n.b = v.MarshalTo(n.b[:0])
		return model.ParseFixed(n.b)
	}
	return fixedOf(v)
}

// fixedOf parses a JSON number or numeric string into Fixed using its exact source text.
func fixedOf(v *fastjson.Value) (model.Fixed, error) {
	if v == nil {
		return 0, errKrakenShape
	}
	switch v.Type() {
	case fastjson.TypeString:
		return model.ParseFixed(v.GetStringBytes())
	case fastjson.TypeNumber:
		var buf [40]byte
		return model.ParseFixed(v.MarshalTo(buf[:0]))
	default:
		return 0, errKrakenShape
	}
}

// KrakenChecksum computes Kraken's v2 book checksum over the top 10 asks (ascending) then the
// top 10 bids (descending): each price and qty formatted with the pair's precision, the decimal
// point removed and leading zeros stripped, all concatenated, CRC32 (IEEE).
func KrakenChecksum(bids, asks []model.Level, p KrakenPrecision, scratch []byte) (uint32, []byte) {
	buf := scratch[:0]
	add := func(levels []model.Level) {
		n := len(levels)
		if n > KrakenDepth {
			n = KrakenDepth
		}
		for _, l := range levels[:n] {
			buf = appendChecksumNum(buf, l.Price, p.Price)
			buf = appendChecksumNum(buf, l.Qty, p.Qty)
		}
	}
	add(asks)
	add(bids)
	return crc32.ChecksumIEEE(buf), buf
}

func appendChecksumNum(dst []byte, f model.Fixed, places int) []byte {
	var tmp [32]byte
	s := f.AppendDecimal(tmp[:0], places)
	start := len(dst)
	for _, c := range s {
		if c == '.' {
			continue
		}
		if c == '0' && len(dst) == start {
			continue // leading zero
		}
		dst = append(dst, c)
	}
	return dst
}

// parseRFC3339Micros parses "2026-11-01T21:35:35.212793Z" to microseconds since epoch without
// allocating; returns 0 if the layout is unexpected.
func parseRFC3339Micros(b []byte) int64 {
	if len(b) < 20 || b[4] != '-' || b[10] != 'T' {
		return 0
	}
	num := func(s []byte) int64 {
		var v int64
		for _, c := range s {
			if c < '0' || c > '9' {
				return -1
			}
			v = v*10 + int64(c-'0')
		}
		return v
	}
	y, mo, d := num(b[0:4]), num(b[5:7]), num(b[8:10])
	h, mi, s := num(b[11:13]), num(b[14:16]), num(b[17:19])
	if y < 0 || mo < 1 || d < 1 || h < 0 || mi < 0 || s < 0 {
		return 0
	}
	var frac int64
	digits := 0
	if b[19] == '.' {
		for i := 20; i < len(b) && b[i] >= '0' && b[i] <= '9'; i++ {
			if digits < 6 {
				frac = frac*10 + int64(b[i]-'0')
				digits++
			}
		}
	}
	for ; digits < 6; digits++ {
		frac *= 10
	}
	return (daysFromCivil(y, mo, d)*86400+h*3600+mi*60+s)*1_000_000 + frac
}

// daysFromCivil converts a proleptic Gregorian date to days since 1970-01-01 (H. Hinnant).
func daysFromCivil(y, m, d int64) int64 {
	if m <= 2 {
		y--
	}
	era := y / 400
	yoe := y - era*400
	mp := (m + 9) % 12
	doy := (153*mp+2)/5 + d - 1
	doe := yoe*365 + yoe/4 - yoe/100 + doy
	return era*146097 + doe - 719468
}
