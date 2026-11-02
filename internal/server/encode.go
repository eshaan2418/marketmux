// Package server exposes consolidated updates over WebSocket (JSON) and gRPC (compact binary
// codec), plus HTTP endpoints for metrics, pprof and the latest BBO.
package server

import (
	"encoding/binary"
	"errors"
	"math"
	"strconv"

	"google.golang.org/grpc/encoding"

	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
)

// AppendJSON encodes an update as compact JSON without reflection:
//
//	{"s":"BTC-USD","q":42,"bp":"77843.1","bq":"0.59","bv":"kraken","ap":...,"aq":...,"av":...,
//	 "x":false,"c":"kraken","rn":123,"pn":456,"v":{"kraken":["bp","bq","ap","aq"],...}}
func AppendJSON(dst []byte, u *pipeline.Update) []byte {
	dst = append(dst, `{"s":`...)
	dst = strconv.AppendQuote(dst, u.Symbol)
	dst = append(dst, `,"q":`...)
	dst = strconv.AppendUint(dst, u.Seq, 10)
	dst = appendPx(dst, `,"bp":`, u.BidPx)
	dst = appendPx(dst, `,"bq":`, u.BidQty)
	dst = append(dst, `,"bv":"`...)
	dst = append(dst, u.BidVenue.String()...)
	dst = appendPx(dst, `","ap":`, u.AskPx)
	dst = appendPx(dst, `,"aq":`, u.AskQty)
	dst = append(dst, `,"av":"`...)
	dst = append(dst, u.AskVenue.String()...)
	dst = append(dst, `","x":`...)
	dst = strconv.AppendBool(dst, u.Crossed)
	dst = append(dst, `,"c":"`...)
	dst = append(dst, u.Cause.String()...)
	dst = append(dst, `","rn":`...)
	dst = strconv.AppendInt(dst, u.RecvNs, 10)
	dst = append(dst, `,"pn":`...)
	dst = strconv.AppendInt(dst, u.PubNs, 10)
	dst = append(dst, `,"v":{`...)
	first := true
	for vi := range u.Venues {
		t := &u.Venues[vi]
		if !t.Valid {
			continue
		}
		if !first {
			dst = append(dst, ',')
		}
		first = false
		dst = append(dst, '"')
		dst = append(dst, model.Venue(vi).String()...)
		dst = appendPx(dst, `":[`, t.Bid.Price)
		dst = appendPx(dst, `,`, t.Bid.Qty)
		dst = appendPx(dst, `,`, t.Ask.Price)
		dst = appendPx(dst, `,`, t.Ask.Qty)
		dst = append(dst, ']')
	}
	return append(dst, `}}`...)
}

func appendPx(dst []byte, prefix string, f model.Fixed) []byte {
	dst = append(dst, prefix...)
	dst = append(dst, '"')
	dst = f.AppendDecimal(dst, model.Decimals)
	return append(dst, '"')
}

// ---------------------------------------------------------------------------------------------
// gRPC messages and codec. The service is registered by hand (no protoc in the toolchain), with a
// fixed-layout little-endian codec named "mmbin". Clients select it with
// grpc.CallContentSubtype(CodecName).

// CodecName is the gRPC content-subtype of the binary codec.
const CodecName = "mmbin"

// SubscribeRequest selects symbols and a slow-consumer policy.
type SubscribeRequest struct {
	Symbols  []string
	Policy   string
	QueueLen uint32
}

// UpdateBatch carries one or more updates.
type UpdateBatch struct {
	Updates []pipeline.Update
}

type codec struct{}

func (codec) Name() string { return CodecName }

func (codec) Marshal(v any) ([]byte, error) {
	switch m := v.(type) {
	case *SubscribeRequest:
		b := binary.LittleEndian.AppendUint32(nil, m.QueueLen)
		b = appendStr(b, m.Policy)
		b = binary.LittleEndian.AppendUint16(b, uint16(len(m.Symbols)))
		for _, s := range m.Symbols {
			b = appendStr(b, s)
		}
		return b, nil
	case *UpdateBatch:
		return AppendBatch(nil, m.Updates), nil
	}
	return nil, errors.New("mmbin: unsupported type")
}

func (codec) Unmarshal(data []byte, v any) error {
	switch m := v.(type) {
	case *SubscribeRequest:
		if len(data) < 6 {
			return errShort
		}
		m.QueueLen = binary.LittleEndian.Uint32(data)
		var err error
		data = data[4:]
		if m.Policy, data, err = readStr(data); err != nil {
			return err
		}
		if len(data) < 2 {
			return errShort
		}
		n := int(binary.LittleEndian.Uint16(data))
		data = data[2:]
		m.Symbols = m.Symbols[:0]
		for i := 0; i < n; i++ {
			var s string
			if s, data, err = readStr(data); err != nil {
				return err
			}
			m.Symbols = append(m.Symbols, s)
		}
		return nil
	case *UpdateBatch:
		var err error
		m.Updates, err = DecodeBatch(m.Updates[:0], data)
		return err
	}
	return errors.New("mmbin: unsupported type")
}

var errShort = errors.New("mmbin: short message")

func appendStr(b []byte, s string) []byte {
	b = binary.LittleEndian.AppendUint16(b, uint16(len(s)))
	return append(b, s...)
}

func readStr(b []byte) (string, []byte, error) {
	if len(b) < 2 {
		return "", nil, errShort
	}
	n := int(binary.LittleEndian.Uint16(b))
	if len(b) < 2+n {
		return "", nil, errShort
	}
	return string(b[2 : 2+n]), b[2+n:], nil
}

// AppendBatch encodes updates: count u16, then per update the symbol and fixed-width fields.
func AppendBatch(b []byte, us []pipeline.Update) []byte {
	le := binary.LittleEndian
	b = le.AppendUint16(b, uint16(len(us)))
	for i := range us {
		u := &us[i]
		b = appendStr(b, u.Symbol)
		b = le.AppendUint64(b, u.Seq)
		for _, f := range [...]model.Fixed{u.BidPx, u.BidQty, u.AskPx, u.AskQty} {
			b = le.AppendUint64(b, uint64(f))
		}
		flags := byte(0)
		if u.Crossed {
			flags = 1
		}
		b = append(b, byte(u.BidVenue), byte(u.AskVenue), byte(u.Cause), flags)
		b = le.AppendUint64(b, uint64(u.RecvNs))
		b = le.AppendUint64(b, uint64(u.PubNs))
		var mask byte
		for vi := range u.Venues {
			if u.Venues[vi].Valid {
				mask |= 1 << vi
			}
		}
		b = append(b, mask)
		for vi := range u.Venues {
			t := &u.Venues[vi]
			if !t.Valid {
				continue
			}
			for _, f := range [...]model.Fixed{t.Bid.Price, t.Bid.Qty, t.Ask.Price, t.Ask.Qty} {
				b = le.AppendUint64(b, uint64(f))
			}
		}
	}
	return b
}

// DecodeBatch is the inverse of AppendBatch.
func DecodeBatch(dst []pipeline.Update, b []byte) ([]pipeline.Update, error) {
	le := binary.LittleEndian
	if len(b) < 2 {
		return dst, errShort
	}
	n := int(le.Uint16(b))
	b = b[2:]
	for i := 0; i < n; i++ {
		var u pipeline.Update
		var err error
		if u.Symbol, b, err = readStr(b); err != nil {
			return dst, err
		}
		if len(b) < 8+32+4+16+1 {
			return dst, errShort
		}
		u.Seq = le.Uint64(b)
		u.BidPx = model.Fixed(le.Uint64(b[8:]))
		u.BidQty = model.Fixed(le.Uint64(b[16:]))
		u.AskPx = model.Fixed(le.Uint64(b[24:]))
		u.AskQty = model.Fixed(le.Uint64(b[32:]))
		u.BidVenue, u.AskVenue, u.Cause = model.Venue(b[40]), model.Venue(b[41]), model.Venue(b[42])
		u.Crossed = b[43]&1 != 0
		u.RecvNs = int64(le.Uint64(b[44:]))
		u.PubNs = int64(le.Uint64(b[52:]))
		mask := b[60]
		b = b[61:]
		for vi := 0; vi < len(u.Venues) && vi < 8; vi++ {
			if mask&(1<<vi) == 0 {
				continue
			}
			if len(b) < 32 {
				return dst, errShort
			}
			u.Venues[vi] = pipeline.Top{Valid: true,
				Bid: model.Level{Price: model.Fixed(le.Uint64(b)), Qty: model.Fixed(le.Uint64(b[8:]))},
				Ask: model.Level{Price: model.Fixed(le.Uint64(b[16:])), Qty: model.Fixed(le.Uint64(b[24:]))}}
			u.Venues[vi].HasBid = u.Venues[vi].Bid.Qty > 0
			u.Venues[vi].HasAsk = u.Venues[vi].Ask.Qty > 0
			b = b[32:]
		}
		if u.AskPx == math.MaxInt64 {
			u.AskPx = 0
		}
		dst = append(dst, u)
	}
	return dst, nil
}

func init() { encoding.RegisterCodec(codec{}) }
