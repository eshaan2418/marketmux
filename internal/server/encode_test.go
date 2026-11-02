package server

import (
	"testing"

	"github.com/valyala/fastjson"

	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
)

func sample() pipeline.Update {
	u := pipeline.Update{Symbol: "BTC-USD", Seq: 42, BidPx: model.MustFixed("77843.1"), BidQty: model.MustFixed("0.5"),
		BidVenue: model.Kraken, AskPx: model.MustFixed("77843.2"), AskQty: model.MustFixed("1"), AskVenue: model.Gemini,
		Cause: model.Kraken, RecvNs: 100, PubNs: 200}
	u.Venues[model.Kraken] = pipeline.Top{Valid: true, HasBid: true, HasAsk: true,
		Bid: model.Level{Price: u.BidPx, Qty: u.BidQty}, Ask: model.Level{Price: model.MustFixed("77844"), Qty: 2}}
	return u
}

func TestBinaryBatchRoundTrip(t *testing.T) {
	in := []pipeline.Update{sample(), sample()}
	in[1].Symbol, in[1].Crossed = "ETH-USD", true
	out, err := DecodeBatch(nil, AppendBatch(nil, in))
	if err != nil || len(out) != 2 {
		t.Fatal(err)
	}
	for i := range in {
		if out[i] != in[i] {
			t.Fatalf("round trip\n got %+v\nwant %+v", out[i], in[i])
		}
	}
	if _, err := DecodeBatch(nil, AppendBatch(nil, in)[:20]); err == nil {
		t.Fatal("truncated batch accepted")
	}
}

func TestJSONIsValid(t *testing.T) {
	u := sample()
	b := AppendJSON(nil, &u)
	v, err := fastjson.ParseBytes(b)
	if err != nil {
		t.Fatalf("%s: %v", b, err)
	}
	if string(v.GetStringBytes("bp")) != "77843.10000000" || string(v.GetStringBytes("bv")) != "kraken" ||
		v.GetArray("v", "kraken") == nil {
		t.Fatalf("%s", b)
	}
}

func FuzzDecodeBatch(f *testing.F) {
	u := sample()
	f.Add(AppendBatch(nil, []pipeline.Update{u}))
	f.Fuzz(func(t *testing.T, b []byte) {
		out, err := DecodeBatch(nil, b)
		if err != nil {
			return
		}
		// Whatever decodes must re-encode to a decodable batch with identical content.
		back, err := DecodeBatch(nil, AppendBatch(nil, out))
		if err != nil || len(back) != len(out) {
			t.Fatalf("re-encode failed: %v", err)
		}
	})
}

func BenchmarkAppendJSON(b *testing.B) {
	u := sample()
	buf := make([]byte, 0, 512)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf = AppendJSON(buf[:0], &u)
	}
}
