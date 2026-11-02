// Command mmanalyze replays a capture and measures, per venue and symbol, where trades print and
// where the venue's mid sits relative to the other venues' mids (in bps). This distinguishes a
// genuine venue premium (trades print at the premium) from a stale replica (they would not).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/eshaan2418/marketmux/internal/capture"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
	"github.com/eshaan2418/marketmux/internal/venue"
)

// acc keeps exact samples (a 30-minute capture has at most a few hundred thousand per key).
type acc struct {
	xs     []float64
	sorted bool
}

func newAcc() *acc             { return &acc{} }
func (a *acc) add(bps float64) { a.xs = append(a.xs, bps); a.sorted = false }
func (a *acc) q(p float64) float64 {
	if len(a.xs) == 0 {
		return 0
	}
	if !a.sorted {
		sort.Float64s(a.xs)
		a.sorted = true
	}
	return a.xs[int(p/100*float64(len(a.xs)-1))]
}

func main() {
	dir := flag.String("capture", "", "capture directory")
	symbols := flag.String("symbols", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,LTC-USD", "symbols")
	out := flag.String("out", "", "JSON output")
	flag.Parse()
	tbl, err := venue.NewSymbolTable(strings.Split(*symbols, ","))
	if err != nil {
		panic(err)
	}
	frames, err := capture.ReadAll(*dir)
	if err != nil {
		panic(err)
	}
	trade := map[string]*acc{}  // venue/sym -> trade px vs other venues' mid
	midDev := map[string]*acc{} // venue/sym -> own mid vs other venues' mid (per update)
	counts := map[string]int{}
	var e *pipeline.Engine
	othersMid := func(sym string, excl model.Venue) (float64, bool) {
		u, ok := e.Last(sym)
		if !ok {
			return 0, false
		}
		var s float64
		n := 0
		for vi, t := range u.Venues {
			if model.Venue(vi) == excl || !t.Valid || !t.HasBid || !t.HasAsk {
				continue
			}
			s += (t.Bid.Price.Float64() + t.Ask.Price.Float64()) / 2
			n++
		}
		if n < 2 {
			return 0, false
		}
		return s / float64(n), true
	}
	pub := pubFunc(func(u *pipeline.Update) {
		t := u.Venues[u.Cause]
		if !t.Valid || !t.HasBid || !t.HasAsk {
			return
		}
		m, ok := othersMid(u.Symbol, u.Cause)
		if !ok {
			return
		}
		k := u.Cause.String() + "/" + u.Symbol
		if midDev[k] == nil {
			midDev[k] = newAcc()
		}
		own := (t.Bid.Price.Float64() + t.Ask.Price.Float64()) / 2
		midDev[k].add((own - m) / m * 1e4)
	})
	e = pipeline.New(tbl, pipeline.Options{Publisher: pub})
	e.OnEvent = func(ev *model.Event) {
		if ev.Kind != model.KindTrade {
			return
		}
		m, ok := othersMid(ev.Symbol, ev.Venue)
		if !ok {
			return
		}
		k := ev.Venue.String() + "/" + ev.Symbol
		if trade[k] == nil {
			trade[k] = newAcc()
		}
		trade[k].add((ev.TradePx.Float64() - m) / m * 1e4)
		counts[k]++
	}
	for i := range frames {
		e.Handle(&frames[i])
	}
	type row struct {
		Key                                  string
		Trades                               int
		TradeVsOthersMidBpsP25, P50, P75     float64
		MidVsOthersMidBpsP25, MidP50, MidP75 float64
	}
	var rows []row
	keys := make([]string, 0, len(midDev))
	for k := range midDev {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		r := row{Key: k, Trades: counts[k], MidVsOthersMidBpsP25: midDev[k].q(25), MidP50: midDev[k].q(50), MidP75: midDev[k].q(75)}
		if a := trade[k]; a != nil {
			r.TradeVsOthersMidBpsP25, r.P50, r.P75 = a.q(25), a.q(50), a.q(75)
		}
		rows = append(rows, r)
	}
	b, _ := json.MarshalIndent(rows, "", "  ")
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
	fmt.Printf("%-18s %7s  %28s  %28s\n", "venue/symbol", "trades", "trade - others' mid (bps) p25/p50/p75", "mid - others' mid (bps) p25/p50/p75")
	for _, r := range rows {
		fmt.Printf("%-18s %7d  %8.2f %8.2f %8.2f          %8.2f %8.2f %8.2f\n", r.Key, r.Trades, r.TradeVsOthersMidBpsP25, r.P50, r.P75, r.MidVsOthersMidBpsP25, r.MidP50, r.MidP75)
	}
}

type pubFunc func(*pipeline.Update)

func (f pubFunc) Publish(u *pipeline.Update) { f(u) }
