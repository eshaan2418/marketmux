package pipeline

import (
	"sort"
	"sync"

	"github.com/HdrHistogram/hdrhistogram-go"

	"github.com/eshaan2418/marketmux/internal/model"
)

// Analytics is a Publisher that measures the consolidated market: internal latency, how often
// and by how much the cross-venue book is crossed, and which venue sets the BBO. It uses only
// frame receive timestamps (RecvNs), so the measurements are reproducible under replay.
type Analytics struct {
	mu   sync.Mutex
	Next Publisher
	// Live: RecvNs and PubNs share a clock (live mode), so receive->publish is meaningful.
	Live bool
	// InternalNs: frame receive (socket read) -> consolidated update published, ns (live only).
	InternalNs *hdrhistogram.Histogram
	// EngineNs: engine starts processing the frame -> update published, ns (live and replay).
	EngineNs *hdrhistogram.Histogram
	sym      map[string]*symAnalytics
}

type symAnalytics struct {
	lastNs        int64
	lastCrossed   bool
	lastCrossBps  float64
	totalNs       int64
	crossedNs     int64
	crossBps      *hdrhistogram.Histogram // cross size in 0.01 bps units, sampled per update
	bidTimeNs     [model.NumVenues]int64
	askTimeNs     [model.NumVenues]int64
	lastBid       model.Venue
	lastAsk       model.Venue
	updates       uint64
	crossedUpdate uint64
}

// NewAnalytics returns an Analytics forwarding to next.
func NewAnalytics(next Publisher) *Analytics {
	return &Analytics{Next: next, InternalNs: hdrhistogram.New(1, 60_000_000_000, 3),
		EngineNs: hdrhistogram.New(1, 60_000_000_000, 3),
		sym:      map[string]*symAnalytics{}}
}

// Publish implements Publisher.
func (a *Analytics) Publish(u *Update) {
	a.mu.Lock()
	if a.Live && u.PubNs > 0 && u.RecvNs > 0 && u.PubNs >= u.RecvNs {
		_ = a.InternalNs.RecordValue(u.PubNs - u.RecvNs)
	}
	if u.PubNs >= u.EngNs && u.EngNs > 0 {
		_ = a.EngineNs.RecordValue(u.PubNs - u.EngNs + 1)
	}
	s, ok := a.sym[u.Symbol]
	if !ok {
		s = &symAnalytics{crossBps: hdrhistogram.New(1, 10_000_000, 3)}
		a.sym[u.Symbol] = s
	}
	// Time-weighted attribution of the interval since the previous update to the previous state.
	if s.lastNs > 0 && u.RecvNs > s.lastNs {
		dt := u.RecvNs - s.lastNs
		s.totalNs += dt
		if s.lastCrossed {
			s.crossedNs += dt
		}
		s.bidTimeNs[s.lastBid] += dt
		s.askTimeNs[s.lastAsk] += dt
	}
	s.lastNs = u.RecvNs
	s.lastBid, s.lastAsk = u.BidVenue, u.AskVenue
	s.lastCrossed = u.Crossed
	s.updates++
	if u.Crossed && u.AskPx > 0 {
		s.crossedUpdate++
		mid := float64(u.BidPx+u.AskPx) / 2
		bps := float64(u.BidPx-u.AskPx) / mid * 1e4
		_ = s.crossBps.RecordValue(int64(bps*100) + 1)
	}
	a.mu.Unlock()
	if a.Next != nil {
		a.Next.Publish(u)
	}
}

// SymbolReport summarises one symbol.
type SymbolReport struct {
	Symbol            string             `json:"symbol"`
	Updates           uint64             `json:"updates"`
	ObservedSeconds   float64            `json:"observed_seconds"`
	CrossedTimeFrac   float64            `json:"crossed_time_fraction"`
	CrossedUpdateFrac float64            `json:"crossed_update_fraction"`
	CrossBpsP50       float64            `json:"cross_bps_p50"`
	CrossBpsP90       float64            `json:"cross_bps_p90"`
	CrossBpsP99       float64            `json:"cross_bps_p99"`
	CrossBpsMax       float64            `json:"cross_bps_max"`
	BestBidTimeShare  map[string]float64 `json:"best_bid_time_share"`
	BestAskTimeShare  map[string]float64 `json:"best_ask_time_share"`
}

// Report returns per-symbol summaries sorted by symbol.
func (a *Analytics) Report() []SymbolReport {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []SymbolReport
	for name, s := range a.sym {
		r := SymbolReport{Symbol: name, Updates: s.updates, ObservedSeconds: float64(s.totalNs) / 1e9,
			BestBidTimeShare: map[string]float64{}, BestAskTimeShare: map[string]float64{}}
		if s.totalNs > 0 {
			r.CrossedTimeFrac = float64(s.crossedNs) / float64(s.totalNs)
			for v := range s.bidTimeNs {
				if s.bidTimeNs[v] > 0 {
					r.BestBidTimeShare[model.Venue(v).String()] = float64(s.bidTimeNs[v]) / float64(s.totalNs)
				}
				if s.askTimeNs[v] > 0 {
					r.BestAskTimeShare[model.Venue(v).String()] = float64(s.askTimeNs[v]) / float64(s.totalNs)
				}
			}
		}
		if s.updates > 0 {
			r.CrossedUpdateFrac = float64(s.crossedUpdate) / float64(s.updates)
		}
		q := func(p float64) float64 { return float64(s.crossBps.ValueAtQuantile(p)-1) / 100 }
		if s.crossBps.TotalCount() > 0 {
			r.CrossBpsP50, r.CrossBpsP90, r.CrossBpsP99 = q(50), q(90), q(99)
			r.CrossBpsMax = float64(s.crossBps.Max()-1) / 100
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Symbol < out[j].Symbol })
	return out
}

// LatencySummary is a percentile summary in nanoseconds.
type LatencySummary struct {
	Count int64 `json:"count"`
	P50   int64 `json:"p50"`
	P90   int64 `json:"p90"`
	P99   int64 `json:"p99"`
	P999  int64 `json:"p99_9"`
	P9999 int64 `json:"p99_99"`
	Max   int64 `json:"max"`
}

// Summarize extracts percentiles from an HDR histogram.
func Summarize(h *hdrhistogram.Histogram) LatencySummary {
	return LatencySummary{Count: h.TotalCount(), P50: h.ValueAtQuantile(50), P90: h.ValueAtQuantile(90),
		P99: h.ValueAtQuantile(99), P999: h.ValueAtQuantile(99.9), P9999: h.ValueAtQuantile(99.99), Max: h.Max()}
}

// InternalSummary returns the receive->publish latency summary (live only).
func (a *Analytics) InternalSummary() LatencySummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Summarize(a.InternalNs)
}

// EngineSummary returns the engine-start->publish latency summary.
func (a *Analytics) EngineSummary() LatencySummary {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Summarize(a.EngineNs)
}
