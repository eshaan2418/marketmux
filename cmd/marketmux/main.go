// Command marketmux connects to public crypto market-data feeds (or replays a capture), builds
// validated per-venue books and a consolidated BBO, and serves it over WebSocket and gRPC.
//
//	marketmux -symbols BTC-USD,ETH-USD -listen :8080 -grpc :9090 -capture data/cap -duration 30m
//	marketmux -replay data/cap -speed 0 -stats-out stats.json
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"google.golang.org/grpc"

	"github.com/eshaan2418/marketmux/internal/capture"
	"github.com/eshaan2418/marketmux/internal/fanout"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
	"github.com/eshaan2418/marketmux/internal/server"
	"github.com/eshaan2418/marketmux/internal/venue"
)

var start = time.Now()

func clock() int64 { return time.Since(start).Nanoseconds() }

type resyncer struct {
	sessions map[model.Venue]*venue.Session
}

func (r resyncer) Resync(v model.Venue, sym, _ string) {
	if s, ok := r.sessions[v]; ok {
		s.RequestResync(sym)
	}
}

func main() {
	var (
		venues     = flag.String("venues", "kraken,coinbase,bitstamp,gemini", "venues to connect")
		symbols    = flag.String("symbols", "BTC-USD,ETH-USD,SOL-USD", "canonical symbols")
		listen     = flag.String("listen", ":8080", "HTTP listen address (WS, REST, metrics, pprof); empty to disable")
		grpcAddr   = flag.String("grpc", ":9090", "gRPC listen address; empty to disable")
		captureDir = flag.String("capture", "", "record raw frames to this directory")
		captureCap = flag.Int64("capture-cap-mb", 2048, "capture size cap (uncompressed MB)")
		duration   = flag.Duration("duration", 0, "stop after this long (0 = until signal)")
		replayDir  = flag.String("replay", "", "replay a capture directory instead of connecting")
		speed      = flag.Float64("speed", 0, "replay speed: 0 = as fast as possible, 1 = real time")
		workers    = flag.Int("hub-workers", 4, "fan-out shard workers")
		inbox      = flag.Int("hub-inbox", 65536, "per-shard inbox size")
		statsOut   = flag.String("stats-out", "", "write final stats JSON here")
		logLevel   = flag.String("log", "info", "log level")
		sndbuf     = flag.Int("sndbuf", 256<<10, "SO_SNDBUF for client connections (0 = OS default)")
	)
	flag.Parse()
	var lvl slog.Level
	_ = lvl.UnmarshalText([]byte(*logLevel))
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if *duration > 0 {
		var c2 context.CancelFunc
		ctx, c2 = context.WithTimeout(ctx, *duration)
		defer c2()
	}

	table, err := venue.NewSymbolTable(strings.Split(*symbols, ","))
	if err != nil {
		fail(err)
	}
	hub := fanout.NewHub(*workers, *inbox)
	hub.Encode = server.JSONEncoder()
	defer hub.Close()
	latest := server.NewLatest(hub)
	analytics := pipeline.NewAnalytics(latest)
	analytics.Live = *replayDir == ""

	frames := make(chan venue.Frame, 8192)
	sessions := map[model.Venue]*venue.Session{}
	engineClock := pipeline.Clock(clock)
	if *replayDir == "" {
		for _, name := range strings.Split(*venues, ",") {
			v := model.ParseVenue(strings.TrimSpace(name))
			spec, ok := venue.SpecFor(v)
			if !ok {
				fail(fmt.Errorf("unknown venue %q", name))
			}
			sessions[v] = venue.NewSession(spec, table.Symbols(), frames, clock, log)
		}
	}
	engine := pipeline.New(table, pipeline.Options{Publisher: analytics, Resyncer: resyncer{sessions}, Clock: engineClock})

	// Engine stats are owned by the engine goroutine; a snapshot is published for HTTP/metrics.
	var statsSnap atomic.Pointer[pipeline.Stats]
	frameNs := hdrhistogram.New(1, 60_000_000_000, 3) // engine time per frame
	queueNs := hdrhistogram.New(1, 60_000_000_000, 3) // receive -> engine dequeue
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	registerMetrics(reg, &statsSnap, sessions, hub)

	srv := &server.Server{Hub: hub, Latest: latest, Registry: reg, Log: log, Status: func() any {
		st := statsSnap.Load()
		sess := map[string]any{}
		for v, s := range sessions {
			sess[v.String()] = map[string]uint64{"connects": s.Connects.Load(), "frames": s.Frames.Load(),
				"bytes": s.Bytes.Load(), "resync_requests": s.ResyncsReq.Load(), "resync_rate_limited": s.ResyncsRL.Load()}
		}
		return map[string]any{"engine": st, "sessions": sess, "clients": hub.Clients.Load(),
			"published": hub.Published.Load(), "shard_drops": hub.ShardDrops(), "uptime_s": time.Since(start).Seconds()}
	}}
	var httpSrv *http.Server
	if *listen != "" {
		httpSrv = &http.Server{Addr: *listen, Handler: srv.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}
		hl, err := net.Listen("tcp", *listen)
		if err != nil {
			fail(err)
		}
		go func() {
			if err := httpSrv.Serve(server.BufferedListener{Listener: hl, WriteBuffer: *sndbuf}); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http", "err", err)
				cancel()
			}
		}()
		log.Info("http listening", "addr", *listen)
	}
	var gs *grpc.Server
	if *grpcAddr != "" {
		lis, err := net.Listen("tcp", *grpcAddr)
		if err != nil {
			fail(err)
		}
		gs = grpc.NewServer()
		srv.RegisterGRPC(gs)
		go func() { _ = gs.Serve(lis) }()
		log.Info("grpc listening", "addr", *grpcAddr)
	}

	var capW *capture.Writer
	if *captureDir != "" {
		capW, err = capture.NewWriter(*captureDir, 256<<20, *captureCap<<20)
		if err != nil {
			fail(err)
		}
	}

	// Producers: live sessions or the capture replayer.
	replayDone := make(chan error, 1)
	if *replayDir != "" {
		go func() { replayDone <- replay(ctx, *replayDir, *speed, frames) }()
	} else {
		for _, s := range sessions {
			go s.Run(ctx)
		}
	}

	// Engine loop (single goroutine owns all books).
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	publish := func() { st := engine.Stats(); statsSnap.Store(&st) }
	publish()
	wallStart := time.Now()
loop:
	for {
		select {
		case f := <-frames:
			if capW != nil {
				_ = capW.Write(&f)
			}
			t0 := clock()
			if *replayDir == "" && f.RecvNs > 0 && t0 >= f.RecvNs {
				_ = queueNs.RecordValue(t0 - f.RecvNs)
			}
			engine.Handle(&f)
			_ = frameNs.RecordValue(clock() - t0 + 1)
		case err := <-replayDone:
			// Drain whatever the replayer queued before it finished.
			for len(frames) > 0 {
				f := <-frames
				t0 := clock()
				engine.Handle(&f)
				_ = frameNs.RecordValue(clock() - t0 + 1)
			}
			if err != nil {
				log.Error("replay", "err", err)
			}
			break loop
		case <-ticker.C:
			publish()
		case <-ctx.Done():
			break loop
		}
	}
	wall := time.Since(wallStart)
	publish()
	if capW != nil {
		_ = capW.Close()
	}
	sctx, scancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer scancel()
	if httpSrv != nil {
		_ = httpSrv.Shutdown(sctx)
	}
	if gs != nil {
		gs.Stop()
	}

	st := engine.Stats()
	out := map[string]any{
		"mode":                       map[bool]string{true: "replay", false: "live"}[*replayDir != ""],
		"wall_seconds":               wall.Seconds(),
		"frames_per_second":          float64(st.Frames) / wall.Seconds(),
		"engine":                     st,
		"engine_ns_per_frame":        pipeline.Summarize(frameNs),
		"receive_to_engine_queue_ns": pipeline.Summarize(queueNs),
		"receive_to_publish_ns":      analytics.InternalSummary(),
		"engine_start_to_publish_ns": analytics.EngineSummary(),
		"symbols":                    analytics.Report(),
		"hub_published":              hub.Published.Load(),
		"hub_shard_drops":            hub.ShardDrops(),
	}
	if capW != nil {
		out["capture_records"] = capW.Records
		out["capture_dropped_by_cap"] = capW.Dropped
	}
	sess := map[string]any{}
	for v, s := range sessions {
		sess[v.String()] = map[string]uint64{"connects": s.Connects.Load(), "frames": s.Frames.Load(),
			"bytes": s.Bytes.Load(), "resync_requests": s.ResyncsReq.Load(), "resync_rate_limited": s.ResyncsRL.Load()}
	}
	out["sessions"] = sess
	b, _ := json.MarshalIndent(out, "", "  ")
	if *statsOut != "" {
		if err := os.WriteFile(*statsOut, b, 0o644); err != nil {
			log.Error("stats-out", "err", err)
		}
	}
	fmt.Println(string(b))
}

func replay(ctx context.Context, dir string, speed float64, out chan<- venue.Frame) error {
	files, err := capture.Files(dir)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("no capture files in %s", dir)
	}
	var first int64 = -1
	t0 := time.Now()
	for _, p := range files {
		f, err := os.Open(p)
		if err != nil {
			return err
		}
		r, err := capture.NewReader(f)
		if err != nil {
			f.Close()
			return err
		}
		for {
			var fr venue.Frame
			if err := r.Next(&fr, true); err != nil {
				f.Close()
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			if first < 0 {
				first = fr.RecvNs
			}
			if speed > 0 {
				due := time.Duration(float64(fr.RecvNs-first) / speed)
				if d := due - time.Since(t0); d > 0 {
					select {
					case <-time.After(d):
					case <-ctx.Done():
						f.Close()
						return ctx.Err()
					}
				}
			}
			select {
			case out <- fr:
			case <-ctx.Done():
				f.Close()
				return ctx.Err()
			}
		}
	}
	return nil
}

func registerMetrics(reg *prometheus.Registry, st *atomic.Pointer[pipeline.Stats], sessions map[model.Venue]*venue.Session, hub *fanout.Hub) {
	eng := func(name, help string, f func(*pipeline.Stats) uint64) {
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "marketmux", Subsystem: "engine", Name: name, Help: help},
			func() float64 {
				if s := st.Load(); s != nil {
					return float64(f(s))
				}
				return 0
			}))
	}
	eng("frames_total", "Frames processed.", func(s *pipeline.Stats) uint64 { return s.Frames })
	eng("decode_errors_total", "Frames that failed to decode.", func(s *pipeline.Stats) uint64 { return s.DecodeErrors })
	eng("checksum_ok_total", "Kraken checksums verified.", func(s *pipeline.Stats) uint64 { return s.ChecksumOK })
	eng("checksum_fail_total", "Kraken checksum mismatches.", func(s *pipeline.Stats) uint64 { return s.ChecksumFail })
	eng("resyncs_total", "Book resyncs triggered.", func(s *pipeline.Stats) uint64 { return s.Resyncs })
	eng("seq_gaps_total", "Sequence gaps detected.", func(s *pipeline.Stats) uint64 { return s.SeqGaps })
	eng("published_total", "Consolidated updates published.", func(s *pipeline.Stats) uint64 { return s.Published })
	eng("crossed_updates_total", "Consolidated updates with a crossed cross-venue book.", func(s *pipeline.Stats) uint64 { return s.CrossedUpdates })
	for v, s := range sessions {
		s := s
		lbl := prometheus.Labels{"venue": v.String()}
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "marketmux", Subsystem: "session", Name: "frames_total", Help: "Frames received.", ConstLabels: lbl},
			func() float64 { return float64(s.Frames.Load()) }))
		reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "marketmux", Subsystem: "session", Name: "connects_total", Help: "Connections established.", ConstLabels: lbl},
			func() float64 { return float64(s.Connects.Load()) }))
	}
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "marketmux", Subsystem: "fanout", Name: "clients", Help: "Connected clients."},
		func() float64 { return float64(hub.Clients.Load()) }))
	reg.MustRegister(prometheus.NewCounterFunc(prometheus.CounterOpts{Namespace: "marketmux", Subsystem: "fanout", Name: "shard_drops_total", Help: "Updates dropped at full shard inboxes."},
		func() float64 { return float64(hub.ShardDrops()) }))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "marketmux", Subsystem: "fanout", Name: "conflated", Help: "Updates conflated for currently connected clients."},
		func() float64 { return float64(hub.Totals().Conflated) }))
	reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{Namespace: "marketmux", Subsystem: "fanout", Name: "dropped", Help: "Updates dropped by queue-policy clients currently connected."},
		func() float64 { return float64(hub.Totals().Dropped) }))
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "marketmux:", err)
	os.Exit(1)
}
