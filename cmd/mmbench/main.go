// Command mmbench measures fan-out: frames (from a capture or the synthetic generator) are paced
// through the engine and hub to N WebSocket (or gRPC) clients over loopback inside one process,
// so every client can compute end-to-end latency (frame injected -> update received) on the same
// monotonic clock. A fraction of clients can be made deliberately slow to test isolation.
//
//	mmbench -clients 1000 -policy conflate -rate 20000 -duration 20s -slow 0.1 -out res.json
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/HdrHistogram/hdrhistogram-go"
	"github.com/coder/websocket"
	"github.com/valyala/fastjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/eshaan2418/marketmux/internal/capture"
	"github.com/eshaan2418/marketmux/internal/fakevenue"
	"github.com/eshaan2418/marketmux/internal/fanout"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
	"github.com/eshaan2418/marketmux/internal/server"
	"github.com/eshaan2418/marketmux/internal/venue"
)

var t0 = time.Now()

func now() int64 { return time.Since(t0).Nanoseconds() }

type clientStats struct {
	mu     sync.Mutex
	h      *hdrhistogram.Histogram
	n      atomic.Int64
	gaps   atomic.Int64
	closed atomic.Int64
}

func main() {
	var (
		clients   = flag.Int("clients", 100, "number of clients")
		transport = flag.String("transport", "ws", "ws | grpc")
		policy    = flag.String("policy", "conflate", "conflate | queue | disconnect")
		queueLen  = flag.Int("queue", 4096, "queue length for queue/disconnect")
		rate      = flag.Float64("rate", 20000, "frames per second to inject (0 = unpaced)")
		duration  = flag.Duration("duration", 20*time.Second, "measurement duration")
		warmup    = flag.Duration("warmup", 3*time.Second, "warm-up (not measured)")
		slowFrac  = flag.Float64("slow", 0, "fraction of clients that sleep between reads")
		slowSleep = flag.Duration("slow-sleep", 50*time.Millisecond, "sleep per read for slow clients (0 with -slow > 0 = stalled: never read)")
		capDir    = flag.String("capture", "", "replay frames from this capture (default: synthetic)")
		symbols   = flag.String("symbols", "BTC-USD,ETH-USD,SOL-USD,XRP-USD,LTC-USD", "symbols")
		workers   = flag.Int("hub-workers", 4, "hub shard workers")
		out       = flag.String("out", "", "write JSON result")
		encOnce   = flag.Bool("encode-once", true, "encode each update once per hub shard (vs per client)")
		full      = flag.Bool("full-parse", false, "clients fully parse each JSON batch instead of scanning three fields")
		cpuprof   = flag.String("cpuprofile", "", "write a CPU profile of the measured phase")
		wsIntv    = flag.Duration("interval", 0, "WS throttled-conflation interval per client (0 = off)")
		sndbuf    = flag.Int("sndbuf", 64<<10, "server-side SO_SNDBUF per client connection (0 = OS default)")
		rcvbuf    = flag.Int("rcvbuf", 64<<10, "client-side SO_RCVBUF (0 = OS default)")
	)
	flag.Parse()
	fullParse = *full
	syms := strings.Split(*symbols, ",")
	tbl, err := venue.NewSymbolTable(syms)
	if err != nil {
		fail(err)
	}

	// Source frames.
	var frames []venue.Frame
	if *capDir != "" {
		frames, err = capture.ReadAll(*capDir)
		if err != nil {
			fail(err)
		}
	} else {
		g := fakevenue.NewGenerator(1, syms)
		for i := range syms {
			frames = append(frames, venue.Frame{Kind: venue.FrameWS, Venue: model.Synthetic, Payload: g.Snapshot(i)})
		}
		for i := 0; i < 2_000_000; i++ {
			frames = append(frames, venue.Frame{Kind: venue.FrameWS, Venue: model.Synthetic, Payload: g.Delta()})
		}
	}

	hub := fanout.NewHub(*workers, 1<<16)
	if *encOnce {
		hub.Encode = server.JSONEncoder()
	}
	latest := server.NewLatest(hub)
	eng := pipeline.New(tbl, pipeline.Options{Publisher: latest, Clock: now})
	srv := &server.Server{Hub: hub, Latest: latest, Status: func() any { return nil }}

	rawLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fail(err)
	}
	lis := server.BufferedListener{Listener: rawLis, WriteBuffer: *sndbuf}
	clientRcvBuf = *rcvbuf
	var addr string
	if *transport == "grpc" {
		gs := grpc.NewServer()
		srv.RegisterGRPC(gs)
		go func() { _ = gs.Serve(lis) }()
		defer gs.Stop()
		addr = lis.Addr().String()
	} else {
		hs := &http.Server{Handler: srv.HTTPHandler(), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = hs.Serve(lis) }()
		defer func() { _ = hs.Close() }()
		addr = "ws://" + lis.Addr().String() + "/ws?policy=" + *policy + fmt.Sprintf("&queue=%d", *queueLen)
		if *wsIntv > 0 {
			addr += "&interval=" + wsIntv.String()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var measuring atomic.Bool
	fast := &clientStats{h: hdrhistogram.New(1, 60_000_000_000, 3)}
	slow := &clientStats{h: hdrhistogram.New(1, 60_000_000_000, 3)}
	nSlow := int(float64(*clients) * *slowFrac)
	var wg sync.WaitGroup
	var connected atomic.Int64
	for i := 0; i < *clients; i++ {
		st := fast
		var sleep time.Duration
		stalled := false
		if i < nSlow {
			st, sleep = slow, *slowSleep
			stalled = sleep == 0
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if *transport == "grpc" {
				runGRPC(ctx, addr, *policy, uint32(*queueLen), st, sleep, &measuring, &connected)
			} else {
				if stalled {
					runStalledWS(ctx, addr, &connected)
				} else {
					runWS(ctx, addr, st, sleep, &measuring, &connected)
				}
			}
		}()
	}
	deadline := time.Now().Add(30 * time.Second)
	for connected.Load() < int64(*clients) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if connected.Load() < int64(*clients) {
		fail(fmt.Errorf("only %d/%d clients connected", connected.Load(), *clients))
	}

	// Inject frames on this goroutine (it is the engine goroutine), stamping RecvNs at injection.
	var ms0, ms1 runtime.MemStats
	var injected, measuredFrames int64
	var pubStart uint64
	runFor := *warmup + *duration
	startInj := time.Now()
	var interval time.Duration
	if *rate > 0 {
		interval = time.Duration(float64(time.Second) / *rate)
	}
	next := time.Now()
	i := 0
	for time.Since(startInj) < runFor {
		if !measuring.Load() && time.Since(startInj) >= *warmup {
			measuring.Store(true)
			if *cpuprof != "" {
				if f, err := os.Create(*cpuprof); err == nil {
					_ = pprof.StartCPUProfile(f)
					defer pprof.StopCPUProfile()
				}
			}
			runtime.ReadMemStats(&ms0)
			pubStart = hub.Published.Load()
			measuredFrames = 0
		}
		f := frames[i%len(frames)]
		i++
		if i%len(frames) == 0 && *capDir == "" {
			// Synthetic source wraps: resnapshot so sequence numbers stay consistent.
			eng.Handle(&venue.Frame{Kind: venue.FrameConnected, Venue: model.Synthetic, RecvNs: now()})
		}
		f.RecvNs = now()
		eng.Handle(&f)
		injected++
		measuredFrames++
		if interval > 0 {
			next = next.Add(interval)
			if d := time.Until(next); d > 0 {
				time.Sleep(d)
			}
		}
	}
	totals := hub.Totals() // snapshot while clients are still connected
	measuring.Store(false)
	runtime.ReadMemStats(&ms1)
	pubs := hub.Published.Load() - pubStart
	elapsed := duration.Seconds()
	time.Sleep(200 * time.Millisecond)
	cancel()
	wg.Wait()

	sum := func(s *clientStats) map[string]any {
		s.mu.Lock()
		defer s.mu.Unlock()
		return map[string]any{"received": s.n.Load(), "msgs_per_sec": float64(s.n.Load()) / elapsed,
			"seq_gaps_observed": s.gaps.Load(), "closed_by_server": s.closed.Load(),
			"latency_ns": pipeline.Summarize(s.h)}
	}
	res := map[string]any{
		"transport": *transport, "policy": *policy, "clients": *clients, "slow_clients": nSlow,
		"slow_sleep_ms": slowSleep.Milliseconds(), "target_rate": *rate, "encode_once": *encOnce, "client_full_parse": *full, "interval_ms": float64(*wsIntv) / 1e6, "sndbuf": *sndbuf, "rcvbuf": *rcvbuf,
		"source":          map[bool]string{true: "capture", false: "synthetic"}[*capDir != ""],
		"frames_measured": measuredFrames, "frames_per_sec": float64(measuredFrames) / elapsed,
		"updates_published": pubs, "updates_per_sec": float64(pubs) / elapsed,
		"shard_drops": hub.ShardDrops(), "duration_s": elapsed, "gomaxprocs": runtime.GOMAXPROCS(0),
		"fast_clients": sum(fast), "slow": sum(slow),
		"alloc_bytes_per_update": float64(ms1.TotalAlloc-ms0.TotalAlloc) / float64(max(pubs, 1)),
		"gc_cycles":              ms1.NumGC - ms0.NumGC,
		"engine":                 eng.Stats(),
		"hub_totals":             totals,
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	if *out != "" {
		_ = os.WriteFile(*out, b, 0o644)
	}
	fmt.Println(string(b))
	hub.Close()
}

func record(st *clientStats, measuring *atomic.Bool, recvNs int64, slowClient bool) {
	if !measuring.Load() {
		return
	}
	lat := now() - recvNs
	if lat < 1 {
		lat = 1
	}
	st.n.Add(1)
	st.mu.Lock()
	_ = st.h.RecordValue(lat)
	st.mu.Unlock()
	_ = slowClient
}

func runWS(ctx context.Context, url string, st *clientStats, sleep time.Duration, measuring *atomic.Bool, connected *atomic.Int64) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: dialer()})
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		return
	}
	c.SetReadLimit(1 << 20)
	defer func() { _ = c.CloseNow() }()
	connected.Add(1)
	last := map[string]uint64{}
	var parser fastjson.Parser // per client: fastjson parsers are not goroutine-safe
	for {
		_, b, err := c.Read(ctx)
		if err != nil {
			var ce websocket.CloseError
			if errors.As(err, &ce) {
				st.closed.Add(1)
			}
			return
		}
		// Light client: extract only symbol, seq and receive timestamp from each object of the
		// batch, so the benchmark measures the server's fan-out rather than client-side JSON
		// parsing (all clients share this machine). -full-parse switches to a full parse.
		if fullParse {
			arr, err := parser.ParseBytes(b)
			if err != nil {
				continue
			}
			for _, v := range arr.GetArray() {
				observe(st, measuring, last, string(v.GetStringBytes("s")), v.GetUint64("q"), v.GetInt64("rn"))
			}
		} else {
			for len(b) > 0 {
				sym, q, rn, rest, ok := scanUpdate(b)
				if !ok {
					break
				}
				observe(st, measuring, last, sym, q, rn)
				b = rest
			}
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

var (
	fullParse    bool
	clientRcvBuf int
)

// dialer sets SO_RCVBUF before connect(2): the receive window is negotiated during the handshake,
// so shrinking it afterwards does not bound the data the peer may have in flight.
func dialer() *http.Client {
	d := &net.Dialer{Control: func(_, _ string, rc syscall.RawConn) error {
		if clientRcvBuf <= 0 {
			return nil
		}
		var serr error
		err := rc.Control(func(fd uintptr) {
			serr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, clientRcvBuf)
		})
		if err != nil {
			return err
		}
		return serr
	}}
	return &http.Client{Transport: &http.Transport{DialContext: d.DialContext}}
}

func observe(st *clientStats, measuring *atomic.Bool, last map[string]uint64, sym string, q uint64, rn int64) {
	if prev, ok := last[sym]; ok && q != prev+1 && measuring.Load() {
		st.gaps.Add(1)
	}
	last[sym] = q
	record(st, measuring, rn, false)
}

var (
	keyS  = []byte(`{"s":"`)
	keyQ  = []byte(`,"q":`)
	keyRN = []byte(`"rn":`)
)

// scanUpdate finds the next update object in b and extracts "s", "q" and "rn". The encoder
// (server.AppendJSON) always emits s, q first and rn later, so a forward scan suffices.
func scanUpdate(b []byte) (sym string, q uint64, rn int64, rest []byte, ok bool) {
	i := bytes.Index(b, keyS)
	if i < 0 {
		return
	}
	b = b[i+len(keyS):]
	j := bytes.IndexByte(b, '"')
	if j < 0 {
		return
	}
	sym = string(b[:j])
	b = b[j:]
	if k := bytes.Index(b, keyQ); k >= 0 {
		b = b[k+len(keyQ):]
		for len(b) > 0 && b[0] >= '0' && b[0] <= '9' {
			q = q*10 + uint64(b[0]-'0')
			b = b[1:]
		}
	}
	k := bytes.Index(b, keyRN)
	if k < 0 {
		return
	}
	b = b[k+len(keyRN):]
	for len(b) > 0 && b[0] >= '0' && b[0] <= '9' {
		rn = rn*10 + int64(b[0]-'0')
		b = b[1:]
	}
	return sym, q, rn, b, true
}

// runStalledWS connects and then never reads: a stuck consumer. Back-pressure reaches the server
// once the kernel buffers fill; the hub's policy counters show what the server did about it.
func runStalledWS(ctx context.Context, url string, connected *atomic.Int64) {
	c, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: dialer()})
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()
	connected.Add(1)
	<-ctx.Done()
}

func runGRPC(ctx context.Context, addr, policy string, ql uint32, st *clientStats, sleep time.Duration, measuring *atomic.Bool, connected *atomic.Int64) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(server.CodecName)))
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	stream, err := conn.NewStream(ctx, server.GRPCStreamDesc, server.StreamMethod)
	if err != nil {
		return
	}
	if err := stream.SendMsg(&server.SubscribeRequest{Policy: policy, QueueLen: ql}); err != nil {
		return
	}
	_ = stream.CloseSend()
	connected.Add(1)
	last := map[string]uint64{}
	var b server.UpdateBatch
	for {
		if err := stream.RecvMsg(&b); err != nil {
			if !errors.Is(err, io.EOF) && ctx.Err() == nil {
				st.closed.Add(1)
			}
			return
		}
		for i := range b.Updates {
			u := &b.Updates[i]
			if prev, ok := last[u.Symbol]; ok && u.Seq != prev+1 && measuring.Load() {
				st.gaps.Add(1)
			}
			last[u.Symbol] = u.Seq
			record(st, measuring, u.RecvNs, sleep > 0)
		}
		if sleep > 0 {
			time.Sleep(sleep)
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "mmbench:", err)
	os.Exit(1)
}
