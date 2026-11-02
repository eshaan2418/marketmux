// Package integration runs the full stack against a fake venue: WebSocket session -> engine ->
// hub -> WebSocket and gRPC clients, with injected sequence gaps and disconnects.
package integration

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/valyala/fastjson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/eshaan2418/marketmux/internal/fakevenue"
	"github.com/eshaan2418/marketmux/internal/fanout"
	"github.com/eshaan2418/marketmux/internal/model"
	"github.com/eshaan2418/marketmux/internal/pipeline"
	"github.com/eshaan2418/marketmux/internal/server"
	"github.com/eshaan2418/marketmux/internal/venue"
)

type stack struct {
	fake    *fakevenue.Server
	session *venue.Session
	hub     *fanout.Hub
	httpURL string
	grpcLis net.Listener
	mu      sync.Mutex
	stats   pipeline.Stats
}

type rs struct{ s *venue.Session }

func (r rs) Resync(_ model.Venue, sym, _ string) { r.s.RequestResync(sym) }

func start(t *testing.T, ctx context.Context) *stack {
	t.Helper()
	syms := []string{"BTC-USD", "ETH-USD"}
	fake := &fakevenue.Server{Symbols: syms, Seed: 1, Rate: 200 * time.Microsecond}
	fs := httptest.NewServer(fake)
	t.Cleanup(fs.Close)
	tbl, _ := venue.NewSymbolTable(syms)
	frames := make(chan venue.Frame, 1024)
	t0 := time.Now()
	clock := func() int64 { return time.Since(t0).Nanoseconds() }
	log := slog.New(slog.NewTextHandler(nil2{}, nil))
	sess := venue.NewSession(venue.SyntheticSpec("ws"+strings.TrimPrefix(fs.URL, "http")), tbl.Symbols(), frames, clock, log)
	hub := fanout.NewHub(2, 4096)
	hub.Encode = server.JSONEncoder()
	t.Cleanup(hub.Close)
	latest := server.NewLatest(hub)
	eng := pipeline.New(tbl, pipeline.Options{Publisher: latest, Resyncer: rs{sess}, Clock: clock})
	st := &stack{fake: fake, session: sess, hub: hub}
	go sess.Run(ctx)
	go func() {
		for {
			select {
			case f := <-frames:
				eng.Handle(&f)
				s := eng.Stats()
				st.mu.Lock()
				st.stats = s
				st.mu.Unlock()
			case <-ctx.Done():
				return
			}
		}
	}()
	srv := &server.Server{Hub: hub, Latest: latest, Status: func() any { return nil }}
	hs := httptest.NewServer(srv.HTTPHandler())
	t.Cleanup(hs.Close)
	st.httpURL = hs.URL
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer()
	srv.RegisterGRPC(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	st.grpcLis = lis
	return st
}

type nil2 struct{}

func (nil2) Write(p []byte) (int, error) { return len(p), nil }

func (s *stack) engineStats() pipeline.Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

func TestEndToEndWebSocketAndRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s := start(t, ctx)

	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(s.httpURL, "http")+"/ws?symbols=BTC-USD&policy=queue&queue=100000", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	var got atomic.Int64
	var lastSeq atomic.Uint64
	var bad atomic.Int64
	go func() {
		var p fastjson.Parser
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				return
			}
			arr, err := p.ParseBytes(b)
			if err != nil || len(arr.GetArray()) == 0 {
				bad.Add(1)
				continue
			}
			for _, v := range arr.GetArray() {
				if string(v.GetStringBytes("s")) != "BTC-USD" {
					bad.Add(1)
					continue
				}
				q := v.GetUint64("q")
				if prev := lastSeq.Swap(q); prev != 0 && q != prev+1 {
					bad.Add(1) // queue policy with a large queue must be gap-free
				}
				got.Add(1)
			}
		}
	}()
	if !fakevenue.Wait(ctx, 10*time.Second, func() bool { return got.Load() > 200 }) {
		t.Fatalf("no updates over WS (got %d)", got.Load())
	}

	// Inject a sequence gap on the next connection, then force a reconnect via a dropped connection.
	s.fake.InjectGap(50)
	s.fake.DropNext()
	s.session.RequestResync("BTC-USD") // synthetic spec: resync == reconnect
	if !fakevenue.Wait(ctx, 15*time.Second, func() bool {
		st := s.engineStats()
		return st.SeqGaps >= 1 && st.Disconnects >= 2 && s.fake.Conns.Load() >= 4
	}) {
		t.Fatalf("recovery not observed: %+v conns=%d", s.engineStats(), s.fake.Conns.Load())
	}
	// After recovery, updates flow again and the consolidated book is valid.
	before := got.Load()
	if !fakevenue.Wait(ctx, 10*time.Second, func() bool { return got.Load() > before+200 }) {
		t.Fatal("updates did not resume after recovery")
	}
	if bad.Load() != 0 {
		t.Fatalf("%d malformed or out-of-sequence WS messages", bad.Load())
	}
	st := s.engineStats()
	if st.DecodeErrors != 0 || st.VenueCrossed != 0 {
		t.Fatalf("%+v", st)
	}
}

func TestEndToEndGRPC(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s := start(t, ctx)
	conn, err := grpc.NewClient(s.grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.CallContentSubtype(server.CodecName)))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := conn.NewStream(ctx, server.GRPCStreamDesc, server.StreamMethod)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.SendMsg(&server.SubscribeRequest{Symbols: []string{"ETH-USD"}, Policy: "conflate"}); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	n := 0
	for n < 100 {
		var b server.UpdateBatch
		if err := stream.RecvMsg(&b); err != nil {
			t.Fatal(err)
		}
		for _, u := range b.Updates {
			if u.Symbol != "ETH-USD" || u.BidVenue != model.Synthetic || u.BidPx >= u.AskPx {
				t.Fatalf("bad update %+v", u)
			}
			n++
		}
	}
	// REST endpoint agrees on the symbol's existence.
	resp, err := http.Get(s.httpURL + "/v1/bbo?symbol=ETH-USD")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("bbo endpoint: %v %v", err, resp)
	}
	resp.Body.Close()
}
