package server

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/pprof"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"

	"github.com/eshaan2418/marketmux/internal/fanout"
	"github.com/eshaan2418/marketmux/internal/pipeline"
)

// Latest keeps the most recent update per symbol for the REST endpoint. It is a Publisher that
// can be chained in front of the hub.
type Latest struct {
	mu   sync.RWMutex
	last map[string]pipeline.Update
	Next pipeline.Publisher
}

// NewLatest returns a Latest forwarding to next.
func NewLatest(next pipeline.Publisher) *Latest {
	return &Latest{last: map[string]pipeline.Update{}, Next: next}
}

// Publish implements pipeline.Publisher.
func (l *Latest) Publish(u *pipeline.Update) {
	l.mu.Lock()
	l.last[u.Symbol] = *u
	l.mu.Unlock()
	if l.Next != nil {
		l.Next.Publish(u)
	}
}

// Get returns the latest update for a symbol.
func (l *Latest) Get(sym string) (pipeline.Update, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	u, ok := l.last[sym]
	return u, ok
}

// Server wires transports to a hub.
type Server struct {
	Hub      *fanout.Hub
	Latest   *Latest
	Registry *prometheus.Registry
	Log      *slog.Logger
	Status   func() any // JSON-able status (engine stats, sessions)
	// WriteTimeout bounds a single client write; a client that cannot accept a write within it
	// is disconnected (protects against half-dead TCP peers).
	WriteTimeout time.Duration
}

// JSONEncoder returns a Hub.Encode function producing the WebSocket JSON for an update.
func JSONEncoder() func(*pipeline.Update) []byte {
	return func(u *pipeline.Update) []byte { return AppendJSON(make([]byte, 0, 320), u) }
}

// HTTPHandler returns the HTTP mux: /ws, /v1/bbo, /v1/status, /metrics, /debug/pprof/.
func (s *Server) HTTPHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("/v1/bbo", func(w http.ResponseWriter, r *http.Request) {
		u, ok := s.Latest.Get(r.URL.Query().Get("symbol"))
		if !ok {
			http.Error(w, "unknown symbol or no data yet", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(AppendJSON(nil, &u))
	})
	mux.HandleFunc("/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(s.Status())
	})
	if s.Registry != nil {
		mux.Handle("/metrics", promhttp.HandlerFor(s.Registry, promhttp.HandlerOpts{}))
	}
	mux.HandleFunc("/debug/pprof/", pprof.Index)
	mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	mux.HandleFunc("/debug/pprof/trace", pprof.Trace)
	return mux
}

// handleWS streams JSON updates: /ws?symbols=BTC-USD,ETH-USD&policy=conflate|queue|disconnect&queue=1024
// Each WebSocket message is a JSON array of one or more updates.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pol, ok := fanout.ParsePolicy(q.Get("policy"))
	if !ok {
		http.Error(w, "bad policy", http.StatusBadRequest)
		return
	}
	var syms []string
	if v := q.Get("symbols"); v != "" {
		syms = strings.Split(v, ",")
	}
	ql, _ := strconv.Atoi(q.Get("queue"))
	var interval time.Duration
	if v := q.Get("interval"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 || d > 10*time.Second {
			http.Error(w, "bad interval", http.StatusBadRequest)
			return
		}
		interval = d
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
	if err != nil {
		return
	}
	defer func() { _ = c.CloseNow() }()
	ctx := c.CloseRead(r.Context()) // we never read; this handles pings/close frames
	cl := s.Hub.Subscribe(pol, syms, ql)
	defer s.Hub.Unsubscribe(cl)
	s.stream(ctx, cl, interval, func(ctx context.Context, batch []fanout.Item, buf []byte) ([]byte, error) {
		// One WebSocket message per batch: a JSON array of the updates ready for this client.
		buf = append(buf[:0], '[')
		for i := range batch {
			if i > 0 {
				buf = append(buf, ',')
			}
			if batch[i].Wire != nil {
				buf = append(buf, batch[i].Wire...)
			} else {
				buf = AppendJSON(buf, &batch[i].U)
			}
		}
		buf = append(buf, ']')
		return buf, c.Write(ctx, websocket.MessageText, buf)
	})
	if cl.Disconnect.Load() {
		_ = c.Close(websocket.StatusPolicyViolation, "slow consumer")
	}
}

// stream writes batches to one client. A non-zero interval throttles the client to at most one
// write per interval; with the conflate policy that yields the latest state of every changed
// symbol each interval ("throttled conflation"), bounding per-client syscalls.
func (s *Server) stream(ctx context.Context, cl *fanout.Client, interval time.Duration,
	write func(context.Context, []fanout.Item, []byte) ([]byte, error)) {
	batch := make([]fanout.Item, 0, 256)
	buf := make([]byte, 0, 1024)
	stop := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(stop)
	}()
	wt := s.WriteTimeout
	if wt == 0 {
		wt = 5 * time.Second
	}
	for {
		var ok bool
		batch, ok = cl.Next(batch[:0], stop)
		if !ok {
			return
		}
		wctx, cancel := context.WithTimeout(ctx, wt)
		var err error
		buf, err = write(wctx, batch, buf)
		timedOut := wctx.Err() == context.DeadlineExceeded && ctx.Err() == nil
		cancel()
		if err != nil {
			if timedOut {
				s.Hub.WriteTimeouts.Add(1) // peer stopped reading: cut it off, never block the hub
			}
			return
		}
		if interval > 0 {
			select {
			case <-time.After(interval):
			case <-stop:
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------------------------
// gRPC: service marketmux.v1.MarketData, method StreamBBO (server streaming).

// ServiceName is the gRPC service name.
const ServiceName = "marketmux.v1.MarketData"

// StreamMethod is the full method name for clients.
const StreamMethod = "/" + ServiceName + "/StreamBBO"

// RegisterGRPC registers the streaming service on g.
func (s *Server) RegisterGRPC(g *grpc.Server) {
	g.RegisterService(&grpc.ServiceDesc{
		ServiceName: ServiceName,
		HandlerType: (*any)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "StreamBBO",
			ServerStreams: true,
			Handler: func(_ any, st grpc.ServerStream) error {
				var req SubscribeRequest
				if err := st.RecvMsg(&req); err != nil {
					return err
				}
				pol, ok := fanout.ParsePolicy(req.Policy)
				if !ok {
					pol = fanout.Conflate
				}
				cl := s.Hub.Subscribe(pol, req.Symbols, int(req.QueueLen))
				defer s.Hub.Unsubscribe(cl)
				var out UpdateBatch
				s.stream(st.Context(), cl, 0, func(_ context.Context, batch []fanout.Item, buf []byte) ([]byte, error) {
					out.Updates = out.Updates[:0]
					for i := range batch {
						out.Updates = append(out.Updates, batch[i].U)
					}
					return buf, st.SendMsg(&out)
				})
				return nil
			},
		}},
		Metadata: "marketmux.proto",
	}, nil)
}

// GRPCStreamDesc is the client-side descriptor for StreamBBO.
var GRPCStreamDesc = &grpc.StreamDesc{StreamName: "StreamBBO", ServerStreams: true}
