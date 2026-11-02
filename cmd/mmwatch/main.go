// Command mmwatch subscribes to a marketmux server over WebSocket and renders the consolidated
// book per symbol in the terminal.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/valyala/fastjson"
)

type row struct {
	bp, bq, bv, ap, aq, av string
	crossed                bool
	venues                 map[string][4]string
	n                      int
}

func f(s string, places int) string {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return s
	}
	return strconv.FormatFloat(v, 'f', places, 64)
}

func main() {
	url := flag.String("url", "ws://127.0.0.1:8080/ws?policy=conflate", "marketmux WS endpoint")
	every := flag.Duration("every", 250*time.Millisecond, "refresh interval")
	dur := flag.Duration("duration", 0, "exit after this long")
	flag.Parse()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if *dur > 0 {
		ctx, cancel = context.WithTimeout(ctx, *dur)
		defer cancel()
	}
	c, _, err := websocket.Dial(ctx, *url, nil)
	if err != nil {
		fmt.Fprintln(os.Stderr, "dial:", err)
		os.Exit(1)
	}
	defer c.CloseNow()
	rows := map[string]*row{}
	msgs := 0
	tick := time.NewTicker(*every)
	defer tick.Stop()
	in := make(chan []byte, 4096)
	go func() {
		for {
			_, b, err := c.Read(ctx)
			if err != nil {
				close(in)
				return
			}
			in <- b
		}
	}()
	var p fastjson.Parser
	for {
		select {
		case b, ok := <-in:
			if !ok {
				return
			}
			arr, err := p.ParseBytes(b)
			if err != nil {
				continue
			}
			for _, v := range arr.GetArray() {
				msgs++
				s := string(v.GetStringBytes("s"))
				r := rows[s]
				if r == nil {
					r = &row{}
					rows[s] = r
				}
				r.n++
				r.bp, r.bq, r.bv = string(v.GetStringBytes("bp")), string(v.GetStringBytes("bq")), string(v.GetStringBytes("bv"))
				r.ap, r.aq, r.av = string(v.GetStringBytes("ap")), string(v.GetStringBytes("aq")), string(v.GetStringBytes("av"))
				r.crossed = v.GetBool("x")
				r.venues = map[string][4]string{}
				v.GetObject("v").Visit(func(k []byte, t *fastjson.Value) {
					a := t.GetArray()
					if len(a) == 4 {
						r.venues[string(k)] = [4]string{string(a[0].GetStringBytes()), string(a[1].GetStringBytes()), string(a[2].GetStringBytes()), string(a[3].GetStringBytes())}
					}
				})
			}
		case <-tick.C:
			render(rows, msgs)
		case <-ctx.Done():
			return
		}
	}
}

func render(rows map[string]*row, msgs int) {
	var sb strings.Builder
	sb.WriteString("\033[2J\033[H")
	fmt.Fprintf(&sb, "marketmux consolidated BBO  %s   updates received: %d\n\n", time.Now().Format("15:04:05.000"), msgs)
	syms := make([]string, 0, len(rows))
	for s := range rows {
		syms = append(syms, s)
	}
	sort.Strings(syms)
	for _, s := range syms {
		r := rows[s]
		bp, _ := strconv.ParseFloat(r.bp, 64)
		ap, _ := strconv.ParseFloat(r.ap, 64)
		state := "spread"
		if r.crossed {
			state = "CROSSED"
		}
		bps := (ap - bp) / ((ap + bp) / 2) * 1e4
		fmt.Fprintf(&sb, "%-8s  bid %12s x %-12s %-9s | ask %12s x %-12s %-9s  %s %+.2f bps\n", s,
			f(r.bp, 4), f(r.bq, 4), r.bv, f(r.ap, 4), f(r.aq, 4), r.av, state, bps)
		vs := make([]string, 0, len(r.venues))
		for v := range r.venues {
			vs = append(vs, v)
		}
		sort.Strings(vs)
		for _, v := range vs {
			t := r.venues[v]
			fmt.Fprintf(&sb, "    %-9s %12s x %-12s   %12s x %-12s\n", v, f(t[0], 4), f(t[1], 4), f(t[2], 4), f(t[3], 4))
		}
		sb.WriteString("\n")
	}
	os.Stdout.WriteString(sb.String())
}
