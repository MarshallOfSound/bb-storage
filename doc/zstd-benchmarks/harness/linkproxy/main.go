//go:build ignore

// linkproxy is a userspace TCP proxy that emulates a WAN link: a fixed
// one-way propagation delay in each direction, a shared bottleneck rate per
// direction, and a bounded bottleneck queue (so a saturated link builds up
// at most -queue of extra delay and pushes back on the sender like a real
// router queue would). It exposes byte counters on an HTTP endpoint.
package main

import (
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type link struct {
	mu       sync.Mutex
	free     time.Time // when the bottleneck finishes serializing queued bytes
	bps      float64   // bytes per second
	delay    time.Duration
	maxQueue time.Duration
	bytes    atomic.Int64
}

// admit reserves serialization time for n bytes and returns when they
// arrive at the far end. It blocks while the bottleneck queue is full.
func (l *link) admit(n int) time.Time {
	for {
		l.mu.Lock()
		now := time.Now()
		if l.free.Before(now) {
			l.free = now
		}
		if backlog := l.free.Sub(now); backlog > l.maxQueue {
			l.mu.Unlock()
			time.Sleep(backlog - l.maxQueue)
			continue
		}
		l.free = l.free.Add(time.Duration(float64(n) / l.bps * float64(time.Second)))
		arrive := l.free.Add(l.delay)
		l.mu.Unlock()
		l.bytes.Add(int64(n))
		return arrive
	}
}

type pkt struct {
	b      []byte
	arrive time.Time
}

func pipe(dst, src net.Conn, l *link) {
	ch := make(chan pkt, 1024)
	go func() {
		defer close(ch)
		for {
			b := make([]byte, 16*1024)
			n, err := src.Read(b)
			if n > 0 {
				ch <- pkt{b[:n], l.admit(n)}
			}
			if err != nil {
				return
			}
		}
	}()
	for p := range ch {
		if d := time.Until(p.arrive); d > 0 {
			time.Sleep(d)
		}
		if _, err := dst.Write(p.b); err != nil {
			break
		}
	}
	for range ch {
	}
	if tc, ok := dst.(*net.TCPConn); ok {
		tc.CloseWrite()
	} else {
		dst.Close()
	}
}

func main() {
	listen := flag.String("listen", "127.0.0.1:28980", "")
	target := flag.String("target", "127.0.0.1:18980", "")
	statsAddr := flag.String("stats", "127.0.0.1:28999", "")
	rttMs := flag.Float64("rtt_ms", 2, "round trip time")
	mbps := flag.Float64("mbps", 2000, "bottleneck rate per direction, megabit/s")
	queueMs := flag.Float64("queue_ms", 0, "max bottleneck queue delay (default: 1 RTT + 2ms)")
	flag.Parse()
	q := *queueMs
	if q == 0 {
		q = *rttMs + 2
	}
	mk := func() *link {
		return &link{
			bps:      *mbps * 1e6 / 8,
			delay:    time.Duration(*rttMs / 2 * float64(time.Millisecond)),
			maxQueue: time.Duration(q * float64(time.Millisecond)),
		}
	}
	up, down := mk(), mk()
	var conns atomic.Int64
	go func() {
		http.HandleFunc("/stats", func(w http.ResponseWriter, r *http.Request) {
			json.NewEncoder(w).Encode(map[string]int64{
				"c2s": up.bytes.Load(), "s2c": down.bytes.Load(), "conns": conns.Load(),
			})
		})
		log.Fatal(http.ListenAndServe(*statsAddr, nil))
	}()
	ln, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("proxy %s -> %s rtt=%.1fms rate=%.0fMbps queue=%.1fms", *listen, *target, *rttMs, *mbps, q)
	for {
		c, err := ln.Accept()
		if err != nil {
			log.Fatal(err)
		}
		conns.Add(1)
		go func(c net.Conn) {
			s, err := net.Dial("tcp", *target)
			if err != nil {
				c.Close()
				return
			}
			go pipe(s, c, up)
			pipe(c, s, down)
			_ = io.EOF
		}(c)
	}
}
