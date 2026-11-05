// subscriber creates independent simulated market-data subscribers and reports
// received event counts, sequence gaps and reconnects without printing each one.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/encoding/protojson"
	pb "pulsefeed/gateway/gen"
)

type counters struct{ events, gaps, epochs, reconnects, failures atomic.Uint64 }

func consume(ctx context.Context, address, token string, id int, quiet bool, delay time.Duration, counts *counters) {
	var epoch string
	var previous uint64
	seen := false
	dialer := websocket.Dialer{Proxy: http.ProxyFromEnvironment, HandshakeTimeout: 5 * time.Second}
	headers := make(http.Header)
	if token != "" {
		headers.Set("Authorization", "Bearer "+token)
	}
	for attempt := uint(0); ctx.Err() == nil; attempt++ {
		conn, response, err := dialer.DialContext(ctx, address, headers)
		if err != nil {
			counts.failures.Add(1)
			if response != nil && response.Body != nil {
				_ = response.Body.Close()
			}
		} else {
			attempt = 0
			stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
			conn.SetReadLimit(1 << 20)
			_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
			conn.SetPingHandler(func(payload string) error {
				_ = conn.SetReadDeadline(time.Now().Add(90 * time.Second))
				return conn.WriteControl(websocket.PongMessage, []byte(payload), time.Now().Add(5*time.Second))
			})
			for ctx.Err() == nil {
				_, payload, readErr := conn.ReadMessage()
				if readErr != nil {
					break
				}
				event := &pb.FeedEvent{}
				if err := protojson.Unmarshal(payload, event); err != nil {
					counts.failures.Add(1)
					continue
				}
				counts.events.Add(1)
				if seen && event.Epoch == epoch && event.Sequence > previous && event.Sequence-previous > 1 {
					counts.gaps.Add(event.Sequence - previous - 1)
				}
				if seen && event.Epoch != epoch {
					counts.epochs.Add(1)
				}
				epoch, previous, seen = event.Epoch, event.Sequence, true
				if id == 0 && !quiet {
					fmt.Println(string(payload))
				}
				if delay > 0 {
					timer := time.NewTimer(delay)
					select {
					case <-ctx.Done():
						timer.Stop()
					case <-timer.C:
					}
				}
			}
			stop()
			_ = conn.Close()
		}
		if ctx.Err() != nil {
			return
		}
		counts.reconnects.Add(1)
		if attempt > 5 {
			attempt = 5
		}
		timer := time.NewTimer(time.Duration(1<<attempt)*100*time.Millisecond + time.Duration(rand.IntN(100))*time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func main() {
	address := flag.String("url", "ws://localhost:8080/ws", "WebSocket endpoint")
	n := flag.Int("clients", 10, "Number of concurrent subscriber connections (1..100000)")
	duration := flag.Duration("duration", 10*time.Second, "Test duration")
	token := flag.String("token", os.Getenv("AUTH_TOKEN"), "Bearer token; defaults to AUTH_TOKEN")
	quiet := flag.Bool("quiet", false, "Suppress first subscriber's event output")
	delay := flag.Duration("read-delay", 0, "Optional delay per event to simulate slow clients")
	flag.Parse()
	u, err := url.Parse(*address)
	if err != nil || u.Host == "" || (u.Scheme != "ws" && u.Scheme != "wss") || *n < 1 || *n > 100000 || *duration <= 0 || *delay < 0 {
		log.Fatal("valid ws(s) URL, 1..100000 clients, positive duration and nonnegative read-delay required")
	}
	signalCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signalCtx, *duration)
	defer cancel()
	counts := &counters{}
	var workers sync.WaitGroup
	start := time.Now()
	for id := 0; id < *n; id++ {
		workers.Add(1)
		go func(id int) { defer workers.Done(); consume(ctx, *address, *token, id, *quiet, *delay, counts) }(id)
	}
	workers.Wait()
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "clients=%d elapsed=%s events=%d events_per_second=%.0f missing_sequences=%d epoch_changes=%d reconnects=%d failures=%d\n", *n, elapsed.Round(time.Millisecond), counts.events.Load(), float64(counts.events.Load())/elapsed.Seconds(), counts.gaps.Load(), counts.epochs.Load(), counts.reconnects.Load(), counts.failures.Load())
	if counts.events.Load() == 0 {
		os.Exit(1)
	}
}
