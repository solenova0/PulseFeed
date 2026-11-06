package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestConsumeCountsEverySubscriberOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for _, payload := range []string{`{"epoch":"test","sequence":"1"}`, `{"epoch":"test","sequence":"2"}`} {
			if err := conn.WriteMessage(websocket.TextMessage, []byte(payload)); err != nil {
				return
			}
		}
		<-ctx.Done()
	}))
	defer server.Close()
	counts := &counters{}
	var workers sync.WaitGroup
	for id := 0; id < 3; id++ {
		workers.Add(1)
		go func(id int) {
			defer workers.Done()
			consume(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), "", id, true, 0, counts)
		}(id)
	}
	defer workers.Wait()
	defer cancel()
	deadline := time.Now().Add(3 * time.Second)
	for counts.events.Load() < 6 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if counts.events.Load() != 6 || counts.clientsWithEvents.Load() != 3 {
		t.Fatalf("events=%d clients_with_events=%d", counts.events.Load(), counts.clientsWithEvents.Load())
	}
	if counts.failures.Load() != 0 {
		t.Fatalf("failures=%d", counts.failures.Load())
	}
}

func TestFailedSubscriberDoesNotCountAsReceiving(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	counts := &counters{}
	consume(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), "", 0, true, 0, counts)
	if counts.failures.Load() == 0 || counts.clientsWithEvents.Load() != 0 {
		t.Fatalf("failures=%d clients_with_events=%d", counts.failures.Load(), counts.clientsWithEvents.Load())
	}
}
