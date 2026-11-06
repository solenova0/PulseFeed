package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "pulsefeed/gateway/gen"
)

func TestSequenceGapEpochAndDuplicate(t *testing.T) {
	m := newMetrics()
	s := sequenceTracker{}
	for _, seq := range []uint64{0, 1, 4} {
		if !s.accept(&pb.FeedEvent{Epoch: "a", Sequence: seq}, m) {
			t.Fatal("fresh sequence rejected")
		}
	}
	if s.accept(&pb.FeedEvent{Epoch: "a", Sequence: 4}, m) || s.accept(&pb.FeedEvent{Epoch: "a", Sequence: 3}, m) {
		t.Fatal("duplicate accepted")
	}
	if !s.accept(&pb.FeedEvent{Epoch: "b", Sequence: 0}, m) {
		t.Fatal("new epoch rejected")
	}
	if got := testutil.ToFloat64(m.gaps); got != 2 {
		t.Fatalf("missing sequences = %v", got)
	}
	if got := testutil.ToFloat64(m.epochs); got != 1 {
		t.Fatalf("epochs = %v", got)
	}
	if got := testutil.ToFloat64(m.duplicates); got != 2 {
		t.Fatalf("duplicates = %v", got)
	}
}

func TestHubEvictsSlowClientAndCachesSnapshot(t *testing.T) {
	m := newMetrics()
	h := newHub(2, 1, m)
	slow := &client{send: make(chan []byte, 1), done: make(chan struct{})}
	fast := &client{send: make(chan []byte, 1), done: make(chan struct{})}
	if !h.add(slow) || !h.add(fast) {
		t.Fatal("could not register")
	}
	h.broadcast([]byte("first"), []byte("snapshot1"))
	<-fast.send
	h.broadcast([]byte("second"), []byte("snapshot2"))
	select {
	case <-slow.done:
	default:
		t.Fatal("slow client not closed")
	}
	if string(<-fast.send) != "second" {
		t.Fatal("fast client lost event")
	}
	late := &client{send: make(chan []byte, 1), done: make(chan struct{})}
	if !h.add(late) || string(<-late.send) != "snapshot2" {
		t.Fatal("late client missing cached snapshot")
	}
	if testutil.ToFloat64(m.slow) != 1 {
		t.Fatal("missing slow client metric")
	}
	h.close()
	if testutil.ToFloat64(m.clients) != 0 {
		t.Fatal("client gauge leaked")
	}
}

func TestHubConcurrentMembership(t *testing.T) {
	h := newHub(100, 4, newMetrics())
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for n := 0; n < 100; n++ {
				c := &client{send: make(chan []byte, 4), done: make(chan struct{})}
				if h.add(c) {
					h.broadcast([]byte("update"), []byte("snapshot"))
					h.remove(c)
				}
			}
		}()
	}
	wg.Wait()
	h.close()
}

func TestConfigurationRejectsUnsafeValues(t *testing.T) {
	t.Setenv("INGEST", "true")
	t.Setenv("ENGINE_TLS", "false")
	t.Setenv("ENGINE_TOKEN", "secret")
	if _, err := loadConfig(); err == nil {
		t.Fatal("plaintext engine token accepted")
	}
	t.Setenv("ENGINE_TOKEN", "")
	t.Setenv("MAX_CLIENTS", "0")
	if _, err := loadConfig(); err == nil {
		t.Fatal("zero connection limit accepted")
	}
	t.Setenv("MAX_CLIENTS", "2")
	t.Setenv("ALLOWED_ORIGINS", "https://example.com/path")
	if _, err := loadConfig(); err == nil {
		t.Fatal("invalid origin accepted")
	}
}

func TestOriginAndAuthentication(t *testing.T) {
	c := testConfig()
	c.authToken = "test-secret"
	a, err := newApplication(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.redis.Close()
	a.redisHealthy.Store(true)
	a.subscribed.Store(true)
	r := httptest.NewRequest(http.MethodGet, "http://example.com/ws", nil)
	r.Header.Set("Origin", "https://other.com")
	if a.allowedOrigin(r) {
		t.Fatal("foreign origin accepted")
	}
	r.Header.Set("Origin", "https://example.com")
	if !a.allowedOrigin(r) {
		t.Fatal("same host rejected")
	}
	response := httptest.NewRecorder()
	a.routes().ServeHTTP(response, r)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated code=%d", response.Code)
	}
}

func TestRedisSubscriptionReconnect(t *testing.T) {
	server := miniredis.RunT(t)
	c := testConfig()
	c.redisAddr = server.Addr()
	a, err := newApplication(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.redis.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	workers.Add(2)
	go func() { defer workers.Done(); a.checkRedis(ctx) }()
	go func() { defer workers.Done(); a.consume(ctx) }()
	defer func() { cancel(); workers.Wait() }()
	eventually(t, func() bool { return a.subscribed.Load() })
	payload, err := proto.Marshal(&pb.FeedEvent{Epoch: "reconnect-test", Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.redis.Publish(ctx, c.redisChannel, payload).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, a.ready)
	server.Close()
	eventually(t, func() bool { return !a.subscribed.Load() })
	if a.stateReady.Load() {
		t.Fatal("cached state remained ready after losing the subscription")
	}
	if err := server.Restart(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return a.redisHealthy.Load() && a.subscribed.Load() })
	if a.ready() {
		t.Fatal("reconnected subscription became ready with stale depth")
	}
	if err := a.redis.Publish(ctx, c.redisChannel, payload).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return testutil.ToFloat64(a.metrics.duplicates) == 1 })
	if a.ready() {
		t.Fatal("duplicate event restored readiness with stale depth")
	}
	payload, err = proto.Marshal(&pb.FeedEvent{Epoch: "reconnect-test", Sequence: 2, Snapshot: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := a.redis.Publish(ctx, c.redisChannel, payload).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, a.ready)
	if testutil.ToFloat64(a.metrics.gaps) != 1 {
		t.Fatal("sequence tracking was lost during the reconnect")
	}
	if testutil.ToFloat64(a.metrics.reconnects) < 1 {
		t.Fatal("subscription reconnect was not counted")
	}
}

type testEngine struct {
	pb.UnimplementedMarketDataServer
	event *pb.FeedEvent
}

func (e *testEngine) Subscribe(_ *pb.SubscribeRequest, stream grpc.ServerStreamingServer[pb.FeedEvent]) error {
	if err := stream.Send(e.event); err != nil {
		return err
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func testConfig() config {
	return config{redisAddr: "localhost:6379", redisChannel: "test-feed", maxClients: 8, clientBuffer: 4, origins: map[string]struct{}{}}
}

func eventually(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}

func TestGRPCRedisWebSocketPipeline(t *testing.T) {
	redisServer := miniredis.RunT(t)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	engine := grpc.NewServer()
	event := &pb.FeedEvent{Epoch: "test-epoch", Sequence: 9007199254740993, UnixNanos: uint64(time.Now().UnixNano()), Bids: []*pb.Level{{Price: 100, Quantity: 20, Orders: 2}}, Trade: &pb.Trade{MakerOrderId: 1, TakerOrderId: 2, Price: 100, Quantity: 1, AggressorSide: pb.Side_BUY}}
	pb.RegisterMarketDataServer(engine, &testEngine{event: event})
	go func() { _ = engine.Serve(listener) }()
	defer engine.Stop()
	c := testConfig()
	c.redisAddr = redisServer.Addr()
	c.engineAddr = listener.Addr().String()
	c.ingest = true
	a, err := newApplication(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.redis.Close()
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	start := func(f func()) { workers.Add(1); go func() { defer workers.Done(); f() }() }
	start(func() { a.checkRedis(ctx) })
	start(func() { a.consume(ctx) })
	defer func() { cancel(); a.hub.close(); workers.Wait() }()
	eventually(t, func() bool { return a.subscribed.Load() })
	start(func() { a.ingest(ctx, insecure.NewCredentials()) })
	eventually(t, func() bool { return a.ready() && testutil.ToFloat64(a.metrics.received) == 1 })
	server := httptest.NewServer(a.routes())
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, payload, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	got := &pb.FeedEvent{}
	if err := protojson.Unmarshal(payload, got); err != nil {
		t.Fatal(err)
	}
	if !got.Snapshot || got.Trade != nil || got.Sequence != event.Sequence || len(got.Bids) != 1 {
		t.Fatalf("invalid initial snapshot: %s", payload)
	}
	if !strings.Contains(string(payload), `"sequence":"9007199254740993"`) && !strings.Contains(string(payload), `"sequence": "9007199254740993"`) {
		t.Fatalf("uint64 precision lost: %s", payload)
	}
	update := proto.Clone(event).(*pb.FeedEvent)
	update.Sequence += 2
	payload, err = proto.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.redis.Publish(ctx, c.redisChannel, payload).Err(); err != nil {
		t.Fatal(err)
	}
	_, payload, err = conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if err := protojson.Unmarshal(payload, got); err != nil {
		t.Fatal(err)
	}
	if got.Sequence != update.Sequence || got.Trade == nil || got.Snapshot {
		t.Fatalf("invalid live event: %s", payload)
	}
	if testutil.ToFloat64(a.metrics.gaps) != 1 {
		t.Fatal("missed sequence gap")
	}
	if err := a.redis.Publish(ctx, c.redisChannel, []byte("broken protobuf")).Err(); err != nil {
		t.Fatal(err)
	}
	eventually(t, func() bool { return testutil.ToFloat64(a.metrics.invalid) == 1 })
}
