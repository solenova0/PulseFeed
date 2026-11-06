// PulseFeed's gateway deliberately keeps all Go allocation, networking and fanout
// outside the C++ matching thread. One ingest replica publishes; all replicas
// subscribe to Redis and suppress duplicate sequence numbers within an epoch.
// The feed is best-effort: every event carries recoverable full top-10 depth.
package main

import (
	"context"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	pb "pulsefeed/gateway/gen"
)

const (
	writeWait     = 10 * time.Second
	pongWait      = 60 * time.Second
	pingPeriod    = 25 * time.Second
	maxEventBytes = 1 << 20
)

type config struct {
	httpAddr, engineAddr, redisAddr, redisURL, redisChannel string
	authToken, engineToken, engineCA, engineServerName      string
	origins                                                 map[string]struct{}
	ingest, engineTLS                                       bool
	maxClients, clientBuffer                                int
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func httpAddress() (string, error) {
	if address := os.Getenv("HTTP_ADDR"); address != "" {
		return address, nil
	}
	port := env("PORT", "8080")
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return "", errors.New("PORT must be 1..65535")
	}
	return net.JoinHostPort("", strconv.Itoa(number)), nil
}

func loadConfig() (config, error) {
	address, err := httpAddress()
	if err != nil {
		return config{}, err
	}
	c := config{
		httpAddr: address, engineAddr: env("ENGINE_ADDR", "engine:50051"),
		redisAddr: env("REDIS_ADDR", "redis:6379"), redisURL: os.Getenv("REDIS_URL"),
		redisChannel: env("REDIS_CHANNEL", "pulsefeed.events"), authToken: os.Getenv("AUTH_TOKEN"),
		engineToken: os.Getenv("ENGINE_TOKEN"), engineCA: os.Getenv("ENGINE_CA_FILE"),
		engineServerName: os.Getenv("ENGINE_SERVER_NAME"), origins: make(map[string]struct{}),
	}
	if c.ingest, err = strconv.ParseBool(env("INGEST", "true")); err != nil {
		return c, fmt.Errorf("INGEST: %w", err)
	}
	if c.engineTLS, err = strconv.ParseBool(env("ENGINE_TLS", "false")); err != nil {
		return c, fmt.Errorf("ENGINE_TLS: %w", err)
	}
	if c.maxClients, err = strconv.Atoi(env("MAX_CLIENTS", "2000")); err != nil || c.maxClients < 1 || c.maxClients > 100000 {
		return c, errors.New("MAX_CLIENTS must be 1..100000")
	}
	if c.clientBuffer, err = strconv.Atoi(env("CLIENT_BUFFER", "64")); err != nil || c.clientBuffer < 1 || c.clientBuffer > 4096 {
		return c, errors.New("CLIENT_BUFFER must be 1..4096")
	}
	for _, origin := range strings.Split(os.Getenv("ALLOWED_ORIGINS"), ",") {
		origin = strings.TrimSpace(origin)
		if origin == "" {
			continue
		}
		u, err := url.Parse(origin)
		if err != nil || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.User != nil {
			return c, fmt.Errorf("invalid allowed origin %q", origin)
		}
		c.origins[origin] = struct{}{}
	}
	if !c.engineTLS && (c.engineCA != "" || c.engineServerName != "" || c.engineToken != "") {
		return c, errors.New("ENGINE_CA_FILE, ENGINE_SERVER_NAME and ENGINE_TOKEN require ENGINE_TLS=true")
	}
	return c, nil
}

type metrics struct {
	registry                                                                                    *prometheus.Registry
	clients, redisUp, subscriberUp, engineUp                                                    prometheus.Gauge
	ingested, published, received, gaps, epochs, duplicates, invalid, slow, rejects, reconnects prometheus.Counter
	eventAge                                                                                    prometheus.Histogram
}

func newMetrics() *metrics {
	r := prometheus.NewRegistry()
	m := &metrics{registry: r}
	gauge := func(name, help string) prometheus.Gauge {
		g := prometheus.NewGauge(prometheus.GaugeOpts{Name: name, Help: help})
		r.MustRegister(g)
		return g
	}
	counter := func(name, help string) prometheus.Counter {
		v := prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: help})
		r.MustRegister(v)
		return v
	}
	m.clients = gauge("pulsefeed_gateway_clients", "Current local WebSocket connections.")
	m.redisUp = gauge("pulsefeed_gateway_redis_up", "Last Redis command health probe succeeded.")
	m.subscriberUp = gauge("pulsefeed_gateway_subscriber_up", "Redis Pub/Sub subscription is confirmed.")
	m.engineUp = gauge("pulsefeed_gateway_engine_up", "Ingest gRPC stream has delivered an event and remains connected.")
	m.ingested = counter("pulsefeed_gateway_ingested_total", "Events received from gRPC.")
	m.published = counter("pulsefeed_gateway_published_total", "Events successfully published to Redis.")
	m.received = counter("pulsefeed_gateway_received_total", "Valid unique Redis events delivered to local fanout.")
	m.gaps = counter("pulsefeed_gateway_sequence_gaps_total", "Missing sequence numbers observed within an engine epoch.")
	m.epochs = counter("pulsefeed_gateway_epoch_changes_total", "Engine epoch changes after the first observed epoch.")
	m.duplicates = counter("pulsefeed_gateway_duplicates_total", "Duplicate or out of order Redis events suppressed.")
	m.invalid = counter("pulsefeed_gateway_invalid_events_total", "Malformed, oversized or invalid Redis events rejected.")
	m.slow = counter("pulsefeed_gateway_slow_clients_total", "Clients evicted when their bounded outbound queue fills.")
	m.rejects = counter("pulsefeed_gateway_rejected_clients_total", "Connections rejected at configured capacity.")
	m.reconnects = counter("pulsefeed_gateway_reconnects_total", "gRPC and Redis subscription retry attempts.")
	m.eventAge = prometheus.NewHistogram(prometheus.HistogramOpts{Name: "pulsefeed_gateway_event_age_seconds", Help: "Engine timestamp to gateway fanout age; requires synchronized clocks, includes all transport and queueing.", Buckets: []float64{.0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 5}})
	r.MustRegister(m.eventAge, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	return m
}

type client struct {
	connMu sync.Mutex
	conn   *websocket.Conn
	send   chan []byte
	done   chan struct{}
	once   sync.Once
}

func (c *client) stop() {
	c.once.Do(func() {
		close(c.done)
		c.connMu.Lock()
		defer c.connMu.Unlock()
		if c.conn != nil {
			_ = c.conn.Close()
		}
	})
}

func (c *client) attach(conn *websocket.Conn) bool {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	select {
	case <-c.done:
		_ = conn.Close()
		return false
	default:
		c.conn = conn
		return true
	}
}

// A short mutex protects membership. No goroutine can close a send channel while
// another writes to it: channels stay open and done carries cancellation.
type hub struct {
	mu          sync.Mutex
	clients     map[*client]struct{}
	latest      []byte
	max, buffer int
	closed      bool
	metrics     *metrics
}

func newHub(max, buffer int, m *metrics) *hub {
	return &hub{clients: make(map[*client]struct{}), max: max, buffer: buffer, metrics: m}
}

func (h *hub) add(c *client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || len(h.clients) >= h.max {
		h.metrics.rejects.Inc()
		return false
	}
	h.clients[c] = struct{}{}
	h.metrics.clients.Inc()
	if h.latest != nil {
		c.send <- h.latest
	}
	return true
}

func (h *hub) remove(c *client) {
	h.mu.Lock()
	if _, exists := h.clients[c]; exists {
		delete(h.clients, c)
		h.metrics.clients.Dec()
	}
	h.mu.Unlock()
	c.stop()
}

func (h *hub) broadcast(payload, snapshot []byte) {
	var evicted []*client
	h.mu.Lock()
	if h.closed {
		h.mu.Unlock()
		return
	}
	h.latest = snapshot // Immutable bytes shared by all new subscriber queues.
	for c := range h.clients {
		select {
		case c.send <- payload:
		default:
			delete(h.clients, c)
			h.metrics.clients.Dec()
			h.metrics.slow.Inc()
			evicted = append(evicted, c)
		}
	}
	h.mu.Unlock()
	for _, c := range evicted {
		c.stop()
	}
}

func (h *hub) close() {
	h.mu.Lock()
	h.closed = true
	clients := make([]*client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
		delete(h.clients, c)
		h.metrics.clients.Dec()
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.stop()
	}
}

func (h *hub) readPump(c *client) {
	defer h.remove(c)
	c.conn.SetReadLimit(1024)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error { return c.conn.SetReadDeadline(time.Now().Add(pongWait)) })
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (h *hub) writePump(c *client) {
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer h.remove(c)
	for {
		select {
		case payload := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
				return
			}
		case <-ticker.C:
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(writeWait)); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

type sequenceTracker struct {
	epoch string
	last  uint64
	seen  bool
}

func (s *sequenceTracker) accept(event *pb.FeedEvent, m *metrics) bool {
	if s.seen && event.Epoch == s.epoch {
		if event.Sequence <= s.last {
			m.duplicates.Inc()
			return false
		}
		if event.Sequence-s.last > 1 {
			m.gaps.Add(float64(event.Sequence - s.last - 1))
		}
	} else if s.seen {
		m.epochs.Inc()
	}
	s.epoch, s.last, s.seen = event.Epoch, event.Sequence, true
	return true
}

type application struct {
	cfg                                     config
	metrics                                 *metrics
	hub                                     *hub
	redis                                   *redis.Client
	redisHealthy, subscribed, engineHealthy atomic.Bool
	stateReady                              atomic.Bool
	stopping                                atomic.Bool
}

func newApplication(c config) (*application, error) {
	options := &redis.Options{Addr: c.redisAddr}
	if c.redisURL != "" {
		var err error
		options, err = redis.ParseURL(c.redisURL)
		if err != nil {
			return nil, fmt.Errorf("REDIS_URL: %w", err)
		}
	}
	options.DialTimeout = 3 * time.Second
	options.ReadTimeout = 3 * time.Second
	options.WriteTimeout = 3 * time.Second
	options.ContextTimeoutEnabled = true
	options.MaxRetries = -1 // App retries whole publish/subscription attempts explicitly.
	m := newMetrics()
	return &application{cfg: c, metrics: m, hub: newHub(c.maxClients, c.clientBuffer, m), redis: redis.NewClient(options)}, nil
}

func (a *application) ready() bool {
	return !a.stopping.Load() && a.stateReady.Load() && a.redisHealthy.Load() && a.subscribed.Load() && (!a.cfg.ingest || a.engineHealthy.Load())
}

func (a *application) allowedOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	if len(a.cfg.origins) > 0 {
		_, ok := a.cfg.origins[origin]
		return ok
	}
	u, err := url.Parse(origin)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && strings.EqualFold(u.Host, r.Host) && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}

func (a *application) routes() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(a.metrics.registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		if a.stopping.Load() {
			http.Error(w, "stopping", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if !a.ready() {
			http.Error(w, "dependencies unavailable", http.StatusServiceUnavailable)
			return
		}
		_, _ = w.Write([]byte("ready\n"))
	})
	mux.HandleFunc("/ws", a.serveWebSocket)
	mux.Handle("/", newAssetHandler())
	return mux
}

func (a *application) serveWebSocket(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "GET required", http.StatusMethodNotAllowed)
		return
	}
	if a.cfg.authToken != "" {
		provided := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		if subtle.ConstantTimeCompare([]byte(provided), []byte(a.cfg.authToken)) != 1 || !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
			w.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}
	if !a.ready() {
		http.Error(w, "gateway not ready", http.StatusServiceUnavailable)
		return
	}
	// Reserve membership before upgrading, so capacity failures return HTTP 503.
	c := &client{send: make(chan []byte, a.cfg.clientBuffer), done: make(chan struct{})}
	if !a.hub.add(c) {
		http.Error(w, "subscriber capacity reached", http.StatusServiceUnavailable)
		return
	}
	upgrader := websocket.Upgrader{ReadBufferSize: 1024, WriteBufferSize: 4096, HandshakeTimeout: 5 * time.Second, CheckOrigin: a.allowedOrigin}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		a.hub.remove(c)
		return
	}
	// Eviction can occur during the handshake; attach closes that connection.
	if !c.attach(conn) {
		return
	}
	go a.hub.writePump(c)
	a.hub.readPump(c)
}

func pause(ctx context.Context, attempt uint) bool {
	if attempt > 5 {
		attempt = 5
	}
	d := (time.Duration(1<<attempt) * 100 * time.Millisecond) + time.Duration(rand.IntN(100))*time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (a *application) checkRedis(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		probe, cancel := context.WithTimeout(ctx, time.Second)
		err := a.redis.Ping(probe).Err()
		cancel()
		a.redisHealthy.Store(err == nil)
		if err == nil {
			a.metrics.redisUp.Set(1)
		} else {
			a.metrics.redisUp.Set(0)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func validEvent(e *pb.FeedEvent) bool {
	if e.Epoch == "" || len(e.Epoch) > 128 || len(e.Bids) > 10 || len(e.Asks) > 10 {
		return false
	}
	for _, side := range [][]*pb.Level{e.Bids, e.Asks} {
		for _, l := range side {
			if l == nil || l.Price == 0 || l.Quantity == 0 || l.Orders == 0 {
				return false
			}
		}
	}
	return true
}

// Pings keep the dedicated subscription socket observable even for an idle
// instrument. ReceiveTimeout bounds silent half-open connections to 30 seconds.
func subscriptionHeartbeat(ctx context.Context, sub *redis.PubSub) func() {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				pingCtx, cancelPing := context.WithTimeout(ctx, time.Second)
				err := sub.Ping(pingCtx)
				cancelPing()
				if err != nil {
					_ = sub.Close()
					return
				}
			}
		}
	}()
	return func() { cancel(); <-done }
}

func (a *application) consume(ctx context.Context) {
	tracker := sequenceTracker{}
	for attempt := uint(0); ctx.Err() == nil; attempt++ {
		sub := a.redis.Subscribe(ctx, a.cfg.redisChannel)
		stopClose := context.AfterFunc(ctx, func() { _ = sub.Close() })
		stopHeartbeat := subscriptionHeartbeat(ctx, sub)
		if _, err := sub.ReceiveTimeout(ctx, 5*time.Second); err == nil {
			a.subscribed.Store(true)
			a.metrics.subscriberUp.Set(1)
			attempt = 0
			for ctx.Err() == nil {
				reply, err := sub.ReceiveTimeout(ctx, 30*time.Second)
				if err != nil {
					if ctx.Err() == nil {
						slog.Warn("Redis subscription interrupted", "error", err)
					}
					break
				}
				message, isMessage := reply.(*redis.Message)
				if !isMessage {
					continue
				}
				if len(message.Payload) > maxEventBytes {
					a.metrics.invalid.Inc()
					continue
				}
				event := &pb.FeedEvent{}
				if err := proto.Unmarshal([]byte(message.Payload), event); err != nil || !validEvent(event) {
					a.metrics.invalid.Inc()
					continue
				}
				if !tracker.accept(event, a.metrics) {
					continue
				}
				encoded, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(event)
				if err != nil {
					a.metrics.invalid.Inc()
					continue
				}
				initial := proto.Clone(event).(*pb.FeedEvent)
				initial.Trade, initial.Snapshot = nil, true
				snapshot, err := (protojson.MarshalOptions{EmitUnpopulated: true}).Marshal(initial)
				if err != nil {
					a.metrics.invalid.Inc()
					continue
				}
				a.metrics.received.Inc()
				if event.UnixNanos > 0 && event.UnixNanos <= uint64(time.Now().UnixNano()) {
					a.metrics.eventAge.Observe(float64(uint64(time.Now().UnixNano())-event.UnixNanos) / 1e9)
				}
				a.hub.broadcast(encoded, snapshot)
				a.stateReady.Store(true)
			}
		} else if ctx.Err() == nil {
			slog.Warn("Redis subscription failed", "error", err)
		}
		// Pub/Sub cannot replay updates missed during a disconnect. Require a
		// fresh full-depth event before admitting clients with a cached snapshot.
		a.stateReady.Store(false)
		_ = sub.Close()
		stopHeartbeat()
		stopClose()
		a.subscribed.Store(false)
		a.metrics.subscriberUp.Set(0)
		if ctx.Err() == nil {
			a.metrics.reconnects.Inc()
			if !pause(ctx, attempt) {
				return
			}
		}
	}
}

func (a *application) engineCredentials() (credentials.TransportCredentials, error) {
	if !a.cfg.engineTLS {
		return insecure.NewCredentials(), nil
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: a.cfg.engineServerName}
	if a.cfg.engineCA != "" {
		pem, err := os.ReadFile(a.cfg.engineCA)
		if err != nil {
			return nil, err
		}
		roots, err := x509.SystemCertPool()
		if err != nil {
			roots = x509.NewCertPool()
		}
		if !roots.AppendCertsFromPEM(pem) {
			return nil, errors.New("ENGINE_CA_FILE contains no certificates")
		}
		tlsConfig.RootCAs = roots
	}
	return credentials.NewTLS(tlsConfig), nil
}

func (a *application) ingest(ctx context.Context, transport credentials.TransportCredentials) {
	conn, err := grpc.NewClient(a.cfg.engineAddr, grpc.WithTransportCredentials(transport), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxEventBytes)), grpc.WithKeepaliveParams(keepalive.ClientParameters{Time: 30 * time.Second, Timeout: 10 * time.Second}))
	if err != nil {
		slog.Error("gRPC client initialization failed", "error", err)
		return
	}
	defer conn.Close()
	client := pb.NewMarketDataClient(conn)
	for attempt := uint(0); ctx.Err() == nil; attempt++ {
		streamCtx, cancel := context.WithCancel(ctx)
		if a.cfg.engineToken != "" {
			streamCtx = metadata.AppendToOutgoingContext(streamCtx, "authorization", "Bearer "+a.cfg.engineToken)
		}
		stream, err := client.Subscribe(streamCtx, &pb.SubscribeRequest{}, grpc.WaitForReady(true))
		if err == nil {
			for ctx.Err() == nil {
				event, receiveErr := stream.Recv()
				if receiveErr != nil {
					err = receiveErr
					break
				}
				a.metrics.ingested.Inc()
				if !validEvent(event) {
					a.metrics.invalid.Inc()
					continue
				}
				a.engineHealthy.Store(true)
				a.metrics.engineUp.Set(1)
				attempt = 0
				payload, marshalErr := proto.Marshal(event)
				if marshalErr != nil {
					err = marshalErr
					break
				}
				publishCtx, publishCancel := context.WithTimeout(ctx, 2*time.Second)
				err = a.redis.Publish(publishCtx, a.cfg.redisChannel, payload).Err()
				publishCancel()
				if err != nil {
					break
				} // Reconnect requests the engine's current snapshot.
				a.metrics.published.Inc()
			}
		}
		cancel()
		a.engineHealthy.Store(false)
		a.metrics.engineUp.Set(0)
		if ctx.Err() == nil {
			slog.Warn("engine ingestion interrupted", "error", err)
			a.metrics.reconnects.Inc()
			if !pause(ctx, attempt) {
				return
			}
		}
	}
}

func run(ctx context.Context, c config) error {
	a, err := newApplication(c)
	if err != nil {
		return err
	}
	defer a.redis.Close()
	transport, err := a.engineCredentials()
	if err != nil {
		return err
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var workers sync.WaitGroup
	start := func(f func()) { workers.Add(1); go func() { defer workers.Done(); f() }() }
	start(func() { a.checkRedis(workerCtx) })
	start(func() { a.consume(workerCtx) })
	if c.ingest {
		start(func() { a.ingest(workerCtx, transport) })
	}
	server := &http.Server{Addr: c.httpAddr, Handler: a.routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	serverErrors := make(chan error, 1)
	go func() { serverErrors <- server.ListenAndServe() }()
	slog.Info("gateway listening", "address", c.httpAddr, "ingest", c.ingest, "max_clients", c.maxClients)
	select {
	case <-ctx.Done():
	case err = <-serverErrors:
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
	}
	a.stopping.Store(true)
	cancelWorkers()
	a.hub.close() // HTTP Shutdown does not close hijacked WebSockets.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		_ = server.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	workers.Wait()
	return err
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "--healthcheck" {
		address, err := httpAddress()
		if err != nil {
			os.Exit(1)
		}
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			os.Exit(1)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		client := &http.Client{Timeout: 2 * time.Second}
		response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/readyz")
		if err != nil {
			os.Exit(1)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK {
			os.Exit(1)
		}
		return
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	c, err := loadConfig()
	if err != nil {
		slog.Error("invalid configuration", "error", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		slog.Error("gateway stopped", "error", err)
		os.Exit(1)
	}
}
