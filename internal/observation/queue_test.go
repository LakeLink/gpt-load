package observation_test

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/ugorji/go/codec"

	"gpt-load/internal/observation"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/httproute"
	"gpt-load/internal/platform/i18n"
)

func TestSubscriptionQueueBudgetsAndCollectionLifetime(t *testing.T) {
	for _, test := range []struct {
		name                string
		events, bytes, want int
	}{
		{"event capacity", 2, 4096, 2},
		{"byte capacity", 5, 512, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			service, connect := observationFixture(t, test.events, test.bytes)
			snapshotCalls := 0
			publish := func() {
				service.Publish("sample", time.UnixMilli(1700000000123), func() any { snapshotCalls++; return map[string]any{"payload": strings.Repeat("x", 200)} })
			}
			publish()
			if snapshotCalls != 0 {
				t.Fatal("constructed snapshot before subscription")
			}
			first := connect()
			sendCommand(t, first, "subscribe")
			readMessage(t, first)
			// 新连接尚未订阅时关闭投递闸门，保留身份所属的离线队列。
			second := connect()
			defer second.CloseNow()
			// 控制回复确认接替已生效，同时保持新连接的订阅投递闸门关闭。
			sendCommand(t, second, "unknown")
			if reply := readMessage(t, second); reply["code"] != "unknown_command" {
				t.Fatalf("replacement readiness=%#v", reply)
			}
			first.CloseNow()
			for range 5 {
				publish()
			}
			sendCommand(t, second, "subscribe")
			reply := readMessage(t, second)
			if reply["type"] != "subscribed" || reply["max_event_bytes"] != int64(test.bytes) {
				t.Fatalf("confirmation=%#v", reply)
			}
			for offset := 0; offset < test.want; offset++ {
				event := readMessage(t, second)
				if event["seq"] != int64(6-test.want+offset) || event["observed_at"] != int64(1700000000123) {
					t.Fatalf("event=%#v", event)
				}
			}
			sendCommand(t, second, "unsubscribe")
			if reply := readMessage(t, second); reply["type"] != "unsubscribed" {
				t.Fatalf("confirmation=%#v", reply)
			}
			publish()
			if snapshotCalls != 5 {
				t.Fatal("constructed snapshot after unsubscribe")
			}
			sendCommand(t, second, "subscribe")
			readMessage(t, second)
			publish()
			if event := readMessage(t, second); event["seq"] != int64(6) {
				t.Fatalf("resubscription sequence=%#v", event)
			}
		})
	}
}

func sendCommand(t *testing.T, conn *websocket.Conn, kind string) {
	t.Helper()
	var data []byte
	err := codec.NewEncoderBytes(&data, &codec.MsgpackHandle{WriteExt: true}).Encode(map[string]string{"type": kind, "command_id": "command"})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(t.Context(), websocket.MessageBinary, data); err != nil {
		t.Fatal(err)
	}
}
func readMessage(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	handle := &codec.MsgpackHandle{WriteExt: true}
	handle.MapType = reflect.TypeOf(map[string]any{})
	handle.SignedInteger = true
	var value map[string]any
	if err := codec.NewDecoderBytes(data, handle).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func observationFixture(t *testing.T, events, bytes int, stall ...*writeStall) (*observation.Service, func() *websocket.Conn) {
	t.Helper()
	if err := i18n.Init(); err != nil {
		t.Fatal(err)
	}
	service := observation.New(&config.Config{Observation: config.ObservationConfig{Plugins: []config.ObservationPlugin{{ID: "local", Token: "test-token"}}, QueueMaxEvents: events, QueueMaxBytes: bytes, EventMaxBytes: bytes, WriteTimeoutSeconds: 1}})
	engine := gin.New()
	routes, err := httproute.NewRegistry(service.HTTPModule())
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.Bind(engine); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewUnstartedServer(engine)
	if len(stall) > 0 {
		host.Listener = stalledListener{host.Listener, stall[0]}
	}
	host.StartTLS()
	t.Cleanup(host.Close)
	return service, func() *websocket.Conn {
		t.Helper()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		conn, _, err := websocket.Dial(ctx, strings.Replace(host.URL, "https://", "wss://", 1)+"/api/plugins/local/events", &websocket.DialOptions{HTTPClient: host.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer test-token"}}, Subprotocols: []string{observation.Subprotocol}})
		if err != nil {
			t.Fatal(err)
		}
		conn.SetReadLimit(int64(bytes))
		return conn
	}
}

func TestCancellationExcludesSnapshotFromResubscription(t *testing.T) {
	service, connect := observationFixture(t, 10, 4096)
	conn := connect()
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	started, release, published := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
		<-published
	}()
	go func() {
		defer close(published)
		service.Publish("old", time.Now(), func() any { close(started); <-release; return "old subscription" })
	}()
	<-started
	sendCommand(t, conn, "unsubscribe")
	// 取消确认与新订阅应在旧快照继续构造期间完成。
	if reply := readMessage(t, conn); reply["type"] != "unsubscribed" {
		t.Fatalf("reply=%#v", reply)
	}
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	close(release)
	<-published
	service.Publish("new", time.Now(), func() any { return "new subscription" })
	if event := readMessage(t, conn); event["type"] != "new" {
		t.Fatalf("old snapshot leaked: %#v", event)
	}
}

func TestShutdownDrainsAndStopsSubscriptionProduction(t *testing.T) {
	service, connect := observationFixture(t, 5, 4096)
	conn := connect()
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	service.BeginShutdown()
	sendCommand(t, conn, "subscribe")
	if reply := readMessage(t, conn); reply["code"] != "shutting_down" {
		t.Fatalf("reply=%#v", reply)
	}
	service.Publish("completion", time.Now(), func() any { return "during business drain" })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- service.Shutdown(ctx) }()
	if event := readMessage(t, conn); event["type"] != "completion" {
		t.Fatalf("event=%#v", event)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	service.Publish("late", time.Now(), func() any { t.Error("snapshot after shutdown"); return nil })
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("connection survived shutdown")
	}
}

// 网络边界在握手后冻结服务端写入，Close 与真实连接一样解除阻塞。
type writeStall struct {
	enabled atomic.Bool
	started chan struct{}
	closed  chan struct{}
}
type stalledListener struct {
	net.Listener
	stall *writeStall
}

func (l stalledListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &stalledConn{Conn: conn, stall: l.stall, closed: make(chan struct{})}, nil
}

type stalledConn struct {
	net.Conn
	stall  *writeStall
	closed chan struct{}
	once   sync.Once
}

func (c *stalledConn) Write(data []byte) (int, error) {
	if c.stall.enabled.Load() {
		select {
		case c.stall.started <- struct{}{}:
		default:
		}
		<-c.closed
		return 0, net.ErrClosed
	}
	return c.Conn.Write(data)
}
func (c *stalledConn) Close() error {
	c.once.Do(func() {
		close(c.closed)
		select {
		case c.stall.closed <- struct{}{}:
		default:
		}
	})
	return c.Conn.Close()
}

func TestWriteTimeoutRetainsRemainingEventsForReconnect(t *testing.T) {
	stall := &writeStall{started: make(chan struct{}, 1), closed: make(chan struct{}, 1)}
	service, connect := observationFixture(t, 5, 4096, stall)
	conn := connect()
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	stall.enabled.Store(true)
	service.Publish("attempt-once", time.Now(), func() any { return "first" })
	select {
	case <-stall.started:
	case <-time.After(time.Second):
		t.Fatal("writer never entered transport")
	}
	for _, kind := range []string{"remaining-1", "remaining-2"} {
		service.Publish(kind, time.Now(), func() any { return kind })
	}
	select {
	case <-stall.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("write timeout failed to release connection")
	}
	stall.enabled.Store(false)
	replacement := connect()
	defer replacement.CloseNow()
	sendCommand(t, replacement, "subscribe")
	if reply := readMessage(t, replacement); reply["type"] != "subscribed" {
		t.Fatalf("confirmation=%#v", reply)
	}
	for index, kind := range []string{"remaining-1", "remaining-2"} {
		if event := readMessage(t, replacement); event["type"] != kind || event["seq"] != int64(index+2) {
			t.Fatalf("recovery=%#v", event)
		}
	}
}

func TestShutdownCancelsBlockedWriterWithinGlobalBudget(t *testing.T) {
	stall := &writeStall{started: make(chan struct{}, 1), closed: make(chan struct{}, 1)}
	service, connect := observationFixture(t, 5, 4096, stall)
	conn := connect()
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	stall.enabled.Store(true)
	service.Publish("blocked", time.Now(), func() any { return "payload" })
	<-stall.started
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if err := service.Shutdown(ctx); err != context.DeadlineExceeded {
		t.Fatalf("shutdown=%v", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown consumed extra write timeout: %s", elapsed)
	}
	select {
	case <-stall.closed:
	default:
		t.Fatal("transport survives shutdown")
	}
}
