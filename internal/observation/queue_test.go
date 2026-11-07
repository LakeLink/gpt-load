package observation_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/ugorji/go/codec"

	"gpt-load/internal/observation"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/httproute"
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
			service := observation.New(&config.Config{Observation: config.ObservationConfig{Plugins: []config.ObservationPlugin{{ID: "local", Token: "test-token"}}, QueueMaxEvents: test.events, QueueMaxBytes: test.bytes, EventMaxBytes: 512, WriteTimeoutSeconds: 1}})
			engine := gin.New()
			routes, err := httproute.NewRegistry(service.HTTPModule())
			if err != nil {
				t.Fatal(err)
			}
			if err := routes.Bind(engine); err != nil {
				t.Fatal(err)
			}
			host := httptest.NewServer(engine)
			defer host.Close()
			connect := func() *websocket.Conn {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				conn, _, err := websocket.Dial(ctx, strings.Replace(host.URL, "http://", "ws://", 1)+"/api/plugins/local/events", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer test-token"}}, Subprotocols: []string{observation.Subprotocol}})
				if err != nil {
					t.Fatal(err)
				}
				return conn
			}
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
