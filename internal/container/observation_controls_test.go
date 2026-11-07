package container

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func TestObservationAuthenticationAndControls(t *testing.T) {
	upstream := httptest.NewTLSServer(http.NotFoundHandler())
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	for _, test := range []struct {
		id, token, version string
		status             int
	}{
		{"research", "wrong", observationSubprotocol, 401},
		{"unknown", "plugin-test-token", observationSubprotocol, 401},
		{"research", "plugin-test-token", "gpt-load.observation.v2.msgpack", 400},
	} {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		conn, response, err := websocket.Dial(ctx, strings.Replace(fixture.server.URL, "https://", "wss://", 1)+"/api/plugins/"+test.id+"/events", &websocket.DialOptions{HTTPClient: fixture.server.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer " + test.token}}, Subprotocols: []string{test.version}})
		cancel()
		if conn != nil {
			conn.CloseNow()
		}
		if err == nil || response == nil || response.StatusCode != test.status {
			t.Fatalf("handshake=%v response=%+v, want %d", err, response, test.status)
		}
	}
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, fixture.server.URL+"/api/plugins/research/events", nil)
	request.Header.Set("Authorization", "Bearer plugin-test-token")
	request.Header.Set("Sec-WebSocket-Protocol", observationSubprotocol)
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatalf("invalid upgrade status=%d", response.StatusCode)
	}
	request, _ = http.NewRequestWithContext(t.Context(), http.MethodGet, fixture.server.URL+"/api/groups", nil)
	request.Header.Set("Authorization", "Bearer plugin-test-token")
	response, err = fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 401 {
		t.Fatalf("management authentication status=%d", response.StatusCode)
	}
	conn := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer conn.CloseNow()
	for _, test := range []struct{ kind, id, want, code string }{
		{"subscribe", "", "error", "invalid_message"},
		{"other", "unknown", "error", "unknown_command"},
		{"error", "pseudo", "error", "unknown_command"},
		{"subscribe", "start", "subscribed", ""},
		{"subscribe", "again", "subscribed", ""},
		{"unsubscribe", "stop", "unsubscribed", ""},
		{"unsubscribe", "again", "unsubscribed", ""},
		{"subscribe", "restart", "subscribed", ""},
	} {
		fixture.command(t, conn, test.kind, test.id)
		reply := readObservation(t, conn)
		messages := map[string]string{"invalid_message": "type 和 command_id 为必填字段", "unknown_command": "未知控制命令"}
		if test.code != "" && reply["message"] != messages[test.code] {
			t.Fatalf("localized control=%#v", reply)
		}
		if reply["type"] != test.want || test.code != "" && reply["code"] != test.code || test.id != "" && reply["command_id"] != test.id {
			t.Fatalf("control=%+v reply=%#v", test, reply)
		}
	}
	if err := conn.Write(t.Context(), websocket.MessageText, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if reply := readObservation(t, conn); reply["code"] != "unsupported_message" {
		t.Fatalf("text reply=%#v", reply)
	}
}

func TestObservationRejectsMalformedControlAndUpgrade(t *testing.T) {
	upstream := httptest.NewTLSServer(http.NotFoundHandler())
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodGet, fixture.server.URL+"/api/plugins/research/events", nil)
	request.Header.Set("Authorization", "Bearer plugin-test-token")
	request.Header.Set("Sec-WebSocket-Protocol", observationSubprotocol)
	request.Header.Set("Connection", "Upgrade")
	request.Header.Set("Upgrade", "websocket")
	request.Header.Set("Sec-WebSocket-Version", "12")
	request.Header.Set("Sec-WebSocket-Key", "AAAAAAAAAAAAAAAAAAAAAA==")
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatalf("invalid version upgrade status=%d", response.StatusCode)
	}
	conn := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer conn.CloseNow()
	if err := conn.Write(t.Context(), websocket.MessageBinary, []byte{0xc1}); err != nil {
		t.Fatal(err)
	}
	if reply := readObservation(t, conn); reply["code"] != "invalid_message" {
		t.Fatalf("malformed reply=%#v", reply)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, _, err = conn.Read(ctx)
	if websocket.CloseStatus(err) != websocket.StatusInvalidFramePayloadData {
		t.Fatalf("malformed control close=%v", err)
	}
}
