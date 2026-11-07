package container

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/ugorji/go/codec"

	"gpt-load/internal/platform/config"
)

func observationLocalUpstream() *httptest.Server {
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"resp_local","object":"response","status":"completed","model":"gpt-5","output":[{"id":"msg_local","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"local answer","annotations":[]}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`+"\n\n")
	}))
}

func TestObservationReconnectRetainsNewestRequestsAndReplacement(t *testing.T) {
	upstream := observationLocalUpstream()
	defer upstream.Close()
	fixture := newObservationHost(t, upstream, func(cfg *config.Config) { cfg.Observation.QueueMaxEvents = 2 })
	first := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	fixture.command(t, first, "subscribe", "initial")
	boot := readObservation(t, first)["boot_id"]
	// 关闭握手完成后，业务请求全部发生于离线订阅期间。
	if err := first.Close(websocket.StatusNormalClosure, "offline"); err != nil {
		t.Fatal(err)
	}
	var lastID string
	for range 4 {
		lastID = fixture.request(t)
	}
	if err := fixture.lifecycle.WaitData(t.Context()); err != nil {
		t.Fatal(err)
	}
	second := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer second.CloseNow()
	fixture.command(t, second, "subscribe", "resume")
	if reply := readObservation(t, second); reply["type"] != "subscribed" || reply["boot_id"] != boot {
		t.Fatalf("resume=%#v", reply)
	}
	var lastSeq int64
	var finished map[string]any
	for range 2 {
		event := readObservation(t, second)
		if event["seq"].(int64) <= lastSeq {
			t.Fatalf("order=%#v", event)
		}
		lastSeq = event["seq"].(int64)
		if event["type"] == "request.finished" {
			finished = event["data"].(map[string]any)
		}
	}
	if finished == nil || finished["request_id"] != lastID {
		t.Fatalf("newest finished=%#v", finished)
	}
	// 失败接入保留有效连接。
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, strings.Replace(fixture.server.URL, "https://", "wss://", 1)+"/api/plugins/research/events", &websocket.DialOptions{HTTPClient: fixture.server.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer wrong"}}, Subprotocols: []string{observationSubprotocol}})
	if err == nil || response == nil || response.StatusCode != 401 {
		t.Fatalf("rejected admission=%v %+v", err, response)
	}
	fixture.command(t, second, "subscribe", "still-current")
	if reply := readObservation(t, second); reply["command_id"] != "still-current" {
		t.Fatalf("existing connection=%#v", reply)
	}
	third := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer third.CloseNow()
	// 旧代次的迟到取消和清理保持身份所属订阅。
	var late []byte
	if err := codec.NewEncoderBytes(&late, &codec.MsgpackHandle{}).Encode(map[string]string{"type": "unsubscribe", "command_id": "late"}); err != nil {
		t.Fatal(err)
	}
	_ = second.Write(ctx, websocket.MessageBinary, late)
	second.CloseNow()
	lastID = fixture.request(t)
	if err := fixture.lifecycle.WaitData(t.Context()); err != nil {
		t.Fatal(err)
	}
	fixture.command(t, third, "subscribe", "replacement")
	if reply := readObservation(t, third); reply["type"] != "subscribed" {
		t.Fatalf("replacement=%#v", reply)
	}
	for range 2 {
		event := readObservation(t, third)
		if event["type"] == "request.finished" && event["data"].(map[string]any)["request_id"] == lastID {
			return
		}
	}
	t.Fatal("replacement lost subscription")
}

func TestObservationShutdownSharesBusinessDeadline(t *testing.T) {
	upstream := observationLocalUpstream()
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	conn := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer conn.CloseNow()
	fixture.command(t, conn, "subscribe", "begin")
	readObservation(t, conn)
	fixture.observations.BeginShutdown()
	fixture.command(t, conn, "subscribe", "late")
	if reply := readObservation(t, conn); reply["code"] != "shutting_down" {
		t.Fatalf("late subscribe=%#v", reply)
	}
	requestID := fixture.request(t)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	fixture.lifecycle.BeginShutdown()
	if err := fixture.lifecycle.WaitData(ctx); err != nil {
		t.Fatal(err)
	}
	go func() { done <- fixture.observations.Shutdown(ctx) }()
	for {
		event := readObservation(t, conn)
		if event["type"] == "request.finished" {
			if event["data"].(map[string]any)["request_id"] != requestID {
				t.Fatal(fmt.Sprint(event))
			}
			break
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if _, _, err := conn.Read(ctx); err == nil {
		t.Fatal("connection survived shutdown")
	}
}
