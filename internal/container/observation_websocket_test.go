package container

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// CPA 自建 TLS dialer；独立进程在首次加载系统根证书前信任测试 CA。
func TestObservationWebsocketConnections(t *testing.T) {
	if os.Getenv("GPTLOAD_WS_OBSERVATION_HELPER") == "" {
		ca := httptest.NewTLSServer(http.NotFoundHandler())
		cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Certificate().Raw})
		ca.Close()
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, cert, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestObservationWebsocketConnections$", "-test.v", "-test.timeout=45s")
		cmd.Env = append(os.Environ(), "GPTLOAD_WS_OBSERVATION_HELPER=1", "SSL_CERT_FILE="+path)
		output, err := cmd.CombinedOutput()
		t.Log(string(output))
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	var connections atomic.Int32
	closed := make(chan struct{}, 4)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := connections.Add(1)
		w.Header()["X-WS-Raw"] = []string{"one", "two"}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		defer func() { closed <- struct{}{} }()
		for turn := 1; ; turn++ {
			_, body, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if !strings.Contains(string(body), `"type":"response.create"`) {
				t.Errorf("upstream frame=%s", body)
				return
			}
			response := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_%d_%d","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`, n, turn)
			if err := conn.Write(r.Context(), websocket.MessageText, []byte(response)); err != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "start")
	readObservation(t, plugin)
	ids := map[any]bool{}
	outcomes := map[any]map[string]any{}
	var previousConnection any
	for session := 0; session < 2; session++ {
		client := dialObservationResponses(t, fixture)
		var previousResponse string
		for turn := 0; turn < 2; turn++ {
			payload := `{"type":"response.create","model":"gpt-5","input":"hello"}`
			if turn > 0 {
				payload = fmt.Sprintf(`{"type":"response.create","model":"gpt-5","input":"hello","previous_response_id":%q}`, previousResponse)
			}
			if err := client.Write(t.Context(), websocket.MessageText, []byte(payload)); err != nil {
				t.Fatal(err)
			}
			_, body, err := client.Read(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(body), `"response.completed"`) {
				t.Fatalf("downstream=%s", body)
			}
			var terminal struct{ Response struct{ ID string } }
			if err := json.Unmarshal(body, &terminal); err != nil {
				t.Fatal(err)
			}
			previousResponse = terminal.Response.ID
			var events []map[string]any
			for {
				event := readObservation(t, plugin)
				events = append(events, event)
				if event["type"] == "request.finished" {
					break
				}
			}
			want := 3
			if turn == 0 {
				want = 5
			}
			if len(events) != want {
				t.Fatalf("turn %d events=%#v", turn, events)
			}
			final := events[len(events)-1]["data"].(map[string]any)
			id := final["request_id"]
			if ids[id] {
				t.Fatal("repeated request identity")
			}
			ids[id] = true
			outcomes[id] = final
			attempt := final["attempts"].([]any)[0].(map[string]any)
			connection := attempt["upstream_connection_id"]
			if connection == nil || connection == "" || attempt["attempt_id"] != id.(string)+":1" || attempt["credential_version"] != int64(1) {
				t.Fatalf("attempt=%#v", attempt)
			}
			if turn == 1 && connection != previousConnection {
				t.Fatal("reused connection identity changed")
			}
			if session == 1 && turn == 0 && connection == previousConnection {
				t.Fatal("rebuilt connection kept identity")
			}
			previousConnection = connection
			if session == 0 && turn == 0 {
				var sent, handshake map[string]any
				for _, event := range events {
					data := event["data"].(map[string]any)
					if event["type"] == "upstream.stream" && data["direction"] == "request" {
						sent = event
					}
					if event["type"] == "upstream.response" && data["phase"] == "websocket_handshake" {
						handshake = event
					}
				}
				if sent == nil || handshake == nil {
					t.Fatalf("missing send/handshake: %#v", events)
				}
				for _, sample := range []struct {
					name   string
					event  map[string]any
					fields []string
				}{
					{"websocket-handshake", handshake, []string{"phase", "status_code", "protocol", "stream", "headers"}},
					{"websocket-stream-request", sent, []string{"transport", "direction", "kind", "protocol", "message_type", "write_result"}},
					{"websocket-finished", events[4], []string{"protocol", "operation", "status", "stream", "request_body"}},
				} {
					encoded, err := os.ReadFile("../../testdata/observation/v2/" + sample.name + ".msgpack")
					if err != nil {
						t.Fatal(err)
					}
					expected := decodeObservation(t, encoded)["data"].(map[string]any)
					actual := sample.event["data"].(map[string]any)
					for _, field := range sample.fields {
						actualValue, expectedValue := actual[field], expected[field]
						if field == "headers" {
							expectedValue = expectedValue.(map[string]any)["X-Ws-Raw"]
							actualValue = actualValue.(map[string]any)["X-Ws-Raw"]
						}
						if !reflect.DeepEqual(actualValue, expectedValue) {
							t.Fatalf("sample %s field %s=%#v actual=%#v", sample.name, field, expected[field], actual[field])
						}
					}
				}
			}
			for _, event := range events[:len(events)-1] {
				data := event["data"].(map[string]any)
				phase := ""
				if event["type"] == "upstream.request" || event["type"] == "upstream.response" {
					phase = "websocket_handshake"
				}
				if data["request_id"] != id || data["attempt_id"] != attempt["attempt_id"] || data["upstream_connection_id"] != connection || (phase != "" && data["phase"] != phase) || data["identity_generation"] != attempt["identity_generation"] {
					t.Fatalf("process=%#v attempt=%#v", event, attempt)
				}
				if event["type"] == "upstream.response" && (data["status_code"] != int64(101) || !reflect.DeepEqual(data["headers"].(map[string]any)["X-Ws-Raw"], []any{"one", "two"})) {
					t.Fatalf("handshake=%#v", data)
				}
			}
		}
		client.CloseNow()
		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatal("upstream connection leaked")
		}
		ends := map[any]bool{}
		for len(ends) < 2 {
			event := readObservation(t, plugin)
			data := event["data"].(map[string]any)
			if event["type"] != "upstream.stream" || data["kind"] != "end" || data["upstream_connection_id"] != previousConnection || data["last_stream_sequence"] != int64(2) || ends[data["direction"]] {
				t.Fatalf("connection end=%#v", event)
			}
			ends[data["direction"]] = true
		}
	}
	rejected := dialObservationResponses(t, fixture)
	if err := rejected.Write(t.Context(), websocket.MessageText, []byte(`{"type":"response.create","stream_id":"lane","model":"gpt-5","input":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	_, body, err := rejected.Read(t.Context())
	if err != nil || !strings.Contains(string(body), `"type":"error"`) {
		t.Fatalf("stream_id response=%s err=%v", body, err)
	}
	rejected.CloseNow()
	fixture.command(t, plugin, "unsubscribe", "stop")
	if event := readObservation(t, plugin); event["type"] != "unsubscribed" {
		t.Fatalf("fictitious upstream event=%#v", event)
	}
	if connections.Load() != 2 {
		t.Fatalf("actual connections=%d", connections.Load())
	}
	if err := fixture.logs.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	for id := range ids {
		record, err := fixture.logs.Get(t.Context(), id.(string))
		if err != nil || record.FirstResponseMs == nil || len(record.Attempts) != 1 || *record.FirstResponseMs != outcomes[id]["first_response_ms"].(int64) || record.DurationMs != outcomes[id]["duration_ms"].(int64) || string(record.Status) != outcomes[id]["status"] {
			t.Fatalf("log=%+v err=%v", record, err)
		}
	}
	t.Run("failed handshake", testObservationWSRejectedHandshake)
	t.Run("concurrent", testObservationWSConcurrent)
}

func dialObservationResponses(t *testing.T, fixture *observationHost) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, strings.Replace(fixture.server.URL, "https://", "wss://", 1)+"/v1/responses", &websocket.DialOptions{HTTPClient: fixture.server.Client(), HTTPHeader: http.Header{"Authorization": {"Bearer client-test-key"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.CloseNow() })
	return conn
}

func testObservationWSRejectedHandshake(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header()["X-WS-Raw"] = []string{"first", "second"}
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(`{"error":{"type":"invalid_request_error","message":"local rejection"}}`))
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "start")
	readObservation(t, plugin)
	client := dialObservationResponses(t, fixture)
	if err := client.Write(t.Context(), websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5","input":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	_, body, err := client.Read(t.Context())
	if err != nil || !strings.Contains(string(body), `"type":"error"`) {
		t.Fatalf("downstream=%s err=%v", body, err)
	}
	request := readObservation(t, plugin)
	response := readObservation(t, plugin)
	final := readObservation(t, plugin)
	rd := request["data"].(map[string]any)
	sd := response["data"].(map[string]any)
	fd := final["data"].(map[string]any)
	attempt := fd["attempts"].([]any)[0].(map[string]any)
	if request["type"] != "upstream.request" || response["type"] != "upstream.response" || sd["phase"] != "websocket_handshake" || sd["status_code"] != int64(403) || !reflect.DeepEqual(sd["headers"].(map[string]any)["X-Ws-Raw"], []any{"first", "second"}) || rd["upstream_connection_id"] == nil || rd["upstream_connection_id"] != sd["upstream_connection_id"] || attempt["upstream_connection_id"] != sd["upstream_connection_id"] || sd["attempt_id"] != attempt["attempt_id"] || sd["request_id"] != fd["request_id"] || fd["status"] != "error" || calls.Load() != 1 {
		t.Fatalf("request=%#v response=%#v final=%#v", request, response, final)
	}
	client.CloseNow()
}
func testObservationWSConcurrent(t *testing.T) {
	const clients, turns = 8, 4
	ready := make(chan struct{}, clients)
	released := make(chan struct{})
	closed := make(chan struct{}, clients)
	var actual atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := actual.Add(1)
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		defer func() { closed <- struct{}{} }()
		for turn := 0; ; turn++ {
			_, _, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			if turn == 0 {
				ready <- struct{}{}
				<-released
			}
			body := fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp_load_%d_%d","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`, n, turn)
			if conn.Write(r.Context(), websocket.MessageText, []byte(body)) != nil {
				return
			}
		}
	}))
	defer upstream.Close()
	var releaseAll func()
	defer func() {
		if releaseAll != nil {
			releaseAll()
		}
	}()
	fixture := newObservationHost(t, upstream)
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	for _, subscribed := range []bool{false, true} {
		if subscribed {
			fixture.command(t, plugin, "subscribe", "start")
			readObservation(t, plugin)
		}
		releaseChannel := released
		releaseAll = sync.OnceFunc(func() { close(releaseChannel) })
		conns := make([]*websocket.Conn, clients)
		for i := range conns {
			conns[i] = dialObservationResponses(t, fixture)
		}
		runtime.GC()
		var before, peak, after runtime.MemStats
		runtime.ReadMemStats(&before)
		beforeG := runtime.NumGoroutine()
		finished := make(chan error, clients)
		started := time.Now()
		for i, conn := range conns {
			go func() {
				defer conn.CloseNow()
				for turn := 0; turn < turns; turn++ {
					payload := fmt.Sprintf(`{"type":"response.create","model":"gpt-5","input":"marker-%d"}`, i)
					if err := conn.Write(t.Context(), websocket.MessageText, []byte(payload)); err != nil {
						finished <- err
						return
					}
					_, body, err := conn.Read(t.Context())
					if err != nil {
						finished <- err
						return
					}
					if !strings.Contains(string(body), `"response.completed"`) {
						finished <- fmt.Errorf("downstream %s", body)
						return
					}
				}
				finished <- nil
			}()
		}
		for range clients {
			select {
			case <-ready:
			case <-time.After(5 * time.Second):
				t.Fatal("connections did not run concurrently")
			}
		}
		runtime.ReadMemStats(&peak)
		peakG := runtime.NumGoroutine()
		releaseAll()
		connectionByMarker := map[string]any{}
		finalIDs := map[any]bool{}
		requestIDs := map[any]map[string]any{}
		if subscribed {
			for len(finalIDs) < clients*turns {
				event := readObservation(t, plugin)
				data := event["data"].(map[string]any)
				id := data["request_id"]
				if event["type"] == "upstream.stream" && data["kind"] == "data" && data["direction"] == "request" {
					marker := ""
					for i := range clients {
						candidate := fmt.Sprintf("marker-%d", i)
						if strings.Contains(string(data["body"].([]byte)), candidate) {
							marker = candidate
							break
						}
					}
					if marker == "" {
						t.Fatalf("serialized request marker missing: %#v", data)
					}
					connection := data["upstream_connection_id"]
					if prior, ok := connectionByMarker[marker]; ok && prior != connection {
						t.Fatal("concurrent turn changed physical connection")
					}
					connectionByMarker[marker] = connection
					if _, ok := requestIDs[id]; ok {
						t.Fatal("duplicate logical request")
					}
					requestIDs[id] = data
				}
				if event["type"] == "request.finished" {
					fact, ok := requestIDs[id]
					attempt := data["attempts"].([]any)[0].(map[string]any)
					if !ok || finalIDs[id] || attempt["attempt_id"] != fact["attempt_id"] || attempt["upstream_connection_id"] != fact["upstream_connection_id"] || attempt["identity_generation"] != fact["identity_generation"] || data["status"] != "success" {
						t.Fatalf("concurrent final=%#v fact=%#v", data, fact)
					}
					finalIDs[id] = true
				}
			}
		}
		for range clients {
			if err := <-finished; err != nil {
				t.Fatal(err)
			}
		}
		elapsed := time.Since(started)
		for range clients {
			select {
			case <-closed:
			case <-time.After(5 * time.Second):
				t.Fatal("concurrent upstream leaked")
			}
		}
		if subscribed {
			if len(connectionByMarker) != clients {
				t.Fatalf("markers=%v", connectionByMarker)
			}
			unique := map[any]bool{}
			for _, id := range connectionByMarker {
				unique[id] = true
			}
			if len(unique) != clients {
				t.Fatal("independent clients shared connection identity")
			}
			fixture.command(t, plugin, "unsubscribe", "stop")
			for {
				reply := readObservation(t, plugin)
				if reply["type"] == "unsubscribed" {
					break
				}
				data := reply["data"].(map[string]any)
				if reply["type"] != "upstream.stream" || data["kind"] != "end" {
					t.Fatal(reply)
				}
			}
		}
		// 轮次返回可早于网关退出；等待连接协程完成后采样。
		deadline := time.Now().Add(5 * time.Second)
		for runtime.NumGoroutine() > beforeG && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		t.Logf("subscribed=%v clients=%d turns=%d elapsed=%s ns/turn=%d allocs/turn=%d bytes/turn=%d heap_before=%d heap_peak=%d heap_after=%d goroutines_before=%d peak=%d after=%d", subscribed, clients, clients*turns, elapsed, elapsed.Nanoseconds()/(clients*turns), (after.Mallocs-before.Mallocs)/(clients*turns), (after.TotalAlloc-before.TotalAlloc)/(clients*turns), before.HeapAlloc, peak.HeapAlloc, after.HeapAlloc, beforeG, peakG, runtime.NumGoroutine())
		if runtime.NumGoroutine() > beforeG {
			t.Fatal("websocket goroutines did not return after connection exit")
		}
		released = make(chan struct{})
	}
	if actual.Load() != clients*2 {
		t.Fatalf("physical connections=%d", actual.Load())
	}
}
