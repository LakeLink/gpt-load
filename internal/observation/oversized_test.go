package observation_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
	"github.com/ugorji/go/codec"
	"gpt-load/internal/automodel"
	"gpt-load/internal/channel"
	"gpt-load/internal/requestaudit"
	"gpt-load/internal/telemetry"

	"gpt-load/internal/observation"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/httproute"
)

func TestEncodedThresholdIncludesEnvelopeAndPreservesSnapshot(t *testing.T) {
	payload := map[string]any{"request_id": "req", "body": []byte(strings.Repeat("x", 512)), "headers": http.Header{"X-Test": {"one", "two"}}, "stream": false, "status_code": 0}
	// 独立按协议编码参考信封，纳入固定长度的 boot_id、序号与时间。
	var reference []byte
	if err := codec.NewEncoderBytes(&reference, &codec.MsgpackHandle{WriteExt: true, PositiveIntUnsigned: true}).Encode(map[string]any{"type": "upstream.request", "boot_id": strings.Repeat("b", 36), "seq": uint64(1), "observed_at": int64(1700000000123), "data": payload}); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(map[int]string{-1: "above", 0: "exact", 1: "below"}[delta], func(t *testing.T) {
			service, connect := oversizedFixture(t, len(reference)+delta, 4096, "a", "b")
			clients := []*websocket.Conn{connect("a"), connect("b")}
			for _, conn := range clients {
				defer conn.CloseNow()
				sendCommand(t, conn, "subscribe")
				readMessage(t, conn)
			}
			calls := 0
			service.Publish("upstream.request", time.UnixMilli(1700000000123), func() any { calls++; return payload })
			for _, conn := range clients {
				event := readMessage(t, conn)
				data := event["data"].(map[string]any)
				if event["type"] != "upstream.request" || event["seq"] != int64(1) || event["observed_at"] != int64(1700000000123) || data["request_id"] != "req" || data["stream"] != false || data["status_code"] != int64(0) {
					t.Fatalf("identity/metadata=%#v", event)
				}
				if delta < 0 {
					if _, ok := data["body"]; ok || event["body_omitted"] != true {
						t.Fatalf("body degradation=%#v", event)
					}
				} else if !reflect.DeepEqual(data["body"], payload["body"]) {
					t.Fatalf("body=%#v", event)
				}
				if !reflect.DeepEqual(data["headers"], map[string]any{"X-Test": []any{"one", "two"}}) {
					t.Fatalf("headers=%#v", data)
				}
			}
			if calls != 1 || len(payload["body"].([]byte)) != 512 || len(payload["headers"].(http.Header)) != 1 {
				t.Fatalf("shared snapshot mutated or rebuilt: %#v calls=%d", payload, calls)
			}
		})
	}
}

func oversizedFixture(t *testing.T, threshold, budget int, ids ...string) (*observation.Service, func(string) *websocket.Conn) {
	t.Helper()
	cfg := config.ObservationConfig{QueueMaxEvents: 200, QueueMaxBytes: budget, EventMaxBytes: threshold, WriteTimeoutSeconds: 1}
	for _, id := range ids {
		cfg.Plugins = append(cfg.Plugins, config.ObservationPlugin{ID: id, Token: "test-token"})
	}
	service := observation.New(&config.Config{Observation: cfg})
	engine := gin.New()
	routes, err := httproute.NewRegistry(service.HTTPModule())
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.Bind(engine); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewTLSServer(engine)
	t.Cleanup(host.Close)
	return service, func(id string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.Dial(t.Context(), strings.Replace(host.URL, "https://", "wss://", 1)+"/api/plugins/"+id+"/events", &websocket.DialOptions{HTTPClient: host.Client(), HTTPHeader: http.Header{"Authorization": {"Bearer test-token"}}, Subprotocols: []string{observation.Subprotocol}})
		if err != nil {
			t.Fatal(err)
		}
		conn.SetReadLimit(int64(budget))
		return conn
	}
}

func TestOversizedKeepsMetadataAndOmitsBothTransportSides(t *testing.T) {
	for _, withBody := range []bool{false, true} {
		t.Run(map[bool]string{false: "absent bodies", true: "present bodies"}[withBody], func(t *testing.T) {
			service, connect := oversizedFixture(t, 512, 12000, "a")
			conn := connect("a")
			defer conn.CloseNow()
			sendCommand(t, conn, "subscribe")
			confirmation := readMessage(t, conn)
			// 元数据自身超过降级阈值，仍应在完整队列预算内原样交付。
			payload := map[string]any{"request_id": "join", "attempt_id": "join:2", "exchange_sequence": 2, "request_headers": http.Header{"Authorization": {strings.Repeat("sensitive-header", 500)}}, "response_headers": http.Header{"X-Long": {strings.Repeat("x", 5000)}}, "model": strings.Repeat("model", 200), "error_summary": strings.Repeat("error", 200), "first_response_ms": int64(0), "stream": false, "unknown": "future-state"}
			if withBody {
				payload["request_body"] = []byte{}
				payload["response_body"] = []byte("private-body")
			}
			service.Publish("upstream.response", time.UnixMilli(1700000000123), func() any { return payload })
			event := readMessage(t, conn)
			data := event["data"].(map[string]any)
			if event["type"] != "oversized" || event["original_type"] != "upstream.response" || event["boot_id"] != confirmation["boot_id"] || event["seq"] != int64(1) || event["observed_at"] != int64(1700000000123) {
				t.Fatalf("identity=%#v", event)
			}
			for _, key := range []string{"request_headers", "response_headers", "request_body", "response_body"} {
				if _, ok := data[key]; ok {
					t.Fatalf("transport field %s survived", key)
				}
			}
			for _, key := range []string{"request_id", "attempt_id", "model", "error_summary", "stream", "unknown"} {
				if !reflect.DeepEqual(data[key], payload[key]) {
					t.Fatalf("metadata %s changed", key)
				}
			}
			if data["first_response_ms"] != int64(0) || data["exchange_sequence"] != int64(2) {
				t.Fatalf("numeric metadata=%#v", data)
			}
			if event["body_omitted"] == true != withBody {
				t.Fatalf("omission marker=%#v", event)
			}
			if len(payload["request_headers"].(http.Header)) != 1 || withBody && payload["response_body"] == nil {
				t.Fatal("source snapshot modified")
			}
			service.Publish("after", time.Now(), func() any { return "one record per observation" })
			if next := readMessage(t, conn); next["type"] != "after" || next["seq"] != int64(2) {
				t.Fatalf("duplicate/degraded identity=%#v", next)
			}
		})
	}
}

func TestBodyAvailabilityAndLargeLegalClientRecord(t *testing.T) {
	service, connect := oversizedFixture(t, 1048576, 1500000, "a")
	conn := connect("a")
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	reply := readMessage(t, conn)
	if reply["max_event_bytes"] != int64(1500000) {
		t.Fatalf("budget=%#v", reply)
	}
	for _, test := range []struct {
		name string
		data map[string]any
	}{
		{"absent", map[string]any{"request_id": "absent"}},
		{"nil", map[string]any{"request_id": "nil", "body": []byte(nil)}},
		{"empty", map[string]any{"request_id": "empty", "body": []byte{}}},
		{"large", map[string]any{"request_id": "large", "body": []byte(strings.Repeat("x", 64<<10))}},
	} {
		service.Publish("upstream.request", time.Now(), func() any { return test.data })
		event := readMessage(t, conn)
		data := event["data"].(map[string]any)
		if event["body_omitted"] != nil || event["type"] != "upstream.request" {
			t.Fatalf("unexpected degradation=%#v", event)
		}
		want, present := test.data["body"]
		got, exists := data["body"]
		if present != exists || want != nil && !reflect.DeepEqual(want, got) && !(test.name == "nil" && got == nil) {
			t.Fatalf("availability %s=%#v", test.name, got)
		}
	}
}

func TestPublicationFailuresLeaveSequenceGapsAndSafeDiagnostics(t *testing.T) {
	logger := logrus.StandardLogger()
	oldOutput, oldHooks := logger.Out, logger.Hooks
	logger.SetOutput(io.Discard)
	logger.ReplaceHooks(make(logrus.LevelHooks))
	hook := logtest.NewGlobal()
	defer func() { logger.SetOutput(oldOutput); logger.ReplaceHooks(oldHooks) }()
	service, connect := oversizedFixture(t, 256, 2048, "a")
	conn := connect("a")
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	confirmation := readMessage(t, conn)
	service.Publish("request.finished", time.UnixMilli(1700000000123), func() any {
		return map[string]any{"model": strings.Repeat("secret-model", 1000), "headers": http.Header{"Authorization": {"private-token"}}, "body": []byte("private-body")}
	})
	service.Publish("invalid", time.UnixMilli(1700000000123), func() any { return complex(1, 2) })
	service.Publish("valid", time.Now(), func() any { return "after errors" })
	if event := readMessage(t, conn); event["seq"] != int64(3) || event["type"] != "valid" {
		t.Fatalf("sequence gap=%#v", event)
	}
	entries := hook.AllEntries()
	if len(entries) != 2 {
		t.Fatalf("diagnostics=%d", len(entries))
	}
	for i, reason := range []string{"encoded_record_exceeds_queue_budget", "messagepack_encoding_failed"} {
		fields := entries[i].Data
		if fields["reason"] != reason || fields["boot_id"] != confirmation["boot_id"] || fields["plugin_id"] != "a" || fields["seq"] != uint64(i+1) || fields["observed_at"] != int64(1700000000123) {
			t.Fatalf("diagnostic=%#v", fields)
		}
		line, err := entries[i].String()
		if err != nil {
			t.Fatal(err)
		}
		for _, secret := range []string{"private-token", "private-body", "secret-model"} {
			if strings.Contains(line, secret) {
				t.Fatalf("raw data in diagnostic: %s", line)
			}
		}
	}
	if entries[0].Data["bytes"].(int) <= 2048 || entries[0].Data["original_bytes"].(int) <= entries[0].Data["bytes"].(int) {
		t.Fatalf("diagnostic sizes=%#v", entries[0].Data)
	}
}

func TestFinishedProjectionRetainsLargeMetadataAndExcludesHostInternals(t *testing.T) {
	service, connect := oversizedFixture(t, 512, 50000, "a")
	conn := connect("a")
	defer conn.CloseNow()
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	zero := int64(0)
	event := telemetry.RequestEvent{RequestID: "many-attempts", CompletedAt: time.UnixMilli(1700000000123), ClientModel: strings.Repeat("model", 100), ErrorSummary: strings.Repeat("error", 100), FirstResponseMs: &zero, RequestBody: []byte(strings.Repeat("body", 1000)), RequestAudit: &requestaudit.Result{}, AutoDecision: &automodel.Decision{}}
	event.Usage.Pricing.ReceiptJSON = strings.Repeat("private-pricing-debug", 10000)
	for i := 1; i <= 8; i++ {
		event.Attempts = append(event.Attempts, telemetry.Attempt{Sequence: i, AttemptID: "attempt", ChannelID: channel.Codex, UpstreamModel: strings.Repeat("upstream-model", 100), ErrorSummary: strings.Repeat("long-error", 100)})
	}
	sink := observation.RequestLogSink{Logs: telemetry.NoopRequestLogSink{}, Observations: service}
	sink.Emit(event)
	record := readMessage(t, conn)
	data := record["data"].(map[string]any)
	if record["type"] != "oversized" || record["original_type"] != "request.finished" || record["body_omitted"] != true || data["client_model"] != event.ClientModel || data["error_summary"] != event.ErrorSummary || data["first_response_ms"] != int64(0) {
		t.Fatalf("finished metadata=%#v", record)
	}
	attempts := data["attempts"].([]any)
	if len(attempts) != 8 || attempts[7].(map[string]any)["error_summary"] != event.Attempts[7].ErrorSummary {
		t.Fatal("attempt metadata lost")
	}
	for _, field := range []string{"request_body", "request_audit", "auto_decision"} {
		if _, ok := data[field]; ok {
			t.Fatalf("field %s survived", field)
		}
	}
	if _, ok := data["usage"].(map[string]any)["pricing"].(map[string]any)["receipt_json"]; ok {
		t.Fatal("pricing receipt exposed")
	}
}

func TestQueueUsesFinalEncodedProjection(t *testing.T) {
	service, connect := oversizedFixture(t, 512, 2000, "a")
	conn := connect("a")
	sendCommand(t, conn, "subscribe")
	readMessage(t, conn)
	// 完成接替握手后暂缓 subscribe，令整个队列保持离线投递状态。
	offline := connect("a")
	defer offline.CloseNow()
	sendCommand(t, offline, "unknown")
	readMessage(t, offline)
	conn.CloseNow()
	payload := map[string]any{"request_id": "small-after-omission", "body": []byte(strings.Repeat("x", 64<<10))}
	for range 30 {
		service.Publish("upstream.request", time.UnixMilli(1700000000123), func() any { return payload })
	}
	sendCommand(t, offline, "subscribe")
	readMessage(t, offline)
	records, total, first, lastSize := 0, 0, int64(0), 0
	for {
		_, raw, err := offline.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		h := &codec.MsgpackHandle{}
		h.SignedInteger = true
		var event map[string]any
		if err := codec.NewDecoderBytes(raw, h).Decode(&event); err != nil {
			t.Fatal(err)
		}
		records++
		total += len(raw)
		lastSize = len(raw)
		if first == 0 {
			first = event["seq"].(int64)
		}
		if event["seq"] == int64(30) {
			break
		}
	}
	if total > 2000 || records != 2000/lastSize || first != int64(31-records) {
		t.Fatalf("queue records=%d bytes=%d first=%d size=%d", records, total, first, lastSize)
	}
}

// 显式运行资源测量；通过实际 WSS 订阅保留和回读队列。
func TestOversizedEncodingResourceMeasurement(t *testing.T) {
	if os.Getenv("OBSERVATION_MEASURE_ENCODING") != "1" {
		t.Skip("set OBSERVATION_MEASURE_ENCODING=1 for encoding resource measurements")
	}
	for _, profile := range []string{"body", "headers", "metadata"} {
		t.Run(profile, func(t *testing.T) {
			service, connect := oversizedFixture(t, 1048576, 4000000, "a", "b")
			for _, id := range []string{"a", "b"} {
				conn := connect(id)
				sendCommand(t, conn, "subscribe")
				readMessage(t, conn)
				conn.Close(websocket.StatusNormalClosure, "offline")
			}
			runtime.GC()
			var baseline, before, after, retained runtime.MemStats
			runtime.ReadMemStats(&baseline)
			var elapsed time.Duration
			func() {
				payload := map[string]any{"request_id": "measurement", "stream": false}
				switch profile {
				case "body":
					payload["body"] = []byte(strings.Repeat("x", 8<<20))
				case "headers":
					payload["headers"] = http.Header{"X-Large": {strings.Repeat("x", 8<<20)}}
				case "metadata":
					payload["model"] = strings.Repeat("m", 1200000)
				}
				runtime.GC()
				runtime.ReadMemStats(&before)
				start := time.Now()
				for range 128 {
					service.Publish("upstream.request", time.UnixMilli(1700000000123), func() any { return payload })
				}
				elapsed = time.Since(start)
				runtime.ReadMemStats(&after)
			}()
			runtime.GC()
			runtime.ReadMemStats(&retained)
			count, total := 0, 0
			for _, id := range []string{"a", "b"} {
				conn := connect(id)
				defer conn.CloseNow()
				sendCommand(t, conn, "subscribe")
				readMessage(t, conn)
				localBytes := 0
				for {
					_, raw, err := conn.Read(t.Context())
					if err != nil {
						t.Fatal(err)
					}
					var event map[string]any
					h := &codec.MsgpackHandle{}
					h.SignedInteger = true
					if err := codec.NewDecoderBytes(raw, h).Decode(&event); err != nil {
						t.Fatal(err)
					}
					count++
					total += len(raw)
					localBytes += len(raw)
					if event["seq"] == int64(128) {
						break
					}
				}
				if localBytes > 4000000 {
					t.Fatalf("retained queue=%d", localBytes)
				}
			}
			t.Logf("ENCODE profile=%s plugins=2 observations=128 ns_per_observation=%d alloc_B_per_observation=%d allocs_per_observation=%d source_live_B=%d post_publish_heap_B=%d post_gc_live_B=%d retained_heap_delta_B=%d retained_records=%d retained_encoded_B=%d", profile, elapsed.Nanoseconds()/128, (after.TotalAlloc-before.TotalAlloc)/128, (after.Mallocs-before.Mallocs)/128, before.HeapAlloc, after.HeapAlloc, retained.HeapAlloc, int64(retained.HeapAlloc)-int64(baseline.HeapAlloc), count, total)
		})
	}
}

func TestSharedProtocolSamplesPreservePrimitiveAndOptionalValues(t *testing.T) {
	for _, sample := range []struct {
		name, kind  string
		bodyOmitted bool
	}{
		{"upstream-request", "upstream.request", false}, {"upstream-response", "upstream.response", false}, {"sse-response", "upstream.response", false}, {"body-omitted", "upstream.request", true}, {"oversized", "oversized", true}, {"zero-values", "request.finished", false}, {"unknown-values", "request.finished", false},
	} {
		t.Run(sample.name, func(t *testing.T) {
			raw, err := os.ReadFile("../../testdata/observation/v1/" + sample.name + ".msgpack")
			if err != nil {
				t.Fatal(err)
			}
			h := &codec.MsgpackHandle{WriteExt: true}
			h.SignedInteger = true
			h.MapType = reflect.TypeFor[map[string]any]()
			var event map[string]any
			if err := codec.NewDecoderBytes(raw, h).Decode(&event); err != nil {
				t.Fatal(err)
			}
			if event["type"] != sample.kind || event["boot_id"] != "sample-boot" || event["seq"] != int64(1) || event["observed_at"] != int64(1700000000123) || (event["body_omitted"] == true) != sample.bodyOmitted {
				t.Fatalf("envelope=%#v", event)
			}
			data := event["data"].(map[string]any)
			switch sample.name {
			case "upstream-request":
				if !reflect.DeepEqual(data["body"], []byte(`{"model":"gpt-5","input":"hello"}`)) || !reflect.DeepEqual(data["headers"].(map[string]any)["X-Multivalue"], []any{"first", "second"}) {
					t.Fatalf("request primitives=%#v", data)
				}
			case "upstream-response":
				if !reflect.DeepEqual(data["body"], []byte(`{"id":"resp_sample"}`)) || data["status_code"] != int64(200) {
					t.Fatalf("response primitives=%#v", data)
				}
			case "sse-response":
				if _, ok := data["body"]; ok || data["phase"] != "sse_initialization" || data["stream"] != true {
					t.Fatalf("SSE availability=%#v", data)
				}
			case "body-omitted":
				if _, ok := data["body"]; ok || data["headers"] == nil {
					t.Fatalf("omitted body=%#v", data)
				}
			case "oversized":
				if _, ok := data["headers"]; ok || event["original_type"] != "upstream.response" || data["status_code"] != int64(200) || data["attempt_id"] != "sample-request:1" {
					t.Fatalf("oversized projection=%#v", event)
				}
			case "zero-values":
				body, ok := data["request_body"].([]byte)
				if !ok || body == nil || len(body) != 0 || data["first_response_ms"] != int64(0) || data["stream"] != false || len(data["attempts"].([]any)) != 0 || data["usage"].(map[string]any)["tokens"].(map[string]any)["output"] != int64(0) {
					t.Fatalf("zero/optional values=%#v", data)
				}
			case "unknown-values":
				if data["status"] != "future_outcome" || data["protocol"] != "future_protocol" || data["future_field"] != "preserved" || data["attempts"].([]any)[0].(map[string]any)["failure_category"] != "future_failure" {
					t.Fatalf("future values=%#v", data)
				}
			}
		})
	}
}

func TestFinalOversizedQueueBudgetIsInclusive(t *testing.T) {
	metadata := strings.Repeat("m", 512)
	var reference []byte
	err := codec.NewEncoderBytes(&reference, &codec.MsgpackHandle{WriteExt: true, PositiveIntUnsigned: true}).Encode(map[string]any{"type": "oversized", "original_type": "upstream.response", "boot_id": strings.Repeat("b", 36), "seq": uint64(1), "observed_at": int64(1700000000123), "body_omitted": true, "data": map[string]any{"model": metadata}})
	if err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int{-1, 0, 1} {
		t.Run(map[int]string{-1: "above budget", 0: "exact budget", 1: "below budget"}[delta], func(t *testing.T) {
			service, connect := oversizedFixture(t, 128, len(reference)+delta, "a")
			conn := connect("a")
			defer conn.CloseNow()
			sendCommand(t, conn, "subscribe")
			readMessage(t, conn)
			service.Publish("upstream.response", time.UnixMilli(1700000000123), func() any {
				return map[string]any{"model": metadata, "headers": http.Header{"X-Large": {strings.Repeat("h", 512)}}, "body": []byte(strings.Repeat("b", 512))}
			})
			if delta < 0 {
				service.Publish("after", time.Now(), func() any { return "continuing" })
			}
			event := readMessage(t, conn)
			if delta < 0 {
				if event["type"] != "after" || event["seq"] != int64(2) {
					t.Fatalf("hard budget gap=%#v", event)
				}
			} else if event["type"] != "oversized" || event["seq"] != int64(1) || event["data"].(map[string]any)["model"] != metadata {
				t.Fatalf("accepted record=%#v", event)
			}
		})
	}
}
