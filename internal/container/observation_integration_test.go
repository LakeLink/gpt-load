package container

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/ugorji/go/codec"
	"go.uber.org/dig"
	"gorm.io/gorm"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	"gpt-load/internal/platform/encryption"
	"gpt-load/internal/platform/i18n"
	"gpt-load/internal/requestlog"
	"gpt-load/internal/state"
	stateloader "gpt-load/internal/state/loader"
	"gpt-load/internal/storage"
	"gpt-load/internal/storage/models"
	"gpt-load/internal/subscription/providers/codex"
)

const observationSubprotocol = "gpt-load.observation.v1.msgpack"

func TestObservationCodexRequestFinished(t *testing.T) {
	upstreamCalls := make(chan http.Header, 4)
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upstreamCalls <- r.Header.Clone()
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header()["X-Research-Raw"] = []string{"first", "second"}
		io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"resp_local","object":"response","status":"completed","model":"gpt-5","output":[{"id":"msg_local","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"local answer","annotations":[]}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`+"\n\n")
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	subscribeBytes, err := os.ReadFile("../../testdata/observation/v1/subscribe.msgpack")
	if err != nil {
		t.Fatal(err)
	}
	if err := plugin.Write(t.Context(), websocket.MessageBinary, subscribeBytes); err != nil {
		t.Fatal(err)
	}
	subscribed := readObservation(t, plugin)
	if subscribed["type"] != "subscribed" || subscribed["command_id"] != "sample-command" || subscribed["boot_id"] == "" || subscribed["max_event_bytes"] != int64(4000000) {
		t.Fatalf("subscription = %#v", subscribed)
	}
	plugin.SetReadLimit(int64(subscribed["max_event_bytes"].(int64)))
	requestID := fixture.request(t)
	requestEvent := readObservation(t, plugin)
	responseEvent := readObservation(t, plugin)
	for index, process := range []map[string]any{requestEvent, responseEvent} {
		data := process["data"].(map[string]any)
		wantType := []string{"upstream.request", "upstream.response"}[index]
		if process["type"] != wantType || data["request_id"] != requestID || data["attempt_id"] != requestID+":1" || data["attempt_sequence"] != int64(1) || data["exchange_sequence"] != int64(1) || data["phase"] != "http_exchange" || data["stream"] != false || data["channel_id"] != "codex" || data["model"] != "gpt-5" || data["protocol"] != "openai-responses" || data["credential_id"] != int64(1) || data["credential_version"] != int64(1) || data["identity_generation"].(int64) == 0 {
			t.Fatalf("process event = %#v", process)
		}
		if _, hasBody := data["body"]; hasBody {
			t.Fatal("transport hook has no available body")
		}
	}
	responseData := responseEvent["data"].(map[string]any)
	if responseData["status_code"] != int64(200) || !reflect.DeepEqual(responseData["headers"].(map[string]any)["X-Research-Raw"], []any{"first", "second"}) {
		t.Fatalf("raw response = %#v", responseData)
	}
	event := readObservation(t, plugin)
	data := event["data"].(map[string]any)
	if event["type"] != "request.finished" || event["seq"] != int64(3) || event["boot_id"] != subscribed["boot_id"] || event["observed_at"].(int64) == 0 || data["request_id"] != requestID || data["status"] != "success" || data["status_code"] != int64(200) {
		t.Fatalf("event = %#v", event)
	}
	if !reflect.DeepEqual(data["request_body"], []byte(`{"model":"gpt-5","input":"hello"}`)) {
		t.Fatalf("request body=%#v", data["request_body"])
	}
	if _, present := data["first_response_ms"]; present {
		t.Fatal("unary observation unexpectedly reports first response timing")
	}
	sampleBytes, err := os.ReadFile("../../testdata/observation/v1/request-finished.msgpack")
	if err != nil {
		t.Fatal(err)
	}
	sample := decodeObservation(t, sampleBytes)["data"].(map[string]any)
	for _, field := range []string{"protocol", "operation", "client_model", "status", "status_code", "stream", "request_body"} {
		if !reflect.DeepEqual(data[field], sample[field]) {
			t.Fatalf("v1 sample field %s=%#v, actual=%#v", field, sample[field], data[field])
		}
	}
	sampleUsage := sample["usage"].(map[string]any)
	actualUsage := data["usage"].(map[string]any)
	for _, field := range []string{"state", "tokens", "pricing"} {
		if !reflect.DeepEqual(actualUsage[field], sampleUsage[field]) {
			t.Fatalf("v1 usage field %s=%#v, actual=%#v", field, sampleUsage[field], actualUsage[field])
		}
	}
	attempt := data["attempts"].([]any)[0].(map[string]any)
	if attempt["attempt_id"] != requestID+":1" || attempt["credential_version"] != int64(1) || attempt["identity_generation"] != requestEvent["data"].(map[string]any)["identity_generation"] {
		t.Fatalf("final identity = %#v", attempt)
	}
	if len(data["attempts"].([]any)) != 1 {
		t.Fatalf("request timing/attempts = %#v", data)
	}
	if err := fixture.logs.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, err := fixture.logs.Get(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if string(persisted.Status) != data["status"] || persisted.StatusCode != int(data["status_code"].(int64)) || persisted.DurationMs != int64(data["duration_ms"].(int64)) {
		t.Fatalf("log=%+v observation=%#v", persisted, data)
	}
	fixture.command(t, plugin, "unsubscribe", "stop")
	if reply := readObservation(t, plugin); reply["type"] != "unsubscribed" || reply["command_id"] != "stop" {
		t.Fatalf("unsubscribe = %#v", reply)
	}
	if headers := <-upstreamCalls; headers.Get("Authorization") != "Bearer local-access" || headers.Get("Chatgpt-Account-Id") != "local-account" {
		t.Fatalf("upstream headers = %#v", headers)
	}
}

type observationHost struct {
	deps   *dig.Container
	server *httptest.Server
	db     *gorm.DB
	logs   *requestlog.Service
}

func newObservationHost(t *testing.T, upstream *httptest.Server) *observationHost {
	t.Helper()
	t.Setenv("AUTH_KEY", "management-test-key")
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("DATABASE_DSN", ":memory:")
	t.Setenv("ENCRYPTION_KEY", "observation-test-encryption-key")
	t.Setenv("OBSERVATION_PLUGINS", `[{"id":"research","token":"plugin-test-token"}]`)
	if err := i18n.Init(); err != nil {
		t.Fatal(err)
	}
	deps, err := BuildContainer(func(c *dig.Container) error {
		return c.Decorate(func(original execution.Executor) execution.Executor {
			return observationTLSExecutor{original, upstream.Client().Transport}
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &observationHost{deps: deps}
	err = deps.Invoke(func(engine *gin.Engine, db *gorm.DB, keys encryption.Service, registry *state.CredentialRegistry, manager *state.Manager, logs *requestlog.Service) {
		if err := storage.AutoMigrate(db); err != nil {
			t.Fatal(err)
		}
		canonical, err := codex.MarshalCredential(codex.Credential{Type: codex.Provider, AccessToken: "local-access", RefreshToken: "local-refresh", AccountID: "local-account", Email: "local@example.invalid", Expire: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
		if err != nil {
			t.Fatal(err)
		}
		ciphertext, err := keys.Encrypt(string(canonical))
		if err != nil {
			t.Fatal(err)
		}
		group := models.Group{Name: "observation-codex", ChannelID: "codex", ConnectionType: models.ConnectionTypeSubscription, Params: models.JSON(`{}`), Models: models.JSON(`[]`), Overrides: models.JSON(`{}`), Enabled: true}
		if err := db.Create(&group).Error; err != nil {
			t.Fatal(err)
		}
		row := models.Credential{GroupID: group.ID, Data: ciphertext, Fingerprint: keys.Hash(string(canonical)), IdentityFingerprint: keys.Hash("local-account"), SecretVersion: 1, AuthState: models.CredentialAuthStateReady, Status: models.CredentialStatusActive}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		generation := stateloader.CredentialIdentityGeneration(row.IdentityFingerprint, "codex", "subscription", json.RawMessage(`{}`))
		if err := registry.ReplaceCredentials([]state.CredentialEntry{{ID: row.ID, GroupID: group.ID, Version: 1, IdentityGeneration: generation, Fingerprint: row.Fingerprint, Status: state.CredentialStatusActive, EncryptedValue: row.Data}}); err != nil {
			t.Fatal(err)
		}
		params, _ := json.Marshal(map[string]string{"base_url": upstream.URL})
		if _, err := manager.Publish(state.CompileInput{ChannelRegistry: channel.NewRegistry(), AccessKeys: []state.AccessKeyConfig{{ID: 1, Name: "local", KeyHash: keys.Hash("client-test-key"), Status: state.AccessKeyStatusActive, RecordRequestBody: true}}, Groups: []state.GroupConfig{{ID: group.ID, Name: group.Name, ChannelID: channel.Codex, ConnectionType: "subscription", Params: params, Models: []state.ModelConfig{{ID: "gpt-5"}}, Enabled: true}}, Credentials: []state.CredentialConfig{{ID: row.ID, GroupID: group.ID, Version: 1, IdentityGeneration: generation, Fingerprint: row.Fingerprint, Status: state.CredentialStatusActive}}}); err != nil {
			t.Fatal(err)
		}
		if err := logs.Start(); err != nil {
			t.Fatal(err)
		}
		fixture.db, fixture.logs = db, logs
		fixture.server = httptest.NewTLSServer(engine)
		t.Cleanup(func() {
			fixture.server.Close()
			_ = logs.Stop(context.Background())
			sqlDB, _ := db.DB()
			_ = sqlDB.Close()
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

type observationTLSExecutor struct {
	execution.Executor
	transport http.RoundTripper
}

func (e observationTLSExecutor) Execute(ctx context.Context, spec execution.AttemptSpec) execution.AttemptResult {
	spec.Proxy = outboundproxy.Effective{}
	return e.Executor.Execute(context.WithValue(ctx, "cliproxy.roundtripper", e.transport), spec)
}
func (e observationTLSExecutor) ExecuteStream(ctx context.Context, spec execution.AttemptSpec, sink execution.StreamSink) execution.StreamResult {
	spec.Proxy = outboundproxy.Effective{}
	return e.Executor.ExecuteStream(context.WithValue(ctx, "cliproxy.roundtripper", e.transport), spec, sink)
}
func (f *observationHost) connect(t *testing.T, id, token, subprotocol string) *websocket.Conn {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, strings.Replace(f.server.URL, "https://", "wss://", 1)+"/api/plugins/"+id+"/events", &websocket.DialOptions{HTTPClient: f.server.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}, Subprotocols: []string{subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	return conn
}
func (f *observationHost) request(t *testing.T) string {
	return f.requestStatus(t, http.StatusOK, "local answer")
}

func (f *observationHost) requestStatus(t *testing.T, wantStatus int, wantBody string) string {
	t.Helper()
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, f.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"hello"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer client-test-key")
	request.Header.Set("Content-Type", "application/json")
	response, err := f.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if response.StatusCode != wantStatus || !strings.Contains(string(body), wantBody) {
		t.Fatalf("response=%d %s", response.StatusCode, body)
	}
	return response.Header.Get("X-GPTLoad-Request-ID")
}
func (f *observationHost) command(t *testing.T, conn *websocket.Conn, kind, id string) {
	t.Helper()
	var body []byte
	err := codec.NewEncoderBytes(&body, &codec.MsgpackHandle{WriteExt: true}).Encode(map[string]string{"type": kind, "command_id": id})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageBinary, body); err != nil {
		t.Fatal(err)
	}
}
func readObservation(t *testing.T, conn *websocket.Conn) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	kind, body, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if kind != websocket.MessageBinary {
		t.Fatal("expected binary message")
	}
	return decodeObservation(t, body)
}
func decodeObservation(t *testing.T, body []byte) map[string]any {
	t.Helper()
	handle := &codec.MsgpackHandle{WriteExt: true}
	handle.SignedInteger = true
	handle.MapType = reflect.TypeOf(map[string]any{})
	var value map[string]any
	if err := codec.NewDecoderBytes(body, handle).Decode(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestObservationCodexFailureAndPublicationIsolation(t *testing.T) {
	for _, test := range []struct {
		name       string
		smallQueue bool
	}{{"failure result", false}, {"publication exceeds capacity", true}} {
		t.Run(test.name, func(t *testing.T) {
			if test.smallQueue {
				t.Setenv("OBSERVATION_QUEUE_MAX_BYTES", "128")
				t.Setenv("OBSERVATION_EVENT_MAX_BYTES", "64")
			}
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"error":{"type":"invalid_request_error","code":"invalid_request_error","message":"local rejection"}}`)
			}))
			defer upstream.Close()
			fixture := newObservationHost(t, upstream)
			conn := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
			defer conn.CloseNow()
			fixture.command(t, conn, "subscribe", "start")
			readObservation(t, conn)
			requestID := fixture.requestStatus(t, http.StatusBadRequest, "local rejection")
			var event map[string]any
			if !test.smallQueue {
				requestEvent := readObservation(t, conn)
				responseEvent := readObservation(t, conn)
				responseData := responseEvent["data"].(map[string]any)
				if requestEvent["type"] != "upstream.request" || responseEvent["type"] != "upstream.response" || responseData["status_code"] != int64(400) || responseData["phase"] != "http_exchange" {
					t.Fatalf("failed exchange = %#v", responseEvent)
				}
				event = readObservation(t, conn)
			}
			fixture.command(t, conn, "unsubscribe", "stop")
			if reply := readObservation(t, conn); reply["type"] != "unsubscribed" {
				t.Fatalf("reply=%#v", reply)
			}
			if err := fixture.logs.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			record, err := fixture.logs.Get(t.Context(), requestID)
			if err != nil {
				t.Fatal(err)
			}
			if record.Status != "error" || record.StatusCode != 400 || len(record.Attempts) != 1 {
				t.Fatalf("failure log=%+v", record)
			}
			if !test.smallQueue {
				data := event["data"].(map[string]any)
				if data["request_id"] != record.RequestID || data["status"] != "error" || data["status_code"] != int64(400) || data["error_code"] != record.ErrorCode || data["error_summary"] != record.ErrorSummary {
					t.Fatalf("failure observation=%#v log=%+v", data, record)
				}
			}
		})
	}
}
