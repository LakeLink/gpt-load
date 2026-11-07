package container

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gpt-load/internal/channel"
	"gpt-load/internal/platform/encryption"
	"gpt-load/internal/state"
	stateloader "gpt-load/internal/state/loader"
	"gpt-load/internal/storage/models"
	"gpt-load/internal/subscription/providers/codex"
)

const observationCompleted = "data: " + `{"type":"response.completed","response":{"id":"resp_local","object":"response","status":"completed","model":"gpt-5","output":[{"id":"msg_local","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"local answer","annotations":[]}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}` + "\n\n"

func TestObservationSSEBeforeCompletion(t *testing.T) {
	for _, outcome := range []string{"success", "error", "incomplete", "canceled", "subscribe during request"} {
		t.Run(outcome, func(t *testing.T) {
			lateSubscription := outcome == "subscribe during request"
			if lateSubscription {
				outcome = "success"
			}
			upstreamStarted, releaseHeaders := make(chan struct{}), make(chan struct{})
			if !lateSubscription {
				close(releaseHeaders)
			}
			release := make(chan struct{})
			upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				close(upstreamStarted)
				select {
				case <-releaseHeaders:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				w.Header()["X-Research-Raw"] = []string{"first", "second"}
				io.WriteString(w, "data: "+`{"type":"response.output_text.delta","response_id":"resp_local","delta":"local "}`+"\n\n")
				w.(http.Flusher).Flush()
				if outcome == "canceled" {
					<-r.Context().Done()
					return
				}
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				switch outcome {
				case "success":
					io.WriteString(w, observationCompleted)
				case "error":
					io.WriteString(w, "data: "+`{"type":"error","error":{"type":"server_error","code":"server_error","message":"local stream failure"}}`+"\n\n")
				case "incomplete":
					io.WriteString(w, "data: "+`{"type":"response.incomplete","response":{"id":"resp_local","status":"incomplete","incomplete_details":{"reason":"max_output_tokens"},"output":[]}}`+"\n\n")
				}
			}))
			defer upstream.Close()
			fixture := newObservationHost(t, upstream)
			plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
			defer plugin.CloseNow()
			if !lateSubscription {
				fixture.command(t, plugin, "subscribe", "start")
				readObservation(t, plugin)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			request, _ := http.NewRequestWithContext(ctx, http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"hello","stream":true}`))
			request.Header.Set("Authorization", "Bearer client-test-key")
			request.Header.Set("Content-Type", "application/json")
			type result struct {
				id, body string
				err      error
			}
			finished := make(chan result, 1)
			firstOutput := make(chan struct{})
			go func() {
				response, err := fixture.server.Client().Do(request)
				if err != nil {
					finished <- result{err: err}
					return
				}
				defer response.Body.Close()
				buffer := make([]byte, 1024)
				n, firstErr := response.Body.Read(buffer)
				close(firstOutput)
				body, err := io.ReadAll(response.Body)
				body = append(buffer[:n], body...)
				if firstErr != nil {
					err = firstErr
				}
				finished <- result{response.Header.Get("X-GPTLoad-Request-ID"), string(body), err}
			}()
			var requestEvent map[string]any
			if lateSubscription {
				<-upstreamStarted
				fixture.command(t, plugin, "subscribe", "start")
				readObservation(t, plugin)
				close(releaseHeaders)
			} else {
				requestEvent = readObservation(t, plugin)
			}
			responseEvent := readObservation(t, plugin)
			responseData := responseEvent["data"].(map[string]any)
			if responseEvent["type"] != "upstream.response" || responseData["phase"] != "sse_initialization" || responseData["stream"] != true || responseData["status_code"] != int64(200) {
				t.Fatalf("stream response=%#v", responseEvent)
			}
			if !lateSubscription {
				requestData := requestEvent["data"].(map[string]any)
				if requestEvent["type"] != "upstream.request" || requestData["phase"] != "http_exchange" || requestData["attempt_id"] != responseData["attempt_id"] {
					t.Fatalf("stream request=%#v response=%#v", requestEvent, responseEvent)
				}
			}

			select {
			case result := <-finished:
				t.Fatalf("stream completed before release: %+v", result)
			default:
			}
			if outcome == "canceled" {
				<-firstOutput
				cancel()
			}
			close(release)
			resultValue := <-finished
			if outcome != "canceled" && (resultValue.err != nil || !strings.Contains(resultValue.body, "local ")) {
				t.Fatalf("stream result = %+v", resultValue)
			}
			final := readObservation(t, plugin)
			data := final["data"].(map[string]any)
			if final["type"] != "request.finished" || data["status"] != outcome || data["request_id"] != responseData["request_id"] || data["first_response_ms"] == nil {
				t.Fatalf("stream outcome = %#v", final)
			}
			if err := fixture.logs.Stop(t.Context()); err != nil {
				t.Fatal(err)
			}
			log, err := fixture.logs.Get(t.Context(), data["request_id"].(string))
			if err != nil {
				t.Fatal(err)
			}
			if log.FirstResponseMs == nil || *log.FirstResponseMs != data["first_response_ms"].(int64) || log.DurationMs != data["duration_ms"].(int64) || string(log.Status) != outcome {
				t.Fatalf("log=%+v event=%#v", log, data)
			}
			if outcome == "canceled" && resultValue.err == nil {
				t.Fatal("canceled client completed successfully")
			}
		})
	}
}

func TestObservationConcurrentExchangesKeepRequestIdentity(t *testing.T) {
	const count = 4
	release := make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		marker := ""
		for _, value := range []string{"marker-0", "marker-1", "marker-2", "marker-3"} {
			if strings.Contains(string(body), value) {
				marker = value
				break
			}
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header()["X-Exchange-Tag"] = []string{marker, "second"}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, observationCompleted)
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "start")
	readObservation(t, plugin)
	type result struct {
		id, marker string
		err        error
	}
	finished := make(chan result, count)
	for _, marker := range []string{"marker-0", "marker-1", "marker-2", "marker-3"} {
		go func() {
			request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"`+marker+`"}`))
			request.Header.Set("Authorization", "Bearer client-test-key")
			request.Header.Set("Content-Type", "application/json")
			response, err := fixture.server.Client().Do(request)
			if err != nil {
				finished <- result{marker: marker, err: err}
				return
			}
			_, err = io.Copy(io.Discard, response.Body)
			response.Body.Close()
			finished <- result{response.Header.Get("X-GPTLoad-Request-ID"), marker, err}
		}()
	}
	requests := make(map[string]bool)
	responses := make(map[string]string)
	for range count * 2 {
		event := readObservation(t, plugin)
		data := event["data"].(map[string]any)
		id := data["request_id"].(string)
		if data["attempt_id"] != id+":1" || data["exchange_sequence"] != int64(1) || data["attempt_sequence"] != int64(1) {
			t.Fatalf("identity=%#v", event)
		}
		switch event["type"] {
		case "upstream.request":
			requests[id] = true
		case "upstream.response":
			tag := data["headers"].(map[string]any)["X-Exchange-Tag"].([]any)
			responses[id] = tag[0].(string)
		default:
			t.Fatalf("unexpected pre-completion event=%#v", event)
		}
	}
	close(release)
	if len(requests) != count || len(responses) != count {
		t.Fatalf("requests=%#v responses=%#v", requests, responses)
	}
	for range count {
		event := readObservation(t, plugin)
		data := event["data"].(map[string]any)
		id := data["request_id"].(string)
		if event["type"] != "request.finished" || !requests[id] || responses[id] == "" || !strings.Contains(string(data["request_body"].([]byte)), responses[id]) {
			t.Fatalf("cross-request association=%#v response=%#v", event, responses)
		}
	}
	for range count {
		result := <-finished
		if result.err != nil || responses[result.id] != result.marker {
			t.Fatalf("client correlation=%+v responses=%#v", result, responses)
		}
	}
}

func TestObservationGatewayRetryCreatesNewAttempt(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Account-Observed", r.Header.Get("Chatgpt-Account-Id"))
		if calls.Add(1) == 1 {
			io.WriteString(w, "data: "+`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`+"\n\n")
			return
		}
		io.WriteString(w, observationCompleted)
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	err := fixture.deps.Invoke(func(registry *state.CredentialRegistry, manager *state.Manager, keys encryption.Service) {
		var first models.Credential
		if err := fixture.db.First(&first).Error; err != nil {
			t.Fatal(err)
		}
		backupGroup := models.Group{Name: "observation-backup", ChannelID: "codex", ConnectionType: models.ConnectionTypeSubscription, Params: models.JSON(`{}`), Models: models.JSON(`[]`), Overrides: models.JSON(`{}`), Enabled: true}
		if err := fixture.db.Create(&backupGroup).Error; err != nil {
			t.Fatal(err)
		}
		canonical, err := codex.MarshalCredential(codex.Credential{Type: codex.Provider, AccessToken: "backup-access", RefreshToken: "backup-refresh", AccountID: "backup-account", Expire: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
		if err != nil {
			t.Fatal(err)
		}
		ciphertext, err := keys.Encrypt(string(canonical))
		if err != nil {
			t.Fatal(err)
		}
		second := models.Credential{GroupID: backupGroup.ID, Data: ciphertext, Fingerprint: keys.Hash(string(canonical)), IdentityFingerprint: keys.Hash("backup-account"), SecretVersion: 2, AuthState: models.CredentialAuthStateReady, Status: models.CredentialStatusActive}
		if err := fixture.db.Create(&second).Error; err != nil {
			t.Fatal(err)
		}
		params, _ := json.Marshal(map[string]string{"base_url": upstream.URL})
		input := state.CompileInput{ChannelRegistry: channel.NewRegistry(), AccessKeys: []state.AccessKeyConfig{{ID: 1, Name: "local", KeyHash: keys.Hash("client-test-key"), Status: state.AccessKeyStatusActive, RecordRequestBody: true}}}
		var entries []state.CredentialEntry
		for index, row := range []models.Credential{first, second} {
			generation := stateloader.CredentialIdentityGeneration(row.IdentityFingerprint, "codex", "subscription", json.RawMessage(`{}`))
			input.Groups = append(input.Groups, state.GroupConfig{ID: row.GroupID, Name: []string{"primary", "backup"}[index], ChannelID: channel.Codex, Priority: int32(100 - index*100), ConnectionType: "subscription", Params: params, Models: []state.ModelConfig{{ID: "gpt-5"}}, Enabled: true})
			input.Credentials = append(input.Credentials, state.CredentialConfig{ID: row.ID, GroupID: row.GroupID, Version: row.SecretVersion, IdentityGeneration: generation, Fingerprint: row.Fingerprint, Status: state.CredentialStatusActive})
			entries = append(entries, state.CredentialEntry{ID: row.ID, GroupID: row.GroupID, Version: row.SecretVersion, IdentityGeneration: generation, Fingerprint: row.Fingerprint, Status: state.CredentialStatusActive, EncryptedValue: row.Data})
		}
		if err := registry.ReplaceCredentials(entries); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Publish(input); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "start")
	readObservation(t, plugin)
	requestID := fixture.request(t)
	var processData []map[string]any
	for range 4 {
		event := readObservation(t, plugin)
		data := event["data"].(map[string]any)
		sequence := int64(len(processData)/2 + 1)
		if data["request_id"] != requestID || data["attempt_sequence"] != sequence || data["exchange_sequence"] != int64(1) || data["credential_id"] != sequence || data["credential_version"] != sequence {
			t.Fatalf("retry process=%#v", event)
		}
		processData = append(processData, data)
	}
	if processData[0]["attempt_id"] == processData[2]["attempt_id"] || processData[1]["headers"].(map[string]any)["X-Account-Observed"].([]any)[0] != "local-account" || processData[3]["headers"].(map[string]any)["X-Account-Observed"].([]any)[0] != "backup-account" {
		t.Fatalf("retry correlation=%#v", processData)
	}
	final := readObservation(t, plugin)
	data := final["data"].(map[string]any)
	attempts := data["attempts"].([]any)
	if final["type"] != "request.finished" || data["status"] != "success" || len(attempts) != 2 || calls.Load() != 2 {
		t.Fatalf("retry outcome=%#v calls=%d", final, calls.Load())
	}
	for index, value := range attempts {
		attempt := value.(map[string]any)
		if attempt["attempt_id"] != processData[index*2]["attempt_id"] || attempt["identity_generation"] != processData[index*2]["identity_generation"] || attempt["credential_version"] != processData[index*2]["credential_version"] {
			t.Fatalf("attempt identity=%#v process=%#v", attempt, processData[index*2])
		}
	}
	if err := fixture.logs.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	log, err := fixture.logs.Get(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if log.Status != "success" || len(log.Attempts) != 2 || !log.Attempts[0].WillRetry || log.DurationMs != data["duration_ms"].(int64) {
		t.Fatalf("retry log=%+v", log)
	}
}

func TestObservationTransportFailureHasFinalAttemptWithoutResponse(t *testing.T) {
	upstream := httptest.NewTLSServer(http.NotFoundHandler())
	fixture := newObservationHost(t, upstream)
	upstream.Close()
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "start")
	readObservation(t, plugin)
	id := fixture.requestStatus(t, http.StatusBadGateway, "")
	request := readObservation(t, plugin)
	final := readObservation(t, plugin)
	requestData := request["data"].(map[string]any)
	finalData := final["data"].(map[string]any)
	if request["type"] != "upstream.request" || final["type"] != "request.finished" || requestData["request_id"] != id || finalData["request_id"] != id || finalData["status"] != "error" || final["seq"] != int64(2) {
		t.Fatalf("request=%#v final=%#v", request, final)
	}
	attempt := finalData["attempts"].([]any)[0].(map[string]any)
	if attempt["attempt_id"] != requestData["attempt_id"] || attempt["dispatch_state"] != "not_sent" || attempt["response_started"] != false {
		t.Fatalf("transport evidence=%#v", attempt)
	}
}
