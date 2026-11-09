package container

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gpt-load/internal/platform/config"
)

func TestObservationLargeRequestAndResponseHeadersThroughWSS(t *testing.T) {
	const budget = 2000000
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("X-Large-Observation", strings.Repeat("header", 200000))
		io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"resp_large","object":"response","status":"completed","model":"gpt-5","output":[{"id":"msg_large","type":"message","role":"assistant","status":"completed","content":[{"type":"output_text","text":"large answer","annotations":[]}]}],"usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}}}`+"\n\n")
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream, func(cfg *config.Config) { cfg.Observation.QueueMaxBytes = budget })
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer plugin.CloseNow()
	fixture.command(t, plugin, "subscribe", "large")
	confirmation := readObservationWithoutHTTP(t, plugin)
	if confirmation["max_event_bytes"] != int64(budget) {
		t.Fatalf("confirmation=%#v", confirmation)
	}
	plugin.SetReadLimit(int64(confirmation["max_event_bytes"].(int64)))
	payload := `{"model":"gpt-5","input":"` + strings.Repeat("x", 1200000) + `"}`
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer client-test-key")
	request.Header.Set("Content-Type", "application/json")
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !strings.Contains(string(body), "large answer") {
		t.Fatalf("business response=%d %s", response.StatusCode, body)
	}
	requestID := response.Header.Get("X-GPTLoad-Request-ID")
	for index, want := range []string{"upstream.request", "oversized", "request.finished"} {
		event := readObservationWithoutHTTP(t, plugin)
		data := event["data"].(map[string]any)
		if event["type"] != want || event["boot_id"] != confirmation["boot_id"] || data["request_id"] != requestID {
			t.Fatalf("record=%#v", event)
		}
		switch index {
		case 0:
			if _, ok := data["headers"]; !ok {
				t.Fatal("normal request headers absent")
			}
		case 1:
			if _, ok := data["headers"]; ok || event["original_type"] != "upstream.response" || event["body_omitted"] != nil || data["status_code"] != int64(200) {
				t.Fatalf("oversized response=%#v", event)
			}
		case 2:
			if _, ok := data["request_body"]; ok || event["body_omitted"] != true || data["status"] != "success" || len(data["attempts"].([]any)) != 1 {
				t.Fatalf("finished=%#v", event)
			}
		}
	}
	if err := fixture.logs.Stop(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, err := fixture.logs.GetRequestBody(t.Context(), requestID)
	if err != nil {
		t.Fatal(err)
	}
	if persisted == nil || persisted.OriginalBytes != int64(len(payload)) || persisted.Body == nil {
		t.Fatalf("persisted body metadata=%#v", persisted)
	}
}
