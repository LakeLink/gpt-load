package codexprivacy

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
)

func TestPreparePreservesIdentityRelationshipsAndOriginalRequest(t *testing.T) {
	const source = "019ae218-3695-7000-8000-123456789abc"
	identity, err := NewCache().Resolve("account-a")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := NewCache().Resolve("account-b")
	nested := `{"thread_id":"` + source + `","session_id":"` + strings.ToUpper(source) + `","window_id":"` + source + `:12","parent_turn_id":null,"turn_id":"","workspaces":{"root":"` + source + `"}}`
	metadata := map[string]any{"x-codex-turn-metadata": nested, "session_id": source, "thread_id": source, "root_turn_id": source, "parent_thread_id": source, "context_window_id": source, "forked_from_thread_id": source}
	m, _ := json.Marshal(metadata)
	payload := []byte(`{"input": [ {"content":"` + source + `"} ], "tools": [{"name":"` + source + `"}], "previous_response_id":"` + source + `", "prompt_cache_key":"guardian:` + source + `","client_metadata":` + string(m) + `}`)
	original := bytes.Clone(payload)
	headers := http.Header{"Session-Id": {source}, "X-Client-Request-Id": {source}, "X-Codex-Turn-Metadata": {nested}, "X-Codex-Turn-State": {"server-token"}}
	body, out, err := identity.Prepare(payload, headers)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatal(err)
	}
	var got map[string]string
	if err := json.Unmarshal(root["client_metadata"], &got); err != nil {
		t.Fatal(err)
	}
	mapped := got["thread_id"]
	if mapped == source || mapped == "" {
		t.Fatal("thread was not derived")
	}
	for _, field := range []string{"session_id", "root_turn_id", "parent_thread_id", "context_window_id", "forked_from_thread_id"} {
		if got[field] != mapped {
			t.Fatalf("%s lost UUID equality", field)
		}
	}
	var turn map[string]any
	if err := json.Unmarshal([]byte(got["x-codex-turn-metadata"]), &turn); err != nil {
		t.Fatal(err)
	}
	if turn["thread_id"] != mapped || turn["session_id"] != mapped || turn["window_id"] != mapped+":12" || turn["turn_id"] != "" || turn["parent_turn_id"] != nil {
		t.Fatalf("turn relationships: %v", turn)
	}
	if out.Get("Session-Id") != mapped || out.Get("X-Client-Request-Id") != mapped || out.Get("X-Codex-Turn-State") != "server-token" {
		t.Fatal("header relationships changed")
	}
	if string(root["prompt_cache_key"]) != `"guardian:`+mapped+`"` {
		t.Fatal("cache source prefix or identity changed")
	}
	for _, unchanged := range []string{`"input": [ {"content":"` + source + `"} ]`, `"tools": [{"name":"` + source + `"}]`, `"previous_response_id":"` + source + `"`} {
		if !bytes.Contains(body, []byte(unchanged)) {
			t.Fatalf("unrelated bytes changed: %s", unchanged)
		}
	}
	if !bytes.Equal(payload, original) || headers.Get("Session-Id") != source || headers.Get("X-Codex-Turn-Metadata") != nested {
		t.Fatal("source request mutated")
	}
	retry, retryHeaders, err := identity.Prepare(payload, headers)
	if err != nil || !bytes.Equal(retry, body) || retryHeaders.Get("Session-Id") != mapped {
		t.Fatal("same-account retry unstable")
	}
	_, switched, err := other.Prepare(payload, headers)
	if err != nil || switched.Get("Session-Id") == mapped {
		t.Fatal("cross-account namespace reused")
	}
}

func TestPrepareOpaqueIDsAndInvalidMetadata(t *testing.T) {
	identity, _ := NewCache().Resolve("account")
	_, headers, err := identity.Prepare(nil, http.Header{"Session-Id": {"opaque-id"}, "X-Client-Request-Id": {"opaque-id"}, "X-Session-Id": {"opaque-id"}, "X-Claude-Code-Session-Id": {"opaque-id"}})
	if err != nil || headers.Get("Session-Id") == "opaque-id" || headers.Get("Session-Id") != headers.Get("X-Client-Request-Id") || headers.Get("Session-Id") != headers.Get("X-Session-Id") || headers.Get("Session-Id") != headers.Get("X-Claude-Code-Session-Id") {
		t.Fatal("opaque mapping lost equality")
	}
	for _, body := range []string{`{"client_metadata":[]}`, `{"client_metadata":{"thread_id":42}}`, `{"client_metadata":{"x-codex-turn-metadata":"broken"}}`, `{"client_metadata":{"x-codex-turn-metadata":"{\"turn_id\":[]}"}}`, `{"prompt_cache_key":false}`, `{"client_metadata":{"thread_id":"one","thread_id":"two"}}`, `{"client_metadata":{},"client_metadata":{}}`} {
		if _, _, err := identity.Prepare([]byte(body), nil); err == nil {
			t.Fatalf("invalid metadata accepted: %s", body)
		}
	}
	if _, _, err := identity.Prepare(nil, http.Header{"X-Codex-Turn-Metadata": {"{"}}); err == nil {
		t.Fatal("invalid header metadata accepted")
	}
}
