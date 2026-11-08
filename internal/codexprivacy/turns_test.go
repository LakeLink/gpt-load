package codexprivacy

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestTurnFirstValueScopeAndGeneration(t *testing.T) {
	store := NewTurns()
	now := time.Unix(1000, 0)
	store.now = func() time.Time { return now }
	store.capacity = 2
	a := store.Acquire("account-a/target", "turn")
	var wg sync.WaitGroup
	for range 16 {
		wg.Go(func() { a.Capture("  opaque-server-value==  ") })
	}
	wg.Wait()
	a.Capture("later")
	if a.Value() != "  opaque-server-value==  " {
		t.Fatal("first value bytes changed")
	}
	if store.Acquire("account-a/target", "turn").Value() != a.Value() {
		t.Fatal("same logical turn lost value")
	}
	if store.Acquire("account-b/target", "turn").Value() != "" {
		t.Fatal("cross-account reuse")
	}
	now = now.Add(turnTTL + time.Second)
	newTurn := store.Acquire("account-a/target", "turn")
	a.Capture("late")
	if newTurn.Value() != "" || a.Value() != "" {
		t.Fatal("expired generation revived")
	}
	newTurn.Capture("new")
	store.Acquire("account-a/target", "other-turn")
	store.Acquire("account-a/other-target", "turn")
	newTurn.Capture("late-evicted")
	if newTurn.Value() != "" {
		t.Fatal("evicted lease revived")
	}
	if len(store.entries) != 2 {
		t.Fatal("store exceeds bound")
	}
}

func TestTurnMetadataCaptureAndReplayPreservesStream(t *testing.T) {
	store := NewTurns()
	turn := store.Acquire("owner", "turn")
	header := http.Header{"X-Codex-Turn-State": {"foreign"}}
	turn.PrepareHeaders(header)
	if header.Get("X-Codex-Turn-State") != "" {
		t.Fatal("foreign inbound state retained")
	}
	body, err := turn.PrepareMetadata([]byte(`{"input":"unchanged","client_metadata":{"x-codex-turn-state":"foreign","turn_id":"original"}}`))
	if err != nil || bytes.Contains(body, []byte("foreign")) || !bytes.Contains(body, []byte(`"turn_id":"original"`)) {
		t.Fatal("routing removal changed identifiers")
	}
	wire := "data: " + strings.Repeat("x", maxRoutingEvent+20) + "\r\n\r\n" +
		"data: {\"type\":\"codex.response.metadata\",\r\ndata: \"headers\":{\"x-codex-turn-state\":\"raw-token==\"}}\r\n\r\n" +
		"data: {\"type\":\"response.completed\"}\r\n\r\n"
	reader := turn.ObserveSSE(io.NopCloser(&singleByteReader{data: []byte(wire)}))
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != wire {
		t.Fatal("stream changed")
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	turn.PrepareHeaders(header)
	if header.Get("X-Codex-Turn-State") != "raw-token==" {
		t.Fatal("metadata first value missing after completion")
	}
	turn.ObserveHeaders(http.Header{"X-Codex-Turn-State": {"replacement"}})
	if turn.Value() != "raw-token==" {
		t.Fatal("first state replaced")
	}
	body, err = turn.PrepareMetadata(body)
	if err != nil || !bytes.Contains(body, []byte(`"x-codex-turn-state":"raw-token=="`)) {
		t.Fatal("state metadata replay missing")
	}
}

type singleByteReader struct{ data []byte }

func (reader *singleByteReader) Read(buffer []byte) (int, error) {
	if len(reader.data) == 0 {
		return 0, io.EOF
	}
	if len(buffer) == 0 {
		return 0, nil
	}
	buffer[0] = reader.data[0]
	reader.data = reader.data[1:]
	return 1, nil
}

func TestOriginalTurnScope(t *testing.T) {
	payload := []byte(`{"client_metadata":{"turn_id":"turn-one","x-codex-turn-metadata":"{\"turn_id\":\"turn-one\"}"}}`)
	first, err := OriginalTurn(payload, nil, "request-one")
	second, err2 := OriginalTurn(payload, nil, "request-two")
	if err != nil || err2 != nil || first != second {
		t.Fatal("logical turn follows request completion")
	}
	if _, err := OriginalTurn(payload, http.Header{"X-Codex-Turn-Id": {"other"}}, "request"); err == nil {
		t.Fatal("conflicting turn accepted")
	}
	first, _ = OriginalTurn([]byte(`{"client_metadata":{"turn_id":""}}`), nil, "request-one")
	second, _ = OriginalTurn([]byte(`{}`), nil, "request-two")
	if first == second {
		t.Fatal("unrelated requests share fallback scope")
	}
	first, _ = OriginalTurn([]byte(`{"client_metadata":{"thread_id":"thread-a","turn_id":"1"}}`), nil, "request")
	second, _ = OriginalTurn([]byte(`{"client_metadata":{"thread_id":"thread-b","turn_id":"1"}}`), nil, "request")
	if first == second {
		t.Fatal("opaque turn IDs crossed threads")
	}
	for _, alias := range []string{"Session-Id", "Session_id", "X-Session-Id", "X-Claude-Code-Session-Id"} {
		first, _ = OriginalTurn([]byte(`{"client_metadata":{"turn_id":"1"}}`), http.Header{alias: {"session-a"}}, "request")
		second, _ = OriginalTurn([]byte(`{"client_metadata":{"turn_id":"1"}}`), http.Header{alias: {"session-b"}}, "request")
		if first == second {
			t.Fatalf("turn scope ignored %s", alias)
		}
	}
}
