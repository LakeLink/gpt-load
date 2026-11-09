package container

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
)

// Search exercises ordinary JSON HTTP through CPA, WSS, and the standalone recorder.
func TestObservationStandaloneHTTPRecorder(t *testing.T) {
	binaryPath := os.Getenv("RECORDER_BINARY")
	if binaryPath == "" {
		t.Skip("set RECORDER_BINARY to run standalone recorder acceptance")
	}
	responseBody := []byte(` {"id":"search_local","object":"search.result","output":[],"padding":"` + strings.Repeat("原始", 14_000) + `","usage":{"input_tokens":3,"output_tokens":2,"total_tokens":5}} `)
	errorBody := []byte(" {\"error\":{\"type\":\"server_error\",\"code\":\"server_is_overloaded\",\"message\":\"Our servers are currently overloaded. Please try again later.\"}}\n")
	type exchange struct {
		request, response []byte
	}
	received := make(chan exchange, 4)
	var calls int
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		calls++
		w.Header().Set("Content-Type", "application/json")
		switch calls {
		case 2:
			received <- exchange{body, errorBody}
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write(errorBody)
		case 4:
			received <- exchange{body, nil}
			w.WriteHeader(http.StatusBadRequest)
		case 3:
			received <- exchange{body, []byte(observationCompleted)}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, observationCompleted)
		default:
			received <- exchange{body, responseBody}
			w.Header().Set("Content-Encoding", "gzip")
			compressed := gzip.NewWriter(w)
			_, _ = compressed.Write(responseBody)
			_ = compressed.Close()
		}
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	configureObservationRetry(t, fixture, upstream.URL)
	output, stop := startStreamRecorder(t, binaryPath, fixture)
	var ids []string
	for index, status := range []int{http.StatusOK, http.StatusOK, http.StatusBadRequest} {
		payload := `{"id":"session-` + fmt.Sprint(index) + `","model":"gpt-5","input":[{"role":"user","content":"` + strings.Repeat("request ", 9_000) + `"}],"metadata":{"case":"` + fmt.Sprint(index) + `"}}`
		path := "/v1/alpha/search"
		want := "search_local"
		if index == 1 {
			path, want = "/v1/responses", "local answer"
		}
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+path, strings.NewReader(payload))
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
		if err != nil || response.StatusCode != status || (status == http.StatusOK && !bytes.Contains(body, []byte(want))) {
			t.Fatalf("search case=%d status=%d body=%s err=%v", index, response.StatusCode, body, err)
		}
		id := response.Header.Get("X-GPTLoad-Request-ID")
		ids = append(ids, id)
		waitRecordedRequest(t, output, id)
	}
	stop()
	decoded, err := exec.Command(binaryPath, "decode", output).Output()
	if err != nil {
		t.Fatal(err)
	}
	type streamKey struct {
		RequestID, AttemptID, Direction string
		Exchange                        uint64
	}
	type content struct {
		body     bytes.Buffer
		sequence uint64
		ended    bool
	}
	streams := map[streamKey]*content{}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	for {
		var event struct {
			Type        string `json:"type"`
			BodyOmitted bool   `json:"body_omitted"`
			Data        struct {
				RequestID string `json:"request_id"`
				AttemptID string `json:"attempt_id"`
				Attempt   uint64 `json:"attempt_sequence"`
				Exchange  uint64 `json:"exchange_sequence"`
				Transport string `json:"transport"`
				Direction string `json:"direction"`
				Kind      string `json:"kind"`
				Body      []byte `json:"body"`
				Sequence  uint64 `json:"stream_sequence"`
				Offset    uint64 `json:"byte_offset"`
				Size      uint64 `json:"body_size"`
				Last      uint64 `json:"last_stream_sequence"`
				Total     uint64 `json:"total_body_bytes"`
				Reason    string `json:"end_reason"`
			} `json:"data"`
		}
		if err := decoder.Decode(&event); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if event.Type != "upstream.stream" {
			continue
		}
		data := event.Data
		if event.BodyOmitted || data.Transport != "http" || data.AttemptID != fmt.Sprintf("%s:%d", data.RequestID, data.Attempt) || data.Exchange != 1 {
			t.Fatalf("HTTP stream identity=%+v", event)
		}
		key := streamKey{data.RequestID, data.AttemptID, data.Direction, data.Exchange}
		stream := streams[key]
		if stream == nil {
			stream = &content{}
			streams[key] = stream
		}
		if stream.ended {
			t.Fatalf("content after end=%+v", event)
		}
		switch data.Kind {
		case "data":
			stream.sequence++
			if data.Sequence != stream.sequence || data.Offset != uint64(stream.body.Len()) || data.Size != uint64(len(data.Body)) || len(data.Body) == 0 || len(data.Body) > 32*1024 {
				t.Fatalf("HTTP content continuity=%+v", event)
			}
			stream.body.Write(data.Body)
		case "end":
			stream.ended = true
			if data.Last != stream.sequence || data.Total != uint64(stream.body.Len()) || data.Reason != "eof" {
				t.Fatalf("HTTP end=%+v", event)
			}
		default:
			t.Fatalf("HTTP stream kind=%+v", event)
		}
	}
	if len(streams) != 8 || len(received) != 4 {
		t.Fatalf("HTTP exchanges: streams=%d received=%d", len(streams), len(received))
	}
	for _, identity := range []struct {
		request string
		attempt int
	}{{ids[0], 1}, {ids[1], 1}, {ids[1], 2}, {ids[2], 1}} {
		upstream := <-received
		for direction, want := range map[string][]byte{"request": upstream.request, "response": upstream.response} {
			key := streamKey{identity.request, fmt.Sprintf("%s:%d", identity.request, identity.attempt), direction, 1}
			stream := streams[key]
			if stream == nil || !stream.ended || !bytes.Equal(stream.body.Bytes(), want) {
				t.Fatalf("HTTP recorded bytes mismatch: key=%+v stream=%+v expected bytes=%d", key, stream, len(want))
			}
			if len(want) > 32*1024 && stream.sequence < 2 {
				t.Fatalf("large HTTP body lacks chunks: key=%+v chunks=%d", key, stream.sequence)
			}
		}
	}
	writeObservationArtifact(t, "http-events.jsonl", decoded)
	writeObservationArtifact(t, "http-decoded-response.json", responseBody)
	recording, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	writeObservationArtifact(t, "http-events.bin", recording)
}
