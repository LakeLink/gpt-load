package container

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

// HTTP request, SSE response, and WebSocket content cross the host and standalone recorder.
func TestObservationStandaloneStreamRecorder(t *testing.T) {
	binaryPath := os.Getenv("RECORDER_BINARY")
	if binaryPath == "" {
		t.Skip("set RECORDER_BINARY to run standalone recorder acceptance")
	}
	if os.Getenv("GPTLOAD_STREAM_RECORDER_HELPER") == "" {
		ca := httptest.NewTLSServer(http.NotFoundHandler())
		certificate := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.Certificate().Raw})
		ca.Close()
		path := filepath.Join(t.TempDir(), "ca.pem")
		if err := os.WriteFile(path, certificate, 0600); err != nil {
			t.Fatal(err)
		}
		cmd := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestObservationStandaloneStreamRecorder$", "-test.v", "-test.timeout=45s", "-test.artifacts", "-test.outputdir="+t.ArtifactDir())
		cmd.Env = append(os.Environ(), "GPTLOAD_STREAM_RECORDER_HELPER=1", "SSL_CERT_FILE="+path)
		output, err := cmd.CombinedOutput()
		t.Log(string(output))
		if err != nil {
			t.Fatal(err)
		}
		return
	}
	prefix := ": 原始注释\r\nevent: response.created\r\n" +
		"data: " + `{"type":"response.created","response":{"id":"resp_local","object":"response","status":"in_progress","headers":{"openai-model":"server-astra","x-codex-safety-buffering-enabled":"true","x-codex-safety-buffering-faster-model":"server-astra","x-codex-turn-state":"fixture-state"},"output":[]}}` + "\r\n\r\n" +
		"data: " + `{"type":"response.output_text.delta","response_id":"resp_local","delta":"local "}` + "\n\n"
	suffix := "event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_local\",\"delta\":\"尾部\"}\n\n" + strings.Replace(observationCompleted, `"model":"gpt-5"`, `"model":"server-astra"`, 1)
	release := make(chan struct{})
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	httpReceived := make(chan []byte, 1)
	wsReceived := make(chan []byte, 2)
	wsClosed := make(chan struct{})
	wsText := []byte(" {\"type\":\"response.created\",\"metadata\":\"原始\",\"response\":{\"id\":\"resp_local\",\"object\":\"response\",\"status\":\"in_progress\",\"output\":[]}} ")
	wsBinary := []byte{0, 0xff, 0x80, 7}
	wsCompleted := []byte(strings.TrimSuffix(strings.TrimPrefix(observationCompleted, "data: "), "\n\n"))
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			conn, err := websocket.Accept(w, r, nil)
			if err != nil {
				return
			}
			defer conn.CloseNow()
			defer close(wsClosed)
			_, payload, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			wsReceived <- bytes.Clone(payload)
			for _, message := range []struct {
				kind websocket.MessageType
				body []byte
			}{{websocket.MessageText, wsText}, {websocket.MessageText, wsCompleted}} {
				if conn.Write(r.Context(), message.kind, message.body) != nil {
					return
				}
			}
			_, second, err := conn.Read(r.Context())
			if err != nil {
				return
			}
			wsReceived <- bytes.Clone(second)
			if conn.Write(r.Context(), websocket.MessageBinary, wsBinary) != nil {
				return
			}
			_, _, _ = conn.Read(r.Context())
			return
		}
		body, err := io.ReadAll(r.Body)
		if err != nil {
			return
		}
		httpReceived <- body
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, prefix)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, suffix)
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream)
	output, stop := startStreamRecorder(t, binaryPath, fixture)
	request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"hello","stream":true}`))
	request.Header.Set("Authorization", "Bearer client-test-key")
	request.Header.Set("Content-Type", "application/json")
	type result struct {
		body []byte
		err  error
	}
	finished := make(chan result, 1)
	go func() {
		response, err := fixture.server.Client().Do(request)
		if err != nil {
			finished <- result{err: err}
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		finished <- result{body: body, err: err}
	}()
	waitRecordedMatch(t, output, func(event map[string]any) bool {
		data, _ := event["data"].(map[string]any)
		return event["type"] == "upstream.stream" && data["transport"] == "sse" && data["kind"] == "data"
	})
	select {
	case value := <-finished:
		t.Fatalf("stream finished before release: %+v", value)
	default:
	}
	close(release)
	response := <-finished
	if response.err != nil || !bytes.Contains(response.body, []byte(`"model":"gpt-5"`)) || !bytes.Contains(response.body, []byte("local answer")) {
		t.Fatalf("downstream SSE=%s err=%v", response.body, response.err)
	}
	waitRecordedMatch(t, output, func(event map[string]any) bool {
		data, _ := event["data"].(map[string]any)
		return event["type"] == "request.finished" && data["stream"] == true
	})
	client := dialObservationResponses(t, fixture)
	if err := client.Write(t.Context(), websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5","input":"hello"}`)); err != nil {
		t.Fatal(err)
	}
	for {
		_, body, err := client.Read(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(body, []byte(`"response.completed"`)) {
			break
		}
	}
	if err := client.Write(t.Context(), websocket.MessageText, []byte(`{"type":"response.create","model":"gpt-5","input":"binary rejection"}`)); err != nil {
		t.Fatal(err)
	}
	_, rejected, rejectedErr := client.Read(t.Context())
	if rejectedErr == nil && !bytes.Contains(rejected, []byte(`"type":"error"`)) {
		t.Fatalf("binary rejection=%s", rejected)
	}
	client.CloseNow()
	select {
	case <-wsClosed:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream WebSocket remained open")
	}
	waitRecordedMatch(t, output, func(event map[string]any) bool {
		data, _ := event["data"].(map[string]any)
		return event["type"] == "upstream.stream" && data["transport"] == "websocket" && data["direction"] == "response" && data["kind"] == "end"
	})
	stop()
	decoded, err := exec.Command(binaryPath, "decode", output).Output()
	if err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	var sse, httpRequest bytes.Buffer
	wsBodies := map[string][][]byte{}
	sequences := map[string]uint64{}
	offsets := map[string]uint64{}
	ends := map[string]int{}
	var sseID, wsConnection string
	for {
		var event struct {
			Type        string `json:"type"`
			BodyOmitted bool   `json:"body_omitted"`
			Data        struct {
				Protocol    string `json:"protocol"`
				Transport   string `json:"transport"`
				Direction   string `json:"direction"`
				Kind        string `json:"kind"`
				RequestID   string `json:"request_id"`
				AttemptID   string `json:"attempt_id"`
				Connection  string `json:"upstream_connection_id"`
				Body        []byte `json:"body"`
				Sequence    uint64 `json:"stream_sequence"`
				Offset      uint64 `json:"byte_offset"`
				Size        uint64 `json:"body_size"`
				Last        uint64 `json:"last_stream_sequence"`
				Total       uint64 `json:"total_body_bytes"`
				MessageType string `json:"message_type"`
				WriteResult string `json:"write_result"`
				Reason      string `json:"end_reason"`
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
		if data.Protocol != "openai-responses" {
			t.Fatalf("stream protocol=%+v", data)
		}
		key := data.Transport + ":" + data.Direction
		if data.Transport == "sse" || data.Transport == "http" {
			if sseID == "" {
				sseID = data.RequestID
			}
			if data.RequestID != sseID || data.AttemptID != sseID+":1" {
				t.Fatalf("SSE identity=%+v", data)
			}
		} else {
			if wsConnection == "" {
				wsConnection = data.Connection
			}
			if data.Connection == "" || data.Connection != wsConnection {
				t.Fatalf("WS identity=%+v", data)
			}
		}
		if data.Kind == "data" {
			sequences[key]++
			if event.BodyOmitted || ends[key] != 0 || data.Sequence != sequences[key] || data.Offset != offsets[key] || data.Size != uint64(len(data.Body)) {
				t.Fatalf("content continuity=%+v", event)
			}
			offsets[key] += data.Size
			if data.Transport == "http" {
				if data.Direction != "request" || len(data.Body) > 32*1024 {
					t.Fatalf("HTTP request content=%+v", data)
				}
				httpRequest.Write(data.Body)
			} else if data.Transport == "sse" {
				sse.Write(data.Body)
			} else {
				wsBodies[data.Direction] = append(wsBodies[data.Direction], data.Body)
				if data.Direction == "request" && data.WriteResult != "success" {
					t.Fatalf("send result=%+v", data)
				}
				if bytes.Equal(data.Body, wsBinary) && data.MessageType != "binary" {
					t.Fatalf("binary kind=%+v", data)
				}
			}
		} else {
			ends[key]++
			if ends[key] != 1 || data.Last != sequences[key] || data.Total != offsets[key] || data.Reason == "" {
				t.Fatalf("end counters=%+v", data)
			}
		}
	}
	if sent := <-httpReceived; !bytes.Equal(sent, httpRequest.Bytes()) || httpRequest.Len() == 0 {
		t.Fatalf("HTTP request sent=%q recorded=%q", sent, httpRequest.Bytes())
	}
	if sse.String() != prefix+suffix {
		t.Fatalf("raw SSE mismatch: got %q want %q", sse.String(), prefix+suffix)
	}
	if !bytes.Contains(sse.Bytes(), []byte(`"model":"server-astra"`)) || bytes.Contains([]byte(prefix), []byte(`"model":`)) {
		t.Fatal("raw model evidence changed")
	}
	if len(wsBodies["request"]) != 2 {
		t.Fatalf("WS recorded sends=%q", wsBodies["request"])
	}
	for _, body := range wsBodies["request"] {
		sent := <-wsReceived
		if !bytes.Equal(body, sent) {
			t.Fatalf("WS sent=%q recorded=%q", sent, body)
		}
	}
	if len(wsBodies["response"]) != 3 || !bytes.Equal(wsBodies["response"][0], wsText) || !bytes.Equal(wsBodies["response"][1], wsCompleted) || !bytes.Equal(wsBodies["response"][2], wsBinary) {
		t.Fatalf("WS responses=%q", wsBodies["response"])
	}
	for _, key := range []string{"http:request", "sse:response", "websocket:request", "websocket:response"} {
		if ends[key] != 1 {
			t.Fatalf("missing end %s: %v", key, ends)
		}
	}
	writeObservationArtifact(t, "sse-upstream.txt", []byte(prefix+suffix))
	writeObservationArtifact(t, "sse-reconstructed.txt", sse.Bytes())
	writeObservationArtifact(t, "sse-http-request.json", httpRequest.Bytes())
	writeObservationArtifact(t, "events.jsonl", decoded)
	recording, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	writeObservationArtifact(t, "events.bin", recording)
	messages, err := json.Marshal(wsBodies)
	if err != nil {
		t.Fatal(err)
	}
	writeObservationArtifact(t, "websocket-messages.json", messages)
}

func startStreamRecorder(t *testing.T, binaryPath string, fixture *observationHost) (string, func()) {
	t.Helper()
	dir := t.TempDir()
	ca := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: fixture.server.Certificate().Raw}), 0600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(dir, "events.bin")
	configuration, _ := json.Marshal(map[string]any{"host_url": strings.Replace(fixture.server.URL, "https:", "wss:", 1), "plugin_id": "research", "token": "plugin-test-token", "output": output, "ca_file": ca})
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, configuration, 0600); err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binaryPath, "record", "--config", configPath)
	var diagnostics bytes.Buffer
	command.Stderr = &diagnostics
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if command.ProcessState == nil {
			command.Process.Kill()
			command.Wait()
		}
	})
	ready := make(chan struct{})
	go func() {
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			if strings.Contains(scanner.Text(), "subscribed") {
				select {
				case <-ready:
				default:
					close(ready)
				}
			}
		}
	}()
	select {
	case <-ready:
	case <-time.After(8 * time.Second):
		t.Fatal("recorder did not subscribe")
	}
	return output, func() {
		t.Helper()
		if err := command.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		if err := command.Wait(); err != nil {
			t.Fatalf("recorder exit=%v %s", err, diagnostics.String())
		}
	}
}

func waitRecordedMatch(t *testing.T, path string, predicate func(map[string]any) bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		file, err := os.Open(path)
		if err == nil {
			found := false
			for {
				var header [4]byte
				if _, err := io.ReadFull(file, header[:]); err != nil {
					break
				}
				size := binary.BigEndian.Uint32(header[:])
				if size > 4_000_000 {
					file.Close()
					t.Fatalf("record size=%d", size)
				}
				body := make([]byte, size)
				if _, err := io.ReadFull(file, body); err != nil {
					break
				}
				if predicate(decodeObservation(t, body)) {
					found = true
					break
				}
			}
			file.Close()
			if found {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatal("recorded event did not arrive")
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func writeObservationArtifact(t *testing.T, name string, data []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(t.ArtifactDir(), name), data, 0600); err != nil {
		t.Fatal(err)
	}
}
