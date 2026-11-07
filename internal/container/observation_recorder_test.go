package container

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gpt-load/internal/platform/config"
)

// RECORDER_BINARY 指向独立插件模块编译的可执行文件，保持宿主模块依赖独立。
func TestObservationStandaloneRecorder(t *testing.T) {
	binaryPath := os.Getenv("RECORDER_BINARY")
	if binaryPath == "" {
		t.Skip("set RECORDER_BINARY to run standalone recorder acceptance")
	}
	upstream := observationLocalUpstream()
	defer upstream.Close()
	fixture := newObservationHost(t, upstream, func(cfg *config.Config) { cfg.Observation.QueueMaxBytes = 150_000 })
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
	command := exec.Command(binaryPath, "record", "--config", configPath)
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
	status := make(chan string, 16)
	go func() {
		defer close(status)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			status <- scanner.Text()
		}
	}()
	waitStatus := func(want string) {
		t.Helper()
		timer := time.NewTimer(8 * time.Second)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-status:
				if !ok {
					t.Fatalf("recorder stopped: %s", diagnostics.String())
				}
				if strings.Contains(line, want) {
					return
				}
			case <-timer.C:
				t.Fatalf("waiting for recorder %s", want)
			}
		}
	}
	waitStatus("subscribed")
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"`+strings.Repeat("b", 80_000)+`"}`))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer client-test-key")
	request.Header.Set("Content-Type", "application/json")
	response, err := fixture.server.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	responseBody, _ := io.ReadAll(response.Body)
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !bytes.Contains(responseBody, []byte("local answer")) {
		t.Fatalf("large request=%d %s", response.StatusCode, responseBody)
	}
	first := response.Header.Get("X-GPTLoad-Request-ID")
	waitRecordedRequest(t, output, first)
	// 新有效连接接替录制连接；离线窗口内产生真实 Codex 请求。
	replacement := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	waitStatus("reconnecting")
	queued := fixture.request(t)
	if err := fixture.lifecycle.WaitData(t.Context()); err != nil {
		t.Fatal(err)
	}
	replacement.CloseNow()
	waitStatus("subscribed")
	waitRecordedRequest(t, output, queued)
	last := fixture.request(t)
	waitRecordedRequest(t, output, last)
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	waitStatus("unsubscribed; recording boundaries complete")
	if err := command.Wait(); err != nil {
		t.Fatalf("recorder exit=%v %s", err, diagnostics.String())
	}
	decoded, err := exec.Command(binaryPath, "decode", output).Output()
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{first, queued, last} {
		if !bytes.Contains(decoded, []byte(id)) {
			t.Fatalf("decoded recording missing request %s", id)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(decoded))
	var boot string
	var lastSequence uint64
	for {
		var identity struct {
			BootID     string `json:"boot_id"`
			Seq        uint64 `json:"seq"`
			ObservedAt int64  `json:"observed_at"`
			Type       string `json:"type"`
			Data       struct {
				RequestID   string `json:"request_id"`
				RequestBody []byte `json:"request_body"`
			} `json:"data"`
		}
		if err := decoder.Decode(&identity); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if boot == "" {
			boot = identity.BootID
		}
		if identity.BootID != boot || identity.Seq <= lastSequence || identity.ObservedAt == 0 {
			t.Fatalf("recorded identity=%+v after seq=%d", identity, lastSequence)
		}
		lastSequence = identity.Seq
		if identity.Type == "request.finished" && identity.Data.RequestID == first && len(identity.Data.RequestBody) < 80_000 {
			t.Fatal("large request body lost in standalone recording")
		}
	}
	if strings.Contains(diagnostics.String(), "plugin-test-token") {
		t.Fatal("token in diagnostics")
	}
	// 取消后重新连接，先订阅前产生的请求应被跳过。
	ignored := fixture.request(t)
	if err := fixture.lifecycle.WaitData(t.Context()); err != nil {
		t.Fatal(err)
	}
	probe := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer probe.CloseNow()
	fixture.command(t, probe, "subscribe", "after-recorder")
	readObservation(t, probe)
	newRequest := fixture.request(t)
	for {
		event := readObservation(t, probe)
		data := event["data"].(map[string]any)
		if data["request_id"] == ignored {
			t.Fatal("recorder subscription remained active")
		}
		if event["type"] == "request.finished" && data["request_id"] == newRequest {
			break
		}
	}
	fixture.command(t, probe, "unsubscribe", "probe-stop")
	readObservation(t, probe)
}

func waitRecordedRequest(t *testing.T, path, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		file, err := os.Open(path)
		if err == nil {
			found := false
			for {
				var header [4]byte
				if _, err := io.ReadFull(file, header[:]); err != nil {
					break
				}
				length := binary.BigEndian.Uint32(header[:])
				if length > 150_000 {
					t.Fatalf("record length=%d", length)
				}
				raw := make([]byte, length)
				if _, err := io.ReadFull(file, raw); err != nil {
					break
				}
				event := decodeObservation(t, raw)
				if event["type"] == "request.finished" && event["data"].(map[string]any)["request_id"] == id {
					found = true
					break
				}
			}
			file.Close()
			if found {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("recording missing finished request %s", id)
}
