package container

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"gpt-load/internal/platform/config"
)

func TestObservationStreamSubscriptionGap(t *testing.T) {
	const pieces = 6
	advance := make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		for i := 0; i < pieces; i++ {
			if i > 0 {
				select {
				case <-advance:
				case <-r.Context().Done():
					return
				}
			}
			io.WriteString(w, observationGapPiece(i))
			w.(http.Flusher).Flush()
		}
		select {
		case <-advance:
		case <-r.Context().Done():
			return
		}
		io.WriteString(w, observationCompleted)
	}))
	defer upstream.Close()
	fixture := newObservationHost(t, upstream, func(cfg *config.Config) { cfg.Observation.QueueMaxEvents = 2 })
	delivered := make(chan int, pieces)
	finished := make(chan error, 1)
	go func() {
		request, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(`{"model":"gpt-5","input":"hello","stream":true}`))
		request.Header.Set("Authorization", "Bearer client-test-key")
		request.Header.Set("Content-Type", "application/json")
		response, err := fixture.server.Client().Do(request)
		if err != nil {
			finished <- err
			return
		}
		defer response.Body.Close()
		scanner := bufio.NewScanner(response.Body)
		for scanner.Scan() {
			line := scanner.Text()
			for i := 0; i < pieces; i++ {
				if strings.Contains(line, fmt.Sprintf(`"delta":"piece%d"`, i)) {
					delivered <- i
				}
			}
		}
		finished <- scanner.Err()
	}()
	waitPiece := func(want int) {
		t.Helper()
		select {
		case got := <-delivered:
			if got != want {
				t.Fatalf("delivered piece=%d want=%d", got, want)
			}
		case err := <-finished:
			t.Fatalf("stream stopped: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatalf("piece %d stalled", want)
		}
	}
	waitPiece(0)
	// The body is already flowing before this subscription exists.
	plugin := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	fixture.command(t, plugin, "subscribe", "middle")
	boot := readObservation(t, plugin)["boot_id"]
	advance <- struct{}{}
	waitPiece(1)
	event := readObservation(t, plugin)
	data := event["data"].(map[string]any)
	if event["type"] != "upstream.stream" || data["kind"] != "data" || data["stream_sequence"] != int64(2) || data["byte_offset"] != int64(len(observationGapPiece(0))) || data["protocol"] != "openai-responses" {
		t.Fatalf("midstream prefix gap=%#v", event)
	}
	firstSequence := event["seq"].(int64)
	if err := plugin.Close(websocket.StatusNormalClosure, "offline"); err != nil {
		t.Fatal(err)
	}
	for i := 2; i < pieces; i++ {
		advance <- struct{}{}
		waitPiece(i)
	}
	advance <- struct{}{}
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("business stream stalled while observer offline")
	}
	if err := fixture.lifecycle.WaitData(t.Context()); err != nil {
		t.Fatal(err)
	}
	resumed := fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
	defer resumed.CloseNow()
	fixture.command(t, resumed, "subscribe", "resume")
	if reply := readObservation(t, resumed); reply["boot_id"] != boot {
		t.Fatalf("resume=%#v", reply)
	}
	end := readObservation(t, resumed)
	totals := end["data"].(map[string]any)
	expectedBytes := len(observationCompleted)
	for i := 0; i < pieces; i++ {
		expectedBytes += len(observationGapPiece(i))
	}
	if end["type"] != "upstream.stream" || totals["kind"] != "end" || end["seq"].(int64) <= firstSequence+1 || totals["last_stream_sequence"] != int64(pieces+1) || totals["total_body_bytes"] != int64(expectedBytes) || (totals["end_reason"] != "eof" && totals["end_reason"] != "closed_early") {
		t.Fatalf("queued stream gap=%#v", end)
	}
	final := readObservation(t, resumed)
	result := final["data"].(map[string]any)
	if final["type"] != "request.finished" || result["status"] != "success" || result["request_id"] != data["request_id"] {
		t.Fatalf("business completion=%#v", final)
	}
	sample, err := json.MarshalIndent(map[string]any{"first_observed": event, "resumed_end": end, "business_completion": final}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	writeObservationArtifact(t, "subscription-gap.json", sample)
}

func observationGapPiece(i int) string {
	return fmt.Sprintf("data: {\"type\":\"response.output_text.delta\",\"response_id\":\"resp_local\",\"delta\":\"piece%d\"}\n\n", i)
}
