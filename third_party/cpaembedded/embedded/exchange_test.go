package embedded

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
)

func TestCodexHTTPExchangeCompressionFallbackSnapshots(t *testing.T) {
	var wireHeaders []http.Header
	var wireBodies [][]byte
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		wireHeaders = append(wireHeaders, r.Header.Clone())
		wireBodies = append(wireBodies, body)
		w.Header()["X-Raw-Values"] = []string{"one", "two"}
		if len(wireHeaders) == 1 {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			io.WriteString(w, `{"error":{"message":"use plaintext"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		io.WriteString(w, "data: "+`{"type":"response.completed","response":{"id":"resp_local","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":3,"output_tokens":0,"total_tokens":3}}}`+"\n\n")
	}))
	defer upstream.Close()
	var retainedRequest *http.Request
	var retainedResponse *http.Response
	// SDK 仅向官方主机压缩。边界传输把目标转向本地 TLS 服务并使用其验证证书。
	transport := claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		retainedRequest = req
		clone := req.Clone(req.Context())
		clone.URL.Scheme = "https"
		clone.URL.Host = strings.TrimPrefix(upstream.URL, "https://")
		resp, err := upstream.Client().Transport.RoundTrip(clone)
		retainedResponse = resp
		return resp, err
	})
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
	executor := NewCodexHTTPExecutor()
	executor.cfg.Codex.UpstreamRequestCompression = true
	var facts []ExchangeObservation
	response, err := executor.ExecuteStreamCanonical(ctx, "test-credential", CodexCredential{
		Type: ProviderCodex, AccessToken: "test-access", RefreshToken: "test-refresh", AccountID: "test-account",
	}, ExecuteRequest{Model: "gpt-5", Format: "openai-response", Payload: []byte(`{"model":"gpt-5","input":"` + strings.Repeat("repeat ", 400) + `"}`),
		Headers: http.Header{"X-Source-Values": {"original", "second"}},
		ObserveExchange: func(fact ExchangeObservation) {
			fact.Header = fact.Header.Clone()
			fact.Body = append([]byte(nil), fact.Body...)
			facts = append(facts, fact)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for chunk := range response.Chunks {
		if chunk.Err != nil {
			t.Fatal(chunk.Err)
		}
	}
	if len(wireHeaders) != 2 || wireHeaders[0].Get("Content-Encoding") != "zstd" || wireHeaders[1].Get("Content-Encoding") != "" {
		t.Fatalf("wire headers = %#v", wireHeaders)
	}
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer decoder.Close()
	decoded, err := decoder.DecodeAll(wireBodies[0], nil)
	if err != nil || string(decoded) != string(wireBodies[1]) {
		t.Fatalf("resend payload changed: %v", err)
	}
	if len(facts) < 6 {
		t.Fatalf("facts = %+v", facts)
	}
	var captured strings.Builder
	var lastSequence uint64
	for _, fact := range facts[4:] {
		if fact.Kind != "stream" || fact.Sequence != 2 || fact.Stream == nil {
			t.Fatalf("stream fact = %+v", fact)
		}
		if fact.Stream.Kind == "data" {
			if fact.Stream.ByteOffset != uint64(captured.Len()) || fact.Stream.StreamSequence != lastSequence+1 {
				t.Fatalf("stream position = %+v", fact.Stream)
			}
			captured.Write(fact.Body)
			lastSequence = fact.Stream.StreamSequence
		}
	}
	end := facts[len(facts)-1].Stream
	if !strings.Contains(captured.String(), `"model":"gpt-5"`) || end.Kind != "end" || end.TotalBodyBytes != uint64(captured.Len()) || end.LastStreamSequence != lastSequence {
		t.Fatalf("captured body = %q, end = %+v", captured.String(), end)
	}
	for index, expected := range []struct {
		kind, phase string
		seq         uint64
		status      int
	}{
		{"request", "http_exchange", 1, 0}, {"response", "http_exchange", 1, 415},
		{"request", "http_exchange", 2, 0}, {"response", "sse_initialization", 2, 200},
	} {
		fact := facts[index]
		if fact.Kind != expected.kind || fact.Phase != expected.phase || fact.Sequence != expected.seq || fact.StatusCode != expected.status || fact.ObservedAt.IsZero() {
			t.Fatalf("fact[%d]=%+v", index, fact)
		}
	}
	retainedRequest.Header.Set("Authorization", "changed")
	retainedResponse.Header["X-Raw-Values"][0] = "changed"
	if facts[2].Header.Get("Authorization") != "Bearer test-access" || strings.Join(facts[3].Header.Values("X-Raw-Values"), ",") != "one,two" || facts[0].Header.Get("Content-Encoding") != "zstd" || facts[2].Header.Get("Content-Encoding") != "" {
		t.Fatalf("unstable snapshots = %+v", facts)
	}
}
