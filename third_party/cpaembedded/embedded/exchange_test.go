package embedded

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
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
	var factsMu sync.Mutex
	response, err := executor.ExecuteStreamCanonical(ctx, "test-credential", CodexCredential{
		Type: ProviderCodex, AccessToken: "test-access", RefreshToken: "test-refresh", AccountID: "test-account",
	}, ExecuteRequest{Model: "gpt-5", Format: "openai-response", Payload: []byte(`{"model":"gpt-5","input":"` + strings.Repeat("repeat ", 400) + `"}`),
		Headers: http.Header{"X-Source-Values": {"original", "second"}},
		ObserveExchange: func(fact ExchangeObservation) {
			factsMu.Lock()
			defer factsMu.Unlock()
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
	factsMu.Lock()
	defer factsMu.Unlock()
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
	var headers []ExchangeObservation
	var captured strings.Builder
	var requestBodies [2]strings.Builder
	var requestEnds, responseEnds [2]*StreamObservation
	var lastSequence uint64
	for _, fact := range facts {
		if fact.Stream == nil {
			headers = append(headers, fact)
			continue
		}
		stream := fact.Stream
		index := int(fact.Sequence) - 1
		if stream.Direction == "request" {
			if stream.Transport != "http" {
				t.Fatalf("request transport = %q", stream.Transport)
			}
			if stream.Kind == "data" {
				requestBodies[index].Write(fact.Body)
			} else {
				requestEnds[index] = stream
			}
			continue
		}
		if stream.Kind == "end" {
			responseEnds[index] = stream
		}
		if fact.Sequence != 2 || stream.Kind != "data" {
			continue
		}
		if stream.Transport != "sse" || stream.ByteOffset != uint64(captured.Len()) || stream.StreamSequence != lastSequence+1 {
			t.Fatalf("stream position = %+v", stream)
		}
		captured.Write(fact.Body)
		lastSequence = stream.StreamSequence
	}
	for index := range requestBodies {
		if requestBodies[index].String() != string(wireBodies[index]) || requestEnds[index] == nil || requestEnds[index].TotalBodyBytes != uint64(len(wireBodies[index])) {
			t.Fatalf("request[%d] bytes = %d, wire = %d, end = %+v", index, requestBodies[index].Len(), len(wireBodies[index]), requestEnds[index])
		}
	}
	end := responseEnds[1]
	if !strings.Contains(captured.String(), `"model":"gpt-5"`) || end == nil || end.TotalBodyBytes != uint64(captured.Len()) || end.LastStreamSequence != lastSequence {
		t.Fatalf("captured body = %q, end = %+v", captured.String(), end)
	}
	if responseEnds[0] == nil || responseEnds[0].Transport != "http" || responseEnds[0].EndReason != "closed_early" || responseEnds[0].TotalBodyBytes != 0 {
		t.Fatalf("unconsumed compression rejection = %+v", responseEnds[0])
	}
	if len(headers) != 4 {
		t.Fatalf("header observations = %d", len(headers))
	}
	for index, expected := range []struct {
		kind, phase string
		seq         uint64
		status      int
	}{
		{"request", "http_exchange", 1, 0}, {"response", "http_exchange", 1, 415},
		{"request", "http_exchange", 2, 0}, {"response", "sse_initialization", 2, 200},
	} {
		fact := headers[index]
		if fact.Kind != expected.kind || fact.Phase != expected.phase || fact.Sequence != expected.seq || fact.StatusCode != expected.status || fact.ObservedAt.IsZero() {
			t.Fatalf("fact[%d]=%+v", index, fact)
		}
	}
	retainedRequest.Header.Set("Authorization", "changed")
	retainedResponse.Header["X-Raw-Values"][0] = "changed"
	if headers[2].Header.Get("Authorization") != "Bearer test-access" || strings.Join(headers[3].Header.Values("X-Raw-Values"), ",") != "one,two" || headers[0].Header.Get("Content-Encoding") != "zstd" || headers[2].Header.Get("Content-Encoding") != "" {
		t.Fatalf("unstable snapshots = %+v", facts)
	}
}

func TestCodexHTTPExchangeTransportReplay(t *testing.T) {
	const payload = `{"input":"replayed request"}`
	request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://upstream.example/responses", strings.NewReader(payload))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("X-Final-Header", "final")
	factory := request.GetBody
	factoryCalls := 0
	factoryFailure := errors.New("replay factory failed")
	request.GetBody = func() (io.ReadCloser, error) {
		factoryCalls++
		// executionObservation reads its snapshot before the replay wrapper is
		// installed; the failed replay should publish no new exchange.
		if factoryCalls == 2 {
			return nil, factoryFailure
		}
		return factory()
	}
	var facts []ExchangeObservation
	observation := &executionObservation{capture: true, exchangeObserver: func(fact ExchangeObservation) {
		fact.Body = append([]byte(nil), fact.Body...)
		fact.Header = fact.Header.Clone()
		facts = append(facts, fact)
	}}
	transport := noRedirectRoundTripper{observation: observation, base: claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
		prefix := make([]byte, 5)
		if _, err := io.ReadFull(req.Body, prefix); err != nil {
			t.Fatal(err)
		}
		if err := req.Body.Close(); err != nil {
			t.Fatal(err)
		}
		if body, err := req.GetBody(); body != nil || err != factoryFailure {
			t.Fatalf("failed replay factory = %v, %v", body, err)
		}
		body, err := req.GetBody()
		if err != nil {
			t.Fatal(err)
		}
		replayed, err := io.ReadAll(body)
		if err != nil || string(replayed) != payload {
			t.Fatalf("replay body = %q, %v", replayed, err)
		}
		if err := body.Close(); err != nil {
			t.Fatal(err)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: http.NoBody}, nil
	})}
	response, err := transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	pending := response.Body.(*pendingCodexHTTPResponseBody)
	response.Body = pending.ObserveDecodedHTTPResponseBody(pending)
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
	var headers []ExchangeObservation
	var bodies [2]strings.Builder
	var requestEnds [2]*StreamObservation
	var responseEnd *StreamObservation
	for _, fact := range facts {
		if fact.Stream == nil {
			headers = append(headers, fact)
			continue
		}
		if fact.Stream.Direction == "response" {
			responseEnd = fact.Stream
			if fact.Sequence != 2 {
				t.Fatalf("response exchange = %d", fact.Sequence)
			}
			continue
		}
		index := int(fact.Sequence) - 1
		if index < 0 || index > 1 {
			t.Fatalf("request exchange = %d", fact.Sequence)
		}
		if fact.Stream.Kind == "end" {
			requestEnds[index] = fact.Stream
			continue
		}
		if fact.Stream.ByteOffset != uint64(bodies[index].Len()) {
			t.Fatalf("request offset = %+v", fact.Stream)
		}
		bodies[index].Write(fact.Body)
	}
	if factoryCalls != 3 || len(headers) != 3 || headers[0].Kind != "request" || headers[0].Sequence != 1 || headers[1].Kind != "request" || headers[1].Sequence != 2 || headers[1].Header.Get("X-Final-Header") != "final" || headers[2].Kind != "response" || headers[2].Sequence != 2 {
		t.Fatalf("factory calls = %d, headers = %+v", factoryCalls, headers)
	}
	if bodies[0].String() != payload[:5] || bodies[1].String() != payload || requestEnds[0] == nil || requestEnds[1] == nil || requestEnds[0].EndReason != "closed_early" || requestEnds[0].TotalBodyBytes != 5 || requestEnds[1].EndReason != "eof" || requestEnds[1].TotalBodyBytes != uint64(len(payload)) || responseEnd == nil || responseEnd.EndReason != "eof" {
		t.Fatalf("request bodies = %q / %q, ends = %+v / %+v, response = %+v", bodies[0].String(), bodies[1].String(), requestEnds[0], requestEnds[1], responseEnd)
	}
}
