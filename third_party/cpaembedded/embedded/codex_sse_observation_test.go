package embedded

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/zstd"
)

func TestCodexSSEObservationDecodedBytes(t *testing.T) {
	raw := ": 注释\r\nevent: unknown\r\ndata: {\"safety_buffering\":true}\r\n\r\n" +
		"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"model\":\"server-model\",\"output\":[]}}\n\npartial-tail"
	for _, encoding := range []string{"identity", "gzip", "deflate", "raw-deflate", "br", "zstd", "gzip, br", "sniff-gzip", "sniff-zstd", "bad-gzip"} {
		t.Run(encoding, func(t *testing.T) {
			var wire []byte
			declared := encoding
			if encoding == "bad-gzip" {
				wire, declared = []byte("invalid gzip"), "gzip"
			} else {
				wire = []byte(raw)
				layers := strings.Split(encoding, ", ")
				for _, layer := range layers {
					if layer == "identity" {
						continue
					}
					var output bytes.Buffer
					var encoder io.WriteCloser
					var err error
					switch layer {
					case "gzip", "sniff-gzip":
						encoder = gzip.NewWriter(&output)
					case "deflate":
						encoder = zlib.NewWriter(&output)
					case "raw-deflate":
						encoder, err = flate.NewWriter(&output, flate.DefaultCompression)
						declared = "deflate"
					case "br":
						encoder = brotli.NewWriter(&output)
					case "zstd", "sniff-zstd":
						encoder, err = zstd.NewWriter(&output)
					}
					if err != nil {
						t.Fatal(err)
					}
					if _, err := encoder.Write(wire); err != nil {
						t.Fatal(err)
					}
					if err := encoder.Close(); err != nil {
						t.Fatal(err)
					}
					wire = output.Bytes()
				}
				if strings.HasPrefix(encoding, "sniff-") {
					declared = ""
				}
			}
			var facts []ExchangeObservation
			transport := claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Request: req, Header: http.Header{
					"Content-Type": {"text/event-stream"}, "Content-Encoding": {declared},
				}, Body: io.NopCloser(bytes.NewReader(wire))}, nil
			})
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
			stream, err := NewCodexHTTPExecutor().ExecuteStreamCanonical(ctx, "credential", CodexCredential{
				Type: ProviderCodex, AccessToken: "access", RefreshToken: "refresh", AccountID: "account",
			}, ExecuteRequest{Model: "requested-model", Payload: []byte(`{"input":"hello"}`), Format: "openai-response",
				ObserveExchange: func(fact ExchangeObservation) {
					if fact.Stream != nil && fact.Stream.Direction == "request" {
						return
					}
					fact.Body = bytes.Clone(fact.Body)
					fact.Header = fact.Header.Clone()
					facts = append(facts, fact)
				}})
			if encoding == "bad-gzip" {
				if err == nil || len(facts) != 3 || facts[2].Stream.EndReason != "read_error" || facts[2].Stream.TotalBodyBytes != 0 {
					t.Fatalf("decode failure: stream=%v err=%v facts=%+v", stream, err, facts)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for chunk := range stream.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
			}
			var captured bytes.Buffer
			for _, fact := range facts {
				if fact.Stream != nil && fact.Path != "/backend-api/codex/responses" {
					t.Fatalf("stream path = %q", fact.Path)
				}
				if fact.Stream != nil && fact.Stream.Kind == "data" {
					captured.Write(fact.Body)
				}
			}
			end := facts[len(facts)-1].Stream
			if captured.String() != raw || facts[1].Phase != "sse_initialization" || facts[1].Header.Get("Content-Encoding") != declared || end.Kind != "end" || end.TotalBodyBytes != uint64(len(raw)) {
				t.Fatalf("captured=%q init=%+v end=%+v", captured.String(), facts[1], end)
			}
		})
	}
}

func TestCodexSSEObservationReadBoundaries(t *testing.T) {
	failure := errors.New("private upstream body")
	for _, test := range []struct {
		name, reason string
		err          error
	}{
		{"eof", "eof", io.EOF}, {"failure", "read_error", failure}, {"canceled", "canceled", context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw := bytes.Repeat([]byte("☃\r\n"), 15000)
			var facts []ExchangeObservation
			active := false
			wire := &pendingCodexHTTPResponseBody{ctx: t.Context(), exchange: 7, ReadCloser: &sseFinalReadBody{body: raw, err: test.err},
				observe: func(fact ExchangeObservation) {
					if active {
						fact.Body = bytes.Clone(fact.Body)
						facts = append(facts, fact)
					}
				}}
			body := wire.ObserveDecodedHTTPResponseBody(wire)
			buffer := make([]byte, len(raw))
			// The first read happens without an active subscription. Counters
			// still advance, making the later missing prefix detectable.
			wire.ReadCloser = &sseFinalReadBody{body: []byte("prefix")}
			if n, err := body.Read(buffer); n != 6 || err != nil {
				t.Fatalf("prefix read=%d,%v", n, err)
			}
			wire.ReadCloser = &sseFinalReadBody{body: raw, err: test.err}
			active = true
			n, err := body.Read(buffer)
			if n != len(raw) || err != test.err || !bytes.Equal(buffer[:n], raw) {
				t.Fatalf("read=%d,%v", n, err)
			}
			clear(buffer)
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			var captured bytes.Buffer
			for i, fact := range facts[:len(facts)-1] {
				if fact.Sequence != 7 || fact.Stream.StreamSequence != uint64(i+2) || fact.Stream.ByteOffset != uint64(6+captured.Len()) || len(fact.Body) > maxHTTPObservationChunk || fact.Stream.BodySize != uint64(len(fact.Body)) {
					t.Fatalf("chunk=%+v", fact)
				}
				captured.Write(fact.Body)
			}
			end := facts[len(facts)-1].Stream
			if !bytes.Equal(captured.Bytes(), raw) || end.Kind != "end" || end.EndReason != test.reason || end.TotalBodyBytes != uint64(6+len(raw)) || end.LastStreamSequence != uint64(len(facts)) {
				t.Fatalf("end=%+v, captured=%d", end, captured.Len())
			}
		})
	}
}

type sseFinalReadBody struct {
	body []byte
	err  error
}

func (body *sseFinalReadBody) Read(buffer []byte) (int, error) {
	n := copy(buffer, body.body)
	body.body = body.body[n:]
	return n, body.err
}

func (*sseFinalReadBody) Close() error { return nil }

func TestCodexSSEObservationConcurrentClose(t *testing.T) {
	for _, canceled := range []bool{false, true} {
		t.Run(map[bool]string{false: "close", true: "cancel"}[canceled], func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reader := &sseBlockedReadBody{entered: make(chan struct{}), closed: make(chan struct{})}
			var facts []ExchangeObservation
			wire := &pendingCodexHTTPResponseBody{ctx: ctx, ReadCloser: reader, observe: func(fact ExchangeObservation) {
				fact.Body = bytes.Clone(fact.Body)
				facts = append(facts, fact)
			}}
			body := wire.ObserveDecodedHTTPResponseBody(wire)
			done := make(chan struct{})
			go func() {
				defer close(done)
				buffer := make([]byte, 32)
				n, err := body.Read(buffer)
				if n != 4 || err != io.ErrClosedPipe || string(buffer[:n]) != "tail" {
					t.Errorf("read=%d,%v body=%q", n, err, buffer[:n])
				}
			}()
			<-reader.entered
			if canceled {
				cancel()
			}
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			<-done
			if err := body.Close(); err != nil {
				t.Fatal(err)
			}
			reason := "closed_early"
			if canceled {
				reason = "canceled"
			}
			if len(facts) != 2 || string(facts[0].Body) != "tail" || facts[1].Stream.EndReason != reason || facts[1].Stream.TotalBodyBytes != 4 {
				t.Fatalf("facts=%+v", facts)
			}
		})
	}
}

type sseBlockedReadBody struct {
	entered, closed chan struct{}
	once            sync.Once
}

func (body *sseBlockedReadBody) Read(buffer []byte) (int, error) {
	close(body.entered)
	<-body.closed
	return copy(buffer, "tail"), io.ErrClosedPipe
}

func (body *sseBlockedReadBody) Close() error {
	body.once.Do(func() { close(body.closed) })
	return nil
}

func TestCodexHTTPObservationEmptyBodiesAndRedirect(t *testing.T) {
	for _, test := range []struct {
		name                      string
		requestBody, responseBody io.ReadCloser
		status                    int
		reason                    string
		failure                   error
	}{
		{name: "nil", status: http.StatusOK, reason: "eof"},
		{name: "NoBody", requestBody: http.NoBody, responseBody: http.NoBody, status: http.StatusOK, reason: "eof"},
		{name: "dial failure", requestBody: io.NopCloser(strings.NewReader("unconsumed")), failure: errors.New("dial failed"), reason: "closed_early"},
		{name: "redirect", responseBody: io.NopCloser(strings.NewReader("unconsumed")), status: http.StatusFound, reason: "closed_early"},
	} {
		t.Run(test.name, func(t *testing.T) {
			var facts []ExchangeObservation
			transport := noRedirectRoundTripper{observation: &executionObservation{exchangeObserver: func(fact ExchangeObservation) {
				if fact.Stream != nil {
					facts = append(facts, fact)
				}
			}},
				base: claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
					if test.failure != nil {
						return nil, test.failure
					}
					return &http.Response{StatusCode: test.status, Body: test.responseBody, Header: http.Header{"Location": {"https://other.example"}}}, nil
				})}
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://upstream.example/responses", test.requestBody)
			if err != nil {
				t.Fatal(err)
			}
			response, err := transport.RoundTrip(request)
			if response != nil && response.Body != nil {
				if body, ok := response.Body.(*pendingCodexHTTPResponseBody); ok {
					response.Body = body.ObserveDecodedHTTPResponseBody(body)
				}
				if _, err := io.Copy(io.Discard, response.Body); err != nil {
					t.Fatal(err)
				}
				if err := response.Body.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if (err != nil) != (test.status == http.StatusFound || test.failure != nil) {
				t.Fatalf("RoundTrip error = %v", err)
			}
			wantFacts := 2
			if test.failure != nil {
				wantFacts = 1
			}
			if len(facts) != wantFacts {
				t.Fatalf("body facts = %d", len(facts))
			}
			for index, fact := range facts {
				wantReason := "eof"
				if index == 1 || test.failure != nil {
					wantReason = test.reason
				}
				if fact.Stream.Kind != "end" || fact.Stream.Transport != "http" || fact.Stream.TotalBodyBytes != 0 || fact.Stream.LastStreamSequence != 0 || fact.Stream.EndReason != wantReason {
					t.Fatalf("end = %+v", fact.Stream)
				}
			}
		})
	}
}
