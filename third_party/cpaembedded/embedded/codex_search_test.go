package embedded

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestCodexSearchResponseLimitPreservesFailureMetadata(t *testing.T) {
	body := strings.Repeat("x", maxObservedBodyBytes+1)
	for _, status := range []int{http.StatusUnauthorized, http.StatusOK} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			transport := claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: status, Header: http.Header{"Retry-After": {"17"}},
					Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
			})
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
			var exchanges []ExchangeObservation
			response, err := NewCodexHTTPExecutor().ExecuteCanonical(ctx, "credential", CodexCredential{
				Type: ProviderCodex, AccessToken: "test-access", RefreshToken: "test-refresh", AccountID: "account-one",
			}, ExecuteRequest{Model: "gpt-5", Payload: []byte(`{"id":"session","model":"gpt-5"}`),
				Format: "openai-response", RequestPath: "/v1/alpha/search", ObserveExchange: func(fact ExchangeObservation) {
					if fact.Stream != nil {
						return
					}
					fact.Header = fact.Header.Clone()
					fact.Body = append([]byte(nil), fact.Body...)
					exchanges = append(exchanges, fact)
				}})
			if err == nil || response.StatusCode != status || response.Headers.Get("Retry-After") != "17" || len(response.Payload) != 0 {
				t.Fatalf("oversized response = %#v, error = %v", response, err)
			}
			if len(exchanges) != 2 || exchanges[0].Kind != "request" || exchanges[1].Kind != "response" || exchanges[1].StatusCode != status || exchanges[1].Header.Get("Retry-After") != "17" || exchanges[1].Phase != "http_exchange" {
				t.Fatalf("search exchanges = %+v", exchanges)
			}
			var statusError interface{ StatusCode() int }
			if status == http.StatusUnauthorized && (!errors.As(err, &statusError) || statusError.StatusCode() != status) {
				t.Fatalf("oversized 401 lost authentication failure: %v", err)
			}
		})
	}
}

func TestCodexSearchObservesFinalRequestAndDecodedResponse(t *testing.T) {
	for _, test := range []struct {
		status  int
		invalid bool
	}{{http.StatusOK, false}, {http.StatusUnauthorized, false}, {http.StatusUnauthorized, true}} {
		status := test.status
		name := http.StatusText(status)
		if test.invalid {
			name += "/invalid-gzip"
		}
		t.Run(name, func(t *testing.T) {
			const payload = `{"result":"压缩内容"}`
			var compressed bytes.Buffer
			encoder := gzip.NewWriter(&compressed)
			if _, err := io.WriteString(encoder, payload); err != nil {
				t.Fatal(err)
			}
			if err := encoder.Close(); err != nil {
				t.Fatal(err)
			}
			if test.invalid {
				compressed.Reset()
				compressed.WriteString("invalid gzip")
			}
			var sent []byte
			transport := claudeRoundTripperFunc(func(req *http.Request) (*http.Response, error) {
				if req.GetBody != nil {
					t.Error("search enabled implicit replay")
				}
				var err error
				sent, err = io.ReadAll(req.Body)
				if err != nil {
					t.Fatal(err)
				}
				if err := req.Body.Close(); err != nil {
					t.Fatal(err)
				}
				return &http.Response{StatusCode: status, Header: http.Header{"Content-Encoding": {"gzip"}, "Retry-After": {"17"}},
					Body: io.NopCloser(bytes.NewReader(compressed.Bytes())), Request: req}, nil
			})
			ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))
			var requestBody, responseBody bytes.Buffer
			var ends []*StreamObservation
			response, err := NewCodexHTTPExecutor().ExecuteCanonical(ctx, "credential", CodexCredential{
				Type: ProviderCodex, AccessToken: "access", RefreshToken: "refresh", AccountID: "account",
			}, ExecuteRequest{Model: "gpt-5", Payload: []byte(`{"model":"gpt-5","query":"hello"}`),
				Format: "openai-response", RequestPath: "/v1/alpha/search",
				Headers: http.Header{"X-Codex-Installation-Id": {"installation-final"}},
				ObserveExchange: func(fact ExchangeObservation) {
					if fact.Stream == nil {
						return
					}
					if fact.Stream.Transport != "http" || fact.Sequence != 1 {
						t.Errorf("stream = %+v", fact.Stream)
					}
					if fact.Stream.Kind == "end" {
						ends = append(ends, fact.Stream)
						return
					}
					if fact.Stream.Direction == "request" {
						requestBody.Write(fact.Body)
					} else {
						responseBody.Write(fact.Body)
					}
				}})
			if test.invalid {
				var statusError *codexSearchHTTPError
				if !errors.As(err, &statusError) || statusError.StatusCode() != status || statusError.RetryAfter().Seconds() != 17 || response.StatusCode != status || response.Headers.Get("Retry-After") != "17" {
					t.Fatalf("decode failure lost metadata: response=%+v error=%v", response, err)
				}
				if responseBody.Len() != 0 || len(ends) != 2 || ends[1].EndReason != "read_error" || ends[1].TotalBodyBytes != 0 {
					t.Fatalf("decode failure body=%q ends=%+v", responseBody.Bytes(), ends)
				}
				return
			}
			if (err != nil) != (status != http.StatusOK) || string(response.Payload) != payload {
				t.Fatalf("response=%+v error=%v", response, err)
			}
			if !bytes.Equal(requestBody.Bytes(), sent) || !bytes.Contains(sent, []byte(`"x-codex-installation-id":"installation-final"`)) || responseBody.String() != payload {
				t.Fatalf("request=%q sent=%q response=%q", requestBody.Bytes(), sent, responseBody.String())
			}
			if len(ends) != 2 || ends[0].Direction != "request" || ends[1].Direction != "response" || ends[0].EndReason != "eof" || ends[1].EndReason != "eof" || ends[1].TotalBodyBytes != uint64(len(payload)) {
				t.Fatalf("ends = %+v", ends)
			}
		})
	}
}
