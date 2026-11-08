package container

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/codextls/testutil"
	"gorm.io/gorm"

	"gpt-load/internal/channel"
	"gpt-load/internal/codexprivacy"
	"gpt-load/internal/outboundproxy"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/encryption"
	"gpt-load/internal/state"
	stateloader "gpt-load/internal/state/loader"
	"gpt-load/internal/storage/models"
	"gpt-load/internal/subscription/providers/codex"
)

type privacyWireRequest struct {
	Transport string
	Header    http.Header
	Body      []byte
}

type privacyHelloConn struct {
	net.Conn
	buffer []byte
	hellos [][]byte
	store  *sync.Map
	done   bool
}

func (connection *privacyHelloConn) Read(buffer []byte) (int, error) {
	n, err := connection.Conn.Read(buffer)
	if connection.done {
		return n, err
	}
	connection.buffer = append(connection.buffer, buffer[:n]...)
	for len(connection.buffer) >= 5 {
		size := 5 + int(binary.BigEndian.Uint16(connection.buffer[3:5]))
		if len(connection.buffer) < size {
			break
		}
		if connection.buffer[0] == 22 && size > 5 && connection.buffer[5] == 1 {
			connection.hellos = append(connection.hellos, bytes.Clone(connection.buffer[:size]))
			connection.store.Store(connection.RemoteAddr().String(), append([][]byte(nil), connection.hellos...))
		}
		if connection.buffer[0] == 23 || len(connection.hellos) == 2 {
			connection.done = true
			connection.buffer = nil
			break
		}
		connection.buffer = connection.buffer[size:]
	}
	return n, err
}

type privacyHelloListener struct {
	net.Listener
	store *sync.Map
}

func (listener privacyHelloListener) Accept() (net.Conn, error) {
	connection, err := listener.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &privacyHelloConn{Conn: connection, store: listener.store}, nil
}

// This process boundary installs the controlled CA before the system root pool
// is initialized. Requests retain their real destination, SNI and TLS dialer.
func privacyTrustedProcess(t *testing.T) bool {
	t.Helper()
	if os.Getenv("GPTLOAD_PRIVACY_CERT_DIR") != "" {
		return true
	}
	dir := t.TempDir()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "privacy acceptance CA"},
		DNSNames: []string{"chatgpt.com", "auth.openai.com"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	for name, block := range map[string]*pem.Block{"cert.pem": {Type: "CERTIFICATE", Bytes: der}, "key.pem": {Type: "PRIVATE KEY", Bytes: keyDER}} {
		if err := os.WriteFile(filepath.Join(dir, name), pem.EncodeToMemory(block), 0600); err != nil {
			t.Fatal(err)
		}
	}
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^"+t.Name()+"$", "-test.v", "-test.timeout=60s")
	command.Env = append(os.Environ(), "GPTLOAD_PRIVACY_CERT_DIR="+dir, "SSL_CERT_FILE="+filepath.Join(dir, "cert.pem"))
	output, err := command.CombinedOutput()
	t.Log(string(output))
	if err != nil {
		t.Fatal(err)
	}
	return false
}

// The CONNECT proxy routes only the named synthetic destinations to the local
// TLS receiver. The production client still verifies the destination hostname.
func newPrivacyUpstream(t *testing.T, handler http.Handler) (*httptest.Server, string) {
	t.Helper()
	dir := os.Getenv("GPTLOAD_PRIVACY_CERT_DIR")
	certificate, err := tls.LoadX509KeyPair(filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	var hellos sync.Map
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		wantSNI := "chatgpt.com"
		if request.URL.Path == "/oauth/token" {
			wantSNI = "auth.openai.com"
		}
		if request.TLS == nil || request.TLS.ServerName != wantSNI {
			t.Errorf("%s target SNI=%v want=%q", request.URL.Path, request.TLS, wantSNI)
		}
		protocol := "http"
		if strings.EqualFold(request.Header.Get("Upgrade"), "websocket") {
			protocol = "websocket"
		}
		captured, ok := hellos.Load(request.RemoteAddr)
		if !ok {
			t.Errorf("missing %s target ClientHello", request.URL.Path)
		} else {
			for _, raw := range captured.([][]byte) {
				if err := testutil.VerifyClientHello(raw, protocol); err != nil {
					t.Errorf("%s target %s ClientHello: %v", request.URL.Path, protocol, err)
				}
			}
		}
		handler.ServeHTTP(w, request)
	}))
	upstream.Listener = privacyHelloListener{Listener: upstream.Listener, store: &hellos}
	upstream.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12}
	upstream.StartTLS()
	t.Cleanup(upstream.Close)
	var mu sync.Mutex
	var connections []net.Conn
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodConnect || (r.Host != "chatgpt.com:443" && r.Host != "auth.openai.com:443") {
			http.Error(w, "unexpected synthetic destination", http.StatusBadGateway)
			return
		}
		target, err := (&net.Dialer{}).DialContext(r.Context(), "tcp", upstream.Listener.Addr().String())
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		client, buffer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			_ = target.Close()
			return
		}
		mu.Lock()
		connections = append(connections, client, target)
		mu.Unlock()
		defer client.Close()
		defer target.Close()
		if _, err := buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			return
		}
		if buffer.Flush() != nil {
			return
		}
		copied := make(chan struct{})
		go func() { _, _ = io.Copy(target, buffer); _ = target.Close(); close(copied) }()
		_, _ = io.Copy(client, target)
		_ = client.Close()
		<-copied
	}))
	t.Cleanup(func() {
		mu.Lock()
		for _, connection := range connections {
			_ = connection.Close()
		}
		mu.Unlock()
		proxy.Close()
	})
	return upstream, proxy.URL
}

type privacyHost struct {
	*observationHost
	fingerprints map[string]string
	credentialID map[string]uint
}

func newPrivacyHost(t *testing.T, upstream *httptest.Server, proxyURL string) *privacyHost {
	t.Helper()
	host := &privacyHost{observationHost: newObservationHostWithTransport(t, upstream, nil), fingerprints: map[string]string{}, credentialID: map[string]uint{}}
	err := host.deps.Invoke(func(db *gorm.DB, keys encryption.Service, registry *state.CredentialRegistry, manager *state.Manager) {
		input := state.CompileInput{ChannelRegistry: channel.NewRegistry(), SystemSettings: config.Settings{state.SettingRetryCount: 1}, GlobalProxy: &outboundproxy.Config{Mode: outboundproxy.ModeCustom, URL: proxyURL}}
		proxy := outboundproxy.Config{Mode: outboundproxy.ModeCustom, URL: proxyURL}
		encodedProxy, err := outboundproxy.Encode(proxy)
		if err != nil {
			t.Fatal(err)
		}
		encryptedProxy, err := keys.Encrypt(encodedProxy)
		if err != nil {
			t.Fatal(err)
		}
		var entries []state.CredentialEntry
		for index, account := range []string{"account-a", "account-b"} {
			group := models.Group{Name: "privacy-" + account, ChannelID: "codex", ConnectionType: models.ConnectionTypeSubscription, Params: models.JSON(`{"base_url":"https://chatgpt.com"}`), Models: models.JSON(`[]`), Overrides: models.JSON(`{}`), ProxyConfig: &encryptedProxy, Enabled: true}
			if err := db.Create(&group).Error; err != nil {
				t.Fatal(err)
			}
			canonical, err := codex.MarshalCredential(codex.Credential{Type: "codex", AccessToken: "access-" + account, RefreshToken: "refresh-" + account, AccountID: account, Expire: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
			if err != nil {
				t.Fatal(err)
			}
			ciphertext, err := keys.Encrypt(string(canonical))
			if err != nil {
				t.Fatal(err)
			}
			row := models.Credential{GroupID: group.ID, Data: ciphertext, Fingerprint: keys.Hash(string(canonical)), IdentityFingerprint: keys.Hash("identity|" + account), SecretVersion: 1, AuthState: models.CredentialAuthStateReady, Status: models.CredentialStatusActive}
			if err := db.Create(&row).Error; err != nil {
				t.Fatal(err)
			}
			host.fingerprints[account], host.credentialID[account] = row.IdentityFingerprint, row.ID
			params := json.RawMessage(`{"base_url":"https://chatgpt.com"}`)
			generation := stateloader.CredentialIdentityGeneration(row.IdentityFingerprint, "codex", "subscription", params)
			entries = append(entries, state.CredentialEntry{ID: row.ID, GroupID: row.GroupID, Version: row.SecretVersion, IdentityGeneration: generation, IdentityFingerprint: row.IdentityFingerprint, Fingerprint: row.Fingerprint, EncryptedValue: row.Data, AuthState: state.CredentialAuthStateReady, Status: state.CredentialStatusActive})
			input.Credentials = append(input.Credentials, state.CredentialConfig{ID: row.ID, GroupID: row.GroupID, Version: row.SecretVersion, IdentityGeneration: generation, Fingerprint: row.Fingerprint, Status: state.CredentialStatusActive})
			input.Groups = append(input.Groups, state.GroupConfig{ID: group.ID, Priority: int32(100 - index*100), Name: group.Name, ChannelID: channel.Codex, ConnectionType: "subscription", Params: params, Models: []state.ModelConfig{{ID: "gpt-5"}, {ID: channel.CodexLiveModelID}}, Settings: config.Settings{state.SettingCodexLiveMode: "direct"}, Proxy: &proxy, Enabled: true})
			input.AccessKeys = append(input.AccessKeys, state.AccessKeyConfig{ID: uint(index + 10), Name: account, KeyHash: keys.Hash("privacy-" + account), Status: state.AccessKeyStatusActive, Filters: state.FilterSet{Groups: map[uint]struct{}{group.ID: {}}}})
		}
		input.AccessKeys = append(input.AccessKeys, state.AccessKeyConfig{ID: 99, Name: "failover", KeyHash: keys.Hash("privacy-failover"), Status: state.AccessKeyStatusActive})
		if err := registry.ReplaceCredentials(entries); err != nil {
			t.Fatal(err)
		}
		if _, err := manager.Publish(input); err != nil {
			t.Fatal(err)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	return host
}

func privacyRequest(t *testing.T, host *privacyHost, account, payload string, headers http.Header) string {
	t.Helper()
	response, err := privacyDo(t.Context(), host, account, payload, headers)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if len(response.Header.Values("Set-Cookie")) != 0 {
		t.Fatal("routing cookies reached the downstream response")
	}
	body, err := io.ReadAll(response.Body)
	if err != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"status":"completed"`) {
		t.Fatalf("privacy HTTP status=%d body=%s error=%v", response.StatusCode, body, err)
	}
	return response.Header.Get("X-GPTLoad-Request-ID")
}

func privacyDo(ctx context.Context, host *privacyHost, account, payload string, headers http.Header) (*http.Response, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, host.server.URL+"/v1/responses", strings.NewReader(payload))
	if err != nil {
		return nil, err
	}
	request.Header = headers.Clone()
	if request.Header == nil {
		request.Header = make(http.Header)
	}
	request.Header.Set("Authorization", "Bearer privacy-"+account)
	request.Header.Set("Content-Type", "application/json")
	return host.server.Client().Do(request)
}

func privacyReceive(t *testing.T, requests <-chan privacyWireRequest) privacyWireRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
		return privacyWireRequest{}
	}
}

func privacyCheckRouting(t *testing.T, request privacyWireRequest, wantCookie, wantState string, handshake bool) {
	t.Helper()
	if request.Header.Get("Cookie") != wantCookie || request.Header.Get("Cookie2") != "" {
		t.Fatalf("%s routing cookies=%q/%q want=%q", request.Transport, request.Header.Get("Cookie"), request.Header.Get("Cookie2"), wantCookie)
	}
	var body struct {
		Metadata map[string]json.RawMessage `json:"client_metadata"`
	}
	if err := json.Unmarshal(request.Body, &body); err != nil {
		t.Fatal(err)
	}
	var state string
	if raw := body.Metadata["x-codex-turn-state"]; raw != nil {
		if err := json.Unmarshal(raw, &state); err != nil {
			t.Fatal(err)
		}
	}
	if state != wantState || (handshake && request.Header.Get("X-Codex-Turn-State") != wantState) {
		t.Fatalf("%s turn-state header/metadata=%q/%q want=%q", request.Transport, request.Header.Get("X-Codex-Turn-State"), state, wantState)
	}
}

func privacyWSCompleted(t *testing.T, connection *websocket.Conn) []byte {
	t.Helper()
	for {
		_, response, err := connection.Read(t.Context())
		if err != nil || strings.Contains(string(response), `"type":"error"`) {
			t.Fatalf("privacy WS response=%s error=%v", response, err)
		}
		if strings.Contains(string(response), "response.completed") {
			return response
		}
	}
}

func TestCodexPrivacyRealHostIdentity(t *testing.T) {
	if !privacyTrustedProcess(t) {
		return
	}
	requests := make(chan privacyWireRequest, 16)
	auxiliary := make(chan string, 8)
	liveOperations := make(chan string, 4)
	liveClosed := make(chan struct{}, 2)
	var sequence atomic.Int32
	var refresh atomic.Bool
	var failover atomic.Bool
	var refreshCalls atomic.Int32
	upstream, proxyURL := newPrivacyUpstream(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/backend-api/codex/realtime/calls":
			body, err := io.ReadAll(r.Body)
			if err != nil || !strings.Contains(string(body), "synthetic-live-offer") {
				t.Errorf("live offer=%s error=%v", body, err)
			}
			liveOperations <- "create"
			w.Header().Set("Location", "/v1/realtime/calls/rtc_privacy")
			w.Header().Set("Content-Type", "application/sdp")
			_, _ = io.WriteString(w, "v=0\r\nsynthetic-live-answer\r\n")
			return
		case "/v1/live/rtc_privacy":
			connection, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			liveOperations <- "sideband"
			defer func() { _ = connection.CloseNow(); liveClosed <- struct{}{} }()
			for {
				kind, message, err := connection.Read(r.Context())
				if err != nil || connection.Write(r.Context(), kind, message) != nil {
					return
				}
			}
		case "/v1/realtime/calls/rtc_privacy/hangup":
			liveOperations <- "hangup"
			w.WriteHeader(http.StatusNoContent)
			return
		}
		switch r.URL.Path {
		case "/backend-api/codex/models", "/backend-api/wham/usage", "/backend-api/subscriptions", "/backend-api/wham/rate-limit-reset-credits":
			auxiliary <- r.URL.Path
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/backend-api/codex/models":
				_, _ = io.WriteString(w, `{"models":[{"slug":"gpt-5"}]}`)
			case "/backend-api/wham/usage":
				_, _ = io.WriteString(w, `{"plan_type":"plus","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":18000}}}`)
			case "/backend-api/subscriptions":
				_, _ = io.WriteString(w, `{"active_until":"2030-12-01T00:00:00Z","will_renew":false}`)
			default:
				_, _ = io.WriteString(w, `{"available_count":0,"credits":[]}`)
			}
			return
		}
		if r.URL.Path == "/oauth/token" {
			refreshCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"access_token":"refreshed-account-a","refresh_token":"refresh-account-a","expires_in":3600,"token_type":"Bearer"}`)
			return
		}
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			account := r.Header.Get("Chatgpt-Account-Id")
			w.Header().Add("Set-Cookie", "__oailb=ws-"+account+"; Path=/; Secure; HttpOnly")
			w.Header().Set("X-Codex-Turn-State", "state-ws-handshake-"+account)
			connection, err := websocket.Accept(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer connection.CloseNow()
			for {
				_, payload, err := connection.Read(r.Context())
				if err != nil {
					return
				}
				requests <- privacyWireRequest{"websocket", r.Header.Clone(), payload}
				metadata := []byte(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"state-ws-metadata-` + account + `"}}`)
				if connection.Write(r.Context(), websocket.MessageText, metadata) != nil {
					return
				}
				completed := strings.TrimSpace(strings.TrimPrefix(observationCompleted, "data: "))
				completed = strings.ReplaceAll(completed, "resp_local", fmt.Sprintf("resp_privacy_ws_%d", sequence.Add(1)))
				if connection.Write(r.Context(), websocket.MessageText, []byte(completed)) != nil {
					return
				}
			}
		}
		payload, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		requests <- privacyWireRequest{"http", r.Header.Clone(), payload}
		account := r.Header.Get("Chatgpt-Account-Id")
		w.Header().Add("Set-Cookie", "__oailb=http-"+account+"; Path=/; Secure; HttpOnly")
		w.Header().Add("Set-Cookie", "auth_session=reject-this-cookie; Path=/; Secure")
		if !strings.Contains(string(payload), `"route_probe":"metadata"`) {
			w.Header().Set("X-Codex-Turn-State", "state-header-"+account)
		}
		if failover.CompareAndSwap(true, false) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, `{"error":{"code":"server_is_overloaded","message":"synthetic failover"}}`)
			return
		}
		if refresh.CompareAndSwap(true, false) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"error":{"code":"invalid_api_key","message":"synthetic token refresh"}}`)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, `data: {"type":"codex.response.metadata","headers":{"x-codex-turn-state":"state-http-metadata-`+account+`"}}`+"\n\n")
		_, _ = io.WriteString(w, strings.ReplaceAll(observationCompleted, "resp_local", fmt.Sprintf("resp_privacy_%d", sequence.Add(1))))
	}))
	host := newPrivacyHost(t, upstream, proxyURL)
	var recording string
	if binary := os.Getenv("RECORDER_BINARY"); binary != "" {
		var stop func()
		recording, stop = startStreamRecorder(t, binary, host.observationHost)
		defer stop()
	}
	const original = "4157fa6a-32f5-4f0e-8df7-bdc349b6e107"
	nested := `{"installation_id":"` + original + `","session_id":"` + strings.ToUpper(original) + `","thread_id":"` + original + `","window_id":"` + original + `:7","turn_id":"","marker":"preserve"}`
	metadata := map[string]any{"installation_id": original, "x-codex-installation-id": original, "x-codex-turn-metadata": nested, "x-codex-turn-state": "foreign-server-state", "session_id": original, "thread_id": original, "turn_id": original, "root_turn_id": original, "parent_thread_id": original, "context_window_id": "opaque-private", "forked_from_thread_id": "opaque-private", "conversation_id": "guardian:" + original, "marker": "preserve"}
	encoded, err := json.Marshal(map[string]any{"model": "gpt-5", "input": []any{map[string]string{"role": "user", "content": "keep " + original}}, "prompt_cache_key": original, "tools": []any{map[string]any{"type": "function", "name": "synthetic", "description": original, "parameters": map[string]any{"type": "object", "properties": map[string]any{"id": map[string]any{"type": "string", "enum": []string{original}}}}}, map[string]any{"type": "image_generation", "output_format": "png"}}, "client_metadata": metadata})
	if err != nil {
		t.Fatal(err)
	}
	payload := string(encoded)
	var originalBody map[string]any
	if err := json.Unmarshal(encoded, &originalBody); err != nil {
		t.Fatal(err)
	}
	headers := http.Header{"X-Codex-Installation-Id": {original}, "X-Codex-Turn-Metadata": {nested}, "Session-Id": {original}, "X-Client-Request-Id": {original}, "X-Codex-Turn-State": {"foreign-server-state"}, "Cookie": {"__oailb=foreign-cookie; auth_session=foreign-auth"}, "Cookie2": {"foreign-cookie-two"}}
	identities := map[string]string{}
	clientIDs := map[string]string{}
	check := func(request privacyWireRequest, account string) {
		t.Helper()
		identity, err := codexprivacy.NewCache().Resolve(host.fingerprints[account])
		if err != nil {
			t.Fatal(err)
		}
		want := identity.InstallationID
		if request.Header.Get("Chatgpt-Account-Id") != account || request.Header.Get("X-Codex-Installation-Id") != want {
			t.Fatalf("%s upstream account/install=%q/%q want=%q/%q", request.Transport, request.Header.Get("Chatgpt-Account-Id"), request.Header.Get("X-Codex-Installation-Id"), account, want)
		}
		var body struct {
			Input    json.RawMessage            `json:"input"`
			Tools    json.RawMessage            `json:"tools"`
			CacheKey string                     `json:"prompt_cache_key"`
			Metadata map[string]json.RawMessage `json:"client_metadata"`
		}
		if err := json.Unmarshal(request.Body, &body); err != nil {
			t.Fatal(err)
		}
		for _, field := range []string{"installation_id", "x-codex-installation-id"} {
			var got string
			if err := json.Unmarshal(body.Metadata[field], &got); err != nil || got != want {
				t.Fatalf("%s metadata %s=%s want=%s error=%v", request.Transport, field, body.Metadata[field], want, err)
			}
		}
		get := func(field string) string {
			t.Helper()
			var value string
			if err := json.Unmarshal(body.Metadata[field], &value); err != nil {
				t.Fatalf("metadata %s: %v", field, err)
			}
			return value
		}
		mapped := get("thread_id")
		if mapped == "" || mapped == original || (clientIDs[account] != "" && clientIDs[account] != mapped) {
			t.Fatalf("unstable client identity for %s: %q", account, mapped)
		}
		clientIDs[account] = mapped
		for _, field := range []string{"session_id", "turn_id", "root_turn_id", "parent_thread_id"} {
			if get(field) != mapped {
				t.Fatalf("metadata relationship %s=%s want=%s", field, get(field), mapped)
			}
		}
		if get("context_window_id") == "opaque-private" || get("context_window_id") != get("forked_from_thread_id") || get("conversation_id") != "guardian:"+mapped {
			t.Fatalf("opaque/composite metadata relationships: %s", request.Body)
		}
		session := request.Header.Get("Session-Id")
		if session == "" {
			session = request.Header.Get("Session_id")
		}
		if session != mapped || request.Header.Get("X-Client-Request-Id") != mapped || body.CacheKey != mapped {
			t.Fatalf("header/cache identity relationships session=%q request=%q cache=%q want=%q", session, request.Header.Get("X-Client-Request-Id"), body.CacheKey, mapped)
		}
		var nested string
		if err := json.Unmarshal(body.Metadata["x-codex-turn-metadata"], &nested); err != nil {
			t.Fatal(err)
		}
		for _, raw := range []string{nested, request.Header.Get("X-Codex-Turn-Metadata")} {
			var metadata map[string]string
			if err := json.Unmarshal([]byte(raw), &metadata); err != nil || metadata["installation_id"] != want || metadata["marker"] != "preserve" || metadata["session_id"] != mapped || metadata["thread_id"] != mapped || metadata["window_id"] != mapped+":7" || metadata["turn_id"] != "" {
				t.Fatalf("nested installation=%s error=%v", raw, err)
			}
		}
		var input, tools any
		if err := json.Unmarshal(body.Input, &input); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(body.Tools, &tools); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(input, originalBody["input"]) || !reflect.DeepEqual(tools, originalBody["tools"]) || string(body.Metadata["marker"]) != `"preserve"` {
			t.Fatalf("unrelated content changed: %s", request.Body)
		}
		identities[account] = want
	}
	for _, test := range []struct {
		account, cookie, turnState string
		stream                     bool
	}{{"account-a", "", "", false}, {"account-a", "__oailb=http-account-a", "state-header-account-a", true}, {"account-b", "", "", false}} {
		body := payload
		if test.stream {
			body = strings.Replace(body, `{`, `{"stream":true,`, 1)
		}
		privacyRequest(t, host, test.account, body, headers)
		request := privacyReceive(t, requests)
		check(request, test.account)
		privacyCheckRouting(t, request, test.cookie, test.turnState, true)
	}
	if identities["account-a"] == identities["account-b"] || identities["account-a"] == original || clientIDs["account-a"] == clientIDs["account-b"] {
		t.Fatal("account installation identities did not separate")
	}
	failover.Store(true)
	retriedID := privacyRequest(t, host, "failover", payload, headers)
	for _, account := range []string{"account-a", "account-b"} {
		request := privacyReceive(t, requests)
		check(request, account)
		privacyCheckRouting(t, request, "__oailb=http-"+account, "state-header-"+account, true)
	}
	if recording != "" {
		for _, attempt := range []int64{1, 2} {
			waitRecordedMatch(t, recording, func(event map[string]any) bool {
				data, _ := event["data"].(map[string]any)
				return event["type"] == "upstream.request" && data["request_id"] == retriedID && data["attempt_id"] == fmt.Sprintf("%s:%d", retriedID, attempt) && data["attempt_sequence"] == attempt && data["exchange_sequence"] == int64(1)
			})
		}
	}
	wsHeaders := headers.Clone()
	wsHeaders.Set("Authorization", "Bearer privacy-account-a")
	connection, _, err := websocket.Dial(t.Context(), strings.Replace(host.server.URL, "https:", "wss:", 1)+"/v1/responses", &websocket.DialOptions{HTTPClient: host.server.Client(), HTTPHeader: wsHeaders})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.CloseNow() })
	var previousResponse string
	for turn := 0; turn < 2; turn++ {
		message := strings.Replace(payload, `{`, `{"type":"response.create",`, 1)
		if turn == 1 {
			message = strings.Replace(message, `"input":`, `"previous_response_id":"`+previousResponse+`","input":`, 1)
		}
		if err := connection.Write(t.Context(), websocket.MessageText, []byte(message)); err != nil {
			t.Fatal(err)
		}
		response := privacyWSCompleted(t, connection)
		request := privacyReceive(t, requests)
		check(request, "account-a")
		privacyCheckRouting(t, request, "__oailb=http-account-a", "state-header-account-a", true)
		if turn == 1 && !strings.Contains(string(request.Body), `"previous_response_id":"`+previousResponse+`"`) {
			t.Fatalf("server response ID changed: %s", request.Body)
		}
		var terminal struct{ Response struct{ ID string } }
		if err := json.Unmarshal(response, &terminal); err != nil {
			t.Fatal(err)
		}
		previousResponse = terminal.Response.ID
	}
	const freshWSTurn = "b00f5cb4-100a-442b-a8aa-584da8930ac0"
	freshWS := strings.Replace(payload, `"turn_id":"`+original+`"`, `"turn_id":"`+freshWSTurn+`"`, 1)
	for _, state := range []string{"", "state-ws-metadata-account-a"} {
		if err := connection.Write(t.Context(), websocket.MessageText, []byte(strings.Replace(freshWS, `{`, `{"type":"response.create",`, 1))); err != nil {
			t.Fatal(err)
		}
		privacyWSCompleted(t, connection)
		privacyCheckRouting(t, privacyReceive(t, requests), "__oailb=http-account-a", state, false)
	}
	_ = connection.CloseNow()
	if recording != "" {
		waitRecordedMatch(t, recording, func(event map[string]any) bool {
			data, _ := event["data"].(map[string]any)
			return event["type"] == "upstream.stream" && data["transport"] == "websocket" && data["upstream_connection_id"] != nil && data["upstream_connection_id"] != "" && data["attempt_id"] != nil
		})
	}
	refresh.Store(true)
	privacyRequest(t, host, "account-a", payload, headers)
	for _, cookie := range []string{"__oailb=ws-account-a", "__oailb=http-account-a"} {
		request := privacyReceive(t, requests)
		check(request, "account-a")
		privacyCheckRouting(t, request, cookie, "state-header-account-a", true)
	}
	if refreshCalls.Load() != 1 {
		t.Fatalf("refresh calls=%d", refreshCalls.Load())
	}
	var stored models.Credential
	if err := host.db.First(&stored, host.credentialID["account-a"]).Error; err != nil || stored.SecretVersion != 2 || stored.IdentityFingerprint != host.fingerprints["account-a"] {
		t.Fatalf("refreshed durable identity version=%d error=%v", stored.SecretVersion, err)
	}
	metadataProbe := map[string]any{}
	if err := json.Unmarshal(encoded, &metadataProbe); err != nil {
		t.Fatal(err)
	}
	probeFields := metadataProbe["client_metadata"].(map[string]any)
	probeFields["turn_id"] = "1fc2405d-6bad-405c-84e7-fc94d35c4966"
	probeFields["route_probe"] = "metadata"
	probe, err := json.Marshal(metadataProbe)
	if err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"", "state-http-metadata-account-a"} {
		privacyRequest(t, host, "account-a", string(probe), headers)
		privacyCheckRouting(t, privacyReceive(t, requests), "__oailb=http-account-a", state, true)
	}
	parallel := make(chan error, 2)
	for _, body := range []string{payload, string(probe)} {
		go func() {
			response, err := privacyDo(t.Context(), host, "account-a", body, headers)
			if err == nil {
				var content []byte
				content, err = io.ReadAll(response.Body)
				_ = response.Body.Close()
				if err == nil && (response.StatusCode != http.StatusOK || !strings.Contains(string(content), `"status":"completed"`)) {
					err = fmt.Errorf("parallel turn response=%d %s", response.StatusCode, content)
				}
			}
			parallel <- err
		}()
	}
	for range 2 {
		if err := <-parallel; err != nil {
			t.Fatal(err)
		}
		request := privacyReceive(t, requests)
		state := "state-header-account-a"
		if strings.Contains(string(request.Body), `"route_probe":"metadata"`) {
			state = "state-http-metadata-account-a"
		}
		privacyCheckRouting(t, request, "__oailb=http-account-a", state, true)
	}
	for _, path := range []string{fmt.Sprintf("/api/groups/%d/models/discover", stored.GroupID), fmt.Sprintf("/api/groups/%d/credentials/%d/observation-refresh", stored.GroupID, stored.ID)} {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, host.server.URL+path, strings.NewReader(`{}`))
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Authorization", "Bearer management-test-key")
		request.Header.Set("Content-Type", "application/json")
		response, err := host.server.Client().Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK {
			t.Fatalf("auxiliary host operation %s status=%d body=%s error=%v", path, response.StatusCode, body, err)
		}
	}
	covered := map[string]bool{}
	for range 4 {
		select {
		case path := <-auxiliary:
			covered[path] = true
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	if len(covered) != 4 {
		t.Fatalf("auxiliary endpoint coverage=%v", covered)
	}
	liveRequest, err := http.NewRequestWithContext(t.Context(), http.MethodPost, host.server.URL+"/v1/live", strings.NewReader("v=0\r\nsynthetic-live-offer\r\n"))
	if err != nil {
		t.Fatal(err)
	}
	liveRequest.Header.Set("Authorization", "Bearer privacy-account-a")
	liveRequest.Header.Set("Content-Type", "application/sdp")
	liveResponse, err := host.server.Client().Do(liveRequest)
	if err != nil {
		t.Fatal(err)
	}
	answer, err := io.ReadAll(liveResponse.Body)
	_ = liveResponse.Body.Close()
	if err != nil || liveResponse.StatusCode != http.StatusCreated || string(answer) != "v=0\r\nsynthetic-live-answer\r\n" {
		t.Fatalf("live create status=%d answer=%s error=%v", liveResponse.StatusCode, answer, err)
	}
	liveLocation := liveResponse.Header.Get("Location")
	for range 2 {
		connection, _, err := websocket.Dial(t.Context(), strings.Replace(host.server.URL, "https:", "wss:", 1)+liveLocation, &websocket.DialOptions{HTTPClient: host.server.Client(), HTTPHeader: http.Header{"Authorization": {"Bearer privacy-account-a"}}})
		if err != nil {
			t.Fatal(err)
		}
		const control = `{"type":"session.update","session":{"instructions":"synthetic-live-control"}}`
		if err := connection.Write(t.Context(), websocket.MessageText, []byte(control)); err != nil {
			t.Fatal(err)
		}
		_, echoed, err := connection.Read(t.Context())
		if err != nil || string(echoed) != control {
			t.Fatalf("live sideband echo=%s error=%v", echoed, err)
		}
		_ = connection.Close(websocket.StatusNormalClosure, "acceptance reconnect")
		select {
		case <-liveClosed:
		case <-t.Context().Done():
			t.Fatal(t.Context().Err())
		}
	}
	hangup, err := http.NewRequestWithContext(t.Context(), http.MethodPost, host.server.URL+liveLocation+"/hangup", nil)
	if err != nil {
		t.Fatal(err)
	}
	hangup.Header.Set("Authorization", "Bearer privacy-account-a")
	hungup, err := host.server.Client().Do(hangup)
	if err != nil {
		t.Fatal(err)
	}
	_ = hungup.Body.Close()
	if hungup.StatusCode != http.StatusNoContent {
		t.Fatalf("live hangup status=%d", hungup.StatusCode)
	}
	for _, want := range []string{"create", "sideband", "sideband", "hangup"} {
		if operation := <-liveOperations; operation != want {
			t.Fatalf("live operation=%q want=%q", operation, want)
		}
	}
	restarted := newPrivacyHost(t, upstream, proxyURL)
	privacyRequest(t, restarted, "account-a", payload, headers)
	request := privacyReceive(t, requests)
	check(request, "account-a")
	privacyCheckRouting(t, request, "", "", true)
}
