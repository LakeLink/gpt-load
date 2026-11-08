package embedded

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/gptload-embedded/modelcatalog"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/tidwall/sjson"
)

// 沿用 CPA 的客户端标识格式，版本统一由 GPT-Load 的已验证版本集固定。
const codexUserAgent = "codex-tui/" + CodexClientVersion + " (Mac OS 26.5.2; arm64) iTerm.app/3.6.11 (codex-tui; " + CodexClientVersion + ")"

func init() {
	// CPA 在 WebSocket 握手的最后应用模型专属头；同步其中的旧版本标识。
	var overrides []*registry.ModelInfo
	if err := json.Unmarshal(modelcatalog.JSON(), &overrides); err != nil {
		panic(err)
	}
	for _, model := range overrides {
		if model == nil {
			continue
		}
		// 官方客户端契约决定可发送档位，避免 CPA 快照遗漏档位而在本地拒绝请求。
		if levels := modelcatalog.ReasoningLevels(model.ID); len(levels) > 0 && model.Thinking != nil {
			model.Thinking.Levels = levels
		}
		if model.Config == nil {
			continue
		}
		for name := range model.Config.OverrideHeader {
			switch {
			case strings.EqualFold(name, "User-Agent"):
				model.Config.OverrideHeader[name] = codexUserAgent
			case strings.EqualFold(name, "Version"):
				model.Config.OverrideHeader[name] = CodexClientVersion
			}
		}
	}
	if len(overrides) > 0 {
		registry.GetGlobalRegistry().RegisterClient("gptload-codex-identity", ProviderCodex, overrides)
	}
}

// codexHeadersRoundTripper 固定客户端版本标识及 HTTP 会话头。
type codexHeadersRoundTripper struct {
	base       http.RoundTripper
	source     http.Header
	configured []string
}

func (transport codexHeadersRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	for _, name := range transport.configured {
		name = http.CanonicalHeaderKey(name)
		switch name {
		case "Originator":
			value, present := codexHeaderValue(transport.source, name)
			if present {
				request.Header.Set(name, value)
			} else {
				request.Header.Del(name)
			}
		}
	}
	request.Header.Set("Version", CodexClientVersion)
	request.Header.Set("User-Agent", codexUserAgent)
	normalizeCodexSessionHeader(request.Header)
	// CPA 的直接图片路径不读取 opts.Headers，补回调用者显式提供的会话。
	if request.Header.Get("Session-Id") == "" {
		if session := transport.source.Get("Session-Id"); session != "" {
			request.Header.Set("Session-Id", session)
		}
	}
	// The host freezes this account identity before constructing the CPA request.
	// Reassert it after CPA and model-specific header overrides.
	if installation := transport.source.Get("X-Codex-Installation-Id"); installation != "" {
		request.Header.Set("X-Codex-Installation-Id", installation)
		// Converted protocols may reconstruct the Responses body. Restore this
		// required account installation copy after translation, without deriving IDs
		// again or touching message/tool fields.
		if request.Body != nil && (strings.HasSuffix(request.URL.Path, "/responses") || strings.HasSuffix(request.URL.Path, "/alpha/search")) {
			body, err := io.ReadAll(request.Body)
			closeErr := request.Body.Close()
			if err != nil || closeErr != nil {
				return nil, errors.New("Codex privacy: read final request metadata")
			}
			body, err = sjson.SetBytes(body, "client_metadata.x-codex-installation-id", installation)
			if err != nil {
				return nil, errors.New("Codex privacy: write final request metadata")
			}
			request.Body = io.NopCloser(bytes.NewReader(body))
			request.ContentLength = int64(len(body))
			request.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
		}
		if metadata := transport.source.Values("X-Codex-Turn-Metadata"); len(metadata) > 0 {
			request.Header["X-Codex-Turn-Metadata"] = append([]string(nil), metadata...)
		}
	}
	hooks := cliproxyexecutor.CodexHeaderHooksFromContext(request.Context())
	if hooks.Prepare != nil {
		if err := hooks.Prepare(request); err != nil {
			return nil, err
		}
	}
	response, err := transport.base.RoundTrip(request)
	if hooks.Observe != nil && response != nil {
		hooks.Observe(request.URL, response.Header.Clone())
	}
	return response, err
}

func normalizedCodexHeaders(headers http.Header) http.Header {
	cloned := headers.Clone()
	if cloned == nil {
		cloned = make(http.Header)
	}
	// 仅 Codex 忽略客户端及分组规则提供的版本身份。
	for name := range cloned {
		if strings.EqualFold(name, "User-Agent") || strings.EqualFold(name, "Version") {
			delete(cloned, name)
		}
	}
	cloned.Set("Version", CodexClientVersion)
	cloned.Set("User-Agent", codexUserAgent)
	normalizeCodexSessionHeader(cloned)
	return cloned
}

// normalizeCodexSessionHeader 兼容下划线拼法；同时存在时以连字符字段为准。
func normalizeCodexSessionHeader(headers http.Header) {
	session, _ := codexHeaderValue(headers, "Session-Id")
	session = strings.TrimSpace(session)
	if session == "" {
		session, _ = codexHeaderValue(headers, "Session_id")
		session = strings.TrimSpace(session)
	}
	for name := range headers {
		if strings.EqualFold(name, "Session-Id") || strings.EqualFold(name, "Session_id") {
			delete(headers, name)
		}
	}
	if session != "" {
		headers.Set("Session-Id", session)
	}
}

func codexHeaderValue(headers http.Header, name string) (string, bool) {
	values, present := headers[http.CanonicalHeaderKey(name)]
	if !present {
		for key, candidate := range headers {
			if strings.EqualFold(key, name) {
				values, present = candidate, true
				break
			}
		}
	}
	if len(values) > 0 {
		return values[0], present
	}
	return "", present
}
