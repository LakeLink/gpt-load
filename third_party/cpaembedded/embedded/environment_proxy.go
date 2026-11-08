package embedded

import (
	"context"
	"errors"
	"net/http"
	"net/url"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/proxyutil"
)

type environmentProxyRoundTripper struct {
	resolveProxy    func(*http.Request) (*url.URL, error)
	transportForURL func(string) http.RoundTripper
}

func executionRoundTripper(
	ctx context.Context,
	cfg *internalconfig.Config,
	auth *cliproxyauth.Auth,
	proxyFromEnvironment bool,
) http.RoundTripper {
	if proxyFromEnvironment {
		return newEnvironmentProxyRoundTripper(cfg, auth)
	}
	client := executionHTTPClient(ctx, cfg, auth)
	if client.Transport != nil {
		return client.Transport
	}
	return http.DefaultTransport
}

func newEnvironmentProxyRoundTripper(cfg *internalconfig.Config, auth *cliproxyauth.Auth) *environmentProxyRoundTripper {
	return &environmentProxyRoundTripper{
		resolveProxy: http.ProxyFromEnvironment,
		transportForURL: func(proxyURL string) http.RoundTripper {
			routed := &cliproxyauth.Auth{ProxyURL: proxyURL}
			if auth != nil {
				routed = auth.Clone()
				routed.ProxyURL = proxyURL
			}
			client := executionHTTPClient(nil, cfg, routed)
			return client.Transport
		},
	}
}

func (transport *environmentProxyRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || request == nil || transport.resolveProxy == nil || transport.transportForURL == nil {
		return nil, errors.New("resolve environment proxy")
	}
	proxyURL, err := transport.resolveProxy(request)
	if err != nil {
		return nil, errors.New("resolve environment proxy")
	}
	setting := "direct"
	if proxyURL != nil {
		setting = proxyURL.String()
		parsed, parseErr := proxyutil.Parse(setting)
		if parseErr != nil || parsed.Mode != proxyutil.ModeProxy {
			return nil, errors.New("resolve environment proxy")
		}
	}
	selected := transport.transportForURL(setting)
	if selected == nil {
		return nil, errors.New("initialize environment proxy transport")
	}
	return selected.RoundTrip(request)
}

func executionHTTPClient(ctx context.Context, cfg *internalconfig.Config, auth *cliproxyauth.Auth) *http.Client {
	if auth != nil && auth.Provider == ProviderCodex {
		return helps.NewCodexHTTPClient(ctx, cfg, auth, 0)
	}
	return helps.NewUtlsHTTPClient(ctx, cfg, auth, 0)
}
