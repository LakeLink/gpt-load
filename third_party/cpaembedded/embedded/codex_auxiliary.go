package embedded

import (
	"context"
	"net/http"

	"github.com/google/uuid"

	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// NewCodexAuxiliaryHTTPClient uses the same pinned rustls transport as Codex
// inference for OAuth, account and model-discovery operations. The embedding
// host supplies its frozen network policy; explicit context transport injection
// retains ownership of its connection and proxy routing. Account scopes isolate
// TLS session caches; operations before account discovery use a fresh scope.
func NewCodexAuxiliaryHTTPClient(ctx context.Context, account, proxyURL string, proxyFromEnvironment bool) *http.Client {
	var transport http.RoundTripper
	if ctx != nil {
		transport, _ = ctx.Value("cliproxy.roundtripper").(http.RoundTripper)
	}
	if transport == nil {
		if account == "" {
			account = uuid.NewString()
		}
		transport = executionRoundTripper(ctx, nil, &cliproxyauth.Auth{ID: "gptload-codex-control/" + account, Provider: ProviderCodex, ProxyURL: proxyURL}, proxyFromEnvironment)
	}
	return clientWithoutRedirects(&http.Client{Transport: transport})
}

func codexAuxiliaryHTTPClient(ctx context.Context, source *http.Client) *http.Client {
	if source == nil {
		return NewCodexAuxiliaryHTTPClient(ctx, "", "", true)
	}
	clone := *source
	if clone.Transport == nil {
		clone.Transport = NewCodexAuxiliaryHTTPClient(ctx, "", "", true).Transport
	}
	return clientWithoutRedirects(&clone)
}
