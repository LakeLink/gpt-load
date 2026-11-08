package cpa

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"gpt-load/internal/codexprivacy"
	"gpt-load/internal/execution"
	"gpt-load/internal/subscription/providers/codex"
)

func codexRouteScope(spec execution.AttemptSpec, baseURL string) string {
	target, _ := json.Marshal(spec.TargetConfig)
	return fmt.Sprintf("%d/%s/%s/%s", spec.Credential.IdentityGeneration, baseURL, spec.ProxyFingerprint, target)
}

func (a *Adapter) bindCodexRouting(ctx context.Context, owner, scope, requestID string, original []byte, originalHeaders http.Header, payload []byte, headers http.Header) (context.Context, []byte, *codexprivacy.Turn, error) {
	turnID, err := codexprivacy.OriginalTurn(original, originalHeaders, requestID)
	if err != nil {
		return ctx, nil, nil, err
	}
	turn := a.turns.Acquire(owner+"/"+scope, turnID)
	// Freeze replay once so concurrent first-value capture cannot make header
	// and body copies disagree within this attempt. Later attempts see new state.
	replay := turn.Value()
	if !strings.HasPrefix(strings.ToLower(headers.Get("Content-Type")), "multipart/") {
		payload, err = codexprivacy.PrepareTurnMetadata(payload, replay)
		if err != nil {
			return ctx, nil, nil, err
		}
	}
	codexprivacy.PrepareTurnHeaders(headers, replay)
	jar := a.cookies.Lease(owner, scope)
	frozenHeaders := headers.Clone()
	ctx = codex.WithHeaderHooks(ctx, codex.HeaderHooks{
		Prepare: func(request *http.Request) error {
			codexprivacy.ApplyClientHeaders(request.Header, frozenHeaders)
			codexprivacy.PrepareTurnHeaders(request.Header, replay)
			return jar.Prepare(request)
		},
		Observe: func(target *url.URL, headers http.Header) {
			jar.Observe(target, headers)
			turn.ObserveHeaders(headers)
		},
	})
	ctx = codex.WithDecodedResponseObserver(ctx, turn.ObserveSSE)
	return ctx, payload, turn, nil
}
