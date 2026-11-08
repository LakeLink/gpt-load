package cpa

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"gpt-load/internal/codexprivacy"
	"gpt-load/internal/execution"
	"gpt-load/internal/subscription/providers/codex"
)

func (a *Adapter) prepareCodexPrivacy(spec execution.AttemptSpec, payload []byte, headers http.Header) ([]byte, http.Header, error) {
	identity, err := a.privacy.Resolve(spec.Credential.IdentityFingerprint())
	if err != nil {
		return nil, nil, err
	}
	if strings.HasPrefix(strings.ToLower(headers.Get("Content-Type")), "multipart/") {
		_, preparedHeaders, err := identity.Prepare(nil, headers)
		return append([]byte(nil), payload...), preparedHeaders, err
	}
	return identity.Prepare(payload, headers)
}

func codexPrivacyEvidence(err error) *execution.ErrorEvidence {
	if errors.Is(err, codexprivacy.ErrMissingIdentityFingerprint) {
		return &execution.ErrorEvidence{Kind: execution.ErrorKindInternal, OriginHint: execution.ErrorOriginInternal, ScopeHint: execution.ErrorScopeCredential, Code: "codex_account_identity_missing", Summary: "Codex account identity fingerprint is missing"}
	}
	evidence := requestValidationEvidence(err)
	evidence.Code = "codex_privacy_prepare_failed"
	return evidence
}

func (a *Adapter) scopeCodexCacheIdentity(ctx context.Context, spec execution.AttemptSpec, request *providerRequest) error {
	identity, err := a.privacy.Resolve(spec.Credential.IdentityFingerprint())
	if err != nil {
		return err
	}
	request.ContinuityKey = identity.AffinityID(strings.TrimSpace(request.ContinuityKey))
	if request.Format == "claude" {
		if source := codex.ClaudeSessionID(ctx, request.Payload, request.Headers); source != "" {
			if request.Headers == nil {
				request.Headers = make(http.Header)
			}
			request.Headers.Set("X-Claude-Code-Session-Id", source)
		}
	}
	return nil
}
