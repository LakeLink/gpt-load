package codex

import (
	"context"
	"net/http"

	cpaembedded "github.com/router-for-me/CLIProxyAPI/v8/gptload-embedded/embedded"
)

func ClaudeSessionID(ctx context.Context, payload []byte, headers http.Header) string {
	return cpaembedded.CodexClaudeSessionID(ctx, payload, headers)
}
