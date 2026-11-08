package embedded

import (
	"context"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
)

// CodexClaudeSessionID reads only the existing Claude Code session-routing
// source so the host can scope it before CPA creates Codex cache/session IDs.
func CodexClaudeSessionID(ctx context.Context, payload []byte, headers http.Header) string {
	return helps.ExtractClaudeCodeSessionID(ctx, payload, headers)
}
