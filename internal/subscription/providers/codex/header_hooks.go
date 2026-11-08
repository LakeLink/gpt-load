package codex

import (
	"context"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type HeaderHooks = cliproxyexecutor.CodexHeaderHooks

func WithHeaderHooks(ctx context.Context, hooks HeaderHooks) context.Context {
	return cliproxyexecutor.WithCodexHeaderHooks(ctx, hooks)
}
