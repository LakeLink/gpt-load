package codex

import (
	"context"
	"io"

	cpaembedded "github.com/router-for-me/CLIProxyAPI/v8/gptload-embedded/embedded"
)

func WithDecodedResponseObserver(ctx context.Context, wrap func(io.ReadCloser) io.ReadCloser) context.Context {
	return cpaembedded.WithDecodedResponseObserver(ctx, wrap)
}
