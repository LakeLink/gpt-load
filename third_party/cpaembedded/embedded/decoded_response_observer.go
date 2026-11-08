package embedded

import (
	"context"
	"io"
)

type decodedResponseObserverKey struct{}

// WithDecodedResponseObserver wraps successful Codex response reads after
// decompression and before parsing. The wrapper must preserve bytes and errors.
func WithDecodedResponseObserver(ctx context.Context, wrap func(io.ReadCloser) io.ReadCloser) context.Context {
	return context.WithValue(ctx, decodedResponseObserverKey{}, wrap)
}
