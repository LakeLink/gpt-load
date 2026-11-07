package embedded

import (
	"context"
	"net/http"
	"time"
)

// ExchangeObservation 保存 HTTP/WS 观察点已有的头部和正文快照，回调同步调用。
// HTTP 请求位于 version/session 头部装配前；WS 请求位于 SDK 头部和帧装配前。
// Sequence 属于一次执行，内部重发递增，请求及其响应共享该值。
type ExchangeObservation struct {
	UpstreamConnectionID string
	Body                 []byte
	Kind                 string
	Sequence             uint64
	Phase                string
	ObservedAt           time.Time
	Method               string
	URL                  string
	Path                 string
	Header               http.Header
	StatusCode           int
}

type codexWSTurnObserverKey struct{}

// WithCodexWSTurnObserver 将观察者绑定至当前轮次；Session 不保存请求身份。
func WithCodexWSTurnObserver(ctx context.Context, observer func(ExchangeObservation)) context.Context {
	return context.WithValue(ctx, codexWSTurnObserverKey{}, observer)
}
