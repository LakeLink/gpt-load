package embedded

import (
	"net/http"
	"time"
)

// ExchangeObservation 保存现有 HTTP 观察点的头部快照，回调同步调用。
// 请求观察位于 Codex 的 version/session 头部装配前；此处没有已持有的正文快照。
// Sequence 属于一次执行，内部重发递增，请求及其响应共享该值。
type ExchangeObservation struct {
	Kind       string
	Sequence   uint64
	Phase      string
	ObservedAt time.Time
	Method     string
	URL        string
	Path       string
	Header     http.Header
	StatusCode int
}
