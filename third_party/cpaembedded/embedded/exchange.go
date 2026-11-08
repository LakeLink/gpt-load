package embedded

import (
	"context"
	"net/http"
	"time"
)

// ExchangeObservation 借用 HTTP/WS 观察点已有的头部和正文，回调同步调用。
// Header 和 Body 为只读借用，仅在回调期间有效；保留数据的消费者负责在回调内构造快照。
// HTTP 请求位于 version/session 头部装配前；WS 请求位于 SDK 头部和帧装配前。
// Sequence 属于一次执行，内部重发递增，请求及其响应共享该值。
type ExchangeObservation struct {
	Stream               *StreamObservation
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

// StreamObservation describes borrowed application bytes or one direction's end.
// Body is carried by ExchangeObservation and is valid during the synchronous callback.
type StreamObservation struct {
	Transport          string
	Direction          string
	Kind               string
	StreamSequence     uint64
	ByteOffset         uint64
	BodySize           uint64
	LastStreamSequence uint64
	TotalBodyBytes     uint64
	MessageType        string
	WriteResult        string
	EndReason          string
	ErrorCode          string
	CloseCode          int
	ConnectionOnly     bool
}

type codexWSTurnObserverKey struct{}

// WithCodexWSTurnObserver 将观察者绑定至当前轮次；Session 不保存请求身份。
func WithCodexWSTurnObserver(ctx context.Context, observer func(ExchangeObservation)) context.Context {
	return context.WithValue(ctx, codexWSTurnObserverKey{}, observer)
}
