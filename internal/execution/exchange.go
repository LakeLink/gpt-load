package execution

import (
	"net/http"
	"time"

	"gpt-load/internal/protocol"
)

// UpstreamExchange 是执行器同步提供的单次传输事实。
// Header 和 Body 为只读借用，仅在观察回调期间有效；保留数据的消费者负责构造快照。
type UpstreamExchange struct {
	Kind                 string
	Sequence             uint64
	Phase                string
	ObservedAt           time.Time
	Method               string
	URL                  string
	Header               http.Header
	Body                 []byte
	StatusCode           int
	Protocol             protocol.Protocol
	UpstreamConnectionID string
}
