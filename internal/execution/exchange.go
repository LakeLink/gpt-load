package execution

import (
	"net/http"
	"time"

	"gpt-load/internal/protocol"
)

// UpstreamExchange 是执行器提供的单次传输事实。Header 和 Body 由生产者提供独立快照。
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
