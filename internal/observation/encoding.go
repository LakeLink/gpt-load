package observation

import (
	"errors"
	"maps"

	"github.com/ugorji/go/codec"
)

// 大小探测只累计编码器输出长度，正文沿用源切片，最终投影才分配编码存储。
type encodedSize int

func (size *encodedSize) Write(data []byte) (int, error) {
	*size += encodedSize(len(data))
	return len(data), nil
}

var errEncodedBudget = errors.New("encoded observation exceeds queue budget")

var eventHandle = &codec.MsgpackHandle{WriteExt: true, PositiveIntUnsigned: true, BasicHandle: codec.BasicHandle{EncodeOptions: codec.EncodeOptions{WriterBufferSize: 4096}}}

type encodedObservation struct {
	bytes            []byte
	event            Event
	originalBytes    int
	bodyOmittedBytes int
	finalBytes       int
	err              error
}

func observationSize(event Event) (int, error) {
	var size encodedSize
	err := codec.NewEncoder(&size, eventHandle).Encode(event)
	return int(size), err
}

func encodeObservation(event Event, threshold, budget int) (result encodedObservation) {
	result.event = event
	result.originalBytes, result.err = observationSize(result.event)
	result.finalBytes = result.originalBytes
	if result.err != nil {
		return
	}
	if result.finalBytes > threshold {
		result.event.Data, result.event.BodyOmitted = omitTransportFields(result.event.Data, false)
		result.bodyOmittedBytes = result.originalBytes
		if result.event.BodyOmitted {
			result.bodyOmittedBytes, result.err = observationSize(result.event)
		}
		result.finalBytes = result.bodyOmittedBytes
		if result.err != nil {
			return
		}
		if result.finalBytes > threshold {
			result.event.OriginalType, result.event.Type = event.Type, "oversized"
			result.event.Data, _ = omitTransportFields(result.event.Data, true)
			result.finalBytes, result.err = observationSize(result.event)
			if result.err != nil {
				return
			}
		}
	}
	if result.finalBytes > budget {
		result.err = errEncodedBudget
		return
	}
	result.bytes = make([]byte, 0, result.finalBytes)
	result.err = codec.NewEncoderBytes(&result.bytes, eventHandle).Encode(result.event)
	return
}

// 当前通用过程和结果投影使用平面 map；只复制待删字段所在的 map。
// 其余嵌套元数据、头部和正文在一次同步发布内保持只读并共享。
func omitTransportFields(data any, headers bool) (any, bool) {
	fields, ok := data.(map[string]any)
	if !ok {
		return data, false
	}
	var projected map[string]any
	bodyOmitted := false
	for _, key := range []string{"body", "request_body", "response_body", "headers", "request_headers", "response_headers"} {
		value, present := fields[key]
		if !present {
			continue
		}
		isHeader := key == "headers" || key == "request_headers" || key == "response_headers"
		if isHeader && !headers {
			continue
		}
		if !isHeader {
			if value == nil {
				continue
			}
			if body, ok := value.([]byte); ok && body == nil {
				continue
			}
			bodyOmitted = true
		}
		if projected == nil {
			projected = maps.Clone(fields)
		}
		delete(projected, key)
	}
	if projected == nil {
		return data, bodyOmitted
	}
	return projected, bodyOmitted
}
