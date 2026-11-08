package codexprivacy

import (
	"bytes"
	"io"
)

// ObserveSSE captures only bounded routing metadata events while returning the
// exact decoded stream to the business reader. Oversized events are skipped.
func (turn *Turn) ObserveSSE(body io.ReadCloser) io.ReadCloser {
	return &turnStream{ReadCloser: body, turn: turn}
}

type turnStream struct {
	io.ReadCloser
	turn                         *Turn
	line, data                   []byte
	afterCR, dropLine, dropEvent bool
}

const maxRoutingEvent = 64 << 10

func (stream *turnStream) Read(buffer []byte) (int, error) {
	n, err := stream.ReadCloser.Read(buffer)
	if stream.turn.Value() != "" {
		stream.line = nil
		stream.data = nil
		return n, err
	}
	for _, b := range buffer[:n] {
		if stream.afterCR {
			stream.afterCR = false
			if b == '\n' {
				continue
			}
		}
		if b == '\r' || b == '\n' {
			stream.finishLine()
			stream.afterCR = b == '\r'
			continue
		}
		if !stream.dropLine {
			if len(stream.line) >= maxRoutingEvent {
				stream.line = nil
				stream.dropLine = true
				stream.dropEvent = true
			} else {
				stream.line = append(stream.line, b)
			}
		}
	}
	return n, err
}

func (stream *turnStream) finishLine() {
	if stream.dropLine {
		stream.dropLine = false
		stream.line = nil
		return
	}
	if len(stream.line) == 0 {
		if !stream.dropEvent && len(stream.data) > 0 {
			stream.turn.ObserveMetadata(stream.data)
		}
		stream.data = nil
		stream.dropEvent = false
	} else if !stream.dropEvent && bytes.HasPrefix(stream.line, []byte("data:")) {
		part := stream.line[5:]
		if len(part) > 0 && part[0] == ' ' {
			part = part[1:]
		}
		if len(stream.data)+len(part)+1 > maxRoutingEvent {
			stream.data = nil
			stream.dropEvent = true
		} else {
			if len(stream.data) > 0 {
				stream.data = append(stream.data, '\n')
			}
			stream.data = append(stream.data, part...)
		}
	}
	stream.line = stream.line[:0]
}
