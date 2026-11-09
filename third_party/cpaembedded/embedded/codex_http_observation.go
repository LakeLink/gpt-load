package embedded

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

const maxHTTPObservationChunk = 32 << 10

// codexHTTPObservationBody observes bytes consumed by the transport or business reader.
// Read serializes decoder access and publication. Close first closes the wire
// body to unblock a pending Read, then releases decoders and publishes the end.
type codexHTTPObservationBody struct {
	ctx       context.Context
	wire      io.ReadCloser
	observe   func(ExchangeObservation)
	exchange  uint64
	path      string
	transport string
	direction string

	readMu       sync.Mutex
	decoded      io.ReadCloser
	closeDecoded bool
	ended        bool
	sequence     uint64
	offset       uint64
	closing      atomic.Bool
	close        sync.Once
	closeErr     error
}

// pendingCodexHTTPResponseBody lets decoder setup failures and concurrent Close share ownership
// of the original response body, including its close error.
type pendingCodexHTTPResponseBody struct {
	io.ReadCloser
	ctx         context.Context
	observe     func(ExchangeObservation)
	exchange    uint64
	path        string
	transport   string
	once        sync.Once
	err         error
	attached    atomic.Bool
	wrapDecoded func(io.ReadCloser) io.ReadCloser
}

func (body *pendingCodexHTTPResponseBody) Close() error {
	body.once.Do(func() {
		body.err = body.ReadCloser.Close()
		if !body.attached.Load() && body.observe != nil {
			// Decoder setup closes the source on failure before a decoded body
			// exists. Settle the initialized stream's explicit zero totals.
			observation := &codexHTTPObservationBody{ctx: body.ctx, observe: body.observe, exchange: body.exchange, path: body.path, transport: body.transport, direction: "response"}
			observation.finish("read_error", nil)
		}
	})
	return body.err
}

func (body *pendingCodexHTTPResponseBody) ObserveDecodedHTTPResponseBody(decoded io.ReadCloser) io.ReadCloser {
	body.attached.Store(true)
	if body.wrapDecoded != nil {
		decoded = body.wrapDecoded(decoded)
	}
	if body.observe == nil {
		return decoded
	}
	return &codexHTTPObservationBody{
		ctx: body.ctx, wire: body, decoded: decoded,
		observe: body.observe, exchange: body.exchange, path: body.path, transport: body.transport, direction: "response", closeDecoded: true,
	}
}

func (body *codexHTTPObservationBody) Read(buffer []byte) (int, error) {
	body.readMu.Lock()
	defer body.readMu.Unlock()
	n, err := body.decoded.Read(buffer)
	if !body.ended {
		for start := 0; start < n; {
			end := min(start+maxHTTPObservationChunk, n)
			chunk := buffer[start:end]
			body.sequence++
			offset := body.offset
			body.offset += uint64(len(chunk))
			body.observe(ExchangeObservation{Kind: "stream", Sequence: body.exchange, Path: body.path, ObservedAt: time.Now(), Body: chunk,
				Stream: &StreamObservation{Transport: body.transport, Direction: body.direction, Kind: "data",
					StreamSequence: body.sequence, ByteOffset: offset, BodySize: uint64(len(chunk))}})
			start = end
		}
		if err != nil {
			reason := "read_error"
			if errors.Is(err, io.EOF) {
				reason = "eof"
			} else if body.closing.Load() {
				reason = "closed_early"
			}
			body.finish(reason, err)
		}
	}
	return n, err
}

func (body *codexHTTPObservationBody) Close() error {
	body.closing.Store(true)
	body.close.Do(func() {
		body.closeErr = body.wire.Close()
		body.readMu.Lock()
		defer body.readMu.Unlock()
		if body.closeDecoded {
			if err := body.decoded.Close(); err != nil {
				body.closeErr = err
			}
		}
		body.finish("closed_early", nil)
	})
	return body.closeErr
}

// finish runs under readMu, after the last data fact has been published.
func (body *codexHTTPObservationBody) finish(reason string, err error) {
	if body.ended {
		return
	}
	body.ended = true
	errorCode := ""
	if body.ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		reason = "canceled"
		errorCode = "canceled"
		if errors.Is(body.ctx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
			errorCode = "deadline_exceeded"
		}
	} else if reason == "read_error" {
		errorCode = "read_error"
	}
	body.observe(ExchangeObservation{Kind: "stream", Sequence: body.exchange, Path: body.path, ObservedAt: time.Now(),
		Stream: &StreamObservation{Transport: body.transport, Direction: body.direction, Kind: "end",
			LastStreamSequence: body.sequence, TotalBodyBytes: body.offset, EndReason: reason, ErrorCode: errorCode}})
}
