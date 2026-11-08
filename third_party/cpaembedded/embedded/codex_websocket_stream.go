package embedded

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// Capture state belongs to the connection rather than a turn or subscription.
// Payloads remain borrowed all the way to the bounded observation publisher.
type codexWSStreamCapture struct {
	mu              sync.Mutex
	connections     map[net.Conn]*codexWSStreamConnection
	latest          *codexWSStreamConnection
	observer        func(ExchangeObservation)
	ctx             context.Context
	active          bool
	exchange        uint64
	closing         bool
	closeReason     string
	closeError      error
	responseID      string
	priorResponseID string
	started         bool
}

type codexWSStreamConnection struct {
	id       string
	request  codexWSStreamDirection
	response codexWSStreamDirection
}

type codexWSStreamDirection struct {
	sequence uint64
	bytes    uint64
	pending  int
	ended    bool
	end      *StreamObservation
}

var _ cliproxyexecutor.WebSocketTransportObserver = (*codexWSResource)(nil)

func (c *codexWSStreamCapture) beginTurn(ctx context.Context, observer func(ExchangeObservation), priorResponseID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ctx, c.observer, c.active, c.exchange = ctx, observer, true, 0
	c.responseID, c.priorResponseID, c.started = "", priorResponseID, false
}

func (c *codexWSStreamCapture) endTurn() {
	c.mu.Lock()
	c.ctx, c.active = nil, false
	c.mu.Unlock()
}

func (c *codexWSStreamCapture) nextExchange() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.exchange++
	return c.exchange
}

func (c *codexWSStreamCapture) registerConnection(conn net.Conn, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connections == nil {
		c.connections = make(map[net.Conn]*codexWSStreamConnection)
	}
	c.latest = &codexWSStreamConnection{id: id}
	c.connections[conn] = c.latest
}

func (c *codexWSStreamCapture) observeExchange(fact ExchangeObservation) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.observer != nil {
		c.observer(fact)
	}
}

func (c *codexWSStreamCapture) connection(conn net.Conn) *codexWSStreamConnection {
	for conn != nil {
		if found := c.connections[conn]; found != nil {
			return found
		}
		// Gorilla reports GotConn before applying its TLS wrapper.
		wrapped, ok := conn.(interface{ NetConn() net.Conn })
		if !ok {
			break
		}
		conn = wrapped.NetConn()
	}
	return nil
}

func (resource *codexWSResource) BeginWebSocketWrite(conn net.Conn, messageType int, payload []byte) func(error) {
	c := &resource.session.stream
	c.mu.Lock()
	connection := c.connection(conn)
	if connection == nil || connection.request.ended {
		c.mu.Unlock()
		return nil
	}
	direction := &connection.request
	direction.sequence++
	offset := direction.bytes
	direction.bytes += uint64(len(payload))

	c.started = c.active && connection == c.latest
	direction.pending++
	c.exchange++
	sequence := c.exchange
	observer := c.observer
	connectionOnly := !c.active || connection != c.latest
	if responseID := gjson.GetBytes(payload, "response_id").String(); responseID != "" && responseID != c.responseID {
		connectionOnly = true
	}
	fact := ExchangeObservation{Kind: "stream", Path: "/v1/responses", Sequence: sequence, UpstreamConnectionID: connection.id,
		Stream: &StreamObservation{Transport: "websocket", Direction: "request", Kind: "data",
			StreamSequence: direction.sequence, ByteOffset: offset, BodySize: uint64(len(payload)),
			MessageType: codexWSMessageType(messageType), ConnectionOnly: connectionOnly}}
	c.mu.Unlock()
	return func(err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		fact.ObservedAt = time.Now()
		fact.Body = payload
		fact.Stream.WriteResult = "success"
		if err != nil {
			fact.Stream.WriteResult = "error"
			fact.Stream.ErrorCode, fact.Stream.CloseCode = codexWSStreamError(err)
		}
		if observer != nil {
			observer(fact)
		}
		direction.pending--
		if err != nil {
			direction.end = nil
			c.end(connection, "request", "write_error", err)
		}
		c.flushEnd(connection, "request")
	}
}

func (resource *codexWSResource) ObserveWebSocketRead(conn net.Conn, messageType int, payload []byte, err error, active bool) {
	c := &resource.session.stream
	c.mu.Lock()
	defer c.mu.Unlock()
	connection := c.connection(conn)
	if connection == nil {
		return
	}
	if err != nil {
		reason := "read_error"
		var closed *websocket.CloseError
		if errors.As(err, &closed) && closed.Code != websocket.CloseAbnormalClosure {
			reason = "closed"
		}
		if c.ctx != nil && c.ctx.Err() != nil {
			reason, err = "canceled", c.ctx.Err()
		}
		if c.closing {
			reason, err = c.closeReason, c.closeError
		}
		c.end(connection, "response", reason, err)
		c.end(connection, "request", reason, err)
		return
	}
	direction := &connection.response
	if direction.ended {
		return
	}
	direction.sequence++
	offset := direction.bytes
	direction.bytes += uint64(len(payload))
	// Attribution only reads optional identity hints. Raw bytes are published
	// before CPA performs business validation, normalization, or filtering.
	responseID := gjson.GetBytes(payload, "response.id").String()
	if responseID == "" {
		responseID = gjson.GetBytes(payload, "response_id").String()
	}
	if active && c.active && c.started && connection == c.latest && c.responseID == "" && responseID != "" && len(responseID) <= 256 && responseID != c.priorResponseID && messageType == websocket.TextMessage && gjson.ValidBytes(payload) {
		switch gjson.GetBytes(payload, "type").String() {
		case "response.created", "response.completed", "response.done", "response.failed", "response.incomplete":
			c.responseID = strings.Clone(responseID)
		}
	}
	owned := active && c.active && connection == c.latest && responseID != "" && responseID == c.responseID
	c.publish(ExchangeObservation{Kind: "stream", Path: "/v1/responses", Sequence: c.exchange, ObservedAt: time.Now(),
		UpstreamConnectionID: connection.id, Body: payload,
		Stream: &StreamObservation{Transport: "websocket", Direction: "response", Kind: "data",
			StreamSequence: direction.sequence, ByteOffset: offset, BodySize: uint64(len(payload)),
			MessageType: codexWSMessageType(messageType), ConnectionOnly: !owned}})
}

func (c *codexWSStreamCapture) publish(fact ExchangeObservation) {
	if c.observer == nil {
		return
	}
	if !c.active {
		fact.Stream.ConnectionOnly = true
	}
	c.observer(fact)
}

func (c *codexWSStreamCapture) end(connection *codexWSStreamConnection, name, reason string, err error) {
	direction := &connection.response
	if name == "request" {
		direction = &connection.request
	}
	if direction.ended || direction.end != nil {
		return
	}
	errorCode, closeCode := codexWSStreamError(err)
	direction.end = &StreamObservation{Transport: "websocket", Direction: name, Kind: "end",
		EndReason: reason, ErrorCode: errorCode, CloseCode: closeCode, ConnectionOnly: true}
	c.flushEnd(connection, name)
}

func (c *codexWSStreamCapture) flushEnd(connection *codexWSStreamConnection, name string) {
	direction := &connection.response
	if name == "request" {
		direction = &connection.request
	}
	if direction.ended || direction.end == nil || direction.pending > 0 {
		return
	}
	direction.ended = true
	direction.end.LastStreamSequence, direction.end.TotalBodyBytes = direction.sequence, direction.bytes
	c.publish(ExchangeObservation{Kind: "stream", Path: "/v1/responses", Sequence: c.exchange, ObservedAt: time.Now(),
		UpstreamConnectionID: connection.id, Stream: direction.end})
}

func (c *codexWSStreamCapture) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	reason := "closed"
	var err error
	if c.ctx != nil && c.ctx.Err() != nil {
		reason, err = "canceled", c.ctx.Err()
	}
	c.closing, c.closeReason, c.closeError = true, reason, err
}

func (resource *codexWSResource) EndWebSocketConnection(conn net.Conn) {
	c := &resource.session.stream
	c.mu.Lock()
	defer c.mu.Unlock()
	connection := c.connection(conn)
	if connection == nil {
		return
	}
	reason, err := "closed", error(nil)
	if c.closing {
		reason, err = c.closeReason, c.closeError
	}
	c.end(connection, "response", reason, err)
	c.end(connection, "request", reason, err)
}

func codexWSMessageType(messageType int) string {
	if messageType == websocket.BinaryMessage {
		return "binary"
	}
	return "text"
}

func codexWSStreamError(err error) (string, int) {
	if err == nil {
		return "", 0
	}
	if errors.Is(err, context.Canceled) {
		return "canceled", 0
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout", 0
	}
	var closeError *websocket.CloseError
	if errors.As(err, &closeError) {
		return "websocket_close", closeError.Code
	}
	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return "timeout", 0
	}
	return "transport_error", 0
}
