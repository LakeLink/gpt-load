package observation

import (
	"context"
	"crypto/subtle"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"

	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/httproute"
)

type subscription struct {
	queue [][]byte
	bytes int
}
type plugin struct {
	token        string
	seq          uint64
	subscription *subscription
	connection   *connection
}
type connection struct {
	socket     *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	commands   chan Control
	wake       chan struct{}
	done       chan struct{}
	delivering bool
}

// Service 按插件身份保留有界采集队列，连接 writer 独立发送。
type Service struct {
	mu      sync.Mutex
	cfg     config.ObservationConfig
	bootID  string
	plugins map[string]*plugin
}

func New(cfg *config.Config) *Service {
	service := &Service{cfg: cfg.Observation, bootID: uuid.NewString(), plugins: make(map[string]*plugin)}
	for _, entry := range cfg.Observation.Plugins {
		service.plugins[entry.ID] = &plugin{token: entry.Token}
	}
	return service
}

func (s *Service) HTTPModule() httproute.Module {
	module := httproute.Module{Name: "observation", Owner: httproute.OwnerPlugin, Auth: httproute.AuthPlugin, Prefix: "/api/plugins", NamespacePrefixes: []string{"/api/plugins"}, Authenticate: s.authenticate}
	if len(s.plugins) > 0 {
		module.Routes = []httproute.Route{{Name: "observation.events", Methods: []string{http.MethodGet}, Path: "/:plugin_id/events", Handlers: gin.HandlersChain{s.handle}}}
	}
	return module
}

func (s *Service) authenticate(c *gin.Context) {
	p := s.plugins[c.Param("plugin_id")]
	scheme, token, ok := strings.Cut(c.GetHeader("Authorization"), " ")
	if p == nil || !ok || !strings.EqualFold(scheme, "Bearer") || subtle.ConstantTimeCompare([]byte(token), []byte(p.token)) != 1 {
		c.AbortWithStatus(http.StatusUnauthorized)
		return
	}
	c.Next()
}

func (s *Service) handle(c *gin.Context) {
	supported := false
	for _, header := range c.Request.Header.Values("Sec-Websocket-Protocol") {
		for _, value := range strings.Split(header, ",") {
			if strings.TrimSpace(value) == Subprotocol {
				supported = true
			}
		}
	}
	if !supported {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}
	socket, err := websocket.Accept(upgradeWriter{c.Writer}, c.Request, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		return
	}
	socket.SetReadLimit(ControlReadLimit)
	ctx, cancel := context.WithCancel(context.Background())
	conn := &connection{socket: socket, ctx: ctx, cancel: cancel, commands: make(chan Control), wake: make(chan struct{}, 1), done: make(chan struct{})}
	p := s.plugins[c.Param("plugin_id")]
	s.mu.Lock()
	old := p.connection
	p.connection = conn
	s.mu.Unlock()
	if old != nil {
		old.cancel()
		_ = old.socket.CloseNow()
	}
	defer func() {
		cancel()
		_ = socket.CloseNow()
		<-conn.done
		s.mu.Lock()
		if p.connection == conn {
			p.connection = nil
		}
		s.mu.Unlock()
	}()
	go s.writeLoop(p, conn)
	for {
		kind, data, err := socket.Read(ctx)
		if err != nil {
			return
		}
		command, decodeErr := decodeCommand(data)
		reply := Control{}
		if kind != websocket.MessageBinary {
			reply = Control{Type: "error", Code: "unsupported_message", Message: "binary MessagePack control required"}
		} else if decodeErr != nil {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "invalid_message", Message: "invalid MessagePack control", fatal: true}
		} else if strings.TrimSpace(command.CommandID) == "" || command.Type == "" {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "invalid_message", Message: "type and command_id are required"}
		} else if command.Type != "subscribe" && command.Type != "unsubscribe" {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "unknown_command", Message: "unknown control command"}
		} else {
			reply = Control{Type: command.Type, CommandID: command.CommandID}
		}
		select {
		case conn.commands <- reply:
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) writeLoop(p *plugin, conn *connection) {
	defer close(conn.done)
	defer conn.cancel()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		// 控制消息优先，并与事件由同一个 writer 串行发送。
		select {
		case reply := <-conn.commands:
			if !s.control(p, conn, reply) {
				return
			}
			continue
		default:
		}
		s.mu.Lock()
		current := p.connection == conn
		var data []byte
		if current && conn.delivering && p.subscription != nil && len(p.subscription.queue) > 0 {
			sub := p.subscription
			data = sub.queue[0]
			sub.queue[0] = nil
			sub.queue = sub.queue[1:]
			sub.bytes -= len(data)
		}
		s.mu.Unlock()
		if !current {
			return
		}
		if data != nil {
			if !s.write(conn, data) {
				return
			}
			continue
		}
		select {
		case <-conn.ctx.Done():
			return
		case reply := <-conn.commands:
			if !s.control(p, conn, reply) {
				return
			}
		case <-conn.wake:
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(conn.ctx, s.writeTimeout())
			err := conn.socket.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		}
	}
}

func (s *Service) control(p *plugin, conn *connection, reply Control) bool {
	s.mu.Lock()
	if p.connection != conn {
		s.mu.Unlock()
		return false
	}
	switch reply.Type {
	case "subscribe":
		if p.subscription == nil {
			p.subscription = &subscription{}
		}
		reply.Type = "subscribed"
		reply.BootID = s.bootID
		reply.MaxEventBytes = s.cfg.QueueMaxBytes
	case "unsubscribe":
		p.subscription = nil
		conn.delivering = false
		reply.Type = "unsubscribed"
		reply.BootID = s.bootID
	case "error":
	default:
		reply.Type = "error"
		reply.Code = "unknown_command"
		reply.Message = "unknown control command"
	}
	s.mu.Unlock()
	data, err := encode(reply)
	if err != nil {
		return false
	}
	if !s.write(conn, data) {
		return false
	}
	if reply.fatal {
		_ = conn.socket.Close(websocket.StatusInvalidFramePayloadData, "invalid MessagePack control")
		return false
	}
	if reply.Type == "subscribed" {
		s.mu.Lock()
		if p.connection == conn {
			conn.delivering = true
		}
		s.mu.Unlock()
	}
	return true
}
func (s *Service) writeTimeout() time.Duration {
	return time.Duration(s.cfg.WriteTimeoutSeconds) * time.Second
}
func (s *Service) write(conn *connection, data []byte) bool {
	ctx, cancel := context.WithTimeout(conn.ctx, s.writeTimeout())
	defer cancel()
	return conn.socket.Write(ctx, websocket.MessageBinary, data) == nil
}

// Publish 仅在存在订阅时构造快照；队列内记录均为独立编码数据。
// yagni: 发布持有进程级锁，保证序号分配与入队顺序一致。
func (s *Service) Publish(kind string, observedAt time.Time, snapshot func() any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var data any
	built := false
	for id, p := range s.plugins {
		sub := p.subscription
		if sub == nil {
			continue
		}
		if !built {
			data = snapshot()
			built = true
		}
		p.seq++
		encoded, err := encode(Event{Type: kind, BootID: s.bootID, Seq: p.seq, ObservedAt: observedAt.UTC().UnixMilli(), Data: data})
		if err != nil || len(encoded) > s.cfg.QueueMaxBytes {
			reason := "encoded_record_exceeds_queue_budget"
			if err != nil {
				reason = "messagepack_encoding_failed"
			}
			logrus.WithFields(logrus.Fields{"event": "observation.publish_failed", "plugin_id": id, "type": kind, "seq": p.seq, "bytes": len(encoded), "reason": reason}).Warn("observation publication failed")
			continue
		}
		for len(sub.queue) > 0 && (len(sub.queue) >= s.cfg.QueueMaxEvents || sub.bytes+len(encoded) > s.cfg.QueueMaxBytes) {
			sub.bytes -= len(sub.queue[0])
			sub.queue[0] = nil
			sub.queue = sub.queue[1:]
		}
		sub.queue = append(sub.queue, encoded)
		sub.bytes += len(encoded)
		if p.connection != nil {
			select {
			case p.connection.wake <- struct{}{}:
			default:
			}
		}
	}
}

// upgradeWriter 将升级校验的客户端错误统一为协议约定的 400。
type upgradeWriter struct{ gin.ResponseWriter }

func (w upgradeWriter) WriteHeader(status int) {
	if status >= 400 && status < 500 {
		status = http.StatusBadRequest
	}
	w.ResponseWriter.WriteHeader(status)
}
