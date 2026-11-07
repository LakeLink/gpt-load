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
	"gpt-load/internal/platform/i18n"
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
type controlReply struct {
	Control
	subscription *subscription
}

type connection struct {
	socket     *websocket.Conn
	ctx        context.Context
	cancel     context.CancelFunc
	commands   chan controlReply
	wake       chan struct{}
	done       chan struct{}
	exited     chan struct{}
	delivering bool
	language   string
}

// Service 按插件身份保留有界采集队列，连接 writer 独立发送。
type Service struct {
	mu          sync.Mutex
	publishMu   sync.Mutex
	closing     bool
	stopped     bool
	connections map[*connection]struct{}
	cfg         config.ObservationConfig
	bootID      string
	plugins     map[string]*plugin
}

func New(cfg *config.Config) *Service {
	service := &Service{cfg: cfg.Observation, bootID: uuid.NewString(), plugins: make(map[string]*plugin), connections: make(map[*connection]struct{})}
	for _, entry := range cfg.Observation.Plugins {
		service.plugins[entry.ID] = &plugin{token: entry.Token}
	}
	return service
}

func (s *Service) HTTPModule() httproute.Module {
	module := httproute.Module{Name: "observation", Owner: httproute.OwnerPlugin, Auth: httproute.AuthPlugin, Prefix: "/api/plugins", NamespacePrefixes: []string{"/api/plugins"}, Authenticate: s.authenticate, BeforeAuth: gin.HandlersChain{i18n.Middleware()}}
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
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		c.AbortWithStatus(http.StatusServiceUnavailable)
		return
	}
	s.mu.Unlock()
	socket, err := websocket.Accept(upgradeWriter{c.Writer}, c.Request, &websocket.AcceptOptions{Subprotocols: []string{Subprotocol}})
	if err != nil {
		return
	}
	socket.SetReadLimit(ControlReadLimit)
	ctx, cancel := context.WithCancel(context.Background())
	conn := &connection{socket: socket, ctx: ctx, cancel: cancel, language: i18n.GetLanguageFromContext(c), commands: make(chan controlReply, 1), wake: make(chan struct{}, 1), done: make(chan struct{}), exited: make(chan struct{})}
	p := s.plugins[c.Param("plugin_id")]
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		cancel()
		_ = socket.CloseNow()
		return
	}
	old := p.connection
	p.connection = conn
	s.connections[conn] = struct{}{}
	s.mu.Unlock()
	if old != nil {
		old.cancel()
		_ = old.socket.CloseNow()
	}
	defer func() {
		defer close(conn.exited)
		cancel()
		_ = socket.CloseNow()
		<-conn.done
		s.mu.Lock()
		delete(s.connections, conn)
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
			reply = Control{Type: "error", Code: "unsupported_message", Message: i18n.Message(c, "observation.binary_control_required")}
		} else if decodeErr != nil {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "invalid_message", Message: i18n.Message(c, "observation.invalid_control"), fatal: true}
		} else if strings.TrimSpace(command.CommandID) == "" || command.Type == "" {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "invalid_message", Message: i18n.Message(c, "observation.control_fields_required")}
		} else if command.Type != "subscribe" && command.Type != "unsubscribe" {
			reply = Control{Type: "error", CommandID: command.CommandID, Code: "unknown_command", Message: i18n.Message(c, "observation.unknown_command")}
		} else {
			reply = Control{Type: command.Type, CommandID: command.CommandID}
		}
		prepared, current := s.prepare(p, conn, reply)
		if !current {
			return
		}
		select {
		case conn.commands <- prepared:
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
		select {
		case <-conn.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(conn.ctx, s.writeTimeout())
			err := conn.socket.Ping(ctx)
			cancel()
			if err != nil {
				return
			}
		default:
		}
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
		drained := s.stopped && data == nil
		s.mu.Unlock()
		if !current || drained {
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

// prepare 在读循环中建立取消栅栏；确认仍由 writer 排在在途写入之后。
func (s *Service) prepare(p *plugin, conn *connection, reply Control) (controlReply, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if p.connection != conn {
		return controlReply{}, false
	}
	result := controlReply{Control: reply}
	switch reply.Type {
	case "subscribe":
		if s.closing {
			result.Control = Control{Type: "error", CommandID: reply.CommandID, Code: "shutting_down", Message: i18n.T(i18n.GetLocalizer(conn.language), "observation.shutting_down")}
			break
		}
		if p.subscription == nil {
			p.subscription = &subscription{}
		}
		result.subscription = p.subscription
		result.Type = "subscribed"
		result.BootID = s.bootID
		result.MaxEventBytes = s.cfg.QueueMaxBytes
	case "unsubscribe":
		if p.subscription != nil {
			p.subscription.queue = nil
			p.subscription.bytes = 0
		}
		p.subscription = nil
		conn.delivering = false
		result.Type = "unsubscribed"
		result.BootID = s.bootID
	}
	return result, true
}

func (s *Service) control(p *plugin, conn *connection, reply controlReply) bool {
	s.mu.Lock()
	current := p.connection == conn
	s.mu.Unlock()
	if !current {
		return false
	}
	data, err := encode(reply.Control)
	if err != nil || !s.write(conn, data) {
		return false
	}
	if reply.fatal {
		// 库的 Close 内置等待上限，读循环仍处理关闭握手。
		_ = conn.socket.Close(websocket.StatusInvalidFramePayloadData, reply.Message)
		return false
	}
	if reply.Type == "subscribed" {
		s.mu.Lock()
		if p.connection == conn && p.subscription == reply.subscription {
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

// Publish 在订阅代次内构造快照，取消可在构造和编码期间推进。
func (s *Service) Publish(kind string, observedAt time.Time, snapshot func() any) {
	type target struct {
		id     string
		plugin *plugin
		sub    *subscription
	}
	s.mu.Lock()
	var targets []target
	if !s.stopped {
		for id, p := range s.plugins {
			if p.subscription != nil {
				targets = append(targets, target{id, p, p.subscription})
			}
		}
	}
	s.mu.Unlock()
	if len(targets) == 0 {
		return
	}
	s.publishMu.Lock()
	defer s.publishMu.Unlock()
	// 等待发布顺序期间也可能发生取消或接替订阅。
	s.mu.Lock()
	active := false
	if !s.stopped {
		for _, target := range targets {
			active = active || target.plugin.subscription == target.sub
		}
	}
	s.mu.Unlock()
	if !active {
		return
	}
	data := snapshot()
	// 相同序号的插件共享最终不可变编码记录，避免重复复制正文。
	encodedBySequence := make(map[uint64]encodedObservation)
	for _, target := range targets {
		s.mu.Lock()
		p, sub := target.plugin, target.sub
		if s.stopped || p.subscription != sub {
			s.mu.Unlock()
			continue
		}
		p.seq++
		seq := p.seq
		s.mu.Unlock()
		result, cached := encodedBySequence[seq]
		if !cached {
			result = encodeObservation(Event{Type: kind, BootID: s.bootID, Seq: seq, ObservedAt: observedAt.UTC().UnixMilli(), Data: data}, s.cfg.EventMaxBytes, s.cfg.QueueMaxBytes)
			encodedBySequence[seq] = result
		}
		fields := logrus.Fields{"plugin_id": target.id, "boot_id": s.bootID, "type": kind, "seq": seq, "observed_at": observedAt.UTC().UnixMilli(), "original_bytes": result.originalBytes, "body_omitted_bytes": result.bodyOmittedBytes, "bytes": result.finalBytes}
		if result.err != nil {
			fields["event"] = "observation.publish_failed"
			fields["reason"] = "messagepack_encoding_failed"
			if result.err == errEncodedBudget {
				fields["reason"] = "encoded_record_exceeds_queue_budget"
			}
			logrus.WithFields(fields).Warn("observation publication failed")
			continue
		}
		if result.event.BodyOmitted || result.event.Type == "oversized" {
			fields["event"] = "observation.degraded"
			fields["reason"] = "body_exceeds_event_threshold"
			if result.event.Type == "oversized" {
				fields["reason"] = "metadata_exceeds_event_threshold"
			}
			logrus.WithFields(fields).Debug("observation transport data omitted")
		}
		encoded := result.bytes
		s.mu.Lock()
		if !s.stopped && p.subscription == sub {
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
		s.mu.Unlock()
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

// BeginShutdown 关闭接入和新订阅，业务排空期间仍接收完成事实。
func (s *Service) BeginShutdown() {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
}

// Shutdown 停止生产并在调用方的全局截止时间内发送积压、释放连接。
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing, s.stopped = true, true
	connections := make([]*connection, 0, len(s.connections))
	for conn := range s.connections {
		connections = append(connections, conn)
		select {
		case conn.wake <- struct{}{}:
		default:
		}
	}
	s.mu.Unlock()
	stop := context.AfterFunc(ctx, func() {
		for _, conn := range connections {
			conn.cancel()
		}
	})
	defer stop()
	for _, conn := range connections {
		<-conn.exited
	}
	s.mu.Lock()
	for _, p := range s.plugins {
		if p.subscription != nil {
			p.subscription.queue = nil
			p.subscription.bytes = 0
		}
		p.subscription = nil
	}
	s.mu.Unlock()
	return ctx.Err()
}
