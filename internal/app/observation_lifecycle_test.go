package app

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/gin-gonic/gin"
	"github.com/ugorji/go/codec"

	"gpt-load/internal/httplifecycle"
	"gpt-load/internal/observation"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/platform/httproute"
	"gpt-load/internal/storage"
)

func TestAppStopPublishesHijackedBusinessCompletionBeforeClosingPlugins(t *testing.T) {
	db, err := storage.Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := httplifecycle.NewCoordinator()
	cfg := testConfig(t)
	cfg.Observation = config.ObservationConfig{Plugins: []config.ObservationPlugin{{ID: "local", Token: "test-token"}}, QueueMaxEvents: 5, QueueMaxBytes: 4096, EventMaxBytes: 4096, WriteTimeoutSeconds: 1}
	observations := observation.New(cfg)
	engine, err := NewEngineWithLifecycle(lifecycle, cfg)
	if err != nil {
		t.Fatal(err)
	}
	routes, err := httproute.NewRegistry(observations.HTTPModule())
	if err != nil {
		t.Fatal(err)
	}
	if err := routes.Bind(engine); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	engine.GET("/business", func(c *gin.Context) {
		ctx, release, accepted := lifecycle.BindData(c.Request)
		if !accepted {
			c.Status(503)
			return
		}
		defer release()
		socket, err := websocket.Accept(c.Writer, c.Request, nil)
		if err != nil {
			return
		}
		defer socket.CloseNow()
		close(entered)
		<-ctx.Done()
		observations.Publish("business.finished", time.Now(), func() any { return "final fact from hijacked handler" })
	})
	application := NewApp(AppParams{Engine: engine, Config: cfg, DB: db, StartupBootstrap: startupBootstrapFunc(noopStartupBootstrap), RuntimeState: runtimeStateLoaderFunc(func(context.Context) error { return nil }), ControlRuntime: newControlRuntimeFake(nil, false), RequestLogs: newRequestLogRuntimeFake(nil, nil), Lifecycle: lifecycle, Observations: observations})
	if err := application.Start(); err != nil {
		t.Fatal(err)
	}
	defer application.Stop(t.Context())
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	url := "ws://" + application.Address()
	plugin, _, err := websocket.Dial(ctx, url+"/api/plugins/local/events", &websocket.DialOptions{HTTPHeader: http.Header{"Authorization": []string{"Bearer test-token"}}, Subprotocols: []string{observation.Subprotocol}})
	if err != nil {
		t.Fatal(err)
	}
	defer plugin.CloseNow()
	var subscribe []byte
	codec.NewEncoderBytes(&subscribe, &codec.MsgpackHandle{}).Encode(map[string]string{"type": "subscribe", "command_id": "begin"})
	if err := plugin.Write(ctx, websocket.MessageBinary, subscribe); err != nil {
		t.Fatal(err)
	}
	if _, _, err := plugin.Read(ctx); err != nil {
		t.Fatal(err)
	}
	business, _, err := websocket.Dial(ctx, url+"/business", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer business.CloseNow()
	<-entered
	stopped := make(chan error, 1)
	go func() { stopped <- application.Stop(ctx) }()
	_, body, err := plugin.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var event struct {
		Type string `codec:"type"`
		Data string `codec:"data"`
	}
	if err := codec.NewDecoderBytes(body, &codec.MsgpackHandle{}).Decode(&event); err != nil {
		t.Fatal(err)
	}
	if event.Type != "business.finished" || !strings.Contains(event.Data, "final fact") {
		t.Fatalf("event=%+v", event)
	}
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
}
