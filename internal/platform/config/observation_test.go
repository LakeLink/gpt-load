package config

import (
	"strings"
	"testing"
)

func TestObservationConfiguration(t *testing.T) {
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("AUTH_KEY", "test-key")
	t.Setenv("OBSERVATION_PLUGINS", `[{"id":"research","token":"sensitive-test-token"}]`)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Observation.Plugins) != 1 || cfg.Observation.QueueMaxEvents != 200 || cfg.Observation.QueueMaxBytes != 4000000 || cfg.Observation.EventMaxBytes != 1048576 || cfg.Observation.WriteTimeoutSeconds != 5 {
		t.Fatalf("observation config = %+v", cfg.Observation)
	}
	for _, input := range []string{`[{"id":"research","token":""}]`, `[{"id":"","token":"sensitive-test-token"}]`, `[{"id":"research","token":"sensitive-test-token"},{"id":"research","token":"other"}]`, `{"token":"sensitive-test-token"}`} {
		t.Setenv("OBSERVATION_PLUGINS", input)
		_, err := Load()
		if err == nil || strings.Contains(err.Error(), "sensitive-test-token") {
			t.Fatalf("invalid config error = %v", err)
		}
	}
	t.Setenv("OBSERVATION_PLUGINS", "[]")
	t.Setenv("OBSERVATION_QUEUE_MAX_EVENTS", "2")
	t.Setenv("OBSERVATION_QUEUE_MAX_BYTES", "512")
	t.Setenv("OBSERVATION_EVENT_MAX_BYTES", "512")
	t.Setenv("OBSERVATION_WRITE_TIMEOUT_SECONDS", "1")
	cfg, err = Load()
	if err != nil || len(cfg.Observation.Plugins) != 0 || cfg.Observation.QueueMaxEvents != 2 || cfg.Observation.QueueMaxBytes != 512 || cfg.Observation.EventMaxBytes != 512 || cfg.Observation.WriteTimeoutSeconds != 1 {
		t.Fatalf("custom empty configuration=%+v err=%v", cfg, err)
	}
	t.Setenv("OBSERVATION_EVENT_MAX_BYTES", "1048576")
	t.Setenv("OBSERVATION_QUEUE_MAX_BYTES", "1024")
	if _, err := Load(); err == nil {
		t.Fatal("accepted event budget above queue budget")
	}
	t.Setenv("OBSERVATION_QUEUE_MAX_BYTES", "4000000")
	t.Setenv("OBSERVATION_QUEUE_MAX_EVENTS", "0")
	if _, err := Load(); err == nil {
		t.Fatal("accepted zero queue capacity")
	}
}
