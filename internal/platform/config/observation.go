package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

// ObservationConfig 是进程级插件接入和资源预算。
type ObservationConfig struct {
	Plugins             []ObservationPlugin
	QueueMaxEvents      int
	QueueMaxBytes       int
	EventMaxBytes       int
	WriteTimeoutSeconds int
}

type ObservationPlugin struct {
	ID    string `json:"id"`
	Token string `json:"token"`
}

func loadObservationConfig() (ObservationConfig, error) {
	result := ObservationConfig{}
	if raw := strings.TrimSpace(os.Getenv("OBSERVATION_PLUGINS")); raw != "" {
		if err := json.Unmarshal([]byte(raw), &result.Plugins); err != nil || raw == "null" {
			return result, fmt.Errorf("OBSERVATION_PLUGINS must be a JSON array of plugin credentials")
		}
	}
	seen := make(map[string]bool)
	for index, plugin := range result.Plugins {
		if strings.TrimSpace(plugin.ID) == "" {
			return result, fmt.Errorf("OBSERVATION_PLUGINS entry %d requires id", index)
		}
		if strings.TrimSpace(plugin.Token) == "" {
			return result, fmt.Errorf("OBSERVATION_PLUGINS plugin %q requires token", plugin.ID)
		}
		if seen[plugin.ID] {
			return result, fmt.Errorf("OBSERVATION_PLUGINS plugin %q has duplicate id", plugin.ID)
		}
		seen[plugin.ID] = true
	}
	for _, field := range []struct {
		name     string
		fallback int
		out      *int
	}{
		{"OBSERVATION_QUEUE_MAX_EVENTS", 200, &result.QueueMaxEvents},
		{"OBSERVATION_QUEUE_MAX_BYTES", 4000000, &result.QueueMaxBytes},
		{"OBSERVATION_EVENT_MAX_BYTES", 1048576, &result.EventMaxBytes},
		{"OBSERVATION_WRITE_TIMEOUT_SECONDS", 5, &result.WriteTimeoutSeconds},
	} {
		value, err := parsePositiveInt(field.name, field.fallback)
		if err != nil {
			return result, err
		}
		*field.out = value
	}
	if result.EventMaxBytes > result.QueueMaxBytes {
		return result, fmt.Errorf("OBSERVATION_EVENT_MAX_BYTES must be less than or equal to OBSERVATION_QUEUE_MAX_BYTES")
	}
	return result, nil
}
