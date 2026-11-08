package codexprivacy

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"github.com/tidwall/sjson"
)

// OriginalTurn resolves only supported metadata. Absent/prewarm IDs use the
// logical request ID supplied by the gateway, shared by all of its attempts.
func OriginalTurn(payload []byte, headers http.Header, requestID string) (string, error) {
	var selected string
	scopeIDs := map[string]string{}
	canonicalID := func(value string) string {
		if parsed, err := uuid.Parse(value); err == nil {
			return parsed.String()
		}
		return value
	}
	selectValue := func(raw json.RawMessage) error {
		var value *string
		if json.Unmarshal(raw, &value) != nil {
			return errors.New("Codex privacy: invalid turn identifier")
		}
		if value == nil || *value == "" {
			return nil
		}
		if parsed, err := uuid.Parse(*value); err == nil {
			canonical := parsed.String()
			value = &canonical
		}
		if selected != "" && selected != *value {
			return errors.New("Codex privacy: conflicting turn identifiers")
		}
		selected = *value
		return nil
	}
	readFields := func(fields map[string]json.RawMessage) error {
		for _, name := range []string{"session_id", "thread_id"} {
			if raw, ok := fields[name]; ok {
				var value *string
				if json.Unmarshal(raw, &value) != nil {
					return errors.New("Codex privacy: invalid turn scope")
				}
				if value != nil && *value != "" {
					scopeIDs[name] = canonicalID(*value)
				}
			}
		}
		for _, name := range []string{"turn_id", "x-codex-turn-id"} {
			if raw, ok := fields[name]; ok {
				if err := selectValue(raw); err != nil {
					return err
				}
			}
		}
		return nil
	}
	readNested := func(raw []byte) error {
		fields, err := object(raw, "turn metadata")
		if err != nil {
			return err
		}
		return readFields(fields)
	}
	for name, values := range headers {
		for _, value := range values {
			switch strings.ToLower(name) {
			case "session-id", "session_id", "x-session-id", "x-claude-code-session-id":
				if value = strings.TrimSpace(value); value != "" {
					scopeIDs["session_id"] = canonicalID(value)
				}
			case "thread-id", "thread_id", "x-codex-thread-id":
				if value = strings.TrimSpace(value); value != "" {
					scopeIDs["thread_id"] = canonicalID(value)
				}
			case "x-codex-turn-id", "turn-id", "turn_id":
				raw, _ := json.Marshal(strings.TrimSpace(value))
				if err := selectValue(raw); err != nil {
					return "", err
				}
			case "x-codex-turn-metadata":
				if err := readNested([]byte(value)); err != nil {
					return "", err
				}
			}
		}
	}
	// Match the transport's canonical session-header precedence.
	for _, alias := range []string{"Session-Id", "Session_id", "X-Session-Id", "X-Claude-Code-Session-Id"} {
		found := ""
		for name, values := range headers {
			if strings.EqualFold(name, alias) && len(values) > 0 {
				found = strings.TrimSpace(values[0])
				break
			}
		}
		if found != "" {
			scopeIDs["session_id"] = canonicalID(found)
			break
		}
	}
	if len(payload) > 0 && !strings.HasPrefix(strings.ToLower(headers.Get("Content-Type")), "multipart/") {
		root, err := object(payload, "request")
		if err != nil {
			return "", err
		}
		if raw, ok := root["client_metadata"]; ok {
			fields, err := object(raw, "client_metadata")
			if err != nil {
				return "", err
			}
			if err := readFields(fields); err != nil {
				return "", err
			}
			if raw, ok := fields["x-codex-turn-metadata"]; ok {
				var value string
				if json.Unmarshal(raw, &value) != nil {
					return "", errors.New("Codex privacy: invalid turn metadata")
				}
				if err := readNested([]byte(value)); err != nil {
					return "", err
				}
			}
		}
	}
	if selected != "" {
		encoded, _ := json.Marshal([3]string{scopeIDs["session_id"], scopeIDs["thread_id"], selected})
		return "turn/" + string(encoded), nil
	}
	if requestID == "" {
		return "", errors.New("Codex privacy: logical request identity missing")
	}
	return "request/" + requestID, nil
}

func (turn *Turn) PrepareHeaders(headers http.Header) {
	PrepareTurnHeaders(headers, turn.Value())
}

func PrepareTurnHeaders(headers http.Header, value string) {
	for name := range headers {
		if strings.EqualFold(name, "X-Codex-Turn-State") {
			delete(headers, name)
		}
	}
	if value != "" {
		headers.Set("X-Codex-Turn-State", value)
	}
}

// PrepareMetadata replaces the only client_metadata routing-state location
// emitted by the pinned CLI. Incoming opaque values never select an owner.
func (turn *Turn) PrepareMetadata(payload []byte) ([]byte, error) {
	return PrepareTurnMetadata(payload, turn.Value())
}

func PrepareTurnMetadata(payload []byte, value string) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}
	if value != "" {
		return sjson.SetBytes(payload, "client_metadata.x-codex-turn-state", value)
	}
	return sjson.DeleteBytes(payload, "client_metadata.x-codex-turn-state")
}

func (turn *Turn) ObserveHeaders(headers http.Header) {
	for name, values := range headers {
		if strings.EqualFold(name, "X-Codex-Turn-State") {
			for _, value := range values {
				turn.Capture(value)
			}
		}
	}
}

// ObserveMetadata inspects only the routing metadata envelope, without
// traversing model output or modifying the event passed to the caller.
func (turn *Turn) ObserveMetadata(event []byte) {
	var envelope struct {
		Type    string                     `json:"type"`
		Headers map[string]json.RawMessage `json:"headers"`
	}
	if json.Unmarshal(event, &envelope) != nil || envelope.Type != "codex.response.metadata" {
		return
	}
	for name, raw := range envelope.Headers {
		if strings.EqualFold(name, "x-codex-turn-state") {
			var value string
			if json.Unmarshal(raw, &value) == nil {
				turn.Capture(value)
			}
		}
	}
}

// ApplyClientHeaders reinforces the already-derived supported fields after
// CPA's handshake construction. It never derives values a second time.
func ApplyClientHeaders(destination, source http.Header) {
	for name, values := range source {
		// This is a conversion input used to derive Codex Session-Id, rather
		// than an additional header to emit to a Codex upstream.
		if strings.EqualFold(name, "X-Claude-Code-Session-Id") {
			continue
		}
		_, known := identifierField(name)
		if !known && !strings.EqualFold(name, "x-codex-installation-id") && !strings.EqualFold(name, "x-codex-turn-metadata") {
			continue
		}
		if strings.EqualFold(name, "session_id") || strings.EqualFold(name, "session-id") {
			continue
		}
		for key := range destination {
			if strings.EqualFold(key, name) {
				delete(destination, key)
			}
		}
		destination[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
	}
	for _, name := range []string{"Session-Id", "Session_id"} {
		var value string
		for key, values := range source {
			if strings.EqualFold(key, name) && len(values) > 0 {
				value = strings.TrimSpace(values[0])
				break
			}
		}
		if value != "" {
			destination.Set("Session-Id", value)
			destination.Del("Session_id")
			break
		}
	}
}
