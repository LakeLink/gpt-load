package codexprivacy

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
	"github.com/tidwall/sjson"
)

type identifierKind uint8

const (
	plainID identifierKind = iota
	windowID
	affinityID
)

// identifierField is an explicit allowlist. It deliberately does not walk
// arbitrary objects, tool data, or server-issued response/item identifiers.
func identifierField(field string) (identifierKind, bool) {
	switch strings.ToLower(field) {
	case "session_id", "session-id", "x-session-id", "x-claude-code-session-id", "conversation_id", "conversation-id", "prompt_cache_key":
		return affinityID, true
	case "window_id", "x-codex-window-id":
		return windowID, true
	case "thread_id", "thread-id", "turn_id", "turn-id", "root_turn_id", "parent_turn_id", "parent_thread_id", "context_window_id", "forked_from_thread_id",
		"x-client-request-id", "x-codex-thread-id", "x-codex-turn-id", "x-codex-root-turn-id", "x-codex-parent-turn-id", "x-codex-parent-thread-id", "x-codex-context-window-id", "x-codex-forked-from-thread-id":
		return plainID, true
	default:
		return plainID, false
	}
}

func (identity Identity) clientID(value string, kind identifierKind) string {
	if value == "" {
		return value
	}
	if parsed, err := uuid.Parse(value); err == nil {
		return uuid.NewSHA1(identity.Namespace, []byte("client-id/v1/"+parsed.String())).String()
	}
	if left, right, ok := strings.Cut(value, ":"); ok {
		if kind == windowID {
			if _, err := uuid.Parse(left); err == nil {
				if _, err := strconv.ParseUint(right, 10, 64); err == nil {
					return identity.clientID(left, plainID) + ":" + right
				}
			}
		}
		// These are the pinned CLI's InternalSessionSource display values.
		if kind == affinityID && (left == "memory_consolidation" || left == "guardian") {
			if _, err := uuid.Parse(right); err == nil {
				return left + ":" + identity.clientID(right, plainID)
			}
		}
	}
	return uuid.NewSHA1(identity.Namespace, []byte("opaque-client-id/v1/"+value)).String()
}

func (identity Identity) mapIdentifierFields(body []byte, fields map[string]json.RawMessage, prefix string) ([]byte, error) {
	for field, raw := range fields {
		kind, ok := identifierField(field)
		if !ok {
			continue
		}
		var value *string
		if err := json.Unmarshal(raw, &value); err != nil {
			return nil, fmt.Errorf("Codex privacy: %s must be text or null", field)
		}
		if value == nil || *value == "" {
			continue
		}
		var err error
		body, err = sjson.SetBytes(body, prefix+field, identity.clientID(*value, kind))
		if err != nil {
			return nil, fmt.Errorf("Codex privacy: update %s", field)
		}
	}
	return body, nil
}

// AffinityID scopes an internal fallback before CPA derives outgoing cache and
// session identifiers from it. The immutable host scheduling key stays intact.
func (identity Identity) AffinityID(value string) string { return identity.clientID(value, affinityID) }
