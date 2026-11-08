package codexprivacy

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/tidwall/sjson"
)

// Prepare copies the request, changing only supported client identity metadata.
// Original payload/header copies stay available to retries and ownership lookup.
func (identity Identity) Prepare(payload []byte, headers http.Header) ([]byte, http.Header, error) {
	out := headers.Clone()
	if out == nil {
		out = make(http.Header)
	}
	for name, values := range out {
		switch {
		case strings.EqualFold(name, "X-Codex-Installation-Id"):
			delete(out, name)
		case strings.EqualFold(name, "X-Codex-Turn-Metadata"):
			mapped := make([]string, len(values))
			for i, value := range values {
				next, err := identity.turnMetadata([]byte(value))
				if err != nil {
					return nil, nil, err
				}
				mapped[i] = string(next)
			}
			out[name] = mapped
		default:
			if kind, ok := identifierField(name); ok {
				for i, value := range values {
					out[name][i] = identity.clientID(strings.TrimSpace(value), kind)
				}
			}
		}
	}
	out.Set("X-Codex-Installation-Id", identity.InstallationID)
	if len(payload) == 0 {
		return nil, out, nil
	}
	root, err := object(payload, "request")
	if err != nil {
		return nil, nil, err
	}
	metadata := map[string]json.RawMessage{}
	if raw, ok := root["client_metadata"]; ok {
		metadata, err = object(raw, "client_metadata")
		if err != nil {
			return nil, nil, err
		}
	}
	for _, field := range []string{"x-codex-installation-id", "installation_id"} {
		if value, ok := metadata[field]; ok && !stringOrNull(value) {
			return nil, nil, fmt.Errorf("Codex privacy: %s must be text or null", field)
		}
	}
	body := append([]byte(nil), payload...)
	body, err = sjson.SetBytes(body, "client_metadata.x-codex-installation-id", identity.InstallationID)
	if err != nil {
		return nil, nil, errors.New("Codex privacy: update installation metadata")
	}
	if _, ok := metadata["installation_id"]; ok {
		body, err = sjson.SetBytes(body, "client_metadata.installation_id", identity.InstallationID)
		if err != nil {
			return nil, nil, err
		}
	}
	if raw, ok := metadata["x-codex-turn-metadata"]; ok {
		var value string
		if json.Unmarshal(raw, &value) != nil || string(raw) == "null" {
			return nil, nil, errors.New("Codex privacy: turn metadata must be JSON text")
		}
		next, err := identity.turnMetadata([]byte(value))
		if err != nil {
			return nil, nil, err
		}
		body, err = sjson.SetBytes(body, "client_metadata.x-codex-turn-metadata", string(next))
		if err != nil {
			return nil, nil, err
		}
	}
	body, err = identity.mapIdentifierFields(body, metadata, "client_metadata.")
	if err != nil {
		return nil, nil, err
	}
	// Only the Responses cache-affinity field is supported at the body root.
	if raw, ok := root["prompt_cache_key"]; ok {
		body, err = identity.mapIdentifierFields(body, map[string]json.RawMessage{"prompt_cache_key": raw}, "")
	}
	return body, out, err
}

func (identity Identity) turnMetadata(raw []byte) ([]byte, error) {
	metadata, err := object(raw, "turn metadata")
	if err != nil {
		return nil, err
	}
	for _, field := range []string{"installation_id", "x-codex-installation-id"} {
		if value, ok := metadata[field]; ok && !stringOrNull(value) {
			return nil, fmt.Errorf("Codex privacy: %s must be text or null", field)
		}
	}
	out, err := sjson.SetBytes(raw, "installation_id", identity.InstallationID)
	if err != nil {
		return nil, errors.New("Codex privacy: update turn metadata")
	}
	if _, ok := metadata["x-codex-installation-id"]; ok {
		out, err = sjson.SetBytes(out, "x-codex-installation-id", identity.InstallationID)
	}
	if err != nil {
		return nil, err
	}
	return identity.mapIdentifierFields(out, metadata, "")
}

func object(raw []byte, name string) (map[string]json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, fmt.Errorf("Codex privacy: %s must be an object", name)
	}
	fields := make(map[string]json.RawMessage)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("Codex privacy: invalid %s", name)
		}
		field, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("Codex privacy: invalid %s", name)
		}
		if _, exists := fields[field]; exists {
			return nil, fmt.Errorf("Codex privacy: duplicate fields in %s", name)
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return nil, fmt.Errorf("Codex privacy: invalid %s", name)
		}
		fields[field] = value
	}
	if end, err := decoder.Token(); err != nil || end != json.Delim('}') {
		return nil, fmt.Errorf("Codex privacy: invalid %s", name)
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, fmt.Errorf("Codex privacy: invalid %s", name)
	}
	return fields, nil
}
func stringOrNull(raw []byte) bool { var text *string; return json.Unmarshal(raw, &text) == nil }
