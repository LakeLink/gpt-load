package observation

import (
	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
)

// ObserveExchange 将执行层已采集的 Codex 传输事实投递到通用事件出口。
func (s *Service) ObserveExchange(spec execution.AttemptSpec, stream bool, fact execution.UpstreamExchange) {
	if spec.ChannelID != string(channel.Codex) {
		return
	}
	s.Publish("upstream."+fact.Kind, fact.ObservedAt, func() any {
		var data map[string]any
		if content := fact.Stream; content != nil {
			data = map[string]any{"protocol": fact.Protocol, "transport": content.Transport, "direction": content.Direction, "kind": content.Kind}
			if !content.ConnectionOnly {
				data["request_id"], data["attempt_id"], data["attempt_sequence"] = spec.RequestID, spec.AttemptID, spec.Sequence
				data["exchange_sequence"], data["channel_id"] = fact.Sequence, spec.ChannelID
				data["credential_id"], data["credential_version"] = spec.Credential.ID, spec.Credential.Version
				data["identity_generation"], data["model"] = spec.Credential.IdentityGeneration, spec.UpstreamModel
			}
			if content.Kind == "data" {
				data["stream_sequence"], data["byte_offset"], data["body_size"] = content.StreamSequence, content.ByteOffset, content.BodySize
				// Publish synchronously encodes the borrowed payload before returning.
				// Keeping it borrowed also avoids copying bodies that the encoder omits.
				data["body"] = fact.Body
				if fact.Body == nil {
					data["body"] = []byte{}
				}
				if content.MessageType != "" {
					data["message_type"] = content.MessageType
				}
				if content.WriteResult != "" {
					data["write_result"] = content.WriteResult
				}
			} else {
				data["last_stream_sequence"], data["total_body_bytes"], data["end_reason"] = content.LastStreamSequence, content.TotalBodyBytes, content.EndReason
			}
			if content.ErrorCode != "" {
				data["error_code"] = content.ErrorCode
			}
			if content.CloseCode != 0 {
				data["close_code"] = content.CloseCode
			}
		} else {
			data = map[string]any{
				"request_id": spec.RequestID, "attempt_id": spec.AttemptID, "attempt_sequence": spec.Sequence,
				"exchange_sequence": fact.Sequence, "channel_id": spec.ChannelID,
				"credential_id": spec.Credential.ID, "credential_version": spec.Credential.Version,
				"identity_generation": spec.Credential.IdentityGeneration,
				"model":               spec.UpstreamModel, "protocol": fact.Protocol, "stream": stream,
				"phase": fact.Phase, "headers": fact.Header.Clone(),
			}
		}
		if fact.Method != "" {
			data["method"] = fact.Method
		}
		if fact.URL != "" {
			data["url"] = fact.URL
		}
		if fact.StatusCode != 0 {
			data["status_code"] = fact.StatusCode
		}
		if fact.Body != nil {
			data["body"] = fact.Body
		}
		if fact.UpstreamConnectionID != "" {
			data["upstream_connection_id"] = fact.UpstreamConnectionID
		}
		return data
	})
}
