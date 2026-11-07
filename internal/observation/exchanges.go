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
		data := map[string]any{
			"request_id": spec.RequestID, "attempt_id": spec.AttemptID, "attempt_sequence": spec.Sequence,
			"exchange_sequence": fact.Sequence, "channel_id": spec.ChannelID,
			"credential_id": spec.Credential.ID, "credential_version": spec.Credential.Version,
			"identity_generation": spec.Credential.IdentityGeneration,
			"model":               spec.UpstreamModel, "protocol": fact.Protocol, "stream": stream,
			"phase": fact.Phase, "headers": fact.Header,
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
