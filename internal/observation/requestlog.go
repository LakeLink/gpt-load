package observation

import (
	"gpt-load/internal/channel"
	"gpt-load/internal/telemetry"
	"gpt-load/internal/usage"
)

// RequestLogSink 在既有日志装配边界复用请求完成事实。
type RequestLogSink struct {
	Logs         telemetry.RequestLogSink
	Observations *Service
}

func (sink RequestLogSink) Emit(event telemetry.RequestEvent) {
	// 先推进既有日志链；插件出口自行消化编码和网络失败。
	sink.Logs.Emit(event)
	for _, attempt := range event.Attempts {
		if attempt.ChannelID == channel.Codex {
			sink.Observations.Publish("request.finished", event.CompletedAt, func() any { return finishedData(event) })
			return
		}
	}
}

func finishedData(event telemetry.RequestEvent) map[string]any {
	data := map[string]any{
		"request_id": event.RequestID, "completed_at": event.CompletedAt.UTC().UnixMilli(),
		"access_key_id": event.AccessKeyID, "protocol": event.Protocol, "operation": event.Operation,
		"client_model": event.ClientModel, "upstream_model": event.UpstreamModel,
		"model_consistency": event.ModelConsistency, "status": event.Status, "status_code": event.StatusCode,
		"stream": event.Stream, "duration_ms": event.DurationMs, "affinity_hit": event.AffinityHit,
		"attempts": make([]map[string]any, 0, len(event.Attempts)), "usage": usageData(event.Usage),
	}
	if event.ClientIP != "" {
		data["client_ip"] = event.ClientIP
	}
	if event.UpstreamReportedModel != "" {
		data["upstream_reported_model"] = event.UpstreamReportedModel
	}
	if event.ErrorCode != "" {
		data["error_code"] = event.ErrorCode
	}
	if event.ErrorSummary != "" {
		data["error_summary"] = event.ErrorSummary
	}
	if event.FirstResponseMs != nil {
		data["first_response_ms"] = *event.FirstResponseMs
	}
	if event.AffinityKind != "" {
		data["affinity_kind"] = event.AffinityKind
	}
	if event.Reasoning.Present() {
		data["reasoning"] = reasoningData(event.Reasoning.Mode, event.Reasoning.Effort, event.Reasoning.BudgetTokens)
	}
	if event.RequestBody != nil {
		data["request_body"] = event.RequestBody
	}
	attempts := data["attempts"].([]map[string]any)
	for _, a := range event.Attempts {
		attempt := map[string]any{
			"sequence": a.Sequence, "completed_at": a.CompletedAt.UTC().UnixMilli(), "group_id": a.GroupID, "group_name": a.GroupName,
			"channel_id": a.ChannelID, "credential_id": a.CredentialID, "operation": a.Operation, "route_mode": a.RouteMode,
			"upstream_model": a.UpstreamModel, "dispatch_state": a.DispatchState, "response_started": a.ResponseStarted,
			"upstream_protocol": a.UpstreamProtocol, "status_code": a.StatusCode, "duration_ms": a.DurationMs,
			"failure_category": a.FailureCategory, "failure_origin": a.FailureOrigin, "failure_scope": a.FailureScope,
			"retry_directive": a.RetryDirective, "effect": a.Effect, "action": a.Action, "will_retry": a.WillRetry, "committed": a.Committed,
		}
		if a.AttemptID != "" {
			attempt["attempt_id"] = a.AttemptID
			attempt["credential_version"] = a.CredentialVersion
			attempt["identity_generation"] = a.IdentityGeneration
		}
		if a.UpstreamConnectionID != "" {
			attempt["upstream_connection_id"] = a.UpstreamConnectionID
		}
		if a.UpstreamRequestID != "" {
			attempt["upstream_request_id"] = a.UpstreamRequestID
		}
		if a.Reasoning.Present() {
			attempt["reasoning"] = reasoningData(a.Reasoning.Mode, a.Reasoning.Effort, a.Reasoning.BudgetTokens)
		}
		if !a.CooldownUntil.IsZero() {
			attempt["cooldown_until"] = a.CooldownUntil.UTC().UnixMilli()
		}
		if a.RuleID != "" {
			attempt["rule_id"] = a.RuleID
		}
		if a.ErrorCode != "" {
			attempt["error_code"] = a.ErrorCode
		}
		if a.ErrorSummary != "" {
			attempt["error_summary"] = a.ErrorSummary
		}
		attempts = append(attempts, attempt)
	}
	data["attempts"] = attempts
	return data
}

func reasoningData(mode, effort string, budget *int64) map[string]any {
	data := map[string]any{}
	if mode != "" {
		data["mode"] = mode
	}
	if effort != "" {
		data["effort"] = effort
	}
	if budget != nil {
		data["budget_tokens"] = *budget
	}
	return data
}

func tokenData(tokens usage.Tokens) map[string]any {
	return map[string]any{"uncached_input": tokens.UncachedInput, "cache_read": tokens.CacheRead, "cache_write_5m": tokens.CacheWrite5M, "cache_write_1h": tokens.CacheWrite1H, "cache_write_unknown": tokens.CacheWriteUnknown, "output": tokens.Output}
}
func usageData(observation telemetry.UsageObservation) map[string]any {
	data := map[string]any{"state": observation.Result.State, "tokens": tokenData(observation.Result.Tokens), "group_id": observation.GroupID, "channel_id": observation.ChannelID, "credential_id": observation.CredentialID, "attempt_sequence": observation.AttemptSequence, "pricing": map[string]any{"upstream_model": observation.Pricing.UpstreamModel, "cost_state": observation.Pricing.CostState, "pricing_completeness": observation.Pricing.PricingCompleteness, "estimated_cost_nano_usd": observation.Pricing.EstimatedCostNanoUSD}}
	if observation.Result.Advisor != nil {
		calls := make([]map[string]any, 0, len(observation.Result.Advisor.Calls))
		for _, call := range observation.Result.Advisor.Calls {
			calls = append(calls, map[string]any{"model": call.Model, "tokens": tokenData(call.Tokens)})
		}
		data["advisor"] = calls
	}
	return data
}
