# Observation protocol v1

Configure `OBSERVATION_PLUGINS` as a JSON array of unique, nonempty `id` and `token` pairs. Connect to `wss://<host>/api/plugins/<plugin_id>/events` through the deployment's existing HTTPS service. Supply `Authorization: Bearer <plugin-token>` and subprotocol `gpt-load.observation.v1.msgpack`. Authentication errors return HTTP 401; invalid upgrades and subprotocols return HTTP 400. Plugin credentials grant access to the exposed observation data, including existing request bodies.

Each binary WebSocket message contains one MessagePack map with string keys. Strings use MessagePack string values; bodies use binary values. The control read limit is 32 KiB. Send a map containing `type: subscribe` and a nonempty `command_id`. The reply contains `type: subscribed`, the same `command_id`, `boot_id`, and `max_event_bytes`. Set the client's read limit to `max_event_bytes` before reading events. This confirmation precedes the connection's first observation. Repeated subscription reuses the queue. Send `unsubscribe` with a command ID to clear the queue and receive `unsubscribed` with the same command ID and boot ID.

Control errors have `type: error`, a stable `code`, a short `message`, and `command_id` when available. Codes are `invalid_message`, `unsupported_message`, and `unknown_command`. Malformed encoding closes the connection after the error reply. Text messages receive `unsupported_message`.

The event envelope contains `type`, `boot_id`, `seq`, `observed_at`, and `data`. The boot ID remains stable during one host process; each plugin's uint64 sequence increases across cancellation and resubscription. Timestamps are UTC Unix milliseconds. Reconnection preserves queued timestamps. Queues evict the oldest records when either the event count or encoded byte budget is exceeded. Collection starts on explicit subscription; unexpected disconnects retain the subscription and its bounded queue.

`request.finished` carries the existing Codex subscription request result: request ID, completion time, access key ID, protocol, operation, client and upstream model, model consistency, final status, status code, total duration, stream intent, affinity, optional reasoning and first-response duration, full attempts, usage and calculated pricing. Attempts retain dispatch, response-started, committed, retry, failure origin/scope, effect, cooldown, and error details from the request log. Optional errors, reported model, request ID from the upstream, and existing recorded request bodies are included when available. Unknown first-response durations are omitted; measured zero values remain present. Audit internals, automatic-selection internals, and pricing receipts stay in the host log chain.

The independent binary samples in [testdata/observation/v1](../../testdata/observation/v1) fix the field and primitive-type contract for clients:

- `subscribe.msgpack`: `{"type":"subscribe","command_id":"sample-command"}`.
- `request-finished.msgpack`: successful unary `gpt-5` Responses outcome with input/output token counts 3/2, zero cache tokens and cost, and a binary request body containing `{"model":"gpt-5","input":"hello"}`. Dynamic identity/time values use fixed sample values. First-response duration is absent for this sample.

The host integration test sends the control sample directly and compares the received result's stable fields, binary body, usage, and pricing with the result sample. Further process event and size-degradation support extends this same envelope.
