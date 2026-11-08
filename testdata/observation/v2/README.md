These canonical v2 MessagePack samples are shared byte-for-byte with the GPT-Load host. Both modules maintain their own protocol types and validate this contract. Identities and payloads are synthetic.

The samples cover request results, HTTP and SSE initialization, WebSocket handshakes, raw SSE chunks and end counters, serialized WebSocket writes, binary reads, connection-direction ends, empty payloads, body omission, oversized degradation, measured zeros and unknown optional fields. `subscribe` is a control command. Binary bodies retain MessagePack binary encoding.
