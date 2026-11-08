# Codex account privacy and routing state

The Codex execution path derives client identifiers after selecting an account. The input is the credential's persisted `IdentityFingerprint`, copied into an execution snapshot without adding database or credential serialization fields. HTTP/SSE, Responses WebSocket and live requests retain their selected account identity throughout execution.

## Deterministic identity

The application UUID namespace is `ce12d2ae-fc14-5a53-bfe6-1aa859d0c91a`. The account base is UUIDv5 over `gpt-load/codex-privacy/v1/` followed by the exact stored fingerprint. Installation ID is UUIDv5 under that base with the name `installation/v1`.

Client UUIDs use the common `client-id/v1/` purpose followed by their canonical UUID representation. Equal original UUIDs therefore stay equal across supported session, thread, turn, parent, root, context-window and fork fields. Opaque values use the separate `opaque-client-id/v1/` purpose. Composite window IDs retain their window number; the pinned CLI's `guardian:UUID` and `memory_consolidation:UUID` affinity values retain the source prefix.

The allowlist covers supported headers, direct `client_metadata`, JSON text in `x-codex-turn-metadata`, and the body-level `prompt_cache_key`. Installation metadata is supplied for every supported inference request, including translated Responses requests. Missing, null and empty optional IDs retain their meaning. Malformed metadata, duplicate fields in parsed metadata containers, and missing account fingerprints fail during preparation. Context, tools, output, server response IDs and unrelated metadata retain their existing handling.

The account identity cache uses a separate bounded LRU with 10,000 entries. Eviction, clearing and process restart recompute the same values. Normal token refresh keeps the stored fingerprint. Duplicate credential rows with equal fingerprints intentionally share privacy IDs; reimporting an account with a changed fingerprint changes its namespace.

## Cookies and logical turns

Runtime routing state is partitioned by frozen account fingerprint and execution target scope, including identity generation and proxy configuration. A host process represents an independent client instance for each account. Restart clears runtime routing state while preserving deterministic identity.

Cookies use the pinned official Codex infrastructure allowlist and trusted ChatGPT hosts. Standard Go cookie-jar rules and the public suffix list enforce domain, path, secure transport, expiry and deletion. Authentication cookies are excluded. Final HTTP and WebSocket preparation replaces incoming and intermediate Cookie/Cookie2 headers with the selected account's matching entries. HTTP and WebSocket share that account's jar. Set-Cookie capture precedes downstream filtering.

Cookie storage is bounded to 256 account/target partitions, 32 cookies per partition, and 4,096 bytes per accepted Set-Cookie. Session cookies and idle partitions expire after 24 hours. Persistent cookie expiry follows the server's Max-Age/Expires. Evicted, expired or explicitly closed partitions reject late writes through previously acquired leases.

Turn-state is an opaque first-value hint scoped to the account, target and original logical turn. When no turn ID is available, the gateway's logical request ID groups its retries. Current WebSocket message metadata takes precedence over historical handshake metadata; header-only clients retain their explicit turn scope. Header and body replay values are frozen together for each attempt; subsequent attempts see values captured in the meantime.

Capture reads HTTP/WS handshake headers and `codex.response.metadata`. HTTP metadata is read after decompression and before CPA parsing, even without an observation subscriber. Streaming inspection retains at most 64 KiB of one metadata event and returns the original bytes and errors. The first valid state is retained exactly; unowned inbound hints are replaced or removed.

Logical turns survive individual `response.completed` events and WebSocket reconnects. The store holds at most 4,096 turns, with one hour of idle lifetime and at most 16 KiB per state. Expiration, eviction and target identity changes select independent state; old leases cannot repopulate replacement entries. The upstream protocol provides no explicit logical-turn termination signal used by this integration, so ordinary response completion relies on this bounded idle lifecycle.

Existing response ownership checks and WebSocket account binding continue to govern `previous_response_id` and continuation. This feature leaves those server values intact.

## TLS profile

Scoped Codex connections use the rustls 0.23.46/AWS-LC configuration from official Codex 0.166.0, commit `6ff1e1ca3b30fc8b914720681fcf5fe62e95f856`. HTTP/SSE, live HTTP operations, token refresh, account queries and model discovery advertise `h2,http/1.1`; Responses and live sideband WebSocket use the corresponding empty-ALPN profile. This selects the official rustls HTTP path explicitly; the CLI's ordinary platform-native HTTP implementation has a different profile.

The shared CPA `sdk/codextls` implementation uses uTLS `v1.8.3-0.20261108164504-a13c59ffe98b`, containing the hybrid HelloRetryRequest fix and ML-DSA certificate verification. CPA and embedded require Go 1.27.0; the host continues to require Go 1.27.3. The host pins CPA to `v8.0.60-0.20261108183735-e79653d5320f`. Native Rust reference generators, Cargo lock/features, raw initial/retry/resumed ClientHello samples and normalization rules accompany the CPA module under `sdk/codextls/testutil/testdata/rustls-0.23.46`. Comparisons check full fields and the reference's randomized extension ordering, with randomness, key material and ticket material normalized structurally.

Certificate and hostname verification, system roots, `SSL_CERT_FILE`, optional `CODEX_CA_CERTIFICATE`, proxy routing and cancellation remain enabled. Direct, CONNECT and SOCKS tests observe the target-side TLS handshake. A deployment proxy that terminates TLS establishes its own upstream fingerprint. Plain HTTP custom targets retain their existing non-TLS transport semantics. HTTP/2 framing and whole-traffic classification are outside this TLS handshake contract.

TLS session caches are bounded and partitioned by account/credential and route; credential replacement and ordinary access-token refresh start a fresh TLS session scope. Auxiliary HTTPS uses account-scoped TLS resources and keeps its existing credential/control-plane behavior. Inference CookieJar and logical-turn state are attached to execution attempts; auxiliary account/model/refresh operations do not share that inference routing store or carry incoming turn metadata.

## Observation and verification

See [capture stages](observation-stream-v2.md) for the distinction between bridge snapshots and final transmitted values. Account policy runs before CPA construction and again at final header preparation. The receiver is the authority for Cookie, turn-state, privacy headers and ClientHello; recorder events continue to correlate requests, attempts, exchanges and connections.

The native privacy integration test drives a real host through a controlled CONNECT proxy to a TLS upstream with a trusted `chatgpt.com` certificate. It exercises two accounts, retries, HTTP/SSE, WebSocket continuation, real credential refresh, host recreation, cookie sharing, first-value state and standalone recorder correlation. Small contract tests cover deterministic vectors, cache eviction, malformed metadata and concurrent/late state capture. No schema migration or per-account identity file is required.
