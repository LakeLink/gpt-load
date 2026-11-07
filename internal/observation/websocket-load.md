# Codex WebSocket observations and concurrent workload

Measured on 2026-11-07 with Go `go1.27.3-X:nodwarf5`, Linux amd64, on the implementation based on integration commit `45f05862`.

Reproduce from the repository root:

```sh
mkdir -p tmp
TMPDIR="$PWD/tmp" GOTMPDIR="$PWD/tmp" \
  go test ./internal/container -run '^TestObservationWebsocketConnections$' -count=1 -v
```

The test runs the real TLS host, container, gateway, Codex credential preparation, embedded CPA bridge, observation subscription, and request log persistence. CPA creates its own WebSocket dialer. A helper process receives the local test CA through `SSL_CERT_FILE` before its first system-root lookup; production HTTPS validation and certificate verification remain active. This follows the [Go system root loading contract](https://pkg.go.dev/crypto/x509#SystemCertPool). Connection identities originate at the existing Gorilla [GotConn trace hook](https://pkg.go.dev/net/http/httptrace#ClientTrace), separately from SDK session handles.

The functional scenario observes two serial turns, including continuation, on one actual connection, closes the client, and repeats with a fresh physical connection and identity. Every turn has a distinct host request ID and its current attempt identity. The first turn has handshake request/response and turn request facts; the next turn has one request fact and retains its connection ID. Raw multi-value headers, 101 upgrade status, prepared binary payload, final attempts, first-response duration, and total duration are checked against the upstream, canonical binary samples, and persisted host logs. Another TLS upstream rejects the handshake with 403; its raw headers and available status join the failed final attempt. A `stream_id` request receives the existing capability rejection and produces zero upstream connections or transport facts.

The concurrent scenario compares an authenticated plugin without a subscription against the same plugin actively consuming observations. Each mode runs eight independent actual upstream WebSocket connections, four serial turns per connection, and small fixed request/completion payloads. First turns block upstream until all eight connections have arrived, establishing actual concurrency. The subscribed mode consumes 32 outcomes, their turn facts, and eight handshake pairs while checking stable per-client connection identity and exact request/attempt/credential association. Both modes finish all 32 requests and close all eight upstream connections.

| Mode | Wall ms for 32 turns | Wall ns/turn | Allocations/turn | Allocated bytes/turn | Heap before / concurrent sample / after GC, bytes | Goroutines before / concurrent / after |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| No subscription | 14.326 | 447697 | 2764 | 552763 | 21426768 / 26998552 / 17714496 | 31 / 71 / 15 |
| Online subscription | 17.364 | 542611 | 3439 | 600020 | 17881368 / 23287496 / 18155696 | 31 / 71 / 15 |

These are whole-host/client in-process measurements from one short local run. Wall ns/turn divides the concurrent workload duration by completed turns. Allocation counters use `runtime.MemStats` deltas and include gateway, SDK, TLS, plugin encoding/decoding, and asynchronous host log work. The concurrent heap/goroutine sample is taken while all first-turn upstreams are blocked; it is a specified sampling point, rather than a continuously sampled maximum. Final heap follows connection cleanup and GC while the fixture, database, logs, plugin connection, and global caches remain alive. The second mode benefits from warmed process state. The figures describe this workload and scheduling, with repeatable assertions on association and cleanup.

All 16 measured upstream connections exit. Goroutines settle at 15 in both modes, below the baseline containing eight open downstream client connections. The online run adds about 95 microseconds of whole-workload wall time, 675 allocations, and 47,257 allocated bytes per completed turn in this sample. Heap after GC differs by 441,200 bytes with both configured host and log chain still alive.

The failure source has one precision limitation: CPA's typed handshake error can normalize quota/capacity statuses to 429. The raw headers are recovered through its native fresh response-header holder. Those process events omit `status_code` because the original status is unavailable; final outcome status/error/retry semantics continue to use the host's established evidence. The embedded regression covers this omission and raw header preservation.

Validation completed for this change:

- Red: the real trusted TLS WS request completed successfully while the plugin received only `request.finished`; the first-turn observation chain assertion failed. The failed-handshake extension also exposed the SDK error's missing raw headers before using its native header holder.
- Green: the trusted TLS integration above, including serial continuation, reuse, recreation, capability rejection, concurrent association, cleanup, canonical samples, and host timing comparison.
- Main module: full tests for `internal/execution/cpa`, `internal/subscription/providers/codex`, `internal/gateway`, `internal/container`, `internal/observation`, and `internal/requestlog`; focused real WS integration under `-race`; affected-package `go vet`; `go mod tidy -diff`.
- Independent embedded module: focused `TestCodexWS|TestCodex.*Header|TestCodex.*Compress` tests, `go vet ./...`, and `go mod tidy -diff`. Its dedicated module race suite runs in CI under `CONTRIBUTING.md`.
