# Observation lifecycle load comparison

Measured on 2026-11-07 with Go `go1.27.3-X:nodwarf5`, Linux amd64, 16 logical CPUs. This run uses the lifecycle implementation atop integration commit `37b74b52`, whose producer publishes `request.finished`. Follow-up producer work should repeat the same harness for its additional events.

Reproduce from the repository root:

```sh
mkdir -p tmp
TMPDIR="$PWD/tmp" GOTMPDIR="$PWD/tmp" OBSERVATION_MEASURE_LOAD=1 \
  go test ./internal/container -run '^TestObservationLoadComparison$' -count=1 -v
```

All modes use one local TLS Codex upstream and the real TLS host/container/gateway/log chain. Each mode warms up 20 requests, then issues 600 successful streaming Responses requests at concurrency eight with a 16 KiB input and the same fixed completion/usage response. The observation queue uses 200 events and 4,000,000 encoded bytes; write timeout is one second for this workload. Online consumption reads continuously; offline collection begins after a completed WebSocket close handshake; slow consumption reads one record every 20 ms. Logging output is discarded for each mode, while the normal request-log persistence path remains active. Modes run sequentially in a shared test process, so later modes retain warmed global caches.

Each row is one short local run, useful for detecting network backpressure or resource growth. CPU and throughput vary with shared-machine scheduling; these figures provide a reproducible observation rather than a production capacity estimate. First-response measurements are client wall time to the first response body byte, including host/upstream processing and local TLS transport.

| Mode | Requests/s | Wall ms | Latency p50 / p95 ms | First byte p50 / p95 ms | CPU ms/request | Allocated KiB/request | Allocations/request |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| disabled | 1866.9 | 321 | 4.019 / 7.421 | 3.845 / 7.173 | 5.706 | 1426.2 | 4016 |
| no-subscription | 1914.6 | 313 | 3.975 / 6.891 | 3.789 / 6.514 | 5.267 | 1414.2 | 3930 |
| online | 1822.8 | 329 | 4.146 / 7.131 | 3.817 / 6.625 | 5.794 | 1493.6 | 4157 |
| offline-full | 1956.1 | 306 | 3.879 / 5.924 | 3.621 / 5.716 | 5.364 | 1443.8 | 4101 |
| slow-consumer | 1987.0 | 301 | 3.821 / 6.235 | 3.562 / 5.919 | 5.176 | 1443.5 | 4090 |

CPU is process user plus system time from `getrusage`; cumulative allocation and allocation counts come from `runtime.MemStats`. A 5 ms sampler records heap and goroutine peaks. Live heap is measured after request handlers finish and a full GC, while the configured log service and database remain alive.

| Mode | Live heap MiB | Live heap increase MiB | Sampled peak heap MiB | Peak minus post-GC live MiB | Goroutines baseline / peak | Retained records | Retained encoded bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| disabled | 35.88 | 22.84 | 81.00 | 45.12 | 14 / 82 | 0 | 0 |
| no-subscription | 36.60 | 23.15 | 71.56 | 34.96 | 17 / 79 | 0 | 0 |
| online | 36.52 | 22.80 | 73.58 | 37.06 | 20 / 78 | 0 | 0 |
| offline-full | 40.17 | 26.21 | 78.53 | 38.36 | 17 / 80 | 200 | 3530200 |
| slow-consumer | 39.23 | 25.63 | 78.35 | 39.12 | 20 / 74 | 200 | 3530200 |

Retained encoded bytes are counted from actual queued MessagePack frame lengths after the heap sample and reconnect/subscribe. The offline and slow-consumer queues each contain 200 records totaling 3,530,200 bytes, within both budgets. The online reader consumed 10,943,210 encoded bytes over warmup and measurement; the slow reader consumed 317,654 bytes before it stopped. Queue encoding is therefore listed separately from the whole-host heap.

The peak-minus-live column describes observed transient whole-host heap, including request bodies, encoding scratch allocations, gateway/SDK work, TLS, log buffers, database work, and garbage awaiting collection. It also reflects the sampling interval; queue budget controls retained encoded data, while whole-host transient memory follows the workload. Live heap increase includes persisted request data and runtime/cache growth, so it should be compared across the five modes alongside the explicit encoded queue count.

The offline and stalled consumers completed all business requests while their queues evicted older records. A separate deterministic transport-stall regression verifies the configured write timeout, single send attempt for the dequeued record, recovery of remaining records after reconnect, and shutdown within a 30 ms shared deadline. Focused race checks include cancellation during snapshot construction, replacement cleanup, queue recovery, and the real application shutdown ordering for upgraded business connections.
