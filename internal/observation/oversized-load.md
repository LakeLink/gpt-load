# Large observation resource measurements

Measured on 2026-11-07 with Go `go1.27.3-X:nodwarf5`, Linux amd64, Intel i5-1240P, 16 logical CPUs. The workload runs locally through TLS. These short runs include shared-machine scheduling noise; they document resource behavior and provide commands for comparison on deployment hardware.

## Encoding, temporary allocation and retained source data

```sh
mkdir -p tmp
TMPDIR="$PWD/tmp" GOTMPDIR="$PWD/tmp" OBSERVATION_MEASURE_ENCODING=1 \
  go test ./internal/observation -run '^TestOversizedEncodingResourceMeasurement$' -count=1 -v
```

Each profile starts two real WSS subscriptions, closes both connections, and synchronously publishes 128 observations to the retained offline subscriptions. The source payload is constructed once and reused read-only. Allocation sampling starts after its construction and a GC; a subsequent GC occurs after the source leaves scope. Each queue has a 200-record / 4,000,000-byte budget, with a 1,048,576-byte degradation threshold. The test reconnects both clients and counts actual retained MessagePack frame lengths.

| Source payload | Time per observation, two plugins | Allocated bytes / observation | Allocations / observation | Retained records, both queues | Retained encoded bytes, both queues |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 MiB binary body | 0.332 ms | 14,085 | 50 | 256 | 37,378 |
| 8 MiB response header | 0.447 ms | 14,126 | 53 | 256 | 39,938 |
| 1,200,000-byte model metadata | 0.398 ms | 1,218,184 | 53 | 6 | 7,201,004 |

The body profile retains normal records with `body_omitted`; the header profile retains `oversized` records. Metadata remains in the oversized data map and exceeds the degradation threshold. Each metadata queue retains three records totaling about 3.60 MB, within its hard budget. Plugins at the same sequence share each immutable encoded frame, so the two queues' logical encoded bytes may reference the same allocation.

| Source payload | Heap with source before publishing | Heap immediately after publishing | Heap after source release and GC | Post-GC change from pre-source baseline |
| --- | ---: | ---: | ---: | ---: |
| 8 MiB body | 9,130,504 B | 10,933,416 B | 782,160 B | -58,480 B |
| 8 MiB header | 9,149,016 B | 10,957,208 B | 791,776 B | -36,480 B |
| 1.2 MB metadata | 1,973,480 B | 9,231,664 B | 4,380,928 B | 3,429,216 B |

The large body/header source storage leaves the heap after publication and GC. The small negative baseline changes reflect unrelated connection/TLS objects released between samples. Metadata's post-GC heap increase reflects three shared encoded allocations. Immediate heap samples include garbage awaiting collection and runtime/TLS state. The allocation deltas isolate the synchronous publish loop, including encoder scratch, maps, queue storage and diagnostics field construction.

The size probe uses a 4 KiB codec writer buffer and counts every encoded byte. It copies through that fixed scratch buffer without retaining a body-sized temporary encoding. Only the final accepted projection gets an exact-sized output allocation. Body/header removal clones the field map, keeping source values read-only. One encoding task per service runs at a time. The service creates no encoding goroutines or additional pending queues.

## Whole-request cost with existing gateway and log persistence

The lifecycle load harness also supports these profiles:

```sh
TMPDIR="$PWD/tmp" GOTMPDIR="$PWD/tmp" OBSERVATION_MEASURE_LOAD=1 \
  OBSERVATION_LOAD_PROFILE=large-body \
  go test ./internal/container -run '^TestObservationLoadComparison$' -count=1 -v
TMPDIR="$PWD/tmp" GOTMPDIR="$PWD/tmp" OBSERVATION_MEASURE_LOAD=1 \
  OBSERVATION_LOAD_PROFILE=large-headers \
  go test ./internal/container -run '^TestObservationLoadComparison$' -count=1 -v
```

Both profiles warm up 20 requests and measure 100 successful streaming Responses requests at concurrency four. `large-body` uses a 1,200,000-byte input string. `large-headers` uses the usual 16 KiB input and adds a 1,200,000-byte upstream response header. Both use the real container, gateway, TLS upstream, request-log chain, and HTTP/SSE observation hooks. The default profile remains the original 600-request, concurrency-eight lifecycle comparison. See [lifecycle-load.md](lifecycle-load.md) for sampling methodology and mode definitions.

| Payload | Mode | Requests/s | Latency p50 / p95 ms | First byte p50 / p95 ms | CPU ms/request | Allocated KiB/request | Allocations/request |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Large body | disabled | 77.4 | 51.424 / 59.526 | 50.971 / 59.296 | 60.899 | 57,753.4 | 4,906 |
| Large body | no subscription | 71.8 | 54.604 / 69.554 | 54.482 / 69.233 | 65.167 | 57,759.6 | 4,889 |
| Large body | online | 76.0 | 51.513 / 60.367 | 51.125 / 59.983 | 62.235 | 57,818.5 | 5,160 |
| Large body | offline full | 76.7 | 51.284 / 60.154 | 51.021 / 59.617 | 61.900 | 57,783.6 | 5,049 |
| Large body | slow consumer | 76.0 | 52.018 / 60.571 | 51.461 / 60.305 | 62.480 | 57,810.4 | 5,110 |
| Large headers | disabled | 299.4 | 12.909 / 18.590 | 12.716 / 18.425 | 19.552 | 10,311.9 | 3,959 |
| Large headers | no subscription | 304.5 | 12.940 / 16.488 | 12.715 / 16.369 | 19.506 | 10,303.2 | 3,928 |
| Large headers | online | 299.4 | 13.036 / 17.362 | 12.644 / 17.023 | 19.923 | 10,408.4 | 4,187 |
| Large headers | offline full | 291.7 | 13.182 / 16.292 | 12.851 / 16.120 | 20.154 | 10,345.5 | 4,045 |
| Large headers | slow consumer | 294.9 | 12.942 / 17.864 | 12.745 / 17.498 | 21.058 | 10,380.9 | 4,148 |

| Payload | Mode | Post-GC live heap MiB | Sampled peak heap MiB | Goroutines baseline / peak | Retained events | Retained encoded bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Large body | disabled | 81.38 | 216.15 | 29 / 77 | 0 | 0 |
| Large body | no subscription | 81.57 | 208.02 | 28 / 74 | 0 | 0 |
| Large body | online | 81.39 | 208.47 | 31 / 79 | 0 | 0 |
| Large body | offline full | 81.79 | 206.54 | 27 / 78 | 200 | 184,491 |
| Large body | slow consumer | 82.84 | 209.77 | 31 / 77 | 0 | 0 |
| Large headers | disabled | 16.11 | 59.22 | 14 / 49 | 0 | 0 |
| Large headers | no subscription | 16.24 | 57.23 | 17 / 49 | 0 | 0 |
| Large headers | online | 16.25 | 60.81 | 20 / 51 | 0 | 0 |
| Large headers | offline full | 17.61 | 62.74 | 17 / 77 | 200 | 1,297,023 |
| Large headers | slow consumer | 17.65 | 53.38 | 20 / 52 | 0 | 0 |

Small degraded records fit the local socket buffers in the slow-reader cases, so their queues drained before sampling; the explicitly offline modes retain 200 records and evict old records under the count budget. Whole-host live and peak memory include request bodies, the unchanged JSON/SDK conversion path, persisted request data, database work, logs, TLS and runtime caches. In particular, large-body business requests already allocate about 57.8 MiB/request with observation disabled; the separate encoder workload above shows the event encoder's bounded temporary allocation and release of the omitted source data.

All business requests completed in every mode. Focused contracts separately verify exact threshold and hard-budget equality, both transport-side omissions, a single final record, metadata preserved above the threshold, sequence gaps and safe diagnostics for rejected records, and queue accounting against the final encoded projection. A real WSS regression sends a large business request with a large upstream header and checks successful response, correlated process/result events and the established request-log body record.
