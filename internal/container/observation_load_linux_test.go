package container

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/sirupsen/logrus"

	"gpt-load/internal/platform/config"
)

// 显式运行负载观测；日常测试保留轻量生命周期回归。
func TestObservationLoadComparison(t *testing.T) {
	if os.Getenv("OBSERVATION_MEASURE_LOAD") != "1" {
		t.Skip("set OBSERVATION_MEASURE_LOAD=1 for same-upstream load measurements")
	}
	previous := logrus.StandardLogger().Out
	logrus.SetOutput(io.Discard)
	defer logrus.SetOutput(previous)
	upstream := observationLocalUpstream()
	defer upstream.Close()
	const requests, workers = 600, 8
	payload := `{"model":"gpt-5","input":"` + strings.Repeat("x", 16<<10) + `","stream":true}`
	for _, mode := range []string{"disabled", "no-subscription", "online", "offline-full", "slow-consumer"} {
		t.Run(mode, func(t *testing.T) {
			fixture := newObservationHost(t, upstream, func(cfg *config.Config) {
				if mode == "disabled" {
					cfg.Observation.Plugins = nil
				}
				cfg.Observation.WriteTimeoutSeconds = 1
			})
			var plugin *websocket.Conn
			var received atomic.Int64
			var retainedBytes, retainedEvents int64
			consumerDone := make(chan struct{})
			var stopConsumer context.CancelFunc = func() {}
			if mode == "online" || mode == "offline-full" || mode == "slow-consumer" {
				plugin = fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
				plugin.SetReadLimit(4000000)
				fixture.command(t, plugin, "subscribe", "load")
				readObservation(t, plugin)
				if mode == "offline-full" {
					plugin.Close(websocket.StatusNormalClosure, "offline")
				} else {
					consumeCtx, cancel := context.WithCancel(t.Context())
					stopConsumer = cancel
					go func() {
						defer close(consumerDone)
						for {
							_, body, err := plugin.Read(consumeCtx)
							if err != nil {
								return
							}
							received.Add(int64(len(body)))
							if mode == "slow-consumer" {
								select {
								case <-time.After(20 * time.Millisecond):
								case <-consumeCtx.Done():
									return
								}
							}
						}
					}()
				}
			}
			// 预热同一业务链，排除 TLS 和运行时初始化的首批开销。
			doRequest := func() (time.Duration, time.Duration, error) {
				started := time.Now()
				req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, fixture.server.URL+"/v1/responses", strings.NewReader(payload))
				req.Header.Set("Authorization", "Bearer client-test-key")
				req.Header.Set("Content-Type", "application/json")
				response, err := fixture.server.Client().Do(req)
				if err != nil {
					return 0, 0, err
				}
				defer response.Body.Close()
				first := make([]byte, 1)
				_, err = response.Body.Read(first)
				firstTime := time.Since(started)
				if err != nil && err != io.EOF {
					return 0, 0, err
				}
				_, err = io.Copy(io.Discard, response.Body)
				if response.StatusCode != 200 {
					return 0, 0, fmt.Errorf("status %d", response.StatusCode)
				}
				return time.Since(started), firstTime, err
			}
			for range 20 {
				if _, _, err := doRequest(); err != nil {
					t.Fatal(err)
				}
			}
			runtime.GC()
			var before, after, retained runtime.MemStats
			runtime.ReadMemStats(&before)
			var cpuBefore, cpuAfter syscall.Rusage
			syscall.Getrusage(syscall.RUSAGE_SELF, &cpuBefore)
			baselineGoroutines := runtime.NumGoroutine()
			var peakGoroutines atomic.Int64
			peakGoroutines.Store(int64(baselineGoroutines))
			sampleDone, sampled := make(chan struct{}), make(chan struct{})
			var peakHeap atomic.Uint64
			go func() {
				defer close(sampled)
				ticker := time.NewTicker(5 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-sampleDone:
						return
					case <-ticker.C:
						count := int64(runtime.NumGoroutine())
						for count > peakGoroutines.Load() {
							old := peakGoroutines.Load()
							if count <= old || peakGoroutines.CompareAndSwap(old, count) {
								break
							}
						}
						var mem runtime.MemStats
						runtime.ReadMemStats(&mem)
						for mem.HeapAlloc > peakHeap.Load() {
							old := peakHeap.Load()
							if mem.HeapAlloc <= old || peakHeap.CompareAndSwap(old, mem.HeapAlloc) {
								break
							}
						}
					}
				}
			}()
			latencies, firstTimes := make([]time.Duration, requests), make([]time.Duration, requests)
			errors := make(chan error, workers)
			var next atomic.Int64
			var group sync.WaitGroup
			started := time.Now()
			for range workers {
				group.Go(func() {
					for {
						index := int(next.Add(1)) - 1
						if index >= requests {
							return
						}
						duration, first, err := doRequest()
						if err != nil {
							errors <- err
							return
						}
						latencies[index], firstTimes[index] = duration, first
					}
				})
			}
			group.Wait()
			elapsed := time.Since(started)
			close(sampleDone)
			<-sampled
			close(errors)
			for err := range errors {
				t.Fatal(err)
			}
			syscall.Getrusage(syscall.RUSAGE_SELF, &cpuAfter)
			runtime.ReadMemStats(&after)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			if err := fixture.lifecycle.WaitData(ctx); err != nil {
				t.Fatal(err)
			}
			runtime.GC()
			runtime.ReadMemStats(&retained)
			if mode == "online" || mode == "slow-consumer" {
				stopConsumer()
				plugin.CloseNow()
				<-consumerDone
			}
			// 用原始 MessagePack 帧长度单列队列编码数据；采样后再读取。
			if mode == "offline-full" || mode == "slow-consumer" {
				plugin = fixture.connect(t, "research", "plugin-test-token", observationSubprotocol)
				plugin.SetReadLimit(4000000)
				fixture.command(t, plugin, "subscribe", "measure-queue")
				readObservation(t, plugin)
				for range 200 {
					readCtx, stop := context.WithTimeout(t.Context(), 100*time.Millisecond)
					_, body, err := plugin.Read(readCtx)
					stop()
					if err != nil {
						break
					}
					retainedEvents++
					retainedBytes += int64(len(body))
				}
			}
			if plugin != nil {
				plugin.CloseNow()
			}
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			sort.Slice(firstTimes, func(i, j int) bool { return firstTimes[i] < firstTimes[j] })
			cpuMicros := func(r syscall.Rusage) int64 {
				return r.Utime.Sec*1000000 + r.Utime.Usec + r.Stime.Sec*1000000 + r.Stime.Usec
			}
			t.Logf("MEASURE mode=%s requests=%d workers=%d req_s=%.1f elapsed_ms=%d latency_p50_us=%d latency_p95_us=%d first_p50_us=%d first_p95_us=%d cpu_us_req=%.1f alloc_B_req=%d allocs_req=%d live_heap_B=%d heap_delta_B=%d peak_heap_B=%d goroutines_base=%d goroutines_peak=%d retained_events=%d retained_encoded_B=%d received_encoded_B=%d", mode, requests, workers, float64(requests)/elapsed.Seconds(), elapsed.Milliseconds(), latencies[requests/2].Microseconds(), latencies[requests*95/100].Microseconds(), firstTimes[requests/2].Microseconds(), firstTimes[requests*95/100].Microseconds(), float64(cpuMicros(cpuAfter)-cpuMicros(cpuBefore))/requests, (after.TotalAlloc-before.TotalAlloc)/requests, (after.Mallocs-before.Mallocs)/requests, retained.HeapAlloc, int64(retained.HeapAlloc)-int64(before.HeapAlloc), peakHeap.Load(), baselineGoroutines, peakGoroutines.Load(), retainedEvents, retainedBytes, received.Load())
		})
	}
}
