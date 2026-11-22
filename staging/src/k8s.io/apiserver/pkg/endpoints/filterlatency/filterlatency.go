/*
Copyright 2020 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package filterlatency 提供成对的 HTTP 中间件，用于测量单个 filter 的
// 耗时：TrackStarted 记录起点，TrackCompleted 在下一层返回时计算差值。
//
// 之所以需要成对使用，是因为 filter 链是嵌套调用，仅靠一次包装无法
// 精确界定“本 filter 自身”的执行时间。
package filterlatency

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel/trace"

	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/server/httplog"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
)

type requestFilterRecordKeyType int

// requestFilterRecordKey is the context key for a request filter record struct.
const requestFilterRecordKey requestFilterRecordKeyType = iota

const minFilterLatencyToLog = 100 * time.Millisecond

// requestFilterRecord 保存当前正在测量的 filter 名称与起始时间。
// 记录放在 context 中，使同一次请求内成对的 Started/Completed 能共享状态。
type requestFilterRecord struct {
	name             string
	startedTimestamp time.Time
}

// withRequestFilterRecord attaches the given request filter record to the parent context.
func withRequestFilterRecord(parent context.Context, fr *requestFilterRecord) context.Context {
	return apirequest.WithValue(parent, requestFilterRecordKey, fr)
}

// requestFilterRecordFrom returns the request filter record from the given context.
func requestFilterRecordFrom(ctx context.Context) *requestFilterRecord {
	fr, _ := ctx.Value(requestFilterRecordKey).(*requestFilterRecord)
	return fr
}

// TrackStarted measures the timestamp the given handler has started execution
// by attaching a handler to the chain.
func TrackStarted(handler http.Handler, tp trace.TracerProvider, name string) http.Handler {
	return trackStarted(handler, tp, name, clock.RealClock{})
}

// TrackCompleted measures the timestamp the given handler has completed execution and then
// it updates the corresponding metric with the filter latency duration.
//
// 指标标签只有 filter 名，不包含用户输入，避免高基数。
func TrackCompleted(handler http.Handler) http.Handler {
	return trackCompleted(handler, clock.RealClock{}, func(ctx context.Context, fr *requestFilterRecord, completedAt time.Time) {
		latency := completedAt.Sub(fr.startedTimestamp)
		metrics.RecordFilterLatency(ctx, fr.name, latency)
		if klog.V(3).Enabled() && latency > minFilterLatencyToLog {
			httplog.AddKeyValue(ctx, fmt.Sprintf("fl_%s", fr.name), latency.String())
		}
	})
}

func trackStarted(handler http.Handler, tp trace.TracerProvider, name string, clock clock.PassiveClock) http.Handler {
	// This is a noop if the tracing is disabled, since tp will be a NoopTracerProvider
	tracer := tp.Tracer("k8s.op/apiserver/pkg/endpoints/filterlatency")
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, span := tracer.Start(r.Context(), name)
		defer span.End()
		// 如果已有记录，说明同一 filter 名被重复包裹：复用并覆盖起点，
		// 让测量对准最内层实际执行的那一次。
		if fr := requestFilterRecordFrom(ctx); fr != nil {
			fr.name = name
			fr.startedTimestamp = clock.Now()
			r = r.WithContext(ctx)
		} else {
			fr := &requestFilterRecord{
				name:             name,
				startedTimestamp: clock.Now(),
			}
			r = r.WithContext(withRequestFilterRecord(ctx, fr))
		}

		handler.ServeHTTP(w, r)
	})
}

func trackCompleted(handler http.Handler, clock clock.PassiveClock, action func(context.Context, *requestFilterRecord, time.Time)) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// The previous filter has just completed.
		// 在进入下一层之前取时间点，因此 action 统计的是“本 filter 到此处
		// 为止”的耗时，而不是整个请求的耗时。
		completedAt := clock.Now()

		defer handler.ServeHTTP(w, r)

		ctx := r.Context()
		if fr := requestFilterRecordFrom(ctx); fr != nil {
			action(ctx, fr, completedAt)
		}
	})
}
