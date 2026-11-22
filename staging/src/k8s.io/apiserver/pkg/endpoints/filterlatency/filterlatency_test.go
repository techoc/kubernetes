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

// 本文件验证请求过滤器耗时统计的包装器：
// trackStarted 负责在上下文中放置/覆盖过滤记录，trackCompleted 负责在请求结束时回调，
// 两者还共同维护 OpenTelemetry span 的嵌套关系。

package filterlatency

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	noopoteltrace "go.opentelemetry.io/otel/trace/noop"

	testingclock "k8s.io/utils/clock/testing"
)

// TestTrackStartedWithContextAlreadyHasFilterRecord 验证上下文已有过滤记录时，
// trackStarted 会用当前过滤器覆盖它：同一个请求内嵌套过滤器应各自计时，而不是复用外层记录。
func TestTrackStartedWithContextAlreadyHasFilterRecord(t *testing.T) {
	ctx := t.Context()
	filterName := "my-filter"
	var (
		callCount    int
		filterRecord *requestFilterRecord
	)
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// we expect the handler to be invoked just once.
		callCount++

		// we expect the filter record to be set in the context
		filterRecord = requestFilterRecordFrom(req.Context())
	})

	requestFilterStarted := time.Now()
	wrapped := trackStarted(handler, noopoteltrace.NewTracerProvider(), filterName, testingclock.NewFakeClock(requestFilterStarted))

	testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}
	testRequest = testRequest.WithContext(withRequestFilterRecord(testRequest.Context(), &requestFilterRecord{
		name:             "foo",
		startedTimestamp: time.Now(),
	}))

	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, testRequest)

	if callCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", callCount)
	}
	if filterRecord == nil {
		t.Fatal("expected a filter record in the request context, but got nil")
	}
	if filterName != filterRecord.name {
		t.Errorf("expected filter name=%s but got=%s", filterName, filterRecord.name)
	}
	if requestFilterStarted != filterRecord.startedTimestamp {
		t.Errorf("expected filter started timestamp=%s but got=%s", requestFilterStarted, filterRecord.startedTimestamp)
	}
}

// TestTrackStartedWithContextDoesNotHaveFilterRecord 验证上下文为空时 trackStarted
// 也能正常注入记录，覆盖最外层过滤器（第一个进入请求链）的场景。
func TestTrackStartedWithContextDoesNotHaveFilterRecord(t *testing.T) {
	ctx := t.Context()
	filterName := "my-filter"
	var (
		callCount    int
		filterRecord *requestFilterRecord
	)
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// we expect the handler to be invoked just once.
		callCount++

		// we expect the filter record to be set in the context
		filterRecord = requestFilterRecordFrom(req.Context())
	})

	requestFilterStarted := time.Now()
	wrapped := trackStarted(handler, noopoteltrace.NewTracerProvider(), filterName, testingclock.NewFakeClock(requestFilterStarted))

	testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}

	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, testRequest)

	if callCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", callCount)
	}
	if filterRecord == nil {
		t.Fatal("expected a filter record in the request context, but got nil")
	}
	if filterName != filterRecord.name {
		t.Errorf("expected filter name=%s but got=%s", filterName, filterRecord.name)
	}
	if requestFilterStarted != filterRecord.startedTimestamp {
		t.Errorf("expected filter started timestamp=%s but got=%s", requestFilterStarted, filterRecord.startedTimestamp)
	}
}

// TestTrackCompletedContextHasFilterRecord 验证请求结束后回调被调用一次，
// 且拿到的是上下文中的记录与完成时刻（用假时钟保证时间可断言）。
func TestTrackCompletedContextHasFilterRecord(t *testing.T) {
	ctx := t.Context()
	var (
		handlerCallCount     int
		actionCallCount      int
		filterRecordGot      *requestFilterRecord
		filterCompletedAtGot time.Time
	)
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// we expect the handler to be invoked just once.
		handlerCallCount++
	})

	requestFilterEndedAt := time.Now()
	wrapped := trackCompleted(handler, testingclock.NewFakeClock(requestFilterEndedAt), func(_ context.Context, fr *requestFilterRecord, completedAt time.Time) {
		actionCallCount++
		filterRecordGot = fr
		filterCompletedAtGot = completedAt
	})

	testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}

	testRequest = testRequest.WithContext(withRequestFilterRecord(testRequest.Context(), &requestFilterRecord{}))

	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, testRequest)

	if handlerCallCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", handlerCallCount)
	}
	if actionCallCount != 1 {
		t.Errorf("expected the action callback to be invoked once, but was actually invoked %d times", actionCallCount)
	}
	if filterRecordGot == nil {
		t.Fatal("expected a filter record in the request context, but got nil")
	}
	if requestFilterEndedAt != filterCompletedAtGot {
		t.Errorf("expected filter ended timestamp=%s but got=%s", requestFilterEndedAt, filterCompletedAtGot)
	}
}

// TestTrackCompletedContextDoesNotHaveFilterRecord 验证没有过滤记录时回调不会被调用，
// 否则会产生没有开始时间、无法计算的耗时样本。
func TestTrackCompletedContextDoesNotHaveFilterRecord(t *testing.T) {
	ctx := t.Context()
	var actionCallCount, handlerCallCount int
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		handlerCallCount++
	})

	wrapped := trackCompleted(handler, testingclock.NewFakeClock(time.Now()), func(_ context.Context, _ *requestFilterRecord, _ time.Time) {
		actionCallCount++
	})

	testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}

	w := httptest.NewRecorder()
	wrapped.ServeHTTP(w, testRequest)

	if handlerCallCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", handlerCallCount)
	}
	if actionCallCount != 0 {
		t.Errorf("expected the callback to not be invoked, but was actually invoked %d times", actionCallCount)
	}
}

// TestStartedAndCompletedOpenTelemetryTracing 验证 TrackStarted/TrackCompleted
// 成对使用时生成一个以过滤器命名的 span，且处理器只被调用一次。
func TestStartedAndCompletedOpenTelemetryTracing(t *testing.T) {
	ctx := t.Context()
	filterName := "my-filter"
	// Seup OTel for testing
	fakeRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(fakeRecorder))

	// base handler func
	var callCount int
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// we expect the handler to be invoked just once.
		callCount++
	})
	// wrap with start and completed handler
	wrapped := TrackCompleted(handler)
	wrapped = TrackStarted(wrapped, tp, filterName)

	testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}

	wrapped.ServeHTTP(httptest.NewRecorder(), testRequest)

	if callCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", callCount)
	}
	output := fakeRecorder.Ended()
	if len(output) != 1 {
		t.Fatalf("got %d; expected len(output) == 1", len(output))
	}
	span := output[0]
	if span.Name() != filterName {
		t.Fatalf("got %s; expected span.Name == my-filter", span.Name())
	}
}

// TestNestedStartedAndCompletedOpenTelemetryTracing 验证嵌套过滤器的 span 结构：
// 开始顺序为外到内，结束顺序为内到外，符合 span 的栈式生命周期。
func TestNestedStartedAndCompletedOpenTelemetryTracing(t *testing.T) {
	outerFilterName := "outer-filter"
	innerFilterName := "inner-filter"
	// Seup OTel for testing
	fakeRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(fakeRecorder))

	// base handler func
	var callCount int
	handler := http.HandlerFunc(func(_ http.ResponseWriter, req *http.Request) {
		// we expect the handler to be invoked just once.
		callCount++
	})
	// wrap the handler with the inner start and completed handler
	wrapped := TrackCompleted(handler)
	wrapped = TrackStarted(wrapped, tp, innerFilterName)

	// wrap with an external handler, nesting the inner span
	wrapped = TrackCompleted(wrapped)
	wrapped = TrackStarted(wrapped, tp, outerFilterName)

	testRequest, err := http.NewRequest(http.MethodGet, "/api/v1/namespaces", nil)
	if err != nil {
		t.Fatalf("failed to create new http request - %v", err)
	}

	wrapped.ServeHTTP(httptest.NewRecorder(), testRequest)

	if callCount != 1 {
		t.Errorf("expected the given handler to be invoked once, but was actually invoked %d times", callCount)
	}

	checkSpans(t, fakeRecorder.Started(), []string{outerFilterName, innerFilterName})
	checkSpans(t, fakeRecorder.Ended(), []string{innerFilterName, outerFilterName})
}

// checkSpans 断言 span 数量与名称顺序完全一致；顺序本身是断言的一部分，
// 因此这里用 Fatalf 而非仅比较集合。
func checkSpans[T sdktrace.ReadOnlySpan](t *testing.T, output []T, spanNames []string) {
	if len(output) != len(spanNames) {
		t.Fatalf("got %d; expected len(output) == %d", len(output), len(spanNames))
	}
	for idx, spanName := range spanNames {
		span := output[idx]
		if span.Name() != spanName {
			t.Fatalf("index %d: got %s; expected span.Name == %s", idx, span.Name(), spanName)
		}
	}
}
