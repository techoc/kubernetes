/*
Copyright 2022 The Kubernetes Authors.

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

package tracing

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	utiltrace "k8s.io/utils/trace"
)

const instrumentationScope = "k8s.io/component-base/tracing"

// Start 同时使用 OpenTelemetry 和 k8s.io/utils/trace 创建 span。
// 仅当传入的 context 已包含 span 时，才创建 OpenTelemetry span。
func Start(ctx context.Context, name string, attributes ...attribute.KeyValue) (context.Context, *Span) {
	// 若传入的 context 已包含 OpenTelemetry span，则使用给定名称和属性创建子 span。
	// 若调用方未使用 OpenTelemetry，或已通过组件级特性开关禁用追踪，则此操作为空操作。
	ctx, otelSpan := trace.SpanFromContext(ctx).TracerProvider().Tracer(instrumentationScope).Start(ctx, name, trace.WithAttributes(attributes...))
	// 若 context 中已有 utiltrace span，则将其用作父 span。
	utilSpan := utiltrace.FromContext(ctx).Nest(name, attributesToFields(attributes)...)
	// 把 utiltrace span 放回 context，使后续 Start 调用形成嵌套层级。
	return utiltrace.ContextWithTrace(ctx, utilSpan), &Span{
		otelSpan: otelSpan,
		utilSpan: utilSpan,
	}
}

// Span 是追踪中的一个组件部分，表示被观察工作流中一个有名称、有时长的操作。
// Span 结合了 OpenTelemetry 与 k8s.io/utils/trace 的 span，以便于迁移到 OpenTelemetry。
type Span struct {
	otelSpan trace.Span
	utilSpan *utiltrace.Trace
}

// AddEvent 添加一个带有名称和属性的时间点事件。
// 事件同时写入两种 span；utilSpan 为 nil 时只写 OpenTelemetry。
func (s *Span) AddEvent(name string, attributes ...attribute.KeyValue) {
	s.otelSpan.AddEvent(name, trace.WithAttributes(attributes...))
	if s.utilSpan != nil {
		s.utilSpan.Step(name, attributesToFields(attributes)...)
	}
}

// End 结束 span；若 span 时长超过 logThreshold，则记录日志。
// 只对 utiltrace 做阈值日志：OpenTelemetry 的采样由 SDK 决定。
func (s *Span) End(logThreshold time.Duration) {
	s.otelSpan.End()
	if s.utilSpan != nil {
		s.utilSpan.LogIfLong(logThreshold)
	}
}

// RecordError 将 err 记录为此 span 的异常事件。
// 若此 span 未被记录或 err 为 nil，则此方法不执行任何操作。
func (s *Span) RecordError(err error, attributes ...attribute.KeyValue) {
	s.otelSpan.RecordError(err, trace.WithAttributes(attributes...))
}

// attributesToFields 把 OpenTelemetry attribute 转成 utiltrace 的字段，
// 保持两套实现的日志内容一致。
func attributesToFields(attributes []attribute.KeyValue) []utiltrace.Field {
	fields := make([]utiltrace.Field, len(attributes))
	for i := range attributes {
		attr := attributes[i]
		fields[i] = utiltrace.Field{Key: string(attr.Key), Value: attr.Value.AsInterface()}
	}
	return fields
}

// SpanFromContext 从当前 context 返回 *Span，其中包含处于活动状态的
// OpenTelemetry 和 k8s.io/utils/trace span。
func SpanFromContext(ctx context.Context) *Span {
	return &Span{
		otelSpan: trace.SpanFromContext(ctx),
		utilSpan: utiltrace.FromContext(ctx),
	}
}

// ContextWithSpan 返回包含该 Span 的 context。
// 两种 span 必须同时写入，否则后续 SpanFromContext 只能拿到其中一种。
func ContextWithSpan(ctx context.Context, s *Span) context.Context {
	return trace.ContextWithSpan(utiltrace.ContextWithTrace(ctx, s.utilSpan), s.otelSpan)
}
