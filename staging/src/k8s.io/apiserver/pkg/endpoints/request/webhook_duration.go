/*
Copyright 2021 The Kubernetes Authors.

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

package request

import (
	"context"
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// sumDuration 是顺序执行的组件（如 mutating webhook）的聚合方式。
func sumDuration(d1 time.Duration, d2 time.Duration) time.Duration {
	return d1 + d2
}

// maxDuration 是并行执行的组件（如 validating webhook）的聚合方式：
// 并行调用的总延迟由最慢的一个决定，累加会重复计算重叠时间。
func maxDuration(d1 time.Duration, d2 time.Duration) time.Duration {
	if d1 > d2 {
		return d1
	}
	return d2
}

// DurationTracker is a simple interface for tracking functions duration,
// it is safe for concurrent use by multiple goroutines.
type DurationTracker interface {
	// Track measures time spent in the given function f and
	// aggregates measured duration using aggregateFunction.
	// if Track is invoked with f from multiple goroutines concurrently,
	// then f must be safe to be invoked concurrently by multiple goroutines.
	Track(f func())

	// TrackDuration tracks latency from the specified duration
	// and aggregate it using aggregateFunction
	TrackDuration(time.Duration)

	// GetLatency returns the total latency incurred so far
	GetLatency() time.Duration
}

// durationTracker implements DurationTracker by measuring function time
// using given clock and aggregates the duration using given aggregate function
// 聚合函数在构造时固定，因此 tracker 本身无需在调用点区分 sum/max 语义。
type durationTracker struct {
	clock             clock.Clock
	latency           time.Duration
	mu                sync.Mutex
	aggregateFunction func(time.Duration, time.Duration) time.Duration
}

// Track measures time spent in given function and aggregates measured
// duration using aggregateFunction
// 使用 defer 保证 f 发生 panic 时仍会记录已消耗的时间。
func (t *durationTracker) Track(f func()) {
	startedAt := t.clock.Now()
	defer func() {
		duration := t.clock.Since(startedAt)
		t.mu.Lock()
		defer t.mu.Unlock()
		t.latency = t.aggregateFunction(t.latency, duration)
	}()

	f()
}

// TrackDuration tracks latency from the given duration
// using aggregateFunction
func (t *durationTracker) TrackDuration(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.latency = t.aggregateFunction(t.latency, d)
}

// GetLatency returns aggregated latency tracked by a tracker
func (t *durationTracker) GetLatency() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.latency
}

// newSumLatencyTracker 创建累加型 tracker，适用于顺序执行或多轮往返的场景。
func newSumLatencyTracker(c clock.Clock) DurationTracker {
	return &durationTracker{
		clock:             c,
		aggregateFunction: sumDuration,
	}
}

// newMaxLatencyTracker 创建取最大型 tracker，适用于并行执行的场景。
func newMaxLatencyTracker(c clock.Clock) DurationTracker {
	return &durationTracker{
		clock:             c,
		aggregateFunction: maxDuration,
	}
}

// LatencyTrackers stores trackers used to measure latecny incurred in
// components within the apiserver.
// 每个字段的聚合方式取决于对应阶段的执行模型：串行用 sum，并行用 max。
// 这些数据最终以审计注解形式输出，用于排查请求延迟分布。
type LatencyTrackers struct {
	// MutatingWebhookTracker tracks the latency incurred in mutating webhook(s).
	// Since mutating webhooks are done sequentially, latency
	// is aggregated using sum function.
	MutatingWebhookTracker DurationTracker

	// ValidatingWebhookTracker tracks the latency incurred in validating webhook(s).
	// Validate webhooks are done in parallel, so max function is used.
	ValidatingWebhookTracker DurationTracker

	// AuthenticationTracker tracks the latency incurred by Authentication of request
	AuthenticationTracker DurationTracker

	// AuthorizationTracker tracks the latency incurred by Authorization of request
	AuthorizationTracker DurationTracker

	// APFQueueWaitTracker tracks the latency incurred by queue wait times
	// from priority & fairness.
	APFQueueWaitTracker DurationTracker

	// StorageTracker tracks the latency incurred inside the storage layer,
	// it accounts for the time it takes to send data to the underlying
	// storage layer (etcd) and get the complete response back.
	// If a request involves N (N>=1) round trips to the underlying
	// stogare layer, the latency will account for the total duration
	// from these N round trips.
	// It does not include the time incurred in admission, or validation.
	StorageTracker DurationTracker

	// TransformTracker tracks the latency incurred in transforming the
	// response object(s) returned from the underlying storage layer.
	// This includes transforming the object to user's desired form
	// (ie. as Table), and also setting appropriate API level fields.
	// This does not include the latency incurred in serialization
	// (json or protobuf) of the response object or writing
	// of it to the http ResponseWriter object.
	TransformTracker DurationTracker

	// SerializationTracker tracks the latency incurred in serialization
	// (json or protobuf) of the response object.
	// NOTE: serialization and writing of the serialized raw bytes to the
	// associated http ResponseWriter object are interleaved, and hence
	// the latency measured here will include the time spent writing the
	// serialized raw bytes to the http ResponseWriter object.
	SerializationTracker DurationTracker

	// ResponseWriteTracker tracks the latency incurred in writing the
	// serialized raw bytes to the http ResponseWriter object (via the
	// Write method) associated with the request.
	// The Write method can be invoked multiple times, so we use a
	// latency tracker that sums up the duration from each call.
	ResponseWriteTracker DurationTracker

	// DecodeTracker is used to track latency incurred inside the function
	// that takes an object returned from the underlying storage layer
	// (etcd) and performs decoding of the response object.
	// When called multiple times, the latency incurred inside to
	// decode func each time will be summed up.
	DecodeTracker DurationTracker

	// ImpersonationTracker tracks the latency incurred in resolving impersonation.
	// This includes mode selection, authorization checks, and cache lookups.
	ImpersonationTracker DurationTracker
}

// latencyTrackersKeyType 是私有类型，避免与其他包的 context key 冲突。
type latencyTrackersKeyType int

// latencyTrackersKey is the key that associates a LatencyTrackers
// instance with the request context.
const latencyTrackersKey latencyTrackersKeyType = iota

// WithLatencyTrackers returns a copy of parent context to which an
// instance of LatencyTrackers is added.
func WithLatencyTrackers(parent context.Context) context.Context {
	return WithLatencyTrackersAndCustomClock(parent, clock.RealClock{})
}

// WithLatencyTrackersAndCustomClock returns a copy of parent context to which
// an instance of LatencyTrackers is added. Tracers use given clock.
// 提供自定义时钟是为了让测试能精确控制耗时，而不依赖真实时间。
func WithLatencyTrackersAndCustomClock(parent context.Context, c clock.Clock) context.Context {
	return WithValue(parent, latencyTrackersKey, &LatencyTrackers{
		// mutating webhook 串行执行，累加各次耗时。
		MutatingWebhookTracker: newSumLatencyTracker(c),
		// validating webhook 并行执行，取最大耗时以避免重复计算。
		ValidatingWebhookTracker: newMaxLatencyTracker(c),
		AuthenticationTracker:    newSumLatencyTracker(c),
		// 授权可能并行查询多个 authorizer，取最大而非累加。
		AuthorizationTracker: newMaxLatencyTracker(c),
		APFQueueWaitTracker:  newMaxLatencyTracker(c),
		StorageTracker:       newSumLatencyTracker(c),
		TransformTracker:     newSumLatencyTracker(c),
		SerializationTracker: newSumLatencyTracker(c),
		ResponseWriteTracker: newSumLatencyTracker(c),
		DecodeTracker:        newSumLatencyTracker(c),
		ImpersonationTracker: newSumLatencyTracker(c),
	})
}

// LatencyTrackersFrom returns the associated LatencyTrackers instance
// from the specified context.
// 额外判断 wd != nil，避免有人把 typed nil 放入 context 导致调用方空指针。
func LatencyTrackersFrom(ctx context.Context) (*LatencyTrackers, bool) {
	wd, ok := ctx.Value(latencyTrackersKey).(*LatencyTrackers)
	return wd, ok && wd != nil
}

// TrackTransformResponseObjectLatency is used to track latency incurred
// inside the function that takes an object returned from the underlying
// storage layer (etcd) and performs any necessary transformations
// of the response object. This does not include the latency incurred in
// serialization (json or protobuf) of the response object or writing of
// it to the http ResponseWriter object.
// When called multiple times, the latency incurred inside the
// transform func each time will be summed up.
// 没有 tracker 时直接执行 transform，保证埋点缺失不影响功能路径。
func TrackTransformResponseObjectLatency(ctx context.Context, transform func()) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.TransformTracker.Track(transform)
		return
	}

	transform()
}

// TrackStorageLatency is used to track latency incurred
// inside the underlying storage layer.
// When called multiple times, the latency provided will be summed up.
func TrackStorageLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.StorageTracker.TrackDuration(d)
	}
}

// TrackSerializeResponseObjectLatency is used to track latency incurred in
// serialization (json or protobuf) of the response object.
// When called multiple times, the latency provided will be summed up.
func TrackSerializeResponseObjectLatency(ctx context.Context, f func()) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.SerializationTracker.Track(f)
		return
	}

	f()
}

// TrackResponseWriteLatency is used to track latency incurred in writing
// the serialized raw bytes to the http ResponseWriter object (via the
// Write method) associated with the request.
// When called multiple times, the latency provided will be summed up.
func TrackResponseWriteLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.ResponseWriteTracker.TrackDuration(d)
	}
}

// TrackAuthenticationLatency is used to track latency incurred
// by Authentication phase of request.
func TrackAuthenticationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.AuthenticationTracker.TrackDuration(d)
	}
}

// TrackAuthorizationLatency is used to track latency incurred
// by Authorization phase of request.
func TrackAuthorizationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.AuthorizationTracker.TrackDuration(d)
	}
}

// TrackAPFQueueWaitLatency is used to track latency incurred
// by priority and fairness queues.
func TrackAPFQueueWaitLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.APFQueueWaitTracker.TrackDuration(d)
	}
}

// TrackDecodeLatency is used to track latency incurred inside the function
// that takes an object returned from the underlying storage layer
// (etcd) and performs decoding of the response object.
// When called multiple times, the latency incurred inside to
// decode func each time will be summed up.
func TrackDecodeLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.DecodeTracker.TrackDuration(d)
	}
}

// TrackImpersonationLatency is used to track latency incurred
// in resolving impersonation for the request.
func TrackImpersonationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.ImpersonationTracker.TrackDuration(d)
	}
}

// AuditAnnotationsFromLatencyTrackers will inspect each latency tracker
// associated with the request context and return a set of audit
// annotations that can be added to the API audit entry.
// 只有非零耗时才写注解，避免为每个请求生成大量无意义的 0s 条目；
// 注解键属于对外可观测契约，重命名会影响依赖审计日志的下游系统。
func AuditAnnotationsFromLatencyTrackers(ctx context.Context) map[string]string {
	const (
		transformLatencyKey         = "apiserver.latency.k8s.io/transform-response-object"
		storageLatencyKey           = "apiserver.latency.k8s.io/etcd"
		serializationLatencyKey     = "apiserver.latency.k8s.io/serialize-response-object"
		responseWriteLatencyKey     = "apiserver.latency.k8s.io/response-write"
		mutatingWebhookLatencyKey   = "apiserver.latency.k8s.io/mutating-webhook"
		validatingWebhookLatencyKey = "apiserver.latency.k8s.io/validating-webhook"
		decodeLatencyKey            = "apiserver.latency.k8s.io/decode-response-object"
		apfQueueWaitLatencyKey      = "apiserver.latency.k8s.io/apf-queue-wait"
		authenticationLatencyKey    = "apiserver.latency.k8s.io/authentication"
		authorizationLatencyKey     = "apiserver.latency.k8s.io/authorization"
		impersonationLatencyKey     = "apiserver.latency.k8s.io/impersonation"
	)

	tracker, ok := LatencyTrackersFrom(ctx)
	if !ok {
		return nil
	}

	// 逐项读取并只保留非零值；各 tracker 的 GetLatency 已加锁，可安全并发读取。
	annotations := map[string]string{}
	if latency := tracker.TransformTracker.GetLatency(); latency != 0 {
		annotations[transformLatencyKey] = latency.String()
	}
	if latency := tracker.StorageTracker.GetLatency(); latency != 0 {
		annotations[storageLatencyKey] = latency.String()
	}
	if latency := tracker.SerializationTracker.GetLatency(); latency != 0 {
		annotations[serializationLatencyKey] = latency.String()
	}
	if latency := tracker.ResponseWriteTracker.GetLatency(); latency != 0 {
		annotations[responseWriteLatencyKey] = latency.String()
	}
	if latency := tracker.MutatingWebhookTracker.GetLatency(); latency != 0 {
		annotations[mutatingWebhookLatencyKey] = latency.String()
	}
	if latency := tracker.ValidatingWebhookTracker.GetLatency(); latency != 0 {
		annotations[validatingWebhookLatencyKey] = latency.String()
	}
	if latency := tracker.DecodeTracker.GetLatency(); latency != 0 {
		annotations[decodeLatencyKey] = latency.String()
	}
	if latency := tracker.APFQueueWaitTracker.GetLatency(); latency != 0 {
		annotations[apfQueueWaitLatencyKey] = latency.String()
	}
	if latency := tracker.AuthenticationTracker.GetLatency(); latency != 0 {
		annotations[authenticationLatencyKey] = latency.String()
	}
	if latency := tracker.AuthorizationTracker.GetLatency(); latency != 0 {
		annotations[authorizationLatencyKey] = latency.String()
	}
	if latency := tracker.ImpersonationTracker.GetLatency(); latency != 0 {
		annotations[impersonationLatencyKey] = latency.String()
	}
	return annotations
}
