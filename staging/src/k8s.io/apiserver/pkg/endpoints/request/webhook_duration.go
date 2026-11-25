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

// DurationTracker 是统计函数耗时的简单接口，可被多个 goroutine 并发使用。
type DurationTracker interface {
	// Track 统计给定函数 f 的执行时间，并用 aggregateFunction 聚合测得的耗时。
	// 如果多个 goroutine 并发调用 Track，那么 f 本身必须支持并发调用。
	Track(f func())

	// TrackDuration 直接把指定时长计入延迟，并用 aggregateFunction 聚合。
	TrackDuration(time.Duration)

	// GetLatency 返回迄今累计的总延迟。
	GetLatency() time.Duration
}

// durationTracker 实现 DurationTracker：用给定的时钟测量函数耗时，
// 并用给定的聚合函数累加时长。
// 聚合函数在构造时固定，因此 tracker 本身无需在调用点区分 sum/max 语义。
type durationTracker struct {
	clock             clock.Clock
	latency           time.Duration
	mu                sync.Mutex
	aggregateFunction func(time.Duration, time.Duration) time.Duration
}

// Track 统计给定函数的执行时间，并用 aggregateFunction 聚合测得的耗时。
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

// TrackDuration 用 aggregateFunction 把给定时长计入延迟。
func (t *durationTracker) TrackDuration(d time.Duration) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.latency = t.aggregateFunction(t.latency, d)
}

// GetLatency 返回该 tracker 聚合后的延迟。
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

// LatencyTrackers 保存用于测量 apiserver 内部各组件延迟的 tracker。
// 每个字段的聚合方式取决于对应阶段的执行模型：串行用 sum，并行用 max。
// 这些数据最终以审计注解形式输出，用于排查请求延迟分布。
type LatencyTrackers struct {
	// MutatingWebhookTracker 统计 mutating webhook 的延迟。
	// 由于 mutating webhook 是串行执行的，延迟用求和方式聚合。
	MutatingWebhookTracker DurationTracker

	// ValidatingWebhookTracker 统计 validating webhook 的延迟。
	// 由于 validating webhook 是并行执行的，因此用取最大值方式聚合。
	ValidatingWebhookTracker DurationTracker

	// AuthenticationTracker 统计请求认证阶段的延迟。
	AuthenticationTracker DurationTracker

	// AuthorizationTracker 统计请求授权阶段的延迟。
	AuthorizationTracker DurationTracker

	// APFQueueWaitTracker 统计优先级与公平性（APF）队列等待带来的延迟。
	APFQueueWaitTracker DurationTracker

	// StorageTracker 统计存储层内部的延迟，即把数据写入底层存储（etcd）
	// 并取回完整响应所花的时间。若一次请求涉及与底层存储的 N（N>=1）次往返，
	// 延迟就是这 N 次往返的总耗时，但不包含准入（admission）或校验的耗时。
	StorageTracker DurationTracker

	// TransformTracker 统计对底层存储返回的响应对象做转换的延迟，
	// 包括把对象转换成用户期望的形式（例如 Table）以及设置 API 层字段。
	// 不包含响应对象序列化（json 或 protobuf）以及写入 http ResponseWriter 的延迟。
	TransformTracker DurationTracker

	// SerializationTracker 统计响应对象序列化（json 或 protobuf）的延迟。
	// 注意：序列化与把字节写入 http ResponseWriter 是交替进行的，
	// 因此这里测得的延迟也包含写入 ResponseWriter 所花的时间。
	SerializationTracker DurationTracker

	// ResponseWriteTracker 统计通过 Write 方法把序列化后的字节写入请求关联的
	// http ResponseWriter 的延迟。Write 可能被调用多次，因此使用求和型 tracker。
	ResponseWriteTracker DurationTracker

	// DecodeTracker 用于统计“从底层存储（etcd）取回对象并解码响应对象”的函数内部延迟，
	// 多次调用时各次解码耗时会被累加。
	DecodeTracker DurationTracker

	// ImpersonationTracker 统计解析冒充（impersonation）过程的延迟，
	// 包括模式选择、授权检查与缓存查询。
	ImpersonationTracker DurationTracker
}

// latencyTrackersKeyType 是私有类型，避免与其他包的 context key 冲突。
type latencyTrackersKeyType int

// latencyTrackersKey 是把 LatencyTrackers 实例与请求上下文关联起来的键。
const latencyTrackersKey latencyTrackersKeyType = iota

// WithLatencyTrackers 返回 parent 的副本，其中加入了 LatencyTrackers 实例。
func WithLatencyTrackers(parent context.Context) context.Context {
	return WithLatencyTrackersAndCustomClock(parent, clock.RealClock{})
}

// WithLatencyTrackersAndCustomClock 返回加入 LatencyTrackers 实例的 parent 副本，
// tracker 使用给定的时钟。
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

// LatencyTrackersFrom 从指定上下文中取出关联的 LatencyTrackers 实例。
// 额外判断 wd != nil，避免有人把 typed nil 放入 context 导致调用方空指针。
func LatencyTrackersFrom(ctx context.Context) (*LatencyTrackers, bool) {
	wd, ok := ctx.Value(latencyTrackersKey).(*LatencyTrackers)
	return wd, ok && wd != nil
}

// TrackTransformResponseObjectLatency 用于统计“取回底层存储（etcd）对象并对响应对象
// 做必要转换”的函数内部延迟，不包含序列化或写入 http ResponseWriter 的延迟。
// 多次调用时，各次转换耗时会被累加。
// 没有 tracker 时直接执行 transform，保证埋点缺失不影响功能路径。
func TrackTransformResponseObjectLatency(ctx context.Context, transform func()) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.TransformTracker.Track(transform)
		return
	}

	transform()
}

// TrackStorageLatency 用于统计底层存储内部的延迟，多次调用时给出的延迟会被累加。
func TrackStorageLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.StorageTracker.TrackDuration(d)
	}
}

// TrackSerializeResponseObjectLatency 用于统计响应对象序列化（json 或 protobuf）的延迟，
// 多次调用时给出的延迟会被累加。
func TrackSerializeResponseObjectLatency(ctx context.Context, f func()) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.SerializationTracker.Track(f)
		return
	}

	f()
}

// TrackResponseWriteLatency 用于统计通过 Write 方法把序列化字节写入请求关联的
// http ResponseWriter 的延迟，多次调用时给出的延迟会被累加。
func TrackResponseWriteLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.ResponseWriteTracker.TrackDuration(d)
	}
}

// TrackAuthenticationLatency 用于统计请求认证阶段的延迟。
func TrackAuthenticationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.AuthenticationTracker.TrackDuration(d)
	}
}

// TrackAuthorizationLatency 用于统计请求授权阶段的延迟。
func TrackAuthorizationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.AuthorizationTracker.TrackDuration(d)
	}
}

// TrackAPFQueueWaitLatency 用于统计优先级与公平性队列等待的延迟。
func TrackAPFQueueWaitLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.APFQueueWaitTracker.TrackDuration(d)
	}
}

// TrackDecodeLatency 用于统计“取回底层存储（etcd）对象并解码响应对象”的函数内部延迟，
// 多次调用时各次解码耗时会被累加。
func TrackDecodeLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.DecodeTracker.TrackDuration(d)
	}
}

// TrackImpersonationLatency 用于统计解析请求冒充关系的延迟。
func TrackImpersonationLatency(ctx context.Context, d time.Duration) {
	if tracker, ok := LatencyTrackersFrom(ctx); ok {
		tracker.ImpersonationTracker.TrackDuration(d)
	}
}

// AuditAnnotationsFromLatencyTrackers 检查请求上下文关联的每个延迟 tracker，
// 返回可写入 API 审计记录的注解集合。
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
