/*
Copyright 2015 The Kubernetes Authors.

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

package rest

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/klog/v2"
)

// 需要退避的响应码集合，通常表示服务端过载；不由用户配置，取值基于约定：
// 429 表示服务端限流；>= 500 且不超过 maxResponseCode 的响应也视为过载。
var serverIsOverloadedSet = sets.NewInt(429)
var maxResponseCode = 499

//go:generate mockery

// Deprecated: BackoffManager.Sleep ignores the caller's context. Use BackoffManagerWithContext instead.
// 已弃用：Sleep 不响应调用方 context，新代码应实现 BackoffManagerWithContext。
type BackoffManager interface {
	UpdateBackoff(actualURL *url.URL, err error, responseCode int)
	CalculateBackoff(actualURL *url.URL) time.Duration
	Sleep(d time.Duration)
}

// BackoffManagerWithContext 是感知 context 的退避接口；所有睡眠都能被取消，
// 避免请求已取消后仍阻塞 goroutine。
type BackoffManagerWithContext interface {
	UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int)
	CalculateBackoffWithContext(ctx context.Context, actualURL *url.URL) time.Duration
	SleepWithContext(ctx context.Context, d time.Duration)
}

var _ BackoffManager = &URLBackoff{}
var _ BackoffManagerWithContext = &URLBackoff{}

// URLBackoff 在 Backoff 之上实现 URL 粒度的指数退避语义。
type URLBackoff struct {
	// 底层使用 Backoff 实现；以 host 为 key 记录失败次数，
	// 因此同一 API server 的不同路径共享退避状态。
	Backoff *flowcontrol.Backoff
}

// NoBackoff 是空实现，可用于 mock 或作为默认值。
type NoBackoff struct {
}

// UpdateBackoff 实现 BackoffManager，忽略 context（兼容旧接口）。
func (n *NoBackoff) UpdateBackoff(actualURL *url.URL, err error, responseCode int) {
	// 什么都不做。
}

// UpdateBackoffWithContext 实现 BackoffManagerWithContext，不记录任何状态。
func (n *NoBackoff) UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int) {
	// 什么都不做。
}

// CalculateBackoff 始终返回 0，即不进行退避。
func (n *NoBackoff) CalculateBackoff(actualURL *url.URL) time.Duration {
	return 0 * time.Second
}

// CalculateBackoffWithContext 始终返回 0，即不进行退避。
func (n *NoBackoff) CalculateBackoffWithContext(ctx context.Context, actualURL *url.URL) time.Duration {
	return 0 * time.Second
}

// Sleep 直接阻塞指定时长，不响应取消。
func (n *NoBackoff) Sleep(d time.Duration) {
	time.Sleep(d)
}

// SleepWithContext 在 d 时长内等待，期间 context 取消则立即返回。
func (n *NoBackoff) SleepWithContext(ctx context.Context, d time.Duration) {
	if d == 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// Disable 把退避时长置零。测试若要在不拖慢速度的情况下执行数千次 mock 请求，
// 可以使用它。
func (b *URLBackoff) Disable() {
	b.Backoff = flowcontrol.NewBackOff(0*time.Second, 0*time.Second)
}

// baseUrlKey 返回 URL 映射到的键。例如 127.0.0.1:8080/api/v2/abcde -> 127.0.0.1:8080。
func (b *URLBackoff) baseUrlKey(rawurl *url.URL) string {
	// 当前实现简单地只取 host；未来可能对特定路径（如 "pods"）做差异化退避。
	host, err := url.Parse(rawurl.String())
	if err != nil {
		panic(fmt.Sprintf("Error parsing bad URL %q: %v", rawurl, err))
	}
	return host.Host
}

// UpdateBackoff 更新退避元数据。
func (b *URLBackoff) UpdateBackoff(actualURL *url.URL, err error, responseCode int) {
	b.UpdateBackoffWithContext(context.Background(), actualURL, err, responseCode)
}

// UpdateBackoffWithContext 更新退避元数据。
func (b *URLBackoff) UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int) {
	// 我们记录的重试次数范围是 [0,13]。
	if responseCode > maxResponseCode || serverIsOverloadedSet.Has(responseCode) {
		// 服务端过载：累计失败次数，后续 CalculateBackoff 会按指数增长等待时间。
		b.Backoff.Next(b.baseUrlKey(actualURL), b.Backoff.Clock.Now())
		return
	} else if responseCode >= 300 || err != nil {
		klog.FromContext(ctx).V(4).Info("Client is returning errors", "code", responseCode, "err", err)
	}

	// 走到这里说明该 URL 不再需要退避：
	// 成功或仅客户端错误时清除该 host 的历史，避免过期的失败记录拖慢后续请求。
	b.Backoff.Reset(b.baseUrlKey(actualURL))
}

// CalculateBackoff 根据已有的失败记录，对 URL 做指数退避。
func (b *URLBackoff) CalculateBackoff(actualURL *url.URL) time.Duration {
	return b.Backoff.Get(b.baseUrlKey(actualURL))
}

// CalculateBackoffWithContext 根据已有的失败记录，对 URL 做指数退避。
func (b *URLBackoff) CalculateBackoffWithContext(_ context.Context, actualURL *url.URL) time.Duration {
	return b.CalculateBackoff(actualURL)
}

// Sleep 通过 Backoff 的 Clock 睡眠，便于测试注入假时钟。
func (b *URLBackoff) Sleep(d time.Duration) {
	b.Backoff.Clock.Sleep(d)
}

// SleepWithContext 在 d 时长内等待，context 取消时提前返回。
func (b *URLBackoff) SleepWithContext(ctx context.Context, d time.Duration) {
	if d == 0 {
		return
	}
	t := b.Backoff.Clock.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C():
	}
}

// backoffManagerNopContext 包装 BackoffManager 并补上 *WithContext 方法。
// 兼容适配器：让仅实现旧接口的 BackoffManager 也能满足新接口要求。
type backoffManagerNopContext struct {
	BackoffManager
}

var _ BackoffManager = &backoffManagerNopContext{}
var _ BackoffManagerWithContext = &backoffManagerNopContext{}

// UpdateBackoffWithContext 忽略 context，委托给旧方法。
func (b *backoffManagerNopContext) UpdateBackoffWithContext(ctx context.Context, actualURL *url.URL, err error, responseCode int) {
	b.UpdateBackoff(actualURL, err, responseCode)
}

// CalculateBackoffWithContext 忽略 context，委托给旧方法。
func (b *backoffManagerNopContext) CalculateBackoffWithContext(ctx context.Context, actualURL *url.URL) time.Duration {
	return b.CalculateBackoff(actualURL)
}

// SleepWithContext 忽略 context，委托给旧方法；因此仍不可取消。
func (b *backoffManagerNopContext) SleepWithContext(ctx context.Context, d time.Duration) {
	b.Sleep(d)
}
