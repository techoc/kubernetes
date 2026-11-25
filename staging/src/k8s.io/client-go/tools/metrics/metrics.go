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

// Package metrics 为“注册记录哪些指标”提供抽象。
// 本包只定义指标接口与进程级全局变量，具体实现（如 Prometheus）由适配包
// 通过 Register 注入。默认全部为空实现，因此未注册时记录指标不会 panic，
// 也不会引入额外依赖。
package metrics

import (
	"context"
	"net/url"
	"sync"
	"time"
)

var (
	registerMetrics      sync.Once
	ensureRegisteredOnce sync.Once
	// ensureRegisteredFn 若通过 RegisterOpts.RegisterFn 设置，会在任何
	// rest 客户端构造之前恰好执行一次。适配包（如
	// k8s.io/component-base/metrics/prometheus/restclient）在各自的 init()
	// 中安装该回调，使真正向 legacyregistry 注册（以及读取 NativeHistograms
	// 等特性门控派生选项的指标 Create()）发生在运行时而非 init() 阶段。
	// 调用方约定见 EnsureRegistered。
	ensureRegisteredFn func()
)

// EnsureRegistered 恰好执行一次通过 RegisterOpts.RegisterFn 安装的回调（若有）。
// 调用方应将其视为幂等操作；后续调用几乎零开销。
//
// client-go 中创建 REST 客户端、HTTP transport 或凭据提供者的新公开构造函数或
// 入口函数，应在函数最开头调用 EnsureRegistered()。适配包的 Observe 方法也会
// 调用它，因此即使某个构造函数忘记调用也不会丢观测点；但若入口调用它，会把
// 注册时机从“首次观测”提前到“首次构造客户端”，使指标序列从进程启动起
// 就能被 Prometheus 抓取到，而不是直到第一个请求后才出现。
//
// 利用 sync.Once 保证回调只执行一次；回调未安装时直接返回。
func EnsureRegistered() {
	if ensureRegisteredFn != nil {
		ensureRegisteredOnce.Do(ensureRegisteredFn)
	}
}

// DurationMetric 测量一段时长。
type DurationMetric interface {
	Observe(duration time.Duration)
}

// ExpiryMetric 设置过期时间。为 nil 时表示不相关。
type ExpiryMetric interface {
	Set(expiry *time.Time)
}

// LatencyMetric 按动词（verb）和 URL 维度观测客户端延迟。
type LatencyMetric interface {
	Observe(ctx context.Context, verb string, u url.URL, latency time.Duration)
}

// ResolverLatencyMetric 观测 DNS 解析延迟，按主机名区分。
type ResolverLatencyMetric interface {
	Observe(ctx context.Context, host string, latency time.Duration)
}

// SizeMetric 按动词和主机维度观测客户端响应大小。
type SizeMetric interface {
	Observe(ctx context.Context, verb string, host string, size float64)
}

// ResultMetric 按方法和主机维度统计响应码。
type ResultMetric interface {
	Increment(ctx context.Context, code string, method string, host string)
}

// CallsMetric 统计特定 exec 插件的调用次数。
type CallsMetric interface {
	// Increment 按 exitCode 和 callStatus 递增计数器。
	Increment(exitCode int, callStatus string)
}

// PolicyCallsMetric 统计 exec 插件执行的成功或失败。
// 名称沿用历史注释，实际统计的是 exec 插件策略检查结果（allowed/denied）。
type PolicyCallsMetric interface {
	// Increment 按状态 { "allowed", "denied" } 递增计数器
	Increment(status string)
}

// RetryMetric 按响应码、方法和主机维度统计发往服务端的重试次数。
type RetryMetric interface {
	IncrementRetry(ctx context.Context, code string, method string, host string)
}

// TransportCacheMetric 表示内部 transport 缓存的条目数
type TransportCacheMetric interface {
	Observe(value int)
}

// TransportCreateCallsMetric 按缓存命中结果（hit、miss、miss-gc、uncacheable）
// 统计 transport 被创建的次数
type TransportCreateCallsMetric interface {
	Increment(result string)
}

// TransportCAReloadsMetric 按结果和原因统计 CA 重载尝试的次数。
type TransportCAReloadsMetric interface {
	Increment(result, reason string)
}

// TransportCertRotationGCCallsMetric 统计 GC 清理时调用证书轮换
// goroutine cancel 函数的次数。
type TransportCertRotationGCCallsMetric interface {
	Increment()
}

// TransportCacheGCCallsMetric 按结果（deleted、skipped）统计 GC 清理
// 尝试删除缓存条目的次数。
type TransportCacheGCCallsMetric interface {
	Increment(result string)
}

var (
	// 以下变量在 Register 之前均为空实现，因此任何调用点都可以安全记录指标。
	// ClientCertExpiry 是客户端证书的过期时间
	ClientCertExpiry ExpiryMetric = noopExpiry{}
	// ClientCertRotationAge 是刚被轮换的证书的年龄。
	ClientCertRotationAge DurationMetric = noopDuration{}
	// RequestLatency 是 rest 客户端会更新的延迟指标。
	RequestLatency LatencyMetric = noopLatency{}
	// ResolverLatency 是 DNS 解析器会更新的延迟指标
	ResolverLatency ResolverLatencyMetric = noopResolverLatency{}
	// RequestSize 是 rest 客户端会更新的请求大小指标。
	RequestSize SizeMetric = noopSize{}
	// ResponseSize 是 rest 客户端会更新的响应大小指标。
	ResponseSize SizeMetric = noopSize{}
	// RateLimiterLatency 是客户端限流器的延迟指标。
	RateLimiterLatency LatencyMetric = noopLatency{}
	// RequestResult 是 rest 客户端会更新的结果指标。
	RequestResult ResultMetric = noopResult{}
	// ExecPluginCalls 是对 exec 插件的调用次数，按退出码和调用状态区分。
	ExecPluginCalls CallsMetric = noopCalls{}
	// ExecPluginPolicyCalls 是插件策略检查的调用次数，按 {"allowed", "denied"} 区分
	ExecPluginPolicyCalls PolicyCallsMetric = noopPolicy{}
	// RequestRetry 跟踪发往服务端的重试次数。
	RequestRetry RetryMetric = noopRetry{}
	// TransportCacheEntries 跟踪内部 transport 缓存的条目数。
	TransportCacheEntries TransportCacheMetric = noopTransportCache{}
	// TransportCreateCalls 统计新 transport 被创建的次数
	TransportCreateCalls TransportCreateCallsMetric = noopTransportCreateCalls{}
	// TransportCAReloads 统计 CA 重载尝试的次数
	TransportCAReloads TransportCAReloadsMetric = noopTransportCAReloads{}
	// TransportCertRotationGCCalls 统计 GC 清理时调用证书轮换 goroutine
	// cancel 函数的次数
	TransportCertRotationGCCalls TransportCertRotationGCCallsMetric = noopTransportCertRotationGCCalls{}
	// TransportCacheGCCalls 按结果（deleted、skipped）统计 GC 清理
	// 尝试删除 transport 缓存条目的次数。
	TransportCacheGCCalls TransportCacheGCCallsMetric = noopTransportCacheGCCalls{}
)

// RegisterOpts 包含所有待注册的指标，指标字段可以为 nil。
// nil 字段表示“保持默认空实现”，这样适配包可以只注册自己关心的指标。
type RegisterOpts struct {
	ClientCertExpiry             ExpiryMetric
	ClientCertRotationAge        DurationMetric
	RequestLatency               LatencyMetric
	ResolverLatency              ResolverLatencyMetric
	RequestSize                  SizeMetric
	ResponseSize                 SizeMetric
	RateLimiterLatency           LatencyMetric
	RequestResult                ResultMetric
	ExecPluginCalls              CallsMetric
	ExecPluginPolicyCalls        PolicyCallsMetric
	RequestRetry                 RetryMetric
	TransportCacheEntries        TransportCacheMetric
	TransportCreateCalls         TransportCreateCallsMetric
	TransportCAReloads           TransportCAReloadsMetric
	TransportCertRotationGCCalls TransportCertRotationGCCallsMetric
	TransportCacheGCCalls        TransportCacheGCCallsMetric

	// RegisterFn 若非 nil，会被 EnsureRegistered() 在第一个 rest 客户端
	// 构造之前恰好调用一次。适配包用它来推迟依赖运行时状态的注册
	// （例如特性门控在指标 Create() 时才被读取），同时不改变适配包的导入侧约定。
	RegisterFn func()
}

// Register 注册 rest 客户端使用的指标，只能调用一次。
// 使用 sync.Once 保证只有首次调用生效，避免多个适配包互相覆盖。
func Register(opts RegisterOpts) {
	registerMetrics.Do(func() {
		if opts.ClientCertExpiry != nil {
			ClientCertExpiry = opts.ClientCertExpiry
		}
		if opts.ClientCertRotationAge != nil {
			ClientCertRotationAge = opts.ClientCertRotationAge
		}
		if opts.RequestLatency != nil {
			RequestLatency = opts.RequestLatency
		}
		if opts.ResolverLatency != nil {
			ResolverLatency = opts.ResolverLatency
		}
		if opts.RequestSize != nil {
			RequestSize = opts.RequestSize
		}
		if opts.ResponseSize != nil {
			ResponseSize = opts.ResponseSize
		}
		if opts.RateLimiterLatency != nil {
			RateLimiterLatency = opts.RateLimiterLatency
		}
		if opts.RequestResult != nil {
			RequestResult = opts.RequestResult
		}
		if opts.ExecPluginCalls != nil {
			ExecPluginCalls = opts.ExecPluginCalls
		}
		if opts.ExecPluginPolicyCalls != nil {
			ExecPluginPolicyCalls = opts.ExecPluginPolicyCalls
		}
		if opts.RequestRetry != nil {
			RequestRetry = opts.RequestRetry
		}
		if opts.TransportCacheEntries != nil {
			TransportCacheEntries = opts.TransportCacheEntries
		}
		if opts.TransportCreateCalls != nil {
			TransportCreateCalls = opts.TransportCreateCalls
		}
		if opts.TransportCAReloads != nil {
			TransportCAReloads = opts.TransportCAReloads
		}
		if opts.TransportCertRotationGCCalls != nil {
			TransportCertRotationGCCalls = opts.TransportCertRotationGCCalls
		}
		if opts.TransportCacheGCCalls != nil {
			TransportCacheGCCalls = opts.TransportCacheGCCalls
		}
		if opts.RegisterFn != nil {
			ensureRegisteredFn = opts.RegisterFn
		}
	})
}

// 以下类型是各指标接口的空实现，用于未注册适配包时的默认值。
type noopDuration struct{}

func (noopDuration) Observe(time.Duration) {}

type noopExpiry struct{}

func (noopExpiry) Set(*time.Time) {}

type noopLatency struct{}

func (noopLatency) Observe(context.Context, string, url.URL, time.Duration) {}

type noopResolverLatency struct{}

func (n noopResolverLatency) Observe(ctx context.Context, host string, latency time.Duration) {
}

type noopSize struct{}

func (noopSize) Observe(context.Context, string, string, float64) {}

type noopResult struct{}

func (noopResult) Increment(context.Context, string, string, string) {}

type noopCalls struct{}

func (noopCalls) Increment(int, string) {}

type noopPolicy struct{}

func (noopPolicy) Increment(string) {}

type noopRetry struct{}

func (noopRetry) IncrementRetry(context.Context, string, string, string) {}

type noopTransportCache struct{}

func (noopTransportCache) Observe(int) {}

type noopTransportCreateCalls struct{}

func (noopTransportCreateCalls) Increment(string) {}

type noopTransportCAReloads struct{}

func (noopTransportCAReloads) Increment(result, reason string) {}

type noopTransportCertRotationGCCalls struct{}

func (noopTransportCertRotationGCCalls) Increment() {}

type noopTransportCacheGCCalls struct{}

func (noopTransportCacheGCCalls) Increment(string) {}
