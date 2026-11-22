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

// Package metrics provides abstractions for registering which metrics
// to record.
//
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
	// ensureRegisteredFn, if set via RegisterOpts.RegisterFn, is invoked
	// exactly once before any rest client is constructed. Adapter packages
	// (e.g. k8s.io/component-base/metrics/prometheus/restclient) install
	// this callback in their init() so that the actual registration with
	// legacyregistry — and the metric Create() that reads
	// feature-gate-derived options like NativeHistograms — happens at
	// runtime rather than at init() time. See EnsureRegistered for the
	// caller-side contract.
	ensureRegisteredFn func()
)

// EnsureRegistered invokes the callback installed via RegisterOpts.RegisterFn (if
// any) exactly once. Callers should treat it as idempotent; subsequent calls are
// effectively free.
//
// New public constructors or entry points for packages in client-go that create
// a REST client, HTTP transport, or credential provider should invoke EnsureRegistered()
// at the very beginning of the function. Adapter Observe methods
// also call EnsureRegistered(), so no observations are lost if a constructor
// forgets to invoke it, but if invoked, the entrypoint call shifts registration from
// "first-observation" to "first client construction", meaning the metric
// series is visible to Promteheus scrapes from process startup rather than
// appearing only after the first request.
//
// 利用 sync.Once 保证回调只执行一次；回调未安装时直接返回。
func EnsureRegistered() {
	if ensureRegisteredFn != nil {
		ensureRegisteredOnce.Do(ensureRegisteredFn)
	}
}

// DurationMetric is a measurement of some amount of time.
type DurationMetric interface {
	Observe(duration time.Duration)
}

// ExpiryMetric sets some time of expiry. If nil, assume not relevant.
type ExpiryMetric interface {
	Set(expiry *time.Time)
}

// LatencyMetric observes client latency partitioned by verb and url.
type LatencyMetric interface {
	Observe(ctx context.Context, verb string, u url.URL, latency time.Duration)
}

// ResolverLatencyMetric 观测 DNS 解析延迟，按主机名区分。
type ResolverLatencyMetric interface {
	Observe(ctx context.Context, host string, latency time.Duration)
}

// SizeMetric observes client response size partitioned by verb and host.
type SizeMetric interface {
	Observe(ctx context.Context, verb string, host string, size float64)
}

// ResultMetric counts response codes partitioned by method and host.
type ResultMetric interface {
	Increment(ctx context.Context, code string, method string, host string)
}

// CallsMetric counts calls that take place for a specific exec plugin.
type CallsMetric interface {
	// Increment increments a counter per exitCode and callStatus.
	Increment(exitCode int, callStatus string)
}

// CallsMetric counts the success or failure of execution for exec plugins.
//
// 名称沿用历史注释，实际统计的是 exec 插件策略检查结果（allowed/denied）。
type PolicyCallsMetric interface {
	// Increment increments a counter per status { "allowed", "denied" }
	Increment(status string)
}

// RetryMetric counts the number of retries sent to the server
// partitioned by code, method, and host.
type RetryMetric interface {
	IncrementRetry(ctx context.Context, code string, method string, host string)
}

// TransportCacheMetric shows the number of entries in the internal transport cache
type TransportCacheMetric interface {
	Observe(value int)
}

// TransportCreateCallsMetric counts the number of times a transport is created
// partitioned by the result of the cache: hit, miss, miss-gc, uncacheable
type TransportCreateCallsMetric interface {
	Increment(result string)
}

// TransportCAReloadsMetric counts the number of times a CA reload is attempted,
// partitioned by the result and reason.
type TransportCAReloadsMetric interface {
	Increment(result, reason string)
}

// TransportCertRotationGCCallsMetric counts the number of times a cert rotation
// goroutine cancel func is called via GC cleanup.
type TransportCertRotationGCCallsMetric interface {
	Increment()
}

// TransportCacheGCCallsMetric counts the number of times a GC cleanup
// attempts to delete a cache entry, partitioned by the result: deleted, skipped.
type TransportCacheGCCallsMetric interface {
	Increment(result string)
}

var (
	// 以下变量在 Register 之前均为空实现，因此任何调用点都可以安全记录指标。
	// ClientCertExpiry is the expiry time of a client certificate
	ClientCertExpiry ExpiryMetric = noopExpiry{}
	// ClientCertRotationAge is the age of a certificate that has just been rotated.
	ClientCertRotationAge DurationMetric = noopDuration{}
	// RequestLatency is the latency metric that rest clients will update.
	RequestLatency LatencyMetric = noopLatency{}
	// ResolverLatency is the latency metric that DNS resolver will update
	ResolverLatency ResolverLatencyMetric = noopResolverLatency{}
	// RequestSize is the request size metric that rest clients will update.
	RequestSize SizeMetric = noopSize{}
	// ResponseSize is the response size metric that rest clients will update.
	ResponseSize SizeMetric = noopSize{}
	// RateLimiterLatency is the client side rate limiter latency metric.
	RateLimiterLatency LatencyMetric = noopLatency{}
	// RequestResult is the result metric that rest clients will update.
	RequestResult ResultMetric = noopResult{}
	// ExecPluginCalls is the number of calls made to an exec plugin, partitioned by
	// exit code and call status.
	ExecPluginCalls CallsMetric = noopCalls{}
	// ExecPluginPolicyCalls is the number of plugin policy check calls, partitioned
	// by {"allowed", "denied"}
	ExecPluginPolicyCalls PolicyCallsMetric = noopPolicy{}
	// RequestRetry is the retry metric that tracks the number of
	// retries sent to the server.
	RequestRetry RetryMetric = noopRetry{}
	// TransportCacheEntries is the metric that tracks the number of entries in the
	// internal transport cache.
	TransportCacheEntries TransportCacheMetric = noopTransportCache{}
	// TransportCreateCalls is the metric that counts the number of times a new transport
	// is created
	TransportCreateCalls TransportCreateCallsMetric = noopTransportCreateCalls{}
	// TransportCAReloads is the metric that counts the number of times a CA reload is attempted
	TransportCAReloads TransportCAReloadsMetric = noopTransportCAReloads{}
	// TransportCertRotationGCCalls counts the number of times a cert rotation goroutine
	// cancel func is called via GC cleanup
	TransportCertRotationGCCalls TransportCertRotationGCCallsMetric = noopTransportCertRotationGCCalls{}
	// TransportCacheGCCalls counts the number of times a GC cleanup attempts
	// to delete a transport cache entry, partitioned by result: deleted, skipped.
	TransportCacheGCCalls TransportCacheGCCallsMetric = noopTransportCacheGCCalls{}
)

// RegisterOpts contains all the metrics to register. Metrics may be nil.
//
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

	// RegisterFn, if non-nil, is invoked exactly once by EnsureRegistered().
	// before the first rest client is constructed. Adapters use this to defer
	// registrations that depend on runtime state (eg., feature gates read my metric
	// Create() without changing the import side contract of the adapter package.)
	RegisterFn func()
}

// Register registers metrics for the rest client to use. This can
// only be called once.
//
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
