/*
Copyright 2024 The Kubernetes Authors.

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

// Package feature 探测 etcd 后端是否支持某些可选特性（如 RequestWatchProgress、
// RangeStream），并把结果缓存起来供存储层在请求路径上快速查询。
package feature

import (
	"context"
	"fmt"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
)

var (
	// Define these static versions to use for checking version of etcd, issue on kubernetes #123192
	//
	// 这些版本号对应已知 bug 的修复版本，写成常量避免每次探测都解析字符串。
	v3_4_31 = version.MustParseSemantic("3.4.31")
	v3_5_0  = version.MustParseSemantic("3.5.0")
	v3_5_13 = version.MustParseSemantic("3.5.13")

	// DefaultFeatureSupportChecker is a shared global etcd FeatureSupportChecker.
	//
	// 全局共享，使不同存储实例对同一集群得到一致的结论。
	DefaultFeatureSupportChecker FeatureSupportChecker = newDefaultFeatureSupportChecker()
)

// rangeStreamRecheckInterval is how long RangeStream stays marked unsupported
// before Supports reports it supported again. See storage.RangeStream.
//
// 不永久禁用 RangeStream，是为了在 etcd 升级后自动恢复能力，而无需重启 apiserver。
const rangeStreamRecheckInterval = 10 * time.Minute

// FeatureSupportChecker to define Supports functions.
type FeatureSupportChecker interface {
	// Supports reports whether the etcd backend supports the given feature, from
	// the result cached by CheckClient/MarkUnsupported. Before support is known it
	// returns the feature's default, documented on the Feature constants (see
	// storage.RequestWatchProgress and storage.RangeStream). Support can flip at
	// runtime as endpoints are checked.
	Supports(feature storage.Feature) bool
	// CheckClient works with etcd client to recalcualte feature support and cache it internally.
	// All etcd clients should support feature to cause `Supports` return true.
	// If client A supports and client B doesn't support the feature, the `Supports` will
	// first return true at client A initializtion and then return false on client B
	// initialzation, it can flip the support at runtime.
	CheckClient(ctx context.Context, c client, feature storage.Feature)
	// MarkUnsupported records that an etcd endpoint reported the feature
	// unimplemented. See the Feature constants for per-feature recheck behavior
	// (e.g. storage.RangeStream).
	MarkUnsupported(feature storage.Feature)
}

// defaultFeatureSupportChecker 缓存探测结果。
//
// progressNotifySupported 为 nil 表示“尚未探测”；RangeStream 用时间戳而非布尔值，
// 以便在 recheck 间隔后自动重新允许。
type defaultFeatureSupportChecker struct {
	lock                     sync.Mutex
	progressNotifySupported  *bool
	rangeStreamUnsupportedAt time.Time
	clock                    clock.Clock
	checkingEndpoint         map[string]struct{}
}

// newDefaultFeatureSupportChecker 创建空的检查器。
func newDefaultFeatureSupportChecker() *defaultFeatureSupportChecker {
	return &defaultFeatureSupportChecker{
		checkingEndpoint: make(map[string]struct{}),
		clock:            clock.RealClock{},
	}
}

// NewDefaultFeatureSupportChecker returns a new FeatureSupportChecker.
// DefaultFeatureSupportChecker is the shared instance used by the storage layer.
func NewDefaultFeatureSupportChecker() FeatureSupportChecker {
	return newDefaultFeatureSupportChecker()
}

// Supports can check the featue from anywhere without storage if it was cached before.
func (f *defaultFeatureSupportChecker) Supports(feature storage.Feature) bool {
	switch feature {
	case storage.RequestWatchProgress:
		f.lock.Lock()
		defer f.lock.Unlock()

		// 未探测前按“不支持”处理，避免向旧 etcd 发送其无法理解的参数。
		return ptr.Deref(f.progressNotifySupported, false)
	case storage.RangeStream:
		f.lock.Lock()
		defer f.lock.Unlock()

		if f.rangeStreamUnsupportedAt.IsZero() {
			return true
		}
		return f.clock.Since(f.rangeStreamUnsupportedAt) >= rangeStreamRecheckInterval
	default:
		runtime.HandleError(fmt.Errorf("feature %q is not implemented in DefaultFeatureSupportChecker", feature))
		return false
	}
}

// CheckClient accepts client and calculate the support per endpoint and caches it.
//
// 每个 endpoint 只探测一次（checkingEndpoint 去重），探测在后台异步进行，
// 避免阻塞存储初始化。
func (f *defaultFeatureSupportChecker) CheckClient(ctx context.Context, c client, feature storage.Feature) {
	switch feature {
	case storage.RequestWatchProgress:
		f.checkClient(ctx, c)
	default:
		runtime.HandleError(fmt.Errorf("feature %q is not implemented in DefaultFeatureSupportChecker", feature))
	}
}

// MarkUnsupported 记录 RangeStream 在某 endpoint 上不可用，并启动重新探测计时。
func (f *defaultFeatureSupportChecker) MarkUnsupported(feature storage.Feature) {
	switch feature {
	case storage.RangeStream:
		f.lock.Lock()
		defer f.lock.Unlock()
		f.rangeStreamUnsupportedAt = f.clock.Now()
	default:
		runtime.HandleError(fmt.Errorf("feature %q is not implemented in DefaultFeatureSupportChecker", feature))
	}
}

// checkClient 为每个尚未探测的 endpoint 启动一个带指数退避的后台探测 goroutine。
func (f *defaultFeatureSupportChecker) checkClient(ctx context.Context, c client) {
	// start with 10 ms, multiply by 2 each step, until 15 s and stays on 15 seconds.
	delayFunc := wait.Backoff{
		Duration: 10 * time.Millisecond,
		Cap:      15 * time.Second,
		Factor:   2.0,
		Steps:    11}.DelayFunc()
	f.lock.Lock()
	defer f.lock.Unlock()
	for _, ep := range c.Endpoints() {
		if _, found := f.checkingEndpoint[ep]; found {
			continue
		}
		f.checkingEndpoint[ep] = struct{}{}
		go func(ep string) {
			defer runtime.HandleCrashWithContext(ctx)
			err := delayFunc.Until(ctx, true, true, func(ctx context.Context) (done bool, err error) {
				internalErr := f.clientSupportsRequestWatchProgress(ctx, c, ep)
				return internalErr == nil, nil
			})
			if err != nil {
				klog.ErrorS(err, "Failed to check if RequestWatchProgress is supported by etcd after retrying")
			}
		}(ep)
	}
}

// clientSupportsRequestWatchProgress 汇总单个 endpoint 的探测结果。
//
// 关键语义：只要有一个 endpoint 不支持，整体就标记为不支持；
// 只有从未见过不支持的 endpoint 时才置为 true。
func (f *defaultFeatureSupportChecker) clientSupportsRequestWatchProgress(ctx context.Context, c client, ep string) error {
	supported, err := endpointSupportsRequestWatchProgress(ctx, c, ep)
	if err != nil {
		return err
	}
	f.lock.Lock()
	defer f.lock.Unlock()

	if !supported {
		klog.Infof("RequestWatchProgress feature is not supported by %q endpoint", ep)
		f.progressNotifySupported = ptr.To(false)
		return nil
	}
	if f.progressNotifySupported == nil {
		f.progressNotifySupported = ptr.To(true)
	}
	return nil
}

// Sub interface of etcd client.
//
// 只抽取探测所需的两个方法，便于测试注入假客户端。
type client interface {
	// Endpoints returns list of endpoints in etcd client.
	Endpoints() []string
	// Status retrieves the status information from the etcd client connected to the specified endpoint.
	// It takes a context.Context parameter for cancellation or timeout control.
	// It returns a clientv3.StatusResponse containing the status information or an error if the operation fails.
	Status(ctx context.Context, endpoint string) (*clientv3.StatusResponse, error)
}

// endpointSupportsRequestWatchProgress evaluates whether RequestWatchProgress supported by current version of etcd endpoint.
// Based on this issues:
//   - https://github.com/etcd-io/etcd/issues/15220 - Fixed in etcd v3.4.25+ and v3.5.8+
//   - https://github.com/etcd-io/etcd/issues/17507 - Fixed in etcd v3.4.31+ and v3.5.13+
//
// 判定规则：3.4.x 需 >= 3.4.31；3.5.x 需 >= 3.5.13；其余（3.6+）视为支持。
func endpointSupportsRequestWatchProgress(ctx context.Context, c client, endpoint string) (bool, error) {
	resp, err := c.Status(ctx, endpoint)
	if err != nil {
		return false, fmt.Errorf("failed checking etcd version, endpoint: %q: %w", endpoint, err)
	}
	ver, err := version.ParseSemantic(resp.Version)
	if err != nil {
		// Assume feature is not supported if etcd version cannot be parsed.
		klog.ErrorS(err, "Failed to parse etcd version", "version", resp.Version)
		return false, nil
	}
	if ver.LessThan(v3_4_31) || ver.AtLeast(v3_5_0) && ver.LessThan(v3_5_13) {
		return false, nil
	}
	return true, nil
}
