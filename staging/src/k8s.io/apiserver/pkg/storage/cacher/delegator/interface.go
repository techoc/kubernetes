/*
Copyright 2025 The Kubernetes Authors.

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

// Package delegator 决定一次 list/watch 请求能否由 watch cache 直接服务，
// 还是必须委托给底层 etcd。判断依据是 ResourceVersion 语义（Exact、
// NotOlderThan、Continue、一致读）以及缓存是否支持进度通知。
package delegator

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apiserver/pkg/storage"
	etcdfeature "k8s.io/apiserver/pkg/storage/feature"
)

// ShouldDelegateListMeta 把 API 层的 ListOptions 转换为存储层 ListOptions
// 后做委托判断；供只需要 metav1 选项的调用方使用。
func ShouldDelegateListMeta(opts *metav1.ListOptions, cache Helper) (Result, error) {
	return ShouldDelegateList(
		storage.ListOptions{
			ResourceVersionMatch: opts.ResourceVersionMatch,
			ResourceVersion:      opts.ResourceVersion,
			Predicate: storage.SelectionPredicate{
				Continue: opts.Continue,
				Limit:    opts.Limit,
			},
			Recursive: true,
		}, cache)
}

// ShouldDelegateList 按 ResourceVersionMatch 的语义决定是否委托 etcd：
// Exact 必须精确读取；NotOlderThan 允许缓存；空值则依次考虑 continue、
// 旧式精确匹配和一致读。未知取值保守地委托给 etcd，避免返回错误数据。
func ShouldDelegateList(opts storage.ListOptions, cache Helper) (Result, error) {
	// see https://kubernetes.io/docs/reference/using-api/api-concepts/#semantics-for-get-and-list
	switch opts.ResourceVersionMatch {
	case metav1.ResourceVersionMatchExact:
		return cache.ShouldDelegateExactRV(opts.ResourceVersion, opts.Recursive)
	case metav1.ResourceVersionMatchNotOlderThan:
		return Result{ShouldDelegate: false}, nil
	case "":
		// Continue
		if len(opts.Predicate.Continue) > 0 {
			return cache.ShouldDelegateContinue(opts.Predicate.Continue, opts.Recursive)
		}
		// 旧式（legacy）exact match：旧客户端用 limit+rv 表达“从某个版本开始分页”，
		// 语义上等同 Exact，因此走同一条判断路径。
		if opts.Predicate.Limit > 0 && len(opts.ResourceVersion) > 0 && opts.ResourceVersion != "0" {
			return cache.ShouldDelegateExactRV(opts.ResourceVersion, opts.Recursive)
		}
		// Consistent Read
		if opts.ResourceVersion == "" {
			return cache.ShouldDelegateConsistentRead()
		}
		return Result{ShouldDelegate: false}, nil
	default:
		return Result{ShouldDelegate: true}, nil
	}
}

// Helper 由具体缓存实现（cacher 或 CacheWithoutSnapshots）提供，
// 让委托判断逻辑与缓存内部细节解耦。
type Helper interface {
	ShouldDelegateExactRV(rv string, recursive bool) (Result, error)
	ShouldDelegateContinue(continueToken string, recursive bool) (Result, error)
	ShouldDelegateConsistentRead() (Result, error)
}

// 委托判断的结果。
type Result struct {
	// 缓存无法服务该请求、必须委托给 etcd。
	ShouldDelegate bool
	// 是否为一致性读，决定是否需要调用 GetCurrentResourceVersion 获取 RV。
	// 放在该接口中是因为只有 cacher 持有解析 continue token 所需的 keyPrefix。
	ConsistentRead bool
}

// CacheWithoutSnapshots 表示“缓存不可用（如已关闭或处于恢复期）”的退化实现：
// 除 NotOlderThan 外一律委托 etcd，保证语义正确性优先于性能。
type CacheWithoutSnapshots struct{}

var _ Helper = CacheWithoutSnapshots{}

func (c CacheWithoutSnapshots) ShouldDelegateContinue(continueToken string, recursive bool) (Result, error) {
	return Result{
		ShouldDelegate: true,
		// 负数 RV 的 continue 本应视为一致性读，但 list_work_estimator.go 中拿不到解析 token 所需的 keyPrefix。
		ConsistentRead: false,
	}, nil
}

func (c CacheWithoutSnapshots) ShouldDelegateExactRV(rv string, recursive bool) (Result, error) {
	return Result{
		ShouldDelegate: true,
		ConsistentRead: false,
	}, nil
}

// ShouldDelegateConsistentRead 在 etcd 支持 WatchProgressNotify 时可以不委托，
// 否则必须直读 etcd 才能保证读到最新数据。
func (c CacheWithoutSnapshots) ShouldDelegateConsistentRead() (Result, error) {
	return Result{
		ShouldDelegate: !ConsistentReadSupported(),
		ConsistentRead: true,
	}, nil
}

// ConsistentReadSupported 判断缓存能否服务 RV 尚未被其观测到的读请求。
// 函数放在此处是为了避免 cacher 与 list_work_estimator 之间的循环导入。
func ConsistentReadSupported() bool {
	return etcdfeature.DefaultFeatureSupportChecker.Supports(storage.RequestWatchProgress)
}
