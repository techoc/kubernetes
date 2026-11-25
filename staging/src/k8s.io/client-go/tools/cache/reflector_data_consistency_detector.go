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

package cache

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/consistencydetector"
)

// checkWatchListDataConsistencyIfRequested 只有在二进制启动时设置了环境变量
// KUBE_WATCHLIST_INCONSISTENCY_DETECTOR 才会执行数据一致性检查。
//
// 该检查只应在 CI 中强制运行，不用于生产环境。它确保 watch-list API 取回的数据
// 与针对 etcd 执行标准 list API 收到的数据完全一致。
// WatchList 相比传统 List+Watch 可能因服务端缓存组合顺序不同而漏事件，
// 该检查把 WatchList 结果与一次直连 etcd 语义的标准 List 做比对。因开销大且可能
// panic，仅应在 CI 中通过环境变量开启，生产环境不要启用。
//
// 注意：检测到数据不一致时本函数会 panic，这是有意为之，便于在 CI 中捕获问题。
func checkWatchListDataConsistencyIfRequested[T runtime.Object, U any](ctx context.Context, identity string, lastSyncedResourceVersion string, listFn consistencydetector.ListFunc[T], listItemTransformFunc func(interface{}) (interface{}, error), retrieveItemsFn consistencydetector.RetrieveItemsFunc[U]) {
	// 环境变量在进程启动时读取一次，未开启时每条 watch 路径零额外开销。
	if !consistencydetector.IsDataConsistencyDetectionForWatchListEnabled() {
		return
	}
	// informer 场景下这里传入空的 ListOptions，因为 listFn 可能在构造 informer 时
	// 已被包装过过滤逻辑；若这里再传限制条件会重复过滤导致误报。
	consistencydetector.CheckDataConsistency(ctx, identity, lastSyncedResourceVersion, listFn, listItemTransformFunc, metav1.ListOptions{}, retrieveItemsFn)
}
