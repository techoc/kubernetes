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

// checkWatchListDataConsistencyIfRequested performs a data consistency check only when
// the KUBE_WATCHLIST_INCONSISTENCY_DETECTOR environment variable was set during a binary startup.
//
// The consistency check is meant to be enforced only in the CI, not in production.
// The check ensures that data retrieved by the watch-list api call
// is exactly the same as data received by the standard list api call against etcd.
//
// Note that this function will panic when data inconsistency is detected.
// This is intentional because we want to catch it in the CI.
//
// 中文说明：WatchList 相比传统 List+Watch 可能因服务端缓存组合顺序不同而漏事件，
// 该检查把 WatchList 结果与一次直连 etcd 语义的标准 List 做比对。因开销大且可能
// panic，仅应在 CI 中通过环境变量开启，生产环境不要启用。
func checkWatchListDataConsistencyIfRequested[T runtime.Object, U any](ctx context.Context, identity string, lastSyncedResourceVersion string, listFn consistencydetector.ListFunc[T], listItemTransformFunc func(interface{}) (interface{}, error), retrieveItemsFn consistencydetector.RetrieveItemsFunc[U]) {
	// 中文：环境变量在进程启动时读取一次，未开启时每条 watch 路径零额外开销。
	if !consistencydetector.IsDataConsistencyDetectionForWatchListEnabled() {
		return
	}
	// for informers we pass an empty ListOptions because
	// listFn might be wrapped for filtering during informer construction.
	// 中文：过滤逻辑已封装在 listFn 中，若这里再传限制条件会重复过滤导致误报。
	consistencydetector.CheckDataConsistency(ctx, identity, lastSyncedResourceVersion, listFn, listItemTransformFunc, metav1.ListOptions{}, retrieveItemsFn)
}
