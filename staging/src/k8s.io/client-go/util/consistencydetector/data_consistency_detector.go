/*
Copyright 2023 The Kubernetes Authors.

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

// Package consistencydetector 实现 WatchList 的“数据一致性检测”：
// 通过与 etcd 快照做额外 LIST 对比，在 CI 中尽早发现 watch cache 漏事件或不一致。
package consistencydetector

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/diff"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// dataConsistencyDetectionForWatchListEnabled 在 init 时一次性解析环境变量，
// 之后只读，避免运行期反复读取环境变量造成不一致。
var dataConsistencyDetectionForWatchListEnabled = false

func init() {
	dataConsistencyDetectionForWatchListEnabled, _ = strconv.ParseBool(os.Getenv("KUBE_WATCHLIST_INCONSISTENCY_DETECTOR"))
}

// 二进制启动时设置了 KUBE_WATCHLIST_INCONSISTENCY_DETECTOR 环境变量时，该函数返回 true。
func IsDataConsistencyDetectionForWatchListEnabled() bool {
	return dataConsistencyDetectionForWatchListEnabled
}

// 该函数供测试开启/关闭数据一致性检测，返回用于恢复原值的函数。
func SetDataConsistencyDetectionForWatchListEnabledForTest(enabled bool) func() {
	original := dataConsistencyDetectionForWatchListEnabled
	dataConsistencyDetectionForWatchListEnabled = enabled
	return func() {
		dataConsistencyDetectionForWatchListEnabled = original
	}
}

// RetrieveItemsFunc 返回已经被 informer 消费的对象，用于与 etcd 快照比较。
type RetrieveItemsFunc[U any] func() []U

// ListFunc 执行一次 LIST 请求；参数与普通列表一致，便于复用现有客户端。
type ListFunc[T runtime.Object] func(ctx context.Context, options metav1.ListOptions) (T, error)

// TransformFunc 在比较前对对象做归一化（例如剥离版本相关字段）。
type TransformFunc func(interface{}) (interface{}, error)

// CheckDataConsistency 仅供测试使用。不能直接调用 checkWatchListDataConsistencyIfRequested，
// 因为它受环境变量控制；又不能修改环境变量，否则会影响本包其他测试。
//
// 若上下文中的 logger 已经能标识被监听的对象，identity 字符串可以留空。
// 检测逻辑会额外发起一次指向 etcd 的 LIST，因此只应在 CI/测试中开启。
func CheckDataConsistency[T runtime.Object, U any](ctx context.Context, identity string, lastSyncedResourceVersion string, listFn ListFunc[T], listItemTransformFunc TransformFunc, listOptions metav1.ListOptions, retrieveItemsFn RetrieveItemsFunc[U]) {
	logger := klog.FromContext(ctx)
	if identity != "" {
		logger = klog.LoggerWithName(logger, identity)
	}
	if !canFormAdditionalListCall(lastSyncedResourceVersion, listOptions) {
		logger.V(4).Info("Data consistency check is enabled but the parameters (RV, ListOptions) doesn't allow for creating a valid LIST request. Skipping the data consistency check.")
		return
	}
	logger.Info("Warning: data consistency check is enabled, this will result in an additional call to the API server.")

	retrievedItems := toMetaObjectSliceOrDie(retrieveItemsFn())
	listOptions = prepareListCallOptions(lastSyncedResourceVersion, listOptions, len(retrievedItems))
	var list runtime.Object
	err := wait.PollUntilContextCancel(ctx, time.Second, true, func(_ context.Context) (done bool, err error) {
		list, err = listFn(ctx, listOptions)
		if err != nil {
			// 一致性检测只在 CI 开启，LIST 调用本身终将由 client-go 淘汰；失败时只记日志并重试。
			logger.Info("Failed to list data from the server, retrying until context is canceled", "err", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		logger.Error(err, "Failed to list data from the server, the data consistency check won't be performed, stopCh was closed")
		return
	}

	rawListItems, err := meta.ExtractListWithAlloc(list)
	if err != nil {
		panic(err) // this should never happen
	}
	// 变换必须在两侧都应用，否则可能因序列化差异产生误报。
	if listItemTransformFunc != nil {
		for i := range rawListItems {
			obj, err := listItemTransformFunc(rawListItems[i])
			if err != nil {
				panic(err)
			}
			rawListItems[i] = obj.(runtime.Object)
		}
	}
	listItems := toMetaObjectSliceOrDie(rawListItems)

	sort.Sort(byUID(listItems))
	sort.Sort(byUID(retrievedItems))

	if !reflect.DeepEqual(listItems, retrievedItems) {
		logger.Info("Previously received data is different than received by the standard list api call against etcd", "diff", diff.Diff(listItems, retrievedItems))
		msg := fmt.Sprintf("data inconsistency detected for %s, panicking!", identity)
		panic(msg)
	}
}

// canFormAdditionalListCall 确保能够构造出用于一致性校验的合法 LIST 请求。
// Exact RV 查询不允许带 continue，也不允许 RV="0"，否则请求会被 apiserver 拒绝。
func canFormAdditionalListCall(lastSyncedResourceVersion string, listOptions metav1.ListOptions) bool {
	// 由于设置了 ResourceVersionMatch=Exact，必须确保没有 continue 参数。
	// https://github.com/kubernetes/kubernetes/blob/be4afb9ef90b19ccb6f7e595cbdb247e088b2347/staging/src/k8s.io/apimachinery/pkg/apis/meta/internalversion/validation/validation.go#L38
	if len(listOptions.Continue) > 0 {
		return false
	}

	// 由于设置了 ResourceVersionMatch=Exact，必须保证 RV 合法：校验逻辑禁止 RV == "0"。
	// https://github.com/kubernetes/kubernetes/blob/be4afb9ef90b19ccb6f7e595cbdb247e088b2347/staging/src/k8s.io/apimachinery/pkg/apis/meta/internalversion/validation/validation.go#L44
	if lastSyncedResourceVersion == "0" {
		return false
	}

	return true
}

// prepareListCallOptions 调整输入选项，使 LIST 请求直达 etcd。
func prepareListCallOptions(lastSyncedResourceVersion string, listOptions metav1.ListOptions, retrievedItemsCount int) metav1.ListOptions {
	// 这是兼容旧行为的分支：ResourceVersion 为 "0" 时 watch cache 会忽略 Limit，因此要与
	// 直达 etcd 的数据比较，这里也必须去掉 Limit。
	// 若返回条目数少于 Limit，说明 watch cache 被禁用或数据不足，此时保留 Limit 也能与 etcd 结果比较。
	if listOptions.ResourceVersion == "0" && listOptions.Limit > 0 && int64(retrievedItemsCount) > listOptions.Limit {
		listOptions.Limit = 0
	}

	// 设置 RV 与 RVM，以便直接从 etcd 获取快照。
	listOptions.ResourceVersion = lastSyncedResourceVersion
	listOptions.ResourceVersionMatch = metav1.ResourceVersionMatchExact

	return listOptions
}

// byUID 以 UID 排序，保证两侧比较不受 LIST 顺序影响。
type byUID []metav1.Object

func (a byUID) Len() int           { return len(a) }
func (a byUID) Less(i, j int) bool { return a[i].GetUID() < a[j].GetUID() }
func (a byUID) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// toMetaObjectSliceOrDie 把任意对象切片转成 metav1.Object；传入非对象属于编程错误，直接 panic。
func toMetaObjectSliceOrDie[T any](s []T) []metav1.Object {
	result := make([]metav1.Object, len(s))
	for i, v := range s {
		m, err := meta.Accessor(v)
		if err != nil {
			panic(err)
		}
		result[i] = m
	}
	return result
}
