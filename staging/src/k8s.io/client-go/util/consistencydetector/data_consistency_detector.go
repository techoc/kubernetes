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

// IsDataConsistencyDetectionForWatchListEnabled returns true when
// the KUBE_WATCHLIST_INCONSISTENCY_DETECTOR environment variable was set during a binary startup.
func IsDataConsistencyDetectionForWatchListEnabled() bool {
	return dataConsistencyDetectionForWatchListEnabled
}

// SetDataConsistencyDetectionForWatchListEnabledForTest allows to enable/disable data consistency detection for testing purposes.
// It returns a function that restores the original value.
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

// CheckDataConsistency exists solely for testing purposes.
// we cannot use checkWatchListDataConsistencyIfRequested because
// it is guarded by an environmental variable.
// we cannot manipulate the environmental variable because
// it will affect other tests in this package.
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
			// the consistency check will only be enabled in the CI
			// and LIST calls in general will be retired by the client-go library
			// if we fail simply log and retry
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
	// since we are setting ResourceVersionMatch to metav1.ResourceVersionMatchExact
	// we need to make sure that the continuation hasn't been set
	// https://github.com/kubernetes/kubernetes/blob/be4afb9ef90b19ccb6f7e595cbdb247e088b2347/staging/src/k8s.io/apimachinery/pkg/apis/meta/internalversion/validation/validation.go#L38
	if len(listOptions.Continue) > 0 {
		return false
	}

	// since we are setting ResourceVersionMatch to metav1.ResourceVersionMatchExact
	// we need to make sure that the RV is valid because the validation code forbids RV == "0"
	// https://github.com/kubernetes/kubernetes/blob/be4afb9ef90b19ccb6f7e595cbdb247e088b2347/staging/src/k8s.io/apimachinery/pkg/apis/meta/internalversion/validation/validation.go#L44
	if lastSyncedResourceVersion == "0" {
		return false
	}

	return true
}

// prepareListCallOptions changes the input list options so that
// the list call goes directly to etcd
func prepareListCallOptions(lastSyncedResourceVersion string, listOptions metav1.ListOptions, retrievedItemsCount int) metav1.ListOptions {
	// this is our legacy case:
	//
	// the watch cache skips the Limit if the ResourceVersion was set to "0"
	// thus, to compare with data retrieved directly from etcd
	// we need to skip the limit to for the list call as well.
	//
	// note that when the number of retrieved items is less than the request limit,
	// it means either the watch cache is disabled, or there is not enough data.
	// in both cases, we can use the limit because we will be able to compare
	// the data with the items retrieved from etcd.
	if listOptions.ResourceVersion == "0" && listOptions.Limit > 0 && int64(retrievedItemsCount) > listOptions.Limit {
		listOptions.Limit = 0
	}

	// set the RV and RVM so that we get the snapshot of data
	// directly from etcd.
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
