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

package cacher

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/consistencydetector"
	"k8s.io/client-go/util/watchlist"
)

// listerWatcher 隐藏 storage.Interface，对外暴露 cache.ListerWatcher。
//
// 它让 informer/reflector 直接使用 cacher 的存储实现，绕过 REST 层；
// 因此请求校验与特性门控需要在这里自行补齐。
type listerWatcher struct {
	storage         storage.Interface
	resourcePrefix  string
	newListFunc     func() runtime.Object
	contextMetadata metadata.MD

	// 在构造时缓存能力判断，避免每次 List/Watch 重复探测。
	unsupportedWatchListSemantics    bool
	watchListConsistencyCheckEnabled bool
}

// NewListerWatcher 返回由 storage.Interface 支撑的 ListerWatcher。
func NewListerWatcher(storage storage.Interface, resourcePrefix string, newListFunc func() runtime.Object, contextMetadata metadata.MD) cache.ListerWatcher {
	return &listerWatcher{
		storage:                          storage,
		resourcePrefix:                   resourcePrefix,
		newListFunc:                      newListFunc,
		contextMetadata:                  contextMetadata,
		unsupportedWatchListSemantics:    watchlist.DoesClientNotSupportWatchListSemantics(storage),
		watchListConsistencyCheckEnabled: consistencydetector.IsDataConsistencyDetectionForWatchListEnabled(),
	}
}

// 实现 cache.ListerWatcher 接口。
func (lw *listerWatcher) List(options metav1.ListOptions) (runtime.Object, error) {
	list := lw.newListFunc()
	// 内部 list 不使用 label/field selector：调用方（reflector）会自行过滤。
	pred := storage.SelectionPredicate{
		Label:    labels.Everything(),
		Field:    fields.Everything(),
		Limit:    options.Limit,
		Continue: options.Continue,
	}

	storageOpts := storage.ListOptions{
		ResourceVersionMatch: options.ResourceVersionMatch,
		Predicate:            pred,
		Recursive:            true,
	}

	// WatchList 特性下 reflector 内置的 ConsistencyChecker 负责
	// 校验从 server（可能来自 watch cache）收到的数据
	// 与 etcd 中存储的数据一致。
	//
	// 校验时它使用初始请求中的 ResourceVersion，
	// 并设置 ResourceVersionMatch，以便直接从 etcd 取完全相同的数据。
	// 这样才能对比两个数据源并确认一致性。
	//
	// 下面判断请求是否来自 ConsistencyChecker。
	// 若是，则允许显式设置 ResourceVersion。
	//
	// 截至 2025 年 10 月，reflector 自身不会设置 RVM=Exact。
	// 但即便此后有变化，也必须把该语义
	// 传递到存储层，保证请求语义正确。
	watchListEnabled := utilfeature.DefaultFeatureGate.Enabled(features.WatchList)
	supportedRVM := options.ResourceVersionMatch == metav1.ResourceVersionMatchExact
	// 只有一致性检查器会带 RVM=Exact；其他请求忽略 RV，继续走缓存的最新快照。
	if watchListEnabled && lw.watchListConsistencyCheckEnabled && supportedRVM {
		storageOpts.ResourceVersion = options.ResourceVersion
	}

	ctx := context.Background()
	if lw.contextMetadata != nil {
		ctx = metadata.NewOutgoingContext(ctx, lw.contextMetadata)
	}
	if err := lw.storage.GetList(ctx, lw.resourcePrefix, storageOpts, list); err != nil {
		return nil, err
	}
	return list, nil
}

// 实现 cache.ListerWatcher 接口。
func (lw *listerWatcher) Watch(options metav1.ListOptions) (watch.Interface, error) {
	pred := storage.Everything
	pred.AllowWatchBookmarks = options.AllowWatchBookmarks
	opts := storage.ListOptions{
		ResourceVersion:   options.ResourceVersion,
		Predicate:         pred,
		Recursive:         true,
		ProgressNotify:    true,
		SendInitialEvents: options.SendInitialEvents,
		RecordTimestamps:  true,
	}
	ctx := context.Background()
	if lw.contextMetadata != nil {
		ctx = metadata.NewOutgoingContext(ctx, lw.contextMetadata)
	}

	// 需要下面的检查，因为 listWatcher 绕过 REST 层，
	// 选项未经验证。否则可能出现请求了流式传输但特性门控未启用，
	// 导致 bookmark 无法发送的情况。
	//
	// 此时 client-go 在 watch-list 请求出错时会回退到普通 LIST。
	// 因此这里主动返回错误，让 client-go 明确回退到普通 LIST，而不是拿到不完整的事件流。
	if isListWatchRequest(opts) && !utilfeature.DefaultFeatureGate.Enabled(features.WatchList) {
		return nil, fmt.Errorf("sendInitialEvents is forbidden for watch unless the WatchList feature gate is enabled")
	}

	return lw.storage.Watch(ctx, lw.resourcePrefix, opts)
}

// IsWatchListSemanticsUnSupported 告知 reflector 该存储是否不支持 watch-list 语义，
// 从而决定是否退回传统 LIST+WATCH。
func (lw *listerWatcher) IsWatchListSemanticsUnSupported() bool {
	return lw.unsupportedWatchListSemantics
}
