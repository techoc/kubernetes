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

// Package watchlist turns a List request into a streaming watch-list request
// (sendInitialEvents) when the client feature gate allows it.
//
// Package watchlist 负责把普通 List 选项改写为 watch-list 请求：服务端先发送
// 初始事件再持续 watch，客户端无需单独 List。改写只在语义等价时进行，
// 否则调用方应退回普通 List。
package watchlist

import (
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metainternalversionvalidation "k8s.io/apimachinery/pkg/apis/meta/internalversion/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/utils/ptr"
)

// scheme 仅用于在外部与内部 ListOptions 之间转换，以便复用服务端的校验逻辑。
var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(metainternalversion.AddToScheme(scheme))
}

// PrepareWatchListOptionsFromListOptions creates a new ListOptions
// that can be used for a watch-list request from the given listOptions.
//
// This function also determines if the given listOptions can be used to form a watch-list request,
// which would result in streaming semantically equivalent data from the server.
//
// 返回的布尔值为 false 时调用方必须使用原始 List；为 true 时 options 已改为
// Watch=true + SendInitialEvents=true，且限制条件与原始 List 等价。
func PrepareWatchListOptionsFromListOptions(listOptions metav1.ListOptions) (metav1.ListOptions, bool, error) {
	if !clientfeatures.FeatureGates().Enabled(clientfeatures.WatchListClient) {
		return metav1.ListOptions{}, false, nil
	}

	// 先校验原始 ListOptions：非法组合（例如 RVM 与 continue 冲突）不应被改写成 watch-list。
	internalListOptions := &metainternalversion.ListOptions{}
	if err := scheme.Convert(&listOptions, internalListOptions, nil); err != nil {
		return metav1.ListOptions{}, false, err
	}
	if errs := metainternalversionvalidation.ValidateListOptions(internalListOptions, true); len(errs) > 0 {
		return metav1.ListOptions{}, false, nil
	}

	watchListOptions := listOptions
	// this is our legacy case, the cache ignores LIMIT for
	// ResourceVersion == 0 and RVM=unset|NotOlderThan
	// 这些组合下 List 会忽略 Limit，watch-list 也必须去掉 Limit 才语义一致。
	if listOptions.Limit > 0 && listOptions.ResourceVersion != "0" {
		return metav1.ListOptions{}, false, nil
	}
	watchListOptions.Limit = 0

	// to ensure that we can create a watch-list request that returns
	// semantically equivalent data for the given listOptions,
	// we need to validate that the RVM for the list is supported by watch-list requests.
	// Exact 语义要求锁定到某个 RV，watch-list 无法保证，故不转换。
	if listOptions.ResourceVersionMatch == metav1.ResourceVersionMatchExact {
		return metav1.ListOptions{}, false, nil
	}
	watchListOptions.ResourceVersionMatch = metav1.ResourceVersionMatchNotOlderThan

	watchListOptions.Watch = true
	watchListOptions.AllowWatchBookmarks = true
	watchListOptions.SendInitialEvents = ptr.To(true)

	// 改写后的参数也必须通过同一套校验，防止组合出服务端不接受的请求。
	internalWatchListOptions := &metainternalversion.ListOptions{}
	if err := scheme.Convert(&watchListOptions, internalWatchListOptions, nil); err != nil {
		return metav1.ListOptions{}, false, err
	}
	if errs := metainternalversionvalidation.ValidateListOptions(internalWatchListOptions, true); len(errs) > 0 {
		return metav1.ListOptions{}, false, nil
	}

	return watchListOptions, true, nil
}

// unSupportedWatchListSemantics 是客户端可选实现的标记接口。
type unSupportedWatchListSemantics interface {
	IsWatchListSemanticsUnSupported() bool
}

// DoesClientNotSupportWatchListSemantics reports whether the given client
// does NOT support WatchList semantics.
//
// A client does NOT support WatchList only if
// it implements `IsWatchListSemanticsUnSupported` and that returns true.
//
// For an explanation of how WatchList works, see:
// https://kubernetes.io/docs/reference/using-api/api-concepts/#streaming-lists
//
// 采用“显式禁用”语义：未实现该接口的客户端视为支持，以保持旧客户端行为不变。
func DoesClientNotSupportWatchListSemantics(client any) bool {
	lw, ok := client.(unSupportedWatchListSemantics)
	if !ok {
		return false
	}
	return lw.IsWatchListSemanticsUnSupported()
}
