/*
Copyright 2021 The Kubernetes Authors.

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

package flowcontrol

import (
	"net/http"
	"sync"

	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	"k8s.io/apimachinery/pkg/apis/meta/internalversion/scheme"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"

	"k8s.io/klog/v2"
)

// 只读请求不会触发 watch 扇出，因此不计入成本估算。
var readOnlyVerbs = sets.NewString("get", "list", "watch", "proxy")

// watchIdentifier 标识一组“相似”的 watch。按 APF KEP 的定义，
// resourceType、namespace 与 name 相同即视为相似；selector 反正每次事件都要算，
// 这里直接忽略。
//
// TODO: 目前只统计本 kube-apiserver 上的 watch，跨实例共享留待后续版本。
type watchIdentifier struct {
	apiGroup  string
	resource  string
	namespace string
	name      string
}

// ForgetWatchFunc 是用于从 watch tracker 中注销先前注册的 watch 的函数。
// 必须且只能调用一次，否则计数会永久泄漏。
type ForgetWatchFunc func()

// WatchTracker 跟踪系统中的 watch 数量，用于估算写请求的扇出成本。
type WatchTracker interface {
	// 根据 http.Request 注册一个 watch，返回结束时应调用的注销函数。
	RegisterWatch(r *http.Request) ForgetWatchFunc

	// 返回可能对给定请求感兴趣的 watch 数量，用于估算该请求成本。
	GetInterestedWatchCount(requestInfo *request.RequestInfo) int
}

// builtinIndexes 记录 watch cache 中用于加速 watch 处理的索引：
// 从资源名映射到对象中被建索引的字段路径。
type builtinIndexes map[string]string

// getBuiltinIndexes 返回 watch cache 中已有的索引。
func getBuiltinIndexes() builtinIndexes {
	// 目前只有两个索引：pod 的 spec.nodeName，以及 node/secret/configmap 的 metadata.Name。
	// 后者可忽略：这些对象的 requestInfo.Name 已填充，本身就能精确统计。
	return map[string]string{
		"pods": "spec.nodeName",
	}
}

// watchTracker 统计系统中的 watch 数量，用于估算写请求成本。
type watchTracker struct {
	// 已注册的索引集合，构造后不再变化。
	indexes builtinIndexes

	lock sync.Mutex
	// watchCount 按 watchIdentifier 聚合；删除到 0 时立即移除条目，防止 map 无限增长。
	watchCount map[watchIdentifier]int
}

// NewWatchTracker 构造一个空的 watch 跟踪器。
func NewWatchTracker() WatchTracker {
	return &watchTracker{
		indexes:    getBuiltinIndexes(),
		watchCount: make(map[watchIdentifier]int),
	}
}

const (
	// unsetValue 表示请求未指定（或无法解析）该索引字段的 field selector。
	unsetValue = "<unset>"
)

// getIndexValue 从 list options 中提取索引字段的精确匹配值；
// 解析失败或未指定时返回 unsetValue，调用方据此走保守计数路径。
func getIndexValue(r *http.Request, field string) string {
	opts := metainternalversion.ListOptions{}
	if err := scheme.ParameterCodec.DecodeParameters(r.URL.Query(), metav1.SchemeGroupVersion, &opts); err != nil {
		klog.Warningf("Couldn't parse list options for %v: %v", r.URL.Query(), err)
		return unsetValue
	}
	if opts.FieldSelector == nil {
		return unsetValue
	}
	if value, ok := opts.FieldSelector.RequiresExactMatch(field); ok {
		return value
	}
	return unsetValue
}

// indexValue 记录某个 watch 的索引字段及取值，用于决定它是否应被计入。
type indexValue struct {
	resource string
	value    string
}

// 非 watch 请求返回 nil，调用方需容忍 nil 的 forget 函数。
func (w *watchTracker) RegisterWatch(r *http.Request) ForgetWatchFunc {
	requestInfo, ok := request.RequestInfoFrom(r.Context())
	if !ok || requestInfo == nil || requestInfo.Verb != "watch" {
		return nil
	}

	var index *indexValue
	if indexField, ok := w.indexes[requestInfo.Resource]; ok {
		index = &indexValue{
			resource: requestInfo.Resource,
			value:    getIndexValue(r, indexField),
		}
	}

	identifier := &watchIdentifier{
		apiGroup:  requestInfo.APIGroup,
		resource:  requestInfo.Resource,
		namespace: requestInfo.Namespace,
		name:      requestInfo.Name,
	}

	w.lock.Lock()
	defer w.lock.Unlock()
	w.updateIndexLocked(identifier, index, 1)
	return w.forgetWatch(identifier, index)
}

// updateIndexLocked 按 incr 调整计数；调用方必须持有锁。
func (w *watchTracker) updateIndexLocked(identifier *watchIdentifier, index *indexValue, incr int) {
	if index == nil {
		w.watchCount[*identifier] += incr
	} else {
		// 有索引的资源上，一个事件只会分发给两类 watch：
		// (a) 未对索引字段设置 field selector；
		// (b) field selector 的值恰好等于事件中对象的值。
		//
		// TODO(wojtek-t): 为简化首版实现，这里把所有取值的 (b) 近似为空串情形。
		// 假设真实分发数 (a)+(b) 与近似值 (a)+[(b) 取空串] 相差不大。
		// 这在几乎所有生产集群中都成立，因此作为解锁相关工作的第一步简化是合理的。
		// 未指定索引字段或取值为空的 watch 会被计入所有事件，属于保守近似。
		if index.value == unsetValue || index.value == "" {
			w.watchCount[*identifier] += incr
		}
	}
}

// forgetWatch 返回注销函数；注销后计数归零则删除 map 条目。
func (w *watchTracker) forgetWatch(identifier *watchIdentifier, index *indexValue) ForgetWatchFunc {
	return func() {
		w.lock.Lock()
		defer w.lock.Unlock()

		w.updateIndexLocked(identifier, index, -1)
		if w.watchCount[*identifier] == 0 {
			delete(w.watchCount, *identifier)
		}
	}
}

// GetInterestedWatchCount 实现 WatchTracker 接口。
//
// TODO(wojtek-t): 创建请求（POST）的 requestInfo 目前没有 Name，
// 需研究如何取得以更准确地估算成本。
//
// TODO(wojtek-t): 需确定如何处理 DELETECOLLECTION 调用。
func (w *watchTracker) GetInterestedWatchCount(requestInfo *request.RequestInfo) int {
	// 只读请求不会产生 watch 扇出。
	if requestInfo == nil || readOnlyVerbs.Has(requestInfo.Verb) {
		return 0
	}

	result := 0
	// 需要统计的 watch 包括：
	// - 某资源类型下所有对象的 watch（未指定 namespace 与 name）
	// - 同命名空间下所有对象的 watch（未指定 name）
	// - 关注这个具体对象的 watch
	identifier := &watchIdentifier{
		apiGroup: requestInfo.APIGroup,
		resource: requestInfo.Resource,
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	// 从最宽泛的标识符开始逐级收窄，累加三个层级的 watch 数量。
	result += w.watchCount[*identifier]

	if requestInfo.Namespace != "" {
		identifier.namespace = requestInfo.Namespace
		result += w.watchCount[*identifier]
	}

	if requestInfo.Name != "" {
		identifier.name = requestInfo.Name
		result += w.watchCount[*identifier]
	}

	return result
}
