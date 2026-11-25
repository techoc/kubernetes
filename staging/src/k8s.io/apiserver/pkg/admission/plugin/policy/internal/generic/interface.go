/*
Copyright 2022 The Kubernetes Authors.

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

package generic

import (
	"context"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
)

// Controller 是“带 informer 的后台同步器”的最小契约。策略类插件用它
// 统一 Run 生命周期与同步状态判断。
type Controller[T runtime.Object] interface {
	// Run 应在 goroutine 内运行：等待并响应控制器所关注类型的变更。
	//
	// 返回的错误总是非 nil，用于说明 worker 停止的原因。
	//
	// 约定：返回 nil 会让上层误以为“正常但意外结束”，因此正常停止也应
	// 通过错误说明原因。
	Run(ctx context.Context) error

	// Informer 返回支撑本控制器所用的 informer。
	//
	// 暴露 informer 是为了让调用方（如 dispatcher）复用同一份缓存，
	// 避免重复 list/watch。
	Informer() Informer[T]

	// HasSynced 在 informer 缓存已同步、且初始列表中的对象都至少完成过一次
	// reconciliation 时返回 true。
	//
	// 注意它比 informer 的 HasSynced 更强：还要求初始对象至少被处理过一次。
	HasSynced() bool
}

// NamespacedLister 是单命名空间视角的只读列表接口。
type NamespacedLister[T any] interface {
	// List 返回 indexer 中给定命名空间下的全部 ValidationRuleSet。
	// 这里返回的对象必须按只读处理。
	//
	// 返回的对象与 informer 缓存共享，修改会导致缓存污染和并发数据竞争。
	List(selector labels.Selector) (ret []T, err error)
	// Get 按给定命名空间与名称从 indexer 中查找 ValidationRuleSet。
	// 这里返回的对象必须按只读处理。
	Get(name string) (T, error)
}

// Informer 把 SharedIndexInformer 与泛型 Lister 合二为一，使调用方无需
// 在 int 类型断言和泛型之间来回转换。
type Informer[T any] interface {
	cache.SharedIndexInformer
	Lister[T]
}

// Lister[T] 提供对 T 的列表能力。
// 这里返回的所有对象必须按只读处理。
type Lister[T any] interface {
	NamespacedLister[T]
	// Namespaced 返回限定命名空间的视图；空字符串语义由实现决定，
	// 通常表示全部命名空间。
	Namespaced(namespace string) NamespacedLister[T]
}
