/*
Copyright 2015 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/util/watchlist"
)

// Lister 是任何知道如何执行初始 list 的对象。
//
// 理想情况下，Lister 的所有实现都应同时实现 ListerWithContext。
type Lister interface {
	// List 应返回一个 list 类型对象；Items 字段会被提取出来，
	// ResourceVersion 字段用于在正确的位置启动 watch。
	//
	// Deprecated: 请改用 ListerWithContext.ListWithContext，以便传递取消信号与上下文日志。
	List(options metav1.ListOptions) (runtime.Object, error)
}

// ListerWithContext 是任何知道如何执行初始 list 的对象。
//
// 返回列表对象的 ResourceVersion 是 Reflector 继续 watch 的起点，
// 因此实现必须保留服务端返回的 RV，不能自行清零。
type ListerWithContext interface {
	// ListWithContext 应返回一个 list 类型对象；Items 字段会被提取出来，
	// ResourceVersion 字段用于在正确的位置启动 watch。
	ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error)
}

// ToListerWithContext 把仅实现旧接口的 Lister 适配为 ListerWithContext；
// 若已实现新接口则直接返回，避免不必要的包装。
func ToListerWithContext(l Lister) ListerWithContext {
	if l, ok := l.(ListerWithContext); ok {
		return l
	}
	return listerWrapper{
		parent: l,
	}
}

// listerWrapper 用后台 context 调用旧的 List，属于兼容垫片。
type listerWrapper struct {
	parent Lister
}

func (l listerWrapper) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	return l.parent.List(options)
}

// Watcher 是任何知道如何在资源上启动 watch 的对象。
//
// 理想情况下，Watcher 的所有实现都应同时实现 WatcherWithContext。
type Watcher interface {
	// Watch 应从指定版本开始建立 watch。
	//
	// 如果 Watch 返回错误，它应自行完成清理，包括但不限于对已构造的
	// watch 调用 Stop()。这样当错误非空时，调用方可以完全忽略 watch。
	//
	// Deprecated: 请改用 WatcherWithContext.WatchWithContext。
	Watch(options metav1.ListOptions) (watch.Interface, error)
}

// WatcherWithContext 是任何知道如何在资源上启动 watch 的对象。
type WatcherWithContext interface {
	// WatchWithContext 应从指定版本开始建立 watch。
	//
	// 如果 WatchWithContext 返回错误，它应自行完成清理，包括但不限于对已构造的
	// watch 调用 Stop()。这样当错误非空时，调用方可以完全忽略 watch。
	WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error)
}

// ToWatcherWithContext 与 ToListerWithContext 同理，为旧实现提供适配。
func ToWatcherWithContext(w Watcher) WatcherWithContext {
	if w, ok := w.(WatcherWithContext); ok {
		return w
	}
	return watcherWrapper{
		parent: w,
	}
}

// watcherWrapper 用后台 context 调用旧 Watch。
type watcherWrapper struct {
	parent Watcher
}

func (l watcherWrapper) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	return l.parent.Watch(options)
}

// ListerWatcher 是任何知道如何执行初始 list 并在资源上启动 watch 的对象。
//
// 理想情况下，ListerWatcher 的所有实现都应同时实现 ListerWatcherWithContext。
type ListerWatcher interface {
	Lister
	Watcher
}

// ListerWatcherWithContext 是任何知道如何执行初始 list 并在资源上启动 watch 的对象。
type ListerWatcherWithContext interface {
	ListerWithContext
	WatcherWithContext
}

// ToListerWatcherWithContext 组合两个适配器；即使 lw 只实现旧接口也能得到新接口。
func ToListerWatcherWithContext(lw ListerWatcher) ListerWatcherWithContext {
	if lw, ok := lw.(ListerWatcherWithContext); ok {
		return lw
	}
	return listerWatcherWrapper{
		ListerWithContext:  ToListerWithContext(lw),
		WatcherWithContext: ToWatcherWithContext(lw),
	}
}

// listerWatcherWrapper 嵌入两个适配器以满足 ListerWatcherWithContext。
type listerWatcherWrapper struct {
	ListerWithContext
	WatcherWithContext
}
type listWatcherWithWatchListSemanticsWrapper struct {
	*ListWatch

	// unsupportedWatchListSemantics 表示该客户端显式“不”支持 WatchList 语义。
	//
	// 多年来 kube 的单元测试写法各不相同。默认启用 WatchListClient 特性后，
	// 已有测试开始失败。为了避免升级后破坏大量现有 client-go 用户，
	// 我们把该字段设计为可选项。
	//
	// 为 true 时，即使 feature gate 已开启，reflector 也会禁用 WatchList。
	unsupportedWatchListSemantics bool
}

// IsWatchListSemanticsUnSupported 供 Reflector 判断是否必须退回普通 List+Watch。
func (lw *listWatcherWithWatchListSemanticsWrapper) IsWatchListSemanticsUnSupported() bool {
	return lw.unsupportedWatchListSemantics
}

// ToListWatcherWithWatchListSemantics 返回一个 ListerWatcher，它能感知所给客户端
// 是否显式不支持 WatchList 语义，从而让 Reflector 依据客户端能力调整行为。
func ToListWatcherWithWatchListSemantics(lw *ListWatch, client any) ListerWatcher {
	return &listWatcherWithWatchListSemanticsWrapper{
		lw,
		watchlist.DoesClientNotSupportWatchListSemantics(client),
	}
}

// ListFunc 表示知道如何 list 资源。
//
// Deprecated: 请改用 ListWithContextFunc。
type ListFunc func(options metav1.ListOptions) (runtime.Object, error)

// ListWithContextFunc 表示知道如何 list 资源。
type ListWithContextFunc func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error)

// WatchFunc 表示知道如何 watch 资源。
//
// Deprecated: 请改用 WatchFuncWithContext。
type WatchFunc func(options metav1.ListOptions) (watch.Interface, error)

// WatchFuncWithContext 表示知道如何 watch 资源。
type WatchFuncWithContext func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error)

// ListWatch 知道如何 list 和 watch 一组 apiserver 资源，同时满足
// ListerWatcher 与 ListerWatcherWithContext 接口，是 NewReflector 等
// 使用方的便捷封装。ListFunc 与 ListWithContextFunc 必须至少设置其一，
// WatchFunc 与 WatchFuncWithContext 同理；若手头有 context，
// 优先使用 ListWithContextFunc 和 WatchFuncWithContext。
//
// 注意两个 WithContext 方法的命名并不一致。这属于无心之失，但修正它
// 会迫使整个生态经历一次破坏性的 Go API 变更，被认为得不偿失。
//
// NewFilteredListWatchFromClient 会同时设置所有函数字段，确保只认识
// ListFunc 和 WatchFunc 的调用方继续可用。
type ListWatch struct {
	// Deprecated: 请改用 ListWithContextFunc。
	ListFunc ListFunc
	// Deprecated: 请改用 WatchFuncWithContext。
	WatchFunc WatchFunc

	ListWithContextFunc  ListWithContextFunc
	WatchFuncWithContext WatchFuncWithContext

	// DisableChunking 请求该 list watcher 不做分块。
	// 分块列表可降低单次响应大小，但某些服务端/代理不支持，故提供关闭开关。
	DisableChunking bool
}

var (
	_ ListerWatcher            = &ListWatch{}
	_ ListerWatcherWithContext = &ListWatch{}
)

// Getter 是知道如何访问 RESTClient 的 Get 方法的接口。
type Getter interface {
	Get() *restclient.Request
}

// NewListWatchFromClient 基于指定的 client、resource、namespace 和字段选择器
// 创建一个新的 ListWatch。为向后兼容，所有函数字段都会被填充。
//
// fieldSelector 通过字符串写入 ListOptions，与服务端字段选择器语法一致。
func NewListWatchFromClient(c Getter, resource string, namespace string, fieldSelector fields.Selector) *ListWatch {
	optionsModifier := func(options *metav1.ListOptions) {
		options.FieldSelector = fieldSelector.String()
	}
	return NewFilteredListWatchFromClient(c, resource, namespace, optionsModifier)
}

// NewFilteredListWatchFromClient 基于指定的 client、resource、namespace 和选项修改函数
// 创建一个新的 ListWatch。选项修改函数接收 ListOptions 并修改最终使用的值，
// 可传入自定义 modifier 来设置字段选择器、标签选择器或其他所需选项。
// 为向后兼容，所有函数字段都会被填充。
//
// 同时填充旧/新两组函数字段，使只认识 ListFunc/WatchFunc 的调用方无需修改。
func NewFilteredListWatchFromClient(c Getter, resource string, namespace string, optionsModifier func(options *metav1.ListOptions)) *ListWatch {
	listFunc := func(options metav1.ListOptions) (runtime.Object, error) {
		optionsModifier(&options)
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, metav1.ParameterCodec).
			Do(context.Background()).
			Get()
	}
	watchFunc := func(options metav1.ListOptions) (watch.Interface, error) {
		options.Watch = true
		optionsModifier(&options)
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, metav1.ParameterCodec).
			Watch(context.Background())
	}
	listFuncWithContext := func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
		optionsModifier(&options)
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, metav1.ParameterCodec).
			Do(ctx).
			Get()
	}
	watchFuncWithContext := func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
		options.Watch = true
		optionsModifier(&options)
		return c.Get().
			Namespace(namespace).
			Resource(resource).
			VersionedParams(&options, metav1.ParameterCodec).
			Watch(ctx)
	}
	return &ListWatch{
		ListFunc:             listFunc,
		WatchFunc:            watchFunc,
		ListWithContextFunc:  listFuncWithContext,
		WatchFuncWithContext: watchFuncWithContext,
	}
}

// List 列举一组 apiserver 资源。
//
// Deprecated: 请改用 ListWithContext。
func (lw *ListWatch) List(options metav1.ListOptions) (runtime.Object, error) {
	// ListWatch 用在 Reflector 中，后者本身已支持分页。
	// 这里不再分页，避免重复。
	if lw.ListFunc != nil {
		return lw.ListFunc(options)
	}
	return lw.ListWithContextFunc(context.Background(), options)
}

// ListWithContext 列举一组 apiserver 资源。
func (lw *ListWatch) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	// ListWatch 用在 Reflector 中，后者本身已支持分页。
	// 这里不再分页，避免重复。
	// 优先新接口；仅设置旧字段时回退，保证向后兼容。
	if lw.ListWithContextFunc != nil {
		return lw.ListWithContextFunc(ctx, options)
	}
	return lw.ListFunc(options)
}

// Watch 监听一组 apiserver 资源。
//
// Deprecated: 请改用 WatchWithContext。
func (lw *ListWatch) Watch(options metav1.ListOptions) (watch.Interface, error) {
	if lw.WatchFunc != nil {
		return lw.WatchFunc(options)
	}
	return lw.WatchFuncWithContext(context.Background(), options)
}

// WatchWithContext 监听一组 apiserver 资源。
func (lw *ListWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	if lw.WatchFuncWithContext != nil {
		return lw.WatchFuncWithContext(ctx, options)
	}
	return lw.WatchFunc(options)
}
