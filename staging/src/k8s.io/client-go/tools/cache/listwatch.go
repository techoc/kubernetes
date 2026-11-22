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

// Lister is any object that knows how to perform an initial list.
//
// Ideally, all implementations of Lister should also implement ListerWithContext.
type Lister interface {
	// List should return a list type object; the Items field will be extracted, and the
	// ResourceVersion field will be used to start the watch in the right place.
	//
	// Deprecated: use ListerWithContext.ListWithContext instead.
	//
	// 已弃用：新代码应使用 ListerWithContext，以便传递取消信号与上下文日志。
	List(options metav1.ListOptions) (runtime.Object, error)
}

// ListerWithContext is any object that knows how to perform an initial list.
//
// 返回列表对象的 ResourceVersion 是 Reflector 继续 watch 的起点，
// 因此实现必须保留服务端返回的 RV，不能自行清零。
type ListerWithContext interface {
	// ListWithContext should return a list type object; the Items field will be extracted, and the
	// ResourceVersion field will be used to start the watch in the right place.
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

// Watcher is any object that knows how to start a watch on a resource.
//
// Ideally, all implementations of Watcher should also implement WatcherWithContext.
type Watcher interface {
	// Watch should begin a watch at the specified version.
	//
	// If Watch returns an error, it should handle its own cleanup, including
	// but not limited to calling Stop() on the watch, if one was constructed.
	// This allows the caller to ignore the watch, if the error is non-nil.
	//
	// Deprecated: use WatcherWithContext.WatchWithContext instead.
	//
	// 已弃用：新代码应使用 WatcherWithContext。
	Watch(options metav1.ListOptions) (watch.Interface, error)
}

// WatcherWithContext is any object that knows how to start a watch on a resource.
type WatcherWithContext interface {
	// WatchWithContext should begin a watch at the specified version.
	//
	// If Watch returns an error, it should handle its own cleanup, including
	// but not limited to calling Stop() on the watch, if one was constructed.
	// This allows the caller to ignore the watch, if the error is non-nil.
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

// ListerWatcher is any object that knows how to perform an initial list and start a watch on a resource.
//
// Ideally, all implementations of ListerWatcher should also implement ListerWatcherWithContext.
type ListerWatcher interface {
	Lister
	Watcher
}

// ListerWatcherWithContext is any object that knows how to perform an initial list and start a watch on a resource.
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

	// unsupportedWatchListSemantics indicates whether a client explicitly does NOT support
	// WatchList semantics.
	//
	// Over the years, unit tests in kube have been written in many different ways.
	// After enabling the WatchListClient feature by default, existing tests started failing.
	// To avoid breaking lots of existing client-go users after upgrade,
	// we introduced this field as an opt-in.
	//
	// When true, the reflector disables WatchList even if the feature gate is enabled.
	unsupportedWatchListSemantics bool
}

// IsWatchListSemanticsUnSupported 供 Reflector 判断是否必须退回普通 List+Watch。
func (lw *listWatcherWithWatchListSemanticsWrapper) IsWatchListSemanticsUnSupported() bool {
	return lw.unsupportedWatchListSemantics
}

// ToListWatcherWithWatchListSemantics returns a ListerWatcher
// that knows whether the provided client explicitly
// does NOT support the WatchList semantics. This allows Reflectors
// to adapt their behavior based on client capabilities.
func ToListWatcherWithWatchListSemantics(lw *ListWatch, client any) ListerWatcher {
	return &listWatcherWithWatchListSemanticsWrapper{
		lw,
		watchlist.DoesClientNotSupportWatchListSemantics(client),
	}
}

// ListFunc knows how to list resources
//
// Deprecated: use ListWithContextFunc instead.
//
// 已弃用：新代码应使用 ListWithContextFunc。
type ListFunc func(options metav1.ListOptions) (runtime.Object, error)

// ListWithContextFunc knows how to list resources
type ListWithContextFunc func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error)

// WatchFunc knows how to watch resources
//
// Deprecated: use WatchFuncWithContext instead.
//
// 已弃用：新代码应使用 WatchFuncWithContext。
type WatchFunc func(options metav1.ListOptions) (watch.Interface, error)

// WatchFuncWithContext knows how to watch resources
type WatchFuncWithContext func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error)

// ListWatch knows how to list and watch a set of apiserver resources.
// It satisfies the ListerWatcher and ListerWatcherWithContext interfaces.
// It is a convenience function for users of NewReflector, etc.
// ListFunc or ListWithContextFunc must be set. Same for WatchFunc and WatchFuncWithContext.
// ListWithContextFunc and WatchFuncWithContext are preferred if
// a context is available, otherwise ListFunc and WatchFunc.
//
// Beware of the inconsistent naming of the two WithContext methods.
// This was unintentional, but fixing it now would force the ecosystem
// to go through a breaking Go API change and was deemed not worth it.
//
// NewFilteredListWatchFromClient sets all of the functions to ensure that callers
// which only know about ListFunc and WatchFunc continue to work.
type ListWatch struct {
	// Deprecated: use ListWithContext instead.
	//
	// 已弃用：新代码应使用 ListWithContext。
	ListFunc ListFunc
	// Deprecated: use WatchWithContext instead.
	//
	// 已弃用：新代码应使用 WatchWithContext。
	WatchFunc WatchFunc

	ListWithContextFunc  ListWithContextFunc
	WatchFuncWithContext WatchFuncWithContext

	// DisableChunking requests no chunking for this list watcher.
	// 分块列表可降低单次响应大小，但某些服务端/代理不支持，故提供关闭开关。
	DisableChunking bool
}

var (
	_ ListerWatcher            = &ListWatch{}
	_ ListerWatcherWithContext = &ListWatch{}
)

// Getter interface knows how to access Get method from RESTClient.
type Getter interface {
	Get() *restclient.Request
}

// NewListWatchFromClient creates a new ListWatch from the specified client, resource, namespace and field selector.
// For backward compatibility, all function fields are populated.
//
// fieldSelector 通过字符串写入 ListOptions，与服务端字段选择器语法一致。
func NewListWatchFromClient(c Getter, resource string, namespace string, fieldSelector fields.Selector) *ListWatch {
	optionsModifier := func(options *metav1.ListOptions) {
		options.FieldSelector = fieldSelector.String()
	}
	return NewFilteredListWatchFromClient(c, resource, namespace, optionsModifier)
}

// NewFilteredListWatchFromClient creates a new ListWatch from the specified client, resource, namespace, and option modifier.
// Option modifier is a function takes a ListOptions and modifies the consumed ListOptions. Provide customized modifier function
// to apply modification to ListOptions with a field selector, a label selector, or any other desired options.
// For backward compatibility, all function fields are populated.
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

// List a set of apiserver resources
//
// Deprecated: use ListWatchWithContext.ListWithContext instead.
func (lw *ListWatch) List(options metav1.ListOptions) (runtime.Object, error) {
	// ListWatch is used in Reflector, which already supports pagination.
	// Don't paginate here to avoid duplication.
	if lw.ListFunc != nil {
		return lw.ListFunc(options)
	}
	return lw.ListWithContextFunc(context.Background(), options)
}

// List a set of apiserver resources
func (lw *ListWatch) ListWithContext(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
	// ListWatch is used in Reflector, which already supports pagination.
	// Don't paginate here to avoid duplication.
	// 优先新接口；仅设置旧字段时回退，保证向后兼容。
	if lw.ListWithContextFunc != nil {
		return lw.ListWithContextFunc(ctx, options)
	}
	return lw.ListFunc(options)
}

// Watch a set of apiserver resources
//
// Deprecated: use ListWatchWithContext.WatchWithContext instead.
func (lw *ListWatch) Watch(options metav1.ListOptions) (watch.Interface, error) {
	if lw.WatchFunc != nil {
		return lw.WatchFunc(options)
	}
	return lw.WatchFuncWithContext(context.Background(), options)
}

// Watch a set of apiserver resources
func (lw *ListWatch) WatchWithContext(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
	if lw.WatchFuncWithContext != nil {
		return lw.WatchFuncWithContext(ctx, options)
	}
	return lw.WatchFunc(options)
}
