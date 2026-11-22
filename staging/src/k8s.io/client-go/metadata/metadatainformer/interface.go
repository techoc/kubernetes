/*
Copyright 2018 The Kubernetes Authors.

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

package metadatainformer

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
)

// SharedInformerFactory provides access to a shared informer and lister for dynamic client
//
// SharedInformerFactory 为 metadata 客户端提供共享 Informer 与 Lister。
// metadata informer 只拉取对象的元数据（PartialObjectMetadata），适合缓存
// 体积大或结构不可知的对象，从而节省内存与反序列化开销。
type SharedInformerFactory interface {
	// Start initializes all requested informers. They are handled in goroutines
	// which run until the stop channel gets closed.
	//
	// 只启动已经通过 ForResource 请求过的 informer；重复 Start 是幂等的。
	Start(stopCh <-chan struct{})

	// ForResource gives generic access to a shared informer of the matching type.
	//
	// 同一 GVR 会复用同一个共享 informer，调用方不应自行创建多个。
	ForResource(gvr schema.GroupVersionResource) informers.GenericInformer

	// WaitForCacheSync blocks until all started informers' caches were synced
	// or the stop channel gets closed.
	WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool

	// Shutdown marks a factory as shutting down. At that point no new
	// informers can be started anymore and Start will return without
	// doing anything.
	//
	// In addition, Shutdown blocks until all goroutines have terminated. For that
	// to happen, the close channel(s) that they were started with must be closed,
	// either before Shutdown gets called or while it is waiting.
	//
	// Shutdown may be called multiple times, even concurrently. All such calls will
	// block until all goroutines have terminated.
	//
	// 注意：Shutdown 不会替你关闭 stopCh，调用方必须先关闭传给 Start 的通道，
	// 否则 Shutdown 会一直阻塞。
	Shutdown()
}

// TweakListOptionsFunc defines the signature of a helper function
// that wants to provide more listing options to API
//
// 该钩子允许在每次 list/watch 时修改 ListOptions，例如注入字段选择器或标签选择器。
type TweakListOptionsFunc func(*metav1.ListOptions)
