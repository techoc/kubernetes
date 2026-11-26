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

// SharedInformerFactory 为 metadata 客户端提供共享 informer 与 lister 的访问入口。
//
// SharedInformerFactory 为 metadata 客户端提供共享 Informer 与 Lister。
// metadata informer 只拉取对象的元数据（PartialObjectMetadata），适合缓存
// 体积大或结构不可知的对象，从而节省内存与反序列化开销。
type SharedInformerFactory interface {
	// Start 启动所有已请求的 informer；它们在独立 goroutine 中运行，直到 stop channel 关闭。
	//
	// 只启动已经通过 ForResource 请求过的 informer；重复 Start 是幂等的。
	Start(stopCh <-chan struct{})

	// ForResource 以通用方式返回匹配类型的共享 informer。
	//
	// 同一 GVR 会复用同一个共享 informer，调用方不应自行创建多个。
	ForResource(gvr schema.GroupVersionResource) informers.GenericInformer

	// WaitForCacheSync 阻塞至所有已启动 informer 的缓存同步完成，或 stop channel 关闭。
	WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool

	// Shutdown 将工厂标记为关闭：此后无法再启动新 informer，Start 直接返回。
	// 此外，Shutdown 会阻塞直到所有 goroutine 退出；为此调用方必须关闭它们启动时
	// 所用的 channel（可在调用 Shutdown 前或等待期间关闭）。
	// Shutdown 可被多次调用，甚至并发调用；所有调用都会阻塞到 goroutine 全部退出。
	//
	// 注意：Shutdown 不会替你关闭 stopCh，调用方必须先关闭传给 Start 的通道，
	// 否则 Shutdown 会一直阻塞。
	Shutdown()
}

// TweakListOptionsFunc 定义辅助函数的签名，用于在请求中附加 ListOptions。
//
// 该钩子允许在每次 list/watch 时修改 ListOptions，例如注入字段选择器或标签选择器。
type TweakListOptionsFunc func(*metav1.ListOptions)
