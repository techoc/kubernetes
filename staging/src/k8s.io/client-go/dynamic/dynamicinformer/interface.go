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

// Package dynamicinformer 为动态客户端提供共享 informer。
//
// dynamicinformer 包为动态客户端（不依赖生成代码、以 GVR 寻址）提供共享 informer 工厂。
// 对象以 *unstructured.Unstructured 形式缓存，从而无需为每种类型生成 informer。
package dynamicinformer

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/informers"
)

// DynamicSharedInformerFactory 为动态客户端提供共享 informer 与 lister 的访问入口。
//
// DynamicSharedInformerFactory 为动态客户端提供共享 informer 与 lister。
// “共享”指同一 GVR 在工厂内只会创建一份 informer/reflector，多个调用方共享同一缓存，
// 从而避免重复的 List/Watch 请求。工厂本身不做类型特化，因此可处理任意 GVR。
type DynamicSharedInformerFactory interface {
	// Start 启动所有已请求的 informer；它们在独立 goroutine 中运行，直到 stop channel 关闭。
	//
	// Start 启动所有已被 ForResource 请求过的 informer；每个 informer 在独立 goroutine 中
	// 运行，直到 stopCh 关闭。重复调用 Start 是幂等的。
	Start(stopCh <-chan struct{})

	// ForResource 以通用方式返回匹配类型的共享 informer。
	//
	// ForResource 返回指定 GVR 的共享 informer；相同 GVR 多次调用返回同一实例。
	ForResource(gvr schema.GroupVersionResource) informers.GenericInformer

	// WaitForCacheSync 阻塞直到所有已启动 informer 的缓存同步完成，或 stop channel 关闭。
	//
	// WaitForCacheSync 阻塞至所有已启动 informer 的缓存同步完成，或 stopCh 关闭；
	// 返回值为每个 GVR 的同步结果。
	WaitForCacheSync(stopCh <-chan struct{}) map[schema.GroupVersionResource]bool

	// Shutdown 将工厂标记为关闭：此后无法再启动新 informer，Start 直接返回。
	// 此外，Shutdown 会阻塞直到所有 goroutine 退出；为此调用方必须关闭它们启动时
	// 所用的 channel（可以在调用 Shutdown 前或等待期间关闭）。
	// Shutdown 可被多次调用，甚至并发调用；所有调用都会阻塞到 goroutine 全部退出。
	//
	// Shutdown 将工厂标记为关闭：此后不允许再新建 informer，Start 直接返回。
	// 它会阻塞到所有 goroutine 退出，因此启用时传入的 stopCh 必须在 Shutdown 之前
	// 或等待期间关闭。可多次并发调用，均会阻塞至全部 goroutine 退出。
	Shutdown()
}

// TweakListOptionsFunc 定义辅助函数的签名，用于在请求中附加 ListOptions。
//
// TweakListOptionsFunc 允许在发起 List/Watch 前就地修改 ListOptions，
// 例如注入 LabelSelector 或 FieldSelector 以缩小监听范围。
type TweakListOptionsFunc func(*metav1.ListOptions)
