/*
Copyright 2016 The Kubernetes Authors.

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

// Package metadata 提供只获取对象元数据的客户端。
// (in the form of PartialObjectMetadata) from any Kubernetes-compatible API.
//
// metadata 包提供只获取对象元数据（以 PartialObjectMetadata 形式）的客户端。
// 它通过 Accept: application/vnd.kubernetes.protobuf;as=PartialObjectMetadata
// 之类的请求头让服务端裁剪响应，从而降低带宽与解码成本。
package metadata

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// Interface 允许调用方从任意兼容 Kubernetes 的资源 API 获取元数据
// （以 PartialObjectMetadata 对象的形式）。
//
// Interface 允许调用方以 PartialObjectMetadata 的形式获取任意兼容 Kubernetes 的资源元数据。
// 相比完整对象，元数据请求可显著降低传输量与解码开销，适合事件分发、
// 标签筛选、按名字查询等只关心 metadata 的场景。
type Interface interface {
	// Resource 返回指定 GVR 对应的 Getter。
	Resource(resource schema.GroupVersionResource) Getter
}

// ResourceInterface 包含按元数据操作对象的方法。服务端不支持 Update，但可用
// Patch 完成 Update 想做的事项。
//
// ResourceInterface 列出可对对象元数据执行的操作。
// 服务端不支持 Update（那需要完整对象），但可用 Patch 达到与 Update 相同的效果；
// 这些方法均支持可变参数 subresources，用于访问 status 等子资源。
type ResourceInterface interface {
	Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error
	DeleteCollection(ctx context.Context, options metav1.DeleteOptions, listOptions metav1.ListOptions) error
	Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*metav1.PartialObjectMetadata, error)
	List(ctx context.Context, opts metav1.ListOptions) (*metav1.PartialObjectMetadataList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*metav1.PartialObjectMetadata, error)
}

// Getter 以一致的方式处理命名空间作用域与非命名空间作用域资源。
//
// Getter 同时覆盖命名空间作用域和集群作用域资源：
// 直接调用 Getter 的方法作用于集群作用域（或默认范围），
// Namespace(n) 则返回绑定到命名空间 n 的操作句柄。
type Getter interface {
	Namespace(string) ResourceInterface
	ResourceInterface
}
