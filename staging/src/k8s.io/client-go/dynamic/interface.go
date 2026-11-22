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

// Package dynamic provides a client that accesses arbitrary Kubernetes resources
// at runtime using unstructured.Unstructured, without generated typed clients.
//
// Package dynamic 提供动态客户端：以 unstructured.Unstructured 为载体，
// 在运行时按 GroupVersionResource 访问任意资源，适合编译期未知的自定义资源。
// 代价是失去类型安全与字段校验，调用方需自行处理对象结构。
package dynamic

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
)

// Interface 是动态客户端的入口，按 GVR 取得资源访问接口。
type Interface interface {
	Resource(resource schema.GroupVersionResource) NamespaceableResourceInterface
}

// ResourceInterface 暴露单个资源的 CRUD/watch/patch 操作。
//
// subresources 参数为子资源路径（如 "status"、"scale"）；传多个值时
// 由实现按 "/" 拼接，对应 API 路径中的嵌套子资源。
type ResourceInterface interface {
	Create(ctx context.Context, obj *unstructured.Unstructured, options metav1.CreateOptions, subresources ...string) (*unstructured.Unstructured, error)
	Update(ctx context.Context, obj *unstructured.Unstructured, options metav1.UpdateOptions, subresources ...string) (*unstructured.Unstructured, error)
	UpdateStatus(ctx context.Context, obj *unstructured.Unstructured, options metav1.UpdateOptions) (*unstructured.Unstructured, error)
	Delete(ctx context.Context, name string, options metav1.DeleteOptions, subresources ...string) error
	DeleteCollection(ctx context.Context, options metav1.DeleteOptions, listOptions metav1.ListOptions) error
	Get(ctx context.Context, name string, options metav1.GetOptions, subresources ...string) (*unstructured.Unstructured, error)
	List(ctx context.Context, opts metav1.ListOptions) (*unstructured.UnstructuredList, error)
	Watch(ctx context.Context, opts metav1.ListOptions) (watch.Interface, error)
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, options metav1.PatchOptions, subresources ...string) (*unstructured.Unstructured, error)
	Apply(ctx context.Context, name string, obj *unstructured.Unstructured, options metav1.ApplyOptions, subresources ...string) (*unstructured.Unstructured, error)
	ApplyStatus(ctx context.Context, name string, obj *unstructured.Unstructured, options metav1.ApplyOptions) (*unstructured.Unstructured, error)
}

// NamespaceableResourceInterface 在 ResourceInterface 之上增加命名空间选择；
// Namespace("") 表示集群级（非命名空间）资源。
type NamespaceableResourceInterface interface {
	Namespace(string) ResourceInterface
	ResourceInterface
}

// APIPathResolverFunc knows how to convert a groupVersion to its API path. The Kind field is optional.
// TODO find a better place to move this for existing callers
//
// APIPathResolverFunc 依据 GVK 决定 REST 根路径（/api 或 /apis）。
type APIPathResolverFunc func(kind schema.GroupVersionKind) string

// LegacyAPIPathResolverFunc can resolve paths properly with the legacy API.
// TODO find a better place to move this for existing callers
//
// 核心组（Group 为空）走 /api，其余组走 /apis；这是 Kubernetes 历史遗留的路径约定。
func LegacyAPIPathResolverFunc(kind schema.GroupVersionKind) string {
	if len(kind.Group) == 0 {
		return "/api"
	}
	return "/apis"
}
