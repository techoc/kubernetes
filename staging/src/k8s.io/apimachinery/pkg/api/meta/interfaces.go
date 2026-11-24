/*
Copyright 2014 The Kubernetes Authors.

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

package meta

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// ListMetaAccessor 表示能够暴露 ListMeta 的对象，通常是 `*List` 类型。
type ListMetaAccessor interface {
	GetListMeta() List
}

// List 让调用方可以统一读写任意外部版本或内部 API 对象的列表元数据。
// 对不支持某字段的对象执行读取或写入是 no-op，并返回默认值。
//
// 这里用类型别名的方式复用 metav1.ListInterface，避免两处接口定义漂移。
type List metav1.ListInterface

// Type 暴露外部版本或内部 API 对象的 Kind 与 APIVersion。
type Type metav1.Type

// MetadataAccessor 让你可以从任意 versioned 或 internal API 对象上操作对象
// 与列表元数据。对不支持某字段的对象（例如在 List 上操作 Name、UID、
// Namespace）执行读取或写入是 no-op，并返回默认值。
//
// MetadataAccessor 以“可作用于多个对象”的方式暴露 Interface；它按
// runtime.Object 取参而不是依赖具体类型断言，因此调用方可以用同一套
// 代码处理不同版本的资源。resourceVersion 相关方法通过内嵌
// runtime.ResourceVersioner 获得。
type MetadataAccessor interface {
	APIVersion(obj runtime.Object) (string, error)
	SetAPIVersion(obj runtime.Object, version string) error

	Kind(obj runtime.Object) (string, error)
	SetKind(obj runtime.Object, kind string) error

	Namespace(obj runtime.Object) (string, error)
	SetNamespace(obj runtime.Object, namespace string) error

	Name(obj runtime.Object) (string, error)
	SetName(obj runtime.Object, name string) error

	GenerateName(obj runtime.Object) (string, error)
	SetGenerateName(obj runtime.Object, name string) error

	UID(obj runtime.Object) (types.UID, error)
	SetUID(obj runtime.Object, uid types.UID) error

	SelfLink(obj runtime.Object) (string, error)
	SetSelfLink(obj runtime.Object, selfLink string) error

	Labels(obj runtime.Object) (map[string]string, error)
	SetLabels(obj runtime.Object, labels map[string]string) error

	Annotations(obj runtime.Object) (map[string]string, error)
	SetAnnotations(obj runtime.Object, annotations map[string]string) error

	Continue(obj runtime.Object) (string, error)
	SetContinue(obj runtime.Object, c string) error

	runtime.ResourceVersioner
}

// RESTScopeName 是 REST 资源作用域的名称；字符串值参与 API 路径构造，
// 不应随意改动。
type RESTScopeName string

const (
	// RESTScopeNameNamespace 表示命名空间内资源。
	RESTScopeNameNamespace RESTScopeName = "namespace"
	// RESTScopeNameRoot 表示集群级（非命名空间）资源。
	RESTScopeNameRoot RESTScopeName = "root"
)

// RESTScope 描述处理层级化 REST 资源所需的信息。
type RESTScope interface {
	// Name 返回作用域名称。
	Name() RESTScopeName
}

// RESTMapping 描述如何以 RESTful 方式处理某个特定资源与 kind 的对象。
type RESTMapping struct {
	// Resource 是该 endpoint 的 GroupVersionResource（即位置）。
	Resource schema.GroupVersionResource

	// GroupVersionKind 是提交给该 endpoint 的数据格式（GVK）。
	GroupVersionKind schema.GroupVersionKind

	// Scope 描述处理层级化 REST 资源所需的信息。
	Scope RESTScope
}

// RESTMapper 允许客户端在资源与 kind 之间做映射，并把 kind/version 映射到
// 操作对象的接口。它主要面向 kubernetes 兼容 REST API 的使用者，规范见
// docs/devel/api-conventions.md。
//
// Kubernetes API 按 API group 组织不同版本的资源与对象 Kind；同名 Kind 或资源可能存在于不同 group 中，
// 因此不能假设 Kind 和 Resource 在整个集群范围内唯一。
//
// RESTMapperWithContext 是更好的替代方案，因为它支持上下文日志与取消。
//
// 为支持上下文日志和取消，新代码应优先使用 RESTMapperWithContext。
type RESTMapper interface {
	// KindFor 接收一个部分限定的资源并返回唯一匹配；若存在多个匹配则报错。
	KindFor(resource schema.GroupVersionResource) (schema.GroupVersionKind, error)

	// KindsFor 接收一个部分限定的资源并按优先级返回所有可能匹配的 kind。
	KindsFor(resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error)

	// ResourceFor 接收一个部分限定的资源并返回唯一匹配；若存在多个匹配则报错。
	ResourceFor(input schema.GroupVersionResource) (schema.GroupVersionResource, error)

	// ResourcesFor 接收一个部分限定的资源并按优先级返回所有可能的资源。
	ResourcesFor(input schema.GroupVersionResource) ([]schema.GroupVersionResource, error)

	// RESTMapping 为给定的 group kind 返回首选的资源映射。
	RESTMapping(gk schema.GroupKind, versions ...string) (*RESTMapping, error)
	// RESTMappings 在未指定版本时返回该 group kind 的所有资源映射；否则
	// 在给定版本中识别首选映射。
	RESTMappings(gk schema.GroupKind, versions ...string) ([]*RESTMapping, error)

	// ResourceSingularizer 返回资源名对应的单数形式。
	ResourceSingularizer(resource string) (singular string, err error)
}

// RESTMapperWithContext 是 RESTMapper 的上下文感知版本，语义与 RESTMapper
// 完全一致，但每个方法都接受 context 以支持上下文日志、取消与超时。
//
// Kubernetes API 按 API group 组织不同版本的资源与对象 Kind；同名 Kind 或资源可能存在于不同 group 中，
// 因此不能假设 Kind 和 Resource 在整个集群范围内唯一。
type RESTMapperWithContext interface {
	// KindForWithContext 接收一个部分限定的资源并返回唯一匹配；若存在多个匹配则报错。
	KindForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionKind, error)

	// KindsForWithContext 接收一个部分限定的资源并按优先级返回所有可能匹配的 kind。
	KindsForWithContext(ctx context.Context, resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error)

	// ResourceForWithContext 接收一个部分限定的资源并返回唯一匹配；若存在多个匹配则报错。
	ResourceForWithContext(ctx context.Context, input schema.GroupVersionResource) (schema.GroupVersionResource, error)

	// ResourcesForWithContext 接收一个部分限定的资源并按优先级返回所有可能的资源。
	ResourcesForWithContext(ctx context.Context, input schema.GroupVersionResource) ([]schema.GroupVersionResource, error)

	// RESTMappingWithContext 为给定的 group kind 返回首选的资源映射。
	RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (*RESTMapping, error)
	// RESTMappingsWithContext 在未指定版本时返回该 group kind 的所有资源映射；
	// 否则在给定版本中识别首选映射。
	RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) ([]*RESTMapping, error)

	// ResourceSingularizerWithContext 返回资源名对应的单数形式。
	ResourceSingularizerWithContext(ctx context.Context, resource string) (singular string, err error)
}

// ToRESTMapperWithContext 把 RESTMapper 适配为 RESTMapperWithContext。
// 若 m 已实现上下文版本则直接返回（避免无意义的包装）；nil 透传为 nil，
// 这样上层对 nil 的判断无需改动。
func ToRESTMapperWithContext(m RESTMapper) RESTMapperWithContext {
	if m == nil {
		return nil
	}
	if m, ok := m.(RESTMapperWithContext); ok {
		return m
	}
	return &restMapperWrapper{
		delegate: m,
	}
}

// restMapperWrapper 为没有上下文方法的旧实现补上 ctx 参数；ctx 被丢弃，
// 因此它只适用于迁移期，无法真正获得取消能力。
type restMapperWrapper struct {
	delegate RESTMapper
}

func (m *restMapperWrapper) KindForWithContext(ctx context.Context, resource schema.GroupVersionResource) (schema.GroupVersionKind, error) {
	return m.delegate.KindFor(resource)
}
func (m *restMapperWrapper) KindsForWithContext(ctx context.Context, resource schema.GroupVersionResource) ([]schema.GroupVersionKind, error) {
	return m.delegate.KindsFor(resource)
}
func (m *restMapperWrapper) ResourceForWithContext(ctx context.Context, input schema.GroupVersionResource) (schema.GroupVersionResource, error) {
	return m.delegate.ResourceFor(input)
}
func (m *restMapperWrapper) ResourcesForWithContext(ctx context.Context, input schema.GroupVersionResource) ([]schema.GroupVersionResource, error) {
	return m.delegate.ResourcesFor(input)
}
func (m *restMapperWrapper) RESTMappingWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) (*RESTMapping, error) {
	return m.delegate.RESTMapping(gk, versions...)
}
func (m *restMapperWrapper) RESTMappingsWithContext(ctx context.Context, gk schema.GroupKind, versions ...string) ([]*RESTMapping, error) {
	return m.delegate.RESTMappings(gk, versions...)
}
func (m *restMapperWrapper) ResourceSingularizerWithContext(ctx context.Context, resource string) (singular string, err error) {
	return m.delegate.ResourceSingularizer(resource)
}

// ResettableRESTMapper 是可以从 discovery 信息重置自身的 RESTMapper。
// 所有委托给其他 mapper 的实现都必须实现该接口，并动态检查 delegate 是否
// 支持 Reset()，否则重置请求会静默丢失。
//
// ResettableRESTMapperWithContext 是更好的替代方案，因为它支持上下文日志与取消。
//
// 为支持上下文日志和取消，新代码应优先使用 ResettableRESTMapperWithContext。
type ResettableRESTMapper interface {
	RESTMapper
	Reset()
}

// ResettableRESTMapperWithContext 是上下文感知的可重置 RESTMapper。
// 所有委托给其他 mapper 的实现都必须实现该接口，并动态检查 delegate 是否
// 支持 ResetWithContext()。
type ResettableRESTMapperWithContext interface {
	RESTMapperWithContext
	ResetWithContext(ctx context.Context)
}

// ToResettableRESTMapperWithContext 把 ResettableRESTMapper 适配为
// ResettableRESTMapperWithContext；若已实现则直接复用，nil 透传为 nil。
func ToResettableRESTMapperWithContext(m ResettableRESTMapper) ResettableRESTMapperWithContext {
	if m == nil {
		return nil
	}
	if m, ok := m.(ResettableRESTMapperWithContext); ok {
		return m
	}
	return &resettableRESTMapperWrapper{
		RESTMapperWithContext: ToRESTMapperWithContext(m),
		delegate:              m,
	}
}

// resettableRESTMapperWrapper 复用非重置版的适配结果，只补上 ResetWithContext。
type resettableRESTMapperWrapper struct {
	RESTMapperWithContext
	delegate ResettableRESTMapper
}

func (m *resettableRESTMapperWrapper) ResetWithContext(ctx context.Context) {
	m.delegate.Reset()
}
