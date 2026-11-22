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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// TODO: move this, Object, List, and Type to a different package
// ObjectMetaAccessor 由任何能提供 Object 元数据访问器的对象实现。
type ObjectMetaAccessor interface {
	GetObjectMeta() Object
}

// Object 允许以统一方式操作任意版本化或内部 API 对象的元数据。
// 对于不支持某字段的对象（例如列表对象上的 Name、UID、Namespace），
// 设置或读取该字段将是空操作并返回默认值，调用方无需先做类型判断。
type Object interface {
	GetNamespace() string
	SetNamespace(namespace string)
	GetName() string
	SetName(name string)
	GetGenerateName() string
	SetGenerateName(name string)
	GetUID() types.UID
	SetUID(uid types.UID)
	GetResourceVersion() string
	SetResourceVersion(version string)
	GetGeneration() int64
	SetGeneration(generation int64)
	GetSelfLink() string
	SetSelfLink(selfLink string)
	GetCreationTimestamp() Time
	SetCreationTimestamp(timestamp Time)
	GetDeletionTimestamp() *Time
	SetDeletionTimestamp(timestamp *Time)
	GetDeletionGracePeriodSeconds() *int64
	SetDeletionGracePeriodSeconds(*int64)
	GetLabels() map[string]string
	SetLabels(labels map[string]string)
	GetAnnotations() map[string]string
	SetAnnotations(annotations map[string]string)
	GetFinalizers() []string
	SetFinalizers(finalizers []string)
	GetOwnerReferences() []OwnerReference
	SetOwnerReferences([]OwnerReference)
	GetManagedFields() []ManagedFieldsEntry
	SetManagedFields(managedFields []ManagedFieldsEntry)
}

// ListMetaAccessor 用于从对象中取出列表元数据接口。
type ListMetaAccessor interface {
	GetListMeta() ListInterface
}

// Common 允许以统一方式操作任意版本化或内部 API 对象的通用元数据。
// 对不支持某字段的对象，设置或读取该字段是空操作并返回默认值。
// TODO: move this, and TypeMeta and ListMeta, to a different package
type Common interface {
	GetResourceVersion() string
	SetResourceVersion(version string)
	GetSelfLink() string
	SetSelfLink(selfLink string)
}

// ListInterface 允许以统一方式操作任意版本化或内部 API 对象的列表元数据。
// 对不支持某字段的对象，设置或读取该字段是空操作并返回默认值。
// TODO: move this, and TypeMeta and ListMeta, to a different package
type ListInterface interface {
	GetResourceVersion() string
	SetResourceVersion(version string)
	GetSelfLink() string
	SetSelfLink(selfLink string)
	GetContinue() string
	SetContinue(c string)
	GetRemainingItemCount() *int64
	SetRemainingItemCount(c *int64)
}

// ShardedListInterface 可由列表类型实现，以表明该列表只代表完整集合的一个分片，
// 而非完整结果。ShardInfo 描述分片边界，供调用方合并多个分片。
type ShardedListInterface interface {
	GetShardInfo() *ShardInfo
	SetShardInfo(*ShardInfo)
}

// Type 暴露版本化或内部 API 对象的类型与 APIVersion。
// TODO: move this, and TypeMeta and ListMeta, to a different package
type Type interface {
	GetAPIVersion() string
	SetAPIVersion(version string)
	GetKind() string
	SetKind(kind string)
}

// 编译期断言：ListMeta 必须实现 ListInterface。
var _ ListInterface = &ListMeta{}

// ListMeta 对 ListInterface 的实现；各方法直接读写同名字段，因此无需额外校验。
func (meta *ListMeta) GetResourceVersion() string        { return meta.ResourceVersion }
func (meta *ListMeta) SetResourceVersion(version string) { meta.ResourceVersion = version }
func (meta *ListMeta) GetSelfLink() string               { return meta.SelfLink }
func (meta *ListMeta) SetSelfLink(selfLink string)       { meta.SelfLink = selfLink }
func (meta *ListMeta) GetContinue() string               { return meta.Continue }
func (meta *ListMeta) SetContinue(c string)              { meta.Continue = c }
func (meta *ListMeta) GetRemainingItemCount() *int64     { return meta.RemainingItemCount }
func (meta *ListMeta) SetRemainingItemCount(c *int64)    { meta.RemainingItemCount = c }
func (meta *ListMeta) GetShardInfo() *ShardInfo          { return meta.ShardInfo }
func (meta *ListMeta) SetShardInfo(s *ShardInfo)         { meta.ShardInfo = s }

// GetObjectKind 返回自身，使嵌入 TypeMeta 的对象自动满足 ObjectKind 接口。
func (obj *TypeMeta) GetObjectKind() schema.ObjectKind { return obj }

// SetGroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta
// 将 GVK 拆成 APIVersion 与 Kind 两个字段写入，二者是同一信息的序列化形式。
func (obj *TypeMeta) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// GroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta
// 从 APIVersion 与 Kind 字段重建 GVK；字段缺失时返回零值 GVK。
func (obj *TypeMeta) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}

// GetListMeta 返回自身，使 ListMeta 满足 ListMetaAccessor。
func (obj *ListMeta) GetListMeta() ListInterface { return obj }

// GetObjectMeta 返回自身，使 ObjectMeta 满足 ObjectMetaAccessor。
func (obj *ObjectMeta) GetObjectMeta() Object { return obj }

// Namespace implements metav1.Object for any object with an ObjectMeta typed field. Allows
// fast, direct access to metadata fields for API objects.
// 以下方法组是 ObjectMeta 对 Object 接口的直接实现：全部是字段读写，无副作用。
func (meta *ObjectMeta) GetNamespace() string                { return meta.Namespace }
func (meta *ObjectMeta) SetNamespace(namespace string)       { meta.Namespace = namespace }
func (meta *ObjectMeta) GetName() string                     { return meta.Name }
func (meta *ObjectMeta) SetName(name string)                 { meta.Name = name }
func (meta *ObjectMeta) GetGenerateName() string             { return meta.GenerateName }
func (meta *ObjectMeta) SetGenerateName(generateName string) { meta.GenerateName = generateName }
func (meta *ObjectMeta) GetUID() types.UID                   { return meta.UID }
func (meta *ObjectMeta) SetUID(uid types.UID)                { meta.UID = uid }
func (meta *ObjectMeta) GetResourceVersion() string          { return meta.ResourceVersion }
func (meta *ObjectMeta) SetResourceVersion(version string)   { meta.ResourceVersion = version }
func (meta *ObjectMeta) GetGeneration() int64                { return meta.Generation }
func (meta *ObjectMeta) SetGeneration(generation int64)      { meta.Generation = generation }
func (meta *ObjectMeta) GetSelfLink() string                 { return meta.SelfLink }
func (meta *ObjectMeta) SetSelfLink(selfLink string)         { meta.SelfLink = selfLink }
func (meta *ObjectMeta) GetCreationTimestamp() Time          { return meta.CreationTimestamp }
func (meta *ObjectMeta) SetCreationTimestamp(creationTimestamp Time) {
	meta.CreationTimestamp = creationTimestamp
}
func (meta *ObjectMeta) GetDeletionTimestamp() *Time { return meta.DeletionTimestamp }
func (meta *ObjectMeta) SetDeletionTimestamp(deletionTimestamp *Time) {
	meta.DeletionTimestamp = deletionTimestamp
}
func (meta *ObjectMeta) GetDeletionGracePeriodSeconds() *int64 {
	return meta.DeletionGracePeriodSeconds
}
func (meta *ObjectMeta) SetDeletionGracePeriodSeconds(deletionGracePeriodSeconds *int64) {
	meta.DeletionGracePeriodSeconds = deletionGracePeriodSeconds
}
func (meta *ObjectMeta) GetLabels() map[string]string                 { return meta.Labels }
func (meta *ObjectMeta) SetLabels(labels map[string]string)           { meta.Labels = labels }
func (meta *ObjectMeta) GetAnnotations() map[string]string            { return meta.Annotations }
func (meta *ObjectMeta) SetAnnotations(annotations map[string]string) { meta.Annotations = annotations }
func (meta *ObjectMeta) GetFinalizers() []string                      { return meta.Finalizers }
func (meta *ObjectMeta) SetFinalizers(finalizers []string)            { meta.Finalizers = finalizers }
func (meta *ObjectMeta) GetOwnerReferences() []OwnerReference         { return meta.OwnerReferences }
func (meta *ObjectMeta) SetOwnerReferences(references []OwnerReference) {
	meta.OwnerReferences = references
}
func (meta *ObjectMeta) GetManagedFields() []ManagedFieldsEntry { return meta.ManagedFields }
func (meta *ObjectMeta) SetManagedFields(managedFields []ManagedFieldsEntry) {
	meta.ManagedFields = managedFields
}
