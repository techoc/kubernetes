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

package unstructured

import (
	"bytes"
	"errors"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Unstructured 允许通用操作未注册 Go 结构体的对象，可用于处理插件的 API 对象。
// 它仍提供有效的 TypeMeta 功能，例如 kind、version 等。
//
// 警告：该对象提供 v1 标准元数据访问器。处理不属于服务端 meta v1 schema 的对象时，
// 不得使用此类型。
//
// TODO: 将此类型的序列化部分与字段访问器分离。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen=true
type Unstructured struct {
	// Object 是 JSON 兼容的 map，可包含字符串、浮点数、整数、布尔值、
	// []interface{} 或 map[string]interface{} 子节点。
	Object map[string]interface{}
}

var _ metav1.Object = &Unstructured{}
var _ runtime.Unstructured = &Unstructured{}
var _ metav1.ListInterface = &Unstructured{}

func (obj *Unstructured) GetObjectKind() schema.ObjectKind { return obj }

// IsList 报告对象是否携带 "items" 数组，这是区分列表与普通对象的唯一结构信号。
func (obj *Unstructured) IsList() bool {
	field, ok := obj.Object["items"]
	if !ok {
		return false
	}
	_, ok = field.([]interface{})
	return ok
}

// ToList 将对象转换为 UnstructuredList。若不是列表，则包装为 Object 使用原 map、
// Items 为空的列表，以保留原元数据而不返回错误。
func (obj *Unstructured) ToList() (*UnstructuredList, error) {
	if !obj.IsList() {
		// 返回空列表。
		return &UnstructuredList{Object: obj.Object}, nil
	}

	// 列表视图与对象共享底层 map；修改其中一方会反映到另一方。
	ret := &UnstructuredList{}
	ret.Object = obj.Object

	err := obj.EachListItem(func(item runtime.Object) error {
		castItem := item.(*Unstructured)
		ret.Items = append(ret.Items, *castItem)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return ret, nil
}

// EachListItem 对 "items" 数组中的每个元素调用 fn。元素必须是 JSON 对象；
// 接口刻意宽容处理畸形数据，以返回描述性错误而非 panic。
func (obj *Unstructured) EachListItem(fn func(runtime.Object) error) error {
	field, ok := obj.Object["items"]
	if !ok {
		return errors.New("content is not a list")
	}
	items, ok := field.([]interface{})
	if !ok {
		return fmt.Errorf("content is not a list: %T", field)
	}
	for _, item := range items {
		child, ok := item.(map[string]interface{})
		if !ok {
			return fmt.Errorf("items member is not an object: %T", child)
		}
		if err := fn(&Unstructured{Object: child}); err != nil {
			return err
		}
	}
	return nil
}

// EachListItemWithAlloc 是 EachListItem 的分配变体。后者已为每项分配
// Unstructured，因此前者直接委托即可。
func (obj *Unstructured) EachListItemWithAlloc(fn func(runtime.Object) error) error {
	// EachListItem 已为用户分配新 Object，可直接使用。
	return obj.EachListItem(fn)
}

// UnstructuredContent 返回底层 map。nil 对象会规范化为空 map，调用方可直接修改结果。
func (obj *Unstructured) UnstructuredContent() map[string]interface{} {
	if obj.Object == nil {
		return make(map[string]interface{})
	}
	return obj.Object
}

// SetUnstructuredContent 替换整个载荷。该 map 直接采用而不复制，调用方之后不得修改它。
func (obj *Unstructured) SetUnstructuredContent(content map[string]interface{}) {
	obj.Object = content
}

// MarshalJSON 确保非结构化对象传给 Go 标准 JSON 库时生成正确的 JSON。
func (u *Unstructured) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	err := UnstructuredJSONScheme.Encode(u, &buf)
	return buf.Bytes(), err
}

// UnmarshalJSON 确保非结构化对象正确解码 Go 标准 JSON 库传入的 JSON。
func (u *Unstructured) UnmarshalJSON(b []byte) error {
	_, _, err := UnstructuredJSONScheme.Decode(b, nil, u)
	return err
}

// NewEmptyInstance 返回仅含 kind/apiVersion、不含其他数据的新实例。
// 非结构化类型应调用此方法而非 reflect.New()，因为 Go 类型本身不保留 kind/apiVersion。
func (in *Unstructured) NewEmptyInstance() runtime.Unstructured {
	out := new(Unstructured)
	if in != nil {
		out.GetObjectKind().SetGroupVersionKind(in.GetObjectKind().GroupVersionKind())
	}
	return out
}

// DeepCopy 返回包括所有嵌套 map 和切片在内的完全独立副本。
// 载荷不是有类型的结构体，因此此处不能使用生成的 DeepCopyInto。
func (in *Unstructured) DeepCopy() *Unstructured {
	if in == nil {
		return nil
	}
	out := new(Unstructured)
	*out = *in
	out.Object = runtime.DeepCopyJSON(in.Object)
	return out
}

// setNestedField 延迟创建对象 map，使访问器无需单独初始化即可用于零值 Unstructured。
func (u *Unstructured) setNestedField(value interface{}, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	SetNestedField(u.Object, value, fields...)
}

func (u *Unstructured) setNestedStringSlice(value []string, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	SetNestedStringSlice(u.Object, value, fields...)
}

func (u *Unstructured) setNestedSlice(value []interface{}, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	SetNestedSlice(u.Object, value, fields...)
}

func (u *Unstructured) setNestedMap(value map[string]string, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	SetNestedStringMap(u.Object, value, fields...)
}

func (u *Unstructured) GetOwnerReferences() []metav1.OwnerReference {
	field, found, err := NestedFieldNoCopy(u.Object, "metadata", "ownerReferences")
	if !found || err != nil {
		return nil
	}
	// 畸形数据返回 nil 而非错误；元数据访问器刻意宽容，以便由校验逻辑报告问题。
	original, ok := field.([]interface{})
	if !ok {
		return nil
	}
	ret := make([]metav1.OwnerReference, 0, len(original))
	for _, obj := range original {
		o, ok := obj.(map[string]interface{})
		if !ok {
			// 预期为 map[string]interface{}，实际为其他类型。
			return nil
		}
		ret = append(ret, extractOwnerReference(o))
	}
	return ret
}

func (u *Unstructured) SetOwnerReferences(references []metav1.OwnerReference) {
	// nil 切片会删除整个字段，避免序列化出空数组，与 Go 结构体的 omitempty 行为一致。
	if references == nil {
		RemoveNestedField(u.Object, "metadata", "ownerReferences")
		return
	}

	newReferences := make([]interface{}, 0, len(references))
	for _, reference := range references {
		out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&reference)
		if err != nil {
			// 跳过坏条目但告知用户；其余引用仍会保存，避免元数据整体丢失。
			utilruntime.HandleError(fmt.Errorf("unable to convert Owner Reference: %v", err))
			continue
		}
		newReferences = append(newReferences, out)
	}
	u.setNestedField(newReferences, "metadata", "ownerReferences")
}

func (u *Unstructured) GetAPIVersion() string {
	return getNestedString(u.Object, "apiVersion")
}

func (u *Unstructured) SetAPIVersion(version string) {
	u.setNestedField(version, "apiVersion")
}

func (u *Unstructured) GetKind() string {
	return getNestedString(u.Object, "kind")
}

func (u *Unstructured) SetKind(kind string) {
	u.setNestedField(kind, "kind")
}

func (u *Unstructured) GetNamespace() string {
	return getNestedString(u.Object, "metadata", "namespace")
}

// SetNamespace 将命名空间存入 metadata。空值会删除键而非写入 ""，
// 以保持序列化对象精简，并使 omitempty 风格的比较生效。
func (u *Unstructured) SetNamespace(namespace string) {
	if len(namespace) == 0 {
		RemoveNestedField(u.Object, "metadata", "namespace")
		return
	}
	u.setNestedField(namespace, "metadata", "namespace")
}

func (u *Unstructured) GetName() string {
	return getNestedString(u.Object, "metadata", "name")
}

func (u *Unstructured) SetName(name string) {
	if len(name) == 0 {
		RemoveNestedField(u.Object, "metadata", "name")
		return
	}
	u.setNestedField(name, "metadata", "name")
}

func (u *Unstructured) GetGenerateName() string {
	return getNestedString(u.Object, "metadata", "generateName")
}

func (u *Unstructured) SetGenerateName(generateName string) {
	if len(generateName) == 0 {
		RemoveNestedField(u.Object, "metadata", "generateName")
		return
	}
	u.setNestedField(generateName, "metadata", "generateName")
}

func (u *Unstructured) GetUID() types.UID {
	return types.UID(getNestedString(u.Object, "metadata", "uid"))
}

func (u *Unstructured) SetUID(uid types.UID) {
	if len(string(uid)) == 0 {
		RemoveNestedField(u.Object, "metadata", "uid")
		return
	}
	u.setNestedField(string(uid), "metadata", "uid")
}

func (u *Unstructured) GetResourceVersion() string {
	return getNestedString(u.Object, "metadata", "resourceVersion")
}

func (u *Unstructured) SetResourceVersion(resourceVersion string) {
	if len(resourceVersion) == 0 {
		RemoveNestedField(u.Object, "metadata", "resourceVersion")
		return
	}
	u.setNestedField(resourceVersion, "metadata", "resourceVersion")
}

func (u *Unstructured) GetGeneration() int64 {
	val, found, err := NestedInt64(u.Object, "metadata", "generation")
	if !found || err != nil {
		return 0
	}
	return val
}

// SetGeneration 在值为零时删除字段，与有类型的元数据结构体的 omitempty 行为一致。
func (u *Unstructured) SetGeneration(generation int64) {
	if generation == 0 {
		RemoveNestedField(u.Object, "metadata", "generation")
		return
	}
	u.setNestedField(generation, "metadata", "generation")
}

func (u *Unstructured) GetSelfLink() string {
	return getNestedString(u.Object, "metadata", "selfLink")
}

func (u *Unstructured) SetSelfLink(selfLink string) {
	if len(selfLink) == 0 {
		RemoveNestedField(u.Object, "metadata", "selfLink")
		return
	}
	u.setNestedField(selfLink, "metadata", "selfLink")
}

func (u *Unstructured) GetContinue() string {
	return getNestedString(u.Object, "metadata", "continue")
}

func (u *Unstructured) SetContinue(c string) {
	if len(c) == 0 {
		RemoveNestedField(u.Object, "metadata", "continue")
		return
	}
	u.setNestedField(c, "metadata", "continue")
}

func (u *Unstructured) GetRemainingItemCount() *int64 {
	return getNestedInt64Pointer(u.Object, "metadata", "remainingItemCount")
}

func (u *Unstructured) SetRemainingItemCount(c *int64) {
	if c == nil {
		RemoveNestedField(u.Object, "metadata", "remainingItemCount")
	} else {
		u.setNestedField(*c, "metadata", "remainingItemCount")
	}
}

func (u *Unstructured) GetCreationTimestamp() metav1.Time {
	var timestamp metav1.Time
	timestamp.UnmarshalQueryParameter(getNestedString(u.Object, "metadata", "creationTimestamp"))
	return timestamp
}

func (u *Unstructured) SetCreationTimestamp(timestamp metav1.Time) {
	ts, _ := timestamp.MarshalQueryParameter()
	// 不得持久化零时间戳，否则每个对象都会携带 "0001-01-01T00:00:00Z"。
	if len(ts) == 0 || timestamp.Time.IsZero() {
		RemoveNestedField(u.Object, "metadata", "creationTimestamp")
		return
	}
	u.setNestedField(ts, "metadata", "creationTimestamp")
}

func (u *Unstructured) GetDeletionTimestamp() *metav1.Time {
	var timestamp metav1.Time
	timestamp.UnmarshalQueryParameter(getNestedString(u.Object, "metadata", "deletionTimestamp"))
	if timestamp.IsZero() {
		return nil
	}
	return &timestamp
}

func (u *Unstructured) SetDeletionTimestamp(timestamp *metav1.Time) {
	if timestamp == nil {
		RemoveNestedField(u.Object, "metadata", "deletionTimestamp")
		return
	}
	ts, _ := timestamp.MarshalQueryParameter()
	u.setNestedField(ts, "metadata", "deletionTimestamp")
}

func (u *Unstructured) GetDeletionGracePeriodSeconds() *int64 {
	val, found, err := NestedInt64(u.Object, "metadata", "deletionGracePeriodSeconds")
	if !found || err != nil {
		return nil
	}
	return &val
}

func (u *Unstructured) SetDeletionGracePeriodSeconds(deletionGracePeriodSeconds *int64) {
	if deletionGracePeriodSeconds == nil {
		RemoveNestedField(u.Object, "metadata", "deletionGracePeriodSeconds")
		return
	}
	u.setNestedField(*deletionGracePeriodSeconds, "metadata", "deletionGracePeriodSeconds")
}

func (u *Unstructured) GetLabels() map[string]string {
	m, _, _ := NestedNullCoercingStringMap(u.Object, "metadata", "labels")
	return m
}

func (u *Unstructured) SetLabels(labels map[string]string) {
	if labels == nil {
		RemoveNestedField(u.Object, "metadata", "labels")
		return
	}
	u.setNestedMap(labels, "metadata", "labels")
}

func (u *Unstructured) GetAnnotations() map[string]string {
	m, _, _ := NestedNullCoercingStringMap(u.Object, "metadata", "annotations")
	return m
}

func (u *Unstructured) SetAnnotations(annotations map[string]string) {
	if annotations == nil {
		RemoveNestedField(u.Object, "metadata", "annotations")
		return
	}
	u.setNestedMap(annotations, "metadata", "annotations")
}

// SetGroupVersionKind 分别写入 apiVersion 和 kind，因为非结构化载荷将它们存为顶层字符串字段。
func (u *Unstructured) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
}

// GroupVersionKind 将 apiVersion 解析回 GVK。格式错误时返回空 GVK 而非错误，
// 以确保访问器不会 panic。
func (u *Unstructured) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(u.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(u.GetKind())
	return gvk
}

func (u *Unstructured) GetFinalizers() []string {
	val, _, _ := NestedStringSlice(u.Object, "metadata", "finalizers")
	return val
}

func (u *Unstructured) SetFinalizers(finalizers []string) {
	if finalizers == nil {
		RemoveNestedField(u.Object, "metadata", "finalizers")
		return
	}
	u.setNestedStringSlice(finalizers, "metadata", "finalizers")
}

func (u *Unstructured) GetManagedFields() []metav1.ManagedFieldsEntry {
	v, found, err := NestedFieldNoCopy(u.Object, "metadata", "managedFields")
	if !found || err != nil {
		return nil
	}
	items, ok := v.([]interface{})
	if !ok {
		return nil
	}
	managedFields := []metav1.ManagedFieldsEntry{}
	for _, item := range items {
		m, ok := item.(map[string]interface{})
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unable to retrieve managedFields for object, item %v is not a map", item))
			return nil
		}
		out := metav1.ManagedFieldsEntry{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(m, &out); err != nil {
			utilruntime.HandleError(fmt.Errorf("unable to retrieve managedFields for object: %v", err))
			return nil
		}
		managedFields = append(managedFields, out)
	}
	return managedFields
}

// SetManagedFields 使用默认转换器转换每项；若某项含有类型结构体无法表示的值，
// 转换会失败，更新中止，并通过 utilruntime 报告错误。
func (u *Unstructured) SetManagedFields(managedFields []metav1.ManagedFieldsEntry) {
	if managedFields == nil {
		RemoveNestedField(u.Object, "metadata", "managedFields")
		return
	}
	items := []interface{}{}
	for _, managedFieldsEntry := range managedFields {
		out, err := runtime.DefaultUnstructuredConverter.ToUnstructured(&managedFieldsEntry)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("unable to set managedFields for object: %v", err))
			return
		}
		items = append(items, out)
	}
	u.setNestedSlice(items, "metadata", "managedFields")
}
