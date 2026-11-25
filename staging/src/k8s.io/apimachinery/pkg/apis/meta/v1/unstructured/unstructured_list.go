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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

var _ runtime.Unstructured = &UnstructuredList{}
var _ metav1.ListInterface = &UnstructuredList{}

// UnstructuredList 允许通用操作未注册 Go 结构体的列表，可用于处理插件的 API 列表。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:deepcopy-gen=true
type UnstructuredList struct {
	Object map[string]interface{}

	// Items 是非结构化对象列表。
	Items []Unstructured `json:"items"`
}

func (u *UnstructuredList) GetObjectKind() schema.ObjectKind { return u }

// UnstructuredList 的 IsList 始终为 true；类型本身就是列表信号。
func (u *UnstructuredList) IsList() bool { return true }

// EachListItem 将每个已存储元素的指针传给 fn；调用方不保留该指针时可避免复制。
func (u *UnstructuredList) EachListItem(fn func(runtime.Object) error) error {
	for i := range u.Items {
		if err := fn(&u.Items[i]); err != nil {
			return err
		}
	}
	return nil
}

// EachListItemWithAlloc 为每项分配新的 Unstructured，调用方可在回调返回后安全保留对象。
func (u *UnstructuredList) EachListItemWithAlloc(fn func(runtime.Object) error) error {
	for i := range u.Items {
		if err := fn(&Unstructured{Object: u.Items[i].Object}); err != nil {
			return err
		}
	}
	return nil
}

// NewEmptyInstance 返回仅含 kind/apiVersion、不含其他数据的新实例。
// 非结构化类型应调用此方法而非 reflect.New()，因为 Go 类型本身不保留 kind/apiVersion。
func (u *UnstructuredList) NewEmptyInstance() runtime.Unstructured {
	out := new(UnstructuredList)
	if u != nil {
		out.SetGroupVersionKind(u.GroupVersionKind())
	}
	return out
}

// UnstructuredContent 返回将 Items 叠加到 Object 上的 map；items 始终覆盖叠加结果。
func (u *UnstructuredList) UnstructuredContent() map[string]interface{} {
	out := make(map[string]interface{}, len(u.Object)+1)

	// 浅复制每个属性。
	for k, v := range u.Object {
		out[k] = v
	}

	items := make([]interface{}, len(u.Items))
	for i, item := range u.Items {
		items[i] = item.UnstructuredContent()
	}
	out["items"] = items
	return out
}

// SetUnstructuredContent 遵循 List 的约定，使 Items 与 items 数组保持同步。
// 若传入 map 中的 items 不是对象数组，不匹配的元素会被删除。
//
// 必须重写该数组：保留非对象元素会导致后续访问器失败，同时 Items 却静默地不一致。
func (obj *UnstructuredList) SetUnstructuredContent(content map[string]interface{}) {
	obj.Object = content
	if content == nil {
		obj.Items = nil
		return
	}
	items, ok := obj.Object["items"].([]interface{})
	if !ok || items == nil {
		items = []interface{}{}
	}
	unstructuredItems := make([]Unstructured, 0, len(items))
	newItems := make([]interface{}, 0, len(items))
	for _, item := range items {
		o, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		unstructuredItems = append(unstructuredItems, Unstructured{Object: o})
		newItems = append(newItems, o)
	}
	obj.Items = unstructuredItems
	obj.Object["items"] = newItems
}

// DeepCopy 返回列表元数据及所有元素的独立副本。载荷无类型，因此不能使用生成代码。
func (u *UnstructuredList) DeepCopy() *UnstructuredList {
	if u == nil {
		return nil
	}
	out := new(UnstructuredList)
	*out = *u
	out.Object = runtime.DeepCopyJSON(u.Object)
	out.Items = make([]Unstructured, len(u.Items))
	for i := range u.Items {
		u.Items[i].DeepCopyInto(&out.Items[i])
	}
	return out
}

// MarshalJSON 确保非结构化列表对象传给 Go 标准 JSON 库时生成正确的 JSON。
func (u *UnstructuredList) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	err := UnstructuredJSONScheme.Encode(u, &buf)
	return buf.Bytes(), err
}

// UnmarshalJSON 确保非结构化列表对象正确解码 Go 标准 JSON 库传入的 JSON。
func (u *UnstructuredList) UnmarshalJSON(b []byte) error {
	_, _, err := UnstructuredJSONScheme.Decode(b, nil, u)
	return err
}

func (u *UnstructuredList) GetAPIVersion() string {
	return getNestedString(u.Object, "apiVersion")
}

func (u *UnstructuredList) SetAPIVersion(version string) {
	u.setNestedField(version, "apiVersion")
}

func (u *UnstructuredList) GetKind() string {
	return getNestedString(u.Object, "kind")
}

func (u *UnstructuredList) SetKind(kind string) {
	u.setNestedField(kind, "kind")
}

func (u *UnstructuredList) GetResourceVersion() string {
	return getNestedString(u.Object, "metadata", "resourceVersion")
}

func (u *UnstructuredList) SetResourceVersion(version string) {
	u.setNestedField(version, "metadata", "resourceVersion")
}

func (u *UnstructuredList) GetSelfLink() string {
	return getNestedString(u.Object, "metadata", "selfLink")
}

func (u *UnstructuredList) SetSelfLink(selfLink string) {
	u.setNestedField(selfLink, "metadata", "selfLink")
}

func (u *UnstructuredList) GetContinue() string {
	return getNestedString(u.Object, "metadata", "continue")
}

func (u *UnstructuredList) SetContinue(c string) {
	u.setNestedField(c, "metadata", "continue")
}

func (u *UnstructuredList) GetRemainingItemCount() *int64 {
	return getNestedInt64Pointer(u.Object, "metadata", "remainingItemCount")
}

func (u *UnstructuredList) SetRemainingItemCount(c *int64) {
	if c == nil {
		RemoveNestedField(u.Object, "metadata", "remainingItemCount")
	} else {
		u.setNestedField(*c, "metadata", "remainingItemCount")
	}
}

func (u *UnstructuredList) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	u.SetAPIVersion(gvk.GroupVersion().String())
	u.SetKind(gvk.Kind)
}

func (u *UnstructuredList) GroupVersionKind() schema.GroupVersionKind {
	gv, err := schema.ParseGroupVersion(u.GetAPIVersion())
	if err != nil {
		return schema.GroupVersionKind{}
	}
	gvk := gv.WithKind(u.GetKind())
	return gvk
}

func (u *UnstructuredList) setNestedField(value interface{}, fields ...string) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	SetNestedField(u.Object, value, fields...)
}
