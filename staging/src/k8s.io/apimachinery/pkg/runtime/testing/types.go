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

package testing

import (
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/json"
)

// EmbeddedTest 在内部表示中直接内嵌 runtime.Object，用于验证转换时对
// 嵌套对象的处理。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type EmbeddedTest struct {
	runtime.TypeMeta
	ID          string
	Object      runtime.Object
	EmptyObject runtime.Object
}

// EmbeddedTestExternal 是 EmbeddedTest 的对外版本：嵌套对象用
// RawExtension 承载，序列化时保持原始字节。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type EmbeddedTestExternal struct {
	runtime.TypeMeta `json:""`
	ID               string               `json:"id,omitempty"`
	Object           runtime.RawExtension `json:"object,omitempty"`
	EmptyObject      runtime.RawExtension `json:"emptyObject,omitempty"`
}

// ObjectTest 用切片承载任意 runtime.Object，用于测试列表语义。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ObjectTest struct {
	runtime.TypeMeta

	ID    string
	Items []runtime.Object
}

// ObjectTestExternal 是 ObjectTest 的对外版本，元素改为 RawExtension。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ObjectTestExternal struct {
	runtime.TypeMeta `yaml:",inline" json:""`

	ID    string                 `json:"id,omitempty"`
	Items []runtime.RawExtension `json:"items,omitempty"`
}

// InternalSimple 是最小的内部类型，用于基础编解码测试。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InternalSimple struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}

// ExternalSimple 与 InternalSimple 结构相同但类型不同，用于验证版本转换
// 确实经过转换而非直接复用指针。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalSimple struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}

// ExtensionA 与 ExtensionB 用于测试扩展字段（RawExtension）在不同
// 目标类型下的解码。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExtensionA struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}

// ExtensionB 是 ExtensionA 的对照类型。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExtensionB struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}

// ExternalExtensionType 的 Extension 字段必填，用于测试缺字段时的行为。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalExtensionType struct {
	runtime.TypeMeta `json:""`
	Extension        runtime.RawExtension `json:"extension"`
}

// InternalExtensionType 与 ExternalExtensionType 对应，内部用 runtime.Object。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InternalExtensionType struct {
	runtime.TypeMeta `json:""`
	Extension        runtime.Object `json:"extension"`
}

// ExternalOptionalExtensionType 的扩展字段可省略，用于验证 omitempty 语义。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalOptionalExtensionType struct {
	runtime.TypeMeta `json:""`
	Extension        runtime.RawExtension `json:"extension,omitempty"`
}

// InternalOptionalExtensionType 与 ExternalOptionalExtensionType 对应。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InternalOptionalExtensionType struct {
	runtime.TypeMeta `json:""`
	Extension        runtime.Object `json:"extension,omitempty"`
}

// InternalComplex 覆盖多种基础数值类型，且没有任何 json tag，用于验证
// 默认字段命名与类型转换。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type InternalComplex struct {
	runtime.TypeMeta
	String    string
	Integer   int
	Integer64 int64
	Int64     int64
	Bool      bool
}

// ExternalComplex 是 InternalComplex 的对外版本，刻意使用不同的 tag
// （含 description、omitempty 与无 tag 字段）来测试标签差异。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalComplex struct {
	runtime.TypeMeta `json:""`
	String           string `json:"string" description:"testing"`
	Integer          int    `json:"int"`
	Integer64        int64  `json:",omitempty"`
	Int64            int64
	Bool             bool `json:"bool"`
}

// 测试一种少见的 version/kind 嵌入格式。
//
// 自定义的 GVK 字段名（myVersionKey/myKindKey）用于验证序列化器不依赖
// 固定的 apiVersion/kind 字段名，而是通过 ObjectKind 接口读写。
// +k8s:deepcopy-gen=false
type MyWeirdCustomEmbeddedVersionKindField struct {
	ID         string `json:"ID,omitempty"`
	APIVersion string `json:"myVersionKey,omitempty"`
	ObjectKind string `json:"myKindKey,omitempty"`
	Z          string `json:"Z,omitempty"`
	Y          uint64 `json:"Y,omitempty"`
}

// TestType1 覆盖全部整数宽度、map、指针与切片字段，是转换测试的主力类型。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TestType1 struct {
	MyWeirdCustomEmbeddedVersionKindField `json:""`
	A                                     string               `json:"A,omitempty"`
	B                                     int                  `json:"B,omitempty"`
	C                                     int8                 `json:"C,omitempty"`
	D                                     int16                `json:"D,omitempty"`
	E                                     int32                `json:"E,omitempty"`
	F                                     int64                `json:"F,omitempty"`
	G                                     uint                 `json:"G,omitempty"`
	H                                     uint8                `json:"H,omitempty"`
	I                                     uint16               `json:"I,omitempty"`
	J                                     uint32               `json:"J,omitempty"`
	K                                     uint64               `json:"K,omitempty"`
	L                                     bool                 `json:"L,omitempty"`
	M                                     map[string]int       `json:"M,omitempty"`
	N                                     map[string]TestType2 `json:"N,omitempty"`
	O                                     *TestType2           `json:"O,omitempty"`
	P                                     []TestType2          `json:"Q,omitempty"`
}

// TestType2 是 TestType1 的嵌套元素类型。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type TestType2 struct {
	A string `json:"A,omitempty"`
	B int    `json:"B,omitempty"`
}

// ExternalTestType2 是 TestType2 的对外版本，二者字段相同但类型独立，
// 使转换器必须逐字段拷贝。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalTestType2 struct {
	A string `json:"A,omitempty"`
	B int    `json:"B,omitempty"`
}

// ExternalTestType1 是 TestType1 的对外版本；注意 P 字段的 JSON 名是 "Q"，
// 这是刻意保留的历史怪癖，用于验证 tag 解析不受字段名影响。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalTestType1 struct {
	MyWeirdCustomEmbeddedVersionKindField `json:""`
	A                                     string                       `json:"A,omitempty"`
	B                                     int                          `json:"B,omitempty"`
	C                                     int8                         `json:"C,omitempty"`
	D                                     int16                        `json:"D,omitempty"`
	E                                     int32                        `json:"E,omitempty"`
	F                                     int64                        `json:"F,omitempty"`
	G                                     uint                         `json:"G,omitempty"`
	H                                     uint8                        `json:"H,omitempty"`
	I                                     uint16                       `json:"I,omitempty"`
	J                                     uint32                       `json:"J,omitempty"`
	K                                     uint64                       `json:"K,omitempty"`
	L                                     bool                         `json:"L,omitempty"`
	M                                     map[string]int               `json:"M,omitempty"`
	N                                     map[string]ExternalTestType2 `json:"N,omitempty"`
	O                                     *ExternalTestType2           `json:"O,omitempty"`
	P                                     []ExternalTestType2          `json:"Q,omitempty"`
}

// ExternalInternalSame 的字段类型是内部类型 TestType2，用于验证同一个
// 类型同时出现在内部与外部结构中时不会重复转换。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ExternalInternalSame struct {
	MyWeirdCustomEmbeddedVersionKindField `json:""`
	A                                     TestType2 `json:"A,omitempty"`
}

// UnversionedType 用于验证无版本对象在编解码时不做版本转换。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type UnversionedType struct {
	MyWeirdCustomEmbeddedVersionKindField `json:""`
	A                                     string `json:"A,omitempty"`
}

// UnknownType 模拟未注册类型，用于验证 runtime.Unknown 的降级路径。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type UnknownType struct {
	MyWeirdCustomEmbeddedVersionKindField `json:""`
	A                                     string `json:"A,omitempty"`
}

func (obj *MyWeirdCustomEmbeddedVersionKindField) GetObjectKind() schema.ObjectKind { return obj }
func (obj *MyWeirdCustomEmbeddedVersionKindField) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.ObjectKind = gvk.ToAPIVersionAndKind()
}
func (obj *MyWeirdCustomEmbeddedVersionKindField) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.ObjectKind)
}

func (obj *TestType2) GetObjectKind() schema.ObjectKind         { return schema.EmptyObjectKind }
func (obj *ExternalTestType2) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }

// Unstructured 是最小化的 Unstructured 实现，字段值只允许 JSON 兼容类型。
// +k8s:deepcopy-gen=false
type Unstructured struct {
	// Object 是与 JSON 兼容的 map，其值可以是 string、float、int、bool、
	// []interface{} 或 map[string]interface{}。
	Object map[string]interface{}
}

var _ runtime.Unstructured = &Unstructured{}

func (obj *Unstructured) GetObjectKind() schema.ObjectKind { return obj }

// IsList 以是否存在 "items" 字段判断列表，这与真实 Unstructured 的约定一致。
func (obj *Unstructured) IsList() bool {
	if obj.Object != nil {
		_, ok := obj.Object["items"]
		return ok
	}
	return false
}

// EachListItem 逐项回调；items 不是 []interface{} 时返回 nil（视为空列表），
// 而元素不是对象时报错，避免把非对象内容静默丢弃。
func (obj *Unstructured) EachListItem(fn func(runtime.Object) error) error {
	if obj.Object == nil {
		return fmt.Errorf("content is not a list")
	}
	field, ok := obj.Object["items"]
	if !ok {
		return fmt.Errorf("content is not a list")
	}
	items, ok := field.([]interface{})
	if !ok {
		return nil
	}
	for _, item := range items {
		child, ok := item.(map[string]interface{})
		if !ok {
			return fmt.Errorf("items member is not an object")
		}
		if err := fn(&Unstructured{Object: child}); err != nil {
			return err
		}
	}
	return nil
}

func (obj *Unstructured) EachListItemWithAlloc(fn func(runtime.Object) error) error {
	// EachListItem 已经为用户分配好新的 Object，可以直接使用。
	return obj.EachListItem(fn)
}

// NewEmptyInstance 返回同 GVK 的空实例；接收者为 nil 时返回零值实例，
// 调用方无需预先判空。
func (obj *Unstructured) NewEmptyInstance() runtime.Unstructured {
	out := new(Unstructured)
	if obj != nil {
		out.SetGroupVersionKind(obj.GroupVersionKind())
	}
	return out
}

// UnstructuredContent 返回内部 map 本身（非拷贝），调用方的修改会直接
// 反映到对象上；nil map 返回空 map 以简化调用方处理。
func (obj *Unstructured) UnstructuredContent() map[string]interface{} {
	if obj.Object == nil {
		return make(map[string]interface{})
	}
	return obj.Object
}

func (obj *Unstructured) SetUnstructuredContent(content map[string]interface{}) {
	obj.Object = content
}

// MarshalJSON 保证 unstructured 对象经 Go 标准 JSON 库序列化时输出正确的 JSON。
func (u *Unstructured) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.Object)
}

// UnmarshalJSON 保证 unstructured 对象经 Go 标准 JSON 库反序列化时能正确解析 JSON。
func (u *Unstructured) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &u.Object)
}

func (in *Unstructured) DeepCopyObject() runtime.Object {
	return in.DeepCopy()
}

// DeepCopy 深拷贝内部 map；nil 接收者返回 nil，保持指针语义。
func (in *Unstructured) DeepCopy() *Unstructured {
	if in == nil {
		return nil
	}
	out := new(Unstructured)
	*out = *in
	out.Object = runtime.DeepCopyJSON(in.Object)
	return out
}

// GroupVersionKind 从 map 中的 apiVersion/kind 解析 GVK；任一字段缺失或
// 格式非法时返回零值 GVK，而不是报错。
func (u *Unstructured) GroupVersionKind() schema.GroupVersionKind {
	apiVersion, ok := u.Object["apiVersion"].(string)
	if !ok {
		return schema.GroupVersionKind{}
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	if err != nil {
		return schema.GroupVersionKind{}
	}
	kind, ok := u.Object["kind"].(string)
	if ok {
		return gv.WithKind(kind)
	}
	return schema.GroupVersionKind{}
}

// SetGroupVersionKind 把 GVK 写回 map；必要时先初始化 map，使零值对象
// 也可直接使用。
func (u *Unstructured) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	if u.Object == nil {
		u.Object = make(map[string]interface{})
	}
	u.Object["apiVersion"] = gvk.GroupVersion().String()
	u.Object["kind"] = gvk.Kind
}
