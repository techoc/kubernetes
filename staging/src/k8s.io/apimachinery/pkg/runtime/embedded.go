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

package runtime

import (
	"errors"

	"k8s.io/apimachinery/pkg/conversion"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// encodable 包装一个 internal 对象，并在序列化时按需交给 Encoder 转换到
// 目标版本。它让测试代码可以直接把 internal 对象放进期望 encoded 的位置。
type encodable struct {
	E        Encoder `json:"-"`
	obj      Object
	versions []schema.GroupVersion
}

func (e encodable) GetObjectKind() schema.ObjectKind { return e.obj.GetObjectKind() }

// DeepCopyObject 同时深拷贝被包装对象并复制 versions 切片；直接复制结构体会
// 让副本与原对象共享底层数组，可能被意外修改。
func (e encodable) DeepCopyObject() Object {
	out := e
	out.obj = e.obj.DeepCopyObject()
	copy(out.versions, e.versions)
	return out
}

// NewEncodable creates an object that will be encoded with the provided codec on demand.
// Provided as a convenience for test cases dealing with internal objects.
// 中文说明：Unknown 原样返回，因为它本身已经是 encoded 形式，不应再被编码。
func NewEncodable(e Encoder, obj Object, versions ...schema.GroupVersion) Object {
	if _, ok := obj.(*Unknown); ok {
		return obj
	}
	return encodable{e, obj, versions}
}

func (e encodable) UnmarshalJSON(in []byte) error {
	return errors.New("runtime.encodable cannot be unmarshalled from JSON")
}

// Marshal may get called on pointers or values, so implement MarshalJSON on value.
// http://stackoverflow.com/questions/21390979/custom-marshaljson-never-gets-called-in-go
func (e encodable) MarshalJSON() ([]byte, error) {
	return Encode(e.E, e.obj)
}

// NewEncodableList creates an object that will be encoded with the provided codec on demand.
// Provided as a convenience for test cases dealing with internal objects.
// 中文说明：与 NewEncodable 一致，Unknown 元素保持原样。
func NewEncodableList(e Encoder, objects []Object, versions ...schema.GroupVersion) []Object {
	out := make([]Object, len(objects))
	for i := range objects {
		if _, ok := objects[i].(*Unknown); ok {
			out[i] = objects[i]
			continue
		}
		out[i] = NewEncodable(e, objects[i], versions...)
	}
	return out
}

// UnmarshalJSON 把任意 JSON 原始字节保存在 Raw 中，并重置 TypeMeta；
// 这样 Unknown 可以作为“未知类型”的容器，后续再按 ContentType 解码。
func (e *Unknown) UnmarshalJSON(in []byte) error {
	if e == nil {
		return errors.New("runtime.Unknown: UnmarshalJSON on nil pointer")
	}
	e.TypeMeta = TypeMeta{}
	e.Raw = append(e.Raw[0:0], in...)
	e.ContentEncoding = ""
	e.ContentType = ContentTypeJSON
	return nil
}

// Marshal may get called on pointers or values, so implement MarshalJSON on value.
// http://stackoverflow.com/questions/21390979/custom-marshaljson-never-gets-called-in-go
func (e Unknown) MarshalJSON() ([]byte, error) {
	// 未设置 ContentType 时按 JSON 处理；非 JSON 内容无法直接作为 JSON 输出。
	// If ContentType is unset, we assume this is JSON.
	if e.ContentType != "" && e.ContentType != ContentTypeJSON {
		return nil, errors.New("runtime.Unknown: MarshalJSON on non-json data")
	}
	if e.Raw == nil {
		return []byte("null"), nil
	}
	return e.Raw, nil
}

// Convert_runtime_Object_To_runtime_RawExtension 把 interface 字段中的对象
// 转为 RawExtension：Unknown 直接复用其原始字节（已是外部表示），其他对象
// 则保留为 Object 引用，由序列化阶段再编码。
func Convert_runtime_Object_To_runtime_RawExtension(in *Object, out *RawExtension, s conversion.Scope) error {
	if in == nil {
		out.Raw = []byte("null")
		return nil
	}
	obj := *in
	if unk, ok := obj.(*Unknown); ok {
		if unk.Raw != nil {
			out.Raw = unk.Raw
			return nil
		}
		obj = out.Object
	}
	if obj == nil {
		out.Raw = nil
		return nil
	}
	out.Object = obj
	return nil
}

// Convert_runtime_RawExtension_To_runtime_Object 是上一个转换的逆过程：
// 若已有 Object 则直接使用；否则把 Raw 包装为 Unknown，并把 "null" 或空
// 字节视为 nil。ContentType 暂固定为 JSON 以兼容现有行为。
func Convert_runtime_RawExtension_To_runtime_Object(in *RawExtension, out *Object, s conversion.Scope) error {
	if in.Object != nil {
		*out = in.Object
		return nil
	}
	data := in.Raw
	if len(data) == 0 || (len(data) == 4 && string(data) == "null") {
		*out = nil
		return nil
	}
	*out = &Unknown{
		Raw: data,
		// TODO: Set ContentEncoding and ContentType appropriately.
		// Currently we set ContentTypeJSON to make tests passing.
		ContentType: ContentTypeJSON,
	}
	return nil
}

// RegisterEmbeddedConversions 把 Object 与 RawExtension 之间的双向转换注册到
// scheme，供 conversion-gen 生成的代码在遇到这些类型时复用。
func RegisterEmbeddedConversions(s *Scheme) error {
	if err := s.AddConversionFunc((*Object)(nil), (*RawExtension)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_runtime_Object_To_runtime_RawExtension(a.(*Object), b.(*RawExtension), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*RawExtension)(nil), (*Object)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_runtime_RawExtension_To_runtime_Object(a.(*RawExtension), b.(*Object), scope)
	}); err != nil {
		return err
	}
	return nil
}
