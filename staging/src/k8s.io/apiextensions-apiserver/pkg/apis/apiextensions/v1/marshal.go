/*
Copyright 2019 The Kubernetes Authors.

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

// 本文件定义 CRD v1 版本中特殊类型的 JSON/CBOR 序列化/反序列化逻辑。
// CRD 的 schema 包含联合类型（如 JSONSchemaPropsOrBool），
// 标准 Go 序列化无法正确处理，需要自定义 Marshal/Unmarshal 方法。
//
// 支持的联合类型：
//   - JSONSchemaPropsOrBool：schema 对象或布尔值（用于 x-kubernetes-preserve-unknown-fields）
//   - JSONSchemaPropsOrStringArray：schema 对象或字符串数组（用于 x-kubernetes-list-map-keys）
//   - JSONSchemaPropsOrArray：schema 对象或 schema 数组（用于 oneOf）
//   - JSON：原始 JSON 值（用于 default 和 example 字段）
package v1

import (
	"bytes"
	"errors"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/util/json"
)

var jsTrue = []byte("true")
var jsFalse = []byte("false")

// CBOR 解析相关常量和函数是内部使用的，
// 用于区分 CBOR 数据的类型（Map、Array、Boolean 等）。
// 当 CBOR 库提供等效功能时可以移除。

type cborMajorType int

const (
	// https://www.rfc-editor.org/rfc/rfc8949.html#section-3.1
	cborUnsignedInteger cborMajorType = 0
	cborNegativeInteger cborMajorType = 1
	cborByteString      cborMajorType = 2
	cborTextString      cborMajorType = 3
	cborArray           cborMajorType = 4
	cborMap             cborMajorType = 5
	cborTag             cborMajorType = 6
	cborOther           cborMajorType = 7
)

const (
	// from https://www.rfc-editor.org/rfc/rfc8949.html#name-jump-table-for-initial-byte.
	// additionally, see https://www.rfc-editor.org/rfc/rfc8949.html#section-3.3-5.
	cborFalseValue = 0xf4
	cborTrueValue  = 0xf5
	cborNullValue  = 0xf6
)

// cborType 从 CBOR 数据的第一个字节提取主类型（高 3 位）。
func cborType(b byte) cborMajorType {
	return cborMajorType(b >> 5)
}

// MarshalJSON 将 JSONSchemaPropsOrBool 序列化为 JSON。
// 如果有 schema 对象，序列化 schema；否则序列化布尔值（true/false）。
func (s JSONSchemaPropsOrBool) MarshalJSON() ([]byte, error) {
	if s.Schema != nil {
		return json.Marshal(s.Schema)
	}

	if s.Schema == nil && !s.Allows {
		return jsFalse, nil
	}
	return jsTrue, nil
}

// UnmarshalJSON 将 JSON 数据反序列化为 JSONSchemaPropsOrBool。
// 根据第一个字节判断类型：'{' 表示 schema 对象，"true" 和 "false" 表示布尔值。
func (s *JSONSchemaPropsOrBool) UnmarshalJSON(data []byte) error {
	var nw JSONSchemaPropsOrBool
	switch {
	case len(data) == 0:
	case data[0] == '{':
		var sch JSONSchemaProps
		if err := json.Unmarshal(data, &sch); err != nil {
			return err
		}
		nw.Allows = true
		nw.Schema = &sch
	case len(data) == 4 && string(data) == "true":
		nw.Allows = true
	case len(data) == 5 && string(data) == "false":
		nw.Allows = false
	default:
		return errors.New("boolean or JSON schema expected")
	}
	*s = nw
	return nil
}

// MarshalCBOR 将 JSONSchemaPropsOrBool 序列化为 CBOR 格式。
func (s JSONSchemaPropsOrBool) MarshalCBOR() ([]byte, error) {
	if s.Schema != nil {
		return cbor.Marshal(s.Schema)
	}
	return cbor.Marshal(s.Allows)
}

// UnmarshalCBOR 将 CBOR 数据反序列化为 JSONSchemaPropsOrBool。
// 通过检查第一个字节的主类型来区分 Map（schema）和 Boolean（true/false）。
func (s *JSONSchemaPropsOrBool) UnmarshalCBOR(data []byte) error {
	switch {
	case len(data) == 0:
		// ideally we would avoid modifying *s here, but we are matching the behavior of UnmarshalJSON
		*s = JSONSchemaPropsOrBool{}
		return nil
	case cborType(data[0]) == cborMap:
		var p JSONSchemaProps
		if err := cbor.Unmarshal(data, &p); err != nil {
			return err
		}
		*s = JSONSchemaPropsOrBool{Allows: true, Schema: &p}
		return nil
	case data[0] == cborTrueValue:
		*s = JSONSchemaPropsOrBool{Allows: true}
		return nil
	case data[0] == cborFalseValue:
		*s = JSONSchemaPropsOrBool{Allows: false}
		return nil
	default:
		// ideally, this case would not also capture a null input value,
		// but we are matching the behavior of the UnmarshalJSON
		return errors.New("boolean or JSON schema expected")
	}
}

// MarshalJSON 将 JSONSchemaPropsOrStringArray 序列化为 JSON。
// 优先级：字符串数组 > schema 对象 > null。
func (s JSONSchemaPropsOrStringArray) MarshalJSON() ([]byte, error) {
	if len(s.Property) > 0 {
		return json.Marshal(s.Property)
	}
	if s.Schema != nil {
		return json.Marshal(s.Schema)
	}
	return []byte("null"), nil
}

// UnmarshalJSON 将 JSON 数据反序列化为 JSONSchemaPropsOrStringArray。
// '{' 表示 schema 对象，'[' 表示字符串数组。
func (s *JSONSchemaPropsOrStringArray) UnmarshalJSON(data []byte) error {
	var first byte
	if len(data) > 1 {
		first = data[0]
	}
	var nw JSONSchemaPropsOrStringArray
	if first == '{' {
		var sch JSONSchemaProps
		if err := json.Unmarshal(data, &sch); err != nil {
			return err
		}
		nw.Schema = &sch
	}
	if first == '[' {
		if err := json.Unmarshal(data, &nw.Property); err != nil {
			return err
		}
	}
	*s = nw
	return nil
}

// MarshalCBOR 将 JSONSchemaPropsOrStringArray 序列化为 CBOR 格式。
func (s JSONSchemaPropsOrStringArray) MarshalCBOR() ([]byte, error) {
	if len(s.Property) > 0 {
		return cbor.Marshal(s.Property)
	}
	if s.Schema != nil {
		return cbor.Marshal(s.Schema)
	}
	return cbor.Marshal(nil)
}

// UnmarshalCBOR 将 CBOR 数据反序列化为 JSONSchemaPropsOrStringArray。
func (s *JSONSchemaPropsOrStringArray) UnmarshalCBOR(data []byte) error {
	if len(data) > 0 && cborType(data[0]) == cborArray {
		var a []string
		if err := cbor.Unmarshal(data, &a); err != nil {
			return err
		}
		*s = JSONSchemaPropsOrStringArray{Property: a}
		return nil
	}
	if len(data) > 0 && cborType(data[0]) == cborMap {
		var p JSONSchemaProps
		if err := cbor.Unmarshal(data, &p); err != nil {
			return err
		}
		*s = JSONSchemaPropsOrStringArray{Schema: &p}
		return nil
	}
	// At this point we either have: empty data, a null value, or an
	// unexpected type. In order to match the behavior of the existing
	// UnmarshalJSON, no error is returned and *s is overwritten here.
	*s = JSONSchemaPropsOrStringArray{}
	return nil
}

// MarshalJSON 将 JSONSchemaPropsOrArray 序列化为 JSON。
// 如果有 schema 数组，序列化数组；否则序列化单个 schema 对象。
func (s JSONSchemaPropsOrArray) MarshalJSON() ([]byte, error) {
	if len(s.JSONSchemas) > 0 {
		return json.Marshal(s.JSONSchemas)
	}
	return json.Marshal(s.Schema)
}

// UnmarshalJSON 将 JSON 数据反序列化为 JSONSchemaPropsOrArray。
// '{' 表示单个 schema 对象，'[' 表示 schema 数组。
func (s *JSONSchemaPropsOrArray) UnmarshalJSON(data []byte) error {
	var nw JSONSchemaPropsOrArray
	var first byte
	if len(data) > 1 {
		first = data[0]
	}
	if first == '{' {
		var sch JSONSchemaProps
		if err := json.Unmarshal(data, &sch); err != nil {
			return err
		}
		nw.Schema = &sch
	}
	if first == '[' {
		if err := json.Unmarshal(data, &nw.JSONSchemas); err != nil {
			return err
		}
	}
	*s = nw
	return nil
}

// MarshalCBOR 将 JSONSchemaPropsOrArray 序列化为 CBOR 格式。
func (s JSONSchemaPropsOrArray) MarshalCBOR() ([]byte, error) {
	if len(s.JSONSchemas) > 0 {
		return cbor.Marshal(s.JSONSchemas)
	}
	return cbor.Marshal(s.Schema)
}

// UnmarshalCBOR 将 CBOR 数据反序列化为 JSONSchemaPropsOrArray。
func (s *JSONSchemaPropsOrArray) UnmarshalCBOR(data []byte) error {
	if len(data) > 0 && cborType(data[0]) == cborMap {
		var p JSONSchemaProps
		if err := cbor.Unmarshal(data, &p); err != nil {
			return err
		}
		*s = JSONSchemaPropsOrArray{Schema: &p}
		return nil
	}
	if len(data) > 0 && cborType(data[0]) == cborArray {
		var a []JSONSchemaProps
		if err := cbor.Unmarshal(data, &a); err != nil {
			return err
		}
		*s = JSONSchemaPropsOrArray{JSONSchemas: a}
		return nil
	}
	// At this point we either have: empty data, a null value, or an
	// unexpected type. In order to match the behavior of the existing
	// UnmarshalJSON, no error is returned and *s is overwritten here.
	*s = JSONSchemaPropsOrArray{}
	return nil
}

// MarshalJSON 将 JSON 类型（原始字节）序列化为 JSON。
// 空字节返回 null，否则直接返回原始字节（避免二次编码）。
func (s JSON) MarshalJSON() ([]byte, error) {
	if len(s.Raw) > 0 {
		return s.Raw, nil
	}
	return []byte("null"), nil

}

// UnmarshalJSON 将 JSON 数据保存为原始字节。
// 字面量 null 不保存，保持 Raw 为空。
func (s *JSON) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && !bytes.Equal(data, nullLiteral) {
		s.Raw = append(s.Raw[0:0], data...)
	}
	return nil
}

// MarshalCBOR 将 JSON 原始字节转换为 CBOR 格式。
// 先将 JSON 字节反序列化为 Go 值，再序列化为 CBOR。
// 注意：此过程会丢失非语义空白字符，但对当前使用场景没有影响。
func (s JSON) MarshalCBOR() ([]byte, error) {
	if len(s.Raw) == 0 {
		return []byte{cborNullValue}, nil
	}
	var u any
	if err := json.Unmarshal(s.Raw, &u); err != nil {
		return nil, err
	}
	return cbor.Marshal(u)
}

// UnmarshalCBOR 将 CBOR 数据转换为 JSON 原始字节。
// 先将 CBOR 反序列化为 Go 值，再序列化为 JSON 字节。
func (s *JSON) UnmarshalCBOR(data []byte) error {
	if len(data) == 0 || data[0] == cborNullValue {
		return nil
	}
	var u any
	if err := cbor.Unmarshal(data, &u); err != nil {
		return err
	}
	raw, err := json.Marshal(u)
	if err != nil {
		return err
	}
	s.Raw = raw
	return nil
}
