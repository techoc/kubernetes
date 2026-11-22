/*
Copyright 2017 The Kubernetes Authors.

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

package v1beta1

import (
	"bytes"
	"errors"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/apimachinery/pkg/util/json"
)

var jsTrue = []byte("true")
var jsFalse = []byte("false")

// 以下与 CBOR 解析相关的常量和函数不导出，
// 以便将来 CBOR 库提供等价功能时能方便地移除。

// cborMajorType 是 CBOR 数据项的主类型（高 3 位）。
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
	cborFalseValue = 0xf4
	cborTrueValue  = 0xf5
	cborNullValue  = 0xf6
)

// cborType 从首字节提取 CBOR 主类型。
func cborType(b byte) cborMajorType {
	return cborMajorType(b >> 5)
}

// MarshalJSON 将 JSONSchemaPropsOrBool 序列化为内联 schema 或布尔值。
func (s JSONSchemaPropsOrBool) MarshalJSON() ([]byte, error) {
	if s.Schema != nil {
		return json.Marshal(s.Schema)
	}

	if s.Schema == nil && !s.Allows {
		return jsFalse, nil
	}
	return jsTrue, nil
}

// UnmarshalJSON 根据 JSON 对象的形态解析出 schema 或布尔值。
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

// MarshalCBOR 将 JSONSchemaPropsOrBool 序列化为内联 schema 或布尔值。
func (s JSONSchemaPropsOrBool) MarshalCBOR() ([]byte, error) {
	if s.Schema != nil {
		return cbor.Marshal(s.Schema)
	}
	return cbor.Marshal(s.Allows)
}

// UnmarshalCBOR 根据 CBOR 数据项的形态解析出 schema 或布尔值。
func (s *JSONSchemaPropsOrBool) UnmarshalCBOR(data []byte) error {
	switch {
	case len(data) == 0:
		// 这里本可以避免修改 *s，但为与 UnmarshalJSON 的行为保持一致而为之。
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
		// 理想情况下此分支不应同时匹配 null 输入，
		// 但这里为与 UnmarshalJSON 的行为保持一致。
		return errors.New("boolean or JSON schema expected")
	}
}

// MarshalJSON 将 JSONSchemaPropsOrStringArray 序列化为属性名列表或内联 schema。
func (s JSONSchemaPropsOrStringArray) MarshalJSON() ([]byte, error) {
	if len(s.Property) > 0 {
		return json.Marshal(s.Property)
	}
	if s.Schema != nil {
		return json.Marshal(s.Schema)
	}
	return []byte("null"), nil
}

// UnmarshalJSON 根据 JSON 对象的形态解析出属性名列表或 schema。
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

// MarshalCBOR 将 JSONSchemaPropsOrStringArray 序列化为属性名列表或内联 schema。
func (s JSONSchemaPropsOrStringArray) MarshalCBOR() ([]byte, error) {
	if len(s.Property) > 0 {
		return cbor.Marshal(s.Property)
	}
	if s.Schema != nil {
		return cbor.Marshal(s.Schema)
	}
	return cbor.Marshal(nil)
}

// UnmarshalCBOR 根据 CBOR 数据项的形态解析出属性名列表或 schema。
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
	// 此时数据要么为空、要么是 null、要么是意外类型。为了与既有
	// UnmarshalJSON 的行为保持一致，不返回错误，而是直接覆盖 *s。
	*s = JSONSchemaPropsOrStringArray{}
	return nil
}

// MarshalJSON 将 JSONSchemaPropsOrArray 序列化为 schema 列表或单个内联 schema。
func (s JSONSchemaPropsOrArray) MarshalJSON() ([]byte, error) {
	if len(s.JSONSchemas) > 0 {
		return json.Marshal(s.JSONSchemas)
	}
	return json.Marshal(s.Schema)
}

// UnmarshalJSON 根据 JSON 对象的形态解析出 schema 列表或单个 schema。
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

// MarshalCBOR 将 JSONSchemaPropsOrArray 序列化为 schema 列表或单个内联 schema。
func (s JSONSchemaPropsOrArray) MarshalCBOR() ([]byte, error) {
	if len(s.JSONSchemas) > 0 {
		return cbor.Marshal(s.JSONSchemas)
	}
	return cbor.Marshal(s.Schema)
}

// UnmarshalCBOR 根据 CBOR 数据项的形态解析出 schema 列表或单个 schema。
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
	// 此时数据要么为空、要么是 null、要么是意外类型。为了与既有
	// UnmarshalJSON 的行为保持一致，不返回错误，而是直接覆盖 *s。
	*s = JSONSchemaPropsOrArray{}
	return nil
}

// MarshalJSON 返回原始 JSON 字节，空值序列化为 null。
func (s JSON) MarshalJSON() ([]byte, error) {
	if len(s.Raw) > 0 {
		return s.Raw, nil
	}
	return []byte("null"), nil

}

// UnmarshalJSON 保存原始 JSON 字节；复用底层数组以避免额外分配。
func (s *JSON) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && !bytes.Equal(data, nullLiteral) {
		s.Raw = append(s.Raw[0:0], data...)
	}
	return nil
}

// MarshalCBOR 将原始 JSON 转码为 CBOR。
func (s JSON) MarshalCBOR() ([]byte, error) {
	// 注意：此处转码会丢失非语义的空白字符。
	// 鉴于该类型目前已知的用途，我们不认为这会成为问题。
	// 通过 dynamic client 往返 JSON 时的其他限制在这里同样适用，
	// 例如：无意义空白处理、数字处理以及 map key 排序。
	if len(s.Raw) == 0 {
		return []byte{cborNullValue}, nil
	}
	var u any
	if err := json.Unmarshal(s.Raw, &u); err != nil {
		return nil, err
	}
	return cbor.Marshal(u)
}

// UnmarshalCBOR 将 CBOR 转码为原始 JSON 字节。
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
