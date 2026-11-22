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

// Package intstr 提供可同时承载整数或字符串的类型，用于既能接受数量
// 又能接受名称/百分比的 API 字段。
package intstr

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"strconv"
	"strings"

	cbor "k8s.io/apimachinery/pkg/runtime/serializer/cbor/direct"
	"k8s.io/klog/v2"
)

// IntOrString 可承载 int32 或 string。在 JSON/YAML 编解码时会生成或消费
// 其内部的值类型，因此字段可以同时接受名称或数字。
// TODO: Rename to Int32OrString
//
// +protobuf=true
// +protobuf.options.(gogoproto.goproto_stringer)=false
// +k8s:openapi-gen=true
// +k8s:openapi-model-package=io.k8s.apimachinery.pkg.util.intstr
type IntOrString struct {
	Type   Type   `protobuf:"varint,1,opt,name=type,casttype=Type"`
	IntVal int32  `protobuf:"varint,2,opt,name=intVal"`
	StrVal string `protobuf:"bytes,3,opt,name=strVal"`
}

// Type 表示 IntOrString 内部存储的值的类型。
type Type int64

const (
	Int    Type = iota // IntOrString 存储整数。
	String             // IntOrString 存储字符串。
)

// FromInt 用 int 值创建 IntOrString。调用方有责任保证值不超出 int32 范围。
// Deprecated: 请改用 FromInt32。
func FromInt(val int) IntOrString {
	if val > math.MaxInt32 || val < math.MinInt32 {
		// 超出 int32 时记录错误与调用栈以便定位，但仍按截断后的值构造。
		//nolint:logcheck // Should not be reached.
		klog.Errorf("value: %d overflows int32\n%s\n", val, debug.Stack())
	}
	return IntOrString{Type: Int, IntVal: int32(val)}
}

// FromInt32 用 int32 值创建 IntOrString。
func FromInt32(val int32) IntOrString {
	return IntOrString{Type: Int, IntVal: val}
}

// FromString 用字符串值创建 IntOrString。
func FromString(val string) IntOrString {
	return IntOrString{Type: String, StrVal: val}
}

// Parse 解析给定字符串，若能转换为 int32 则以整数形式保存，否则作为字符串保存。
func Parse(val string) IntOrString {
	i, err := strconv.ParseInt(val, 10, 32)
	if err != nil {
		return FromString(val)
	}
	return FromInt32(int32(i))
}

// UnmarshalJSON 实现 json.Unmarshaller 接口。
// 以引号开头的值按字符串解码，否则按整数解码，与 JSON 的常见写法保持一致。
func (intstr *IntOrString) UnmarshalJSON(value []byte) error {
	if value[0] == '"' {
		intstr.Type = String
		return json.Unmarshal(value, &intstr.StrVal)
	}
	intstr.Type = Int
	return json.Unmarshal(value, &intstr.IntVal)
}

// UnmarshalCBOR 解析 CBOR 值：先尝试字符串，失败再尝试整数，
// 与 UnmarshalJSON 的宽松语义保持一致。
func (intstr *IntOrString) UnmarshalCBOR(value []byte) error {
	if err := cbor.Unmarshal(value, &intstr.StrVal); err == nil {
		intstr.Type = String
		return nil
	}

	if err := cbor.Unmarshal(value, &intstr.IntVal); err != nil {
		return err
	}

	intstr.Type = Int
	return nil
}

// String 返回字符串值；若存储的是整数，则返回其十进制表示；nil 接收者返回 "<nil>"。
func (intstr *IntOrString) String() string {
	if intstr == nil {
		return "<nil>"
	}
	if intstr.Type == String {
		return intstr.StrVal
	}
	return strconv.Itoa(intstr.IntValue())
}

// IntValue 在类型为 Int 时返回 IntVal；若为 String，则尝试转换为 int，
// 解析失败时返回 0。
func (intstr *IntOrString) IntValue() int {
	if intstr.Type == String {
		i, _ := strconv.Atoi(intstr.StrVal)
		return i
	}
	return int(intstr.IntVal)
}

// MarshalJSON 实现 json.Marshaller 接口：按内部类型输出整数或字符串。
func (intstr IntOrString) MarshalJSON() ([]byte, error) {
	switch intstr.Type {
	case Int:
		return json.Marshal(intstr.IntVal)
	case String:
		return json.Marshal(intstr.StrVal)
	default:
		return []byte{}, fmt.Errorf("impossible IntOrString.Type")
	}
}

// MarshalCBOR 按内部类型将 IntOrString 编码为 CBOR。
func (intstr IntOrString) MarshalCBOR() ([]byte, error) {
	switch intstr.Type {
	case Int:
		return cbor.Marshal(intstr.IntVal)
	case String:
		return cbor.Marshal(intstr.StrVal)
	default:
		return nil, fmt.Errorf("impossible IntOrString.Type")
	}
}

// OpenAPISchemaType 供 kube-openapi 生成器在构建本类型的 OpenAPI 规范时使用。
//
// See: https://github.com/kubernetes/kube-openapi/tree/master/pkg/generators
func (IntOrString) OpenAPISchemaType() []string { return []string{"string"} }

// OpenAPISchemaFormat 供 kube-openapi 生成器在构建 OpenAPI 规范时使用。
func (IntOrString) OpenAPISchemaFormat() string { return "int-or-string" }

// OpenAPIV3OneOfTypes 供 kube-openapi 生成器在构建 OpenAPI v3 规范时使用，
// 声明该值可以是整数或字符串。
func (IntOrString) OpenAPIV3OneOfTypes() []string { return []string{"integer", "string"} }

// ValueOrDefault 在 intOrPercent 为 nil 时返回默认值，否则返回原值。
func ValueOrDefault(intOrPercent *IntOrString, defaultValue IntOrString) *IntOrString {
	if intOrPercent == nil {
		return &defaultValue
	}
	return intOrPercent
}

// GetScaledValueFromIntOrPercent 用于替代 GetValueFromIntOrPercent。
// 它返回 IntOrString 的缩放值：百分比字符串按 total 缩放；
// 整数按原值返回；非数字或缺少 '%' 后缀的字符串则返回错误。
func GetScaledValueFromIntOrPercent(intOrPercent *IntOrString, total int, roundUp bool) (int, error) {
	if intOrPercent == nil {
		return 0, errors.New("nil value for IntOrString")
	}
	value, isPercent, err := getIntOrPercentValueSafely(intOrPercent)
	if err != nil {
		return 0, fmt.Errorf("invalid value for IntOrString: %v", err)
	}
	if isPercent {
		// 百分比按 total 缩放；roundUp 控制向上或向下取整。
		if roundUp {
			value = int(math.Ceil(float64(value) * (float64(total)) / 100))
		} else {
			value = int(math.Floor(float64(value) * (float64(total)) / 100))
		}
	}
	return value, nil
}

// GetValueFromIntOrPercent 已由 GetScaledValueFromIntOrPercent 取代。
// 旧实现把所有整数当作数值、把所有字符串（无论是否带百分号）都当作百分比。
// Deprecated
func GetValueFromIntOrPercent(intOrPercent *IntOrString, total int, roundUp bool) (int, error) {
	if intOrPercent == nil {
		return 0, errors.New("nil value for IntOrString")
	}
	value, isPercent, err := getIntOrPercentValue(intOrPercent)
	if err != nil {
		return 0, fmt.Errorf("invalid value for IntOrString: %v", err)
	}
	if isPercent {
		if roundUp {
			value = int(math.Ceil(float64(value) * (float64(total)) / 100))
		} else {
			value = int(math.Floor(float64(value) * (float64(total)) / 100))
		}
	}
	return value, nil
}

// getIntOrPercentValue 是遗留实现，只应被 GetValueFromIntOrPercent 调用。
// 更正确的实现是 getIntOrPercentValueSafely。
func getIntOrPercentValue(intOrStr *IntOrString) (int, bool, error) {
	switch intOrStr.Type {
	case Int:
		return intOrStr.IntValue(), false, nil
	case String:
		// 旧的宽松语义：直接剥离所有 '%' 后按数字解析，因此 "50foo" 之外的
		// 非百分比字符串也会被当作百分比处理。
		s := strings.Replace(intOrStr.StrVal, "%", "", -1)
		v, err := strconv.Atoi(s)
		if err != nil {
			return 0, false, fmt.Errorf("invalid value %q: %v", intOrStr.StrVal, err)
		}
		return int(v), true, nil
	}
	return 0, false, fmt.Errorf("invalid type: neither int nor percentage")
}

// getIntOrPercentValueSafely 是严格实现：只有以 '%' 结尾的字符串才视为百分比，
// 其余字符串一律报错，从而避免把名称误当数字。
func getIntOrPercentValueSafely(intOrStr *IntOrString) (int, bool, error) {
	switch intOrStr.Type {
	case Int:
		return intOrStr.IntValue(), false, nil
	case String:
		isPercent := false
		s := intOrStr.StrVal
		if strings.HasSuffix(s, "%") {
			isPercent = true
			s = strings.TrimSuffix(intOrStr.StrVal, "%")
		} else {
			return 0, false, fmt.Errorf("invalid type: string is not a percentage")
		}
		v, err := strconv.Atoi(s)
		if err != nil {
			return 0, false, fmt.Errorf("invalid value %q: %v", intOrStr.StrVal, err)
		}
		return int(v), isPercent, nil
	}
	return 0, false, fmt.Errorf("invalid type: neither int nor percentage")
}
