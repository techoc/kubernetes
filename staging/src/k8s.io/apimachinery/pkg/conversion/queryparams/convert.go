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

package queryparams

import (
	"fmt"
	"net/url"
	"reflect"
	"strings"
)

// Marshaler 将对象转换为查询参数的字符串表示。
//
// 实现此接口的类型会优先于反射展开被序列化，用于 Duration 等需要特殊
// 文本表示的值类型。
type Marshaler interface {
	MarshalQueryParameter() (string, error)
}

// Unmarshaler 将字符串表示还原为对象。
type Unmarshaler interface {
	UnmarshalQueryParameter(string) error
}

// jsonTag 提取结构体字段的 JSON 标签名以及是否带 omitempty。被标记为 "-"
// 的字段返回空名，调用方据此跳过该字段；无标签同样视为不参与查询参数。
func jsonTag(field reflect.StructField) (string, bool) {
	structTag, exists := field.Tag.Lookup("json")
	if !exists || len(structTag) == 0 {
		return "", false
	}
	parts := strings.Split(structTag, ",")
	tag := parts[0]
	if tag == "-" {
		tag = ""
	}
	omitempty := false
	parts = parts[1:]
	for _, part := range parts {
		if part == "omitempty" {
			omitempty = true
			break
		}
	}
	return tag, omitempty
}

func isPointerKind(kind reflect.Kind) bool {
	return kind == reflect.Pointer
}

func isStructKind(kind reflect.Kind) bool {
	return kind == reflect.Struct
}

// isValueKind 判断类型是否可作为一个标量查询参数值直接格式化。注意这里
// 不包含 uintptr，也不包含复合类型。
func isValueKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.String, reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8,
		reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32,
		reflect.Float64, reflect.Complex64, reflect.Complex128:
		return true
	default:
		return false
	}
}

// zeroValue 用 DeepEqual 比较零值，因此对 slice/map 等不可比较类型也成立。
// 注意：nil 与非 nil 的空 slice 并不相等，omitempty 对二者行为不同。
func zeroValue(value reflect.Value) bool {
	return reflect.DeepEqual(reflect.Zero(value.Type()).Interface(), value.Interface())
}

// customMarshalValue 尝试让值通过 MarshalQueryParameter 自行编码。值接收者
// 和指针接收者都要支持：反射拿到的 field 不可寻址时，只能依赖值接收者实现。
func customMarshalValue(value reflect.Value) (reflect.Value, bool) {
	// 只有实现了自定义查询编码器的值才继续处理。
	if !value.CanInterface() {
		return reflect.Value{}, false
	}

	marshaler, ok := value.Interface().(Marshaler)
	if !ok {
		if !isPointerKind(value.Kind()) && value.CanAddr() {
			marshaler, ok = value.Addr().Interface().(Marshaler)
			if !ok {
				return reflect.Value{}, false
			}
		} else {
			return reflect.Value{}, false
		}
	}

	// 不要在 nil 指针上调用方法；nil 指针直接编码为空串，由 omitempty 决定是否输出。
	if isPointerKind(value.Kind()) && zeroValue(value) {
		return reflect.ValueOf(""), true
	}

	// 取自定义编码结果。
	v, err := marshaler.MarshalQueryParameter()
	if err != nil {
		return reflect.Value{}, false
	}
	return reflect.ValueOf(v), true
}

// addParam 把单个值格式化为字符串加入查询参数。fmt 的 "<nil>" 被转成空串，
// 因为 nil 指针在 URL 中应表现为“参数存在但值为空”，而不是字面量 "<nil>"。
func addParam(values url.Values, tag string, omitempty bool, value reflect.Value) {
	if omitempty && zeroValue(value) {
		return
	}
	val := ""
	iValue := fmt.Sprintf("%v", value.Interface())

	if iValue != "<nil>" {
		val = iValue
	}
	values.Add(tag, val)
}

// addListOfParams 为列表的每个元素追加同名参数（重复 key），而不是把列表
// 编码成逗号分隔的单个值，这是 url.Values 的标准多值语义。
func addListOfParams(values url.Values, tag string, omitempty bool, list reflect.Value) {
	for i := 0; i < list.Len(); i++ {
		addParam(values, tag, omitempty, list.Index(i))
	}
}

// Convert 按 JSON tag 的名称把对象转换为 url.Values。
// 只序列化顶层的简单值、数组和切片；嵌套结构体与 map 会被忽略，因为
// 查询参数没有表示这些复合类型的统一语义。
//
// 唯一支持的输入是指针（或指向结构体的 interface）；nil 返回空参数集而不是
// 错误，便于调用方无条件下传。map 没有对应语义，因此被静默忽略而不是报错。
func Convert(obj interface{}) (url.Values, error) {
	result := url.Values{}
	if obj == nil {
		return result, nil
	}
	var sv reflect.Value
	switch reflect.TypeOf(obj).Kind() {
	case reflect.Pointer, reflect.Interface:
		sv = reflect.ValueOf(obj).Elem()
	default:
		return nil, fmt.Errorf("expecting a pointer or interface")
	}
	st := sv.Type()
	if !isStructKind(st.Kind()) {
		return nil, fmt.Errorf("expecting a pointer to a struct")
	}

	// 逐个字段转换。
	convertStruct(result, st, sv)

	return result, nil
}

// convertStruct 递归展开结构体。非 nil 指针字段会强制关闭 omitempty，
// 因为“显式设置了指针”本身就是要表达的意图（即使指向零值）。
func convertStruct(result url.Values, st reflect.Type, sv reflect.Value) {
	for i := 0; i < st.NumField(); i++ {
		field := sv.Field(i)
		tag, omitempty := jsonTag(st.Field(i))
		// 没有标签（含 json:"-"）的字段不参与查询参数。
		if len(tag) == 0 {
			continue
		}
		ft := field.Type()

		kind := ft.Kind()
		if isPointerKind(kind) {
			ft = ft.Elem()
			kind = ft.Kind()
			if !field.IsNil() {
				field = reflect.Indirect(field)
				// 显式非 nil 的指针必须输出，即使指向零值。
				omitempty = false
			}
		}

		switch {
		case isValueKind(kind):
			addParam(result, tag, omitempty, field)
		case kind == reflect.Array || kind == reflect.Slice:
			// 只处理标量元素列表；结构体/指针切片无定义行为，跳过。
			if isValueKind(ft.Elem().Kind()) {
				addListOfParams(result, tag, omitempty, field)
			}
		case isStructKind(kind) && !(zeroValue(field) && omitempty):
			// 结构体优先尝试自定义序列化，否则递归展开其字段。
			if marshalValue, ok := customMarshalValue(field); ok {
				addParam(result, tag, omitempty, marshalValue)
			} else {
				convertStruct(result, ft, field)
			}
		}
	}
}
