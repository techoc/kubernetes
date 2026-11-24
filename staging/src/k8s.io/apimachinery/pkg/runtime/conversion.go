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

// 本文件定义通用类型与 Go 结构体之间的转换规则，主要用于把 URL 查询参数映射到 API 请求结构体。
package runtime

import (
	"fmt"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/conversion"
)

// 注册 metav1 通用字段选择器转换规则。metadata.name 与 metadata.namespace 可以转换为内部字段表示；集群级资源使用 namespace 空值是合法的，指定具体 namespace 则不会匹配集群级对象。
func DefaultMetaV1FieldSelectorConversion(label, value string) (string, string, error) {
	switch label {
	case "metadata.name":
		return label, value, nil
	case "metadata.namespace":
		return label, value, nil
	default:
		return "", "", fmt.Errorf("%q is not a known field selector: only %q, %q", label, "metadata.name", "metadata.namespace")
	}
}

// 把查询参数切片的第一个元素转换为 string。参数不存在时写入空字符串；存在多个值时只使用第一个。
func Convert_Slice_string_To_string(in *[]string, out *string, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = ""
		return nil
	}
	*out = (*in)[0]
	return nil
}

// 把查询参数切片的第一个元素按十进制转换为 int。参数不存在时写入 0；格式不合法或超出平台 int 范围时返回解析错误。
func Convert_Slice_string_To_int(in *[]string, out *int, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = 0
		return nil
	}
	str := (*in)[0]
	i, err := strconv.Atoi(str)
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// 把查询参数的第一个值转换为 bool。参数缺失、值为 false（忽略大小写）或 0 时为 false；其他任何值（包括显式空字符串）均为 true，以保持 Kubernetes 历史查询参数语义。
func Convert_Slice_string_To_bool(in *[]string, out *bool, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = false
		return nil
	}
	// 显式空字符串（例如 ?flag=）按 true 处理，以保持 Kubernetes 查询参数的历史行为。
	switch {
	case (*in)[0] == "0", strings.EqualFold((*in)[0], "false"):
		*out = false
	default:
		*out = true
	}
	return nil
}

// 将查询参数转换为 *bool。参数缺失时返回指向 false 的非 nil 指针，使调用方能够获得确定的布尔值；显式 false/0 返回 false 指针，其余值返回 true 指针。
func Convert_Slice_string_To_Pointer_bool(in *[]string, out **bool, s conversion.Scope) error {
	if len(*in) == 0 {
		boolVar := false
		*out = &boolVar
		return nil
	}
	switch {
	case (*in)[0] == "0", strings.EqualFold((*in)[0], "false"):
		boolVar := false
		*out = &boolVar
	default:
		boolVar := true
		*out = &boolVar
	}
	return nil
}

// string_to_int64 按十进制解析 int64。统一使用该辅助函数，确保所有整数转换都拒绝非十进制输入。
func string_to_int64(in string) (int64, error) {
	return strconv.ParseInt(in, 10, 64)
}

// 将字符串转换为 int64。输入指针为 nil 时写入 0；非 nil 时按十进制解析，非法格式或溢出会返回错误。
func Convert_string_To_int64(in *string, out *int64, s conversion.Scope) error {
	if in == nil {
		*out = 0
		return nil
	}
	i, err := string_to_int64(*in)
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// 将查询参数切片的第一个值按十进制转换为 int64。切片为空时写入 0，解析失败时返回错误。
func Convert_Slice_string_To_int64(in *[]string, out *int64, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = 0
		return nil
	}
	i, err := string_to_int64((*in)[0])
	if err != nil {
		return err
	}
	*out = i
	return nil
}

// 将字符串转换为 *int64。输入指针为 nil 时输出 nil，以区分字段未设置与显式设置为 0；否则解析后返回新值的指针。
func Convert_string_To_Pointer_int64(in *string, out **int64, s conversion.Scope) error {
	if in == nil {
		*out = nil
		return nil
	}
	i, err := string_to_int64(*in)
	if err != nil {
		return err
	}
	*out = &i
	return nil
}

// 将查询参数切片的第一个值转换为 *int64。参数不存在时输出 nil，存在时解析并返回指向结果的指针。
func Convert_Slice_string_To_Pointer_int64(in *[]string, out **int64, s conversion.Scope) error {
	if len(*in) == 0 {
		*out = nil
		return nil
	}
	i, err := string_to_int64((*in)[0])
	if err != nil {
		return err
	}
	*out = &i
	return nil
}

// 向 Scheme 注册查询参数切片到 string、int、bool 和 int64 的转换函数，使反射式对象转换可以复用这些规则。指针类型转换按需单独注册，因为并非所有 API 字段都适用。
func RegisterStringConversions(s *Scheme) error {
	if err := s.AddConversionFunc((*[]string)(nil), (*string)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_string(a.(*[]string), b.(*string), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*int)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_int(a.(*[]string), b.(*int), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*bool)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_bool(a.(*[]string), b.(*bool), scope)
	}); err != nil {
		return err
	}
	if err := s.AddConversionFunc((*[]string)(nil), (*int64)(nil), func(a, b interface{}, scope conversion.Scope) error {
		return Convert_Slice_string_To_int64(a.(*[]string), b.(*int64), scope)
	}); err != nil {
		return err
	}
	return nil
}
