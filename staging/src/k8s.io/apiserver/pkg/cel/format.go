/*
Copyright 2024 The Kubernetes Authors.

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

// 本文件实现 CEL 的 NamedFormat 类型：把 OpenAPI 的 format 语义带入表达式，
// 支持类型检查、比较与成本估算。

package cel

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

var (
	// FormatObject 用于类型检查器（checker）阶段声明 NamedFormat 类型。
	FormatObject = decls.NewObjectType("kubernetes.NamedFormat")
	// FormatType 用于运行时类型系统，两者必须保持名称一致，否则检查与运行阶段行为不一致。
	FormatType = cel.ObjectType("kubernetes.NamedFormat")
)

// Format 是 Kubernetes format 在 CEL 中的表示。
// 它把 OpenAPI 中的 format（如 dns1123Label）包装成可在表达式中比较和传递的值。
// 之所以不直接暴露为字符串，是为了保留格式语义以便做类型检查与成本估算。
type Format struct {
	Name         string
	ValidateFunc func(string) []string

	// 正则字符串（或等价估算值）的长度，用于成本估算。
	// 成本估算需要知道校验正则的规模，否则无法为 format() 调用定价。
	MaxRegexSize int
}

// ConvertToNative 不支持转换为任意 Go 原生类型：Format 是 CEL 专有值，
// 跨出表达式边界后没有对应表示。
func (d Format) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	return nil, fmt.Errorf("type conversion error from 'Format' to '%v'", typeDesc)
}

// ConvertToType 实现 ref.Val：支持转换为自身类型及类型对象（type 值），其余一律报错。
func (d Format) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case FormatType:
		return d
	case types.TypeType:
		return FormatType
	default:
		return types.NewErr("type conversion error from '%s' to '%s'", FormatType, typeVal)
	}
}

// Equal 按 Name 判等，符合 CEL 对自定义对象“值相等”的预期；
// 类型不匹配时返回标准重载错误，便于用户定位表达式问题。
func (d Format) Equal(other ref.Val) ref.Val {
	otherDur, ok := other.(Format)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(d.Name == otherDur.Name)
}

// Type 返回 CEL 运行时类型标识。
func (d Format) Type() ref.Type {
	return FormatType
}

// Value 返回底层 Go 值，供 CEL 运行时在需要原生表示时使用。
func (d Format) Value() interface{} {
	return d
}
