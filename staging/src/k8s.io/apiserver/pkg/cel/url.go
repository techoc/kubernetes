/*
Copyright 2022 The Kubernetes Authors.

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

package cel

import (
	"fmt"
	"net/url"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

// URL provides a CEL representation of a URL.
// 包装 *url.URL 以便实现 ref.Val，使 CEL 表达式可以直接持有并比较 URL
// 值，而不必退化为字符串比较。
type URL struct {
	*url.URL
}

var (
	// URLObject is the legacy decls-based type used by older callers.
	URLObject = decls.NewObjectType("kubernetes.URL")
	// typeValue is the runtime ref.Type identity for kubernetes.URL.
	typeValue = types.NewTypeValue("kubernetes.URL")
	// URLType is the cel-go type handle used when declaring variables.
	URLType = cel.ObjectType("kubernetes.URL")
)

// ConvertToNative implements ref.Val.ConvertToNative.
// 支持转换到 *url.URL 或 string，其他目标类型直接报错，避免静默产生零值。
func (d URL) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if reflect.TypeOf(d.URL).AssignableTo(typeDesc) {
		return d.URL, nil
	}
	if reflect.TypeOf("").AssignableTo(typeDesc) {
		return d.URL.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'URL' to '%v'", typeDesc)
}

// ConvertToType implements ref.Val.ConvertToType.
// 只接受自身类型与 type 类型；其他转换一律返回 CEL 错误值。
func (d URL) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case typeValue:
		return d
	case types.TypeType:
		return typeValue
	}
	return types.NewErr("type conversion error from '%s' to '%s'", typeValue, typeVal)
}

// Equal implements ref.Val.Equal.
// 按字符串形式比较而非结构体比较：URL 的语义相等应以序列化结果为准，
// 这样不同构造方式得到的等价 URL 也能判定相等。
func (d URL) Equal(other ref.Val) ref.Val {
	otherDur, ok := other.(URL)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(d.URL.String() == otherDur.URL.String())
}

// Type implements ref.Val.Type.
func (d URL) Type() ref.Type {
	return typeValue
}

// Value implements ref.Val.Value.
// 返回底层 *url.URL，供需要 Go 原生值的调用方使用。
func (d URL) Value() interface{} {
	return d.URL
}
