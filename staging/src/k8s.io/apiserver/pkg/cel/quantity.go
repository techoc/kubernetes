/*
Copyright 2023 The Kubernetes Authors.

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

// Package cel 提供 Kubernetes 专用类型在 CEL 中的表示与扩展函数。
// 这里的类型（Quantity、IP、CIDR、Format、URL、Semver 等）在表达式求值时
// 以 ref.Val 形式出现，它们的成本估算与转换语义直接影响准入策略的安全性。
package cel

import (
	"fmt"
	"reflect"

	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/checker/decls"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
	"k8s.io/apimachinery/pkg/api/resource"
)

var (
	QuantityObject    = decls.NewObjectType("kubernetes.Quantity")
	quantityTypeValue = types.NewTypeValue("kubernetes.Quantity")
	QuantityType      = cel.ObjectType("kubernetes.Quantity")
)

// Quantity 提供 resource.Quantity 的 CEL 表示。
//
// 内嵌 *resource.Quantity，因此可以直接复用其比较与字符串化逻辑；
// CEL 侧看到的类型名是 kubernetes.Quantity。
type Quantity struct {
	*resource.Quantity
}

// ConvertToNative 支持转换回 *resource.Quantity 或字符串表示；
// 其它目标类型返回错误而不是静默截断。
func (d Quantity) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if reflect.TypeOf(d.Quantity).AssignableTo(typeDesc) {
		return d.Quantity, nil
	}
	if reflect.TypeOf("").AssignableTo(typeDesc) {
		return d.Quantity.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'Quantity' to '%v'", typeDesc)
}

func (d Quantity) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case quantityTypeValue:
		return d
	case types.TypeType:
		return quantityTypeValue
	default:
		return types.NewErr("type conversion error from '%s' to '%s'", quantityTypeValue, typeVal)
	}
}

// Equal 实现 CEL 的等值语义：底层 quantity 相等即视为相等。
// 类型不匹配时返回 no-such-overload 错误，与 CEL 内建类型保持一致。
func (d Quantity) Equal(other ref.Val) ref.Val {
	otherDur, ok := other.(Quantity)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(d.Quantity.Equal(*otherDur.Quantity))
}

func (d Quantity) Type() ref.Type {
	return quantityTypeValue
}

func (d Quantity) Value() interface{} {
	return d.Quantity
}
