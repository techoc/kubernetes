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

package cel

import (
	"fmt"
	"reflect"

	"github.com/blang/semver/v4"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/common/types"
	"github.com/google/cel-go/common/types/ref"
)

var (
	// SemverType 是 CEL 中暴露的对象类型名，策略中可用 kubernetes.Semver 声明变量。
	SemverType = cel.ObjectType("kubernetes.Semver")
)

// Semver provdes a CEL representation of a [semver.Version].
//
// 内嵌 semver.Version 使比较运算（EQ/LT 等）直接复用库实现；这里只需补齐
// cel-go 的 ref.Val 接口。
type Semver struct {
	semver.Version
}

// ConvertToNative 支持转换为 semver.Version 或字符串；其他目标类型返回错误，
// 避免 CEL 求值时静默得到零值。
func (v Semver) ConvertToNative(typeDesc reflect.Type) (interface{}, error) {
	if reflect.TypeOf(v.Version).AssignableTo(typeDesc) {
		return v.Version, nil
	}
	if reflect.TypeOf("").AssignableTo(typeDesc) {
		return v.Version.String(), nil
	}
	return nil, fmt.Errorf("type conversion error from 'Semver' to '%v'", typeDesc)
}

// ConvertToType 实现 CEL 类型转换：转成自身类型返回原值，转成 type 返回类型对象。
func (v Semver) ConvertToType(typeVal ref.Type) ref.Val {
	switch typeVal {
	case SemverType:
		return v
	case types.TypeType:
		return SemverType
	default:
		return types.NewErr("type conversion error from '%s' to '%s'", SemverType, typeVal)
	}
}

// Equal 仅在对方也是 Semver 时比较版本；类型不匹配返回 CEL 重载错误而非 false，
// 以便策略作者发现误用。
func (v Semver) Equal(other ref.Val) ref.Val {
	otherDur, ok := other.(Semver)
	if !ok {
		return types.MaybeNoSuchOverloadErr(other)
	}
	return types.Bool(v.Version.EQ(otherDur.Version))
}

// Type 返回 CEL 类型标识。
func (v Semver) Type() ref.Type {
	return SemverType
}

// Value 返回底层 semver.Version，供 CEL 运行时取原始值。
func (v Semver) Value() interface{} {
	return v.Version
}
