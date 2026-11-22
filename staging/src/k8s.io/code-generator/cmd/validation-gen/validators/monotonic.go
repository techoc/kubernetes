/*
Copyright 2025 The Kubernetes Authors.

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

package validators

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/code-generator/cmd/validation-gen/util"
	"k8s.io/gengo/v2/codetags"
	"k8s.io/gengo/v2/types"
)

func init() {
	RegisterTagValidator(&monotonicTagValidator{})
}

const (
	monotonicTagName = "k8s:monotonic"
)

var (
	// Monotonic 是 apimachinery 中实现“更新时不得减小”的运行时校验函数。
	monotonicValidator = types.Name{Package: libValidationPkg, Name: "Monotonic"}
)

// monotonicTagValidator 处理 +k8s:monotonic：生成的校验会比较新旧值，
// 保证字段在 update 时单调不减。
type monotonicTagValidator struct{}

// Init 无需配置，保持接口一致。
func (v *monotonicTagValidator) Init(_ Config) {}

// TagName 返回标签名。
func (v *monotonicTagValidator) TagName() string {
	return monotonicTagName
}

// ValidScopes 可用于类型或字段；类型作用域常用于同类型字段的批量约束。
func (v *monotonicTagValidator) ValidScopes() sets.Set[Scope] {
	return sets.New(ScopeType, ScopeField)
}

// GetValidations 校验目标必须是整数类型（含别名与指针），并登记运行时函数调用。
// 指针先解引用再判断，因为 +k8s:monotonic 的语义针对数值本身而非可空性。
func (v *monotonicTagValidator) GetValidations(context Context, tag codetags.Tag) (Validations, error) {
	t := util.NonPointer(util.NativeType(context.Type))
	if !types.IsInteger(t) {
		return Validations{}, fmt.Errorf("must be an integer type (got %s)", rootTypeString(context.Type, t))
	}

	result := Validations{}
	result.AddFunction(Function(monotonicTagName, DefaultFlags, monotonicValidator))
	return result, nil
}

// Docs 返回标签文档；monotonic 目前为 Alpha 稳定性。
func (v *monotonicTagValidator) Docs() TagDoc {
	return TagDoc{
		Tag:            monotonicTagName,
		StabilityLevel: TagStabilityLevelAlpha,
		Scopes:         sets.List(v.ValidScopes()),
		Description:    "ensures that a field's value never decreases on update",
	}
}
