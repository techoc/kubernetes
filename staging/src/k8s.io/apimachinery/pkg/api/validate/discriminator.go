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

package validate

import (
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// DiscriminatedRule 定义针对某个具体 discriminator 取值应用的校验。
type DiscriminatedRule[Tfield any, Tdisc comparable] struct {
	// Value 是触发该规则的 discriminator 值。
	Value Tdisc
	// Validation 是匹配时执行的校验；为 nil 表示该取值不做校验。
	Validation ValidateFunc[Tfield]
}

// Discriminated 根据 discriminator 的取值校验成员字段。
// 它按顺序遍历规则，应用第一条与 discriminator 匹配的规则；
// 若没有规则匹配，则应用 defaultValidation（若已提供）。
//
// 它实现了 ratcheting：若操作为 Update，且 discriminator 与字段值
// （通过 equiv 比较）均未变化，则跳过校验。
//
// 对于可为 nil 的字段，equiv 可能以 nil 参数被调用。
func Discriminated[Tfield any, Tdisc comparable, Tstruct any](ctx context.Context, op operation.Operation, structPath *field.Path,
	obj, oldObj *Tstruct, fieldName string, getMemberValue func(*Tstruct) Tfield, getDiscriminator func(*Tstruct) Tdisc,
	equiv MatchFunc[Tfield], defaultValidation ValidateFunc[Tfield], rules []DiscriminatedRule[Tfield, Tdisc],
) field.ErrorList {
	value := getMemberValue(obj)
	discriminator := getDiscriminator(obj)
	var oldValue Tfield
	var oldDiscriminator Tdisc

	if oldObj != nil {
		oldValue = getMemberValue(oldObj)
		oldDiscriminator = getDiscriminator(oldObj)
	}

	// Ratcheting：更新时若 discriminator 与值都未变，重复校验没有意义，
	// 直接放行以避免已持久化的非法数据阻塞无关更新。
	if op.Type == operation.Update && oldObj != nil && discriminator == oldDiscriminator && equiv(value, oldValue) {
		return nil
	}

	fldPath := structPath.Child(fieldName)
	for _, rule := range rules {
		if rule.Value == discriminator {
			if rule.Validation == nil {
				return nil
			}
			return rule.Validation(ctx, op, fldPath, value, oldValue)
		}
	}

	if defaultValidation != nil {
		return defaultValidation(ctx, op, fldPath, value, oldValue)
	}

	return nil
}
