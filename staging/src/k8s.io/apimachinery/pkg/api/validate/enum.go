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

package validate

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Enum 校验给定值是否属于枚举取值集合。
// 同时会考虑“选项启用/禁用时生效的排除规则”：只要某值的任一排除规则命中，
// 该值在本次校验中即被排除。
//
// 参数说明：value 为待校验值的指针（nil 视为未设置，直接通过）；
// validValues 为完整枚举集合；exclusions 为条件排除规则。
func Enum[T ~string](_ context.Context, op operation.Operation, fldPath *field.Path, value, _ *T, validValues sets.Set[T], exclusions []EnumExclusion[T]) field.ErrorList {
	if value == nil {
		return nil
	}
	// 先判定条件排除；选项未定义属于策略配置错误，应作为内部错误暴露。
	excluded, err := isExcluded(op, exclusions, *value)
	if err != nil {
		return field.ErrorList{field.InternalError(fldPath, err)}
	}
	if !validValues.Has(*value) || excluded {
		// 报错时需要列出当前实际可用的值（已剔除被排除项），
		// 否则用户会看到自己无法使用的“受支持值”。
		supported, err := supportedValues(op, validValues, exclusions)
		if err != nil {
			return field.ErrorList{field.InternalError(fldPath, err)}
		}
		return field.ErrorList{field.NotSupported[T](fldPath, *value, supported)}
	}
	return nil
}

// supportedValues 返回排序后的受支持值列表。
// 被排除的枚举值不会出现在结果中，保证错误信息与实际可用集合一致。
func supportedValues[T ~string](op operation.Operation, values sets.Set[T], exclusions []EnumExclusion[T]) ([]T, error) {
	res := make([]T, 0, len(values))
	for key := range values {
		excluded, err := isExcluded(op, exclusions, key)
		if err != nil {
			return nil, err
		}
		if excluded {
			continue
		}
		res = append(res, key)
	}
	// 集合遍历顺序不确定，排序以保证错误信息稳定、可断言。
	slices.Sort(res)
	return res, nil
}

// EnumExclusion 表示单条枚举排除规则。
type EnumExclusion[T ~string] struct {
	// Value 指定被条件排除的枚举值。
	Value T
	// ExcludeWhen 决定排除条件。
	// 为 true 时，选项存在则排除该值；
	// 为 false 时，选项不存在则排除该值。
	ExcludeWhen bool
	// Option 是控制该排除的选项（特性开关）名称。
	Option string
}

// isExcluded 判断 value 是否命中任一排除规则。
// 选项未定义时返回错误，而不是静默视为禁用——那会掩盖策略配置遗漏。
func isExcluded[T ~string](op operation.Operation, exclusions []EnumExclusion[T], value T) (bool, error) {
	for _, rule := range exclusions {
		on, defined := op.HasOption(rule.Option)
		if !defined {
			return false, fmt.Errorf("undefined validation option %q", rule.Option)
		}
		if rule.Value == value && rule.ExcludeWhen == on {
			return true, nil
		}
	}
	return false, nil
}
