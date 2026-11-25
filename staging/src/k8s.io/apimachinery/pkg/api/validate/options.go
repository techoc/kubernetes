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
	"fmt"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// IfOption 仅在 option 与 enabled 均为 true，或均为 false 时调用校验函数。
// enabled 表示期望状态：实际状态与期望一致时才校验。
//
// 操作未定义该 option 属于编程错误，返回 InternalError，而非静默跳过。
//
// 从而跳过校验。
func IfOption[T any](ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue T,
	optionName string, enabled bool, validator func(context.Context, operation.Operation, *field.Path, T, T) field.ErrorList,
) field.ErrorList {
	on, defined := op.HasOption(optionName)
	if !defined {
		return field.ErrorList{field.InternalError(fldPath, fmt.Errorf("undefined validation option %q", optionName))}
	}
	if on == enabled {
		return validator(ctx, op, fldPath, value, oldValue)
	}
	return nil
}
