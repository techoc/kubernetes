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
	"k8s.io/apimachinery/pkg/api/validate/constraints"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Monotonic 校验整数在更新时没有减小（允许持平）。
// 仅对 Update 操作生效：Create 无旧值可比，Delete 不关心数值变化。
// 任一侧为 nil（字段未设置）时跳过校验，避免把"未设置"误判为 0。
func Monotonic[T constraints.Integer](_ context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *T) field.ErrorList {
	// 只有更新操作才存在"单调不回退"的语义。
	if op.Type != operation.Update {
		return nil
	}

	// 缺少新值或旧值时无法比较，交由 required 等其他校验规则处理。
	if value == nil || oldValue == nil {
		return nil
	}

	if *value < *oldValue {
		return field.ErrorList{field.Invalid(fldPath, *value, fmt.Sprintf("may not be decreased from %v", *oldValue)).WithOrigin("monotonic")}
	}

	return nil
}
