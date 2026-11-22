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

// GetFieldFunc is a function that extracts a field from a type and returns a
// nilable value.
//
// 用于从结构体中取出待校验字段；返回类型可以是值或指针。
type GetFieldFunc[Tstruct any, Tfield any] func(*Tstruct) Tfield

// Subfield validates a subfield of a struct against a validator function. If
// the value of the subfield is the same as the previous value, as per the
// equiv function, then no validation is performed.
//
// The equiv function can be called with nil arguments in the case of nilable
// fields.
//
// The fldPath passed to the validator includes the subfield name.
//
// 当操作是 Update 且新旧子字段相等时直接跳过校验，这是 ratcheting 的关键：
// 历史对象中已存在的“不合法”值不会因为一次无关更新而突然报错。
func Subfield[Tstruct any, Tfield any](
	ctx context.Context, op operation.Operation, fldPath *field.Path,
	newStruct, oldStruct *Tstruct,
	fldName string, getField GetFieldFunc[Tstruct, Tfield],
	equiv MatchFunc[Tfield],
	validator ValidateFunc[Tfield],
) field.ErrorList {
	var errs field.ErrorList
	newVal := getField(newStruct)
	var oldVal Tfield
	// oldStruct 可能为 nil（创建场景），此时 oldVal 保持零值。
	if oldStruct != nil {
		oldVal = getField(oldStruct)
	}
	// 仅更新且值未变时短路；创建/删除仍需完整校验。
	if op.Type == operation.Update && oldStruct != nil && equiv(newVal, oldVal) {
		return nil
	}
	// 错误路径带上子字段名，保证报错定位到具体字段。
	errs = append(errs, validator(ctx, op, fldPath.Child(fldName), newVal, oldVal)...)
	return errs
}
