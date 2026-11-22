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

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// RequiredValue verifies that the specified value is not the zero-value for
// its type.
//
// value 与 oldValue 的签名遵循该包的校验函数约定：大多数校验函数会用到旧值，
// 但 required 语义与更新无关，故两个 context/operation/oldValue 参数均忽略。
func RequiredValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value != zero {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredPointer verifies that the specified pointer is not nil.
func RequiredPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	// 注意这里判断的是指针本身，而不是其指向的值：*value 为 nil 指针也应报错。
	if value != nil {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredSlice verifies that the specified slice is not empty.
func RequiredSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	// 空 slice 与 nil slice 视为等价，都是"未设置"。
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredMap verifies that the specified map is not empty.
func RequiredMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// ForbiddenValue verifies that the specified value is the zero-value for its
// type.
func ForbiddenValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value == zero {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenPointer verifies that the specified pointer is nil.
func ForbiddenPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenSlice verifies that the specified slice is empty.
func ForbiddenSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	if len(value) == 0 {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenMap verifies that the specified map is empty.
func ForbiddenMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) == 0 {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// OptionalValue verifies that the specified value is not the zero-value for
// its type. This is identical to RequiredValue, but the caller should treat an
// error here as an indication that the optional value was not specified.
func OptionalValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value != zero {
		return nil
	}
	// 与 RequiredValue 共用 Required 错误码，但附加说明以便区分"未指定"与"必填缺失"。
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalPointer verifies that the specified pointer is not nil. This is
// identical to RequiredPointer, but the caller should treat an error here as an
// indication that the optional value was not specified.
func OptionalPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value != nil {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalSlice verifies that the specified slice is not empty. This is
// identical to RequiredSlice, but the caller should treat an error here as an
// indication that the optional value was not specified.
func OptionalSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalMap verifies that the specified map is not empty. This is identical
// to RequiredMap, but the caller should treat an error here as an indication that
// the optional value was not specified.
func OptionalMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}
