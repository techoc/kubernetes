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

// value 与 oldValue 的签名遵循该包的校验函数约定：大多数校验函数会用到旧值，
// 但 required 语义与更新无关，故两个 context/operation/oldValue 参数均忽略。
func RequiredValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value != zero {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredPointer 校验指定指针非 nil。
func RequiredPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	// 注意这里判断的是指针本身，而不是其指向的值：*value 为 nil 指针也应报错。
	if value != nil {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredSlice 校验指定切片非空。
func RequiredSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	// 空 slice 与 nil slice 视为等价，都是"未设置"。
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// RequiredMap 校验指定 map 非空。
func RequiredMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "")}
}

// ForbiddenValue 校验指定值为其类型的零值。
// 类型。
func ForbiddenValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value == zero {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenPointer 校验指定指针为 nil。
func ForbiddenPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value == nil {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenSlice 校验指定切片为空。
func ForbiddenSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	if len(value) == 0 {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// ForbiddenMap 校验指定 map 为空。
func ForbiddenMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) == 0 {
		return nil
	}
	return field.ErrorList{field.Forbidden(fldPath, "")}
}

// OptionalValue 校验值非类型零值；语义同 RequiredValue，但调用方应将错误解释为未指定可选值。
func OptionalValue[T comparable](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	var zero T
	if *value != zero {
		return nil
	}
	// 与 RequiredValue 共用 Required 错误码，但附加说明以便区分"未指定"与"必填缺失"。
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalPointer 校验指针非 nil；语义同 RequiredPointer，但错误表示未指定可选值。
func OptionalPointer[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ *T) field.ErrorList {
	if value != nil {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalSlice 校验切片非空；语义同 RequiredSlice，但错误表示未指定可选值。
func OptionalSlice[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ []T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}

// OptionalMap 校验 map 非空；语义同 RequiredMap，但错误表示未指定可选值。
func OptionalMap[K comparable, T any](_ context.Context, _ operation.Operation, fldPath *field.Path, value, _ map[K]T) field.ErrorList {
	if len(value) > 0 {
		return nil
	}
	return field.ErrorList{field.Required(fldPath, "optional value was not specified")}
}
