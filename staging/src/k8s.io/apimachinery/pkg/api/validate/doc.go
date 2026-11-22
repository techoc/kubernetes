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

// Package validate holds API validation functions which are designed for use
// with the k8s.io/code-generator/cmd/validation-gen tool.  Each validation
// function has a similar fingerprint:
//
//	func <Name>(ctx context.Context,
//	            op operation.Operation,
//	            fldPath *field.Path,
//	            value, oldValue <nilable type>,
//	            <other args...>) field.ErrorList
//
// The value and oldValue arguments will always be a nilable type.  If the
// original value was a string, these will be a *string.  If the original value
// was a slice or map, these will be the same slice or map type.
//
// For a CREATE operation, the oldValue will always be nil.  For an UPDATE
// operation, either value or oldValue may be nil, e.g. when adding or removing
// a value in a list-map.  Validators which care about UPDATE operations should
// look at the opCtx argument to know which operation is being executed.
//
// Tightened validation (also known as ratcheting validation) is supported by
// defining a new validation function. For example:
//
//	func TightenedMaxLength(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
//	  if oldValue != nil && len(MaxLength(ctx, op, fldPath, oldValue, nil)) > 0 {
//	    // old value is not valid, so this value skips the tightened validation
//	    return nil
//	  }
//	  return MaxLength(ctx, op, fldPath, value, nil)
//	}
//
// In general, we cannot distinguish a non-specified slice or map from one that
// is specified but empty.  Validators should not rely on nil values, but use
// len() instead.
//
// 中文说明：本包为 validation-gen 代码生成器提供可组合的校验原语。所有函数遵循
// 统一签名，以便生成器按字段类型自动选择实现；指针参数模拟“是否设置”，
// CREATE 时 oldValue 为 nil，UPDATE 时 value/oldValue 均可能为 nil（如 list-map 的增删）。
package validate
