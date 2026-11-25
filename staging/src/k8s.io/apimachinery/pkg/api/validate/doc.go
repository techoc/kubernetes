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

//	func <Name>(ctx context.Context,
//	            op operation.Operation,
//	            fldPath *field.Path,
//	            value, oldValue <nilable type>,
//	            <other args...>) field.ErrorList
//
//	func TightenedMaxLength(ctx context.Context, op operation.Operation, fldPath *field.Path, value, oldValue *string) field.ErrorList {
//	  if oldValue != nil && len(MaxLength(ctx, op, fldPath, oldValue, nil)) > 0 {
//	    // 旧值非法，因此当前值跳过收紧后的校验。
//	    return nil
//	  }
//	  return MaxLength(ctx, op, fldPath, value, nil)
//	}
//
// 本包为 validation-gen 代码生成器提供可组合的校验原语。所有函数遵循
// 统一签名，以便生成器按字段类型自动选择实现；指针参数模拟“是否设置”，
// CREATE 时 oldValue 为 nil，UPDATE 时 value/oldValue 均可能为 nil（如 list-map 的增删）。
package validate
