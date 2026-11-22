/*
Copyright The Kubernetes Authors.

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

package customvalidation

import (
	"context"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// These functions are structural markers, not real rules: each emits one error
// at the path it is called with, so tests can assert where the generated
// traversal invokes custom validation.
//
// 函数名遵循 ValidateCustom_<Type>[_<Field>] 约定；所有函数都只返回一个固定错误，
// 不含真实校验逻辑，测试通过错误路径与消息定位调用点。

// ValidateCustom_Struct 对应类型级 +k8s:customValidation，验证整对象校验的调用点。
func ValidateCustom_Struct(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *Struct) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_Struct")}
}

// ValidateCustom_Struct_StringField 对应 Struct.StringField 的字段级自定义校验。
func ValidateCustom_Struct_StringField(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *string) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_Struct_StringField")}
}

// ValidateCustom_Struct_MaxLengthField 验证自定义校验与声明式 +k8s:maxLength 共存：
// 同一字段会同时产生两类错误。
func ValidateCustom_Struct_MaxLengthField(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *string) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_Struct_MaxLengthField")}
}

// ValidateCustom_StringType 作用于 typedef 类型，凡是使用该类型的字段都会经遍历触发。
func ValidateCustom_StringType(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *StringType) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_StringType")}
}

// ValidateCustom_OtherStruct_StringField 验证嵌套结构体字段的自定义校验调用点。
func ValidateCustom_OtherStruct_StringField(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *string) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_OtherStruct_StringField")}
}

// ValidateCustom_OptionStruct_StringField 验证 +k8s:ifEnabled 门控的自定义校验：
// 特性开关关闭时不应被调用。
func ValidateCustom_OptionStruct_StringField(_ context.Context, _ operation.Operation, fldPath *field.Path, _, _ *string) field.ErrorList {
	return field.ErrorList{field.Invalid(fldPath, nil, "ValidateCustom_OptionStruct_StringField")}
}
