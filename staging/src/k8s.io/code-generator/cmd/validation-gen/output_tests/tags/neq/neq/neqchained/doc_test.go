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

package neqchained

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 +k8s:neq 链式作用于结构体、切片、map key/value 与类型定义时
// 的错误路径，以及旧值相同情况下的 ratcheting。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法取值（不在禁止集合中）以及空集合/未设置都应通过。
	st.Value(&Struct{
		StructField:          InnerStruct{StringField: "allowed"},
		StructPtrField:       &InnerStruct{StringField: "valid"},
		StringSliceField:     []string{"allowed", "valid"},
		StringMapField:       map[string]string{"k1": "allowed", "k2": "valid"},
		StringMapKeyField:    map[string]string{"allowed-key": "v1", "valid-key": "v2"},
		ValidatedSliceField:  []string{"allowed", "valid"},
		ValidatedStructField: ValidatedInnerStruct{StringField: "allowed"},
	}).ExpectValid()

	// 测试空集合与未设置。
	st.Value(&Struct{
		StructField:          InnerStruct{},
		StructPtrField:       &InnerStruct{StringField: "valid"},
		StringSliceField:     []string{},
		StringMapField:       map[string]string{},
		StringMapKeyField:    map[string]string{},
		ValidatedSliceField:  []string{},
		ValidatedStructField: ValidatedInnerStruct{StringField: "allowed"},
	}).ExpectValid()

	// 测试非法值触发所有预期的校验错误
	// 每个被禁止的位置都应独立报错，索引/键路径用于精确定位。
	invalidStruct := &Struct{
		StructField:          InnerStruct{StringField: "disallowed-subfield"},
		StructPtrField:       &InnerStruct{StringField: "disallowed-subfield-ptr"},
		StringSliceField:     []string{"valid", "disallowed-slice", "disallowed-slice"},
		StringMapField:       map[string]string{"a": "disallowed-map-val", "b": "valid", "c": "disallowed-map-val"},
		StringMapKeyField:    map[string]string{"disallowed-key": "value", "allowed": "ok"},
		ValidatedSliceField:  []string{"valid", "disallowed-typedef", "disallowed-typedef"},
		ValidatedStructField: ValidatedInnerStruct{StringField: "disallowed-typedef-struct"},
	}

	st.Value(invalidStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("structField", "stringField"), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("structPtrField", "stringField"), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("stringSliceField").Index(1), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("stringSliceField").Index(2), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("stringMapField").Key("a"), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("stringMapField").Key("c"), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("stringMapKeyField"), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("validatedSliceField").Index(1), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("validatedSliceField").Index(2), nil, "").WithOrigin("neq"),
		field.Invalid(field.NewPath("validatedStructField", "stringField"), nil, "").WithOrigin("neq"),
	})

	// 测试 ratcheting 对已有非法值的放行
	st.Value(invalidStruct).OldValue(invalidStruct).ExpectValid()
}
