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

package transitions

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 item 级别的不可变转换：对 listType=map 的列表，
// 同一 key 的元素字段被修改时应报 immutable 错误，且按 key 关联新旧元素。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	old := &Struct{
		ListField: []Item{
			{Key1: "a", StringField: "s1"},
			{Key1: "b", StringField: "s2"},
			{Key1: "c", StringField: "s3"},
		},
	}

	new := &Struct{
		ListField: []Item{
			{Key1: "a", StringField: "changed"},
			{Key1: "b", StringField: "changed"},
			{Key1: "c", StringField: "changed"},
		},
	}

	// 三个元素的字段都被修改；错误路径应按元素索引或子字段定位。
	st.Value(new).OldValue(old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("listField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listField").Index(1).Child("stringField"), nil, "immutable").WithOrigin("immutable"),
	})

	// 旧列表为空时，新增元素不受不可变约束，但已存在的元素修改仍应报错。
	st.Value(new).OldValue(&Struct{ListField: []Item{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("listField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listField").Index(1).Child("stringField"), nil, "immutable").WithOrigin("immutable"),
	})

	// 未被约束的 key "c" 可以独立修改。
	// 验证 "c" 可以独立变化
	st.Value(&Struct{
		ListField: []Item{
			{Key1: "a", StringField: "s1"},
			{Key1: "b", StringField: "s2"},
			{Key1: "c", StringField: "changed"},
		},
	}).OldValue(old).ExpectValid()
}
