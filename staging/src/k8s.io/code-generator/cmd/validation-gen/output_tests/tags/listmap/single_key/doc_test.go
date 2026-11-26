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

package singlekey

import (
	"testing"

	field "k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestUniqueness 验证单键 map 列表的唯一性：重复 key 的第二个元素报
// Duplicate 错误，字段直接使用、typedef 与嵌套 typedef 三种形态行为一致。
func TestUniqueness(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		ListField: []OtherStruct{
			{"key1", "one"},
			{"key2", "two"},
			{"key2", "two"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key1", "one"},
			{"key2", "two"},
			{"key2", "two"},
		},
		TypedefField: ListType{
			{"key1", "one"},
			{"key2", "two"},
			{"key2", "two"},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Duplicate(field.NewPath("listField").Index(2), nil),
		field.Duplicate(field.NewPath("listTypedefField").Index(2), nil),
		field.Duplicate(field.NewPath("typedefField").Index(2), nil),
	})
}

// TestUpdateCorrelation 验证更新时按 key 关联新旧元素：仅顺序变化视为无变化，
// 真正修改了某个 key 对应元素的值才会触发 immutable 错误。
func TestUpdateCorrelation(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	structA1 := Struct{
		ListField: []OtherStruct{
			{"key1", "one"},
			{"key2", "two"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key1", "one"},
			{"key2", "two"},
		},
		TypedefField: ListType{
			{"key1", "one"},
			{"key2", "two"},
		},
	}

	// 数据相同，顺序不同。
	structA2 := Struct{
		ListField: []OtherStruct{
			{"key2", "two"},
			{"key1", "one"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key2", "two"},
			{"key1", "one"},
		},
		TypedefField: ListType{
			{"key2", "two"},
			{"key1", "one"},
		},
	}

	// 数据不同。
	structB := Struct{
		ListField: []OtherStruct{
			{"key3", "THREE"},
			{"key1", "ONE"},
			{"key2", "TWO"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key3", "THREE"},
			{"key1", "ONE"},
			{"key2", "TWO"},
		},
		TypedefField: ListType{
			{"key3", "THREE"},
			{"key1", "ONE"},
			{"key2", "TWO"},
		},
	}

	st.Value(&structA1).OldValue(&structA2).ExpectValid()

	st.Value(&structA2).OldValue(&structA1).ExpectValid()

	st.Value(&structA1).OldValue(&structB).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("listField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listField").Index(1), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listTypedefField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listTypedefField").Index(1), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("typedefField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("typedefField").Index(1), nil, "immutable").WithOrigin("immutable"),
	})

	st.Value(&structB).OldValue(&structA1).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("listField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listField").Index(1), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listField").Index(2), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listTypedefField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listTypedefField").Index(1), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("listTypedefField").Index(2), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("typedefField").Index(0), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("typedefField").Index(1), nil, "immutable").WithOrigin("immutable"),
		field.Invalid(field.NewPath("typedefField").Index(2), nil, "immutable").WithOrigin("immutable"),
	})
}

// TestRatcheting 验证 map 列表的 ratcheting：新旧列表数据相同（仅顺序不同）
// 时，validateFalse 错误被抑制，可比较与不可比较元素都如此。
func TestRatcheting(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	struct1 := Struct{
		ListComparableField: []OtherStruct{
			{"key1", "one"},
			{"key2", "two"},
		},
		ListNonComparableField: []NonComparableStruct{
			{"key1", ptr.To("one")},
			{"key2", ptr.To("two")},
		},
	}

	// 数据相同，顺序不同。
	struct2 := Struct{
		ListComparableField: []OtherStruct{
			{"key2", "two"},
			{"key1", "one"},
		},
		ListNonComparableField: []NonComparableStruct{
			{"key2", ptr.To("two")},
			{"key1", ptr.To("one")},
		},
	}
	st.Value(&struct1).ExpectValidateFalseByPath(map[string][]string{
		"listComparableField[0]":    {"field Struct.ListComparableField[*]"},
		"listComparableField[1]":    {"field Struct.ListComparableField[*]"},
		"listNonComparableField[0]": {"field Struct.ListNonComparableField[*]"},
		"listNonComparableField[1]": {"field Struct.ListNonComparableField[*]"},
	})
	st.Value(&struct1).OldValue(&struct2).ExpectValid()
	st.Value(&struct2).OldValue(&struct1).ExpectValid()
}

// TestUniquenessPtrKey 验证指针键的唯一性：值相同的不同指针视为重复 key，
// 多个 nil 指针也视为重复，因为 map 语义按键值而非地址比较。
func TestUniquenessPtrKey(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		ListPtrKeyField: []PtrKeyStruct{
			{ptr.To("key1"), "one"},
			{ptr.To("key2"), "two"},
			{ptr.To("key2"), "three"}, // duplicate key
			{nil, "four"},
			{nil, "five"}, // duplicate nil key
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Duplicate(field.NewPath("listPtrKeyField").Index(2), nil),
		field.Duplicate(field.NewPath("listPtrKeyField").Index(4), nil),
	})
}
