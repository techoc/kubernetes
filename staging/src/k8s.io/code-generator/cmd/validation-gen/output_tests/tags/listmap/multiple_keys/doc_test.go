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

package multiplekeys

import (
	"testing"

	field "k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestUniqueness 验证复合 key 列表的唯一性：只有所有 key 字段都相同的元素
// 才被视为重复；三种列表声明形式（普通/typedef 元素/typedef 列表）行为一致。
func TestUniqueness(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		ListField: []OtherStruct{
			{"key1", 1, "one"}, // unique
			{"key2", 2, "two"}, // dup
			{"key2", 2, "two"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key1", 1, "one"}, // unique
			{"key2", 2, "two"}, // dup
			{"key2", 2, "two"},
		},
		TypedefField: ListType{
			{"key1", 1, "one"}, // unique
			{"key2", 2, "two"}, // dup
			{"key2", 2, "two"},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Duplicate(field.NewPath("listField").Index(2), nil),
		field.Duplicate(field.NewPath("listTypedefField").Index(2), nil),
		field.Duplicate(field.NewPath("typedefField").Index(2), nil),
	})
}

// TestUpdateCorrelation 验证更新时按 key（而非下标）关联新旧元素：
// 仅重排顺序视为未变化；key 不同或内容变化才会触发 immutable 错误。
func TestUpdateCorrelation(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	structA1 := Struct{
		ListField: []OtherStruct{
			{"key1", 1, "one"},
			{"key2", 2, "two"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key1", 1, "one"},
			{"key2", 2, "two"},
		},
		TypedefField: ListType{
			{"key1", 1, "one"},
			{"key2", 2, "two"},
		},
	}

	// Same data, different order.
	structA2 := Struct{
		ListField: []OtherStruct{
			{"key2", 2, "two"},
			{"key1", 1, "one"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key2", 2, "two"},
			{"key1", 1, "one"},
		},
		TypedefField: ListType{
			{"key2", 2, "two"},
			{"key1", 1, "one"},
		},
	}

	// Different data.
	structB := Struct{
		ListField: []OtherStruct{
			{"key3", 3, "THREE"},
			{"key1", 1, "ONE"},
			{"key2", 2, "TWO"},
		},
		ListTypedefField: []OtherTypedefStruct{
			{"key3", 3, "THREE"},
			{"key1", 1, "ONE"},
			{"key2", 2, "TWO"},
		},
		TypedefField: ListType{
			{"key3", 3, "THREE"},
			{"key1", 1, "ONE"},
			{"key2", 2, "TWO"},
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

// TestRatcheting 验证列表重排不会重新触发校验：新旧列表语义等价时，
// 逐元素校验（eachVal）的结果应被跳过。
func TestRatcheting(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	struct1 := Struct{
		ListComparableField: []OtherStruct{
			{"key1", 1, "one"},
			{"key2", 2, "two"},
		},
		ListNonComparableField: []NonComparableStruct{
			{"key1", 1, []string{"one"}},
			{"key2", 2, []string{"two"}},
		},
	}

	// Same data, different order.
	struct2 := Struct{
		ListComparableField: []OtherStruct{
			{"key2", 2, "two"},
			{"key1", 1, "one"},
		},
		ListNonComparableField: []NonComparableStruct{
			{"key2", 2, []string{"two"}},
			{"key1", 1, []string{"one"}},
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

// TestItemWithPtrKey 验证 item 匹配对指针 key 的处理：
// 非 nil 且值相等才算匹配，nil key 与任何值都不匹配；混合指针/值 key 时
// 需全部匹配才命中。
func TestItemWithPtrKey(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		ListPtrKeyField: []PtrKeyStruct{
			{Key1Field: ptr.To("target-ptr"), Key2Field: 42, DataField: "match"},
			{Key1Field: ptr.To("target-ptr"), Key2Field: 99, DataField: "no match, int differs"},
			{Key1Field: ptr.To("other"), Key2Field: 42, DataField: "no match, string differs"},
			{Key1Field: nil, Key2Field: 42, DataField: "no match, nil string"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`listPtrKeyField[0]`: {
			"item ListPtrKeyField[key1Field=target-ptr,key2Field=42]",
		},
	})

	st.Value(&Struct{
		ListPtrKeyField: []PtrKeyStruct{
			{Key1Field: ptr.To("other"), Key2Field: 42, DataField: "d1"},
			{Key1Field: nil, Key2Field: 99, DataField: "d2"},
		},
	}).ExpectValid()

	st.Value(&Struct{
		ListMixedPtrKeyField: []MixedPtrKeyStruct{
			{StringPtrKey: ptr.To("target-ptr"), StringKey: "target", DataField: "match"},
			{StringPtrKey: ptr.To("target-ptr"), StringKey: "other", DataField: "no match"},
			{StringPtrKey: nil, StringKey: "target", DataField: "no match"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		`listMixedPtrKeyField[0]`: {
			"item ListMixedPtrKeyField",
		},
	})
}
