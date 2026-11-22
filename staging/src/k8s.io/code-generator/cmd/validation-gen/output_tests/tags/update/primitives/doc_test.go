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

package primitives

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestUpdateTags 穷举值类型、指针、结构体（可比与不可比）、自定义类型上的
// update 约束语义：NoSet 禁止“未设置→已设置”，NoUnset 禁止反向，
// NoModify 只禁止已设置值之间变化；零值即“未设置”。
func TestUpdateTags(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// baseStruct 全零值，作为每次比较的“未设置”基线。
	baseStruct := UpdateTestStruct{}

	// String NoSet
	//
	// 从空串到非空视为设置，被 NoSet 拦截。
	old := baseStruct
	old.StringNoSet = "" // unset

	new := baseStruct
	new.StringNoSet = "value"

	st.Value(&new).OldValue(&old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	st.Value(&old).OldValue(&old).ExpectValid()

	// String NoUnset
	//
	// 首次设置允许，但从非空清空会被拒绝。
	oldWithValue := baseStruct
	oldWithValue.StringNoUnset = "value"

	newUnset := baseStruct
	newUnset.StringNoUnset = ""

	// Can set initially (empty to non-empty)
	st.Value(&oldWithValue).OldValue(&baseStruct).ExpectValid()

	// Cannot unset (non-empty to empty)
	st.Value(&newUnset).OldValue(&oldWithValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// String NoModify
	//
	// NoModify 允许设置与清空，只拦截两个非空值之间的变化。
	oldEmpty := baseStruct
	withValue := baseStruct
	withValue.StringNoModify = "value"
	modified := baseStruct
	modified.StringNoModify = "different"

	// Can set initially (empty to non-empty)
	st.Value(&withValue).OldValue(&oldEmpty).ExpectValid()

	// Can unset (non-empty to empty)
	st.Value(&oldEmpty).OldValue(&withValue).ExpectValid()

	// Cannot modify (non-empty to different non-empty)
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// String Fully Restricted
	//
	// 三种约束叠加后，任何表意上的变化都会被拒绝。
	oldEmpty = baseStruct
	withValue = baseStruct
	withValue.StringFullyRestricted = "value"
	modified = baseStruct
	modified.StringFullyRestricted = "different"

	// Cannot set (NoSet)
	st.Value(&withValue).OldValue(&oldEmpty).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// Cannot unset (NoUnset)
	st.Value(&oldEmpty).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// Cannot modify (NoModify)
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// String Set-Once Pattern
	//
	// NoUnset+NoModify 的组合等价于“只能设置一次”。
	oldEmpty = baseStruct
	withValue = baseStruct
	withValue.StringSetOnce = "value"
	modified = baseStruct
	modified.StringSetOnce = "different"

	// Can set once (empty to non-empty)
	st.Value(&withValue).OldValue(&oldEmpty).ExpectValid()

	// Cannot unset (NoUnset)
	st.Value(&oldEmpty).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringSetOnce"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// Cannot modify (NoModify)
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringSetOnce"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Int NoModify
	//
	// 数值类型用零值判断“未设置”，0→10 允许。
	oldZero := baseStruct
	withValue = baseStruct
	withValue.IntNoModify = 10

	// Can transition from 0 to 10 (unset to set is allowed)
	st.Value(&withValue).OldValue(&oldZero).ExpectValid()

	// Cannot modify from one non-zero to another
	modified = baseStruct
	modified.IntNoModify = 20
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 其余数值宽度（int32/uint/float32/float64/byte）走同一套零值逻辑。
	// Int32 NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.Int32NoModify = 42

	// Can set initially
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// Cannot modify
	modified = baseStruct
	modified.Int32NoModify = 100
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("int32NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Uint NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.UintNoModify = 42

	// Can set initially
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// Cannot modify
	modified = baseStruct
	modified.UintNoModify = 100
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("uintNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Bool NoModify
	oldFalse := baseStruct
	withTrue := baseStruct
	withTrue.BoolNoModify = true

	// Can transition from false to true (unset to set)
	st.Value(&withTrue).OldValue(&oldFalse).ExpectValid()

	// Cannot modify back to false
	st.Value(&oldFalse).OldValue(&withTrue).ExpectValid() // This is allowed as it's set->unset

	// Float32 NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.Float32NoModify = 3.14

	// Can set initially
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// Cannot modify
	modified = baseStruct
	modified.Float32NoModify = 2.71
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("float32NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Float64 NoModify
	oldZero = baseStruct
	withValue = baseStruct
	withValue.Float64NoModify = 3.14

	// Can transition from 0.0 to 3.14
	st.Value(&withValue).OldValue(&oldZero).ExpectValid()

	// Cannot modify to different value
	modified = baseStruct
	modified.Float64NoModify = 2.71
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("float64NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Byte NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.ByteNoModify = 255

	// Can set initially
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// Cannot modify
	modified = baseStruct
	modified.ByteNoModify = 128
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("byteNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Struct NoModify
	//
	// 值类型结构体没有 nil，任何值（含零值）都视为“已设置”，
	// 因此从零值到非零值也算修改。
	withStruct := baseStruct
	withStruct.StructNoModify = TestStruct{StringField: "value", IntField: 42}

	modifiedStruct := baseStruct
	modifiedStruct.StructNoModify = TestStruct{StringField: "different", IntField: 100}

	// Cannot modify (struct fields are always set, never unset)
	st.Value(&modifiedStruct).OldValue(&withStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("structNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// NonComparable Struct NoModify - uses reflection
	// For non-pointer structs, even zero value is considered "set"
	// So any change is a modification
	//
	// 不可比较结构体必须用反射深比较，不能依赖 ==。
	old = baseStruct
	old.NonComparableStructNoModify = NonComparableStruct{} // zero value

	withValue = baseStruct
	withValue.NonComparableStructNoModify = NonComparableStruct{
		SliceField: []string{"a", "b"},
		IntField:   42,
	}

	// Cannot change from zero value to non-zero (both are "set")
	st.Value(&withValue).OldValue(&old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("nonComparableStructNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Also cannot modify between two non-zero values
	modified = baseStruct
	modified.NonComparableStructNoModify = NonComparableStruct{
		SliceField: []string{"c", "d"},
		IntField:   100,
	}
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("nonComparableStructNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Can keep the same value
	st.Value(&withValue).OldValue(&withValue).ExpectValid()

	// Pointer NoSet
	//
	// nil 表示未设置，指针的“设置/清空”判断基于 nil，而非指向的内容。
	withSet := baseStruct
	withSet.PointerNoSet = ptr.To("value")

	// Cannot set after creation (nil to non-nil)
	st.Value(&withSet).OldValue(&baseStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// Pointer NoUnset
	withPointer := baseStruct
	withPointer.PointerNoUnset = ptr.To("value")

	// Can set initially
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// Cannot unset (non-nil to nil)
	st.Value(&baseStruct).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// Pointer NoModify
	withPointer = baseStruct
	withPointer.PointerNoModify = ptr.To("value")

	modifiedPointer := baseStruct
	modifiedPointer.PointerNoModify = ptr.To("different")

	// Can set initially
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// Can unset (NoModify allows set/unset transitions)
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// Cannot modify content
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Pointer Fully Restricted
	withPointer = baseStruct
	withPointer.PointerFullyRestricted = ptr.To("value")

	modifiedPointer = baseStruct
	modifiedPointer.PointerFullyRestricted = ptr.To("different")

	// Cannot set (NoSet)
	st.Value(&withPointer).OldValue(&baseStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// Cannot unset (NoUnset)
	st.Value(&baseStruct).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// Cannot modify (NoModify)
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Int Pointer NoModify
	withPointer = baseStruct
	withPointer.IntPointerNoModify = ptr.To(42)

	modifiedPointer = baseStruct
	modifiedPointer.IntPointerNoModify = ptr.To(100)

	// Can set initially
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// Can unset
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// Cannot modify content
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Bool Pointer NoModify
	//
	// 指针指向 false 仍是“已设置”，因此 false→true 属于修改。
	falseVal := false
	trueVal := true

	withFalse := baseStruct
	withFalse.BoolPointerNoModify = &falseVal

	withTrue = baseStruct
	withTrue.BoolPointerNoModify = &trueVal

	// Can set initially
	st.Value(&withFalse).OldValue(&baseStruct).ExpectValid()

	// Cannot modify content
	st.Value(&withTrue).OldValue(&withFalse).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("boolPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Struct Pointer NoModify
	withPointer = baseStruct
	withPointer.StructPointerNoModify = &TestStruct{StringField: "value", IntField: 42}

	modifiedPointer = baseStruct
	modifiedPointer.StructPointerNoModify = &TestStruct{StringField: "different", IntField: 100}

	// Can set initially
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// Can unset
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// Cannot modify content
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("structPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Custom Type NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.CustomTypeNoModify = "custom-value"

	// Can set initially
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// Cannot modify
	modified = baseStruct
	modified.CustomTypeNoModify = "different-value"
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("customTypeNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// Custom Type NoSet
	old = baseStruct
	withValue = baseStruct
	withValue.CustomTypeNoSet = 42

	// Cannot set
	st.Value(&withValue).OldValue(&old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("customTypeNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
	})
}
