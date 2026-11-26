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

	// string NoSet
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

	// string NoUnset
	//
	// 首次设置允许，但从非空清空会被拒绝。
	oldWithValue := baseStruct
	oldWithValue.StringNoUnset = "value"

	newUnset := baseStruct
	newUnset.StringNoUnset = ""

	// 初始可设置（空到非空）
	st.Value(&oldWithValue).OldValue(&baseStruct).ExpectValid()

	// 不可取消设置（非空到空）
	st.Value(&newUnset).OldValue(&oldWithValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// string NoModify
	//
	// NoModify 允许设置与清空，只拦截两个非空值之间的变化。
	oldEmpty := baseStruct
	withValue := baseStruct
	withValue.StringNoModify = "value"
	modified := baseStruct
	modified.StringNoModify = "different"

	// 初始可设置（空到非空）
	st.Value(&withValue).OldValue(&oldEmpty).ExpectValid()

	// 可取消设置（非空到空）
	st.Value(&oldEmpty).OldValue(&withValue).ExpectValid()

	// 不可修改（非空到另一个非空）
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// string 完全受限
	//
	// 三种约束叠加后，任何表意上的变化都会被拒绝。
	oldEmpty = baseStruct
	withValue = baseStruct
	withValue.StringFullyRestricted = "value"
	modified = baseStruct
	modified.StringFullyRestricted = "different"

	// 不可设置（NoSet）
	st.Value(&withValue).OldValue(&oldEmpty).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// 不可取消设置（NoUnset）
	st.Value(&oldEmpty).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// 不可修改（NoModify）
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringFullyRestricted"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// string 一次性设置模式
	//
	// NoUnset+NoModify 的组合等价于“只能设置一次”。
	oldEmpty = baseStruct
	withValue = baseStruct
	withValue.StringSetOnce = "value"
	modified = baseStruct
	modified.StringSetOnce = "different"

	// 只能设置一次（空到非空）
	st.Value(&withValue).OldValue(&oldEmpty).ExpectValid()

	// 不可取消设置（NoUnset）
	st.Value(&oldEmpty).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringSetOnce"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// 不可修改（NoModify）
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("stringSetOnce"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// int NoModify
	//
	// 数值类型用零值判断“未设置”，0→10 允许。
	oldZero := baseStruct
	withValue = baseStruct
	withValue.IntNoModify = 10

	// 可以从 0 变为 10（未设置到已设置允许）
	st.Value(&withValue).OldValue(&oldZero).ExpectValid()

	// 不能从一个非零值改为另一个
	modified = baseStruct
	modified.IntNoModify = 20
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 其余数值宽度（int32/uint/float32/float64/byte）走同一套零值逻辑。
	// int32 NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.Int32NoModify = 42

	// 初始可设置
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// 不可修改
	modified = baseStruct
	modified.Int32NoModify = 100
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("int32NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// uint NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.UintNoModify = 42

	// 初始可设置
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// 不可修改
	modified = baseStruct
	modified.UintNoModify = 100
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("uintNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// bool NoModify
	oldFalse := baseStruct
	withTrue := baseStruct
	withTrue.BoolNoModify = true

	// 可以从 false 变为 true（未设置到已设置）
	st.Value(&withTrue).OldValue(&oldFalse).ExpectValid()

	// 不能改回 false
	st.Value(&oldFalse).OldValue(&withTrue).ExpectValid() // This is allowed as it's set->unset

	// float32 NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.Float32NoModify = 3.14

	// 初始可设置
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// 不可修改
	modified = baseStruct
	modified.Float32NoModify = 2.71
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("float32NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// float64 NoModify
	oldZero = baseStruct
	withValue = baseStruct
	withValue.Float64NoModify = 3.14

	// 可以从 0.0 变为 3.14
	st.Value(&withValue).OldValue(&oldZero).ExpectValid()

	// 不能改为不同的值
	modified = baseStruct
	modified.Float64NoModify = 2.71
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("float64NoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// byte NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.ByteNoModify = 255

	// 初始可设置
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// 不可修改
	modified = baseStruct
	modified.ByteNoModify = 128
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("byteNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// struct NoModify
	//
	// 值类型结构体没有 nil，任何值（含零值）都视为“已设置”，
	// 因此从零值到非零值也算修改。
	withStruct := baseStruct
	withStruct.StructNoModify = TestStruct{StringField: "value", IntField: 42}

	modifiedStruct := baseStruct
	modifiedStruct.StructNoModify = TestStruct{StringField: "different", IntField: 100}

	// 不可修改（结构体字段始终为已设置，不存在未设置状态）
	st.Value(&modifiedStruct).OldValue(&withStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("structNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 不可比较结构体的 NoModify——使用反射
	// 非指针结构体即使零值也视为“已设置”
	// 所以任何变化都算修改
	//
	// 不可比较结构体必须用反射深比较，不能依赖 ==。
	old = baseStruct
	old.NonComparableStructNoModify = NonComparableStruct{} // zero value

	withValue = baseStruct
	withValue.NonComparableStructNoModify = NonComparableStruct{
		SliceField: []string{"a", "b"},
		IntField:   42,
	}

	// 不能从零值改为非零值（两者都算“已设置”）
	st.Value(&withValue).OldValue(&old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("nonComparableStructNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 两个非零值之间也不能修改
	modified = baseStruct
	modified.NonComparableStructNoModify = NonComparableStruct{
		SliceField: []string{"c", "d"},
		IntField:   100,
	}
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("nonComparableStructNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 可以保持不变
	st.Value(&withValue).OldValue(&withValue).ExpectValid()

	// 指针 NoSet
	//
	// nil 表示未设置，指针的“设置/清空”判断基于 nil，而非指向的内容。
	withSet := baseStruct
	withSet.PointerNoSet = ptr.To("value")

	// 创建后不可设置（nil 到非 nil）
	st.Value(&withSet).OldValue(&baseStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// 指针 NoUnset
	withPointer := baseStruct
	withPointer.PointerNoUnset = ptr.To("value")

	// 初始可设置
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// 不能取消设置（非 nil 到 nil）
	st.Value(&baseStruct).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoUnset"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// 指针 NoModify
	withPointer = baseStruct
	withPointer.PointerNoModify = ptr.To("value")

	modifiedPointer := baseStruct
	modifiedPointer.PointerNoModify = ptr.To("different")

	// 初始可设置
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// 可取消设置（NoModify 允许设置/取消设置的转换）
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// 不可修改内容
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 指针完全受限
	withPointer = baseStruct
	withPointer.PointerFullyRestricted = ptr.To("value")

	modifiedPointer = baseStruct
	modifiedPointer.PointerFullyRestricted = ptr.To("different")

	// 不可设置（NoSet）
	st.Value(&withPointer).OldValue(&baseStruct).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be set once created").WithOrigin("update"),
	})

	// 不可取消设置（NoUnset）
	st.Value(&baseStruct).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be cleared once set").WithOrigin("update"),
	})

	// 不可修改（NoModify）
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("pointerFullyRestricted"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// int 指针 NoModify
	withPointer = baseStruct
	withPointer.IntPointerNoModify = ptr.To(42)

	modifiedPointer = baseStruct
	modifiedPointer.IntPointerNoModify = ptr.To(100)

	// 初始可设置
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// 可取消设置
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// 不可修改内容
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// bool 指针 NoModify
	//
	// 指针指向 false 仍是“已设置”，因此 false→true 属于修改。
	falseVal := false
	trueVal := true

	withFalse := baseStruct
	withFalse.BoolPointerNoModify = &falseVal

	withTrue = baseStruct
	withTrue.BoolPointerNoModify = &trueVal

	// 初始可设置
	st.Value(&withFalse).OldValue(&baseStruct).ExpectValid()

	// 不可修改内容
	st.Value(&withTrue).OldValue(&withFalse).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("boolPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// struct 指针 NoModify
	withPointer = baseStruct
	withPointer.StructPointerNoModify = &TestStruct{StringField: "value", IntField: 42}

	modifiedPointer = baseStruct
	modifiedPointer.StructPointerNoModify = &TestStruct{StringField: "different", IntField: 100}

	// 初始可设置
	st.Value(&withPointer).OldValue(&baseStruct).ExpectValid()

	// 可取消设置
	st.Value(&baseStruct).OldValue(&withPointer).ExpectValid()

	// 不可修改内容
	st.Value(&modifiedPointer).OldValue(&withPointer).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("structPointerNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 自定义类型 NoModify
	old = baseStruct
	withValue = baseStruct
	withValue.CustomTypeNoModify = "custom-value"

	// 初始可设置
	st.Value(&withValue).OldValue(&old).ExpectValid()

	// 不可修改
	modified = baseStruct
	modified.CustomTypeNoModify = "different-value"
	st.Value(&modified).OldValue(&withValue).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("customTypeNoModify"), nil, "field cannot be modified once set").WithOrigin("update"),
	})

	// 自定义类型 NoSet
	old = baseStruct
	withValue = baseStruct
	withValue.CustomTypeNoSet = 42

	// 不可设置
	st.Value(&withValue).OldValue(&old).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("customTypeNoSet"), nil, "field cannot be set once created").WithOrigin("update"),
	})
}
