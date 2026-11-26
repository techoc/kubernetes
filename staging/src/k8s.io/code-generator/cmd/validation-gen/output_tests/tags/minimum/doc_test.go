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

package minimum

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestBasicStruct 验证非 optional/required 字段的 minimum=1 约束及 ratcheting。
func TestBasicStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值低于 minimum=1，应被拒绝。
	// 测试零值因低于 minimum=1 而被拒绝。
	st.Value(&BasicStruct{
		// 全零值
		IntPtrField:     ptr.To(0),
		UintPtrField:    ptr.To(uint(0)),
		TypedefPtrField: ptr.To(IntType(0)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("intPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int16Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int32Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int64Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("uintField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("uintPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("uint16Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("uint32Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("uint64Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("typedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("typedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 校验 ratcheting：新旧值相同（都非法）时不报错。
	// 测试 ratcheting：未变化的非法数据应放行。
	st.Value(&BasicStruct{
		IntPtrField:     ptr.To(0),
		UintPtrField:    ptr.To(uint(0)),
		TypedefPtrField: ptr.To(IntType(0)),
	}).OldValue(&BasicStruct{
		IntPtrField:     ptr.To(0),
		UintPtrField:    ptr.To(uint(0)),
		TypedefPtrField: ptr.To(IntType(0)),
	}).ExpectValid()

	// 值变得更小（从 0 改为 -1）时仍应报错。
	// 值发生变化后仍被拒绝。
	st.Value(&BasicStruct{
		IntField:        -1,
		IntPtrField:     ptr.To(-1),
		Int16Field:      -1,
		Int32Field:      -1,
		Int64Field:      -1,
		TypedefField:    IntType(-1),
		TypedefPtrField: ptr.To(IntType(-1)),
	}).OldValue(&BasicStruct{
		IntField:        0,
		IntPtrField:     ptr.To(0),
		Int16Field:      0,
		Int32Field:      0,
		Int64Field:      0,
		TypedefField:    IntType(0),
		TypedefPtrField: ptr.To(IntType(0)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("intPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int16Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int32Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("int64Field"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("typedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("typedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 满足 minimum=1 的值合法。
	// 测试满足 minimum=1 的值合法。
	st.Value(&BasicStruct{
		IntField:        1,
		IntPtrField:     ptr.To(1),
		Int16Field:      1,
		Int32Field:      1,
		Int64Field:      1,
		UintField:       1,
		Uint16Field:     1,
		Uint32Field:     1,
		Uint64Field:     1,
		UintPtrField:    ptr.To(uint(1)),
		TypedefField:    IntType(1),
		TypedefPtrField: ptr.To(IntType(1)),
	}).ExpectValid()
}

// TestOptionalStruct 验证 optional 字段的 minimum 行为：
// nil 指针短路校验，非 nil 的零值仍会被校验。
func TestOptionalStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 显式提供零值的 optional 指针仍会触发 minimum 校验。
	// 测试显式提供的 optional 指针零值仍受 minimum 校验。
	st.Value(&OptionalStruct{
		// 零值
		OptionalIntPtrField:     ptr.To(0),
		OptionalTypedefPtrField: ptr.To(IntType(0)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 省略 optional 指针字段（nil）时，optional 先短路，minimum 不会执行。
	// 测试省略 optional 指针字段（nil）会短路 minimum。
	st.Value(&OptionalStruct{
		// OptionalIntField 为零且 OptionalIntPtrField 为 nil，因此 optional
		// 会在 minimum 之前短路。
	}).ExpectValid()

	// 校验 ratcheting：新旧值相同（都非法）时不报错。
	// 测试 ratcheting：未变化的非法数据应放行。
	st.Value(&OptionalStruct{
		OptionalIntField:        -1,
		OptionalIntPtrField:     ptr.To(-1),
		OptionalTypedefField:    IntType(-1),
		OptionalTypedefPtrField: ptr.To(IntType(-1)),
	}).OldValue(&OptionalStruct{
		OptionalIntField:        -1,
		OptionalIntPtrField:     ptr.To(-1),
		OptionalTypedefField:    IntType(-1),
		OptionalTypedefPtrField: ptr.To(IntType(-1)),
	}).ExpectValid()

	// 值变得更小（从 -1 改为 -2）时仍应报错。
	// 值发生变化后仍被拒绝。
	st.Value(&OptionalStruct{
		OptionalIntField:        -2,
		OptionalIntPtrField:     ptr.To(-2),
		OptionalTypedefField:    IntType(-2),
		OptionalTypedefPtrField: ptr.To(IntType(-2)),
	}).OldValue(&OptionalStruct{
		OptionalIntField:        -1,
		OptionalIntPtrField:     ptr.To(-1),
		OptionalTypedefField:    IntType(-1),
		OptionalTypedefPtrField: ptr.To(IntType(-1)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalIntField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalTypedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 测试满足 minimum=1 的值合法。
	st.Value(&OptionalStruct{
		OptionalIntField:        1,
		OptionalIntPtrField:     ptr.To(1),
		OptionalTypedefField:    IntType(1),
		OptionalTypedefPtrField: ptr.To(IntType(1)),
	}).ExpectValid()

	// 测试低于 minimum 的非法值被拒绝。
	st.Value(&OptionalStruct{
		OptionalIntField:        -1,
		OptionalIntPtrField:     ptr.To(-1),
		OptionalTypedefField:    IntType(-1),
		OptionalTypedefPtrField: ptr.To(IntType(-1)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalIntField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalTypedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})
}

// TestRequiredStruct 验证 required 字段的 minimum 行为：
// 非指针零值触发 Required；非 nil 指针的零值触发 minimum。
func TestRequiredStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 显式提供指针零值时按 minimum 校验；非指针零值触发 required。
	// 测试显式提供的指针零值按 minimum 校验，
	// 而非指针零值触发 required 校验。
	st.Value(&RequiredStruct{
		// 零值
		RequiredIntField:        0,
		RequiredIntPtrField:     ptr.To(0),
		RequiredTypedefField:    IntType(0),
		RequiredTypedefPtrField: ptr.To(IntType(0)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Required(field.NewPath("requiredIntField"), ""),
		field.Invalid(field.NewPath("requiredIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Required(field.NewPath("requiredTypedefField"), ""),
		field.Invalid(field.NewPath("requiredTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 省略 required 指针（nil）时只报 Required，并短路 minimum。
	// 测试省略 required 指针字段（nil）只报 field.Required，并短路 minimum。
	st.Value(&RequiredStruct{
		// RequiredIntField 为零且 RequiredIntPtrField 为 nil。
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Required(field.NewPath("requiredIntField"), ""),
		field.Required(field.NewPath("requiredIntPtrField"), ""),
		field.Required(field.NewPath("requiredTypedefField"), ""),
		field.Required(field.NewPath("requiredTypedefPtrField"), ""),
	})

	// 校验 ratcheting：新旧值相同（都非法）时不报错。
	// 测试 ratcheting：未变化的非法数据应放行。
	st.Value(&RequiredStruct{
		RequiredIntField:        0,
		RequiredIntPtrField:     ptr.To(0),
		RequiredTypedefField:    IntType(0),
		RequiredTypedefPtrField: ptr.To(IntType(0)),
	}).OldValue(&RequiredStruct{
		RequiredIntField:        0,
		RequiredIntPtrField:     ptr.To(0),
		RequiredTypedefField:    IntType(0),
		RequiredTypedefPtrField: ptr.To(IntType(0)),
	}).ExpectValid()

	// 值变得更小（从 0 改为 -1）时仍应报错。
	// 值发生变化后仍被拒绝。
	st.Value(&RequiredStruct{
		RequiredIntField:        -1,
		RequiredIntPtrField:     ptr.To(-1),
		RequiredTypedefField:    IntType(-1),
		RequiredTypedefPtrField: ptr.To(IntType(-1)),
	}).OldValue(&RequiredStruct{
		RequiredIntField:        0,
		RequiredIntPtrField:     ptr.To(0),
		RequiredTypedefField:    IntType(0),
		RequiredTypedefPtrField: ptr.To(IntType(0)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("requiredIntField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredTypedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 满足 minimum=1 的值合法。
	// 测试满足 minimum=1 的值合法。
	st.Value(&RequiredStruct{
		RequiredIntField:        1,
		RequiredIntPtrField:     ptr.To(1),
		RequiredTypedefField:    IntType(1),
		RequiredTypedefPtrField: ptr.To(IntType(1)),
	}).ExpectValid()

	// 低于 minimum 的值不合法。
	// 测试低于 minimum 的非法值被拒绝。
	st.Value(&RequiredStruct{
		RequiredIntField:        -1,
		RequiredIntPtrField:     ptr.To(-1),
		RequiredTypedefField:    IntType(-1),
		RequiredTypedefPtrField: ptr.To(IntType(-1)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("requiredIntField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredIntPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredTypedefField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredTypedefPtrField"), nil, "").WithOrigin("minimum"),
	})
}

// TestNegativeMinimumStruct 验证负的 minimum（-10）：
// 零值合法，只有低于 -10 的值才报错。
func TestNegativeMinimumStruct(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值对 -10 合法；required 非指针零值仍触发 Required。
	// 测试 required 指针零值按负 minimum 校验，
	// 而非指针零值触发 required 校验。
	st.Value(&NegativeMinimumStruct{
		// 零值（对 -10 合法）
		NegativeMinimumPtrField:         ptr.To(0),
		OptionalNegativeMinimumPtrField: ptr.To(0),
		RequiredNegativeMinimumField:    0,
		RequiredNegativeMinimumPtrField: ptr.To(0),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Required(field.NewPath("requiredNegativeMinimumField"), ""),
	})

	// 边界值 -10 合法。
	// 测试负 minimum 边界值（-10）合法。
	st.Value(&NegativeMinimumStruct{
		NegativeMinimumField:            -10,
		NegativeMinimumPtrField:         ptr.To(-10),
		OptionalNegativeMinimumField:    -10,
		OptionalNegativeMinimumPtrField: ptr.To(-10),
		RequiredNegativeMinimumField:    -10,
		RequiredNegativeMinimumPtrField: ptr.To(-10),
	}).ExpectValid()

	// 校验 ratcheting：新旧值相同（都非法）时不报错。
	// 测试 ratcheting：未变化的非法数据应放行。
	st.Value(&NegativeMinimumStruct{
		NegativeMinimumField:            -11,
		NegativeMinimumPtrField:         ptr.To(-11),
		OptionalNegativeMinimumField:    -11,
		OptionalNegativeMinimumPtrField: ptr.To(-11),
		RequiredNegativeMinimumField:    -11,
		RequiredNegativeMinimumPtrField: ptr.To(-11),
	}).OldValue(&NegativeMinimumStruct{
		NegativeMinimumField:            -11,
		NegativeMinimumPtrField:         ptr.To(-11),
		OptionalNegativeMinimumField:    -11,
		OptionalNegativeMinimumPtrField: ptr.To(-11),
		RequiredNegativeMinimumField:    -11,
		RequiredNegativeMinimumPtrField: ptr.To(-11),
	}).ExpectValid()

	// 值变得更小（从 -11 改为 -12）时仍应报错。
	// 值发生变化后仍被拒绝。
	st.Value(&NegativeMinimumStruct{
		NegativeMinimumField:            -12,
		NegativeMinimumPtrField:         ptr.To(-12),
		OptionalNegativeMinimumField:    -12,
		OptionalNegativeMinimumPtrField: ptr.To(-12),
		RequiredNegativeMinimumField:    -12,
		RequiredNegativeMinimumPtrField: ptr.To(-12),
	}).OldValue(&NegativeMinimumStruct{
		NegativeMinimumField:            -11,
		NegativeMinimumPtrField:         ptr.To(-11),
		OptionalNegativeMinimumField:    -11,
		OptionalNegativeMinimumPtrField: ptr.To(-11),
		RequiredNegativeMinimumField:    -11,
		RequiredNegativeMinimumPtrField: ptr.To(-11),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("negativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("negativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalNegativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalNegativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredNegativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredNegativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
	})

	// 低于负 minimum（-10）的值不合法。
	// 测试低于负 minimum（-10）的非法值被拒绝。
	st.Value(&NegativeMinimumStruct{
		NegativeMinimumField:            -11,
		NegativeMinimumPtrField:         ptr.To(-11),
		OptionalNegativeMinimumField:    -11,
		OptionalNegativeMinimumPtrField: ptr.To(-11),
		RequiredNegativeMinimumField:    -11,
		RequiredNegativeMinimumPtrField: ptr.To(-11),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("negativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("negativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalNegativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("optionalNegativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredNegativeMinimumField"), nil, "").WithOrigin("minimum"),
		field.Invalid(field.NewPath("requiredNegativeMinimumPtrField"), nil, "").WithOrigin("minimum"),
	})
}
