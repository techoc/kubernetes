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

package monotonic

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 覆盖 +k8s:monotonic 在更新场景下的语义：只允许不减，
// 并与 required、NoUnset、minimum 等其他约束叠加。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// structOrig 是基线旧值；后续用增加/减少两种新值与之比较。
	structOrig := Struct{
		IntField:          10,
		Int64Field:        20,
		Uint64Field:       30,
		IntPtrField:       new(40),
		MonotonicField:    50,
		MonotonicPtrField: new(MonotonicType(60)),
		OptionalInt:       10,
		RequiredInt:       10,
		OptionalIntPtr:    new(10),
		RequiredIntPtr:    new(10),
		NegativeInt:       0,
	}

	// structIncrease 是所有字段均增大的新值，作为合法更新用例。
	structIncrease := Struct{
		IntField:          11,
		Int64Field:        21,
		Uint64Field:       31,
		IntPtrField:       new(41),
		MonotonicField:    51,
		MonotonicPtrField: new(MonotonicType(61)),
		OptionalInt:       11,
		RequiredInt:       11,
		OptionalIntPtr:    new(11),
		RequiredIntPtr:    new(11),
		NegativeInt:       5,
	}

	// structDecrease 是所有字段均减小的新值，作为非法更新用例。
	structDecrease := Struct{
		IntField:          9,
		Int64Field:        19,
		Uint64Field:       29,
		IntPtrField:       new(39),
		MonotonicField:    49,
		MonotonicPtrField: new(MonotonicType(59)),
		OptionalInt:       9,
		RequiredInt:       9,
		OptionalIntPtr:    new(9),
		RequiredIntPtr:    new(9),
		NegativeInt:       0,
	}

	// 合法更新
	st.Value(&structOrig).OldValue(&structOrig).ExpectValid()
	st.Value(&structIncrease).OldValue(&structOrig).ExpectValid()

	// 非法更新（值变小）
	st.Value(&structDecrease).OldValue(&structOrig).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("intField"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("int64Field"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("uint64Field"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("intPtrField"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("monotonicField"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("monotonicPtrField"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("optionalInt"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("requiredInt"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("optionalIntPtr"), nil, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("requiredIntPtr"), nil, "").WithOrigin("monotonic"),
	})

	// 测试特殊情况：零值与 nil
	structZero := structOrig
	structZero.OptionalInt = 0
	structZero.RequiredInt = 0
	structZero.OptionalIntPtr = new(0)
	structZero.RequiredIntPtr = new(0)

	// OptionalInt（非指针）-> 0 非法（因存在 +k8s:update=NoUnset）。
	// RequiredInt -> 0 非法（因 +k8s:required）。
	// OptionalIntPtr -> 0 非法（单调性检查发现值变小）。
	// RequiredIntPtr -> 0 非法（单调性检查发现值变小）。
	st.Value(&structZero).OldValue(&structOrig).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalInt"), 0, "").WithOrigin("update"),
		field.Required(field.NewPath("requiredInt"), ""),
		field.Invalid(field.NewPath("optionalIntPtr"), 0, "").WithOrigin("monotonic"),
		field.Invalid(field.NewPath("requiredIntPtr"), 0, "").WithOrigin("monotonic"),
	})

	// OptionalIntPtr -> nil 非法，因 +k8s:update=NoUnset
	structNil := structOrig
	structNil.OptionalIntPtr = nil
	st.Value(&structNil).OldValue(&structOrig).ExpectMatches(field.ErrorMatcher{}.ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalIntPtr"), nil, "").WithOrigin("update"),
	})

	// 创建应合法（单调性校验只用于更新）
	st.Value(&structDecrease).ExpectValid()

	// OptionalIntPtr 未设置 -> 设置应合法（nil 到值）
	// nil 到有值视为“增加”，应通过。
	structSet := structOrig
	structSet.OptionalIntPtr = new(10)
	oldObjectSet := structSet
	oldObjectSet.OptionalIntPtr = nil
	st.Value(&structSet).OldValue(&oldObjectSet).ExpectValid()

	// OptionalIntPtr 已设置 -> 未设置现在非法，因 +k8s:update=NoUnset
	structUnset := structOrig
	structUnset.OptionalIntPtr = nil
	st.Value(&structUnset).OldValue(&structOrig).ExpectMatches(field.ErrorMatcher{}.ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("optionalIntPtr"), nil, "").WithOrigin("update"),
	})

	// 非法：违反 minimum 约束（origin = "minimum"）
	structBad := Struct{RequiredInt: 1, RequiredIntPtr: new(int)}
	structBad.NegativeInt = -11 // below the declared -10

	st.Value(&structBad).ExpectMatches(
		field.ErrorMatcher{}.ByOrigin(),
		field.ErrorList{
			field.Invalid(field.NewPath("negativeInt"), -11, "").
				WithOrigin("minimum"),
		})
}
