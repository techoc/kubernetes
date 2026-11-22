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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证自定义校验函数在各种作用域/形态下都被调用，并与声明式校验共存。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)
	matcher := field.ErrorMatcher{}.ByType().ByField()

	mk := func() *Struct {
		return &Struct{
			StringField:       "s",
			MaxLengthField:    "toolong", // longer than maxLength=3
			TypedefField:      "t",
			TypedefPtrField:   new(StringType("p")),
			TypedefSliceField: []StringType{"e"},
			TypedefMapField:   map[string]StringType{"k": "m"},
			StructField:       OtherStruct{StringField: "n"},
		}
	}

	// On create, custom validation runs at every scope/shape: root type, a field,
	// a field combined with maxLength, the reusable type (field/pointer/list
	// element), and a nested-struct field.
	//
	// 错误顺序对应调用顺序：先根类型，再按字段声明顺序，最后下钻到嵌套结构体。
	st.Value(mk()).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(nil, nil, ""),
		field.Invalid(field.NewPath("stringField"), nil, ""),
		field.Invalid(field.NewPath("maxLengthField"), nil, ""),
		field.TooLongCharacters(field.NewPath("maxLengthField"), "", 3),
		field.Invalid(field.NewPath("typedefField"), nil, ""),
		field.Invalid(field.NewPath("typedefPtrField"), nil, ""),
		field.Invalid(field.NewPath("typedefSliceField").Index(0), nil, ""),
		field.Invalid(field.NewPath("typedefMapField").Key("k"), nil, ""),
		field.Invalid(field.NewPath("structField", "stringField"), nil, ""),
	})

	// On a no-op update, field and embedded calls are skipped (value unchanged);
	// the root type-scoped call still runs (the framework does not skip it).
	//
	// 无变化更新时仅根类型校验仍执行，说明跳过逻辑作用于字段级调用。
	st.Value(mk()).OldValue(mk()).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(nil, nil, ""),
	})
}

// TestIfEnabled covers custom validation gated by a feature option via ifEnabled.
func TestIfEnabled(t *testing.T) {
	st := localSchemeBuilder.Test(t)
	matcher := field.ErrorMatcher{}.ByType().ByField()

	// Option disabled: the gated custom validation does not run.
	st.Value(&OptionStruct{}).Opts(map[string]bool{"FeatureX": false}).ExpectValid()

	// Option enabled: the custom validation runs.
	st.Value(&OptionStruct{}).Opts(map[string]bool{"FeatureX": true}).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(field.NewPath("stringField"), nil, ""),
	})
}

// TestEachVal covers custom validation (via the element type) coexisting with a
// declarative per-element check applied by eachVal.
//
// 同一元素同时触发类型自带的自定义校验和 eachVal 上的 maxLength，
// 二者互不吞并，错误按先后顺序上报。
func TestEachVal(t *testing.T) {
	st := localSchemeBuilder.Test(t)
	matcher := field.ErrorMatcher{}.ByType().ByField()

	st.Value(&EachStruct{SliceField: []StringType{"toolong"}}).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(field.NewPath("sliceField").Index(0), nil, ""),         // custom, via StringType
		field.TooLongCharacters(field.NewPath("sliceField").Index(0), "", 3), // eachVal maxLength
	})
}
