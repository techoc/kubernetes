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

	// 创建时，自定义校验在所有作用域/形态上运行：根类型、字段、
	// 字段叠加 maxLength、可复用类型（字段/指针/列表
	// 元素），以及嵌套结构体字段。
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

	// 无变化的更新会跳过字段级与嵌入调用（值未变）；
	// 根类型作用域的调用仍会执行（框架不跳过它）。
	//
	// 无变化更新时仅根类型校验仍执行，说明跳过逻辑作用于字段级调用。
	st.Value(mk()).OldValue(mk()).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(nil, nil, ""),
	})
}

// TestIfEnabled 验证由 ifEnabled 按特性开关控制的自定义校验。
func TestIfEnabled(t *testing.T) {
	st := localSchemeBuilder.Test(t)
	matcher := field.ErrorMatcher{}.ByType().ByField()

	// 选项关闭：受控的自定义校验不运行。
	st.Value(&OptionStruct{}).Opts(map[string]bool{"FeatureX": false}).ExpectValid()

	// 选项开启：自定义校验运行。
	st.Value(&OptionStruct{}).Opts(map[string]bool{"FeatureX": true}).ExpectMatches(matcher, field.ErrorList{
		field.Invalid(field.NewPath("stringField"), nil, ""),
	})
}

// TestEachVal 覆盖自定义校验（经元素类型）与
// eachVal 应用的声明式逐元素校验共存。
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
