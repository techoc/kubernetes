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

package unions

import (
	"k8s.io/apimachinery/pkg/util/validation/field"
	"testing"
)

// Test 验证 ifEnabled/ifDisabled 条件作用在 union、zeroOrOneOf 及其列表项上：
// 条件开关会切换哪一组成员参与 union 校验。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 全部为零值，FeatureX 关闭：无成员被指定，union 无错误。
	st.Value(&Struct{
		// 全部为零值
	}).Opts(map[string]bool{"FeatureX": false}).ExpectValid()

	// FeatureX 开启：discriminator 为 xEnabledField，但 xDisabledField 也被设置，
	// 违反 union；zeroOrOneOf 同时指定两个成员也违反约束。
	st.Value(&Struct{
		UnionField: Union{
			Discriminator:  "xEnabledField",
			XEnabledField:  "foo",
			XDisabledField: "bar", // Invalid since XEnabledField is the discriminator
		},
		ZeroOrOneOfField: ZeroOrOneOf{
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfItem: []Task{
			{Name: "succeeded"},
			{Name: "failed"},
		},
	}).Opts(map[string]bool{"FeatureX": true}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Invalid(field.NewPath("unionField", "xDisabledField"), "", "").WithOrigin("union"),
			field.Invalid(field.NewPath("unionField", "xEnabledField"), "", "").WithOrigin("union"),
			field.Invalid(field.NewPath("zeroOrOneOfField"), "{XEnabledField:\"foo\", XDisabledField:\"bar\"}", "").WithOrigin("zeroOrOneOf"),
			field.Invalid(field.NewPath("zeroOrOneOfItem"), "[{Name:\"succeeded\", State:\"\"} {Name:\"failed\", State:\"\"}]", "").WithOrigin("zeroOrOneOf"),
		},
	)

	// FeatureX 关闭：同一组数据不再违反 union 约束。
	st.Value(&Struct{
		UnionField: Union{
			Discriminator:  "xEnabledField",
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfField: ZeroOrOneOf{
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfItem: []Task{
			{Name: "succeeded"},
			{Name: "failed"},
		},
	}).Opts(map[string]bool{"FeatureX": false}).ExpectValid()

	// 对称场景：Disabled 版本在 FeatureX 关闭时应报错。
	st.Value(&Struct{
		UnionFieldDisabled: UnionDisabled{
			Discriminator:  "xEnabledField",
			XEnabledField:  "foo",
			XDisabledField: "bar", // Invalid since XEnabledField is the discriminator
		},
		ZeroOrOneOfFieldDisabled: ZeroOrOneOfDisabled{
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfItemDisabled: []Task{
			{Name: "succeeded"},
			{Name: "failed"},
		},
	}).Opts(map[string]bool{"FeatureX": false}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Invalid(field.NewPath("unionFieldDisabled", "xDisabledField"), "", "").WithOrigin("union"),
			field.Invalid(field.NewPath("unionFieldDisabled", "xEnabledField"), "", "").WithOrigin("union"),
			field.Invalid(field.NewPath("zeroOrOneOfFieldDisabled"), "{XEnabledField:\"foo\", XDisabledField:\"bar\"}", "").WithOrigin("zeroOrOneOf"),
			field.Invalid(field.NewPath("zeroOrOneOfItemDisabled"), "[{Name:\"succeeded\", State:\"\"} {Name:\"failed\", State:\"\"}]", "").WithOrigin("zeroOrOneOf"),
		},
	)

	// FeatureX 开启时，Disabled 版本的约束不生效。
	st.Value(&Struct{
		UnionFieldDisabled: UnionDisabled{
			Discriminator:  "xEnabledField",
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfFieldDisabled: ZeroOrOneOfDisabled{
			XEnabledField:  "foo",
			XDisabledField: "bar",
		},
		ZeroOrOneOfItemDisabled: []Task{
			{Name: "succeeded"},
			{Name: "failed"},
		},
	}).Opts(map[string]bool{"FeatureX": true}).ExpectValid()
}
