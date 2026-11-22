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

package simple

import (
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 ifEnabled/ifDisabled 的四种组合，以及选项未声明时的错误处理。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 两个选项都关闭：所有 ifDisabled 校验触发。
	st.Value(&Struct{
		// All zero values
	}).Opts(map[string]bool{"FeatureX": false, "FeatureY": false}).ExpectValidateFalseByPath(map[string][]string{
		// All ifDisabled validations should trigger
		"xDisabledField": {"field Struct.XDisabledField"},
		"yDisabledField": {"field Struct.YDisabledField"},
		"xyMixedField":   {"field Struct.XYMixedField/Y"},
	})

	// 两个选项都开启：所有 ifEnabled 校验触发，slice 别名字段也生效。
	st.Value(&Struct{
		// All zero values
	}).Opts(map[string]bool{"FeatureX": true, "FeatureY": true}).ExpectValidateFalseByPath(map[string][]string{
		// All ifEnabled validations should trigger
		"xEnabledField":     {"field Struct.XEnabledField"},
		"yEnabledField":     {"field Struct.YEnabledField"},
		"xyMixedField":      {"field Struct.XYMixedField/X"},
		"nilableAliasField": {"field Struct.NilableAliasField"},
	})

	// X 开、Y 关：同一字段上的两条对立校验同时触发。
	st.Value(&Struct{
		// All zero values
	}).Opts(map[string]bool{"FeatureX": true, "FeatureY": false}).ExpectValidateFalseByPath(map[string][]string{
		// All ifEnabled validations should trigger
		"xEnabledField":  {"field Struct.XEnabledField"},
		"yDisabledField": {"field Struct.YDisabledField"},
		"xyMixedField": {
			"field Struct.XYMixedField/X",
			"field Struct.XYMixedField/Y"},
		"nilableAliasField": {"field Struct.NilableAliasField"},
	})

	// X 关、Y 开：与上一种组合互补。
	st.Value(&Struct{
		// All zero values
	}).Opts(map[string]bool{"FeatureX": false, "FeatureY": true}).ExpectValidateFalseByPath(map[string][]string{
		// All ifEnabled validations should trigger
		"xDisabledField": {"field Struct.XDisabledField"},
		"yEnabledField":  {"field Struct.YEnabledField"},
	})

	// No options declared: every referenced option is undeclared.
	//
	// 未声明的选项按内部错误处理，而不是静默忽略：这能暴露漏传配置的问题。
	internal := func(p string) *field.Error {
		return field.InternalError(field.NewPath(p), errors.New(""))
	}
	st.Value(&Struct{}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		internal("xEnabledField"),
		internal("xDisabledField"),
		internal("yEnabledField"),
		internal("yDisabledField"),
		internal("xyMixedField"), // ifEnabled(FeatureX)
		internal("xyMixedField"), // ifDisabled(FeatureY)
		internal("nilableAliasField"),
	})
}
