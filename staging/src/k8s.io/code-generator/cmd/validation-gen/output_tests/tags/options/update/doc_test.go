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

package update

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 immutable 与 update=NoModify 在 ifEnabled/ifDisabled 条件下的行为：
// 只有条件成立的分支才会在更新时阻止字段修改。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	structA := Struct{
		ImmutableEnabled:       "aaa",
		ImmutableDisabled:      "bbb",
		UpdateNoModifyEnabled:  "ccc",
		UpdateNoModifyDisabled: "ddd",
	}

	structB := Struct{
		ImmutableEnabled:       "eee",
		ImmutableDisabled:      "fff",
		UpdateNoModifyEnabled:  "ggg",
		UpdateNoModifyDisabled: "hhh",
	}

	// 新旧值相同：没有任何字段被修改，合法。
	st.Value(&structA).OldValue(&structA).Opts(map[string]bool{"FeatureX": false}).ExpectValid()

	// FeatureX 关闭：仅 disabled 分支的 immutable/NoModify 生效。
	st.Value(&structB).OldValue(&structA).Opts(map[string]bool{"FeatureX": false}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByDetailSubstring(),
		field.ErrorList{
			field.Invalid(field.NewPath("immutableDisabled"), nil, "").WithOrigin("immutable"),
			field.Invalid(field.NewPath("updateNoModifyDisabled"), nil, "field cannot be modified once set").WithOrigin("update"),
		},
	)

	// FeatureX 开启：仅 enabled 分支的约束生效。
	st.Value(&structB).OldValue(&structA).Opts(map[string]bool{"FeatureX": true}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByDetailSubstring(),
		field.ErrorList{
			field.Invalid(field.NewPath("immutableEnabled"), nil, "").WithOrigin("immutable"),
			field.Invalid(field.NewPath("updateNoModifyEnabled"), nil, "field cannot be modified once set").WithOrigin("update"),
		},
	)
}
