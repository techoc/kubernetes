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

package discriminators

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 +k8s:ifEnabled/ifDisabled 包裹 discriminator 校验时，
// FeatureZ 的开关会决定哪个字段的错误被触发。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值结构始终合法。
	st.Value(&Struct{
		// All zero values
	}).Opts(map[string]bool{"FeatureZ": false}).ExpectValid()

	// FeatureZ 关闭时，只有 DiscriminatorFieldDisabled 生效。
	st.Value(&Struct{
		DiscriminatorField: Discriminator{
			Discriminator: "A",
			FieldB:        ptr.To("invalid"), // invalid because discriminator is A
		},
		DiscriminatorFieldDisabled: DiscriminatorDisabled{
			Discriminator: "A",
			FieldB:        ptr.To("invalid"), // invalid because discriminator is A
		},
	}).Opts(map[string]bool{"FeatureZ": false}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{field.Forbidden(field.NewPath("discriminatorFieldDisabled", "fieldB"), "").WithOrigin("")},
	)

	// FeatureZ 开启时，只有 DiscriminatorField 生效。
	st.Value(&Struct{
		DiscriminatorField: Discriminator{
			Discriminator: "A",
			FieldB:        ptr.To("invalid"), // invalid because discriminator is A
		},
		DiscriminatorFieldDisabled: DiscriminatorDisabled{
			Discriminator: "A",
			FieldB:        ptr.To("invalid"), // invalid because discriminator is A
		},
	}).Opts(map[string]bool{"FeatureZ": true}).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{field.Forbidden(field.NewPath("discriminatorField", "fieldB"), "").WithOrigin("")},
	)
}
