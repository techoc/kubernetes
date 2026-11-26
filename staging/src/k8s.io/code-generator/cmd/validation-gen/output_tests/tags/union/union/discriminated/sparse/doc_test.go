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

package sparse

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证带 discriminator 的稀疏 union：discriminator 为 M2 时允许不设置
// 任何成员（稀疏），但设置了不匹配的成员或该设未设都应报错。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 全零值：discriminator 为空，不要求任何成员。
	st.Value(&Struct{ /* zero values */ }).ExpectValid()

	st.Value(&Struct{D: DM1, M1: &M1{}}).ExpectValid()
	st.Value(&Struct{D: DM2}).ExpectValid()

	st.Value(&Struct{D: DM2, M1: &M1{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "may only be specified when"),
	}.WithOrigin("union"))

	st.Value(&Struct{D: DM1}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// 测试 ratcheting 行为
	st.Value(&Struct{D: DM2, M1: &M1{}}).OldValue(&Struct{D: DM2, M1: &M1{}}).ExpectValid()
	st.Value(&Struct{D: DM1}).OldValue(&Struct{D: DM1}).ExpectValid()
}
