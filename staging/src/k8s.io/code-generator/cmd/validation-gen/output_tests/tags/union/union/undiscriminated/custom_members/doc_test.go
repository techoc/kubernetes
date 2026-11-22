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

package custommembers

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证无 discriminator union：必须且只能指定一个成员；
// 同时指定或都不指定都应报 union 错误，并受 ratcheting 影响。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 只指定一个成员合法。
	st.Value(&Struct{M1: &M1{}}).ExpectValid()
	st.Value(&Struct{M2: &M2{}}).ExpectValid()

	// 同时指定两个成员：报 "must specify exactly one of"。
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify exactly one of"),
	}.WithOrigin("union"))
	// 一个都没指定：报 "must specify one of"。
	st.Value(&Struct{}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify one of"),
	}.WithOrigin("union"))

	// 校验 ratcheting：旧值同样违反约束时不再报错。
	// Test validation ratcheting
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).OldValue(&Struct{M1: &M1{}, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{}).OldValue(&Struct{}).ExpectValid()
}
