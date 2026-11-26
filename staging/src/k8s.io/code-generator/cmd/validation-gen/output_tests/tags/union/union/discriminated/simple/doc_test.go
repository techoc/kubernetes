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
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 是 discriminated union 的主用例集：discriminator 值决定哪个成员
// 必须出现、哪些成员被禁止；同时覆盖 nil/空切片的差异、指针/map 成员，
// 以及更新时的 ratcheting 与 nil 旧值处理。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{ /* zero values */ }).ExpectValid()

	st.Value(&Struct{D: DM1, M1: &M1{}}).ExpectValid()
	st.Value(&Struct{D: DM2, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{D: DM3, M3: []string{"a"}}).ExpectValid()
	st.Value(&Struct{D: DM4, M4: map[string]string{"k": "v"}}).ExpectValid()

	st.Value(&Struct{D: DM2, M1: &M1{}, M2: &M2{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "may only be specified when"),
	}.WithOrigin("union"))

	st.Value(&Struct{D: DM1}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// slice 成员：判别式匹配但 slice 为 nil
	st.Value(&Struct{D: DM3}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m3"), nil, "must be specified when"),
	}.WithOrigin("union"))
	// slice 成员：判别式匹配但 slice 为空（len==0，非 nil）
	st.Value(&Struct{D: DM3, M3: []string{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m3"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// slice 成员：判别式不匹配但 slice 已设置
	st.Value(&Struct{D: DM1, M1: &M1{}, M3: []string{"a"}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m3"), nil, "may only be specified when"),
	}.WithOrigin("union"))

	// map 成员：判别式匹配但 map 为 nil
	st.Value(&Struct{D: DM4}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m4"), nil, "must be specified when"),
	}.WithOrigin("union"))
	// map 成员：判别式匹配但 map 为空（len==0，非 nil）
	st.Value(&Struct{D: DM4, M4: map[string]string{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m4"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// map 成员：判别式不匹配但 map 已设置
	st.Value(&Struct{D: DM1, M1: &M1{}, M4: map[string]string{"k": "v"}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m4"), nil, "may only be specified when"),
	}.WithOrigin("union"))

	// 测试 ratcheting 行为
	st.Value(&Struct{D: DM2, M1: &M1{}, M2: &M2{}}).OldValue(&Struct{D: DM2, M1: &M1{}, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{D: DM1}).OldValue(&Struct{D: DM1}).ExpectValid()

	// slice 成员 ratcheting：成员关系未变
	st.Value(&Struct{D: DM3, M3: []string{"a"}}).OldValue(&Struct{D: DM3, M3: []string{"b"}}).ExpectValid()
	// slice 成员 ratcheting：从已设置变为未设置
	st.Value(&Struct{D: DM3}).OldValue(&Struct{D: DM3, M3: []string{"a"}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m3"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// map 成员 ratcheting：成员关系未变
	st.Value(&Struct{D: DM4, M4: map[string]string{"k": "v1"}}).OldValue(&Struct{D: DM4, M4: map[string]string{"k": "v2"}}).ExpectValid()
	// map 成员 ratcheting：从已设置变为未设置
	st.Value(&Struct{D: DM4}).OldValue(&Struct{D: DM4, M4: map[string]string{"k": "v"}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m4"), nil, "must be specified when"),
	}.WithOrigin("union"))

	// 测试 old value 为 nil（模拟更新时新设置的指针字段）。
	// 即使 oldObj 为 nil，判别式联合体校验仍应发现不匹配。
	st.Value(&Struct{D: DM1}).OldValue(nil).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "must be specified when"),
	}.WithOrigin("union"))
}
