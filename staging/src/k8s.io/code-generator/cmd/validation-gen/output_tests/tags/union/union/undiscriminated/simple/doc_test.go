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
	"k8s.io/utils/ptr"
)

// Test 覆盖无判别字段的 union：零个成员报 “must specify one of”，
// 多个成员报 “must specify exactly one of”；空 slice/map 视为未设置；
// 更新时只比较成员是否设置，不比较值。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify one of"),
	}.WithOrigin("union"))

	st.Value(&Struct{M1: &M1{}}).ExpectValid()
	st.Value(&Struct{M2: &M2{}}).ExpectValid()
	st.Value(&Struct{M3: "a string"}).ExpectValid()
	st.Value(&Struct{M4: ptr.To("a string")}).ExpectValid()
	st.Value(&Struct{M5: []string{"a string"}}).ExpectValid()
	st.Value(&Struct{M6: map[string]string{"k": "v"}}).ExpectValid()

	// 空 slice/map 视为“未设置”（联合体归属判定等同 nil）
	st.Value(&Struct{M5: []string{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify one of"),
	}.WithOrigin("union"))
	st.Value(&Struct{M6: map[string]string{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify one of"),
	}.WithOrigin("union"))

	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify exactly one of"),
	}.WithOrigin("union"))
	st.Value(&Struct{M1: &M1{}, M3: "a string"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify exactly one of"),
	}.WithOrigin("union"))
	st.Value(&Struct{M1: &M1{}, M4: ptr.To("a string")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify exactly one of"),
	}.WithOrigin("union"))

	// 更新校验只关心字段是否被设置，不关心值。
	st.Value(&Struct{M3: "a string"}).OldValue(&Struct{M3: "different string"}).ExpectValid()

	// 测试 ratcheting 行为
	st.Value(&Struct{}).OldValue(&Struct{}).ExpectValid()
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).OldValue(&Struct{M1: &M1{}, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{M3: "a string", M2: &M2{}}).OldValue(&Struct{M3: "different string", M2: &M2{}}).ExpectValid()

	// slice/map 成员 ratcheting：成员关系未变、值不同
	st.Value(&Struct{M5: []string{"a"}}).OldValue(&Struct{M5: []string{"b"}}).ExpectValid()
	st.Value(&Struct{M6: map[string]string{"k": "v1"}}).OldValue(&Struct{M6: map[string]string{"k": "v2"}}).ExpectValid()

	// 测试 old value 为 nil（模拟更新时新增 map 条目或新设置的指针字段）。
	// 即使 oldObj 为 nil，联合体校验仍应发现空联合体。
	st.Value(&Struct{}).OldValue(nil).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify one of").WithOrigin("union"),
	})
}
