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

// Test 验证 zeroOrOneOf union：零个或至多一个成员合法，多个成员非法；
// 覆盖指针、字符串、指针字符串成员，以及更新时只看“是否设置”的语义和
// nil 旧值的处理。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 对 zeroOrOneOf 来说空联合体是合法的
	st.Value(&Struct{}).ExpectValid()

	st.Value(&Struct{M1: &M1{}}).ExpectValid()
	st.Value(&Struct{M2: &M2{}}).ExpectValid()
	st.Value(&Struct{M3: "a string"}).ExpectValid()
	st.Value(&Struct{M4: ptr.To("a string")}).ExpectValid()

	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify at most one of").WithOrigin("zeroOrOneOf"),
	})
	st.Value(&Struct{M1: &M1{}, M3: "a string"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify at most one of").WithOrigin("zeroOrOneOf"),
	})
	st.Value(&Struct{M1: &M1{}, M4: ptr.To("a string")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify at most one of").WithOrigin("zeroOrOneOf"),
	})

	// update 只关心字段是否被设置，不关心具体值。
	st.Value(&Struct{M3: "a string"}).OldValue(&Struct{M3: "different string"}).ExpectValid()

	// 测试校验的 ratcheting 行为。
	st.Value(&Struct{}).OldValue(&Struct{}).ExpectValid()
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).OldValue(&Struct{M1: &M1{}, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{M3: "a string", M2: &M2{}}).OldValue(&Struct{M3: "different string", M2: &M2{}}).ExpectValid()

	// 测试旧值为 nil 的 update（模拟 update 时新增 map 条目）。
	// 即使旧值为 nil，空联合体对 zeroOrOneOf 仍合法。
	st.Value(&Struct{}).OldValue((*Struct)(nil)).ExpectValid()
	// 即使旧值为 nil，设置多个成员仍应被检出。
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).OldValue((*Struct)(nil)).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(nil, nil, "must specify at most one of").WithOrigin("zeroOrOneOf"),
	})
}
