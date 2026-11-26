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

// Test 验证使用自定义成员名的 zero-or-one-of union：空集合或单个成员合法，
// 两个成员同时设置应报错；错误消息应使用自定义成员名。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 空联合体是合法的
	// 空 union 合法。
	st.Value(&Struct{}).ExpectValid()

	st.Value(&Struct{M1: &M1{}}).ExpectValid()
	st.Value(&Struct{M2: &M2{}}).ExpectValid()

	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring(), field.ErrorList{
		field.Invalid(nil, nil, "must specify at most one of"),
	})

	// 测试校验的 ratcheting 行为。
	st.Value(&Struct{M1: &M1{}, M2: &M2{}}).OldValue(&Struct{M1: &M1{}, M2: &M2{}}).ExpectValid()
	st.Value(&Struct{}).OldValue(&Struct{}).ExpectValid()
}
