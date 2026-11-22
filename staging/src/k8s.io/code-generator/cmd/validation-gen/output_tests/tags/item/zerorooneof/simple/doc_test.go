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

package zeroroneofsimple

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 zeroOrOneOf 语义：零个或恰好一个成员合法，两个同时出现才报错。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 只有非 union 成员。
	st.Value(&Struct{
		Tasks: []Task{
			{Name: "other", State: "Other"},
		},
	}).ExpectValid()

	// 恰好一个成员。
	st.Value(&Struct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
		},
	}).ExpectValid()

	// 零个成员同样合法，这是与 union（必须恰好一个）的关键差异。
	st.Value(&Struct{
		Tasks: []Task{},
	}).ExpectValid()

	invalidBothSet := &Struct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "failed", State: "Failed"},
		},
	}
	// 两个成员同时存在：错误挂在列表字段上，origin 标记为 zeroOrOneOf。
	st.Value(invalidBothSet).ExpectMatches(
		field.ErrorMatcher{}.ByType().ByField().ByOrigin(),
		field.ErrorList{
			field.Invalid(field.NewPath("tasks"), nil, "").WithOrigin("zeroOrOneOf"),
		},
	)

	// Test ratcheting.
	//
	// 旧值同样非法时放行，避免存量对象升级后突然失败。
	st.Value(invalidBothSet).OldValue(invalidBothSet).ExpectValid()
}
