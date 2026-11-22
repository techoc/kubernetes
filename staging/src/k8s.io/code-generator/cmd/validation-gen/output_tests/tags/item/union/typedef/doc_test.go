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

package uniontypedef

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 union 约束在 typedef 列表上的三种状态：恰好一个成员、
// 同时出现多个成员、以及一个成员都没有。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 只设置 succeeded，满足“恰好一个”的约束。
	st.Value(&Struct{
		Tasks: TaskList{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "other", State: "Other"},
		},
	}).ExpectValid()

	invalidBothSet := &Struct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "failed", State: "Failed"},
		},
	}

	// 两个 union 成员同时存在：错误值按 key 排序后拼接，保证消息稳定。
	st.Value(invalidBothSet).ExpectMatches(
		field.ErrorMatcher{},
		field.ErrorList{
			field.Invalid(field.NewPath("tasks"), "{TaskList[{\"name\": \"failed\"}], TaskList[{\"name\": \"succeeded\"}]}",
				"must specify exactly one of: `TaskList[{\"name\": \"succeeded\"}]`, `TaskList[{\"name\": \"failed\"}]`"),
		},
	)

	invalidEmpty := &Struct{
		Tasks: TaskList{},
	}
	// 空列表：提示语从 "exactly one" 变为 "one of"。
	st.Value(invalidEmpty).ExpectMatches(
		field.ErrorMatcher{},
		field.ErrorList{
			field.Invalid(field.NewPath("tasks"), "",
				"must specify one of: `TaskList[{\"name\": \"succeeded\"}]`, `TaskList[{\"name\": \"failed\"}]`"),
		},
	)

	// Test ratcheting.
	//
	// 旧值同样非法时按 ratcheting 规则放行，避免存量对象在升级后突然报错。
	st.Value(invalidEmpty).OldValue(invalidEmpty).ExpectValid()

	st.Value(invalidBothSet).OldValue(invalidBothSet).ExpectValid()
}
