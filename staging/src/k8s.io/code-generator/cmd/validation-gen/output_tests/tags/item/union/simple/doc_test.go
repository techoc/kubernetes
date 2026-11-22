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

package unionsimple

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证列表项 union：恰好设置一个成员合法，全部未设置或设置多个都应报错，
// 且错误消息需列出可选的成员路径；最后确认 ratcheting 对未变化的非法旧值放行。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 只有一个成员被设置，合法。
	st.Value(&Struct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "other", State: "Other"},
		},
	}).ExpectValid()

	// 两个成员同时被设置：错误消息按路径排序后列出成员。
	invalidBothSet := &Struct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "failed", State: "Failed"},
		},
	}

	st.Value(invalidBothSet).ExpectMatches(
		field.ErrorMatcher{},
		field.ErrorList{
			field.Invalid(field.NewPath("tasks"), "{Tasks[{\"name\": \"failed\"}], Tasks[{\"name\": \"succeeded\"}]}",
				"must specify exactly one of: `Tasks[{\"name\": \"succeeded\"}]`, `Tasks[{\"name\": \"failed\"}]`"),
		},
	)

	// 一个成员都没有：普通 union 要求恰好一个，因此报错。
	invalidEmpty := &Struct{
		Tasks: []Task{},
	}
	st.Value(invalidEmpty).ExpectMatches(
		field.ErrorMatcher{},
		field.ErrorList{
			field.Invalid(field.NewPath("tasks"), "",
				"must specify one of: `Tasks[{\"name\": \"succeeded\"}]`, `Tasks[{\"name\": \"failed\"}]`"),
		},
	)

	// 测试 ratcheting：未变化的非法旧值不应在更新时重新报错。
	st.Value(invalidEmpty).OldValue(invalidEmpty).ExpectValid()
	st.Value(invalidBothSet).OldValue(invalidBothSet).ExpectValid()
}
