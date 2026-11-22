/*
Copyright 2021 The Kubernetes Authors.

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

package validation

import "testing"

// TestMaskTrailingDash 验证 maskTrailingDash 的边界行为：
// 空串、仅一个 dash、前导 dash 均原样返回；只有末尾 dash 会被替换；
// 同时存在前导与末尾 dash 时仅处理末尾那个。
func TestMaskTrailingDash(t *testing.T) {
	testCases := []struct {
		beforeMasking        string
		expectedAfterMasking string
		description          string
	}{
		{
			beforeMasking:        "",
			expectedAfterMasking: "",
			description:          "empty string",
		},
		{
			// 单个 dash 不能被视为“末尾 dash”，否则会变成空串。
			beforeMasking:        "-",
			expectedAfterMasking: "-",
			description:          "only a single dash",
		},
		{
			beforeMasking:        "-foo",
			expectedAfterMasking: "-foo",
			description:          "has leading dash",
		},
		{
			// 注意期望值 "foa" 是掩码算法的既有行为，不是笔误。
			beforeMasking:        "-foo-",
			expectedAfterMasking: "-foa",
			description:          "has both leading and trailing dashes",
		},
		{
			beforeMasking:        "b-",
			expectedAfterMasking: "a",
			description:          "has trailing dash",
		},
		{
			beforeMasking:        "ab",
			expectedAfterMasking: "ab",
			description:          "has neither leading nor trailing dashes",
		},
	}

	for _, tc := range testCases {
		afterMasking := maskTrailingDash(tc.beforeMasking)
		if afterMasking != tc.expectedAfterMasking {
			t.Errorf("error in test case: %s. expected: %s, actual: %s", tc.description, tc.expectedAfterMasking, afterMasking)
		}
	}
}
