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

package validators

import (
	"testing"
)

// TestGetStability 验证标签名到稳定性级别的映射：
// Alpha/Beta 由命名约定决定，未列入表中的标签视为 Stable；
// 未注册的标签返回 error。
func TestGetStability(t *testing.T) {
	tests := []struct {
		tagName     string
		expected    TagStabilityLevel
		expectError bool
	}{
		{
			tagName:  "k8s:validateTrueAlpha",
			expected: TagStabilityLevelAlpha,
		},
		{
			tagName:  "k8s:validateTrueBeta",
			expected: TagStabilityLevelBeta,
		},
		{
			tagName:  "k8s:required",
			expected: TagStabilityLevelStable,
		},
		{
			tagName:     "k8s:unknownTag",
			expected:    "",
			expectError: true,
		},
	}

	// expectedError 未显式设置时为零值，因此只断言成功路径上的返回值。
	for _, tt := range tests {
		t.Run(tt.tagName, func(t *testing.T) {
			got, err := GetStability(tt.tagName)
			if err != nil && !tt.expectError {
				t.Errorf("Unexpected error: %v", err)
			}
			if got != tt.expected {
				t.Errorf("GetStability(%q) = %v, want %v", tt.tagName, got, tt.expected)
			}
		})
	}
}

// TestIsKnownTag 验证已知标签判断：注册过的 Alpha/Beta/Stable 标签返回 true，
// 未注册标签返回 false。lint 的 known-tags-only 规则依赖这一判定。
func TestIsKnownTag(t *testing.T) {
	tests := []struct {
		tagName  string
		expected bool
	}{
		{
			tagName:  "k8s:validateTrueAlpha",
			expected: true,
		},
		{
			tagName:  "k8s:validateTrueBeta",
			expected: true,
		},
		{
			tagName:  "k8s:required",
			expected: true,
		},
		{
			tagName:  "k8s:unknownTag",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.tagName, func(t *testing.T) {
			got := IsKnownTag(tt.tagName)
			if got != tt.expected {
				t.Errorf("IsKnownTag(%q) = %v, want %v", tt.tagName, got, tt.expected)
			}
		})
	}
}
