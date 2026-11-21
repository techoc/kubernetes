/*
Copyright The Kubernetes Authors.

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

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/apiserver/pkg/admission/plugin/manifest"
)

// TestValidateStaticSuffix 验证 ValidateStaticSuffix 只拒绝以 .static.k8s.io 结尾的名称，
// 相似后缀、部分匹配以及空名称都应放行。
func TestValidateStaticSuffix(t *testing.T) {
	tests := []struct {
		name      string
		inputName string
		wantError bool
	}{
		{
			name:      "valid name without suffix",
			inputName: "my-webhook",
			wantError: false,
		},
		{
			name:      "valid name with similar suffix",
			inputName: "my-webhook.k8s.io",
			wantError: false,
		},
		{
			name:      "valid name with partial match",
			inputName: "static.k8s.io.my-webhook",
			wantError: false,
		},
		{
			name:      "invalid name with static suffix",
			inputName: "my-webhook.static.k8s.io",
			wantError: true,
		},
		{
			name:      "invalid name with only static suffix",
			inputName: ".static.k8s.io",
			wantError: true,
		},
		{
			name:      "valid empty name",
			inputName: "",
			wantError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// 只要返回任何错误即视为拒绝，这里不关心错误的具体内容。
			errs := ValidateStaticSuffix(tc.inputName, field.NewPath("metadata", "name"))
			gotError := len(errs) > 0
			if gotError != tc.wantError {
				t.Errorf("ValidateStaticSuffix(%q) = %v errors, want error: %v", tc.inputName, errs, tc.wantError)
			}
		})
	}
}

// TestWarningsForStaticSuffix 验证带静态配置后缀的名称会产生告警，但不阻止创建。
func TestWarningsForStaticSuffix(t *testing.T) {
	tests := []struct {
		name        string
		inputName   string
		wantWarning bool
	}{
		{
			name:        "valid name without suffix",
			inputName:   "my-webhook",
			wantWarning: false,
		},
		{
			name:        "name with static suffix",
			inputName:   "my-webhook.static.k8s.io",
			wantWarning: true,
		},
		{
			name:        "valid name with similar suffix",
			inputName:   "my-webhook.k8s.io",
			wantWarning: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			warnings := WarningsForStaticSuffix(tc.inputName)
			gotWarning := len(warnings) > 0
			if gotWarning != tc.wantWarning {
				t.Errorf("WarningsForStaticSuffix(%q) = %v, want warning: %v", tc.inputName, warnings, tc.wantWarning)
			}
		})
	}
}

// TestStaticConfigSuffixInSync 确保 pkg 侧与 staging 侧的静态配置后缀常量保持一致，
// 防止两边漂移导致校验与告警行为不一致。
func TestStaticConfigSuffixInSync(t *testing.T) {
	if StaticConfigSuffix != manifest.StaticConfigSuffix {
		t.Errorf("StaticConfigSuffix constants are out of sync: pkg=%q, staging=%q", StaticConfigSuffix, manifest.StaticConfigSuffix)
	}
}
