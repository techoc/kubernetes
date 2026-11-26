/*
Copyright 2017 The Kubernetes Authors.

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

	"k8s.io/apiserver/pkg/apis/audit"
)

// TestValidatePolicy 覆盖审计策略校验的两大类场景：
//   - 合法规则：仅 level、按用户组/请求/资源过滤、非资源 URL、省略阶段等；
//   - 非法规则：缺少或非法 level、资源与非资源条件混用、非法组名/URL、
//     ResourceNames 缺少 Resources、非法 omitStages 等。
//
// 空策略本身是合法的，但包含空规则（缺少 Level）则非法。
func TestValidatePolicy(t *testing.T) {
	validRules := []audit.PolicyRule{
		{ // Defaulting rule
			Level: audit.LevelMetadata,
		}, { // Matching non-humans
			Level:      audit.LevelNone,
			UserGroups: []string{"system:serviceaccounts", "system:nodes"},
		}, { // Specific request
			Level:      audit.LevelRequestResponse,
			Verbs:      []string{"get"},
			Resources:  []audit.GroupResources{{Group: "rbac.authorization.k8s.io", Resources: []string{"roles", "rolebindings"}}},
			Namespaces: []string{"kube-system"},
		}, { // Some non-resource URLs
			Level:      audit.LevelMetadata,
			UserGroups: []string{"developers"},
			NonResourceURLs: []string{
				"/logs*",
				"/healthz*",
				"/metrics",
				"*",
			},
		}, { // Omit RequestReceived stage
			Level: audit.LevelMetadata,
			OmitStages: []audit.Stage{
				audit.Stage("RequestReceived"),
			},
		},
	}
	successCases := []audit.Policy{}
	for _, rule := range validRules {
		successCases = append(successCases, audit.Policy{Rules: []audit.PolicyRule{rule}})
	}
	successCases = append(successCases, audit.Policy{})                         // Empty policy is valid.
	successCases = append(successCases, audit.Policy{OmitStages: []audit.Stage{ // Policy with omitStages
		audit.Stage("RequestReceived")}})
	successCases = append(successCases, audit.Policy{Rules: validRules}) // Multiple rules.

	for i, policy := range successCases {
		if errs := ValidatePolicy(&policy); len(errs) != 0 {
			t.Errorf("[%d] Expected policy %#v to be valid: %v", i, policy, errs)
		}
	}

	invalidRules := []audit.PolicyRule{
		{}, // Empty rule (missing Level)
		{ // Missing level
			Verbs:      []string{"get"},
			Resources:  []audit.GroupResources{{Resources: []string{"secrets"}}},
			Namespaces: []string{"kube-system"},
		}, { // Invalid Level
			Level: "FooBar",
		}, { // NonResourceURLs + Namespaces
			Level:           audit.LevelMetadata,
			Namespaces:      []string{"default"},
			NonResourceURLs: []string{"/logs*"},
		}, { // NonResourceURLs + ResourceKinds
			Level:           audit.LevelMetadata,
			Resources:       []audit.GroupResources{{Resources: []string{"secrets"}}},
			NonResourceURLs: []string{"/logs*"},
		}, { // invalid group name
			// 组名不能带版本号，只接受纯组名。
			Level:     audit.LevelMetadata,
			Resources: []audit.GroupResources{{Group: "rbac.authorization.k8s.io/v1beta1", Resources: []string{"roles"}}},
		}, { // invalid non-resource URLs
			Level: audit.LevelMetadata,
			NonResourceURLs: []string{
				"logs",
				"/healthz*",
			},
		}, { // empty non-resource URLs
			Level: audit.LevelMetadata,
			NonResourceURLs: []string{
				"",
				"/healthz*",
			},
		}, { // invalid non-resource URLs with multi "*"
			Level: audit.LevelMetadata,
			NonResourceURLs: []string{
				"/logs/*/*",
				"/metrics",
			},
		}, { // invalid non-resrouce URLs with "*" not in the end
			Level: audit.LevelMetadata,
			NonResourceURLs: []string{
				"/logs/*.log",
				"/metrics",
			},
		},
		{ // ResourceNames without Resources
			Level:      audit.LevelMetadata,
			Verbs:      []string{"get"},
			Resources:  []audit.GroupResources{{ResourceNames: []string{"leader"}}},
			Namespaces: []string{"kube-system"},
		},
		{ // invalid omitStages in rule
			Level: audit.LevelMetadata,
			OmitStages: []audit.Stage{
				audit.Stage("foo"),
			},
		},
	}
	errorCases := []audit.Policy{}
	for _, rule := range invalidRules {
		errorCases = append(errorCases, audit.Policy{Rules: []audit.PolicyRule{rule}})
	}

	// Multiple rules.
	errorCases = append(errorCases, audit.Policy{Rules: append(validRules, audit.PolicyRule{})})

	// 策略中存在无效的 omitStages
	policy := audit.Policy{OmitStages: []audit.Stage{
		audit.Stage("foo"),
	},
		Rules: []audit.PolicyRule{{
			Level: audit.LevelMetadata,
		}},
	}
	errorCases = append(errorCases, policy)

	for i, policy := range errorCases {
		if errs := ValidatePolicy(&policy); len(errs) == 0 {
			t.Errorf("[%d] Expected policy %#v to be invalid!", i, policy)
		}
	}
}
