/*
Copyright 2018 The Kubernetes Authors.

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

package v1

import (
	"fmt"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
)

// VerbMatches 判断规则是否允许请求的 verb。
func VerbMatches(rule *rbacv1.PolicyRule, requestedVerb string) bool {
	for _, ruleVerb := range rule.Verbs {
		// 通配符 "*" 覆盖所有 verb，命中即放行。
		if ruleVerb == rbacv1.VerbAll {
			return true
		}
		if ruleVerb == requestedVerb {
			return true
		}
	}

	return false
}

// APIGroupMatches 判断规则是否覆盖请求的 API group。
func APIGroupMatches(rule *rbacv1.PolicyRule, requestedGroup string) bool {
	for _, ruleGroup := range rule.APIGroups {
		// 通配符 "*" 表示所有组；core 组的请求组名为空字符串，按精确匹配处理。
		if ruleGroup == rbacv1.APIGroupAll {
			return true
		}
		if ruleGroup == requestedGroup {
			return true
		}
	}

	return false
}

// ResourceMatches 判断规则是否允许访问请求的资源（含子资源）。
// combinedRequestedResource 是 "resource/subresource" 形式的组合名。
func ResourceMatches(rule *rbacv1.PolicyRule, combinedRequestedResource, requestedSubresource string) bool {
	for _, ruleResource := range rule.Resources {
		// 通配符 "*" 表示允许所有资源，命中即放行。
		if ruleResource == rbacv1.ResourceAll {
			return true
		}
		// 资源名完全一致时直接命中。
		if ruleResource == combinedRequestedResource {
			return true
		}

		// 还支持 "*/subresource" 形式：对任意资源的该子资源授权。
		// 请求不涉及子资源时无需继续尝试这种形式。
		if len(requestedSubresource) == 0 {
			continue
		}
		// 规则不是 "*/subresource" 格式（长度不符或缺少前缀/后缀）时不匹配。
		if len(ruleResource) == len(requestedSubresource)+2 &&
			strings.HasPrefix(ruleResource, "*/") &&
			strings.HasSuffix(ruleResource, requestedSubresource) {
			return true

		}
	}

	return false
}

// ResourceNameMatches 判断规则是否允许访问指定资源名。
func ResourceNameMatches(rule *rbacv1.PolicyRule, requestedName string) bool {
	// ResourceNames 为空表示不按名称限制，即匹配任意名称。
	if len(rule.ResourceNames) == 0 {
		return true
	}

	for _, ruleName := range rule.ResourceNames {
		if ruleName == requestedName {
			return true
		}
	}

	return false
}

// NonResourceURLMatches 判断规则是否允许访问请求的非资源 URL（如 /healthz）。
func NonResourceURLMatches(rule *rbacv1.PolicyRule, requestedURL string) bool {
	for _, ruleURL := range rule.NonResourceURLs {
		// 通配符 "*" 表示允许所有非资源 URL。
		if ruleURL == rbacv1.NonResourceAll {
			return true
		}
		// URL 完全一致时直接命中。
		if ruleURL == requestedURL {
			return true
		}
		// 仅支持结尾 "*" 的前缀匹配，例如 "/healthz/*" 覆盖该前缀下的所有路径。
		if strings.HasSuffix(ruleURL, "*") && strings.HasPrefix(requestedURL, strings.TrimRight(ruleURL, "*")) {
			return true
		}
	}

	return false
}

// CompactString 返回规则的紧凑字符串表示，用于权限提升（escalation）相关的错误信息。
func CompactString(r rbacv1.PolicyRule) string {
	formatStringParts := []string{}
	formatArgs := []interface{}{}
	if len(r.APIGroups) > 0 {
		formatStringParts = append(formatStringParts, "APIGroups:%q")
		formatArgs = append(formatArgs, r.APIGroups)
	}
	if len(r.Resources) > 0 {
		formatStringParts = append(formatStringParts, "Resources:%q")
		formatArgs = append(formatArgs, r.Resources)
	}
	if len(r.NonResourceURLs) > 0 {
		formatStringParts = append(formatStringParts, "NonResourceURLs:%q")
		formatArgs = append(formatArgs, r.NonResourceURLs)
	}
	if len(r.ResourceNames) > 0 {
		formatStringParts = append(formatStringParts, "ResourceNames:%q")
		formatArgs = append(formatArgs, r.ResourceNames)
	}
	if len(r.Verbs) > 0 {
		formatStringParts = append(formatStringParts, "Verbs:%q")
		formatArgs = append(formatArgs, r.Verbs)
	}
	formatString := "{" + strings.Join(formatStringParts, ", ") + "}"
	return fmt.Sprintf(formatString, formatArgs...)
}

// SortableRuleSlice 是可排序的 PolicyRule 切片，便于对规则做确定性输出。
type SortableRuleSlice []rbacv1.PolicyRule

// Len 返回规则数量。
func (s SortableRuleSlice) Len() int { return len(s) }

// Swap 交换两个位置的规则。
func (s SortableRuleSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

// Less 按规则的字符串表示排序，保证输出顺序稳定、便于比较。
func (s SortableRuleSlice) Less(i, j int) bool {
	return strings.Compare(s[i].String(), s[j].String()) < 0
}
