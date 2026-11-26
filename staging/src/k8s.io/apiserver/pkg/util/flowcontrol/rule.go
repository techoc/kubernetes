/*
Copyright 2019 The Kubernetes Authors.

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

package flowcontrol

import (
	"strings"

	flowcontrol "k8s.io/api/flowcontrol/v1"
	"k8s.io/apiserver/pkg/authentication/serviceaccount"
	"k8s.io/apiserver/pkg/authentication/user"
	"k8s.io/apiserver/pkg/endpoints/request"
)

// 判断给定请求与 FlowSchema 是否匹配。双方都不会被修改。
//
// FlowSchema 的规则之间是“或”关系：命中任一规则即匹配；调用方按优先级
// 顺序遍历 flowSchemas，因此这里只关心单张 schema 内部。
func matchesFlowSchema(digest RequestDigest, flowSchema *flowcontrol.FlowSchema) bool {
	for _, policyRule := range flowSchema.Spec.Rules {
		if matchesPolicyRule(digest, &policyRule) {
			return true
		}
	}
	return false
}

// matchesPolicyRule 要求 subject 与 resource/non-resource 两类维度同时匹配；
// 资源型规则与非资源型规则互不相通。
func matchesPolicyRule(digest RequestDigest, policyRule *flowcontrol.PolicyRulesWithSubjects) bool {
	if !matchesASubject(digest.User, policyRule.Subjects) {
		return false
	}
	if digest.RequestInfo.IsResourceRequest {
		return matchesAResourceRule(digest.RequestInfo, policyRule.ResourceRules)
	}
	return matchesANonResourceRule(digest.RequestInfo, policyRule.NonResourceRules)
}

func matchesASubject(user user.Info, subjects []flowcontrol.Subject) bool {
	for _, subject := range subjects {
		if matchesSubject(user, subject) {
			return true
		}
	}
	return false
}

// matchesSubject 按 Kind 分派；未知 Kind 一律不匹配（校验层已保证不会出现）。
func matchesSubject(user user.Info, subject flowcontrol.Subject) bool {
	switch subject.Kind {
	case flowcontrol.SubjectKindUser:
		return subject.User != nil && (subject.User.Name == flowcontrol.NameAll || subject.User.Name == user.GetName())
	case flowcontrol.SubjectKindGroup:
		if subject.Group == nil {
			return false
		}
		seek := subject.Group.Name
		if seek == "*" {
			return true
		}
		for _, userGroup := range user.GetGroups() {
			if userGroup == seek {
				return true
			}
		}
		return false
	case flowcontrol.SubjectKindServiceAccount:
		if subject.ServiceAccount == nil {
			return false
		}
		if subject.ServiceAccount.Name == flowcontrol.NameAll {
			return serviceAccountMatchesNamespace(subject.ServiceAccount.Namespace, user.GetName())
		}
		return serviceaccount.MatchesUsername(subject.ServiceAccount.Namespace, subject.ServiceAccount.Name, user.GetName())
	default:
		return false
	}
}

// serviceAccountMatchesNamespace 检查 service account 用户名是否匹配命名空间，且不
// 产生分配。用已知字符串检查 SA 命名空间时应使用此函数。
// 摘自 `k8s.io/apiserver/pkg/authentication/serviceaccount::MatchesUsername`，简化为不检查名字部分。
func serviceAccountMatchesNamespace(namespace string, username string) bool {
	const (
		ServiceAccountUsernamePrefix    = "system:serviceaccount:"
		ServiceAccountUsernameSeparator = ":"
	)
	if !strings.HasPrefix(username, ServiceAccountUsernamePrefix) {
		return false
	}
	username = username[len(ServiceAccountUsernamePrefix):]

	if !strings.HasPrefix(username, namespace) {
		return false
	}
	username = username[len(namespace):]

	return strings.HasPrefix(username, ServiceAccountUsernameSeparator)
}

func matchesAResourceRule(ri *request.RequestInfo, rules []flowcontrol.ResourcePolicyRule) bool {
	for _, rr := range rules {
		if matchesResourcePolicyRule(ri, rr) {
			return true
		}
	}
	return false
}

// matchesResourcePolicyRule 依次校验 verb、resource/subresource、group 与
// 命名空间/集群作用域；命名空间为空表示这是集群作用域请求，只看 ClusterScope。
func matchesResourcePolicyRule(ri *request.RequestInfo, policyRule flowcontrol.ResourcePolicyRule) bool {
	if !matchPolicyRuleVerb(policyRule.Verbs, ri.Verb) {
		return false
	}
	if !matchPolicyRuleResource(policyRule.Resources, ri.Resource, ri.Subresource) {
		return false
	}
	if !matchPolicyRuleAPIGroup(policyRule.APIGroups, ri.APIGroup) {
		return false
	}
	if len(ri.Namespace) == 0 {
		return policyRule.ClusterScope
	}
	return containsString(ri.Namespace, policyRule.Namespaces, flowcontrol.NamespaceEvery)
}

func matchesANonResourceRule(ri *request.RequestInfo, rules []flowcontrol.NonResourcePolicyRule) bool {
	for _, rr := range rules {
		if matchesNonResourcePolicyRule(ri, rr) {
			return true
		}
	}
	return false
}

func matchesNonResourcePolicyRule(ri *request.RequestInfo, policyRule flowcontrol.NonResourcePolicyRule) bool {
	if !matchPolicyRuleVerb(policyRule.Verbs, ri.Verb) {
		return false
	}
	return matchPolicyRuleNonResourceURL(policyRule.NonResourceURLs, ri.Path)
}

func matchPolicyRuleVerb(policyRuleVerbs []string, requestVerb string) bool {
	return containsString(requestVerb, policyRuleVerbs, flowcontrol.VerbAll)
}

// matchPolicyRuleNonResourceURL 支持两类写法：完全匹配（含 "*" 表示全部）与
// 以 "*" 结尾的前缀匹配；前缀匹配时要求以 "/" 为边界，避免 /healthz 误配 /healthzfoo。
func matchPolicyRuleNonResourceURL(policyRuleRequestURLs []string, requestPath string) bool {
	for _, rulePath := range policyRuleRequestURLs {
		if rulePath == flowcontrol.NonResourceAll || rulePath == requestPath {
			return true
		}
		rulePrefix := strings.TrimSuffix(rulePath, "*")
		if !strings.HasSuffix(rulePrefix, "/") {
			rulePrefix = rulePrefix + "/"
		}
		if strings.HasPrefix(requestPath, rulePrefix) {
			return true
		}
	}
	return false
}

func matchPolicyRuleAPIGroup(policyRuleAPIGroups []string, requestAPIGroup string) bool {
	return containsString(requestAPIGroup, policyRuleAPIGroups, flowcontrol.APIGroupAll)
}

// rsJoin 把 resource 与 subresource 拼成规则中使用的 "resource/subresource" 形式。
func rsJoin(requestResource, requestSubresource string) string {
	seekString := requestResource
	if requestSubresource != "" {
		seekString = requestResource + "/" + requestSubresource
	}
	return seekString
}

// matchPolicyRuleResource 按 "resource/subresource" 整体匹配规则列表，
// 因此 "pods/log" 这样的规则只命中 log 子资源。
func matchPolicyRuleResource(policyRuleRequestResources []string, requestResource, requestSubresource string) bool {
	return containsString(rsJoin(requestResource, requestSubresource), policyRuleRequestResources, flowcontrol.ResourceAll)
}

// 若 `x` 或 `wildcard` 在 `list` 中，containsString 返回 true。
// wildcard 不是用来匹配 `x` 的模式，它的存在表示
// 调用方希望 `x` 的所有值都匹配该列表。
//
// 此函数假设 wildcard 在列表中时是唯一成员，这由校验保证。
func containsString(x string, list []string, wildcard string) bool {
	if len(list) == 1 && list[0] == wildcard {
		return true
	}
	for _, y := range list {
		if x == y {
			return true
		}
	}
	return false
}
