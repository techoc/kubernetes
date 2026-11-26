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

package policy

import (
	"strings"

	"k8s.io/apiserver/pkg/apis/audit"
	auditinternal "k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/authorization/authorizer"
)

const (
	// DefaultAuditLevel 是没有策略规则命中时使用的默认审计级别
	DefaultAuditLevel = audit.LevelNone
)

// NewPolicyRuleEvaluator 创建一个新的策略规则求值器
//
// 构造时把全局 OmitStages 合并进每条规则，使后续匹配无需再重复计算；
// 这也意味着规则级别的 OmitStages 是"全局 + 规则"的并集，只能扩大省略范围。
func NewPolicyRuleEvaluator(policy *audit.Policy) auditinternal.PolicyRuleEvaluator {
	for i, rule := range policy.Rules {
		policy.Rules[i].OmitStages = unionStages(policy.OmitStages, rule.OmitStages)
	}
	return &policyRuleEvaluator{*policy}
}

// unionStages 对阶段列表求并集；返回顺序不保证稳定，但调用方只做集合判断。
func unionStages(stageLists ...[]audit.Stage) []audit.Stage {
	m := make(map[audit.Stage]bool)
	for _, sl := range stageLists {
		for _, s := range sl {
			m[s] = true
		}
	}
	result := make([]audit.Stage, 0, len(m))
	for key := range m {
		result = append(result, key)
	}
	return result
}

// NewFakePolicyRuleEvaluator 创建一个假的策略规则求值器，
// 对所有请求返回固定级别（用于测试）。
func NewFakePolicyRuleEvaluator(level audit.Level, stage []audit.Stage) auditinternal.PolicyRuleEvaluator {
	return &fakePolicyRuleEvaluator{level, stage}
}

// policyRuleEvaluator 按顺序匹配策略规则，返回第一条命中的审计配置。
type policyRuleEvaluator struct {
	audit.Policy
}

// EvaluatePolicyRule 返回请求应采用的审计级别。
// 规则按配置顺序匹配，首个命中者生效；全部未命中时使用全局默认级别
// （None，即不记录），因此"未配置规则"不会产生任何审计事件。
func (p *policyRuleEvaluator) EvaluatePolicyRule(attrs authorizer.Attributes) auditinternal.RequestAuditConfig {
	for _, rule := range p.Rules {
		if ruleMatches(&rule, attrs) {
			return auditinternal.RequestAuditConfig{
				Level:             rule.Level,
				OmitStages:        rule.OmitStages,
				OmitManagedFields: isOmitManagedFields(&rule, p.OmitManagedFields),
			}
		}
	}

	return auditinternal.RequestAuditConfig{
		Level:             DefaultAuditLevel,
		OmitStages:        p.OmitStages,
		OmitManagedFields: p.OmitManagedFields,
	}
}

// isOmitManagedFields 判断是否从写入 API 审计日志的请求与响应体中
// 省略 managed fields。
// 若策略规则中指定了 OmitManagedFields，则它覆盖
// Policy.OmitManagedFields 中的全局默认值。
func isOmitManagedFields(policyRule *audit.PolicyRule, policyDefault bool) bool {
	if policyRule.OmitManagedFields == nil {
		return policyDefault
	}

	return *policyRule.OmitManagedFields
}

// 检查规则是否匹配请求属性
//
// 所有已设置的过滤字段都必须匹配（AND 语义）；未设置的字段视为通配。
// 资源规则与非资源规则互斥：一旦指定 Namespaces/Resources 就不再匹配
// 非资源 URL，反之亦然。
func ruleMatches(r *audit.PolicyRule, attrs authorizer.Attributes) bool {
	user := attrs.GetUser()
	if len(r.Users) > 0 {
		if user == nil || !hasString(r.Users, user.GetName()) {
			return false
		}
	}
	if len(r.UserGroups) > 0 {
		if user == nil {
			return false
		}
		matched := false
		for _, group := range user.GetGroups() {
			if hasString(r.UserGroups, group) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	if len(r.Verbs) > 0 {
		if !hasString(r.Verbs, attrs.GetVerb()) {
			return false
		}
	}

	if len(r.Namespaces) > 0 || len(r.Resources) > 0 {
		return ruleMatchesResource(r, attrs)
	}

	if len(r.NonResourceURLs) > 0 {
		return ruleMatchesNonResource(r, attrs)
	}

	return true
}

// 检查规则的非资源 URL 是否匹配请求属性
func ruleMatchesNonResource(r *audit.PolicyRule, attrs authorizer.Attributes) bool {
	if attrs.IsResourceRequest() {
		return false
	}

	path := attrs.GetPath()
	for _, spec := range r.NonResourceURLs {
		if pathMatches(path, spec) {
			return true
		}
	}

	return false
}

// 检查路径是否匹配路径规格
//
// 支持三种形式：精确路径、尾随 "*" 的前缀路径、单独的 "*"。
// 前缀比较前先去掉所有 "*"，因此 "/foo*" 与 "/foo/**" 等价。
func pathMatches(path, spec string) bool {
	// 允许通配匹配
	if spec == "*" {
		return true
	}
	// 允许精确匹配
	if spec == path {
		return true
	}
	// 允许匹配尾随 * 的子路径
	if strings.HasSuffix(spec, "*") && strings.HasPrefix(path, strings.TrimRight(spec, "*")) {
		return true
	}
	return false
}

// 检查规则的资源字段是否匹配请求属性
//
// 资源项支持通配组合："*"、resource、"*/subresource"、"resource/*"。
// ResourceNames 为空表示匹配任意名称，否则按名称精确过滤。
func ruleMatchesResource(r *audit.PolicyRule, attrs authorizer.Attributes) bool {
	if !attrs.IsResourceRequest() {
		return false
	}

	if len(r.Namespaces) > 0 {
		if !hasString(r.Namespaces, attrs.GetNamespace()) { // Non-namespaced resources use the empty string.
			return false
		}
	}
	if len(r.Resources) == 0 {
		return true
	}

	apiGroup := attrs.GetAPIGroup()
	resource := attrs.GetResource()
	subresource := attrs.GetSubresource()
	combinedResource := resource
	// 若带子资源，策略中的资源必须匹配 "(resource)/(subresource)"
	if subresource != "" {
		combinedResource = resource + "/" + subresource
	}

	name := attrs.GetName()

	for _, gr := range r.Resources {
		if gr.Group == apiGroup || gr.Group == "*" {
			if len(gr.Resources) == 0 {
				return true
			}
			for _, res := range gr.Resources {
				if len(gr.ResourceNames) == 0 || hasString(gr.ResourceNames, name) {
					// 匹配 "*"
					if res == combinedResource || res == "*" {
						return true
					}
					// 匹配 "*/subresource"
					if len(subresource) > 0 && strings.HasPrefix(res, "*/") && subresource == strings.TrimPrefix(res, "*/") {
						return true
					}
					// 匹配 "resource/*"
					if strings.HasSuffix(res, "/*") && resource == strings.TrimSuffix(res, "/*") {
						return true
					}
				}
			}
		}
	}
	return false
}

// 检查字符串切片是否包含某个字符串的工具函数
//
// 规则列表通常很短，线性扫描比构造 set 更划算。
func hasString(slice []string, value string) bool {
	for _, s := range slice {
		if s == value {
			return true
		}
	}
	return false
}

// fakePolicyRuleEvaluator 对所有请求返回固定的级别与省略阶段，用于测试。
type fakePolicyRuleEvaluator struct {
	level audit.Level
	stage []audit.Stage
}

// EvaluatePolicyRule 忽略属性，返回预设配置。
func (f *fakePolicyRuleEvaluator) EvaluatePolicyRule(_ authorizer.Attributes) auditinternal.RequestAuditConfig {
	return auditinternal.RequestAuditConfig{
		Level:      f.level,
		OmitStages: f.stage,
	}
}
