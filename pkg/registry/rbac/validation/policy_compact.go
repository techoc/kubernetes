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
	"reflect"

	rbacv1 "k8s.io/api/rbac/v1"
)

type simpleResource struct {
	Group             string
	Resource          string
	ResourceNameExist bool
	ResourceName      string
}

// CompactRules 将 APIGroup/Resource/ResourceName 完全相同、仅 Verbs 不同的简单规则合并，便于展示精简的缺失权限列表。

// 复杂规则原样保留；简单规则按资源键聚合 verbs，避免在授权错误中重复列出几乎相同的规则。
func CompactRules(rules []rbacv1.PolicyRule) ([]rbacv1.PolicyRule, error) {
	compacted := make([]rbacv1.PolicyRule, 0, len(rules))

	simpleRules := map[simpleResource]*rbacv1.PolicyRule{}
	for _, rule := range rules {
		if resource, isSimple := isSimpleResourceRule(&rule); isSimple {
			if existingRule, ok := simpleRules[resource]; ok {
				// 将当前规则中的 verbs 合并到已有的同资源简单规则中。
				if existingRule.Verbs == nil {
					existingRule.Verbs = []string{}
				}
				existingRule.Verbs = append(existingRule.Verbs, rule.Verbs...)
			} else {
				// 复制当前规则，作为后续聚合同一资源其他 verbs 的目标对象。
				simpleRules[resource] = rule.DeepCopy()
			}
		} else {
			compacted = append(compacted, rule)
		}
	}

	// 简单资源规则合并完成后，将结果追加到精简后的规则列表。
	for _, simpleRule := range simpleRules {
		compacted = append(compacted, *simpleRule)
	}

	return compacted, nil
}

// isSimpleResourceRule 判断规则是否只包含 verbs、单个 APIGroup、单个 Resource 和至多一个 ResourceName，不含其他属性。
func isSimpleResourceRule(rule *rbacv1.PolicyRule) (simpleResource, bool) {
	resource := simpleResource{}

	// 如果规则包含复杂属性，立即返回，避免不必要的内存分配和高成本比较。
	if len(rule.ResourceNames) > 1 || len(rule.NonResourceURLs) > 0 {
		return resource, false
	}
	// 如果规则包含多个 APIGroup 或 Resource，则不能安全合并，直接返回。
	if len(rule.APIGroups) != 1 || len(rule.Resources) != 1 {
		return resource, false
	}

	// 进一步确认规则除 APIGroup、Resource、Verb 和 ResourceName 外没有其他字段。
	simpleRule := &rbacv1.PolicyRule{APIGroups: rule.APIGroups, Resources: rule.Resources, Verbs: rule.Verbs, ResourceNames: rule.ResourceNames}
	if !reflect.DeepEqual(simpleRule, rule) {
		return resource, false
	}

	if len(rule.ResourceNames) == 0 {
		resource = simpleResource{Group: rule.APIGroups[0], Resource: rule.Resources[0], ResourceNameExist: false}
	} else {
		resource = simpleResource{Group: rule.APIGroups[0], Resource: rule.Resources[0], ResourceNameExist: true, ResourceName: rule.ResourceNames[0]}
	}

	return resource, true
}
