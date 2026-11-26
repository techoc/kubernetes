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

package rbac

import (
	"reflect"
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/util/sets"
)

type simpleResource struct {
	Group             string
	Resource          string
	ResourceNameExist bool
	ResourceName      string
}

// 合并只有 verb 不同、其余属性相同的单 API 组/资源规则。
// 这是快速检查，适合处理 Covers 结果拆解出的“缺失规则”列表。
func CompactRules(rules []rbacv1.PolicyRule) ([]rbacv1.PolicyRule, error) {
	compacted := make([]rbacv1.PolicyRule, 0, len(rules))

	simpleRules := map[simpleResource]*rbacv1.PolicyRule{}
	for _, rule := range rules {
		if resource, isSimple := isSimpleResourceRule(&rule); isSimple {
			if existingRule, ok := simpleRules[resource]; ok {
				// 把新 verb 加到已有的简单资源规则上。
				if existingRule.Verbs == nil {
					existingRule.Verbs = []string{}
				}
				existingVerbs := sets.New[string](existingRule.Verbs...)
				for _, verb := range rule.Verbs {
					if !existingVerbs.Has(verb) {
						existingRule.Verbs = append(existingRule.Verbs, verb)
					}
				}

			} else {
				// 复制规则，用于累积匹配的简单资源规则。
				simpleRules[resource] = rule.DeepCopy()
			}
		} else {
			compacted = append(compacted, rule)
		}
	}

	// 合并简单资源规则后加入压缩结果。
	for _, simpleRule := range simpleRules {
		compacted = append(compacted, *simpleRule)
	}

	return compacted, nil
}

// 仅含 verbs、单个资源、单个 API 组、至多一个资源名且无其他属性时返回 true。
func isSimpleResourceRule(rule *rbacv1.PolicyRule) (simpleResource, bool) {
	resource := simpleResource{}

	// 规则含“复杂”属性时直接返回，避免分配和昂贵比较。
	if len(rule.ResourceNames) > 1 || len(rule.NonResourceURLs) > 0 {
		return resource, false
	}
	// 有多个 API 组或资源时直接返回。
	if len(rule.APIGroups) != 1 || len(rule.Resources) != 1 {
		return resource, false
	}

	// 检查规则是否只含 APIGroups/Resources/Verbs/ResourceNames。
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

// 把一条规则拆成等价规则列表，每条最多一个 verb、一个资源
// 和一个资源名。
func BreakdownRule(rule rbacv1.PolicyRule) []rbacv1.PolicyRule {
	subrules := []rbacv1.PolicyRule{}
	for _, group := range rule.APIGroups {
		for _, resource := range rule.Resources {
			for _, verb := range rule.Verbs {
				if len(rule.ResourceNames) > 0 {
					for _, resourceName := range rule.ResourceNames {
						subrules = append(subrules, rbacv1.PolicyRule{APIGroups: []string{group}, Resources: []string{resource}, Verbs: []string{verb}, ResourceNames: []string{resourceName}})
					}

				} else {
					subrules = append(subrules, rbacv1.PolicyRule{APIGroups: []string{group}, Resources: []string{resource}, Verbs: []string{verb}})
				}

			}
		}
	}

	// 非资源 URL 特殊，因为它只与 verb 组合。
	for _, nonResourceURL := range rule.NonResourceURLs {
		for _, verb := range rule.Verbs {
			subrules = append(subrules, rbacv1.PolicyRule{NonResourceURLs: []string{nonResourceURL}, Verbs: []string{verb}})
		}
	}

	return subrules
}

// 用于规则切片排序。
type SortableRuleSlice []rbacv1.PolicyRule

func (s SortableRuleSlice) Len() int      { return len(s) }
func (s SortableRuleSlice) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s SortableRuleSlice) Less(i, j int) bool {
	return strings.Compare(s[i].String(), s[j].String()) < 0
}
