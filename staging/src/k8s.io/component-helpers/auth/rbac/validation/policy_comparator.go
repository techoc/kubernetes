/*
Copyright 2016 The Kubernetes Authors.

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

// Package validation 提供 RBAC 策略规则的覆盖关系判断。
//
// 这类判断用于“某个角色是否被另一个角色覆盖”之类的授权推导，
// 例如聚合 ClusterRole 或提升请求权限时的前置检查。
package validation

import (
	"strings"

	rbacv1 "k8s.io/api/rbac/v1"
)

// Covers 判断 ownerRules 在“允许的操作”这一层面是否覆盖 servantRules。
// 第一个返回值表示是否全部覆盖；第二个返回值列出 ownerRules 未能覆盖的规则。
//
// 把“整条规则是否被覆盖”拆成“每个最小单元是否被单条 owner 规则覆盖”，
// 是因为 RBAC 规则里的多个 group/resource/verb 是笛卡尔积关系；若不做拆分，
// 就无法保证覆盖结论能落到某一条具体的 owner 规则上。
func Covers(ownerRules, servantRules []rbacv1.PolicyRule) (bool, []rbacv1.PolicyRule) {
	// 1. 把每条 servantRule 拆成最小的规则元组：group、verb、resource、resourceName。
	// 2. 把每个最小规则与每条 owner 规则逐一比较。由于已经拆到最原子粒度，
	//    可以保证每个最小 servant 规则要么被某一条 owner 规则完全覆盖，要么完全不被覆盖。
	// 3. 剩余的最小规则说明未被覆盖，同时正好构成需要返回的未覆盖列表。
	// TODO: 可以把未覆盖列表折叠成更易读的形式。

	subrules := []rbacv1.PolicyRule{}
	for _, servantRule := range servantRules {
		subrules = append(subrules, BreakdownRule(servantRule)...)
	}

	uncoveredRules := []rbacv1.PolicyRule{}
	for _, subrule := range subrules {
		covered := false
		for _, ownerRule := range ownerRules {
			if ruleCovers(ownerRule, subrule) {
				covered = true
				break
			}
		}

		if !covered {
			uncoveredRules = append(uncoveredRules, subrule)
		}
	}

	return (len(uncoveredRules) == 0), uncoveredRules
}

// BreakdownRule 把一条规则拆分成等价的规则列表，使每条子规则最多只含一个
// verb、一个 resource 和一个 resourceName。
//
// 拆分后每条子规则只含一个元组，Covers 才能逐条判断“是否被某一条 owner 规则单独覆盖”。
// 非资源 URL 没有 group/resource 维度，只与 verb 组合，因此需要单独处理。
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

	// 非资源 URL 的特殊之处在于它只与 verb 组合，没有 group/resource 维度。
	for _, nonResourceURL := range rule.NonResourceURLs {
		for _, verb := range rule.Verbs {
			subrules = append(subrules, rbacv1.PolicyRule{NonResourceURLs: []string{nonResourceURL}, Verbs: []string{verb}})
		}
	}

	return subrules
}

// has 线性报告 ele 是否在集合中；集合规模很小，避免为每次查找构造 map。
func has(set []string, ele string) bool {
	for _, s := range set {
		if s == ele {
			return true
		}
	}
	return false
}

// hasAll 判断 contains 是否为 set 的子集；先把 set 转成 map，
// 让多个元素的成员检查接近常数时间。
func hasAll(set, contains []string) bool {
	owning := make(map[string]struct{}, len(set))
	for _, ele := range set {
		owning[ele] = struct{}{}
	}
	for _, ele := range contains {
		if _, ok := owning[ele]; !ok {
			return false
		}
	}
	return true
}

// resourceCoversAll 判断 setResources 是否覆盖 coversResources。
// 除了全量通配符和精确匹配，RBAC 还允许 "*/subresource" 形式的通配符，
// 即对任意资源的某个子资源授权，这里必须单独识别这种语义。
func resourceCoversAll(setResources, coversResources []string) bool {
	// 只要存在全量通配符，或能逐项精确匹配所有目标资源，就直接判定覆盖。
	// 全量通配符与逐项精确匹配能直接覆盖所有情况，先走快速路径。
	if has(setResources, rbacv1.ResourceAll) || hasAll(setResources, coversResources) {
		return true
	}

	for _, path := range coversResources {
		// 精确命中则该资源已被覆盖，跳过后续的子资源判断。
		if has(setResources, path) {
			continue
		}
		// 没有 "/" 说明是普通资源，此时不存在 "*/subresource" 的等价授权。
		if !strings.Contains(path, "/") {
			return false
		}
		// 只看子资源部分：授权 "*/scale" 即覆盖任意资源的 scale 子资源。
		tokens := strings.SplitN(path, "/", 2)
		resourceToCheck := "*/" + tokens[1]
		if !has(setResources, resourceToCheck) {
			return false
		}
	}

	return true
}

// nonResourceURLsCoversAll 要求 covers 中每个 URL 都能被 set 中某条 URL 覆盖。
// 这里的判断是“或”：多条权限叠加即可覆盖多个 URL。
func nonResourceURLsCoversAll(set, covers []string) bool {
	for _, path := range covers {
		covered := false
		for _, owner := range set {
			if nonResourceURLCovers(owner, path) {
				covered = true
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

// nonResourceURLCovers 判断 ownerPath 是否覆盖 subPath。
// 除精确相等外，RBAC 只支持尾部的 '*' 前缀匹配，所以这里的 "*" 必须出现在末尾，
// 并且匹配的是以去星号前缀开头的所有路径；中间的 '*' 不构成通配符。
func nonResourceURLCovers(ownerPath, subPath string) bool {
	if ownerPath == subPath {
		return true
	}
	return strings.HasSuffix(ownerPath, "*") && strings.HasPrefix(subPath, strings.TrimRight(ownerPath, "*"))
}

// ruleCovers 判断 ownerRule 是否覆盖 subRule。
// ownerRule 可能含多个 verb、resource 和 resourceName，而 subRule 是最多各含一个的最小单元。
//
// 各维度必须同时满足才算覆盖：RBAC 的授权是各字段的合取，任一维度不匹配就不构成覆盖。
// resourceNames 的语义稍有不同：owner 不限制名称时视为覆盖任意名称；
// 若 subrule 不限定名称而 owner 限定了，则不能算覆盖。
func ruleCovers(ownerRule, subRule rbacv1.PolicyRule) bool {
	verbMatches := has(ownerRule.Verbs, rbacv1.VerbAll) || hasAll(ownerRule.Verbs, subRule.Verbs)
	groupMatches := has(ownerRule.APIGroups, rbacv1.APIGroupAll) || hasAll(ownerRule.APIGroups, subRule.APIGroups)
	resourceMatches := resourceCoversAll(ownerRule.Resources, subRule.Resources)
	nonResourceURLMatches := nonResourceURLsCoversAll(ownerRule.NonResourceURLs, subRule.NonResourceURLs)

	resourceNameMatches := false

	// subrule 未指定 resourceNames 时，owner 也不能指定，否则 owner 的授权范围更窄。
	if len(subRule.ResourceNames) == 0 {
		resourceNameMatches = (len(ownerRule.ResourceNames) == 0)
	} else {
		// subrule 指定了名称时，owner 要么不做限制，要么包含所有这些名称。
		resourceNameMatches = (len(ownerRule.ResourceNames) == 0) || hasAll(ownerRule.ResourceNames, subRule.ResourceNames)
	}

	return verbMatches && groupMatches && resourceMatches && resourceNameMatches && nonResourceURLMatches
}
