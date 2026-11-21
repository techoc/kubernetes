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
	"fmt"
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/kubernetes/pkg/apis/admissionregistration"
)

// WarningsForValidatingWebhookRules 检查验证 webhook 配置中的规则，
// 当规则显式点名了 excludedResources 中的资源（准入 webhook 不会拦截的虚拟资源）时返回警告。
// 被排除的资源集合由调用方传入，避免这里依赖服务端专用包。
func WarningsForValidatingWebhookRules(webhooks []admissionregistration.ValidatingWebhook, excludedResources []schema.GroupResource) []string {
	excluded := sets.New(excludedResources...)
	webhooksPath := field.NewPath("webhooks")
	var warnings []string
	for i := range webhooks {
		warnings = append(warnings, warningsForExcludedRules(webhooksPath.Index(i).Child("rules"), webhooks[i].Rules, excluded)...)
	}
	return warnings
}

// WarningsForMutatingWebhookRules 检查变更 webhook 配置中的规则，
// 当规则显式点名了 excludedResources 中的资源时返回警告。
func WarningsForMutatingWebhookRules(webhooks []admissionregistration.MutatingWebhook, excludedResources []schema.GroupResource) []string {
	excluded := sets.New(excludedResources...)
	webhooksPath := field.NewPath("webhooks")
	var warnings []string
	for i := range webhooks {
		warnings = append(warnings, warningsForExcludedRules(webhooksPath.Index(i).Child("rules"), webhooks[i].Rules, excluded)...)
	}
	return warnings
}

// warningsForExcludedRules 遍历规则列表，为其中每条命中的被排除资源生成警告。
func warningsForExcludedRules(rulesPath *field.Path, rules []admissionregistration.RuleWithOperations, excluded sets.Set[schema.GroupResource]) []string {
	var warnings []string
	for i := range rules {
		for _, gr := range excludedResourcesNamedByRule(rules[i].APIGroups, rules[i].Resources, excluded) {
			warnings = append(warnings, fmt.Sprintf("%s: %s is excluded from admission webhooks; this rule will have no effect", rulesPath.Index(i), gr))
		}
	}
	return warnings
}

// excludedResourcesNamedByRule 返回某条规则显式点名的被排除 GroupResource 列表。
// 如果规则在 apiGroups 或 resources 中使用了通配符（"*"），则不产生警告，
// 因为此时规则针对被排除资源的意图不明确。
// 不检查 apiVersions，因为被排除资源的所有版本都会被排除。
func excludedResourcesNamedByRule(apiGroups, resources []string, excluded sets.Set[schema.GroupResource]) []schema.GroupResource {
	if slices.Contains(apiGroups, "*") || slices.Contains(resources, "*") {
		return nil
	}
	// 用 set 去重，使同一 group/resource 在规则中重复出现时只报告一次，
	// 同时保留规则中列出的顺序。
	seen := sets.New[schema.GroupResource]()
	var matched []schema.GroupResource
	for _, group := range apiGroups {
		for _, resource := range resources {
			gr := schema.GroupResource{Group: group, Resource: resource}
			if excluded.Has(gr) && !seen.Has(gr) {
				seen.Insert(gr)
				matched = append(matched, gr)
			}
		}
	}
	return matched
}
