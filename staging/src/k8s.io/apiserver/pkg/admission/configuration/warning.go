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

package configuration

import (
	"slices"

	v1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// logExcludedResourcesForValidatingWebhook 针对规则中点名了「被准入 webhook 排除资源」的
// ValidatingWebhookConfiguration 输出一条提示日志；excludedWebhookResources 为空时什么都不做。
// 被排除的资源（例如租约类资源）即使出现在 webhook 规则里也不会触发调用，
// 这里只记录日志而不拒绝配置，避免破坏既有清单的可用性。
func logExcludedResourcesForValidatingWebhook(name string, webhooks []v1.ValidatingWebhook, excludedWebhookResources sets.Set[schema.GroupResource]) {
	// 排除集合为空说明当前没有任何资源被排除，无需扫描规则。
	if excludedWebhookResources.Len() == 0 {
		return
	}
	var rules []v1.RuleWithOperations
	for _, w := range webhooks {
		rules = append(rules, w.Rules...)
	}
	logExcludedWebhookResources("ValidatingWebhookConfiguration", name, rules, excludedWebhookResources)
}

// logExcludedResourcesForMutatingWebhook 针对规则中点名了「被准入 webhook 排除资源」的
// MutatingWebhookConfiguration 输出一条提示日志；excludedWebhookResources 为空时什么都不做。
// 与 validating 版本对称，仅提示配置中的规则不会生效。
func logExcludedResourcesForMutatingWebhook(name string, webhooks []v1.MutatingWebhook, excludedWebhookResources sets.Set[schema.GroupResource]) {
	if excludedWebhookResources.Len() == 0 {
		return
	}
	var rules []v1.RuleWithOperations
	for _, w := range webhooks {
		rules = append(rules, w.Rules...)
	}
	logExcludedWebhookResources("MutatingWebhookConfiguration", name, rules, excludedWebhookResources)
}

// logExcludedWebhookResources 汇总所有规则命中的被排除资源，并按字典序输出一条结构化日志。
// 去重是必要的：同一资源可能被多条规则或同一配置中的多个 webhook 重复命名。
func logExcludedWebhookResources(kind, name string, rules []v1.RuleWithOperations, excludedWebhookResources sets.Set[schema.GroupResource]) {
	excluded := sets.New[schema.GroupResource]()
	for _, r := range rules {
		excluded.Insert(excludedResourcesNamedByRule(r.APIGroups, r.Resources, excludedWebhookResources)...)
	}
	if excluded.Len() == 0 {
		return
	}
	resources := make([]string, 0, excluded.Len())
	for gr := range excluded {
		resources = append(resources, gr.String())
	}
	// 排序保证日志稳定，便于 grep 与测试比对。
	slices.Sort(resources)
	klog.InfoS("Admission webhook configuration names resources that are excluded from admission webhooks; the matching rules will have no effect",
		"kind", kind, "name", name, "excludedResources", resources)
}

// excludedResourcesNamedByRule 返回某条规则显式点名的那些被排除 GroupResource。
// 若在 apiGroups 或 resources 中使用了通配符 "*"，则不提示：通配符规则对被排除资源的意图并不明确，
// 可能是有意覆盖全部资源，提示它只会制造噪音。
// 也不看 apiVersions，因为排除按 GroupResource 粒度生效，资源的各个版本都被排除。
func excludedResourcesNamedByRule(apiGroups, resources []string, excludedWebhookResources sets.Set[schema.GroupResource]) []schema.GroupResource {
	// 通配符规则语义含糊，跳过。
	if slices.Contains(apiGroups, "*") || slices.Contains(resources, "*") {
		return nil
	}
	var matched []schema.GroupResource
	for _, group := range apiGroups {
		for _, resource := range resources {
			gr := schema.GroupResource{Group: group, Resource: resource}
			if excludedWebhookResources.Has(gr) {
				matched = append(matched, gr)
			}
		}
	}
	return matched
}
