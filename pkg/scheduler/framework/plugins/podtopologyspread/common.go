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

package podtopologyspread

import (
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	v1helper "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/utils/ptr"
)

// topologySpreadConstraint 是 v1.TopologySpreadConstraint 的内部表示，
// selector 已在此处预先解析好，避免调度热路径重复解析。
// 字段导出仅为便于测试中直接比较。
type topologySpreadConstraint struct {
	MaxSkew            int32
	TopologyKey        string
	Selector           labels.Selector
	MinDomains         int32
	NodeAffinityPolicy v1.NodeInclusionPolicy
	NodeTaintsPolicy   v1.NodeInclusionPolicy
}

// matchNodeInclusionPolicies 判断节点是否满足约束的节点亲和性与污点纳入策略。
// 任一策略设为 Honor 且节点不满足时返回 false。
func (tsc *topologySpreadConstraint) matchNodeInclusionPolicies(logger klog.Logger, pod *v1.Pod, node *v1.Node, require nodeaffinity.RequiredNodeAffinity, enableComparisonOperators bool) bool {
	if tsc.NodeAffinityPolicy == v1.NodeInclusionPolicyHonor {
		// 为保持向后兼容，忽略解析错误；解析失败按不匹配处理。
		if match, _ := require.Match(node); !match {
			return false
		}
	}

	if tsc.NodeTaintsPolicy == v1.NodeInclusionPolicyHonor {
		if _, untolerated := v1helper.FindMatchingUntoleratedTaint(logger, node.Spec.Taints, pod.Spec.Tolerations, helper.DoNotScheduleTaintsFilterFunc(), enableComparisonOperators); untolerated {
			return false
		}
	}
	return true
}

// buildDefaultConstraints 基于默认约束以及匹配该 Pod 的 Service、RC、RS、SS
// 推导出的选择器，为 Pod 构建默认约束列表。
// 推导出空选择器时返回 nil，表示没有可用的默认约束。
func (pl *PodTopologySpread) buildDefaultConstraints(p *v1.Pod, action v1.UnsatisfiableConstraintAction) ([]topologySpreadConstraint, error) {
	constraints, err := pl.filterTopologySpreadConstraints(pl.defaultConstraints, p.Labels, action)
	if err != nil || len(constraints) == 0 {
		return nil, err
	}
	selector := helper.DefaultSelector(p, pl.services, pl.replicationCtrls, pl.replicaSets, pl.statefulSets)
	if selector.Empty() {
		return nil, nil
	}
	for i := range constraints {
		constraints[i].Selector = selector
	}
	return constraints, nil
}

// nodeLabelsMatchSpreadConstraints 检查节点标签是否包含所有约束的拓扑键。
// 缺少任一拓扑键即返回 false，因为该节点无法参与对应域的均匀分布统计。
func nodeLabelsMatchSpreadConstraints(nodeLabels map[string]string, constraints []topologySpreadConstraint) bool {
	for _, c := range constraints {
		if _, ok := nodeLabels[c.TopologyKey]; !ok {
			return false
		}
	}
	return true
}

// filterTopologySpreadConstraints 筛选 WhenUnsatisfiable 等于 action 的约束并转为内部表示。
// 同时处理 matchLabelKeys 的合并以及节点纳入策略的默认值。
func (pl *PodTopologySpread) filterTopologySpreadConstraints(constraints []v1.TopologySpreadConstraint, podLabels map[string]string, action v1.UnsatisfiableConstraintAction) ([]topologySpreadConstraint, error) {
	var result []topologySpreadConstraint
	for _, c := range constraints {
		// Filter 与 Score 扩展点分别处理 DoNotSchedule 和 ScheduleAnyway，
		// 避免同一约束被重复计分或强制过滤。
		if c.WhenUnsatisfiable == action {
			selector, err := metav1.LabelSelectorAsSelector(c.LabelSelector)
			if err != nil {
				return nil, err
			}

			if pl.enableMatchLabelKeysInPodTopologySpread && len(c.MatchLabelKeys) > 0 {
				matchLabels := make(labels.Set)
				for _, labelKey := range c.MatchLabelKeys {
					// 只取 Pod 实际携带的键；缺失的键不加入选择器，
					// 否则会把 labelSelector 的限制意外收紧。
					if value, ok := podLabels[labelKey]; ok {
						matchLabels[labelKey] = value
					}
				}
				if len(matchLabels) > 0 {
					selector = mergeLabelSetWithSelector(matchLabels, selector)
				}
			}

			tsc := topologySpreadConstraint{
				MaxSkew:            c.MaxSkew,
				TopologyKey:        c.TopologyKey,
				Selector:           selector,
				MinDomains:         ptr.Deref(c.MinDomains, 1),   // nil 时默认为 1
				NodeAffinityPolicy: v1.NodeInclusionPolicyHonor,  // nil 时默认遵循节点亲和性
				NodeTaintsPolicy:   v1.NodeInclusionPolicyIgnore, // nil 时默认忽略节点污点
			}
			// 特性门控开启时，用户显式指定的策略优先于默认值。
			if pl.enableNodeInclusionPolicyInPodTopologySpread {
				if c.NodeAffinityPolicy != nil {
					tsc.NodeAffinityPolicy = *c.NodeAffinityPolicy
				}
				if c.NodeTaintsPolicy != nil {
					tsc.NodeTaintsPolicy = *c.NodeTaintsPolicy
				}
			}
			result = append(result, tsc)
		}
	}
	return result, nil
}

// mergeLabelSetWithSelector 把 matchLabelKeys 对应的标签要求合并进原选择器。
// 原选择器无法拆解为 requirements 时直接返回原选择器，避免改变其语义。
func mergeLabelSetWithSelector(matchLabels labels.Set, s labels.Selector) labels.Selector {
	mergedSelector := labels.SelectorFromSet(matchLabels)

	requirements, ok := s.Requirements()
	if !ok {
		return s
	}

	for _, r := range requirements {
		mergedSelector = mergedSelector.Add(r)
	}

	return mergedSelector
}

// countPodsMatchSelector 统计命名空间内匹配选择器的 Pod 数量。
// 空选择器返回 0：约束的标签选择器为空意味着限定"无标签 Pod"，
// 但默认约束下这种情况应视为没有匹配项，避免把全部 Pod 算入同一域。
func countPodsMatchSelector(podInfos []fwk.PodInfo, selector labels.Selector, ns string) int {
	if selector.Empty() {
		return 0
	}
	count := 0
	for _, p := range podInfos {
		// 跳过正在终止的 Pod（见 #87621），它们即将释放资源，
		// 不应影响均匀分布的计算。
		if p.GetPod().DeletionTimestamp != nil || p.GetPod().Namespace != ns {
			continue
		}
		if selector.Matches(labels.Set(p.GetPod().Labels)) {
			count++
		}
	}
	return count
}

// podLabelsMatchSpreadConstraints 判断给定标签是否匹配任一约束的选择器。
// 只要匹配一个约束，Pod 就需要参与拓扑分布统计。
func podLabelsMatchSpreadConstraints(constraints []topologySpreadConstraint, labels labels.Set) bool {
	for _, c := range constraints {
		if c.Selector.Matches(labels) {
			return true
		}
	}
	return false
}
