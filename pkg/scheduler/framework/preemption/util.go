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

package preemption

import (
	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// PodTerminatingByPreemption 判断 Pod 是否因调度器抢占而处于终止状态。
func PodTerminatingByPreemption(p *v1.Pod) bool {
	if p.DeletionTimestamp == nil {
		return false
	}

	for _, condition := range p.Status.Conditions {
		if condition.Type == v1.DisruptionTarget {
			return condition.Status == v1.ConditionTrue && condition.Reason == v1.PodReasonPreemptionByScheduler
		}
	}
	return false
}

// MoreImportantVictim 判断两个抢占单元中哪一个更重要，依次适用以下规则：
//
//  1. 优先级：优先级更高者更重要。
//
//  2. 工作负载类型：CompositePodGroup 比 PodGroup 重要，PodGroup 又比单个 Pod 重要，
//     以尽量保持组完整性。
//
//  3. 运行时长/启动时间（仅单个 Pod）：启动更早（运行更久）的 Pod 更重要，
//     体现"先到先服务"。
//
//  4. 组规模（PodGroup 与 CompositePodGroup）：同类型时成员更多者更重要，
//     避免重新调度大规模作业的高代价。
//
//  5. 启动时间（组类型的兜底）：类型与规模都相同时，启动更早（拥有最老 Pod）的组更重要。
func MoreImportantVictim(vi1, vi2 Victim) bool {
	if vi1.Priority() != vi2.Priority() {
		return vi1.Priority() > vi2.Priority()
	}

	rank1 := victimRank(vi1)
	rank2 := victimRank(vi2)

	if rank1 != rank2 {
		return rank1 > rank2
	}

	if len(vi1.Pods()) != len(vi2.Pods()) {
		return len(vi1.Pods()) > len(vi2.Pods())
	}

	return vi1.EarliestStartTime().Before(vi2.EarliestStartTime())
}

// victimRank 返回抢占单元的类型权重，值越大越重要。
func victimRank(vi Victim) int {
	switch vi.Type() {
	case fwk.CompositePodGroupKeyType:
		return 3
	case fwk.PodGroupKeyType:
		return 2
	default:
		return 1
	}
}

// TraverseHierarchyUp 从 startKey 开始向上遍历 PodGroup/CompositePodGroup 层级。
// 每个节点（PodGroup 或 CompositePodGroup）都会调用 visitFn；
// 若 visitFn 返回 stop=true，则立即停止遍历。父节点缺失或无法查询时同样停止。
// 调用方需确保 GenericWorkload 与 CompositePodGroup 两个特性门均已启用。
// TODO: log/return an error if there is a gap in the hierarchy.
func TraverseHierarchyUp(
	namespace string,
	startKey fwk.EntityKey,
	pgLister fwk.PodGroupLister,
	cpgLister fwk.CompositePodGroupLister,
	visitFn func(key fwk.EntityKey, pg *schedulingv1beta1.PodGroup, cpg *schedulingv1alpha3.CompositePodGroup) (stop bool),
) {
	if pgLister == nil || cpgLister == nil {
		return
	}
	currentKey := startKey
	// 层级可能存在环，必须去重；同时用最大深度兜底防止无限循环。
	visited := sets.New[fwk.EntityKey]()
	for range schedulingv1beta1.WorkloadMaxTreeDepth {
		if visited.Has(currentKey) {
			break
		}
		visited.Insert(currentKey)

		switch currentKey.Type {
		case fwk.PodGroupKeyType:
			pg, err := pgLister.Get(namespace, currentKey.Name)
			if err != nil || pg == nil {
				return
			}
			if visitFn(currentKey, pg, nil) {
				return
			}
			if pg.Spec.ParentCompositePodGroupName == nil {
				return
			}
			currentKey = fwk.CompositePodGroupKey(namespace, *pg.Spec.ParentCompositePodGroupName)

		case fwk.CompositePodGroupKeyType:
			cpg, err := cpgLister.Get(namespace, currentKey.Name)
			if err != nil || cpg == nil {
				return
			}
			if visitFn(currentKey, nil, cpg) {
				return
			}
			if cpg.Spec.ParentCompositePodGroupName == nil {
				return
			}
			currentKey = fwk.CompositePodGroupKey(namespace, *cpg.Spec.ParentCompositePodGroupName)
		}
	}
}

// GetPodPriority 返回 Pod 的有效抢占优先级。若 Pod 属于某个 PodGroup/CompositePodGroup 层级，
// 则返回该层级根组的优先级；若 podGroupLister 为 nil 或 Pod 不属于任何组，则返回 Pod 自身优先级。
// 若 compositePodGroupLister 为 nil 或层级中某个父组查不到，则退回到最近一次成功解析的组优先级
// （若一次都没成功，则退回 Pod 自身优先级）。
// TODO: log/return an error if there is a gap in the hierarchy.
func GetPodPriority(p *v1.Pod, podGroupLister fwk.PodGroupLister, compositePodGroupLister fwk.CompositePodGroupLister) int32 {
	if p.Spec.SchedulingGroup == nil || podGroupLister == nil {
		return corev1helpers.PodPriority(p)
	}
	if compositePodGroupLister == nil {
		pg, err := podGroupLister.Get(p.Namespace, *p.Spec.SchedulingGroup.PodGroupName)
		if err != nil || pg == nil {
			return corev1helpers.PodPriority(p)
		}
		return util.PodGroupPriority(pg)
	}

	startKey := fwk.PodGroupKey(p.Namespace, *p.Spec.SchedulingGroup.PodGroupName)
	var lastPG *schedulingv1beta1.PodGroup
	var lastCPG *schedulingv1alpha3.CompositePodGroup

	TraverseHierarchyUp(p.Namespace, startKey, podGroupLister, compositePodGroupLister, func(key fwk.EntityKey, pg *schedulingv1beta1.PodGroup, cpg *schedulingv1alpha3.CompositePodGroup) bool {
		if pg != nil {
			lastPG = pg
			lastCPG = nil
		} else if cpg != nil {
			lastCPG = cpg
			lastPG = nil
		}
		return false
	})

	if lastCPG != nil {
		return util.CompositePodGroupPriority(lastCPG)
	}
	if lastPG != nil {
		return util.PodGroupPriority(lastPG)
	}
	return corev1helpers.PodPriority(p)
}

// FilterVictimsWithPDBViolation 按抢占时是否会违反 PDB，
// 把 victims 分为 violatingVictims 与 nonViolatingVictims 两组。
func FilterVictimsWithPDBViolation[T Victim](victims []T, pdbs []*policy.PodDisruptionBudget) (violatingVictims []ViolatingVictim[T], nonViolatingVictims []T) {
	pdbsAllowed := make([]int32, len(pdbs))
	podIsViolating := func(pod *v1.Pod) bool {
		if len(pod.Labels) == 0 {
			return false
		}

		for i, pdb := range pdbs {
			if pdb.Namespace != pod.Namespace {
				continue
			}
			selector, err := metav1.LabelSelectorAsSelector(pdb.Spec.Selector)
			if err != nil {
				// 选择器非法，视为不匹配任何 Pod。
				continue
			}
			// nil 或空选择器不匹配任何对象。
			if selector.Empty() || !selector.Matches(labels.Set(pod.Labels)) {
				continue
			}

			// 已出现在 DisruptedPods 中说明 apiserver 已处理过该 Pod，不算新的违反。
			if _, exist := pdb.Status.DisruptedPods[pod.Name]; exist {
				continue
			}
			// 仅对未计入 DisruptedPods 的 PDB 扣减预算，否则会重复扣减。
			pdbsAllowed[i]--
			// 找到匹配的 PDB。
			if pdbsAllowed[i] < 0 {
				return true
			}
		}

		return false
	}

	for i, pdb := range pdbs {
		pdbsAllowed[i] = pdb.Status.DisruptionsAllowed
	}

	// Victim 是原子单位：其中任一 Pod 被驱逐会违反 PDB，则整个 Victim 都算违反。
	// PodGroup 不允许部分抢占，不能只赦免违反 PDB 的 Pod 而驱逐其余成员。
	for _, victim := range victims {
		violatingPods := 0

		for _, pi := range victim.Pods() {
			if podIsViolating(pi.GetPod()) {
				violatingPods++
			}
		}
		if violatingPods > 0 {
			violatingVictims = append(violatingVictims, ViolatingVictim[T]{
				Victim:       victim,
				ViolateCount: violatingPods,
			})
		} else {
			nonViolatingVictims = append(nonViolatingVictims, victim)
		}
	}

	return violatingVictims, nonViolatingVictims
}
