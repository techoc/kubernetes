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
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	policylisters "k8s.io/client-go/listers/policy/v1"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/metrics"

	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
)

// PodGroupEvaluator 是抢占评估器，处理以 PodGroup 为抢占者、整个集群为域的抢占场景。
type PodGroupEvaluator struct {
	Handle                         fwk.Handle
	pdbLister                      policylisters.PodDisruptionBudgetLister
	podGroupSnapshot               fwk.PodGroupLister
	compositePodGroupSnapshot      fwk.CompositePodGroupLister
	enablePodGroupPreemptionPolicy bool

	Executor *Executor
}

// NewPodGroupEvaluator 创建一个 PodGroup 抢占评估器。
func NewPodGroupEvaluator(fh fwk.Handle, executor *Executor, fts feature.Features) *PodGroupEvaluator {
	evaluator := &PodGroupEvaluator{
		Handle:                         fh,
		pdbLister:                      fh.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister(),
		podGroupSnapshot:               fh.MutableSnapshotSharedLister().PodGroups(),
		enablePodGroupPreemptionPolicy: fts.EnablePodGroupPreemptionPolicy,
		Executor:                       executor,
	}
	// 仅特性开启时才取组合 PodGroup 快照，避免不必要的初始化。
	if fts.EnableCompositePodGroup {
		evaluator.compositePodGroupSnapshot = fh.MutableSnapshotSharedLister().CompositePodGroups()
	}
	return evaluator
}

// evaluate 只评估并选出受害者，不实际执行抢占。
func (ev *PodGroupEvaluator) evaluate(ctx context.Context, preemptor *podGroupPreemptor, domain *domain, podGroupSchedulingFunc fwk.PodGroupSchedulingFunc) (res *selectVictimsResult, status *fwk.Status) {
	startTime := time.Now()
	defer func() {
		metrics.PreemptionEvaluationDuration.WithLabelValues("podgroup", status.Code().String()).Observe(metrics.SinceInSeconds(startTime))
	}()

	pdbs, err := getPodDisruptionBudgets(ev.pdbLister)
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("failed to get pod disruption budgets: %w", err))
	}

	return ev.selectVictimsOnDomain(ctx, preemptor, domain, pdbs, podGroupSchedulingFunc)
}

// Preempt 实现以 PodGroup 为抢占者、整个集群为域的抢占逻辑：
// 从集群中抢占 Pod，为整个 PodGroup 腾出足够空间。
// 返回 PodGroupPreemptorResult，包含组内每个 Pod 的提名节点映射及整个抢占过程的状态。
// 该逻辑会修改 Handle 提供的 NodeInfo，因此调用方需在调用前备份、调用后回滚。
// podGroupSchedulingFunc 在移除所有受害者后仅调用一次，用于检查修改后的节点状态下
// PodGroup 是否可调度；随后逻辑会在假定抢占者 Pod 已就位的前提下，尽量恢复更多受害者。
func (ev *PodGroupEvaluator) Preempt(ctx context.Context, pgInfo fwk.PodGroupInfo, podGroupSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// 工作负载感知抢占以整个集群为域。这里不复制 NodeInfo 快照：
	// 这些节点要与传入的 PodGroup 调度算法共享同一份可变状态。
	domain, err := newDomainForWorkloadPreemption(logger, ev.Handle.MutableSnapshotSharedLister(), ev.podGroupSnapshot, ev.compositePodGroupSnapshot, "cluster-domain")
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("failed to create domain: %w", err))
	}
	preemptor := newPodGroupPreemptor(pgInfo, ev.enablePodGroupPreemptionPolicy)

	// 先确认抢占者具备抢占资格，避免无谓的受害者筛选。
	if ok, msg := ev.preemptorEligibleToPreemptOthers(ctx, preemptor); !ok {
		logger.V(5).Info("Preemptor is not eligible for preemption", "preemptor", preemptor.getObj(), "type", preemptor.getType(), "reason", msg)
		return nil, fwk.NewStatus(fwk.Unschedulable, msg)
	}

	if ev.isOngoingPreemption(ctx, preemptor, domain.Nodes()) {
		return &fwk.PodGroupPostFilterResult{NominatingInfos: buildCurrentNominatingInfos(preemptor)}, fwk.NewStatus(fwk.Success, "ongoing preemption on nominated nodes")
	}

	res, status := ev.evaluate(ctx, preemptor, domain, podGroupSchedulingFunc)
	if !status.IsSuccess() {
		return nil, status
	}
	candidate := &candidate{
		victims:                res.victims,
		numPodGroupDisruptions: res.numPodGroupDisruptions,
		name:                   "cluster",
	}
	status = ev.Executor.actuatePodGroupPreemption(ctx, candidate, pgInfo, names.DefaultPreemption)
	if status.IsSuccess() {
		status = fwk.NewStatus(fwk.Success, fmt.Sprintf("found a placement for podgroup, preempting %d victims", len(res.victims.Pods)))
	}
	return &fwk.PodGroupPostFilterResult{NominatingInfos: res.nominatedNodeNames}, status
}

// selectVictimsResult 保存受害者筛选结果：提名节点、被破坏的 PodGroup 数量及受害者列表。
type selectVictimsResult struct {
	nominatedNodeNames     map[types.NamespacedName]*fwk.NominatingInfo
	numPodGroupDisruptions int
	victims                *extenderv1.Victims
}

// selectVictimsOnDomain 选出一组受害者，移除它们可为抢占者腾出足够空间。
// 优先选择不受 PDB 保护的受害者，以尽量减少对可用性的影响。
func (ev *PodGroupEvaluator) selectVictimsOnDomain(
	ctx context.Context,
	preemptor *podGroupPreemptor,
	domain *domain,
	pdbs []*policy.PodDisruptionBudget,
	podGroupSchedulingFunc fwk.PodGroupSchedulingFunc) (*selectVictimsResult, *fwk.Status) {
	logger := klog.FromContext(ctx)

	nameToNode := domain.Nodes()

	mutableLister := ev.Handle.MutableSnapshotSharedLister()

	// removePods 从快照中移除所有受害者。
	// 必须在 podGroupSchedulingFunc 之前调用：后者会创建空的 CycleState，
	// 并通过为抢占者 Pod 运行 PreFilter 插件来填充，无需我们更新任何 cycle state。
	removePods := func(v *DomainVictim) error {
		for _, pi := range v.Pods() {
			if err := mutableLister.RemovePod(logger, pi.GetPod(), pi.GetPod().Spec.NodeName); err != nil {
				return err
			}
		}
		return nil
	}

	// addVictimPodsWithPreFilter 模拟把受害者的 Pod 加回快照，
	// 并对抢占者所有有效候选分配调用 PreFilterExtensionAddPod()。
	// 传给 RunPreFilterExtensionAddPod 的 NodeInfo 已包含被加回的受害者 Pod。
	addVictimPodsWithPreFilter := func(v *DomainVictim, preemptorAssignments []fwk.ProposedAssignment) error {
		for _, pi := range v.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if err := mutableLister.AddPod(pi, pi.GetPod().Spec.NodeName); err != nil {
				return err
			}
			for _, assignment := range preemptorAssignments {
				status := ev.Handle.RunPreFilterExtensionAddPod(ctx, assignment.GetCycleState(), assignment.GetPod(), pi, nodeInfo)
				if !status.IsSuccess() {
					return status.AsError()
				}
			}
		}
		return nil
	}

	// removeVictimPodsWithPreFilter 从快照中移除所有受害者，
	// 并对所有抢占者 Pod 调用 PreFilterExtensionRemovePod(victim)。
	// 传给 RunPreFilterExtensionRemovePod 的 NodeInfo 已移除受害者 Pod。
	removeVictimPodsWithPreFilter := func(v *DomainVictim, preemptorAssignments []fwk.ProposedAssignment) error {
		for _, pi := range v.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			if err := mutableLister.RemovePod(logger, pi.GetPod(), pi.GetPod().Spec.NodeName); err != nil {
				return err
			}
			for _, assignment := range preemptorAssignments {
				status := ev.Handle.RunPreFilterExtensionRemovePod(ctx, assignment.GetCycleState(), assignment.GetPod(), pi, nodeInfo)
				if !status.IsSuccess() {
					return status.AsError()
				}
			}
		}
		return nil
	}

	var potentialVictims []*DomainVictim
	allPossiblyAffectedVictims := domain.GetAllPossibleVictims()
	for _, victim := range allPossiblyAffectedVictims {
		if ev.isPreemptionAllowed(victim, preemptor) {
			potentialVictims = append(potentialVictims, victim)
		}
	}

	// 没有可抢占对象，直接判定为不可调度且该状态无法通过再排队解决。
	if len(potentialVictims) == 0 {
		return nil, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming preemptor")
	}

	// 作为受害者的 PodGroup，其 Pod 可能分布在当前域之外。
	// 需要把这些外部节点的信息补充进来，才能正确模拟移除后的调度。
	for _, victim := range potentialVictims {
		for name, nodeInfo := range victim.AffectedNodes() {
			_, ok := nameToNode[name]
			if !ok {
				nameToNode[name] = nodeInfo
			}
		}

		if err := removePods(victim); err != nil {
			return nil, fwk.AsStatus(err)
		}
	}

	// 即使移除全部潜在受害者仍调度失败，说明抢占帮不上忙，直接返回失败状态。
	podGroupAssignments, status := podGroupSchedulingFunc(ctx)
	if !status.IsSuccess() {
		return nil, status
	}

	sort.Slice(potentialVictims, func(i, j int) bool {
		return MoreImportantVictim(potentialVictims[i], potentialVictims[j])
	})

	violatingVictims, nonViolatingVictims := FilterVictimsWithPDBViolation(potentialVictims, pdbs)
	numViolatingVictim := 0

	validAssignment := make([]fwk.ProposedAssignment, 0, len(podGroupAssignments.ProposedAssignments))

	// 只保留已分配到具体节点的抢占者 Pod，未分配的不参与后续模拟。
	for _, assignment := range podGroupAssignments.ProposedAssignments {
		if assignment.GetNodeName() != "" {
			validAssignment = append(validAssignment, assignment)
		}
	}

	// reprieveVictim 尝试以 PodGroup 为单位整体恢复一个受害者：
	// 把该受害者的所有 Pod 加回快照和抢占者的 CycleState，
	// 再按调度周期中的相同顺序，对抢占者的每个候选分配运行 FilterPlugins。
	// 全部通过则返回 true。
	// 这里复用的 CycleState 来自“无受害者集群”上的那次调度，因此第 N 个抢占者的状态
	// 是在“前面抢占者均已假定并预留资源、且不知道后续抢占者”的前提下创建的。
	reprieveVictim := func(v *DomainVictim, preemptorAssignments []fwk.ProposedAssignment) (fits bool, err error) {
		if err = addVictimPodsWithPreFilter(v, preemptorAssignments); err != nil {
			return false, err
		}
		cleanupFns := []func() error{}
		defer func() {
			for i := len(cleanupFns) - 1; i >= 0; i-- {
				if cleanupErr := cleanupFns[i](); cleanupErr != nil {
					err = errors.Join(err, cleanupErr)
				}
			}
		}()
		fits = true
		for _, assignment := range preemptorAssignments {
			nodeInfo := nameToNode[assignment.GetNodeName()]
			s := ev.Handle.RunFilterPluginsWithNominatedPods(ctx, assignment.GetCycleState(), assignment.GetPod(), nodeInfo)
			if !s.IsSuccess() {
				if err = removeVictimPodsWithPreFilter(v, preemptorAssignments); err != nil {
					return false, err
				}
				if l := logger.V(6); l.Enabled() {
					l.Info("Pods are potential preemption victims on domain", "pods", toPodNames(v.Pods()), "domain", domain.GetName())
				}
				return false, nil
			}
			// 模拟假定抢占者 Pod 并预留有状态插件的资源。
			// 无需把该 Pod 加入后续抢占者的 CycleState，因为这些状态创建时
			// 已经假定所有前序抢占者就位。
			if err = mutableLister.AddPod(assignment.GetPodInfo(), assignment.GetNodeName()); err != nil {
				return false, err
			}
			ev.Handle.RunReservePluginsReserve(ctx, assignment.GetCycleState(), assignment.GetPod(), nodeInfo.Node().GetName())
			cleanupFns = append(cleanupFns, func() error {
				if ev.Handle.RunReservePluginsUnreserve(ctx, assignment.GetCycleState(), assignment.GetPod(), nodeInfo.Node().GetName()); err != nil {
					return err
				}
				return mutableLister.RemovePod(logger, assignment.GetPod(), assignment.GetNodeName())
			})
		}
		return fits, nil
	}

	// 尽量恢复更多受害者：先处理违反 PDB 的，再处理未违反的；
	// 两类都从优先级最高的受害者开始尝试恢复。
	var victimsToPreempt []*DomainVictim
	for _, violatingVictim := range violatingVictims {
		v := violatingVictim.Victim
		if fits, err := reprieveVictim(v, validAssignment); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			victimsToPreempt = append(victimsToPreempt, v)
			numViolatingVictim += violatingVictim.ViolateCount
		}
	}

	for _, v := range nonViolatingVictims {
		if fits, err := reprieveVictim(v, validAssignment); err != nil {
			return nil, fwk.AsStatus(err)
		} else if !fits {
			victimsToPreempt = append(victimsToPreempt, v)
		}
	}

	sort.Slice(victimsToPreempt, func(i, j int) bool {
		return MoreImportantVictim(victimsToPreempt[i], victimsToPreempt[j])
	})
	numPodGroupDisruptions := 0
	var podsToPreempt []*v1.Pod
	for _, v := range victimsToPreempt {
		if v.IsGroup() {
			numPodGroupDisruptions++
		}
		for _, pi := range v.Pods() {
			podsToPreempt = append(podsToPreempt, pi.GetPod())
		}
	}

	v := &extenderv1.Victims{
		Pods:             podsToPreempt,
		NumPDBViolations: int64(numViolatingVictim),
	}
	n := make(map[types.NamespacedName]*fwk.NominatingInfo)
	for _, p := range validAssignment {
		pod := p.GetPod()
		podKey := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		n[podKey] = &fwk.NominatingInfo{
			NominatingMode:    fwk.ModeOverride,
			NominatedNodeName: p.GetNodeName(),
		}
	}

	return &selectVictimsResult{
		nominatedNodeNames:     n,
		victims:                v,
		numPodGroupDisruptions: numPodGroupDisruptions,
	}, nil
}

// toPodNames 把 Pod 列表格式化为 "namespace/name" 逗号分隔的字符串，仅用于日志。
func toPodNames(pods []fwk.PodInfo) string {
	names := make([]string, len(pods))
	for i, p := range pods {
		names[i] = p.GetPod().Namespace + "/" + p.GetPod().Name
	}
	return strings.Join(names, ",")
}

// isPreemptionAllowed 判断给定受害者是否可被抢占者抢占：
// 只有优先级低于抢占者的对象才可能被抢占。
func (ev *PodGroupEvaluator) isPreemptionAllowed(victim Victim, preemptor *podGroupPreemptor) bool {
	return victim.Priority() < preemptor.priority
}

// preemptorEligibleToPreemptOthers 判断抢占者是否有资格抢占其他 Pod。
// 返回布尔值表示是否具备资格；不具备时，字符串给出具体原因。
func (ev *PodGroupEvaluator) preemptorEligibleToPreemptOthers(_ context.Context, preemptor *podGroupPreemptor) (bool, string) {
	if preemptor.PreemptionPolicy() == schedulingapi.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}

	return true, ""
}

// isOngoingPreemption 检查该 PodGroup 的 Pod 所提名的节点上是否正在进行抢占，
// 避免对同一批节点重复发起抢占。
func (ev *PodGroupEvaluator) isOngoingPreemption(_ context.Context, preemptor *podGroupPreemptor, nameToNode map[string]fwk.NodeInfo) bool {
	nominatedNodes := sets.New[string]()
	for _, pod := range preemptor.Members() {
		if len(pod.Status.NominatedNodeName) > 0 {
			nominatedNodes.Insert(pod.Status.NominatedNodeName)
		}
	}

	for nomNodeName := range nominatedNodes {
		if nodeInfo, exists := nameToNode[nomNodeName]; exists {
			for _, p := range nodeInfo.GetPods() {
				if GetPodPriority(p.GetPod(), ev.podGroupSnapshot, ev.compositePodGroupSnapshot) < preemptor.Priority() && PodTerminatingByPreemption(p.GetPod()) {
					return true
				}
			}
		}
	}

	return false
}

// buildCurrentNominatingInfos 根据抢占者 Pod 当前设置的 NNN（提名节点名）
// 构造提名信息映射。
func buildCurrentNominatingInfos(preemptor *podGroupPreemptor) map[types.NamespacedName]*fwk.NominatingInfo {
	n := make(map[types.NamespacedName]*fwk.NominatingInfo)
	for _, pod := range preemptor.Members() {
		podKey := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		n[podKey] = &fwk.NominatingInfo{
			NominatingMode:    fwk.ModeOverride,
			NominatedNodeName: pod.Status.NominatedNodeName,
		}
	}
	return n
}
