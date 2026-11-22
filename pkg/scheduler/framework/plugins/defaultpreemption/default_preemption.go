/*
Copyright 2020 The Kubernetes Authors.

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

package defaultpreemption

import (
	"context"
	"fmt"
	"math/rand"
	"sort"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/apis/config/validation"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/feature"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/kubernetes/pkg/scheduler/framework/preemption"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// Name 是插件在注册表和配置中使用的名字。
const Name = names.DefaultPreemption

// IsEligiblePodFunc 可赋给 DefaultPreemption，用于扩展抢占资格的过滤规则；
// 这只是对“受害者优先级低于抢占者”这一内部要求的补充。
// 任何自定义实现都应允许系统服务抢占普通 Pod，避免系统 Pod 无处可去。
//
// 对 PodGroup 受害者，该函数对每个受影响的节点调用一次：nodeInfo 对应当前节点，
// victim 始终代表跨所有节点的整个 PodGroup。只有每个受影响节点都返回 true 才算合格。
type IsEligiblePodFunc func(nodeInfo fwk.NodeInfo, victim preemption.Victim, preemptor *v1.Pod) bool

// MoreImportantVictimFunc 可赋给 DefaultPreemption：若第一个受害者比第二个更重要
// （因此应更晚被抢占），返回 true。
// 出于性能考虑，寻找可抢占节点的方式是：先从节点移除所有合格受害者，检查抢占者能否放下，
// 再从最重要的受害者开始逐个放回仍能容纳的受害者。
// 默认不考量抢占者与受害者之间的 Pod 亲和性，因为可互相抢占的 Pod 之间设亲和性本就不推荐。
type MoreImportantVictimFunc func(victim1, victim2 preemption.Victim) bool

// podGroupEvaluator 抽象出 PodGroup 抢占的执行入口。
type podGroupEvaluator interface {
	Preempt(ctx context.Context, pgInfo fwk.PodGroupInfo, podGroupSchedulingFunc fwk.PodGroupSchedulingFunc) (*fwk.PodGroupPostFilterResult, *fwk.Status)
}

// DefaultPreemption 是实现抢占逻辑的 PostFilter 插件。
type DefaultPreemption struct {
	fh   fwk.Handle
	fts  feature.Features
	args config.DefaultPreemptionArgs

	Executor          *preemption.Executor
	Evaluator         *preemption.Evaluator
	pgLister          fwk.PodGroupLister
	pgSnapshotLister  fwk.PodGroupLister
	cpgSnapshotLister fwk.CompositePodGroupLister
	podGroupEvaluator podGroupEvaluator

	// IsEligiblePod 判断受害者（单个 Pod/PodGroup）能否被抢占者抢占。
	// 这是对“受害者优先级更低”这一内部要求的补充过滤。任何自定义实现都应允许系统服务
	// 抢占普通 Pod，避免系统 Pod 无处可去。
	IsEligiblePod IsEligiblePodFunc

	// MoreImportantVictim 用于就地按重要性从高到低排序合格受害者，越重要的越不会被抢占。
	//
	// 默认采用级联规则评估候选受害者：先看优先级，始终优先保留高优先级者。
	// 禁用 GenericWorkload 时退化为优先保留运行更久的 Pod；启用 WAP 时还会考量受害者类型，
	// 相对独立 Pod 更偏向保留 PodGroup；多个 PodGroup 之间先比组大小，最后以启动时间兜底。
	MoreImportantVictim MoreImportantVictimFunc
}

var _ fwk.PostFilterPlugin = &DefaultPreemption{}
var _ fwk.PreEnqueuePlugin = &DefaultPreemption{}

// Name 返回插件名，用于日志等场合。
func (pl *DefaultPreemption) Name() string {
	return Name
}

// New 初始化并返回一个新插件，保留具体类型以便外部修改其可配置字段。
func New(_ context.Context, dpArgs runtime.Object, fh fwk.Handle, fts feature.Features) (*DefaultPreemption, error) {
	args, ok := dpArgs.(*config.DefaultPreemptionArgs)
	if !ok {
		return nil, fmt.Errorf("got args of type %T, want *DefaultPreemptionArgs", dpArgs)
	}
	if err := validation.ValidateDefaultPreemptionArgs(nil, args); err != nil {
		return nil, err
	}

	pl := DefaultPreemption{
		fh:   fh,
		fts:  fts,
		args: *args,
	}
	pl.Executor = preemption.NewExecutor(fh, fts)
	pl.Evaluator = preemption.NewEvaluator(Name, fh, &pl, pl.Executor)

	if pl.fts.EnableGenericWorkload {
		pl.pgLister = fh.PodGroupManager().PodGroups()
		pl.pgSnapshotLister = fh.SnapshotSharedLister().PodGroups()
		pl.podGroupEvaluator = preemption.NewPodGroupEvaluator(fh, pl.Executor, pl.fts)
	}

	if pl.fts.EnableCompositePodGroup {
		pl.cpgSnapshotLister = fh.SnapshotSharedLister().CompositePodGroups()
	}

	// 默认行为：除“受害者优先级更低”外不做额外过滤。
	pl.IsEligiblePod = func(nodeInfo fwk.NodeInfo, victim preemption.Victim, preemptor *v1.Pod) bool {
		return true
	}

	// 默认的受害者排序规则，依次为：
	// 1. 优先级更高者优先保留；
	// 2. 工作负载类型：CompositePodGroup > PodGroup > 独立 Pod；
	// 3. 独立 Pod 之间：启动时间更早（运行更久）者优先保留；
	// 4. 组类型之间：组更大者优先，其次启动时间更早者。
	pl.MoreImportantVictim = preemption.MoreImportantVictim

	return &pl, nil
}

// PostFilter 在 postFilter 扩展点调用。
func (pl *DefaultPreemption) PostFilter(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	defer func() {
		metrics.PreemptionAttempts.Inc()
	}()

	result, status := pl.Evaluator.Preempt(ctx, state, pod, m)
	msg := status.Message()
	if len(msg) > 0 {
		return result, fwk.NewStatus(status.Code(), "preemption: "+msg)
	}
	return result, status
}

// PreEnqueue 在异步抢占进行期间阻止相关 Pod 入队。
func (pl *DefaultPreemption) PreEnqueue(ctx context.Context, p *v1.Pod) *fwk.Status {
	if !pl.fts.EnableAsyncPreemption {
		return nil
	}
	if p.Spec.SchedulingGroup != nil && pl.fts.EnableGenericWorkload {
		var rootUID types.UID
		if !pl.fts.EnableCompositePodGroup {
			pg, err := pl.pgLister.Get(p.Namespace, *p.Spec.SchedulingGroup.PodGroupName)
			// 找不到 PodGroup 时不要阻塞 Pod：阻止无 PG 的组内 Pod 入队不是默认抢占插件的职责。
			if err != nil {
				return nil
			}
			rootUID = pg.GetUID()
		} else {
			snapshot, err := pl.fh.PodGroupManager().BuildHierarchySnapshotFromPod(p)
			// 根组无法解析时不要阻塞 Pod：阻止根组缺失的组内 Pod 入队不是默认抢占插件的职责。
			if err != nil {
				return nil
			}
			podGroupKey := fwk.PodGroupKey(p.Namespace, *p.Spec.SchedulingGroup.PodGroupName)
			rootKey, ok, err := snapshot.GetRootKeyForGroup(podGroupKey)
			if !ok || err != nil {
				return nil
			}
			switch rootKey.Type {
			case fwk.PodGroupKeyType:
				pg, err := snapshot.PodGroups().Get(rootKey.Namespace, rootKey.Name)
				if err != nil {
					return nil
				}
				rootUID = pg.GetUID()
			case fwk.CompositePodGroupKeyType:
				cpg, err := snapshot.CompositePodGroups().Get(rootKey.Namespace, rootKey.Name)
				if err != nil {
					return nil
				}
				rootUID = cpg.GetUID()
			}
		}
		if pl.Executor.IsPodGroupRunningPreemption(rootUID) {
			return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for the preemption for this pod group to be finished")
		}
		return nil
	}

	if pl.Executor.IsPodRunningPreemption(p.GetUID()) {
		return fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "waiting for the preemption for this pod to be finished")
	}
	return nil
}

// EventsToRegister 返回可能让被本插件拒绝的 Pod 变为可调度的事件。
func (pl *DefaultPreemption) EventsToRegister(_ context.Context) ([]fwk.ClusterEventWithHint, error) {
	if pl.fts.EnableAsyncPreemption {
		return []fwk.ClusterEventWithHint{
			// 注册该事件是为了通知调度队列：Pod 可能在其他 Pod 删除后解除阻塞。
			{Event: fwk.ClusterEvent{Resource: fwk.AssignedPod, ActionType: fwk.Delete}, QueueingHintFn: pl.isPodSchedulableAfterAssignedPodDeletion},
		}, nil
	}

	// 禁用异步抢占时 PreEnqueue 恒返回 nil，因此本插件不会拒绝任何 Pod。
	return nil, nil
}

// isPodSchedulableAfterAssignedPodDeletion 返回已分配 Pod 删除事件后 Pod 的入队提示，恒为 Skip。
// 这里有点绕：被本插件拒绝的 Pod 正是那些已经/正在异步抢占的 Pod，
// 它们的 pInfo.UnschedulablePlugins 中总有其他插件，抢占完成后由那些插件来解决失败原因。
// 因此抢占插件不该越俎代庖决定何时重新入队，而应交给那些插件负责。
func (pl *DefaultPreemption) isPodSchedulableAfterAssignedPodDeletion(logger klog.Logger, pod *v1.Pod, oldObj, newObj interface{}) (fwk.QueueingHint, error) {
	return fwk.QueueSkip, nil
}

// calculateNumCandidates 根据 MinCandidateNodesPercentage 与 MinCandidateNodesAbsolute
// 计算 FindCandidates 试运行需产生的候选节点数，结果不会超过 numNodes。
func (pl *DefaultPreemption) calculateNumCandidates(numNodes int32) int32 {
	n := (numNodes * pl.args.MinCandidateNodesPercentage) / 100
	if n < pl.args.MinCandidateNodesAbsolute {
		n = pl.args.MinCandidateNodesAbsolute
	}
	if n > numNodes {
		n = numNodes
	}
	return n
}

// getOffsetRand 是 GetOffsetAndNumCandidates 专用的随机源。
// 默认是 rand.Int31n，做成包变量便于单测覆盖为确定性实现。
var getOffsetRand = rand.Int31n

// GetOffsetAndNumCandidates 选取一个随机偏移量，并计算进入抢占试运行的候选数量。
func (pl *DefaultPreemption) GetOffsetAndNumCandidates(numNodes int32) (int32, int32) {
	return getOffsetRand(numNodes), pl.calculateNumCandidates(numNodes)
}

// CandidatesToVictimsMap 把候选节点映射为其受害者集合。
// 对在同一提名节点上尝试不同候选的树外抢占插件，本方法不适用。
func (pl *DefaultPreemption) CandidatesToVictimsMap(candidates []preemption.Candidate) map[string]*extenderv1.Victims {
	m := make(map[string]*extenderv1.Victims, len(candidates))
	for _, c := range candidates {
		m[c.Name()] = c.Victims()
	}
	return m
}

// SelectVictimsOnNode 找出最小的受害者集合，使抢占者能放入 nodeInfo 代表的节点（“主节点”）。
//
// 进入函数时 nodeInfo 只代表主节点。若候选受害者中有跨节点的 PodGroup，
// 本函数会在移除阶段从 victim.AffectedNodes() 发现这些节点并扩展内部 nameToNode，
// 以便 PreFilterExtension 钩子能在其上执行。主节点与远端节点的 NodeInfo 修改约定见函数体内的块注释。
func (pl *DefaultPreemption) SelectVictimsOnNode(
	ctx context.Context,
	cycleState fwk.CycleState,
	preemptor *v1.Pod,
	nodeInfo fwk.NodeInfo,
	allPossibleVictims []*preemption.DomainVictim,
	pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status) {
	logger := klog.FromContext(ctx)
	// 在一次 SelectVictimsOnNode 评估中，我们只修改主节点的 NodeInfo（即试图放入抢占者的节点）。
	// 稍后会通过 RunFilterPluginsWithNominatedPods 对主节点重跑 Filter，因此它们必须看到移除后的状态。
	//
	// 对仅因 PodGroup 受害者成员所在而受影响的远端节点，我们刻意不修改其 NodeInfo，
	// 只把远端 NodeInfo 传给 PreFilterExtension 的 Remove/AddPod，让插件更新自己维护的 CycleState
	// （如拓扑分布倾斜、Pod 亲和计数）。Filter 期间直接读远端 NodeInfo 的插件会看到旧状态；
	// 这可以接受，因为这里的 Filter 只针对主节点运行。
	mainNodeName := nodeInfo.Node().Name
	nameToNode := map[string]fwk.NodeInfo{mainNodeName: nodeInfo}

	removeVictim := func(dv *preemption.DomainVictim) error {
		for _, pi := range dv.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			// 见 SelectVictimsOnNode 的块注释：只修改主节点的 NodeInfo，
			// 远端节点仅通过 PreFilterExtension 处理。
			if pi.GetPod().Spec.NodeName == mainNodeName {
				if err := nodeInfo.RemovePod(logger, pi.GetPod()); err != nil {
					return err
				}
			}
			status := pl.fh.RunPreFilterExtensionRemovePod(ctx, cycleState, preemptor, pi, nodeInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}

		return nil
	}
	addVictim := func(pu *preemption.DomainVictim) error {
		for _, pi := range pu.Pods() {
			nodeInfo := nameToNode[pi.GetPod().Spec.NodeName]
			// 见 SelectVictimsOnNode 的块注释：只修改主节点的 NodeInfo，
			// 远端节点仅通过 PreFilterExtension 处理。
			if pi.GetPod().Spec.NodeName == mainNodeName {
				nodeInfo.AddPodInfo(pi)
			}
			status := pl.fh.RunPreFilterExtensionAddPod(ctx, cycleState, preemptor, pi, nodeInfo)
			if !status.IsSuccess() {
				return status.AsError()
			}
		}

		return nil
	}

	var potentialVictims []*preemption.DomainVictim
	for _, victim := range allPossibleVictims {
		if pl.isPreemptionAllowedAcrossAllVictimNodes(victim, preemptor) {
			potentialVictims = append(potentialVictims, victim)
		}
	}

	// 没有可抢占的受害者，节点状态未变，无需再次评估。
	if len(potentialVictims) == 0 {
		return nil, 0, fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "No preemption victims found for incoming pod")
	}

	// 第一步：移除节点上所有可抢占的受害者。
	for _, victim := range potentialVictims {
		// 若受害者是跨节点的 PodGroup，某些受影响节点可能还不在 nameToNode 中，
		// 先补进去，确保抢占时会考虑到它们。
		for name, nodeInfo := range victim.AffectedNodes() {
			if _, ok := nameToNode[name]; !ok {
				nameToNode[name] = nodeInfo
			}
		}
		if err := removeVictim(victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	// 移除所有合格受害者后仍放不下抢占者，则该节点不适合抢占，可以收尾了。
	// 唯一还能检查的是抢占者是否因与某些受害者的 Pod 亲和而调度失败，
	// 但出于性能考虑不支持该场景；对低优先级 Pod 设亲和性本就不推荐。
	if status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo); !status.IsSuccess() {
		return nil, 0, status
	}

	// 按重要性降序排列候选受害者，确保优先恢复更重要的受害者。
	sort.Slice(potentialVictims, func(i, j int) bool {
		return pl.MoreImportantVictim(potentialVictims[i], potentialVictims[j])
	})

	// 尽量多恢复受害者：先处理违反 PDB 的，再处理不违反的，且都从最重要的开始尝试。
	violatingVictims, nonViolatingVictims := preemption.FilterVictimsWithPDBViolation(potentialVictims, pdbs)
	var victims []*preemption.DomainVictim
	reprieveVictim := func(v *preemption.DomainVictim) (bool, error) {
		if err := addVictim(v); err != nil {
			return false, err
		}

		status := pl.fh.RunFilterPluginsWithNominatedPods(ctx, cycleState, preemptor, nodeInfo)
		fits := status.IsSuccess()
		if !fits {
			if err := removeVictim(v); err != nil {
				return false, err
			}
			victims = append(victims, v)
			if loggerV := logger.V(5); loggerV.Enabled() {
				var pods []klog.ObjectRef
				for _, p := range v.Pods() {
					pods = append(pods, klog.KObj(p.GetPod()))
				}
				loggerV.Info("Pods are potential preemption victims on node", "pods", pods, "node", mainNodeName)
			}
		}

		return fits, nil
	}

	numViolatingVictim := 0
	for _, violatingVictim := range violatingVictims {
		if fits, err := reprieveVictim(violatingVictim.Victim); err != nil {
			return nil, 0, fwk.AsStatus(err)
		} else if !fits {
			numViolatingVictim += violatingVictim.ViolateCount
		}
	}

	// 接着尝试恢复不违反 PDB 的受害者。
	for _, v := range nonViolatingVictims {
		if _, err := reprieveVictim(v); err != nil {
			return nil, 0, fwk.AsStatus(err)
		}
	}

	// 恢复操作后重新排序，使最终受害者列表按重要性从高到低排列。
	if len(violatingVictims) != 0 && len(nonViolatingVictims) != 0 {
		sort.Slice(victims, func(i, j int) bool { return pl.MoreImportantVictim(victims[i], victims[j]) })
	}
	var victimPods []*v1.Pod
	for _, vi := range victims {
		for _, pi := range vi.Pods() {
			victimPods = append(victimPods, pi.GetPod())
		}
	}

	return victimPods, numViolatingVictim, nil
}

// PodEligibleToPreemptOthers 返回一个布尔值和一个原因字符串：
// 布尔值表示 Pod 是否可考虑抢占其他 Pod，不可用时字符串给出原因。
// 不可用的情形包括：
//  1. Pod 的 preemptionPolicy 为 Never；
//  2. Pod 已经抢占过其他 Pod，且受害者仍在优雅终止期。
//     当前只检查其提名节点：只要该节点上还有正在终止的 Pod，就不再尝试抢占更多 Pod。
func (pl *DefaultPreemption) PodEligibleToPreemptOthers(_ context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string) {
	if pod.Spec.PreemptionPolicy != nil && *pod.Spec.PreemptionPolicy == v1.PreemptNever {
		return false, "not eligible due to preemptionPolicy=Never."
	}

	nodeInfos := pl.fh.MutableSnapshotSharedLister().NodeInfos()
	nomNodeName := pod.Status.NominatedNodeName
	if len(nomNodeName) > 0 {
		// 若过滤器认为提名节点 UnschedulableAndUnresolvable，则应允许该 Pod 再次尝试抢占。
		if nominatedNodeStatus.Code() == fwk.UnschedulableAndUnresolvable {
			return true, ""
		}

		if nodeInfo, _ := nodeInfos.Get(nomNodeName); nodeInfo != nil {
			for _, p := range nodeInfo.GetPods() {
				victim := preemption.NewPodVictim(p, pl.pgSnapshotLister, pl.cpgSnapshotLister)
				if pl.isPreemptionAllowed(nodeInfo, victim, pod) && preemption.PodTerminatingByPreemption(p.GetPod()) {
					// 提名节点上存在正在终止的 Pod。
					return false, "not eligible due to a terminating pod on the nominated node."
				}
			}
		}
	}
	return true, ""
}

// isPreemptionAllowed 判断 nodeInfo 上的受害者能否被抢占者抢占。
func (pl *DefaultPreemption) isPreemptionAllowed(nodeInfo fwk.NodeInfo, victim preemption.Victim, preemptor *v1.Pod) bool {
	// 除 IsEligiblePod 的过滤外，受害者优先级必须低于抢占者。
	return victim.Priority() < corev1helpers.PodPriority(preemptor) && pl.IsEligiblePod(nodeInfo, victim, preemptor)
}

// OrderedScoreFuncs 返回按优先级排列的节点打分函数；本插件不提供自定义打分，返回 nil。
func (pl *DefaultPreemption) OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64 {
	return nil
}

// isPreemptionAllowedAcrossAllVictimNodes 判断受害者在其所有受影响节点上都可被抢占。
func (pl *DefaultPreemption) isPreemptionAllowedAcrossAllVictimNodes(victim *preemption.DomainVictim, preemptor *v1.Pod) bool {
	if victim.Priority() >= corev1helpers.PodPriority(preemptor) {
		return false
	}

	for _, nodeInfo := range victim.AffectedNodes() {
		if !pl.IsEligiblePod(nodeInfo, victim, preemptor) {
			return false
		}
	}

	return true
}

// PodGroupPostFilter 为 PodGroup 执行默认抢占。
func (pl *DefaultPreemption) PodGroupPostFilter(ctx context.Context, state fwk.PodGroupCycleState, pgInfo fwk.PodGroupInfo, pgSchedulingFunc fwk.PodGroupSchedulingFunc) (postFilterResult *fwk.PodGroupPostFilterResult, status *fwk.Status) {
	defer func() {
		metrics.WorkloadPreemptionAttempts.WithLabelValues(status.Code().String()).Inc()
	}()

	mutableLister := pl.fh.MutableSnapshotSharedLister()
	err := mutableLister.StartMutations()
	if err != nil {
		return nil, fwk.AsStatus(fmt.Errorf("pod group preemption: failed to start mutations: %w", err))
	}
	defer func() {
		if err := mutableLister.EndMutations(); err != nil {
			status = fwk.AsStatus(fmt.Errorf("pod group preemption: failed to end mutations: %w", err))
		}
	}()

	res, status := pl.podGroupEvaluator.Preempt(ctx, pgInfo, pgSchedulingFunc)
	msg := status.Message()
	if len(msg) > 0 {
		return res, fwk.NewStatus(status.Code(), "pod group preemption: "+msg)
	}
	return res, status
}
