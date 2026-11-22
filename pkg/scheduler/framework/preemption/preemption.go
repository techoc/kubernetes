/*
Copyright 2021 The Kubernetes Authors.

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

// Package preemption 提供抢占调度框架：评估候选节点、选择被抢占的 Pod（victims）并执行抢占。
package preemption

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	policy "k8s.io/api/policy/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	corelisters "k8s.io/client-go/listers/core/v1"
	policylisters "k8s.io/client-go/listers/policy/v1"
	corev1helpers "k8s.io/component-helpers/scheduling/corev1"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// Interface 由各抢占插件实现：这些成员方法相对默认抢占逻辑可能有不同行为。
type Interface interface {
	// GetOffsetAndNumCandidates 选择一个随机偏移量，并计算试运行（dry run）抢占时需要筛选的候选节点数量。
	GetOffsetAndNumCandidates(nodes int32) (int32, int32)
	// CandidatesToVictimsMap 构造从目标节点到待抢占 Pod 列表及 PDB 违规数的映射。
	CandidatesToVictimsMap(candidates []Candidate) map[string]*extenderv1.Victims
	// PodEligibleToPreemptOthers 返回两个值：布尔值表示该 Pod 是否可考虑抢占其他 Pod，
	// 字符串说明不可抢占的原因。
	PodEligibleToPreemptOthers(ctx context.Context, pod *v1.Pod, nominatedNodeStatus *fwk.Status) (bool, string)
	// SelectVictimsOnNode 找出最小被抢占 Pod 集合，使抢占者能落到指定节点（由 nodeInfo 表示）。
	// 候选 victims（allPossibleVictims）可能包含跨节点的 Pod（如 PodGroup）。
	// 注意：cycleState 由调用方克隆，nodeInfo 是快照副本，实现可安全修改。
	SelectVictimsOnNode(ctx context.Context, cycleState fwk.CycleState, preemptor *v1.Pod, nodeInfo fwk.NodeInfo, allPossibleVictims []*DomainVictim, pdbs []*policy.PodDisruptionBudget) ([]*v1.Pod, int, *fwk.Status)
	// OrderedScoreFuncs 返回按优先级排列的打分函数，用于选出最适合执行抢占的节点。
	// 仅当出现多个节点同分时才依次调用后续函数；返回 nil 时使用默认评分函数以保持向后兼容。
	OrderedScoreFuncs(ctx context.Context, nodesToVictims map[string]*extenderv1.Victims) []func(node string) int64
}

// Evaluator 是抢占评估器，使用给定的 Interface 执行抢占逻辑。
type Evaluator struct {
	PluginName                string
	Handler                   fwk.Handle
	PodLister                 corelisters.PodLister
	PdbLister                 policylisters.PodDisruptionBudgetLister
	podGroupSnapshot          fwk.PodGroupLister
	compositePodGroupSnapshot fwk.CompositePodGroupLister

	Interface
	executor *Executor
}

// NewEvaluator 创建一个新的 Evaluator，并从 framework handle 中提取 Pod/PDB lister 与快照。
func NewEvaluator(pluginName string, fh fwk.Handle, i Interface, executor *Executor) *Evaluator {
	ev := &Evaluator{
		PluginName: pluginName,
		Handler:    fh,
		PodLister:  fh.SharedInformerFactory().Core().V1().Pods().Lister(),
		PdbLister:  fh.SharedInformerFactory().Policy().V1().PodDisruptionBudgets().Lister(),
		Interface:  i,
		executor:   executor,
	}
	if executor.fts.EnableGenericWorkload {
		ev.podGroupSnapshot = fh.MutableSnapshotSharedLister().PodGroups()
	}
	if executor.fts.EnableCompositePodGroup {
		ev.compositePodGroupSnapshot = fh.MutableSnapshotSharedLister().CompositePodGroups()
	}
	return ev
}

// evaluate 只做抢占评估（选出最佳候选），不实际执行抢占动作。
func (ev *Evaluator) evaluate(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (_ Candidate, _ *fwk.PostFilterResult, status *fwk.Status) {
	logger := klog.FromContext(ctx)
	startTime := time.Now()
	defer func() {
		metrics.PreemptionEvaluationDuration.WithLabelValues("pod", status.Code().String()).Observe(metrics.SinceInSeconds(startTime))
	}()

	// 0) 获取 <pod> 的最新版本。
	// 调度器创建时 informer 缓存已初始化，这里直接读取是安全的；
	// 但测试场景可能需要手动初始化共享 Pod informer。
	podNamespace, podName := pod.Namespace, pod.Name
	pod, err := ev.PodLister.Pods(podNamespace).Get(podName)
	if err != nil {
		logger.Error(err, "Could not get the updated preemptor pod object", "pod", klog.KRef(podNamespace, podName))
		return nil, nil, fwk.AsStatus(err)
	}

	// 1) 确认抢占者有资格抢占其他 Pod。
	nominatedNodeStatus := m.Get(pod.Status.NominatedNodeName)
	if ok, msg := ev.PodEligibleToPreemptOthers(ctx, pod, nominatedNodeStatus); !ok {
		logger.V(5).Info("Pod is not eligible for preemption", "pod", klog.KObj(pod), "reason", msg)
		return nil, nil, fwk.NewStatus(fwk.Unschedulable, msg)
	}

	// 2) 获取所有节点，供后续筛选抢占候选。
	allNodes, err := ev.Handler.MutableSnapshotSharedLister().NodeInfos().List()
	if err != nil {
		return nil, nil, fwk.AsStatus(err)
	}

	candidates, nodeToStatusMap, err := ev.findCandidates(ctx, state, allNodes, pod, m)
	if err != nil && len(candidates) == 0 {
		return nil, nil, fwk.AsStatus(err)
	}

	// 仅在完全没有可用候选时才返回 FitError：此时抢占对调度无帮助，
	// 顺便清空 Pod 原有的 nominatedNodeName 状态。
	if len(candidates) == 0 {
		logger.V(2).Info("No preemption candidate is found; preemption is not helpful for scheduling", "pod", klog.KObj(pod))
		fitError := &framework.FitError{
			Pod:         pod,
			NumAllNodes: len(allNodes),
			Diagnosis: framework.Diagnosis{
				NodeToStatus: nodeToStatusMap,
				// 此错误用于清空 nominatedNodeName，不会参与 Pod 迁移，故无需设置插件列表。
			},
		}
		fitError.Diagnosis.NodeToStatus.SetAbsentNodesStatus(fwk.NewStatus(fwk.UnschedulableAndUnresolvable, "Preemption is not helpful for scheduling"))
		// 返回空的 nominatedNodeName，以清除 Pod 可能残留的 nominatedNodeName 状态。
		return nil, framework.NewPostFilterResultWithNominatedNode(""), fwk.NewStatus(fwk.Unschedulable, fitError.Error())
	}

	// 3) 与已注册的 Extender 交互，按需过滤候选节点。
	candidates, status = ev.callExtenders(logger, pod, candidates)
	if !status.IsSuccess() {
		return nil, nil, status
	}

	// 4) 从候选中选出最优节点。
	bestCandidate := ev.SelectCandidate(ctx, candidates)
	if bestCandidate == nil || len(bestCandidate.Name()) == 0 {
		return nil, nil, fwk.NewStatus(fwk.Unschedulable, "no candidate node for preemption")
	}

	return bestCandidate, nil, fwk.NewStatus(fwk.Success)
}

// Preempt 执行抢占，返回携带建议 nominatedNodeName 的 PostFilterResult 以及 Status。
// 不同场景下 <PostFilterResult, Status> 组合的语义如下：
//
//   - <nil, Error>：暂态/罕见错误，后续调度周期可能自愈。
//
//   - <nil, Unschedulable>：属预期情况，例如抢占者正在等待 victims 完全终止。
//
//   - 上述两种情况都返回 nil PostFilterResult，以保持 Pod 的 nominatedNodeName 不变。
//
//   - <non-nil PostFilterResult, Unschedulable>：即使抢占也无法调度该 Pod。
//     此时返回非 nil 结果，由 result.NominatingMode 指示如何处理 nominatedNodeName。
//
//   - <non-nil PostFilterResult, Success>：常规成功路径，非空的 nominatedNodeName 会写入抢占者 Pod。
func (ev *Evaluator) Preempt(ctx context.Context, state fwk.CycleState, pod *v1.Pod, m fwk.NodeToStatusReader) (*fwk.PostFilterResult, *fwk.Status) {
	logger := klog.FromContext(ctx)

	bestCandidate, fitErrorResult, status := ev.evaluate(ctx, state, pod, m)
	if !status.IsSuccess() {
		return fitErrorResult, status
	}

	logger.V(2).Info("the target node for the preemption is determined", "node", bestCandidate.Name(), "pod", klog.KObj(pod))

	// 5) 真正执行抢占动作。
	if status := ev.executor.actuatePodPreemption(ctx, bestCandidate, pod, ev.PluginName); !status.IsSuccess() {
		return nil, status
	}

	return framework.NewPostFilterResultWithNominatedNode(bestCandidate.Name()), fwk.NewStatus(fwk.Success, fmt.Sprintf("found a potential placement for pod on node %v, preempting %d victims", bestCandidate.Name(), len(bestCandidate.Victims().Pods)))
}

// findCandidates 计算抢占候选切片，每个候选都可执行并使给定 <pod> 可被调度。
func (ev *Evaluator) findCandidates(ctx context.Context, state fwk.CycleState, allNodes []fwk.NodeInfo, pod *v1.Pod, m fwk.NodeToStatusReader) ([]Candidate, *framework.NodeToStatus, error) {
	if len(allNodes) == 0 {
		return nil, nil, errors.New("no nodes available")
	}
	logger := klog.FromContext(ctx)
	// 找出因不可调度（Unschedulable）而失败的节点——这些节点可通过移除 Pod 来满足调度条件。
	potentialNodes, err := m.NodesForStatusCode(ev.Handler.MutableSnapshotSharedLister().NodeInfos(), fwk.Unschedulable)
	if err != nil {
		return nil, nil, err
	}
	if len(potentialNodes) == 0 {
		logger.V(3).Info("Preemption will not help schedule pod on any node", "pod", klog.KObj(pod))
		return nil, framework.NewDefaultNodeToStatus(), nil
	}

	pdbs, err := getPodDisruptionBudgets(ev.PdbLister)
	if err != nil {
		return nil, nil, err
	}

	offset, candidatesNum := ev.GetOffsetAndNumCandidates(int32(len(potentialNodes)))
	return ev.DryRunPreemption(ctx, state, pod, potentialNodes, pdbs, offset, candidatesNum)
}

// callExtenders 调用支持抢占的 extender 过滤候选。
// 不支持抢占的 extender 虽不参与此处筛选，但之后仍可能阻止抢占者落到 nominated 节点；
// 若发生这种情况，调度器会在后续调度周期为抢占者另选节点。
func (ev *Evaluator) callExtenders(logger klog.Logger, pod *v1.Pod, candidates []Candidate) ([]Candidate, *fwk.Status) {
	extenders := ev.Handler.Extenders()
	nodeLister := ev.Handler.MutableSnapshotSharedLister().NodeInfos()
	if len(extenders) == 0 {
		return candidates, nil
	}

	// 将候选切片转成 victimsMap 以适配 Extender 接口；该转换仅适用于 nominated 节点名唯一的场景。
	victimsMap := ev.CandidatesToVictimsMap(candidates)
	if len(victimsMap) == 0 {
		return candidates, nil
	}
	for _, extender := range extenders {
		if !extender.SupportsPreemption() || !extender.IsInterested(pod) {
			continue
		}
		nodeNameToVictims, err := extender.ProcessPreemption(pod, victimsMap, nodeLister)
		if err != nil {
			if extender.IsIgnorable() {
				logger.V(2).Info("Skipped extender as it returned error and has ignorable flag set",
					"extender", extender.Name(), "err", err)
				continue
			}
			return nil, fwk.AsStatus(err)
		}
		// 校验 extender 返回的 victims 是否有效：节点至少要有一个 victim。
		for nodeName, victims := range nodeNameToVictims {
			if victims == nil || len(victims.Pods) == 0 {
				if extender.IsIgnorable() {
					delete(nodeNameToVictims, nodeName)
					logger.V(2).Info("Ignored node for which the extender didn't report victims", "node", klog.KRef("", nodeName), "extender", extender.Name())
					continue
				}
				return nil, fwk.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeName))
			}
		}
		// 用本轮结果替换 victimsMap，供后续 extender 继续作为输入使用。
		victimsMap = nodeNameToVictims

		// 节点列表已空，后续 extender 再怎么过滤也不可能有抢占，直接结束。
		if len(victimsMap) == 0 {
			break
		}
	}

	var newCandidates []Candidate
	for nodeName := range victimsMap {
		newCandidates = append(newCandidates, &candidate{
			victims: victimsMap[nodeName],
			name:    nodeName,
		})
	}
	return newCandidates, nil
}

// SelectCandidate 从给定 <candidates> 中选出最合适的候选并返回。
// 注意：导出此方法是为了便于默认抢占插件的测试。
func (ev *Evaluator) SelectCandidate(ctx context.Context, candidates []Candidate) Candidate {
	logger := klog.FromContext(ctx)

	if len(candidates) == 0 {
		return nil
	}
	if len(candidates) == 1 {
		return candidates[0]
	}

	victimsMap := ev.CandidatesToVictimsMap(candidates)
	scoreFuncs := ev.OrderedScoreFuncs(ctx, victimsMap)
	candidateNode := pickOneNodeForPreemption(logger, victimsMap, scoreFuncs)

	// 与 CandidatesToVictimsMap 同理：对于在同一 nominated 节点上试跑不同候选的外部抢占插件，此逻辑不适用。
	if victims := victimsMap[candidateNode]; victims != nil {
		return &candidate{
			victims: victims,
			name:    candidateNode,
		}
	}

	// 理论上不会走到这里。
	utilruntime.HandleErrorWithContext(ctx, nil, "Unexpected case no candidate was selected", "candidates", candidates)
	// 为不中断整个流程，兜底返回第一个候选。
	return candidates[0]
}

// getPodDisruptionBudgets 通过 lister 获取全部 PDB；lister 为 nil 时返回空。
func getPodDisruptionBudgets(pdbLister policylisters.PodDisruptionBudgetLister) ([]*policy.PodDisruptionBudget, error) {
	if pdbLister != nil {
		return pdbLister.List(labels.Everything())
	}
	return nil, nil
}

// pickOneNodeForPreemption 从给定节点中选出一个。
// 假设每个 map 条目中的 Pod 已按优先级降序排列。
// 若 scoreFuncs 非空，则按这些函数返回的分数挑选；否则按以下规则依次比较：
// 1. PDB 违规数最少的节点。
// 2. 最高优先级 victim  priority 最小的节点。
// 3. 仍相同则比较所有 victim 优先级之和。
// 4. 仍相同则选 victim 数量最少的节点。
// 5. 仍相同则选最高优先级 victims 启动时间最晚的节点。
// 6. 仍相同则取第一个这样的节点（近似随机）。
func pickOneNodeForPreemption(logger klog.Logger, nodesToVictims map[string]*extenderv1.Victims, scoreFuncs []func(node string) int64) string {
	if len(nodesToVictims) == 0 {
		return ""
	}

	allCandidates := make([]string, 0, len(nodesToVictims))
	for node := range nodesToVictims {
		allCandidates = append(allCandidates, node)
	}

	if len(scoreFuncs) == 0 {
		minNumPDBViolatingScoreFunc := func(node string) int64 {
			// PDB 违规越少，分数越高。
			return -nodesToVictims[node].NumPDBViolations
		}
		minHighestPriorityScoreFunc := func(node string) int64 {
			// highestPodPriority 是该节点 victims 中的最高优先级。
			highestPodPriority := corev1helpers.PodPriority(nodesToVictims[node].Pods[0])
			// 最高优先级越低，分数越高：尽量抢占优先级较低的 Pod。
			return -int64(highestPodPriority)
		}
		minSumPrioritiesScoreFunc := func(node string) int64 {
			var sumPriorities int64
			for _, pod := range nodesToVictims[node].Pods {
				// 所有优先级加 MaxInt32+1 使其非负，避免负优先级场景下
				// "少量负值 Pod 的节点"因总和较小被误选。
				sumPriorities += int64(corev1helpers.PodPriority(pod)) + int64(math.MaxInt32+1)
			}
			// 优先级总和越小，分数越高。
			return -sumPriorities
		}
		minNumPodsScoreFunc := func(node string) int64 {
			// victim 数量越少，分数越高。
			return -int64(len(nodesToVictims[node].Pods))
		}
		latestStartTimeScoreFunc := func(node string) int64 {
			// 取该节点所有 Pod 中最早的启动时间。
			earliestStartTimeOnNode := util.GetEarliestPodStartTime(nodesToVictims[node])
			if earliestStartTimeOnNode == nil {
				utilruntime.HandleErrorWithLogger(logger, nil, "Unexpected nil earliestStartTime for node", "node", node)
				return int64(math.MinInt64)
			}
			// 最早启动时间越晚，分数越高：尽量少影响运行已久的 Pod。
			return earliestStartTimeOnNode.UnixNano()
		}

		// 每个评分函数按各自规则打分并保留最高分节点；仅当出现多个同分节点时，
		// 才按优先级顺序执行下一个评分函数。
		scoreFuncs = []func(string) int64{
			// 优先选 PDB 违规最少的节点。
			minNumPDBViolatingScoreFunc,
			// 其次选最高优先级 victim 优先级最低的节点。
			minHighestPriorityScoreFunc,
			// 再者选 victim 优先级总和最小的节点。
			minSumPrioritiesScoreFunc,
			// 然后选 victim 数量最少的节点。
			minNumPodsScoreFunc,
			// 最后选最高优先级 victims 启动时间最晚的节点。
			latestStartTimeScoreFunc,
			// 仍有同分时取列表中的第一个节点。
		}
	}

	for _, f := range scoreFuncs {
		selectedNodes := []string{}
		maxScore := int64(math.MinInt64)
		for _, node := range allCandidates {
			score := f(node)
			if score > maxScore {
				maxScore = score
				selectedNodes = []string{}
			}
			if score == maxScore {
				selectedNodes = append(selectedNodes, node)
			}
		}
		if len(selectedNodes) == 1 {
			return selectedNodes[0]
		}
		allCandidates = selectedNodes
	}

	return allCandidates[0]
}

// DryRunPreemption 并行地在 <potentialNodes> 上模拟抢占，返回抢占候选及被过滤节点的状态映射。
// 候选数量取决于插件参数中的约束；返回列表里不违反 PDB 的候选排在违反 PDB 的候选之前。
// 注意：导出此方法是为了便于默认抢占插件的测试。
func (ev *Evaluator) DryRunPreemption(ctx context.Context, state fwk.CycleState, pod *v1.Pod, potentialNodes []fwk.NodeInfo,
	pdbs []*policy.PodDisruptionBudget, offset int32, candidatesNum int32) ([]Candidate, *framework.NodeToStatus, error) {

	fh := ev.Handler
	nonViolatingCandidates := newCandidateList(candidatesNum)
	violatingCandidates := newCandidateList(candidatesNum)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	nodeStatuses := framework.NewDefaultNodeToStatus()

	logger := klog.FromContext(ctx)
	logger.V(5).Info("Dry run the preemption", "potentialNodesNumber", len(potentialNodes), "pdbsNumber", len(pdbs), "offset", offset, "candidatesNumber", candidatesNum)

	var mu sync.Mutex
	var errs []error
	// checkNode 独立评估单个候选节点。每个 goroutine 通过 NewDomainForPodByPodPreemption 构建自己的 Domain，
	// 因此跨 A、B 两节点的 PodGroup victim 会同时出现在 A 和 B 的评估中。这是有意为之：
	// 下游 SelectCandidate 每次抢占最多只选一个候选，故该 PodGroup 最多被抢占一次。
	// 不要在此假设 goroutine 之间有协调——checkNode 必须保持独立。
	checkNode := func(i int) {
		nodeInfo := potentialNodes[(int(offset)+i)%len(potentialNodes)]
		logger.V(5).Info("Check the potential node for preemption", "node", nodeInfo.Node().Name)

		// GetVictimsOnNode 使用共享的 nodeInfo，返回的 DomainVictims 中受影响节点的 NodeInfo 也是共享的。
		// 这是安全的：SelectVictimsOnNode 只会修改主节点的 NodeInfo（通过 nodeInfo.Snapshot() 传入克隆副本），
		// 远端节点的 NodeInfo 仅传给 PreFilterExtension 的 Add/RemovePod 钩子，这些钩子不得修改 NodeInfo。
		allPossibleVictims, err := ev.GetVictimsOnNode(ctx, nodeInfo)
		if err != nil {
			mu.Lock()
			errs = append(errs, err)
			mu.Unlock()
			return
		}
		pods, numPDBViolations, status := ev.SelectVictimsOnNode(ctx, state.Clone(), pod, nodeInfo.Snapshot(), allPossibleVictims, pdbs)

		if status.IsSuccess() && len(pods) != 0 {
			victims := extenderv1.Victims{
				Pods:             pods,
				NumPDBViolations: int64(numPDBViolations),
			}
			c := &candidate{
				victims: &victims,
				name:    nodeInfo.Node().Name,
			}
			if numPDBViolations == 0 {
				nonViolatingCandidates.add(c)
			} else {
				violatingCandidates.add(c)
			}
			nvcSize, vcSize := nonViolatingCandidates.size(), violatingCandidates.size()
			if nvcSize > 0 && nvcSize+vcSize >= candidatesNum {
				cancel()
			}
			return
		}
		if status.IsSuccess() && len(pods) == 0 {
			status = fwk.AsStatus(fmt.Errorf("expected at least one victim pod on node %q", nodeInfo.Node().Name))
		}
		mu.Lock()
		if status.Code() == fwk.Error {
			errs = append(errs, status.AsError())
		}
		nodeStatuses.Set(nodeInfo.Node().Name, status)
		mu.Unlock()
	}
	fh.Parallelizer().Until(ctx, len(potentialNodes), checkNode, ev.PluginName)
	return append(nonViolatingCandidates.get(), violatingCandidates.get()...), nodeStatuses, utilerrors.NewAggregate(errs)
}

// GetVictimsOnNode 返回给定节点上潜在的被抢占对象（victim）列表。
// 开启 GenericWorkload 时，会把集群内属于同一 PodGroup（中断模式为 All）的 Pod 合并为一个 victim；
// 关闭时，节点上的每个 Pod 各算一个 victim。
func (ev *Evaluator) GetVictimsOnNode(ctx context.Context, nodeInfo fwk.NodeInfo) ([]*DomainVictim, error) {
	fh := ev.Handler

	// GenericWorkload 关闭时，每个 Pod 独立作为 victim。此处绕过 getCrossNodesVictims，
	// 并向 NewPodVictim 传 nil pgLister 以强制使用 Pod 自身优先级；否则 pgLister 会解析出
	// PodGroup 优先级，而在该特性关闭时它可能为 nil（被当作 0）。
	if !ev.executor.fts.EnableGenericWorkload {
		var victims []Victim
		for _, podInfo := range nodeInfo.GetPods() {
			victims = append(victims, NewPodVictim(podInfo, nil, nil))
		}
		return createDomainVictims(fh.MutableSnapshotSharedLister(), victims)
	}

	logger := klog.FromContext(ctx)
	return getCrossNodesVictims(logger, fh.MutableSnapshotSharedLister(), ev.podGroupSnapshot, ev.compositePodGroupSnapshot, []fwk.NodeInfo{nodeInfo})
}
