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
	"fmt"
	"sync/atomic"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
	"k8s.io/kubernetes/pkg/scheduler/util"
)

// podGroupPreemptor 把 PodGroup（或 CompositePodGroup）当作一个整体抢占者，
// 记录其优先级、未调度 Pod 列表以及解析后的抢占策略。
type podGroupPreemptor struct {
	priority          int32
	pods              []*v1.Pod
	podGroup          *schedulingv1beta1.PodGroup
	compositePodGroup *schedulingv1alpha3.CompositePodGroup
	preemptionPolicy  schedulingv1beta1.PreemptionPolicy
}

// newPodGroupPreemptor 根据 PodGroup 快照信息构造抢占者。
// 优先级与抢占策略取决于该组是普通 PodGroup 还是 CompositePodGroup。
func newPodGroupPreemptor(pgInfo fwk.PodGroupInfo, enablePodGroupPreemptionPolicy bool) *podGroupPreemptor {
	p := &podGroupPreemptor{
		pods: pgInfo.GetUnscheduledPods(),
	}
	// 组合组与普通组互斥，按实际类型解析优先级和抢占策略。
	if pgInfo.GetCompositePodGroup() != nil {
		cpg := pgInfo.GetCompositePodGroup()
		p.compositePodGroup = cpg
		p.priority = util.CompositePodGroupPriority(cpg)
		p.preemptionPolicy = resolveCompositePreemptionPolicy(cpg, p.pods, enablePodGroupPreemptionPolicy)
	} else {
		pg := pgInfo.GetPodGroup()
		p.podGroup = pg
		p.priority = util.PodGroupPriority(pg)
		p.preemptionPolicy = resolvePreemptionPolicy(pg, p.pods, enablePodGroupPreemptionPolicy)
	}
	return p
}

// getType 返回抢占者的实体类型字符串（普通组或组合组）。
func (p *podGroupPreemptor) getType() string {
	if p.compositePodGroup != nil {
		return string(fwk.CompositePodGroupKeyType)
	}
	return string(fwk.PodGroupKeyType)
}

// getObj 返回抢占者对应的组对象，供日志等场景使用。
func (p *podGroupPreemptor) getObj() klog.KMetadata {
	if p.compositePodGroup != nil {
		return p.compositePodGroup
	}
	return p.podGroup
}

// resolvePreemptionPolicy 解析 PodGroup 的抢占策略。
// 开启 PodGroupPreemptionPolicy 时优先用组上的字段；否则退回根据成员 Pod 的 PreemptNever 判断。
func resolvePreemptionPolicy(pg *schedulingv1beta1.PodGroup, pods []*v1.Pod, enablePodGroupPreemptionPolicy bool) schedulingv1beta1.PreemptionPolicy {
	if enablePodGroupPreemptionPolicy {
		// 特性关闭时创建的 PodGroup 其 PreemptionPolicy 字段为空，此时返回默认策略。
		if pg.Spec.PreemptionPolicy != nil {
			return *pg.Spec.PreemptionPolicy
		}
	} else {
		for _, pod := range pods {
			if p := pod.Spec.PreemptionPolicy; p != nil && *p == v1.PreemptNever {
				return schedulingv1beta1.PreemptNever
			}
		}
	}
	return schedulingv1beta1.PreemptLowerPriority
}

// resolveCompositePreemptionPolicy 解析 CompositePodGroup 的抢占策略。
// 组合组的策略只有 LowerPriority / Never 两种，开启特性时直接取组上的字段，
// 否则退回按成员 Pod 是否声明 PreemptNever 判断。
func resolveCompositePreemptionPolicy(cpg *schedulingv1alpha3.CompositePodGroup, pods []*v1.Pod, enablePodGroupPreemptionPolicy bool) schedulingv1beta1.PreemptionPolicy {
	if enablePodGroupPreemptionPolicy {
		if cpg.Spec.PreemptionPolicy != nil {
			if *cpg.Spec.PreemptionPolicy == schedulingv1alpha3.PreemptLowerPriority {
				return schedulingv1beta1.PreemptLowerPriority
			}
			return schedulingv1beta1.PreemptNever
		}
	} else {
		for _, pod := range pods {
			if p := pod.Spec.PreemptionPolicy; p != nil && *p == v1.PreemptNever {
				return schedulingv1beta1.PreemptNever
			}
		}
	}
	return schedulingv1beta1.PreemptLowerPriority
}

// Priority 返回抢占者的调度优先级。
// 抢占者只能抢占优先级严格更低的受害者，因此该值用于筛选候选受害者。
func (p *podGroupPreemptor) Priority() int32 {
	return p.priority
}

// Members 返回属于该抢占者的 Pod 列表（即组内未调度的 Pod）。
func (p *podGroupPreemptor) Members() []*v1.Pod {
	return p.pods
}

// PodGroup 返回与该抢占者关联的 PodGroup；组合组抢占者返回 nil。
func (p *podGroupPreemptor) PodGroup() *schedulingv1beta1.PodGroup {
	return p.podGroup
}

// CompositePodGroup 返回与该抢占者关联的 CompositePodGroup；普通组抢占者返回 nil。
func (p *podGroupPreemptor) CompositePodGroup() *schedulingv1alpha3.CompositePodGroup {
	return p.compositePodGroup
}

// PreemptionPolicy 返回该抢占者的抢占策略。
func (p *podGroupPreemptor) PreemptionPolicy() schedulingv1beta1.PreemptionPolicy {
	return p.preemptionPolicy
}

// domain 表示执行抢占评估的边界或范围。
// 它抽象了调度域：普通 Pod 抢占时是单个节点，PodGroup 抢占时则可能是
// 一组节点甚至整个集群。
type domain struct {
	nodes              map[string]fwk.NodeInfo
	name               string
	allPossibleVictims []*DomainVictim
}

// Nodes 返回该域内的 NodeInfo，以节点名为键。
// 抢占逻辑据此在特定范围内检查可行性与可用资源。
func (d *domain) Nodes() map[string]fwk.NodeInfo {
	return d.nodes
}

// GetAllPossibleVictims 返回该域内所有潜在受害者（单个 Pod 或 PodGroup）。
func (d *domain) GetAllPossibleVictims() []*DomainVictim {
	return d.allPossibleVictims
}

// GetName 返回域的唯一标识，主要用于日志与调试。
func (d *domain) GetName() string {
	return d.name
}

// getHighestAllAncestor 返回组层级中 disruption mode 为 All 的最高层祖先的键。
// 找到时返回 (key, true)，否则返回 (空键, false)。
// TODO：层级存在断点时记录日志或返回错误。
func getHighestAllAncestor(pod *v1.Pod, pgLister fwk.PodGroupLister, cpgLister fwk.CompositePodGroupLister) (fwk.EntityKey, bool) {
	if pod.Spec.SchedulingGroup == nil || pgLister == nil {
		return fwk.EntityKey{}, false
	}
	if cpgLister == nil {
		pg, err := pgLister.Get(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
		if err != nil || pg == nil {
			return fwk.EntityKey{}, false
		}
		if pg.Spec.DisruptionMode != nil && pg.Spec.DisruptionMode.All != nil {
			return fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName), true
		}
		return fwk.EntityKey{}, false
	}

	startKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	var highestAllKey fwk.EntityKey
	var hasAll bool

	TraverseHierarchyUp(pod.Namespace, startKey, pgLister, cpgLister, func(key fwk.EntityKey, pg *schedulingv1beta1.PodGroup, cpg *schedulingv1alpha3.CompositePodGroup) bool {
		if pg != nil {
			if pg.Spec.DisruptionMode != nil && pg.Spec.DisruptionMode.All != nil {
				highestAllKey = key
				hasAll = true
			}
		} else if cpg != nil {
			if cpg.Spec.DisruptionMode != nil && cpg.Spec.DisruptionMode.All != nil {
				highestAllKey = key
				hasAll = true
			}
		}
		return false
	})

	return highestAllKey, hasAll
}

// createDomainVictims 把去重后的 Victim 转成 DomainVictim：
// 从快照补充各受害者所影响节点的元数据，让抢占评估器能衡量驱逐的影响范围。
func createDomainVictims(snapshot fwk.SharedLister, victims []Victim) ([]*DomainVictim, error) {
	var allPossibleVictims []*DomainVictim
	for _, vi := range victims {
		v, err := newDomainVictim(snapshot, vi.Pods(), vi.Priority(), vi.Type())
		if err != nil {
			return nil, err
		}
		allPossibleVictims = append(allPossibleVictims, v)
	}
	return allPossibleVictims, nil
}

// newDomainForWorkloadPreemption 为工作负载抢占创建一个覆盖整个集群的域。
// 受害者根据 Pod 及其调度组计算：disruption mode 为 All 的组的所有 Pod 合并成一个受害者，
// 其余 Pod 各自作为单独受害者；两种情况下受害者优先级都取自对应的组优先级。
func newDomainForWorkloadPreemption(logger klog.Logger, snapshot fwk.SharedLister, podGroupSnapshot fwk.PodGroupLister, compositePodGroupSnapshot fwk.CompositePodGroupLister, name string) (*domain, error) {
	nodes, err := snapshot.NodeInfos().List()
	if err != nil {
		return nil, err
	}

	allPossibleVictims, err := getCrossNodesVictims(logger, snapshot, podGroupSnapshot, compositePodGroupSnapshot, nodes)
	if err != nil {
		return nil, err
	}

	nodesMap := make(map[string]fwk.NodeInfo, len(nodes))
	for _, nodeInfo := range nodes {
		if nodeInfo != nil && nodeInfo.Node() != nil {
			nodesMap[nodeInfo.Node().Name] = nodeInfo
		}
	}

	return &domain{
		nodes:              nodesMap,
		allPossibleVictims: allPossibleVictims,
		name:               name,
	}, nil
}

// getCrossNodesVictims 把给定节点上的 Pod 汇总成集群级抢占候选。
// 当 Pod 所属组层级中存在 disruption mode 为 All 的祖先时，把该层级在全集群的
// 已调度 Pod 合并为单个受害者，以便评估驱逐整个组的总代价和影响范围。
func getCrossNodesVictims(logger klog.Logger, snapshot fwk.SharedLister, podGroupSnapshot fwk.PodGroupLister, compositePodGroupSnapshot fwk.CompositePodGroupLister, nodes []fwk.NodeInfo) ([]*DomainVictim, error) {
	existing := sets.New[fwk.EntityKey]()
	var victims []Victim
	for _, node := range nodes {
		for _, podInfo := range node.GetPods() {
			p := podInfo.GetPod()

			highestAllKey, hasAll := getHighestAllAncestor(p, podGroupSnapshot, compositePodGroupSnapshot)
			if !hasAll {
				itemKey := fwk.PodKey(p.Namespace, p.Name)
				if !existing.Has(itemKey) {
					existing.Insert(itemKey)
					victims = append(victims, NewPodVictim(podInfo, podGroupSnapshot, compositePodGroupSnapshot))
				}
				continue
			}

			if !existing.Has(highestAllKey) {
				existing.Insert(highestAllKey)
				victims = append(victims, searchCrossNodesVictimPods(logger, highestAllKey, podInfo, snapshot, podGroupSnapshot, compositePodGroupSnapshot))
			}
		}
	}

	return createDomainVictims(snapshot, victims)
}

// searchCrossNodesVictimPods 收集以 pgKey 为根的层级中各叶子 PodGroup 的已调度 Pod，
// 合并成一个 Victim。若叶子组里找不到已调度 Pod，则退回为给定 podInfo 创建单个 Pod 的受害者。
func searchCrossNodesVictimPods(
	logger klog.Logger,
	pgKey fwk.EntityKey,
	podInfo fwk.PodInfo,
	snapshot fwk.SharedLister,
	podGroupSnapshot fwk.PodGroupLister,
	compositePodGroupSnapshot fwk.CompositePodGroupLister,
) Victim {
	var allPods []*v1.Pod
	if compositePodGroupSnapshot == nil {
		pgState, err := snapshot.PodGroupStates().Get(podInfo.GetPod().GetNamespace(), pgKey.Name)
		if err != nil {
			// 特性开启且 Pod 存在时理论上应成功；失败则保守地只按当前 Pod 构造受害者。
			return NewPodVictim(podInfo, podGroupSnapshot, compositePodGroupSnapshot)
		}
		allPods = append(allPods, pgState.ScheduledPods()...)
	} else {
		for pgState, err := range helper.GetPodGroupStates(snapshot, pgKey) {
			if err != nil {
				logger.Error(err, "Failed to get pod group state during preemption victim collection", "rootKey", pgKey)
				continue
			}
			if pgState != nil {
				allPods = append(allPods, pgState.ScheduledPods()...)
			}
		}
	}

	if len(allPods) == 0 {
		// podInfo 来自快照中的节点，本应出现在叶子 PodGroup 的 ScheduledPods() 中。
		// 若叶子遍历或状态查询失败，按单个 Pod 处理以免漏掉受害者。
		return NewPodVictim(podInfo, podGroupSnapshot, compositePodGroupSnapshot)
	}

	podInfos := make([]fwk.PodInfo, len(allPods))
	for i, p := range allPods {
		// ScheduledPods() 中的 Pod 已通过 Filter/Reserve，构造 PodInfo 不会失败。
		podInfos[i], _ = framework.NewPodInfo(p)
	}

	priority := GetPodPriority(podInfo.GetPod(), podGroupSnapshot, compositePodGroupSnapshot)
	// NewVictim 只在 podInfos 为空时报错，这里保证非空，故忽略错误。
	victim, _ := NewVictim(podInfos, priority, pgKey.Type)
	return victim
}

// Victim 表示可被抢占的最小原子单位。
// 它把单个 Pod 与 PodGroup 统一抽象，确保驱逐时整个原子单位被当作一体处理。
type Victim interface {
	// Priority 返回抢占单位的优先级。
	// 单个 Pod 时是 Pod 的优先级，PodGroup 时是组的优先级。
	Priority() int32

	// Pods 返回该抢占单位包含的所有 Pod。
	// 驱逐该单位意味着驱逐列表中的所有 Pod。
	Pods() []fwk.PodInfo

	// EarliestStartTime 返回该抢占单位中所有 Pod 的最早启动时间。
	EarliestStartTime() *metav1.Time

	// IsGroup 在抢占单位代表 PodGroup 或 CompositePodGroup 时返回 true。
	// 该方法只应在 GenericWorkload 特性开启时调用。
	IsGroup() bool

	// Type 返回抢占单位的实体类型。
	Type() fwk.EntityKeyType
}

// victim 是 Victim 接口的具体实现：把一组 Pod 连同其整体优先级和最早启动时间打包，
// 使单个 Pod 与 gang 式 PodGroup 在抢占候选排序时能统一评估和比较。
type victim struct {
	pods              []fwk.PodInfo
	priority          int32
	earliestStartTime *metav1.Time
	keyType           fwk.EntityKeyType
}

var _ Victim = &victim{}

// Pods 返回该抢占单位包含的所有 Pod。
func (v *victim) Pods() []fwk.PodInfo {
	return v.pods
}

// Priority 返回抢占单位的优先级：单个 Pod 取 Pod 优先级，PodGroup 取组优先级。
func (v *victim) Priority() int32 {
	return v.priority
}

// EarliestStartTime 返回该受害者中所有 Pod 的最早启动时间。
func (v *victim) EarliestStartTime() *metav1.Time {
	return v.earliestStartTime
}

// IsGroup 在抢占单位代表 PodGroup 或 CompositePodGroup 时返回 true。
// 该方法只应在 GenericWorkload 特性开启时调用。
func (v *victim) IsGroup() bool {
	return v.keyType == fwk.PodGroupKeyType || v.keyType == fwk.CompositePodGroupKeyType
}

// Type 返回抢占单位的实体类型。
func (v *victim) Type() fwk.EntityKeyType {
	return v.keyType
}

// NewPodVictim 创建一个代表单个 Pod 的 Victim。
// 计算 Pod 优先级时会考虑其所属调度组；对单个合法 Pod，NewVictim 必然成功，故忽略其错误。
// TODO：下面的处理方式并不理想：
//   - 从受害者重要性角度看，单个 Pod 本应始终使用 PodKeyType。
//   - 但出于 WAP 相关指标统计，又需要记录该 Pod 属于 single 模式的 PodGroup。
//   - 更合理的做法是额外保存一个字段，标记受害者 Pod 所属顶层组的类型。
//
// 后续为 CompositePodGroup 增加 WAP 指标支持时一并修正。
func NewPodVictim(podInfo fwk.PodInfo, pgLister fwk.PodGroupLister, cpgLister fwk.CompositePodGroupLister) Victim {
	priority := GetPodPriority(podInfo.GetPod(), pgLister, cpgLister)
	keyType := fwk.PodKeyType
	// 属于调度组的 Pod 用组类型标记，便于 WAP 指标按组维度统计。
	if podInfo.GetPod().Spec.SchedulingGroup != nil && pgLister != nil {
		keyType = fwk.PodGroupKeyType
	}
	vi, _ := NewVictim([]fwk.PodInfo{podInfo}, priority, keyType)
	return vi
}

// NewVictim 创建一个代表一组可一起被抢占的 Pod（或 PodGroup）的 Victim。
// 同时计算所有 Pod 中最早的启动时间；pods 为空或 keyType 为空时返回错误。
func NewVictim(pods []fwk.PodInfo, priority int32, keyType fwk.EntityKeyType) (Victim, error) {
	if len(pods) == 0 {
		return nil, fmt.Errorf("no pods provided")
	}
	if keyType == "" {
		return nil, fmt.Errorf("keyType cannot be empty")
	}

	var earliest *metav1.Time
	for _, pInfo := range pods {
		t := util.GetPodStartTime(pInfo.GetPod())
		if earliest == nil || (t != nil && t.Before(earliest)) {
			earliest = t
		}
	}

	return &victim{
		priority:          priority,
		pods:              pods,
		earliestStartTime: earliest,
		keyType:           keyType,
	}, nil
}

// DomainVictim 在 Victim 基础上补充驱逐所影响节点的信息。
// 它表示特定调度域内的一个抢占单位，让抢占逻辑能评估跨节点的影响范围。
type DomainVictim struct {
	Victim
	// affectedNodes 记录承载该受害者至少一个 Pod 的所有节点，
	// 便于调度器评估整个驱逐 blast radius 内各节点受到的影响。
	affectedNodes map[string]fwk.NodeInfo
}

// AffectedNodes 返回当前承载该受害者 Pod 的所有节点，以节点名为键。
func (dv *DomainVictim) AffectedNodes() map[string]fwk.NodeInfo {
	return dv.affectedNodes
}

// newDomainVictim 根据给定 Pod、优先级和类型创建 DomainVictim。
// 它从快照取每个 Pod 所在节点的 NodeInfo，存入 affectedNodes 以表示驱逐的影响范围。
func newDomainVictim(snapshot fwk.SharedLister, pods []fwk.PodInfo, priority int32, keyType fwk.EntityKeyType) (*DomainVictim, error) {
	nodeSnapshot := snapshot.NodeInfos()
	nodes := make(map[string]fwk.NodeInfo)
	for _, pInfo := range pods {
		nodeName := pInfo.GetPod().Spec.NodeName
		// 同一节点上可能有多个受害者 Pod，只需查一次快照。
		if _, ok := nodes[nodeName]; ok {
			continue
		}

		nodeInfo, err := nodeSnapshot.Get(nodeName)
		if err != nil {
			return nil, fmt.Errorf("failed to get node info for node %q from snapshot: %w", nodeName, err)
		}
		nodes[nodeName] = nodeInfo
	}

	victim, err := NewVictim(pods, priority, keyType)
	if err != nil {
		return nil, err
	}

	return &DomainVictim{
		Victim:        victim,
		affectedNodes: nodes,
	}, nil
}

// Candidate 表示一个候选调度位置（抢占者可落地的节点或域），
// 以及为了让抢占者放得下而需要驱逐的受害者列表。
type Candidate interface {
	// Victims 返回待抢占 Pod 列表及违反 PDB 的数量。
	Victims() *extenderv1.Victims
	// Name 返回抢占者被提名运行的目标域（PodGroup 场景）或节点名。
	Name() string
	// NumPodGroupDisruptions 返回影响 PodGroup 的抢占单位数量。
	// 一个抢占单位可能是整组 Pod（DisruptionMode=all）或单个 Pod（DisruptionMode=single）。
	NumPodGroupDisruptions() int
}

// candidate 是 Candidate 接口的具体实现。
type candidate struct {
	victims                *extenderv1.Victims
	numPodGroupDisruptions int
	name                   string
}

// Victims 返回该候选的受害者信息。
func (s *candidate) Victims() *extenderv1.Victims {
	return s.victims
}

// Name 返回该候选的目标域或节点名。
func (s *candidate) Name() string {
	return s.name
}

// NumPodGroupDisruptions 返回该候选造成的 PodGroup 中断数量。
func (s *candidate) NumPodGroupDisruptions() int {
	return s.numPodGroupDisruptions
}

// candidateList 是固定容量的并发安全候选列表：多个 goroutine 可同时 add，
// 超出容量的候选会被静默丢弃。
type candidateList struct {
	idx   int32
	items []Candidate
}

// newCandidateList 创建一个容量为 capacity 的空候选列表。
func newCandidateList(capacity int32) *candidateList {
	return &candidateList{idx: -1, items: make([]Candidate, capacity)}
}

// add 以原子方式把候选加入内部数组。
// 注意：列表已满时该候选会被直接丢弃，不会写入数组。
func (cl *candidateList) add(c *candidate) {
	if idx := atomic.AddInt32(&cl.idx, 1); idx < int32(len(cl.items)) {
		cl.items[idx] = c
	}
}

// size 返回已存储的候选数量。
// 调用时可能仍有 add() 在执行，访问元素前必须确保所有 add() 已经结束。
func (cl *candidateList) size() int32 {
	return min(atomic.LoadInt32(&cl.idx)+1, int32(len(cl.items)))
}

// get 返回内部的候选切片。该方法不是原子的，
// 调用前必须确保所有 add() 已完成。
func (cl *candidateList) get() []Candidate {
	return cl.items[:cl.size()]
}

// ViolatingVictim 表示抢占它会违反一个或多个 PodDisruptionBudget 的受害者。
// 记录受害者本身以及其中会导致 PDB 违规的 Pod 数量。
type ViolatingVictim[T Victim] struct {
	Victim       T
	ViolateCount int
}
