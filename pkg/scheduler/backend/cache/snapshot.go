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

package cache

import (
	"fmt"
	"maps"
	"slices"

	v1 "k8s.io/api/core/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingv1beta1 "k8s.io/api/scheduling/v1beta1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework"
)

// placementNodes 保存当前 placement（布置范围）中的节点。
// placement 是 pod group 调度周期内使用的一个受限节点集合。
type placementNodes struct {
	// 用于假定该 placement 的对象。
	placement *fwk.Placement
	// placement 中的节点集合，便于快速判断某节点是否属于该范围。
	nodeInfoSet sets.Set[string]
}

// snapshotBackupData 保存原始快照数据的浅拷贝。
type snapshotBackupData struct {
	nodeInfoMap                                               map[string]*framework.NodeInfo
	nodeInfoList                                              []fwk.NodeInfo
	havePodsWithAffinityNodeInfoList                          []fwk.NodeInfo
	havePodsWithRequiredAntiAffinityNodeInfoList              []fwk.NodeInfo
	havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList []fwk.NodeInfo
	usedPVCRefCounts                                          map[string]int
	podGroupStates                                            map[fwk.EntityKey]*podGroupStateSnapshot
}

// newSnapshotBackupData 从快照的原始数据创建 snapshotBackupData。
// 注意：这是浅拷贝。使用该方式时必须确保在创建备份后对快照数据做深拷贝，
// 否则备份与被修改的数据会共享底层结构。
func newSnapshotBackupData(s *Snapshot) *snapshotBackupData {
	return &snapshotBackupData{
		nodeInfoMap:                      s.nodeInfoMap,
		nodeInfoList:                     s.nodeInfoList,
		havePodsWithAffinityNodeInfoList: s.havePodsWithAffinityNodeInfoList,
		havePodsWithRequiredAntiAffinityNodeInfoList:              s.havePodsWithRequiredAntiAffinityNodeInfoList,
		havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList: s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList,
		usedPVCRefCounts: s.usedPVCRefCounts,
		podGroupStates:   s.podGroupStates,
	}
}

// restore 用备份数据恢复快照的各字段指针。
func (b *snapshotBackupData) restore(s *Snapshot) {
	s.nodeInfoMap = b.nodeInfoMap
	s.nodeInfoList = b.nodeInfoList
	s.havePodsWithAffinityNodeInfoList = b.havePodsWithAffinityNodeInfoList
	s.havePodsWithRequiredAntiAffinityNodeInfoList = b.havePodsWithRequiredAntiAffinityNodeInfoList
	s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = b.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList
	s.usedPVCRefCounts = b.usedPVCRefCounts
	s.podGroupStates = b.podGroupStates
}

// Snapshot 是缓存 NodeInfo 及 NodeTree 顺序的快照。
// 调度器在每个调度周期开始时拍摄快照，整个周期内的操作都基于它进行，
// 从而避免调度过程中缓存的并发变动影响决策。
type Snapshot struct {
	// 节点名到其 NodeInfo 快照的映射。
	nodeInfoMap map[string]*framework.NodeInfo
	// 按缓存 nodeTree 顺序排列的节点列表。
	nodeInfoList []fwk.NodeInfo
	// 至少有一个 Pod 声明了亲和性规则的节点列表。
	havePodsWithAffinityNodeInfoList []fwk.NodeInfo
	// 至少有一个 Pod 声明了必需反亲和性规则的节点列表。
	havePodsWithRequiredAntiAffinityNodeInfoList []fwk.NodeInfo
	// 至少有一个 Pod 声明了拓扑键非 kubernetes.io/hostname 的必需反亲和性规则的节点列表。
	// InterPodAffinityHostnameFastPath 特性门控关闭时该列表必须为空。
	havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList []fwk.NodeInfo
	// 集群内每个 PVC 被多少节点使用，key 格式为 "namespace/name"。
	usedPVCRefCounts map[string]int
	generation       int64
	// 单个 pod group 调度周期内，Pod key 到其假定时刻状态的映射。
	// 状态记录 AssumePod 对快照级索引所做的全部改动，使 ForgetPod 无需重扫快照即可撤销。
	// 下一个周期开始前应清空该映射。
	assumedPodStates map[string]*assumedPodState
	// 按假定顺序记录假定 Pod 的 key，便于 forgetAllAssumedPods 逆序回滚，
	// 满足 assumedPodState 所述的 LIFO 约定。
	assumedPodKeys []string
	// Pod group key 到其状态快照的映射，在 pod group 调度周期中使用。
	podGroupStates map[fwk.EntityKey]*podGroupStateSnapshot
	// composite pod group key 到其状态快照的映射，在 pod group 调度周期中使用。
	compositePodGroupStates map[fwk.EntityKey]*compositePodGroupStateSnapshot
	// 当前 placement 中的节点。未设置 placement 时为 nil。
	// 仅在 pod group 调度周期检查能否在 placement 内调度时设置，检查完成后应清空。
	placementNodes *placementNodes
	// GenericWorkload 特性门控的值。
	genericWorkloadEnabled bool
	// 保存变更前的原始快照信息，仅在变更会话（mutation session）中使用。
	// StartMutations 填充它，EndMutations 从中恢复数据。
	snapshotBackup *snapshotBackupData
	// CompositePodGroup 特性门控的值。
	compositePodGroupEnabled bool
}

var _ fwk.SharedLister = &Snapshot{}
var _ fwk.MutableSnapshotSharedLister = &Snapshot{}

// assumedPodState 记录一次 AssumePod 调用向快照共享亲和性索引
// （havePodsWithAffinityNodeInfoList、havePodsWithRequiredAntiAffinityNodeInfoList
// 及 havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList）添加了什么。
// InterPodAffinityHostnameFastPath 特性门控开启时，
// havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList 会被正确填充；关闭时保持为空。
// ForgetPod 读取该状态，以每个索引 O(1) 的代价精确撤销这些添加。
// PVC 索引通过引用计数（usedPVCRefCounts）维护，直接依据 Pod 的 volume 撤销，
// 因此这里无需按 Pod 单独记账。
//
// 这依赖快照级 Assume/Forget 的约定：
//  1. 只对先前假定过的 Pod 调用 ForgetPod。
//  2. Pod 按假定的逆序被遗忘。
//  3. 同一 Pod 的 AssumePod 与 ForgetPod 之间不发生其他快照变更。
type assumedPodState struct {
	// 被假定的 Pod。保留它以便 forgetAllAssumedPods 回滚残留 Pod 时无需调用方再次提供。
	pod *v1.Pod
	// 为 true 表示 AssumePod 把该 Pod 所在节点加入了快照的亲和性列表
	// （假定前该节点上没有带亲和性规则的 Pod）。
	addedToAffinityList bool
	// 为 true 表示 AssumePod 把该 Pod 所在节点加入了快照的必需反亲和性列表
	// （假定前该节点上没有带必需反亲和性规则的 Pod）。
	addedToAntiAffinityList bool
	// 为 true 表示 AssumePod 把该 Pod 所在节点加入了快照的非主机域必需反亲和性列表
	// （假定前该节点上没有此类 Pod）。
	// InterPodAffinityHostnameFastPath 开启时记录对
	// havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList 的更新；关闭时恒为 false。
	addedToNonHostScopedAntiAffinityList bool
}

// NewEmptySnapshot 初始化并返回一个空的 Snapshot。
func NewEmptySnapshot() *Snapshot {
	return &Snapshot{
		nodeInfoMap:              make(map[string]*framework.NodeInfo),
		usedPVCRefCounts:         make(map[string]int),
		assumedPodStates:         make(map[string]*assumedPodState),
		podGroupStates:           make(map[fwk.EntityKey]*podGroupStateSnapshot),
		compositePodGroupStates:  make(map[fwk.EntityKey]*compositePodGroupStateSnapshot),
		genericWorkloadEnabled:   utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload),
		compositePodGroupEnabled: utilfeature.DefaultFeatureGate.Enabled(features.CompositePodGroup),
	}
}

// NewSnapshot 根据给定的 Pod 和 Node 列表初始化 Snapshot 并返回。
// 仅应在测试中使用。
func NewSnapshot(pods []*v1.Pod, nodes []*v1.Node) *Snapshot {
	nodeInfoMap := createNodeInfoMap(pods, nodes)
	nodeInfoList := make([]fwk.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithAffinityNodeInfoList := make([]fwk.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredAntiAffinityNodeInfoList := make([]fwk.NodeInfo, 0, len(nodeInfoMap))
	havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList := make([]fwk.NodeInfo, 0, len(nodeInfoMap))
	for _, v := range nodeInfoMap {
		nodeInfoList = append(nodeInfoList, v)
		if len(v.PodsWithAffinity) > 0 {
			havePodsWithAffinityNodeInfoList = append(havePodsWithAffinityNodeInfoList, v)
		}
		if len(v.PodsWithRequiredAntiAffinity) > 0 {
			havePodsWithRequiredAntiAffinityNodeInfoList = append(havePodsWithRequiredAntiAffinityNodeInfoList, v)
		}
		if len(v.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
			havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, v)
		}
	}

	s := NewEmptySnapshot()
	s.nodeInfoMap = nodeInfoMap
	s.nodeInfoList = nodeInfoList
	s.havePodsWithAffinityNodeInfoList = havePodsWithAffinityNodeInfoList
	s.havePodsWithRequiredAntiAffinityNodeInfoList = havePodsWithRequiredAntiAffinityNodeInfoList
	s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList
	s.usedPVCRefCounts = createUsedPVCRefCounts(nodeInfoMap)
	if s.genericWorkloadEnabled {
		s.podGroupStates = createPodGroupStates(pods)
	}
	if s.compositePodGroupEnabled {
		s.compositePodGroupStates = make(map[fwk.EntityKey]*compositePodGroupStateSnapshot)
	}

	return s
}

// NewTestSnapshotWithPodGroups 初始化一个包含 pod group 的 Snapshot 并返回。
// 仅应在测试中使用。
func NewTestSnapshotWithPodGroups(pods []*v1.Pod, nodes []*v1.Node, podGroups []*schedulingv1beta1.PodGroup) *Snapshot {
	s := NewSnapshot(pods, nodes)
	for _, podGroup := range podGroups {
		key := fwk.PodGroupKey(podGroup.Namespace, podGroup.Name)
		pgs, ok := s.podGroupStates[key]
		if !ok {
			pgs = &podGroupStateSnapshot{podGroupStateData: newPodGroupStateData()}
			s.podGroupStates[key] = pgs
		}
		pgs.podGroup = podGroup
	}
	return s
}

// NewTestSnapshotWithCompositePodGroups 初始化一个包含 pod group 与 composite pod group 的 Snapshot 并返回。
// 同时建立父子关系，仅应在测试中使用。
func NewTestSnapshotWithCompositePodGroups(pods []*v1.Pod, nodes []*v1.Node, podGroups []*schedulingv1beta1.PodGroup, compositePodGroups []*schedulingv1alpha3.CompositePodGroup) *Snapshot {
	s := NewTestSnapshotWithPodGroups(pods, nodes, podGroups)
	for _, cpg := range compositePodGroups {
		key := fwk.CompositePodGroupKey(cpg.Namespace, cpg.Name)
		cpgs, ok := s.compositePodGroupStates[key]
		if !ok {
			cpgs = &compositePodGroupStateSnapshot{compositePodGroupStateData: newCompositePodGroupStateData()}
			s.compositePodGroupStates[key] = cpgs
		}
		cpgs.compositePodGroup = cpg
	}
	for entityKey, cpgs := range s.compositePodGroupStates {
		if cpgs.compositePodGroup == nil || cpgs.compositePodGroup.Spec.ParentCompositePodGroupName == nil {
			continue
		}
		parentKey := fwk.CompositePodGroupKey(cpgs.compositePodGroup.Namespace, *cpgs.compositePodGroup.Spec.ParentCompositePodGroupName)
		parentState, ok := s.compositePodGroupStates[parentKey]
		if !ok {
			parentState = &compositePodGroupStateSnapshot{compositePodGroupStateData: newCompositePodGroupStateData()}
			s.compositePodGroupStates[parentKey] = parentState
		}
		parentState.addChild(entityKey)
	}
	for entityKey, pgs := range s.podGroupStates {
		if pgs.podGroup == nil || pgs.podGroup.Spec.ParentCompositePodGroupName == nil {
			continue
		}
		parentKey := fwk.CompositePodGroupKey(pgs.podGroup.Namespace, *pgs.podGroup.Spec.ParentCompositePodGroupName)
		parentState, ok := s.compositePodGroupStates[parentKey]
		if !ok {
			parentState = &compositePodGroupStateSnapshot{compositePodGroupStateData: newCompositePodGroupStateData()}
			s.compositePodGroupStates[parentKey] = parentState
		}
		parentState.addChild(entityKey)
	}
	return s
}

// createPodGroupStates 根据 Pod 列表构建初始的 pod group 状态快照映射。
func createPodGroupStates(pods []*v1.Pod) map[fwk.EntityKey]*podGroupStateSnapshot {
	podGroupStates := make(map[fwk.EntityKey]*podGroupStateSnapshot)
	for _, pod := range pods {
		if pod.Spec.SchedulingGroup == nil {
			continue
		}
		key := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
		pgs, ok := podGroupStates[key]
		if !ok {
			pgs = &podGroupStateSnapshot{podGroupStateData: newPodGroupStateData()}
			podGroupStates[key] = pgs
		}
		pgs.addPod(pod)
	}
	return podGroupStates
}

// StartMutations 备份当前快照状态并开启一个变更会话。
// 用于单个 pod group 调度周期内修改快照：对快照做深拷贝，
// 保存原始对象，以便 EndMutations 时恢复。
// 上一个变更会话未结束时不能再次调用。
func (s *Snapshot) StartMutations() error {
	if s.snapshotBackup != nil {
		return fmt.Errorf("cannot stack mutations")
	}
	s.snapshotBackup = newSnapshotBackupData(s)

	s.nodeInfoMap = make(map[string]*framework.NodeInfo)
	for k, v := range s.snapshotBackup.nodeInfoMap {
		s.nodeInfoMap[k] = v.Snapshot().(*framework.NodeInfo)
	}

	s.nodeInfoList = make([]fwk.NodeInfo, 0, len(s.nodeInfoMap))
	s.havePodsWithAffinityNodeInfoList = make([]fwk.NodeInfo, 0, len(s.nodeInfoMap))
	s.havePodsWithRequiredAntiAffinityNodeInfoList = make([]fwk.NodeInfo, 0, len(s.nodeInfoMap))
	s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = make([]fwk.NodeInfo, 0, len(s.nodeInfoMap))

	for _, v := range s.snapshotBackup.nodeInfoList {
		clonedNode := s.nodeInfoMap[v.Node().Name]
		s.nodeInfoList = append(s.nodeInfoList, clonedNode)
		if len(clonedNode.PodsWithAffinity) > 0 {
			s.havePodsWithAffinityNodeInfoList = append(s.havePodsWithAffinityNodeInfoList, clonedNode)
		}
		if len(clonedNode.PodsWithRequiredAntiAffinity) > 0 {
			s.havePodsWithRequiredAntiAffinityNodeInfoList = append(s.havePodsWithRequiredAntiAffinityNodeInfoList, clonedNode)
		}
		if len(clonedNode.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
			s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, clonedNode)
		}
	}

	s.usedPVCRefCounts = make(map[string]int)
	maps.Copy(s.usedPVCRefCounts, s.snapshotBackup.usedPVCRefCounts)

	if s.genericWorkloadEnabled {
		s.podGroupStates = make(map[fwk.EntityKey]*podGroupStateSnapshot)
		for k, v := range s.snapshotBackup.podGroupStates {
			s.podGroupStates[k] = v.Clone()
		}
	}

	return nil
}

// EndMutations 结束变更会话，将快照恢复到 StartMutations 之前的状态。
// 若未调用 StartMutations 就调用它，则不做任何恢复。
func (s *Snapshot) EndMutations() error {
	if s.snapshotBackup == nil {
		return fmt.Errorf("no mutation session started")
	}

	s.snapshotBackup.restore(s)
	s.snapshotBackup = nil
	return nil
}

// createNodeInfoMap 将 Pod 列表按节点名"透视"为映射，
// value 是该节点上所有 Pod 的聚合信息（NodeInfo）。
func createNodeInfoMap(pods []*v1.Pod, nodes []*v1.Node) map[string]*framework.NodeInfo {
	nodeNameToInfo := make(map[string]*framework.NodeInfo)
	for _, pod := range pods {
		nodeName := pod.Spec.NodeName
		if _, ok := nodeNameToInfo[nodeName]; !ok {
			nodeNameToInfo[nodeName] = framework.NewNodeInfo()
		}
		nodeNameToInfo[nodeName].AddPod(pod)
	}
	imageExistenceMap := createImageExistenceMap(nodes)

	for _, node := range nodes {
		if _, ok := nodeNameToInfo[node.Name]; !ok {
			nodeNameToInfo[node.Name] = framework.NewNodeInfo()
		}
		nodeInfo := nodeNameToInfo[node.Name]
		nodeInfo.SetNode(node)
		nodeInfo.ImageStates = getNodeImageStates(node, imageExistenceMap)
	}
	return nodeNameToInfo
}

// createUsedPVCRefCounts 汇总所有节点的 PVC 引用次数，得到集群级别的使用计数。
func createUsedPVCRefCounts(nodeInfoMap map[string]*framework.NodeInfo) map[string]int {
	usedPVCRefCounts := make(map[string]int)
	for _, nodeInfo := range nodeInfoMap {
		for pvcKey, count := range nodeInfo.PVCRefCounts {
			usedPVCRefCounts[pvcKey] += count
		}
	}
	return usedPVCRefCounts
}

// getNodeImageStates 根据镜像存在性映射返回指定节点上各镜像的状态（大小、所在节点数）。
func getNodeImageStates(node *v1.Node, imageExistenceMap map[string]sets.Set[string]) map[string]*fwk.ImageStateSummary {
	imageStates := make(map[string]*fwk.ImageStateSummary)

	for _, image := range node.Status.Images {
		for _, name := range image.Names {
			imageStates[name] = &fwk.ImageStateSummary{
				Size:     image.SizeBytes,
				NumNodes: imageExistenceMap[name].Len(),
			}
		}
	}
	return imageStates
}

// createImageExistenceMap 返回镜像名到"拥有该镜像的节点集合"的映射。
func createImageExistenceMap(nodes []*v1.Node) map[string]sets.Set[string] {
	imageExistenceMap := make(map[string]sets.Set[string])
	for _, node := range nodes {
		for _, image := range node.Status.Images {
			for _, name := range image.Names {
				if _, ok := imageExistenceMap[name]; !ok {
					imageExistenceMap[name] = sets.New(node.Name)
				} else {
					imageExistenceMap[name].Insert(node.Name)
				}
			}
		}
	}
	return imageExistenceMap
}

// NodeInfos 返回 NodeInfoLister，这里 Snapshot 自身即实现该接口。
func (s *Snapshot) NodeInfos() fwk.NodeInfoLister {
	return s
}

// StorageInfos 返回 StorageInfoLister，这里 Snapshot 自身即实现该接口。
func (s *Snapshot) StorageInfos() fwk.StorageInfoLister {
	return s
}

// PodGroupStates 返回基于快照状态的 PodGroupStateLister。
func (s *Snapshot) PodGroupStates() fwk.PodGroupStateLister {
	return &podGroupStateSnapshotLister{podGroupStates: s.podGroupStates}
}

// PodGroups 返回基于快照的 PodGroupLister。
func (s *Snapshot) PodGroups() fwk.PodGroupLister {
	return &podGroupSnapshotListerImpl{snapshot: s}
}

// podGroupSnapshotListerImpl 从快照中读取 PodGroup 对象。
type podGroupSnapshotListerImpl struct {
	snapshot *Snapshot
}

// Get 按命名空间和名称返回快照中的 PodGroup 对象。
func (l *podGroupSnapshotListerImpl) Get(namespace, name string) (*schedulingv1beta1.PodGroup, error) {
	if !l.snapshot.genericWorkloadEnabled {
		return nil, fmt.Errorf("generic workload feature gate is disabled")
	}
	key := fwk.PodGroupKey(namespace, name)
	pgs, exists := l.snapshot.podGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("pod group state not found for pod group %s", key)
	}
	pg := pgs.podGroup
	if pg == nil {
		return nil, fmt.Errorf("pod group object not found for pod group %s", key)
	}
	return pg, nil
}

// CompositePodGroupStates 返回基于快照状态的 CompositePodGroupStateLister。
func (s *Snapshot) CompositePodGroupStates() fwk.CompositePodGroupStateLister {
	return &compositePodGroupStateSnapshotLister{compositePodGroupStates: s.compositePodGroupStates}
}

// CompositePodGroups 返回基于快照的 CompositePodGroupLister。
func (s *Snapshot) CompositePodGroups() fwk.CompositePodGroupLister {
	return &compositePodGroupSnapshotListerImpl{snapshot: s}
}

var _ fwk.CompositePodGroupLister = &compositePodGroupSnapshotListerImpl{}

// compositePodGroupSnapshotListerImpl 从快照中读取 CompositePodGroup 对象。
type compositePodGroupSnapshotListerImpl struct {
	snapshot *Snapshot
}

// Get 按命名空间和名称返回快照中的 CompositePodGroup 对象。
func (l *compositePodGroupSnapshotListerImpl) Get(namespace string, name string) (*schedulingv1alpha3.CompositePodGroup, error) {
	if !l.snapshot.compositePodGroupEnabled {
		return nil, fmt.Errorf("composite pod group feature gate is disabled")
	}
	key := fwk.CompositePodGroupKey(namespace, name)
	pgs, exists := l.snapshot.compositePodGroupStates[key]
	if !exists {
		return nil, fmt.Errorf("composite pod group not found in snapshot: %v", key)
	}
	cpg := pgs.compositePodGroup
	if cpg == nil {
		return nil, fmt.Errorf("composite pod group object not found for pod group %q", key)
	}
	return cpg, nil
}

var _ fwk.CompositePodGroupStateLister = &compositePodGroupStateSnapshotLister{}

// compositePodGroupStateSnapshotLister 从快照中读取 composite pod group 的状态。
type compositePodGroupStateSnapshotLister struct {
	compositePodGroupStates map[fwk.EntityKey]*compositePodGroupStateSnapshot
}

// Get 按命名空间和名称返回快照中 composite pod group 的状态。
func (l *compositePodGroupStateSnapshotLister) Get(namespace string, name string) (fwk.CompositePodGroupState, error) {
	key := fwk.CompositePodGroupKey(namespace, name)
	state, ok := l.compositePodGroupStates[key]
	if !ok {
		return nil, fmt.Errorf("composite pod group state not found for pod group %s/%s", namespace, name)
	}
	return state, nil
}

var _ fwk.PodGroupStateLister = &podGroupStateSnapshotLister{}

// podGroupStateSnapshotLister 从快照中读取 pod group 的状态。
type podGroupStateSnapshotLister struct {
	podGroupStates map[fwk.EntityKey]*podGroupStateSnapshot
}

// Get 按命名空间和名称返回快照中 pod group 的状态。
func (l *podGroupStateSnapshotLister) Get(namespace string, podGroupName string) (fwk.PodGroupState, error) {
	key := fwk.PodGroupKey(namespace, podGroupName)
	state, ok := l.podGroupStates[key]
	if !ok {
		return nil, fmt.Errorf("pod group state not found for pod group %s/%s", namespace, podGroupName)
	}
	return state, nil
}

// NumNodesInPlacement 返回当前 placement 内的节点数。
// 未设置 placement 时返回快照中的全部节点数。
// 非线程安全，须确保无其他 goroutine 写快照时调用。
func (s *Snapshot) NumNodesInPlacement() int {
	if s.placementNodes != nil {
		return len(s.placementNodes.placement.Nodes)
	}
	return len(s.nodeInfoList)
}

// List 返回快照中的节点列表。
func (s *Snapshot) List() ([]fwk.NodeInfo, error) {
	return s.nodeInfoList, nil
}

// HavePodsWithAffinityList 返回至少有一个 Pod 带 inter-pod 亲和性的节点列表。
func (s *Snapshot) HavePodsWithAffinityList() ([]fwk.NodeInfo, error) {
	return s.havePodsWithAffinityNodeInfoList, nil
}

// HavePodsWithRequiredAntiAffinityList 返回至少有一个 Pod 带必需 inter-pod 反亲和性的节点列表。
func (s *Snapshot) HavePodsWithRequiredAntiAffinityList() ([]fwk.NodeInfo, error) {
	return s.havePodsWithRequiredAntiAffinityNodeInfoList, nil
}

// HavePodsWithRequiredNonHostScopedAntiAffinityList 返回至少有一个 Pod 带
// 拓扑键非 kubernetes.io/hostname 的必需反亲和性的节点列表。
// InterPodAffinityHostnameFastPath 特性门控关闭时返回空列表且无错误。
func (s *Snapshot) HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]fwk.NodeInfo, error) {
	return s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nil
}

// Get 按节点名返回对应的 NodeInfo。
func (s *Snapshot) Get(nodeName string) (fwk.NodeInfo, error) {
	if v, ok := s.nodeInfoMap[nodeName]; ok && v.Node() != nil {
		return v, nil
	}
	return nil, fmt.Errorf("nodeinfo not found for node name %q", nodeName)
}

// IsPVCUsedByPods 判断给定 PVC key 是否被任何 Pod 使用。
func (s *Snapshot) IsPVCUsedByPods(key string) bool {
	return s.usedPVCRefCounts[key] > 0
}

// AssumePod 在快照中假定一个 Pod。除了将 Pod 加入其节点的 NodeInfo，
// 它还维护快照级的亲和性、反亲和性及 PVC 索引
// （havePodsWithAffinityNodeInfoList、havePodsWithRequiredAntiAffinityNodeInfoList、
// havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList、usedPVCRefCounts）的一致性，
// 使调度插件在 pod group 周期内看到最新状态。
// 注意：havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList 仅在
// InterPodAffinityHostnameFastPath 特性门控开启时填充，否则不使用。
// 亲和性索引的添加会记录在 assumedPodState 中，便于 ForgetPod 直接撤销。
// 在与缓存同步快照之前应先调用 ForgetPod。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) AssumePod(podInfo *framework.PodInfo) error {
	pod := podInfo.Pod
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}
	nodeInfo, ok := s.nodeInfoMap[pod.Spec.NodeName]
	if !ok {
		nodeInfo = framework.NewNodeInfo()
		s.nodeInfoMap[pod.Spec.NodeName] = nodeInfo
	}
	// AddPodInfo 会递增 nodeInfo 的 Generation。
	// 该操作只影响快照，故保留旧值以与缓存中的对象保持一致。
	oldGeneration := nodeInfo.Generation
	hadPodsWithAffinity := len(nodeInfo.PodsWithAffinity) > 0
	hadPodsWithRequiredAntiAffinity := len(nodeInfo.PodsWithRequiredAntiAffinity) > 0
	hadPodsWithRequiredNonHostScopedAntiAffinity := len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0
	nodeInfo.AddPodInfo(podInfo)
	nodeInfo.Generation = oldGeneration
	// AddPodInfo 维护了 NodeInfo 自身的亲和性与 PVC 索引；
	// 快照级索引必须同步更新，否则 inter-pod（反）亲和性和
	// VolumeRestrictions 插件会看到过期状态。
	state := &assumedPodState{pod: pod}
	if !hadPodsWithAffinity && len(nodeInfo.PodsWithAffinity) > 0 {
		s.havePodsWithAffinityNodeInfoList = append(s.havePodsWithAffinityNodeInfoList, nodeInfo)
		state.addedToAffinityList = true
	}
	if !hadPodsWithRequiredAntiAffinity && len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 {
		s.havePodsWithRequiredAntiAffinityNodeInfoList = append(s.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
		state.addedToAntiAffinityList = true
	}
	if !hadPodsWithRequiredNonHostScopedAntiAffinity && len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
		s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo)
		state.addedToNonHostScopedAntiAffinityList = true
	}
	for pvcKey := range framework.PodPVCKeys(pod) {
		s.usedPVCRefCounts[pvcKey]++
	}
	s.assumedPodStates[key] = state
	s.assumedPodKeys = append(s.assumedPodKeys, key)
	// Pod 属于 pod group 时，同步更新快照中的 pod group 状态
	if !s.genericWorkloadEnabled || pod.Spec.SchedulingGroup == nil {
		return nil
	}
	pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
	if pgs, ok := s.podGroupStates[pgKey]; ok {
		pgs.assumePod(pod)
	}
	return nil
}

// ForgetPod 从快照中遗忘一个 Pod。除了从其节点的 NodeInfo 移除 Pod，
// 它还撤销 AssumePod 记录的快照级索引改动
// （havePodsWithAffinityNodeInfoList、havePodsWithRequiredAntiAffinityNodeInfoList、
// havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList、usedPVCRefCounts）。
// 撤销非主机域反亲和性列表仅在 InterPodAffinityHostnameFastPath 门控开启时生效。
// 撤销亲和性列表依赖 assumedPodState 中所述的 LIFO Assume/Forget 约定。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) ForgetPod(logger klog.Logger, pod *v1.Pod) error {
	key, err := framework.GetPodKey(pod)
	if err != nil {
		return err
	}
	state, ok := s.assumedPodStates[key]
	if !ok {
		return fmt.Errorf("assumed pod %q not found in the snapshot", key)
	}
	delete(s.assumedPodStates, key)
	// LIFO Assume/Forget 约定（见 assumedPodState）保证被遗忘的 Pod 就是最后假定的那个，
	// 因此从末尾弹出其 key。
	if n := len(s.assumedPodKeys); n > 0 && s.assumedPodKeys[n-1] == key {
		s.assumedPodKeys = s.assumedPodKeys[:n-1]
	} else {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot remove assumed pod key on ForgetPod: the pod is not the last assumed one", "pod", klog.KObj(pod))
	}
	assumedPod := state.pod
	nodeName := assumedPod.Spec.NodeName
	if nodeInfo, ok := s.nodeInfoMap[nodeName]; ok {
		// RemovePod 会递增 nodeInfo 的 Generation。
		// 该操作只影响快照，故保留旧值以与缓存中的对象保持一致。
		oldGeneration := nodeInfo.Generation
		err := nodeInfo.RemovePod(logger, pod)
		if err != nil {
			return err
		}
		nodeInfo.Generation = oldGeneration
		// 只撤销该 Pod 的 AssumePod 对快照级索引的添加；
		// NodeInfo 自身的索引由 RemovePod 维护。
		if state.addedToAffinityList {
			s.havePodsWithAffinityNodeInfoList = removeAssumedNodeInfo(logger, s.havePodsWithAffinityNodeInfoList, nodeInfo, "havePodsWithAffinityNodeInfoList", pod)
		}
		if state.addedToAntiAffinityList {
			s.havePodsWithRequiredAntiAffinityNodeInfoList = removeAssumedNodeInfo(logger, s.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo, "havePodsWithRequiredAntiAffinityNodeInfoList", pod)
		}
		if state.addedToNonHostScopedAntiAffinityList {
			s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = removeAssumedNodeInfo(logger, s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo, "havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList", pod)
		}
		for pvcKey := range framework.PodPVCKeys(pod) {
			s.usedPVCRefCounts[pvcKey]--
			if s.usedPVCRefCounts[pvcKey] <= 0 {
				delete(s.usedPVCRefCounts, pvcKey)
			}
		}
		if len(nodeInfo.Pods) == 0 && nodeInfo.Node() == nil {
			delete(s.nodeInfoMap, nodeName)
		}
	}
	// Pod 属于 pod group 时，同步更新快照中的 pod group 状态
	if !s.genericWorkloadEnabled || pod.Spec.SchedulingGroup == nil {
		return nil
	}
	pgKey := fwk.PodGroupKey(assumedPod.Namespace, *assumedPod.Spec.SchedulingGroup.PodGroupName)
	if pgs, ok := s.podGroupStates[pgKey]; ok {
		pgs.forgetPod(assumedPod.UID)
	}
	return nil
}

// forgetAllAssumedPods 遗忘快照中所有仍未遗忘的假定 Pod。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) forgetAllAssumedPods(logger klog.Logger) {
	if len(s.assumedPodStates) == 0 {
		return
	}
	logger.Error(nil, "Found assumed pods in the snapshot that were not forgotten", "assumedPodsCount", len(s.assumedPodStates))
	// 按假定的逆序遗忘，满足 ForgetPod 撤销快照级索引所依赖的 LIFO 约定
	// （见 assumedPodState）。克隆切片避免遍历时修改原切片。
	keys := slices.Clone(s.assumedPodKeys)
	for i := len(keys) - 1; i >= 0; i-- {
		state, ok := s.assumedPodStates[keys[i]]
		if !ok {
			utilruntime.HandleErrorWithLogger(logger, nil, "Assumed pod state not found for the recorded assumed pod key", "podKey", keys[i])
			continue
		}
		if err := s.ForgetPod(logger, state.pod); err != nil {
			utilruntime.HandleErrorWithLogger(logger, err, "Failed to forget assumed pod")
		}
	}
}

// removeAssumedNodeInfo 从列表末尾移除 nodeInfo。
// AssumePod 只向这些列表追加元素，LIFO  Assume/Forget 约定保证当前遗忘 Pod 对应的项就在末尾。
// 若末项不匹配，说明约定被破坏：报告错误并原样返回列表。
func removeAssumedNodeInfo(logger klog.Logger, list []fwk.NodeInfo, nodeInfo fwk.NodeInfo, listName string, pod *v1.Pod) []fwk.NodeInfo {
	if len(list) == 0 || list[len(list)-1] != nodeInfo {
		utilruntime.HandleErrorWithLogger(logger, nil, "Cannot revert snapshot index on ForgetPod: list does not end with the assumed pod's node", "list", listName, "pod", klog.KObj(pod), "node", nodeInfo.Node().Name)
		return list
	}
	list[len(list)-1] = nil
	return list[:len(list)-1]
}

// GetPlacement 返回当前假定的 placement 对象，未假定时返回 nil。
func (s *Snapshot) GetPlacement() *fwk.Placement {
	if s.placementNodes != nil {
		return s.placementNodes.placement
	}
	return nil
}

// AssumePlacement 在快照中设置 placement 上下文。
// 假定 placement 后不应再修改快照，不再需要时须用 ForgetPlacement 清除。
// 重复调用会覆盖先前假定的 placement。
// 仅供调度器限制候选节点使用。非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) AssumePlacement(placement *fwk.Placement) error {
	if placement == nil {
		s.ForgetPlacement()
		return nil
	}
	if len(placement.Nodes) == len(s.nodeInfoList) {
		// placement 涵盖全部节点，等价于无 placement，直接清除以省去缓冲区复制
		s.ForgetPlacement()
		return nil
	}
	s.placementNodes = &placementNodes{
		placement:   placement,
		nodeInfoSet: sets.New[string](),
	}
	for _, node := range placement.Nodes {
		snapshotNode, ok := s.nodeInfoMap[node.Node().Name]
		if !ok {
			s.ForgetPlacement()
			return fmt.Errorf("node %s in placement is not present in snapshot", node.Node().Name)
		}
		if snapshotNode != node {
			s.ForgetPlacement()
			return fmt.Errorf("node %s in placement is not the same instance as in the snapshot", node.Node().Name)
		}
		s.placementNodes.nodeInfoSet.Insert(node.Node().Name)
	}
	return nil
}

// ForgetPlacement 清除 placement。
// 仅供调度器在 Pod 已针对该 placement 评估完毕后调用。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) ForgetPlacement() {
	s.placementNodes = nil
}

// GetNodeInPlacement 返回当前 placement 内指定节点的 NodeInfo。
// 未设置 placement 时等价于 Get。placement 通常在 pod group 调度周期内设置。
// 仅供调度器限制候选节点使用，插件一般无需关心。
// 非线程安全，须确保无其他 goroutine 写快照时调用。
func (s *Snapshot) GetNodeInPlacement(nodeName string) (fwk.NodeInfo, error) {
	if s.placementNodes == nil || s.placementNodes.nodeInfoSet.Has(nodeName) {
		return s.Get(nodeName)
	}
	return nil, fmt.Errorf("node %q not found in placement", nodeName)
}

// ListNodesInPlacement 返回当前 placement 内的节点列表。
// 未设置 placement 时等价于 List。placement 通常在 pod group 调度周期内设置。
// 仅供调度器限制候选节点使用，插件一般无需关心。
// 非线程安全，须确保无其他 goroutine 写快照时调用。
func (s *Snapshot) ListNodesInPlacement() ([]fwk.NodeInfo, error) {
	if s.placementNodes == nil {
		return s.List()
	}
	return s.placementNodes.placement.Nodes, nil
}

// AddPod 向快照添加一个 Pod。
// 只有在通过 StartMutations 开启变更会话后才能调用。
// 与 AssumePod 不同，AddPod 无需用 RemovePod 撤销：EndMutations 时统一恢复状态。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) AddPod(podInfo fwk.PodInfo, nodeName string) error {
	if s.snapshotBackup == nil {
		return fmt.Errorf("AddPod() called outside of mutation session")
	}
	nodeInfo, ok := s.nodeInfoMap[nodeName]
	if !ok {
		nodeInfo = framework.NewNodeInfo()
		s.nodeInfoMap[nodeName] = nodeInfo
	}

	hadPodsWithAffinity := len(nodeInfo.PodsWithAffinity) > 0
	hadPodsWithRequiredAntiAffinity := len(nodeInfo.PodsWithRequiredAntiAffinity) > 0
	hadPodsWithRequiredNonHostScopedAntiAffinity := len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0

	pod := podInfo.GetPod()
	nodeInfo.AddPodInfo(podInfo)

	// AddPodInfo 维护了 NodeInfo 自身的亲和性与 PVC 索引；
	// 快照级索引必须同步更新，否则 inter-pod（反）亲和性和
	// VolumeRestrictions 插件会看到过期状态。
	if !hadPodsWithAffinity && len(nodeInfo.PodsWithAffinity) > 0 {
		s.havePodsWithAffinityNodeInfoList = append(s.havePodsWithAffinityNodeInfoList, nodeInfo)
	}
	if !hadPodsWithRequiredAntiAffinity && len(nodeInfo.PodsWithRequiredAntiAffinity) > 0 {
		s.havePodsWithRequiredAntiAffinityNodeInfoList = append(s.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
	}
	if !hadPodsWithRequiredNonHostScopedAntiAffinity && len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0 {
		s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = append(s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo)
	}

	for pvcKey := range framework.PodPVCKeys(pod) {
		s.usedPVCRefCounts[pvcKey]++
	}

	if s.genericWorkloadEnabled && pod.Spec.SchedulingGroup != nil {
		pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
		if pgs, ok := s.podGroupStates[pgKey]; ok {
			pgs.addPod(pod)
		}
	}

	return nil
}

// RemovePod 从快照移除一个 Pod。
// 只有在通过 StartMutations 开启变更会话后才能调用。
// 状态将在 EndMutations 时统一恢复。
// 非线程安全，须确保无其他 goroutine 读写快照时调用。
func (s *Snapshot) RemovePod(logger klog.Logger, pod *v1.Pod, nodeName string) error {
	if s.snapshotBackup == nil {
		return fmt.Errorf("RemovePod() called outside of mutation session")
	}
	nodeInfo, ok := s.nodeInfoMap[nodeName]
	if !ok {
		return fmt.Errorf("node %q not found in the snapshot", nodeName)
	}

	hadPodsWithAffinity := len(nodeInfo.PodsWithAffinity) > 0
	hadPodsWithRequiredAntiAffinity := len(nodeInfo.PodsWithRequiredAntiAffinity) > 0
	hadPodsWithRequiredNonHostScopedAntiAffinity := len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0

	if err := nodeInfo.RemovePod(logger, pod); err != nil {
		return err
	}

	havePodsWithAffinity := len(nodeInfo.PodsWithAffinity) > 0
	havePodsWithRequiredAntiAffinity := len(nodeInfo.PodsWithRequiredAntiAffinity) > 0
	havePodsWithRequiredNonHostScopedAntiAffinity := len(nodeInfo.PodsWithRequiredNonHostScopedAntiAffinity) > 0

	if hadPodsWithAffinity && !havePodsWithAffinity {
		s.havePodsWithAffinityNodeInfoList = removeNodeInfoFromList(logger, s.havePodsWithAffinityNodeInfoList, nodeInfo)
	}
	if hadPodsWithRequiredAntiAffinity && !havePodsWithRequiredAntiAffinity {
		s.havePodsWithRequiredAntiAffinityNodeInfoList = removeNodeInfoFromList(logger, s.havePodsWithRequiredAntiAffinityNodeInfoList, nodeInfo)
	}
	if hadPodsWithRequiredNonHostScopedAntiAffinity && !havePodsWithRequiredNonHostScopedAntiAffinity {
		s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList = removeNodeInfoFromList(logger, s.havePodsWithRequiredNonHostScopedAntiAffinityNodeInfoList, nodeInfo)
	}
	for pvcKey := range framework.PodPVCKeys(pod) {
		s.usedPVCRefCounts[pvcKey]--
		if s.usedPVCRefCounts[pvcKey] <= 0 {
			delete(s.usedPVCRefCounts, pvcKey)
		}
	}

	if s.genericWorkloadEnabled && pod.Spec.SchedulingGroup != nil {
		pgKey := fwk.PodGroupKey(pod.Namespace, *pod.Spec.SchedulingGroup.PodGroupName)
		if pgs, ok := s.podGroupStates[pgKey]; ok {
			pgs.deletePod(pod.UID)
		}
	}

	if len(nodeInfo.Pods) == 0 && nodeInfo.Node() == nil {
		delete(s.nodeInfoMap, nodeName)
	}
	return nil
}

// removeNodeInfoFromList 快速从列表中移除 NodeInfo，不保证保持原有顺序：
// 用末尾元素填补空位，以便 O(1) 删除。
func removeNodeInfoFromList(logger klog.Logger, list []fwk.NodeInfo, nodeInfoToRemove fwk.NodeInfo) []fwk.NodeInfo {
	for i, nodeInfo := range list {
		if nodeInfo == nodeInfoToRemove {
			list[i] = list[len(list)-1]
			list[len(list)-1] = nil
			return list[:len(list)-1]
		}
	}
	logger.Error(nil, "NodeInfo not found in the list", "nodeInfo", nodeInfoToRemove)
	return list
}

// GetRootKeyForGroup 沿父引用向上追溯，返回给定 EntityKey 的根节点 key。
// key 必须是 PodGroupKey 或 CompositePodGroupKey 类型。
// 检测到层级环时返回错误。
func (s *Snapshot) GetRootKeyForGroup(key fwk.EntityKey) (fwk.EntityKey, bool, error) {
	currentKey := key
	visited := sets.New[fwk.EntityKey]()
	for {
		if visited.Has(currentKey) {
			return fwk.EntityKey{}, false, fmt.Errorf("cycle detected in the hierarchy: %v", visited.UnsortedList())
		}
		visited.Insert(currentKey)

		switch currentKey.Type {
		case fwk.PodGroupKeyType:
			pgs, ok := s.podGroupStates[currentKey]
			if !ok {
				return fwk.EntityKey{}, false, nil
			}
			pg := pgs.podGroup
			if pg == nil {
				return fwk.EntityKey{}, false, nil
			}
			if !s.compositePodGroupEnabled || pg.Spec.ParentCompositePodGroupName == nil {
				return currentKey, true, nil
			}
			currentKey = fwk.CompositePodGroupKey(pg.Namespace, *pg.Spec.ParentCompositePodGroupName)
		case fwk.CompositePodGroupKeyType:
			cpgs, ok := s.compositePodGroupStates[currentKey]
			if !ok {
				return fwk.EntityKey{}, false, nil
			}
			cpg := cpgs.compositePodGroup
			if cpg == nil {
				return fwk.EntityKey{}, false, nil
			}
			if cpg.Spec.ParentCompositePodGroupName == nil {
				return currentKey, true, nil
			}
			currentKey = fwk.CompositePodGroupKey(cpg.Namespace, *cpg.Spec.ParentCompositePodGroupName)
		case fwk.PodKeyType:
			return fwk.EntityKey{}, false, fmt.Errorf("pod key type not supported in snapshot GetRootKeyForGroup for %s", currentKey.String())
		}
	}
}

// BuildHierarchySnapshotFromPod 根据给定 Pod 构建层级快照。
// Snapshot 本身已包含完整的 pod group 层级状态，因此直接返回自身。
func (s *Snapshot) BuildHierarchySnapshotFromPod(pod *v1.Pod) (fwk.PodGroupManager, error) {
	return s, nil
}
