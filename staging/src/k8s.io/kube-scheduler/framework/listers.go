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

package framework

import (
	v1 "k8s.io/api/core/v1"
	resourceapi "k8s.io/api/resource/v1"
	schedulingv1alpha3 "k8s.io/api/scheduling/v1alpha3"
	schedulingapi "k8s.io/api/scheduling/v1beta1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
)

// 把 List/Get 包成接口而不是直接暴露 informer lister，是因为调度器需要的是「经过聚合的 NodeInfo」
// （节点上跑着哪些 Pod、资源画像等），原始 informer 数据远不够用；
// 同时插件只应看到快照口径的数据，避免各自去拼装、口径不一。
// NodeInfoLister 抽象按节点名列出或查询 NodeInfo 的能力。
type NodeInfoLister interface {
	// List 返回全部 NodeInfo。
	List() ([]NodeInfo, error)
	// HavePodsWithAffinityList 返回其 Pod 带 Pod 间（反）亲和约束的节点。
	//
	// 单独维护「有亲和性 Pod 的节点」这个子集，是为了让 PreFilter 在扫描亲和性约束时
	// 只遍历可能相关的节点，不必对全量节点做 O(n) 检查。
	HavePodsWithAffinityList() ([]NodeInfo, error)
	// HavePodsWithRequiredAntiAffinityList 返回其 Pod 带硬性反亲和约束的节点。
	HavePodsWithRequiredAntiAffinityList() ([]NodeInfo, error)
	// HavePodsWithRequiredNonHostScopedAntiAffinityList 返回其 Pod 需跨更广拓扑扫描的节点（topologyKey 非 hostname）。
	// PreFilter 借此找出可能冲突的存量 Pod，跳过不可能冲突的节点。
	// InterPodAffinityHostnameFastPath 关闭时返回空列表且无错误。
	//
	// 非 hostname 作用域的反亲和需要跨拓扑域扫描，代价高得多；
	// 单独列出这类节点，PreFilter 就能先快速排除不可能冲突的节点。
	HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]NodeInfo, error)
	// Get 返回指定节点名的 NodeInfo。
	Get(nodeName string) (NodeInfo, error)
}

// StorageInfoLister 抽象存储相关操作与资源的查询能力。
type StorageInfoLister interface {
	// IsPVCUsedByPods 判断 PVC 是否被已调度 Pod 使用，
	// 键格式为 "namespace/name"。
	//
	// 把 PVC 使用情况做成只读查询，是为了让 VolumeBinding 插件判断能否安全迁移/释放卷时，
	// 依据的是调度器已 Assume 的 Pod 视角，而不是可能滞后或与实际绑定不一致的外部状态。
	IsPVCUsedByPods(key string) bool
}

// 把一组 lister 收进一个聚合接口，是为了让 Handle 只暴露一个入口；
// 新增 lister 时只改这里和实现，不必动所有插件的签名。
// SharedLister 汇总调度器专用的各类 lister。
type SharedLister interface {
	NodeInfos() NodeInfoLister
	StorageInfos() StorageInfoLister
	// PodGroupStates 提供 PodGroup 的动态状态信息。
	PodGroupStates() PodGroupStateLister
	// PodGroups 提供缓存中的 PodGroup 对象。
	PodGroups() PodGroupLister
	// CompositePodGroupStates 提供组合 PodGroup 的动态状态信息。
	CompositePodGroupStates() CompositePodGroupStateLister
	// CompositePodGroups 提供缓存中的组合 PodGroup 对象。
	CompositePodGroups() CompositePodGroupLister
}

// PodGroupLister 提供对缓存 PodGroup 的只读访问。
type PodGroupLister interface {
	// Get 返回指定 namespace/name 的 PodGroup。
	Get(namespace, name string) (*schedulingapi.PodGroup, error)
}

// 可变更快照只开放给 PodGroupPostFilter：抢占/重排需要「假设移走某些 Pod 后」试探多种方案，
// 又必须在同一调度周期内恢复原状，否则会污染后续插件看到的世界。
// 限制只有一个 session，是因为快照是共享可变状态，并发会话没法保证回滚到正确基线。
// MutableSnapshotSharedLister 允许临时修改快照并随后恢复。
// 它在 SharedLister 之上扩展。
// 仅 PodGroupPostFilter 扩展点可用。
type MutableSnapshotSharedLister interface {
	SharedLister
	// StartMutations 开启一次修改会话。
	// 用于需改动快照以推演多种场景的操作。
	// 同一时刻只允许一个会话。
	// 调用 StartMutations() 后必须在同一调度周期内调用 EndMutations()。
	//
	// 把「开始试探」做成显式方法而非隐式构造，是为了强制调用方配对 EndMutations；
	// 整个周期内只允许一个 session，省去多会话叠加时基线难以定义的麻烦。
	StartMutations() error
	// EndMutations 结束会话并把快照恢复到 StartMutations 之前。
	EndMutations() error
	// AddPod 把给定 Pod 加入快照。
	// 只有先调用 StartMutations 才能调用 AddPod。
	// 该函数非线程安全，须确保没有其它 goroutine 读写快照。
	//
	// 明确要求非线程安全，是因为快照会被多个插件共享；
	// 若允许并发写，Read 到的中间态会让试探结果失去意义，索性把同步责任交给调用方。
	AddPod(podInfo PodInfo, nodeName string) error
	// RemovePod 从快照中移除给定 Pod。
	// 只有先调用 StartMutations 才能调用 RemovePod。
	// 调用 EndMutations 时状态会回滚。
	RemovePod(logger klog.Logger, pod *v1.Pod, nodeName string) error
}

// PodGroupStateLister 提供 PodGroup 状态的只读访问。
type PodGroupStateLister interface {
	// Get 返回指定 PodGroup 的 PodGroupState。
	Get(namespace string, podGroupName string) (PodGroupState, error)
}

// CompositePodGroupLister 提供缓存组合 PodGroup 的只读访问。
type CompositePodGroupLister interface {
	// Get 返回指定 namespace/name 的 CompositePodGroup。
	Get(namespace, name string) (*schedulingv1alpha3.CompositePodGroup, error)
}

// CompositePodGroupStateLister 提供组合 PodGroup 状态的只读访问。
type CompositePodGroupStateLister interface {
	// Get 返回指定组合 PodGroup 的 CompositePodGroupState。
	Get(namespace string, compositePodGroupName string) (CompositePodGroupState, error)
}

type CSINodeLister interface {
	// List 返回全部 CSINode。
	List() ([]*storagev1.CSINode, error)
	// Get 返回指定名字的 CSINode。
	Get(name string) (*storagev1.CSINode, error)
}

// 把「应用 DeviceTaint 规则」的口径收进 lister 内部，是为了让调度器与 DRA 驱动
// 对同一份 ResourceSlice 的看法一致；否则每个调用方各做一遍 taint 过滤，迟早出现分歧。
// ResourceSliceLister 用于获取 ResourceSlice。
type ResourceSliceLister interface {
	// 启用 DRADeviceTaints 时返回应用了 DeviceTaintRules 的 ResourceSlice，
	// 否则返回原始列表。
	//
	// k8s.io/dynamic-resource-allocation/resourceslice/tracker 提供所需逻辑的实现，
	// 它可替代普通 ResourceSlice informer，
	// 并提供 ListPatchedResourceSlices 方法。
	ListWithDeviceTaintRules() ([]*resourceapi.ResourceSlice, error)
}

// DeviceClassLister 用于获取 DeviceClass。
type DeviceClassLister interface {
	// List 返回全部 DeviceClass。
	List() ([]*resourceapi.DeviceClass, error)
	// Get 返回指定名字的 DeviceClass。
	Get(className string) (*resourceapi.DeviceClass, error)
}

// 在 informer 缓存之外再叠一层内存追踪，是为了填平「API 已改、informer 还没更新」的时间窗：
// 绑定阶段分配的 claim 必须立刻对后续调度可见，否则同一 claim 会被重复分配。
// ResourceClaimTracker 获取 ResourceClaim 并在内存中跟踪其变化。
//
// 若要在绑定阶段通过 API 分配 Claim（调度器场景），它可避免
// 调度/绑定以及绑定/informer 更新之间的竞争。
//
// 若不执行绑定（如 Cluster Autoscaler 只跑调度阶段、在内存中模拟绑定），
// 调用方可取得 DRA 插件产生的分配结果并保存在 API 之外。
type ResourceClaimTracker interface {
	// List 列出 ResourceClaim，结果保证立即反映 AssumeClaimAfterAPICall()
	// 与 SignalClaimPendingAllocation() 带来的变更。
	//
	// 保证 List 立即反映 Assume/Pending 变更，是为了避免调度周期读到旧状态而做出错误分配决策；
	// 若依赖 informer 的最终一致性，窗口期内的竞争几乎无法避免。
	List() ([]*resourceapi.ResourceClaim, error)
	// Get 类似 List()，但只针对单个 Claim。
	Get(namespace, claimName string) (*resourceapi.ResourceClaim, error)
	// ListAllAllocatedDevices 列出所有已分配设备，结果保证立即反映
	// AssumeClaimAfterAPICall() 与 SignalClaimPendingAllocation() 的变更。
	ListAllAllocatedDevices() (sets.Set[structured.DeviceID], error)
	// GatherAllocatedState 汇总已分配设备状态，结果保证立即反映
	// AssumeClaimAfterAPICall() 与 SignalClaimPendingAllocation() 的变更。
	GatherAllocatedState() (*structured.AllocatedState, error)

	// SignalClaimPendingAllocation 告知 tracker：该 Claim 将在绑定阶段通过 API
	// 分配，因此它及其 Status.Allocation 都不能为 nil。
	// 已有 pending 分配时会被共享；同一 Claim 上调用 SignalClaimPendingAllocation()
	// 多少次，最终就应调用同样多次的 MaybeRemoveClaimPendingAllocation()。
	// 该变化立即反映在 List() 等访问结果中。
	//
	// 引用计数式的 pending 状态，是因为多个 Pod 可能同时依赖同一笔即将发生的分配；
	// 等所有调用方都显式撤销后再清理，才不会提前释放别的 Pod 仍在等待的资源。
	SignalClaimPendingAllocation(claimUID types.UID, allocatedClaim *resourceapi.ResourceClaim) error
	// ClaimHasPendingAllocation 判断绑定阶段是否有 pending 分配，可避免
	// 后续调度阶段的竞争。
	GetPendingAllocation(claimUID types.UID) *resourceapi.AllocationResult
	// MaybeRemoveClaimPendingAllocation 移除之前登记的 pending 分配；
	// forceRemove 为 true 时强制移除，否则只有
	// 没有其它 Pod 依赖它（含 SignalClaimPendingAllocation 与 AcquirePendingAllocation）时才移除。
	// 返回是否曾存在并被移除。
	// 移除后立即不再出现在 List() 等结果中。
	MaybeRemoveClaimPendingAllocation(claimUID types.UID, forceRemove bool) (deleted bool)

	// AssumeClaimAfterAPICall 告知 tracker：绑定阶段已发起修改该 Claim 的 API 调用，
	// 变更很快会进入 informer；该变化立即反映在访问结果中。
	// 用于避免 informer 更新与后续调度之间的竞争。
	//
	// Assume 是乐观更新：先按「请求会成功」推进调度，失败再回滚。
	// 这样无需等待 informer 回灌就能继续下一轮调度，代价是必须提供成对的 Restore。
	AssumeClaimAfterAPICall(claim *resourceapi.ResourceClaim) error
	// AssumedClaimRestore 告知 tracker：修改 Claim 的 API 调用失败，
	// 变更不会进入 informer；访问结果立即撤销假设变更，
	// 回退到 informer 中的版本。
	AssumedClaimRestore(namespace, claimName string)
}

// DeviceClassResolver 从扩展资源名解析出设备类名。
type DeviceClassResolver interface {
	// GetDeviceClass 返回给定扩展资源名对应的设备类。
	// 无映射或 DRAExtendedResource 关闭时
	// 返回 nil。
	GetDeviceClass(resourceName v1.ResourceName) *resourceapi.DeviceClass
}

// 做成可替换的管理器，是为了让 Cluster Autoscaler 这类只用调度阶段的调用方
// 能注入模拟的 DRA 状态；默认实现走 API，模拟场景则在内存里伪造对象。
// SharedDRAManager 供 DRA 插件获取 DRA 对象并在内存中跟踪修改。
// 默认实现从 API 获取对象；也可注入其它实现来模拟 DRA 状态，
// 例如 Cluster Autoscaler 在内存中模拟调度变化时，
// 可用它向 DRA 插件提供正确的 DRA 对象状态。
type SharedDRAManager interface {
	ResourceClaims() ResourceClaimTracker
	ResourceSlices() ResourceSliceLister
	DeviceClasses() DeviceClassLister
	DeviceClassResolver() DeviceClassResolver
}

// 与 SharedDRAManager 同理：抽象出来是为了让模拟调度能替换 CSINode 的数据来源，
// 而不必真的连 apiserver。
// CSIManager 用于获取 CSINode 并在内存中跟踪其变化。
// 默认实现从 API 获取对象；也可注入其它实现来模拟 DRA 状态，
// 例如 Cluster Autoscaler 在内存中模拟调度变化时，
// 可用它向 CSINode 插件提供正确的对象状态。
type CSIManager interface {
	CSINodes() CSINodeLister
}

// 把组相关的 lister 和层级快照构造收在一个接口里，是因为 pod group 可以嵌套；
// 调用方需要沿层级向上追溯时，入口集中在这里比散落各处更可控。
// PodGroupManager 提供调度器缓存中 PodGroup 运行时信息的访问入口。
type PodGroupManager interface {
	// PodGroupStates 返回 PodGroupStateLister。
	PodGroupStates() PodGroupStateLister
	// PodGroups 返回 PodGroupLister。
	PodGroups() PodGroupLister
	// CompositePodGroupStates 返回组合状态 lister。
	CompositePodGroupStates() CompositePodGroupStateLister
	// CompositePodGroups 返回组合对象 lister。
	CompositePodGroups() CompositePodGroupLister
	// BuildHierarchySnapshotFromPod 从给定 Pod 构造组层级快照。
	//
	// 从 Pod 反推出整棵组层级并生成快照，是为了让后续决策基于同一时刻的一致视图，
	// 而不是多次查询缓存时组关系已经变了。
	BuildHierarchySnapshotFromPod(pod *v1.Pod) (PodGroupManager, error)
	// GetRootKeyForGroup 返回给定 EntityKey 的根节点 key。
	GetRootKeyForGroup(key EntityKey) (EntityKey, bool, error)
}

// 把 assumed 和 assigned 分开统计，是因为两者处在调度流程的不同阶段：
// 前者刚过 Reserve、尚未真正绑定，后者已落盘； gang scheduling 判断「够不够数」时需要区分。
// PodGroupState 提供单个 PodGroup 的状态视图。
type PodGroupState interface {
	// AllPods 返回调度器已知该组全部 Pod 的 UID。
	AllPods() sets.Set[types.UID]
	// AllPodsCount 返回该组调度器已知 Pod 的总数。
	AllPodsCount() int
	// UnscheduledPods 返回该组中所有尚未调度的 Pod，
	// 即既未 assume 也未分配。
	// 返回的 map 类型与 PodActivator.Activate 的参数一致。
	//
	// 直接返回 Pod 指针映射而非 UID 集合，是因为它要作为 PodActivator.Activate 的入参；
	// 形状对齐可以避免调用方再转换一遍，也减少中途对象被替换的可能。
	UnscheduledPods() map[string]*v1.Pod
	// AssumedPods 返回该组中处于 "assumed" 状态的 Pod（
	// 即已通过 Reserve 阶段）。
	AssumedPods() sets.Set[types.UID]
	// AssignedPods 返回该组已分配的 Pod UID。
	AssignedPods() sets.Set[types.UID]
	// ScheduledPods 返回该组中 assumed 或已分配的 Pod。
	ScheduledPods() []*v1.Pod
	// ScheduledPodsCount 返回该组中 assumed 或已分配的 Pod 数。
	ScheduledPodsCount() int
}

// CompositePodGroupState 提供单个组合 PodGroup 的状态视图。
type CompositePodGroupState interface {
	// GetChildren 返回子组的 key。
	GetChildren() []EntityKey
}
