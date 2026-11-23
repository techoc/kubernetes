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
// NodeInfoLister interface represents anything that can list/get NodeInfo objects from node name.
type NodeInfoLister interface {
	// List returns the list of NodeInfos.
	List() ([]NodeInfo, error)
	// HavePodsWithAffinityList returns the list of NodeInfos of nodes with pods with inter-pod (anti-)affinity terms.
	//
	// 单独维护「有亲和性 Pod 的节点」这个子集，是为了让 PreFilter 在扫描亲和性约束时
	// 只遍历可能相关的节点，不必对全量节点做 O(n) 检查。
	HavePodsWithAffinityList() ([]NodeInfo, error)
	// HavePodsWithRequiredAntiAffinityList returns the list of NodeInfos of nodes with pods with required inter-pod anti-affinity terms.
	HavePodsWithRequiredAntiAffinityList() ([]NodeInfo, error)
	// HavePodsWithRequiredNonHostScopedAntiAffinityList returns nodes containing pods that require a wider topology scan (topologyKey other than hostname).
	// PreFilter uses this to identify existing pods whose non-hostname scoped anti-affinity rules might conflict with the incoming pod, allowing it to skip scanning nodes that cannot possibly conflict.
	// It returns an empty list and no error if the InterPodAffinityHostnameFastPath feature gate is disabled.
	//
	// 非 hostname 作用域的反亲和需要跨拓扑域扫描，代价高得多；
	// 单独列出这类节点，PreFilter 就能先快速排除不可能冲突的节点。
	HavePodsWithRequiredNonHostScopedAntiAffinityList() ([]NodeInfo, error)
	// Get returns the NodeInfo of the given node name.
	Get(nodeName string) (NodeInfo, error)
}

// StorageInfoLister interface represents anything that handles storage-related operations and resources.
type StorageInfoLister interface {
	// IsPVCUsedByPods returns true/false on whether the PVC is used by one or more scheduled pods,
	// keyed in the format "namespace/name".
	//
	// 把 PVC 使用情况做成只读查询，是为了让 VolumeBinding 插件判断能否安全迁移/释放卷时，
	// 依据的是调度器已 Assume 的 Pod 视角，而不是可能滞后或与实际绑定不一致的外部状态。
	IsPVCUsedByPods(key string) bool
}

// 把一组 lister 收进一个聚合接口，是为了让 Handle 只暴露一个入口；
// 新增 lister 时只改这里和实现，不必动所有插件的签名。
// SharedLister groups scheduler-specific listers.
type SharedLister interface {
	NodeInfos() NodeInfoLister
	StorageInfos() StorageInfoLister
	// PodGroupStates provides access to dynamic state information for pod groups.
	PodGroupStates() PodGroupStateLister
	// PodGroups provides access to cached pod group objects.
	PodGroups() PodGroupLister
	// CompositePodGroupStates provides access to dynamic state information for composite pod groups.
	CompositePodGroupStates() CompositePodGroupStateLister
	// CompositePodGroups provides access to cached composite pod group objects.
	CompositePodGroups() CompositePodGroupLister
}

// PodGroupLister provides read access to cached pod group objects.
type PodGroupLister interface {
	// Get returns the PodGroup with the given namespace and name.
	Get(namespace, name string) (*schedulingapi.PodGroup, error)
}

// 可变更快照只开放给 PodGroupPostFilter：抢占/重排需要「假设移走某些 Pod 后」试探多种方案，
// 又必须在同一调度周期内恢复原状，否则会污染后续插件看到的世界。
// 限制只有一个 session，是因为快照是共享可变状态，并发会话没法保证回滚到正确基线。
// MutableSnapshotSharedLister interface represents a lister that allows mutating snapshot and restoring it afterwards.
// It extends SharedLister interface.
// Only PodGroupPostFilter extension point can use this.
type MutableSnapshotSharedLister interface {
	SharedLister
	// StartMutations starts a mutation session.
	// It is used for operations requiring modifying snapshot state for checking multiple scenarios.
	// There can be only one mutation session at the moment.
	// If StartMutations() is called, EndMutations() must be called in the same scheduling cycle.
	//
	// 把「开始试探」做成显式方法而非隐式构造，是为了强制调用方配对 EndMutations；
	// 整个周期内只允许一个 session，省去多会话叠加时基线难以定义的麻烦。
	StartMutations() error
	// EndMutations ends the mutation session and restores the snapshot state to the one before StartMutations.
	EndMutations() error
	// AddPod adds a given pod to the snapshot.
	// AddPod should be called only if the mutation was started via StartMutations.
	// This function is not thread safe, so it should be executed when no other routines can write/read from the snapshot.
	//
	// 明确要求非线程安全，是因为快照会被多个插件共享；
	// 若允许并发写，Read 到的中间态会让试探结果失去意义，索性把同步责任交给调用方。
	AddPod(podInfo PodInfo, nodeName string) error
	// RemovePod removes a given pod from the snapshot.
	// RemovePod should be called only if the mutation was started via StartMutations.
	// The state will be reverted when EndMutations is called.
	RemovePod(logger klog.Logger, pod *v1.Pod, nodeName string) error
}

// PodGroupStateLister provides read access to pod group states.
type PodGroupStateLister interface {
	// Get returns the PodGroupState of the given pod group.
	Get(namespace string, podGroupName string) (PodGroupState, error)
}

// CompositePodGroupLister provides read access to cached composite pod group objects.
type CompositePodGroupLister interface {
	// Get returns the CompositePodGroup with the given namespace and name.
	Get(namespace, name string) (*schedulingv1alpha3.CompositePodGroup, error)
}

// CompositePodGroupStateLister provides read access to composite pod group states.
type CompositePodGroupStateLister interface {
	// Get returns the CompositePodGroupState of the given composite pod group.
	Get(namespace string, compositePodGroupName string) (CompositePodGroupState, error)
}

type CSINodeLister interface {
	// List returns a list of all CSINodes.
	List() ([]*storagev1.CSINode, error)
	// Get returns the CSINode with the given name.
	Get(name string) (*storagev1.CSINode, error)
}

// 把「应用 DeviceTaint 规则」的口径收进 lister 内部，是为了让调度器与 DRA 驱动
// 对同一份 ResourceSlice 的看法一致；否则每个调用方各做一遍 taint 过滤，迟早出现分歧。
// ResourceSliceLister can be used to obtain ResourceSlices.
type ResourceSliceLister interface {
	// ListWithDeviceTaintRules returns a list of all ResourceSlices with DeviceTaintRules applied
	// if the DRADeviceTaints feature is enabled, otherwise without them.
	//
	// k8s.io/dynamic-resource-allocation/resourceslice/tracker provides an implementation
	// of the necessary logic. That tracker can be instantiated as a replacement for
	// a normal ResourceSlice informer and provides a ListPatchedResourceSlices method.
	ListWithDeviceTaintRules() ([]*resourceapi.ResourceSlice, error)
}

// DeviceClassLister can be used to obtain DeviceClasses.
type DeviceClassLister interface {
	// List returns a list of all DeviceClasses.
	List() ([]*resourceapi.DeviceClass, error)
	// Get returns the DeviceClass with the given className.
	Get(className string) (*resourceapi.DeviceClass, error)
}

// 在 informer 缓存之外再叠一层内存追踪，是为了填平「API 已改、informer 还没更新」的时间窗：
// 绑定阶段分配的 claim 必须立刻对后续调度可见，否则同一 claim 会被重复分配。
// ResourceClaimTracker can be used to obtain ResourceClaims, and track changes to ResourceClaims in-memory.
//
// If the claims are meant to be allocated in the API during the binding phase (when used by scheduler), the tracker helps avoid
// race conditions between scheduling and binding phases (as well as between the binding phase and the informer cache update).
//
// If the binding phase is not run (e.g. when used by Cluster Autoscaler which only runs the scheduling phase, and simulates binding in-memory),
// the tracker allows the framework user to obtain the claim allocations produced by the DRA plugin, and persist them outside of the API (e.g. in-memory).
type ResourceClaimTracker interface {
	// List lists ResourceClaims. The result is guaranteed to immediately include any changes made via AssumeClaimAfterAPICall(),
	// and SignalClaimPendingAllocation().
	//
	// 保证 List 立即反映 Assume/Pending 变更，是为了避免调度周期读到旧状态而做出错误分配决策；
	// 若依赖 informer 的最终一致性，窗口期内的竞争几乎无法避免。
	List() ([]*resourceapi.ResourceClaim, error)
	// Get works like List(), but for a single claim.
	Get(namespace, claimName string) (*resourceapi.ResourceClaim, error)
	// ListAllAllocatedDevices lists all allocated Devices from allocated ResourceClaims. The result is guaranteed to immediately include
	// any changes made via AssumeClaimAfterAPICall(), and SignalClaimPendingAllocation().
	ListAllAllocatedDevices() (sets.Set[structured.DeviceID], error)
	// GatherAllocatedState gathers information about allocated devices from allocated ResourceClaims. The result is guaranteed to immediately include
	// any changes made via AssumeClaimAfterAPICall(), and SignalClaimPendingAllocation().
	GatherAllocatedState() (*structured.AllocatedState, error)

	// SignalClaimPendingAllocation signals to the tracker that the given ResourceClaim will be allocated via an API call in the
	// binding phase, therefore the given ResourceClaim must be non-nil and have a non-nil Status.Allocation.
	// If the claim already has a pending allocation, then the allocation becomes shared. The same number of SignalClaimPendingAllocation() callers
	// for a given claimUID is expected to eventually call MaybeRemoveClaimPendingAllocation() for that claimUID.
	// This change is immediately reflected in the result of List() and the other accessors.
	//
	// 引用计数式的 pending 状态，是因为多个 Pod 可能同时依赖同一笔即将发生的分配；
	// 等所有调用方都显式撤销后再清理，才不会提前释放别的 Pod 仍在等待的资源。
	SignalClaimPendingAllocation(claimUID types.UID, allocatedClaim *resourceapi.ResourceClaim) error
	// ClaimHasPendingAllocation answers whether a given claim has a pending allocation during the binding phase. It can be used to avoid
	// race conditions in subsequent scheduling phases.
	GetPendingAllocation(claimUID types.UID) *resourceapi.AllocationResult
	// MaybeRemoveClaimPendingAllocation might remove the pending allocation for the given ResourceClaim from the tracker if any was signaled via
	// SignalClaimPendingAllocation(). When `forceRemove` is true, it always removes the pending allocation. Otherwise, it removes the pending
	// allocation only when no other pods are still using that pending allocation (from SignalClaimPendingAllocation and AcquirePendingAllocation).
	// Returns whether there was a pending allocation and it was removed.
	// List() and the other accessors immediately stop reflecting the pending allocation in the results when the pending allocation is removed.
	MaybeRemoveClaimPendingAllocation(claimUID types.UID, forceRemove bool) (deleted bool)

	// AssumeClaimAfterAPICall signals to the tracker that an API call modifying the given ResourceClaim was made in the binding phase, and the
	// changes should be reflected in informers very soon. This change is immediately reflected in the result of List() and the other accessors.
	// This mechanism can be used to avoid race conditions between the informer update and subsequent scheduling phases.
	//
	// Assume 是乐观更新：先按「请求会成功」推进调度，失败再回滚。
	// 这样无需等待 informer 回灌就能继续下一轮调度，代价是必须提供成对的 Restore。
	AssumeClaimAfterAPICall(claim *resourceapi.ResourceClaim) error
	// AssumedClaimRestore signals to the tracker that something went wrong with the API call modifying the given ResourceClaim, and
	// the changes won't be reflected in informers after all. List() and the other accessors immediately stop reflecting the assumed change,
	// and go back to the informer version.
	AssumedClaimRestore(namespace, claimName string)
}

// DeviceClassResolver resolves device class names from extended resource names.
type DeviceClassResolver interface {
	// GetDeviceClass returns the device class for the given extended resource name.
	// Returns nil if no mapping exists for the resource name or
	// the DRAExtendedResource feature is disabled.
	GetDeviceClass(resourceName v1.ResourceName) *resourceapi.DeviceClass
}

// 做成可替换的管理器，是为了让 Cluster Autoscaler 这类只用调度阶段的调用方
// 能注入模拟的 DRA 状态；默认实现走 API，模拟场景则在内存里伪造对象。
// SharedDRAManager can be used to obtain DRA objects, and track modifications to them in-memory - mainly by the DRA plugin.
// The plugin's default implementation obtains the objects from the API. A different implementation can be
// plugged into the framework in order to simulate the state of DRA objects. For example, Cluster Autoscaler
// can use this to provide the correct DRA object state to the DRA plugin when simulating scheduling changes in-memory.
type SharedDRAManager interface {
	ResourceClaims() ResourceClaimTracker
	ResourceSlices() ResourceSliceLister
	DeviceClasses() DeviceClassLister
	DeviceClassResolver() DeviceClassResolver
}

// 与 SharedDRAManager 同理：抽象出来是为了让模拟调度能替换 CSINode 的数据来源，
// 而不必真的连 apiserver。
// CSIManager can be used to obtain CSINode objects, and track changes to CSINode objects in-memory.
// The plugin's default implementation obtains the objects from the API. A different implementation can be
// plugged into the framework in order to simulate the state of CSINode objects. For example, Cluster Autoscaler
// can use this to provide the correct CSINode object state to the CSINode plugin when simulating scheduling changes in-memory.
type CSIManager interface {
	CSINodes() CSINodeLister
}

// 把组相关的 lister 和层级快照构造收在一个接口里，是因为 pod group 可以嵌套；
// 调用方需要沿层级向上追溯时，入口集中在这里比散落各处更可控。
// PodGroupManager provides an interface for runtime information about pod groups in the scheduler cache.
type PodGroupManager interface {
	// PodGroupStates returns the PodGroupStateLister.
	PodGroupStates() PodGroupStateLister
	// PodGroups returns the PodGroupLister.
	PodGroups() PodGroupLister
	// CompositePodGroupStates returns the CompositePodGroupStateLister.
	CompositePodGroupStates() CompositePodGroupStateLister
	// CompositePodGroups returns the CompositePodGroupLister.
	CompositePodGroups() CompositePodGroupLister
	// BuildHierarchySnapshotFromPod builds a hierarchy snapshot from the given pod.
	//
	// 从 Pod 反推出整棵组层级并生成快照，是为了让后续决策基于同一时刻的一致视图，
	// 而不是多次查询缓存时组关系已经变了。
	BuildHierarchySnapshotFromPod(pod *v1.Pod) (PodGroupManager, error)
	// GetRootKeyForGroup returns the root key of the given EntityKey.
	GetRootKeyForGroup(key EntityKey) (EntityKey, bool, error)
}

// 把 assumed 和 assigned 分开统计，是因为两者处在调度流程的不同阶段：
// 前者刚过 Reserve、尚未真正绑定，后者已落盘； gang scheduling 判断「够不够数」时需要区分。
// PodGroupState provides an interface to view the state of a single pod group.
type PodGroupState interface {
	// AllPods returns the UIDs of all pods known to the scheduler for this group.
	AllPods() sets.Set[types.UID]
	// AllPodsCount returns the number of all pods known to the scheduler for this group.
	AllPodsCount() int
	// UnscheduledPods returns all pods that are unscheduled for this group,
	// i.e., are neither assumed nor assigned.
	// The returned map type corresponds to the argument of the PodActivator.Activate method.
	//
	// 直接返回 Pod 指针映射而非 UID 集合，是因为它要作为 PodActivator.Activate 的入参；
	// 形状对齐可以避免调用方再转换一遍，也减少中途对象被替换的可能。
	UnscheduledPods() map[string]*v1.Pod
	// AssumedPods returns the UIDs of all pods for this group in the "assumed" state,
	// i.e., passed the Reserve gate.
	AssumedPods() sets.Set[types.UID]
	// AssignedPods returns the UIDs of all pods already assigned (bound) for this group.
	AssignedPods() sets.Set[types.UID]
	// ScheduledPods returns the pods that are either assumed or assigned for this pod group.
	ScheduledPods() []*v1.Pod
	// ScheduledPodsCount returns the number of pods for this group that are either assumed or assigned.
	ScheduledPodsCount() int
}

// CompositePodGroupState provides an interface to view the state of a single composite pod group.
type CompositePodGroupState interface {
	// GetChildren returns the keys of child groups.
	GetChildren() []EntityKey
}
