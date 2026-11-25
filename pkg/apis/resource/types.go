/*
Copyright 2022 The Kubernetes Authors.

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

package resource

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	// Finalizer is the finalizer that gets set for claims
	// which were allocated through a builtin controller.
	// Reserved for use by Kubernetes, DRA driver controllers must
	// use their own finalizer.
	Finalizer = "resource.kubernetes.io/delete-protection"
	// ExtendedResourceClaimAnnotation 是加在生成的特殊 ResourceClaim 上的注解。
	// 其唯一合法值为 "true"，且仅在调度器内部使用。
	ExtendedResourceClaimAnnotation = "resource.kubernetes.io/extended-resource-claim"
	// PodResourceClaimAnnotation is the annotation set on template-generated
	// ResourceClaims by the ResourceClaim controller. Its value is the
	// spec.resourceClaims[].name of the Pod or PodGroup for which the claim was
	// generated. The Pod and its PodGroup are guaranteed to use the same name.
	PodResourceClaimAnnotation = "resource.kubernetes.io/pod-claim-name"
	// 资源设备类前缀用于当设备类未指定 ExtendedResourceName 时，生成隐式的扩展资源名。
	// 生成名为该前缀加上设备类名
	// 生成的名称可能不是 pod.Spec.Resources.Requests 中合法的扩展资源名，
	// 此时必须在设备类中显式指定合法名称
	ResourceDeviceClassPrefix string = "deviceclass.resource.kubernetes.io/"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceSlice 表示由同一驱动管理的一组同类资源中的一个或多个资源。
// 一个池可以由多个 ResourceSlice 组成，具体数量由驱动决定。
//
// 目前唯一支持的资源是具有属性和容量的设备。给定池中的每个设备，
// 无论跨多少个 ResourceSlice，都必须具有唯一名称。设备发布所在的
// ResourceSlice 可能随时间变化。设备的唯一标识是三元组
// <driver name>、<pool name>、<device name>。
//
// 每当驱动需要更新池时，会将 pool.Spec.Pool.Generation 加一，并用该新编号
// 和新资源定义更新所有 ResourceSlice。使用者必须只使用 generation 编号最大的
// ResourceSlice，忽略其他所有。
//
// 在为满足特定条件的池分配所有资源时，或在若干不同备选方案中寻找最佳解时，
// 使用者应检查池中的 ResourceSlice 数量（包含在每个 ResourceSlice 中），
// 以判断其对池的视图是否完整；若不完整，应等待驱动完成池更新。
//
// 对于非节点本地资源，不设置节点名，而是
// 由驱动使用节点选择器指明设备可用的位置
//
// 这是 alpha 类型，需要开启 DynamicResourceAllocation 特性开关。
type ResourceSlice struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta

	// 包含由驱动发布的信息。
	//
	// 修改 spec 会自动递增 metadata.generation。
	Spec ResourceSliceSpec
}

const (
	// ResourceSliceSelectorNodeName 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.NodeName] 过滤。
	ResourceSliceSelectorNodeName = "spec.nodeName"
	// ResourceSliceSelectorDriver 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.Driver] 过滤。
	ResourceSliceSelectorDriver = "spec.driver"
	// ResourceSliceSelectorPoolName 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.Pool.Name] 过滤。
	ResourceSliceSelectorPoolName = "spec.pool.name"
)

// ResourceSliceSpec 保存单个 ResourceSlice 中由驱动发布的信息
type ResourceSliceSpec struct {
	// Driver 标识提供容量信息的 DRA 驱动。
	// 可用字段选择器只列出指定驱动的 ResourceSlice 对象。
	//
	// 必须是 DNS 子域，并应以驱动供应商所属的 DNS 域结尾。
	// 该字段不可变。
	//
	// +required
	Driver string

	// Pool 描述本 ResourceSlice 所属的池。
	// +required
	Pool ResourcePool

	// NodeName 标识提供此池中资源的节点。
	// 可用字段选择器只列出指定驱动的 ResourceSlice 对象
	// 属于特定节点的 ResourceSlice 对象
	//
	// 此字段可用于将节点对 ResourceSlice 的访问限制为同名节点。
	// 它也向自动扩缩容器表明：添加与某些旧节点同类型的新节点
	// 可能同样会带来新资源。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	// 该字段不可变。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeName *string

	// NodeSelector 定义当池不限于单个节点时，哪些节点可访问池中的资源。
	//
	// 必须且只能使用一个匹配项（term）
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeSelector *core.NodeSelector

	// AllNodes 表示所有节点均可访问池中的资源。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	AllNodes *bool

	// Devices 列出池中的部分或全部设备
	// 不得超过 128 项。若任一设备使用污点或消费计数器，上限为 64
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一
	// +optional
	// +listType=atomic
	// +zeroOrOneOf=ResourceSliceType
	Devices []Device

	// PerDeviceNodeSelection 定义节点对池中资源的访问是针对整个池统一设置，
	// 还是逐设备设置。
	// 设为 true 时，ResourceSlice 中定义的每个设备
	// 必须单独指定这一点。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	// +featureGate=DRAPartitionableDevices
	PerDeviceNodeSelection *bool

	// SharedCounters 定义计数器集合列表，每个集合都有一个名称和可用的计数器列表。
	// 计数器集合的名称在 ResourcePool 中必须唯一。
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一。
	// 计数器集合最多 8 个。
	// +optional
	// +listType=atomic
	// +featureGate=DRAPartitionableDevices
	// +zeroOrOneOf=ResourceSliceType
	SharedCounters []CounterSet

	// PartitionTypeAttribute names a string device attribute (by fully
	// qualified name, e.g. "gpu.example.com/profile") whose value labels
	// each device with its partition type, such as "Full" or "Half" for a
	// MIG-style GPU.
	//
	// When set, every partitionable device in the slice must carry the attribute
	// and devices sharing a value must share the same ConsumesCounters cost.
	//
	// +optional
	// +featureGate=DRAPartitionableDevicesType
	PartitionTypeAttribute *FullyQualifiedName

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，本分片中设备将跳过的
	// 节点本地资源操作（gRPC 调用）。如果某驱动在声明中的所有已分配设备都跳过某操作，
	// 该 gRPC 调用就会被跳过。合法取值为：
	// - "NodePrepareResources"：跳过 NodePrepareResources gRPC 调用。除非同时列出
	// "NodeUnprepareResources"（或指定 "*"），否则不能指定该值。
	// - "NodeUnprepareResources"：跳过 NodeUnprepareResources gRPC 调用。
	// - "*"：跳过所有节点本地资源操作。
	// 未来可能增加其他取值，kubelet 必须忽略未知取值。
	// +optional
	// +listType=set
	// +featureGate=DRAOptionalNodeOperations
	SkipNodeOperations []SkipNodeOperation
}

// +enum
type SkipNodeOperation string

const (
	// SkipNodeOperationNodePrepareResources 表示跳过 NodePrepareResources gRPC 调用。
	SkipNodeOperationNodePrepareResources SkipNodeOperation = "NodePrepareResources"

	// SkipNodeOperationNodeUnprepareResources 表示跳过 NodeUnprepareResources gRPC 调用。
	SkipNodeOperationNodeUnprepareResources SkipNodeOperation = "NodeUnprepareResources"

	// SkipNodeOperationAll 表示跳过所有节点本地资源操作
	SkipNodeOperationAll SkipNodeOperation = "*"
)

// CounterSet defines a named set of counters
// that are available to be used by devices defined in the
// ResourcePool.
//
// The counters are not allocatable by themselves, but
// can be referenced by devices. When a device is allocated,
// the portion of counters it uses will no longer be available for use
// by other devices.
type CounterSet struct {
	// Name 定义计数器集合的名称，必须是 DNS label。
	// +required
	Name string

	// Counters 定义本 CounterSet 的计数器集合。
	// 每个计数器的名称在集合内必须唯一，且必须是 DNS 标签。
	// 计数器最大数量为 32。
	// +required
	Counters map[string]Counter
}

// DriverNameMaxLength 是 ResourceSliceSpec 及其他位置中驱动名的最大合法长度，
// 与 CSI 驱动名的限制相同
const DriverNameMaxLength = 63

// ResourcePool 描述 ResourceSlice 所属的池
type ResourcePool struct {
	// Name 用于标识池。对于节点本地设备，它通常是节点名，但并非必须。
	// 可以使用字段选择器只列出属于某个池的 ResourceSlice 对象。
	// 长度不得超过 253 个字符，且必须由一个或多个以斜杠分隔的 DNS 子域组成。
	// 该字段不可变。
	// +required
	Name string

	// Generation tracks the change in a pool over time. Whenever a driver
	// changes something about one or more of the resources in a pool, it
	// must change the generation in all ResourceSlices which are part of
	// that pool. Consumers of ResourceSlices should only consider
	// resources from the pool with the highest generation number. The
	// generation may be reset by drivers, which should be fine for
	// consumers, assuming that all ResourceSlices in a pool are updated to
	// match or deleted.
	//
	// Combined with ResourceSliceCount, this mechanism enables consumers to
	// detect pools which are comprised of multiple ResourceSlices and are
	// in an incomplete state.
	//
	// +required
	Generation int64

	// ResourceSliceCount is the total number of ResourceSlices in the pool at this
	// generation number. Must be greater than zero.
	//
	// Consumers can use this to check whether they have seen all ResourceSlices
	// belonging to the same pool.
	//
	// +required
	ResourceSliceCount int64
}

const ResourceSliceMaxSharedCapacity = 128
const ResourceSliceMaxDevices = 128

// ResourceSliceMaxDevicesWithAdvancedFeatures 定义当任意设备使用高级特性时，
// 一个 ResourceSlice 中设备的最大数量。高级特性包括：
// - 设备污点（DRADeviceTaints 特性门控）
// - 消费计数器（DRAPartitionableDevices 特性门控）
// - 列表属性（DRAListTypeAttributes 特性门控）
const ResourceSliceMaxDevicesWithAdvancedFeatures = 64

const PoolNameMaxLength = validation.DNS1123SubdomainMaxLength // Same as for a single node name.
const BindingConditionsMaxSize = 4
const BindingFailureConditionsMaxSize = 4

// 定义 ResourceSlice 中通过 SharedCounters 字段
// 可定义的计数器集合的最大数量
const ResourceSliceMaxCounterSets = 8

// 定义单个计数器集合（counter set）中可定义的计数器最大数量
const ResourceSliceMaxCountersPerCounterSet = 32

// 定义每个设备通过 ConsumesCounters 字段
// 可定义的计数器消费记录的最大数量
const ResourceSliceMaxDeviceCounterConsumptionsPerDevice = 2

// 定义每次设备计数器消费中可定义的计数器最大数量
const ResourceSliceMaxCountersPerDeviceCounterConsumption = 32

// 定义每次设备计数器消费中可声明的兼容组最大数量
// DeviceCompatibilityGroupsMaxSize 是单次计数器消费中兼容组的最大数量
const DeviceCompatibilityGroupsMaxSize = 2

// Device represents one individual hardware instance that can be selected based
// on its attributes. Besides the name, exactly one field must be set.
type Device struct {
	// Name 是池中该驱动管理的所有设备之间的唯一标识，必须是 DNS label。
	// +required
	Name string

	// Attributes 定义该设备的属性集合
	// 每个属性的名称在集合内必须唯一
	// 属性与容量的总数最多 32 个
	// +optional
	Attributes map[QualifiedName]DeviceAttribute

	// Capacity 定义该设备的容量集合。每个容量的名称在集合内必须唯一。
	// 属性与容量的总数最多为 32。
	// +optional
	Capacity map[QualifiedName]DeviceCapacity

	// ConsumesCounters 定义对 sharedCounters 的引用列表，
	// 以及设备将从这些计数器集合中消费的计数器集合
	// 每个 counterSet 只能有一条记录
	// 每个设备最多 2 条计数器消费记录
	// +optional
	// +listType=atomic
	// +featureGate=DRAPartitionableDevices
	ConsumesCounters []DeviceCounterConsumption

	// NodeName 标识设备可用的节点。
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才可设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeName *string

	// NodeSelector 定义设备可用的节点。
	//
	// 必须且只能使用一个匹配项（term）
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才可设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeSelector *core.NodeSelector

	// AllNodes 表示所有节点均可访问该设备。
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才可设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	AllNodes *bool

	// 若指定，这些是驱动定义的污点。
	//
	// 污点的最大数量为 16。若 ResourceSlice 中任意设备设置了污点，
	// 则每个 ResourceSlice 允许的最大设备数为 64 而非 128。
	//
	// 这是 beta 字段，需要开启 DRADeviceTaints 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	Taints []DeviceTaint

	// BindsToNode 表示涉及此设备的分配使用是否必须绑定到做出分配的那个精确节点。
	// 设为 true 时，调度器会将 ResourceClaim.Status.Allocation.NodeSelector 设置为匹配
	// 执行分配所在的节点
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindsToNode *bool

	// BindingConditions 定义继续执行绑定所需满足的条件。
	// 调度 Pod 时，这些条件必须在各设备的 status.conditions 中
	// 全部为 True，才能继续将 Pod 绑定到节点
	//
	// 条件必须是合法的 condition 类型字符串，
	// 因此所有条目都必须是标签。
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindingConditions []string

	// BindingFailureConditions 定义绑定失败的条件。
	// 这些条件可设置在各设备的 status.conditions 中
	// 若其中任一条件为 True，即发生了绑定失败。
	//
	// 条件必须是合法的 condition 类型字符串，
	// 因此所有条目都必须是标签。
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindingFailureConditions []string

	// AllowMultipleAllocations marks whether the device is allowed to be allocated to multiple DeviceRequests.
	//
	// If AllowMultipleAllocations is set to true, the device can be allocated more than once,
	// and all of its capacity is consumable, regardless of whether the requestPolicy is defined or not.
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	AllowMultipleAllocations *bool

	// NodeAllocatableResources defines the mapping of node resources
	// that are managed by the DRA driver exposing this device. This includes resources currently
	// reported in v1.Node `status.allocatable` that are not extended resources
	// (see https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources).
	// Examples include "cpu", "memory", "ephemeral-storage", and hugepages.
	// In addition to standard requests made through the Pod `spec`, these resources
	// can also be requested through claims and allocated by the DRA driver.
	// For example, a CPU DRA driver might allocate exclusive CPUs or auxiliary node memory
	// dependencies of an accelerator device.
	// The keys of this map are the node-allocatable resource names (e.g., "cpu", "memory").
	// Extended resource names are not permitted as keys.
	// +optional
	// +featureGate=DRANodeAllocatableResources
	NodeAllocatableResources map[v1.ResourceName]NodeAllocatableResource
}

// NodeAllocatableResource defines the translation of the requested DRA device/capacity
// units to the corresponding quantity of the node-allocatable resource.
// At least one of Mapping or Overhead must be specified.
type NodeAllocatableResource struct {
	// Mapping is used when the device directly models a node allocatable resource like standard CPU or memory
	// (e.g., with a CPU DRA driver). The calculated quantity is accounted for exactly once per claim instance
	// on the node. To prevent node cgroup isolation friction, the scheduler explicitly
	// blocks sharing mapped device claims across multiple pods.
	// +optional
	Mapping *NodeAllocatableMapping

	// Overhead contains fields for modeling auxiliary overhead incurred on node allocatable resources
	// when allocating devices that are not themselves modeling a node allocatable resource (e.g., host memory overhead for GPUs).
	// Sharing overhead-mapped claims across multiple pods is allowed. The node allocatable overhead is accounted
	// for individually for each pod referencing the claim.
	// Overhead is always subtracted from the node's allocatable capacity for the resource, even when mapping
	// is specified for the same resource.
	// Eg: If a device models memory capacity per socket as a consumable capacity pool via Mapping (with CapacityKey),
	// any overhead specified for the same resource will be subtracted from the node's general allocatable capacity
	// and not from the per-socket capacity pool in Mapping.
	// +optional
	Overhead *NodeAllocatableOverhead
}

// NodeAllocatableMapping defines how a DRA allocation directly translates into a node allocatable resource quantity.
// The mapping can be derived from either the count of allocated devices (via deviceMultiplier) or the specific capacity consumed (via capacityKey and capacityMultiplier). These options are mutually exclusive.
// Kubelet adds this mapped resource quantity from claim to both requests and limits at the pod-level cgroup, and to limits at the container-level cgroup for each container referencing the claim.
type NodeAllocatableMapping struct {
	// CapacityKey references a capacity name defined as a key in the
	// `spec.devices[*].capacity` map. When this field is set, the value associated with
	// this key in the `status.allocation.devices.results[*].consumedCapacity` map
	// (for a specific claim allocation) determines the base quantity for
	// the node allocatable resource. `capacityMultiplier` must also be set and is
	// multiplied with the base quantity.
	// For example, if `spec.devices[*].capacity` has an entry "dra.example.com/memory": "128Gi",
	// and this field is set to "dra.example.com/memory", then for a claim allocation
	// that consumes { "dra.example.com/memory": "4Gi" } the base quantity for the
	// node allocatable resource mapping will be "4Gi".
	// The final node allocatable resource amount is `consumedCapacity[capacityKey]` * `capacityMultiplier`.
	// +optional
	CapacityKey *QualifiedName

	// CapacityMultiplier 用作已分配且被消耗容量的乘数，仅在设置了 `capacityKey` 时有效。
	// 最终节点可分配资源量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// 例如，若消耗了某设备的容量 "dra.example.com/cores"，且每个 "core" 提供 2 个 "cpu"，
	// 映射为：{ResourceName: "cpu", capacityKey: "dra.example.com/cores", capacityMultiplier: "2"}。
	// 若声明消耗 8 个 "dra.example.com/cores"，则 CPU 占用为 8 * 2 = 16。
	// +optional
	CapacityMultiplier *resource.Quantity

	// DeviceMultiplier is used as a multiplier for the allocated device count in the claim.
	// The final node allocatable resource amount is `deviceCount` * `deviceMultiplier`.
	// For example, a DRA driver representing each cache complex (CCX) as a device would have
	// {ResourceName: "cpu", deviceMultiplier: "8"} in its `nodeAllocatableResources`.
	// If 2 devices (CCX) are allocated to the claim, 2 * 8 = 16 CPUs would be considered as allocated.
	// It is only valid when `capacityKey` and `capacityMultiplier` are not set.
	// +optional
	DeviceMultiplier *resource.Quantity
}

// NodeAllocatableOverhead 定义分配设备时产生的辅助资源开销。
// 开销可以指定为引用该 claim 的每个 Pod 的固定成本、每次容器引用的可变成本，或两者兼有。
// Kubelet 通过将开销同时加入引用容器的 Pod 级与容器级 cgroup 来计入此开销。
type NodeAllocatableOverhead struct {
	// PerPod 是对本节点上每个引用该 claim 的 Pod 施加一次的开销
	// 它是每个引用 claim 的 Pod 都产生的固定开销
	// +optional
	PerPod *resource.Quantity

	// PerContainer is applied per container reference to the claim.
	// This models overhead scaling linearly with the number of containers actively using the device.
	// When both PerPod and PerContainer are specified, the total overhead allocated for each pod referencing
	// the claim is computed as:
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet accounts for this overhead in cgroups:
	// - Pod-level cgroup (requests and limits): Kubelet adds PerPod + (PerContainer * NumReferences).
	// - Container-level cgroup (limits only): Kubelet adds PerPod + PerContainer for each referencing container.
	// This allows any single container to access the pod-level overhead, while the parent cgroup caps the total usage to account for PerPod exactly once.
	// +optional
	PerContainer *resource.Quantity
}

// DeviceCounterConsumption defines a set of counters that
// a device will consume from a CounterSet.
type DeviceCounterConsumption struct {
	// CounterSet 是要消耗的计数器所属集合的名称。
	// +required
	CounterSet string

	// Counters 定义设备将要消费的计数器。
	// 计数器最多 32 个。
	// +required
	Counters map[string]Counter

	// CompatibilityGroups 是此计数器集消费对应的一组不透明（opaque）组名。
	//
	// 从同一计数器集消费计数器的设备，只有当它们共享至少一个公共组时
	// 才能同时被分配（"共同分配"）：该计数器集上所有被共同分配设备的
	// CompatibilityGroups 交集必须非空。从不同计数器集消费的设备
	// 永远不会通过此字段进行比较。
	//
	// 未设置、显式 nil 与空列表等价，均表示"没有组"：这样的设备只能与
	// 同一计数器集上同样没有组的兄弟设备共同分配，且绝不能与声明了一个
	// 或多个组的设备共同分配。
	//
	// 组名是不透明的，仅在发布驱动的池内有意义。
	//
	// 组数最多 2 个，名称必须唯一
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceCompatibilityGroups
	CompatibilityGroups []string
}

// DeviceCapacity 描述与设备关联的数量。
type DeviceCapacity struct {
	// Value 定义设备拥有某种容量的数量
	// 本字段反映固定的总容量，不会改变
	// 已消费的数量由调度器单独跟踪，
	// 不影响本值
	// +required
	Value resource.Quantity

	// RequestPolicy 定义当设备允许被多个分配共享时，本 DeviceCapacity 应如何被消耗。
	// 要设置 requestPolicy，设备必须将 allowMultipleAllocations 设为 true。
	// 未设置时，容量请求没有约束：请求可以消耗任意数量的容量，
	// 只要所有分配消耗的总量不超过设备定义的容量即可。若请求也未设置，默认值为全部容量。
	// +optional
	// +featureGate=DRAConsumableCapacity
	RequestPolicy *CapacityRequestPolicy
}

// Counter describes a quantity associated with a device.
type Counter struct {
	// Value 定义某项设备计数器可用的数量。
	// +required
	Value resource.Quantity
}

// CapacityRequestPolicyDiscreteMaxOptions limits the number of discrete capacity values allowed in a requestPolicy.
const CapacityRequestPolicyDiscreteMaxOptions = 10

// CapacityRequestPolicy 定义请求如何消费设备容量。
//
// 最多只能设置一个 ValidRequestValues。
type CapacityRequestPolicy struct {
	// Default 指定当请求在 DeviceRequest 的 Capacity 中没有对应条目时，
	// 该容量默认被消费的数量。
	// +optional
	Default *resource.Quantity

	// ValidValues 定义消费请求中可接受的数量值集合。
	// 不得超过 10 项，且必须按升序排列。
	// 设置该字段时，必须定义 Default，且 Default 必须包含在 ValidValues 列表中。
	// 若请求数量不匹配任何有效值但小于某些有效值，调度器会计算出大于等于该请求的最小有效值，
	// 即：min(ceil(requestedValue) ∈ validValues)，其中 requestedValue ≤ max(validValues)。
	// 若请求数量超过所有有效值，则该请求违反策略，无法分配该设备。
	// +optional
	// +listType=atomic
	// +oneOf=ValidRequestValues
	ValidValues []resource.Quantity

	// ValidRange 定义消费请求中可接受的数量范围。
	// 设置该字段时，必须定义 Default，且 Default 必须落在 ValidRange 范围内。
	// 若请求数量不在该范围内，则请求违反策略，无法分配该设备。
	// 若请求不包含该容量条目，则使用 Default 值。
	// +optional
	// +oneOf=ValidRequestValues
	ValidRange *CapacityRequestPolicyRange
}

// CapacityRequestPolicyRange 定义可消费容量值的合法范围
//
// 如果启用 DRAFractionalCapacityRange 特性门控，
// 且 Min、Max、Step 中至少有一个是分数量（即其值不是整数），
// 则改用毫单位算术，
// 支持最多 3 位小数（例如 100m = 0.1）
// 此时支持的最大值比使用 64 位整数时小 1000 倍
// 否则，所有比较都通过 resource.Quantity.Value() 使用 64 位整数算术
//
// - 若请求量小于 Min，向上取整到 Min
// - 若设置了 Step，且请求量在 Min 与 Max 之间但未与 Step 对齐，
// 则向上取整到下一个等于 Min + (n * Step) 的值
// - 若未设置 Step，请求量在 Min 到 Max（如设置）范围内时原样使用
// - 若请求量或取整后的值超过 Max（如设置），则请求不满足策略，
// 设备不可被分配
type CapacityRequestPolicyRange struct {
	// Min 指定消费请求允许的最小容量。
	// Min 必须大于等于零，且小于等于容量值。
	// requestPolicy.default 必须大于等于该最小值。
	// +required
	Min *resource.Quantity

	// Max 定义可请求容量的上限。
	// Max 必须小于等于容量值。Min 和 requestPolicy.default 必须小于等于该上限。
	// +optional
	Max *resource.Quantity

	// Step 定义范围内有效容量值之间的步长。
	// Max（若设置）和 requestPolicy.default 必须是 Step 的倍数。
	// Min + Step 必须小于等于容量值。
	// +optional
	Step *resource.Quantity
}

// 属性与容量条目数之和的上限
const ResourceSliceMaxAttributesAndCapacitiesPerDevice = 32

// 每个设备中，列表与非列表属性里字符串、版本、布尔或整数值的总数上限
// ResourceSliceMaxAttributeValuesPerDevice 是单个设备中属性值的最大数量
const ResourceSliceMaxAttributeValuesPerDevice = 48

// QualifiedName 是设备属性或容量的名称。
//
// 属性和容量由特定驱动的所有者（通常是供应商）或第三方（例如 Kubernetes
// 项目）定义。由于它们有时需要在设备之间比较，因此同一个名称在所有设备
// 上应当含义相同、类型相同。
//
// 名称必须是 C 标识符（例如 "theName"），或者 DNS 子域加斜杠（"/"）再加
// C 标识符（例如 "dra.example.com/theName"）。不带域名前缀的名称被认为属于
// 驱动的域。第三方定义的属性或容量必须带域名前缀。
//
// DNS 子域的最大长度为 63 个字符（与驱动名相同），C 标识符的最大长度
// 为 32。
type QualifiedName string

// FullyQualifiedName is a QualifiedName where the domain is set.
type FullyQualifiedName string

// DeviceMaxDomainLength 是完全限定名中域前缀的最大长度
const DeviceMaxDomainLength = 63

// DeviceMaxIDLength 是设备属性或容量名（`<domain>/<ID>`）中标识符的最大长度。
const DeviceMaxIDLength = 32

// DeviceAttribute 必须且只能设置一个字段
type DeviceAttribute struct {
	// 下面的 Go 字段名带 Value 后缀，是为了避免字段 "String" 与对应方法名冲突；
	// 该方法是必需的
	// Kubernetes API 定义中不带该后缀，以保持自然

	// IntValue is a number.
	//
	// +optional
	// +oneOf=ValueType
	IntValue *int64

	// BoolValue is a true/false value.
	//
	// +optional
	// +oneOf=ValueType
	BoolValue *bool

	// StringValue 是字符串，长度不得超过 64 个字符。
	// +optional
	// +oneOf=ValueType
	StringValue *string

	// VersionValue 是符合 semver.org 2.0.0 规范的语义化版本，长度不得超过 64 个字符。
	// +optional
	// +oneOf=ValueType
	VersionValue *string

	// IntValues 是一组非空数字。
	//
	// 这是 alpha 字段，需要开启 DRAListTypeAttributes 特性开关。
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValueType
	// +featureGate=DRAListTypeAttributes
	IntValues []int64

	// BoolValues is a non-empty list of true/false values.
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValueType
	// +featureGate=DRAListTypeAttributes
	BoolValues []bool

	// StringValues 是一组非空字符串。
	// 每个字符串不得超过 64 个字符
	//
	// 这是 alpha 字段，需要开启 DRAListTypeAttributes 特性开关。
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValueType
	// +featureGate=DRAListTypeAttributes
	StringValues []string

	// VersionValues 是一组非空的、符合 semver.org spec 2.0.0 的语义化版本。
	// 每个版本字符串长度不得超过 64 个字符。
	//
	// 这是 alpha 字段，需要开启 DRAListTypeAttributes 特性开关。
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValueType
	// +featureGate=DRAListTypeAttributes
	VersionValues []string
}

// DeviceAttributeMaxValueLength 是字符串或版本属性值的最大长度。
const DeviceAttributeMaxValueLength = 64

// DeviceTaintsMaxLength 是每个设备污点的最大数量
const DeviceTaintsMaxLength = 16

// The device this taint is attached to has the "effect" on
// any claim which does not tolerate the taint and, through the claim,
// to pods using the claim.
type DeviceTaint struct {
	// 施加到设备上的污点键
	// 必须是 label 名
	// +required
	Key string

	// 与 taint 键对应的 taint 值，必须是 label 值。
	// +optional
	Value string

	// taint 对不容忍它的 claim 的影响，并通过这些 claim 影响使用它们的 Pod。
	// 有效 effect 为 None、NoSchedule 和 NoExecute。节点上使用的 PreferNoSchedule 在这里无效。
	// 未来可能添加更多 effect。消费方必须将未知 effect 当作 None 处理。
	// +required
	Effect DeviceTaintEffect

	// ^^^^
	//
	// 实现 PreferNoSchedule 依赖于 DRA 的评分方案
	// 届时可能会一并加入
	//
	// 未来可能新增效果 NoExecuteWithPodDisruptionBudget：
	// 遵循 PodDisruptionBudget，而非直接删除 Pod
	// 目前尚未确定，也可能作为单独字段实现
	//
	// 校验必须允许已存储对象中出现未知枚举值，
	// 这样才能在单个版本内新增枚举值，而无需
	// ratcheting.

	// TimeAdded represents the time at which the taint was added or
	// (only in a DeviceTaintRule) the effect was modified.
	// Added automatically during create or update if not set.
	// In addition, in a DeviceTaintRule a value provided during
	// an update gets replaced with the current time if the provided
	// value is the same as the old one and the new effect is different.
	//
	// +optional
	TimeAdded *metav1.Time

	// ^^^
	//
	// 对于节点污点，该字段原先的定义是“只对 NoExecute 污点写入”。
	// 但实际上 Kubernetes 从未对它做过任何处理（无校验、无默认值，
	// 在 pkg/controller/tainteviction 的 Pod 驱逐中被忽略）。
}

// +enum
type DeviceTaintEffect string

const (
	// 无影响，该 taint 仅提供信息。
	DeviceTaintEffectNone DeviceTaintEffect = "None"

	// 不允许新的 Pod 调度到使用被污染设备的节点上，除非它们容忍该污点；
	// 但允许所有未经调度器、直接提交给 Kubelet 的 Pod 启动，
	// 也允许所有已经在运行的 Pod 继续运行。
	DeviceTaintEffectNoSchedule DeviceTaintEffect = "NoSchedule"

	// 驱逐所有不容忍该设备污点的已运行 Pod
	DeviceTaintEffectNoExecute DeviceTaintEffect = "NoExecute"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceSliceList 是 ResourceSlice 的集合。
type ResourceSliceList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items is the list of resource ResourceSlices.
	Items []ResourceSlice
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaim 描述对集群中资源的访问请求，供工作负载使用。
// 例如，若工作负载需要具有特定属性的加速器设备，就通过此对象表达该请求。
// status 部分跟踪该声明是否已被满足，以及分配了哪些具体资源。
//
// 这是 alpha 类型，需要开启 DynamicResourceAllocation 特性开关。
type ResourceClaim struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta

	// Spec 描述请求的内容及配置方式，该 spec 不可变。
	Spec ResourceClaimSpec

	// Status 描述该 claim 是否可用以及已分配的资源。
	// +optional
	Status ResourceClaimStatus
}

// ResourceClaimSpec 定义 ResourceClaim 请求的内容及配置方式
type ResourceClaimSpec struct {
	// Devices 定义如何请求设备
	// +optional
	Devices DeviceClaim

	// Controller 自 Kubernetes 1.32 起被墓碑化，在该版本中
	// 该字段已被移除。待不再需要兼容 v1alpha3 解码时可复用。
	// Controller string
}

// DeviceClaim 定义如何通过 ResourceClaim 请求设备
type DeviceClaim struct {
	// Requests 表示对不同设备的独立请求，所有请求都必须得到满足。
	// 若为空，则无需分配任何资源。
	// +optional
	// +listType=atomic
	Requests []DeviceRequest

	// 为该 claim 分配的设备集合必须满足这些约束。
	// +optional
	// +listType=atomic
	Constraints []DeviceConstraint

	// 该字段保存若干潜在驱动的配置，这些驱动都可能满足本 claim 中的请求。
	// 分配 claim 时会忽略该字段
	// +optional
	// +listType=atomic
	Config []DeviceClaimConfiguration

	// 未来可能的扩展，旧调度器会忽略。这是可行的，
	// 因为评分只表达用户偏好，
	// 不会构成硬性要求
	//
	// Score *SomeScoringStruct
}

const (
	DeviceRequestsMaxSize          = AllocationResultsMaxSize
	DeviceConstraintsMaxSize       = 32
	DeviceConfigMaxSize            = 32
	DeviceDerivedAttributesMaxSize = 32
)

// DRAAdminNamespaceLabelKey 是用于授予命名空间内某些 resource.k8s.io API 类型管理访问权限的标签键。
// 当命名空间上该标签的值（区分大小写）为 "true" 时，
// 允许在任何命名空间级的 resource.k8s.io API 类型中使用 adminAccess: true。
// 目前该权限适用于
// ResourceClaim 和 ResourceClaimTemplate 对象
const (
	DRAAdminNamespaceLabelKey = "resource.kubernetes.io/admin-access"
)

// DeviceRequest is a request for devices required for a claim.
// This is typically a request for a single resource like a device, but can
// also ask for several identical devices. With FirstAvailable it is also
// possible to provide a prioritized list of requests.
type DeviceRequest struct {
	// Name 可用于在 pod.spec.containers[].resources.claims 条目及 claim 的约束中引用本请求
	// 设置 Exactly 时，通过 DeviceRequest 名称的引用能唯一标识一个请求；
	// 设置 FirstAvailable 时，对 DeviceRequest 名称的引用
	// 会匹配调度器最终选中的任意子请求
	// 必须是 DNS label
	// +required
	Name string

	// Exactly 指定单个请求的细节，必须完全满足该请求才算成功。
	// Exactly 或 FirstAvailable 必须设置一个。
	// +optional
	// +oneOf=deviceRequestType
	Exactly *ExactDeviceRequest

	// FirstAvailable 包含若干子请求，调度器会从中选择一个。
	// 它按此处列出的顺序依次尝试满足，
	// 因此若列表中有两项，只有判定第一项不可用后，
	// 调度器才会检查第二项
	// DRA 尚未实现评分，因此调度器会
	// 选择第一组满足 claim 中所有请求的设备。
	// 若需求可以在多个节点上满足，
	// 则由其他调度特性决定选择哪个节点。
	// 这意味着分配给 claim 的设备集合
	// 可能不是集群中的最优选择。评分功能将在以后实现
	// +optional
	// +oneOf=deviceRequestType
	// +listType=atomic
	// +featureGate=DRAPrioritizedList
	FirstAvailable []DeviceSubRequest
}

// ExactDeviceRequest 表示对一个或多个相同设备的请求
type ExactDeviceRequest struct {
	// DeviceClassName 引用一个具体的 DeviceClass，该类可定义本请求继承的额外配置与选择器
	// DeviceClassName 必填
	// 管理员可以只安装带有允许设备选择器的类，以限制用户可请求的设备。
	// 若允许用户无限制地请求任意资源，
	// 管理员可以创建一个空的 DeviceClass 供用户引用
	// +required
	DeviceClassName string

	// Selectors 定义某个设备要被本请求考虑所必须满足的条件，所有选择器都必须满足
	// +optional
	// +listType=atomic
	Selectors []DeviceSelector

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求。支持的值为：
	// - ExactCount：请求指定数量的设备
	// 这是默认值，具体数量在 count 字段中给出
	// - All：请求池中全部匹配的设备
	// 节点上至少需要一个设备，分配才能成功
	// 若部分设备已被分配，分配将失败，
	// 除非请求了 adminAccess
	// 未指定 AllocationMode 时默认为 ExactCount。若模式为 ExactCount 且未指定 count，默认为 1。
	// 其他子请求必须指定本字段
	// 未来可能增加更多模式。客户端必须拒绝处理未知模式的请求
	// +optional
	AllocationMode DeviceAllocationMode

	// Count 仅在模式为 "ExactCount" 时使用，必须大于零
	// 若 AllocationMode 为 ExactCount 且未指定本字段，默认为 1
	// +optional
	// +oneOf=AllocationMode
	Count int64

	// AdminAccess 表示这是对设备的管理访问请求。带 AdminAccess 的 claim 预期用于
	// 设备监控或其他管理服务。在访问模式和资源分配方面，它们会忽略设备上所有普通 claim。
	// 这是 alpha 字段，需要启用 DRAAdminAccess feature gate。未设置或设为 false 时
	// 禁用管理访问，否则启用。
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool

	// 若指定，此为请求的容忍（tolerations）。
	//
	// 要分配带有该效果污点的设备，必须具有对应的 NoSchedule 容忍。
	// NoExecute 同理。
	//
	// 此外，若任何已分配设备在分配后被加上 NoExecute 污点且未被容忍，
	// 则所有消费该 ResourceClaim 的 Pod 会被删除以驱逐它们。当 claim
	// 持有这些带污点设备时，调度器不会让新 Pod 预留该 claim。一旦所有
	// Pod 都被驱逐，该 claim 将被解除分配。
	//
	// tolerations 的最大数量为 16。
	//
	// 这是 beta 字段，需要开启 DRADeviceTaints 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	Tolerations []DeviceToleration

	// Capacity 定义针对每项容量的资源需求。
	// 若未设置该字段且设备支持多次分配，则根据 requestPolicy 对每项容量应用默认值。
	// 没有 requestPolicy 的容量，默认值为全部容量。
	// 该配置适用于每次设备分配。若 Count > 1，当没有足够设备满足要求时请求失败。
	// 若 AllocationMode 为 All，当存在其他方面匹配请求、具备该容量且值 >= 请求量、
	// 但无法分配给本请求的设备时，请求失败。
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements

	// DerivedAttributes 定义一组虚拟属性，它们通过 CEL 表达式针对每个候选
	// 设备计算得出。这些虚拟属性可以在 `.devices.constraints` 中被引用，用于
	// 对齐和匹配不同设备（例如在同一 NUMA 节点上共同分配 GPU 和网卡），即使
	// 它们的驱动发布了不同的属性。在求值 selector 表达式的 CEL 环境中，
	// 派生属性不通过 `device.attributes` 提供。
	//
	// 派生属性允许你在调度时提取、转换或规范化拓扑信息（例如从复杂拓扑字符串
	// 中提取 NUMA 索引，或重命名供应商特定属性）为统一的虚拟属性名。调度器在
	// 匹配约束时会像求值静态属性一样求值这些虚拟属性。
	//
	// 列表中定义的每个派生属性必须被 `.devices.constraints` 列表中至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性最多 32 个
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	DerivedAttributes []DeviceDerivedAttribute
}

// DeviceSubRequest describes a request for device provided in the
// claim.spec.devices.requests[].firstAvailable array. Each
// is typically a request for a single resource like a device, but can
// also ask for several identical devices.
//
// DeviceSubRequest is similar to ExactDeviceRequest, but doesn't expose the
// AdminAccess field as that one is only supported when requesting a
// specific device.
type DeviceSubRequest struct {
	// Name 可用于在 claim 的约束列表或配置列表中引用本子请求。
	// 引用必须使用 <main request>/<subrequest> 格式
	// 必须是 DNS label
	// +required
	Name string

	// DeviceClassName 引用一个具体的 DeviceClass，该类可定义本请求继承的额外配置与选择器
	// 类必填，可用类取决于集群配置
	// 管理员可以只安装带有允许设备选择器的类，以限制用户可请求的设备。
	// 若允许用户无限制地请求任意资源，
	// 管理员可以创建一个空的 DeviceClass 供用户引用
	// +required
	DeviceClassName string

	// Selectors 定义某设备被本子请求考虑所必须满足的条件，设备必须满足所有选择器。
	// +optional
	// +listType=atomic
	Selectors []DeviceSelector

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求。支持的值包括：
	// - ExactCount：请求特定数量的设备，这是默认值，具体数量在 count 字段中给出。
	// - All：本子请求需要池中全部匹配的设备。若部分设备已被分配，分配会失败，
	// 除非请求了 adminAccess。
	// 未指定 AllocationMode 时默认为 ExactCount；若模式为 ExactCount 且未指定 count，
	// 则默认数量为 1。其他任何子请求都必须指定该字段。
	// 未来可能添加更多模式。客户端必须拒绝处理未知模式的请求。
	// +optional
	AllocationMode DeviceAllocationMode

	// Count 仅在模式为 "ExactCount" 时使用，必须大于零
	// 若 AllocationMode 为 ExactCount 且未指定本字段，默认为 1
	// +optional
	// +oneOf=AllocationMode
	Count int64

	// 若指定，此为请求的容忍（tolerations）。
	//
	// 要分配带有该效果污点的设备，必须具有对应的 NoSchedule 容忍。
	// NoExecute 同理。
	//
	// 此外，若任何已分配设备在分配后被加上 NoExecute 污点且未被容忍，
	// 则所有消费该 ResourceClaim 的 Pod 会被删除以驱逐它们。当 claim
	// 持有这些带污点设备时，调度器不会让新 Pod 预留该 claim。一旦所有
	// Pod 都被驱逐，该 claim 将被解除分配。
	//
	// tolerations 的最大数量为 16。
	//
	// 这是 beta 字段，需要开启 DRADeviceTaints 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	Tolerations []DeviceToleration

	// Capacity 定义针对每项容量的资源需求。
	// 若未设置该字段且设备支持多次分配，则根据 requestPolicy 对每项容量应用默认值。
	// 没有 requestPolicy 的容量，默认值为全部容量。
	// 该配置适用于每次设备分配。若 Count > 1，当没有足够设备满足要求时请求失败。
	// 若 AllocationMode 为 All，当存在其他方面匹配请求、具备该容量且值 >= 请求量、
	// 但无法分配给本请求的设备时，请求失败。
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements

	// DerivedAttributes 定义一组虚拟属性，它们通过 CEL 表达式针对每个候选
	// 设备计算得出。这些虚拟属性可以在 `.devices.constraints` 中被引用，用于
	// 对齐和匹配不同设备（例如在同一 NUMA 节点上共同分配 GPU 和网卡），即使
	// 它们的驱动发布了不同的属性。在求值 selector 表达式的 CEL 环境中，
	// 派生属性不通过 `device.attributes` 提供。
	//
	// 派生属性允许你在调度时提取、转换或规范化拓扑信息（例如从复杂拓扑字符串
	// 中提取 NUMA 索引，或重命名供应商特定属性）为统一的虚拟属性名。调度器在
	// 匹配约束时会像求值静态属性一样求值这些虚拟属性。
	//
	// 列表中定义的每个派生属性必须被 `.devices.constraints` 列表中至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性最多 32 个
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	DerivedAttributes []DeviceDerivedAttribute
}

// CapacityRequirements 定义特定设备请求的容量需求。
type CapacityRequirements struct {
	// Requests represent individual device resource requests for distinct resources,
	// all of which must be provided by the device.
	//
	// This value is used as an additional filtering condition against the available capacity on the device.
	// This is semantically equivalent to a CEL selector with
	// `device.capacity[<domain>].<name>.compareTo(quantity(<request quantity>)) >= 0`.
	// For example, device.capacity['test-driver.cdi.k8s.io'].counters.compareTo(quantity('2')) >= 0.
	//
	// When a requestPolicy is defined, the requested amount is adjusted upward
	// to the nearest valid value based on the policy.
	// If the requested amount cannot be adjusted to a valid value—because it exceeds what the requestPolicy allows—
	// the device is considered ineligible for allocation.
	//
	// For any capacity that is not explicitly requested:
	// - If no requestPolicy is set, the default consumed capacity is equal to the full device capacity
	//   (i.e., the whole device is claimed).
	// - If a requestPolicy is set, the default consumed capacity is determined according to that policy.
	//
	// If the device allows multiple allocation,
	// the aggregated amount across all requests must not exceed the capacity value.
	// The consumed capacity, which may be adjusted based on the requestPolicy if defined,
	// is recorded in the resource claim’s status.devices[*].consumedCapacity field.
	//
	// +optional
	Requests map[QualifiedName]resource.Quantity
}

const (
	DeviceSelectorsMaxSize             = 32
	FirstAvailableDeviceRequestMaxSize = 8
	DeviceTolerationsMaxLength         = 16
)

type DeviceAllocationMode string

// Valid [DeviceRequest.CountMode] values.
const (
	DeviceAllocationModeExactCount = DeviceAllocationMode("ExactCount")
	DeviceAllocationModeAll        = DeviceAllocationMode("All")
)

// DeviceSelector 必须且只能设置一个字段。
type DeviceSelector struct {
	// CEL 包含用于选择设备的 CEL 表达式。
	// +optional
	// +oneOf=SelectorType
	CEL *CELDeviceSelector
}

// CELDeviceSelector 包含用于选择设备的 CEL 表达式。
type CELDeviceSelector struct {
	// Expression is a CEL expression which evaluates a single device. It
	// must evaluate to true when the device under consideration satisfies
	// the desired criteria, and false when it does not. Any other result
	// is an error and causes allocation of devices to abort.
	//
	// The expression's input is an object named "device", which carries
	// the following properties:
	//  - driver (string): the name of the driver which defines this device.
	//  - attributes (map[string]object): the device's attributes, grouped by prefix
	//    (e.g. device.attributes["dra.example.com"] evaluates to an object with all
	//    of the attributes which were prefixed by "dra.example.com".
	//  - capacity (map[string]object): the device's capacities, grouped by prefix.
	//  - allowMultipleAllocations (bool): the allowMultipleAllocations property of the device
	//    (v1.34+ with the DRAConsumableCapacity feature enabled).
	//
	// Example: Consider a device with driver="dra.example.com", which exposes
	// two attributes named "model" and "ext.example.com/family" and which
	// exposes one capacity named "modules". This input to this expression
	// would have the following fields:
	//
	//     device.driver
	//     device.attributes["dra.example.com"].model
	//     device.attributes["ext.example.com"].family
	//     device.capacity["dra.example.com"].modules
	//
	// The device.driver field can be used to check for a specific driver,
	// either as a high-level precondition (i.e. you only want to consider
	// devices from this driver) or as part of a multi-clause expression
	// that is meant to consider devices from different drivers.
	//
	// The value type of each attribute is defined by the device
	// definition, and users who write these expressions must consult the
	// documentation for their specific drivers. The value type of each
	// capacity is Quantity.
	//
	// If an unknown prefix is used as a lookup in either device.attributes
	// or device.capacity, an empty map will be returned. Any reference to
	// an unknown field will cause an evaluation error and allocation to
	// abort.
	//
	// A robust expression should check for the existence of attributes
	// before referencing them.
	//
	// For ease of use, the cel.bind() function is enabled, and can be used
	// to simplify expressions that access multiple attributes with the
	// same domain. For example:
	//
	//     cel.bind(dra, device.attributes["dra.example.com"], dra.someBool && dra.anotherBool)
	//
	// When the DRAListTypeAttributes feature gate is enabled,
	// the includes() helper is available and it can work for both scalar
	// and list-type attributes. It was introduced to support smooth migration
	// from scalar attributes to list-type attributes while keeping
	// CEL expressions simple. For example:
	//
	//     device.attributes["dra.example.com"].models.includes("some-model")
	//
	// The length of the expression must be smaller or equal to 10 Ki. The
	// cost of evaluating it is also limited based on the estimated number
	// of logical steps.
	//
	// +required
	Expression string
}

// DeviceDerivedAttribute 定义通过 CEL 计算出的派生属性。
type DeviceDerivedAttribute struct {
	// Name is the identifier for this derived attribute, used in constraints.
	//
	// It must be a DNS subdomain followed by a slash ("/") followed by a C identifier
	// (e.g. "example.com/numaNode" or "derived/numaNode").
	//
	// If the chosen name matches an existing physical attribute from a driver,
	// the derived attribute's expression will shadow the physical attribute,
	// and its evaluated value will be used in constraints instead. When the goal
	// is to define a derived attribute that is only used within the ResourceClaim
	// and not meant to shadow an existing attribute, use a domain prefix that
	// no DRA driver should be using (e.g. "derived/myAttribute").
	//
	// It is not valid to define a derived attribute that isn't used in at least
	// one constraint.
	//
	// +required
	Name FullyQualifiedName

	// Expression is a CEL expression evaluated against each candidate device.
	// The expression must evaluate to a primitive scalar (string, integer,
	// boolean, or semver) or a list of these scalars ([]string, []int64,
	// []bool, []semver) to act as a virtual grouping key. Any other return type
	// is an error and causes CEL evaluation for the device to fail.
	//
	// The expression's input is an object named "device", which carries the
	// same properties as in a CELDeviceSelector.
	//
	// When pod scheduling encounters CEL runtime errors (such as looking
	// up an attribute that isn't defined) for some devices, it will abort
	// allocation and fail scheduling for the Pod. Surfacing evaluation
	// errors immediately prevents silent topology matching failures that are
	// extremely hard to detect. A robust expression should, for example, check
	// for the existence of attributes before referencing them to avoid
	// runtime evaluation errors.
	//
	// The expression gets evaluated after a device has passed the other
	// selector expressions for the request in which this expression is used.
	// This allows writing expressions that are tailored towards the specific
	// devices being requested (for example, by assuming the device is from a
	// certain vendor and skipping those checks).
	//
	// The length of the expression must be smaller or equal to 10 Ki. The
	// cost of evaluating it is also limited based on the estimated number
	// of logical steps; the combined cost of all derived attributes in a
	// claim is capped by a shared CEL cost budget.
	//
	// +required
	Expression string
}

// CELSelectorExpressionMaxCost 指定单次 CEL selector 求值的成本上限。
//
// 选择设备时没有总体预算，因此实际耗时与 CEL selector 的数量及其
// 求值频率成正比，后者会随若干因素变化（设备数量、集群利用率、
// 附加约束）。
//
// 针对此上限与 [CELSelectorExpressionMaxLength] 的校验只在首次设置
// 或更改表达式时进行。若未来 Kubernetes 版本更改了这些限制，可以
// 保证现有用户的已有表达式依然有效。
//
// 但 kube-scheduler 在运行时也会应用该成本上限，因此升级或降级后，
// 一个原本有效的表达式可能在运行时失败。
// 即使没有版本差异，若成本估算低估了实际成本也可能发生这种情况。
// 正是出于这个原因，
// kube-scheduler 强制执行运行时限制，而不只依赖校验。
//
// 根据
// https://github.com/kubernetes/kubernetes/blob/4aeaf1e99e82da8334c0d6dddd848a194cd44b4f/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go#L20-L22,
// 每次表达式求值大约需要 0.1 秒
// 但这取决于机器性能
const CELSelectorExpressionMaxCost = 1000000

// DeviceClaimDerivedAttributeCELMaxCost 是 claim 中所有派生属性表达式被
// 允许的总执行成本上限。
//
// 为控制在预算内，使用者应简化 CEL 表达式，
// 避免深层嵌套或迭代（如 `.all()` 或 `.exists()`）等高开销操作，
// 并尽量减少 claim 中派生属性的总数。
//
// 这一共享预算可防止过于复杂的 claim 拖累 kube-scheduler 的性能。
// 该上限设为 1,000,000 次指令执行（约 0.1 秒求值时间），从而将所有
// 派生属性的总成本绑定到单个 CEL selector 允许的最大成本。
const DeviceClaimDerivedAttributeCELMaxCost = 1000000

// CELSelectorExpressionMaxLength 是 CEL 选择器表达式字符串的最大长度。
const CELSelectorExpressionMaxLength = 10 * 1024

// 除 Requests 外，DeviceConstraint 必须且只能设置一个字段
type DeviceConstraint struct {
	// Requests 是此声明中必须共同满足此约束的一个或多个请求的列表。
	// 若某个请求由多个设备满足，则所有这些设备都必须满足该约束。
	// 若未指定，则此约束适用于此声明中的所有请求。
	//
	// 对子请求的引用必须包含主请求的名称，
	// 并可使用 <主请求>[/<子请求>] 的格式包含子请求。若只
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	Requests []string

	// MatchAttribute requires that all devices in question have this
	// attribute and that its type and value are the same across those
	// devices.
	//
	// For example, if you specified "dra.example.com/numa" (a hypothetical example!),
	// then only devices in the same NUMA node will be chosen. A device which
	// does not have that attribute will not be chosen. All devices should
	// use a value of the same type for this attribute because that is part of
	// its specification, but if one device doesn't, then it also will not be
	// chosen.
	//
	// When the DRAListTypeAttributes feature gate is enabled, comparison uses
	// set semantics(i.e., element order and duplicates are ignored): list-valued attributes
	// match when the intersection across all devices is non-empty.
	// Scalar values are treated as singleton sets for backward compatibility.
	//
	// Must include the domain qualifier.
	//
	// +optional
	// +oneOf=ConstraintType
	MatchAttribute *FullyQualifiedName

	// Potential future extension, not part of the current design:
	// A CEL expression which compares different devices and returns
	// true if they match.
	//
	// Because it would be part of a one-of, old schedulers will not
	// accidentally ignore this additional, for them unknown match
	// criteria.
	//
	// MatchExpression string

	// DistinctAttribute requires that all devices in question have this
	// attribute and that its type and value are unique across those devices.
	//
	// When the DRAListTypeAttributes feature gate is enabled, comparison uses
	// set semantics (i.e., element order and duplicates are ignored):
	// list-valued attributes must be pairwise disjoint across devices.
	// Scalar values are treated as singleton sets for backward compatibility.
	//
	// This acts as the inverse of MatchAttribute.
	//
	// This constraint is used to avoid allocating multiple requests to the same device
	// by ensuring attribute-level differentiation.
	//
	// This is useful for scenarios where resource requests must be fulfilled by separate physical devices.
	// For example, a container requests two network interfaces that must be allocated from two different physical NICs.
	//
	// +optional
	// +oneOf=ConstraintType
	// +featureGate=DRAConsumableCapacity
	DistinctAttribute *FullyQualifiedName
}

// DeviceClaimConfiguration 用于 DeviceClaim 中的配置参数。
type DeviceClaimConfiguration struct {
	// Requests 列出此配置适用到的请求名称。
	// 为空时适用于所有请求
	//
	// 对子请求的引用必须包含主请求的名称，
	// 并可使用 <主请求>[/<子请求>] 的格式包含子请求。若只
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	Requests []string

	DeviceConfiguration // inline
}

// DeviceConfiguration must have exactly one field set. It gets embedded
// inline in some other structs which have other fields, so field names must
// not conflict with those.
type DeviceConfiguration struct {
	// Opaque 提供驱动特有的配置参数。
	// +optional
	// +oneOf=ConfigurationType
	Opaque *OpaqueDeviceConfiguration
}

// OpaqueDeviceConfiguration contains configuration parameters for a driver
// in a format defined by the driver vendor.
type OpaqueDeviceConfiguration struct {
	// Driver 用于确定这些配置参数需要传给哪个 kubelet 插件。
	//
	// 驱动开发者提供的准入策略可以据此判断
	// 是否需要校验它们。
	//
	// 必须是 DNS 子域，并应以所属的 DNS 域结尾，
	// 驱动供应商。
	//
	// +required
	Driver string

	// Parameters 可包含任意数据。校验与版本管理由驱动开发者负责，
	// 通常包括自标识与版本号（Kubernetes 类型使用 "kind" + "apiVersion"），
	// 以及不同版本之间的转换
	// 原始数据长度不得超过 10 Ki
	// +required
	Parameters runtime.RawExtension
}

// OpaqueParametersMaxLength 是 [OpaqueDeviceConfiguration.Parameters] 字段中原始数据的最大长度。
const OpaqueParametersMaxLength = 10 * 1024

// 附着本 DeviceToleration 的 ResourceClaim 会容忍所有按 <key,value,effect> 三元组及
// 匹配使用操作符 <operator>。
type DeviceToleration struct {
	// Key 是 toleration 适用的 taint 键，为空表示匹配所有 taint 键。
	// 键为空时 operator 必须为 Exists，该组合表示匹配所有值和所有键。
	// 必须是 label 名。
	// +optional
	Key string

	// Operator 表示键与值的关系。合法运算符为 Exists 与 Equal，默认为 Equal。
	// Exists 等价于值的通配符，使 ResourceClaim 能容忍某一类别的所有污点。
	// +optional
	// +default="Equal"
	Operator DeviceTolerationOperator

	// Value 是容忍规则匹配的污点值
	// operator 为 Exists 时值必须为空，否则为普通字符串
	// 必须是 label 值
	// +optional
	Value string

	// Effect 表示要匹配的 taint effect，为空表示匹配所有 effect。
	// 指定时允许的值为 NoSchedule 和 NoExecute。
	// +optional
	Effect DeviceTaintEffect

	// TolerationSeconds 表示容忍规则容忍污点的时长（效果必须为 NoExecute，否则忽略该字段）。
	// 默认情况下
	// 未设置该字段，表示永久容忍（不驱逐）。零值和
	// 负值会被系统视为 0（立即驱逐）
	// 大于零时，Pod 需要被驱逐的时间计算为 <污点添加时间> + <容忍秒数>
	// +optional
	TolerationSeconds *int64
}

// TolerationOperator 是容忍中可以使用的操作符集合。
//
// +enum
type DeviceTolerationOperator string

const (
	DeviceTolerationOpExists DeviceTolerationOperator = "Exists"
	DeviceTolerationOpEqual  DeviceTolerationOperator = "Equal"
)

// ResourceClaimStatus tracks whether the resource has been allocated and what
// the result of that was.
type ResourceClaimStatus struct {
	// 声明成功分配后会设置 Allocation。
	// +optional
	Allocation *AllocationResult

	// ReservedFor 表示当前允许哪些实体使用该 claim。引用了未为本 Pod 预留的 ResourceClaim 的 Pod 不会启动。
	// 已被使用或因预留而可能正在使用的 claim 不得被解除分配
	// 在有多个调度器实例的集群中，两个 Pod 可能被不同调度器并发调度。
	// 若它们引用同一个已达到最大消费者数的 ResourceClaim，只有一个 Pod 能成功调度
	// 两个调度器都会尝试把各自的 Pod 加入 claim.status.reservedFor，
	// 但只有先到达 API server 的更新会被保存。
	// 另一个更新会失败，发起它的调度器据此知道
	// 必须把 Pod 放回队列，
	// 等待 ResourceClaim 再次可用
	// 此类预留最多 256 个，未来可能增加，但不会减少
	// +optional
	// +listType=map
	// +listMapKey=uid
	// +patchStrategy=merge
	// +patchMergeKey=uid
	ReservedFor []ResourceClaimConsumerReference

	// DeallocationRequested 自 Kubernetes 1.32 起被墓碑化，在该版本中
	// 该字段已被移除。待不再需要兼容 v1alpha3 解码时可复用。
	// DeallocationRequested bool

	// Devices 包含驱动上报的、为本声明分配的每个设备的状态。其中可能包含驱动特有的信息，
	// 各条目由相应驱动负责维护。
	// +optional
	// +listType=map
	// +listMapKey=driver
	// +listMapKey=device
	// +listMapKey=pool
	// +listMapKey=shareID
	// +featureGate=DRAResourceClaimDeviceStatus
	Devices []AllocatedDeviceStatus
}

// ResourceClaimReservedForMaxSize 是 claim.status.reservedFor 中条目的最大数量。
const ResourceClaimReservedForMaxSize = 256

// ResourceClaimConsumerReference contains enough information to let you
// locate the consumer of a ResourceClaim. The user must be a resource in the same
// namespace as the ResourceClaim.
type ResourceClaimConsumerReference struct {
	// APIGroup 是被引用资源所属的组；核心 API 下为空。
	// 它与创建资源时使用的 APIVersion 中的组名一致。
	// +optional
	APIGroup string
	// Resource 是被引用资源的类型，例如 "pods"
	// +required
	Resource string
	// Name 是被引用资源的名称
	// +required
	Name string
	// UID 唯一标识该资源的一个实例。
	// +required
	UID types.UID
}

// AllocationResult 包含已分配资源的属性。
type AllocationResult struct {
	// Devices 是设备分配的结果。
	// +optional
	Devices DeviceAllocationResult

	// NodeSelector 定义已分配资源在哪些节点可用。未设置时表示所有节点均可用
	// +optional
	NodeSelector *core.NodeSelector

	// Controller 自 Kubernetes 1.32 起被墓碑化，在该版本中
	// 该字段已被移除。待不再需要兼容 v1alpha3 解码时可复用。
	// Controller string

	// AllocationTimestamp 存储资源被分配的时间。
	// 该字段不保证一定被设置；未设置时表示时间未知。
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	AllocationTimestamp *metav1.Time
}

// DeviceAllocationResult 是设备分配的结果。
type DeviceAllocationResult struct {
	// Results 列出所有已分配设备
	// +optional
	// +listType=atomic
	Results []DeviceRequestAllocationResult

	// 该字段是 claim 和 class 的所有配置参数的组合，驱动可通过标志区分两者来源。
	// 其中包含结果中没有已分配设备的驱动的配置参数，因为支持哪些配置参数由驱动决定，
	// 驱动可以静默忽略未知的配置参数。
	// +optional
	// +listType=atomic
	Config []DeviceAllocationConfiguration
}

// AllocationResultsMaxSize 是分配结果中设备的最大数量。
const AllocationResultsMaxSize = 32

// DeviceRequestAllocationResult 包含一个请求的设备分配结果
type DeviceRequestAllocationResult struct {
	// Request 是 claim 中导致该设备被分配的请求名。若引用 DeviceRequest 的 firstAvailable
	// 列表中的子请求，本字段必须同时包含主请求名和子请求，格式为 <main request>/<subrequest>。
	// 每个请求可能分配了多个设备。
	// +required
	Request string

	// Driver 指定当 claim 在节点上需要时，应调用哪个 DRA 驱动的
	// kubelet 插件来处理分配。
	//
	// 必须是 DNS 子域，并应以所属的 DNS 域结尾，
	// 驱动供应商。
	//
	// +required
	Driver string

	// This name together with the driver name and the device name field
	// identify which device was allocated (`<driver name>/<pool name>/<device name>`).
	//
	// Must not be longer than 253 characters and may contain one or more
	// DNS sub-domains separated by slashes.
	//
	// +required
	Pool string

	// Device 通过设备在驱动资源池中的名称引用某一个设备实例，必须是 DNS 标签。
	// +required
	Device string

	// AdminAccess 表示该设备是为管理员访问而分配的，定义见对应的请求字段
	// 未设置或为 false 时禁用管理员访问，否则启用
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool

	// 此为设备被分配时请求中指定的所有容忍的副本。
	//
	// tolerations 的最大数量为 16。
	//
	// 这是 beta 字段，需要开启 DRADeviceTaints 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	Tolerations []DeviceToleration

	// BindingConditions 包含设备 BindingConditions 的副本。
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindingConditions []string

	// BindingFailureConditions 包含设备 BindingFailureConditions 的副本。
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// 这是 beta 字段，需要开启 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性。
	// 特性开关。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindingFailureConditions []string

	// ShareID 唯一标识设备的某一次独立分配份额，用于设备支持并发多次分配的场景。
	// 它作为附加的 map 键，用于区分同一设备的不同并发份额。
	// +optional
	// +featureGate=DRAConsumableCapacity
	ShareID *types.UID

	// ConsumedCapacity 跟踪作为 claim 请求的一部分每台设备消费的容量。
	// 消费量可能不同于请求量：如适用，会按设备的 requestPolicy
	// 向上取整到最近的合法值（即不会小于请求量）。
	//
	// 每台设备的消费容量总和不得超过 DeviceCapacity 的 Value
	//
	// 此字段仅对允许多次分配的设备填充。
	// 所有容量条目都会包含在内，即使消费量为零。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	ConsumedCapacity map[QualifiedName]resource.Quantity

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，本已分配设备将跳过的节点本地资源操作
	// （gRPC 调用）。如果某驱动在声明中的所有已分配设备都跳过某操作，该 gRPC 调用就会被跳过。
	// 它是分配时 ResourceSlice.spec.skipNodeOperations 的副本。
	// +optional
	// +listType=set
	// +featureGate=DRAOptionalNodeOperations
	SkipNodeOperations []SkipNodeOperation
}

// DeviceAllocationConfiguration 被嵌入 AllocationResult
type DeviceAllocationConfiguration struct {
	// Source 记录配置是来自类（因此普通用户无法设置）还是来自声明。
	// +required
	Source AllocationConfigSource

	// Requests 列出此配置适用到的请求名称。
	// 为空时适用于所有请求
	//
	// 对子请求的引用必须包含主请求的名称，
	// 并可使用 <主请求>[/<子请求>] 的格式包含子请求。若只
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	Requests []string

	DeviceConfiguration // inline
}

type AllocationConfigSource string

// Valid [DeviceAllocationConfiguration.Source] values.
const (
	AllocationConfigSourceClass AllocationConfigSource = "FromClass"
	AllocationConfigSourceClaim AllocationConfigSource = "FromClaim"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaimList 是 claim 的集合
type ResourceClaimList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items 是资源声明的列表。
	Items []ResourceClaim
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeviceClass 是由供应商或管理员提供的资源，包含设备配置和选择器。
// 它可以在 claim 的设备请求中被引用以应用这些预设。集群级作用域。
//
// 这是 alpha 类型，需要开启 DynamicResourceAllocation 特性开关。
type DeviceClass struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta

	// Spec 定义可以分配什么以及如何配置。
	//
	// 这是可变的。使用者必须准备好应对类随时变化，无论是因为更新还是替换。
	// claim 的分配只根据分配时类中的设置进行一次。
	//
	// 修改 spec 会自动递增 metadata.generation。
	Spec DeviceClassSpec
}

// DeviceClassSpec is used in a [DeviceClass] to define what can be allocated
// and how to configure it.
type DeviceClassSpec struct {
	// Each selector must be satisfied by a device which is claimed via this class.
	//
	// +optional
	// +listType=atomic
	Selectors []DeviceSelector

	// Config 定义适用于通过该类的每个设备的配置参数。
	// 某些类可能由多个驱动满足，因此每个供应商配置实例只适用于一个驱动。
	// 这些参数会传给驱动，但在分配 claim 时不会被考虑。
	// +optional
	// +listType=atomic
	Config []DeviceClassConfiguration

	// SuitableNodes 自 Kubernetes 1.32 起被墓碑化，在该版本中
	// 该字段已被移除。待不再需要兼容 v1alpha3 解码时可复用。
	// SuitableNodes *core.NodeSelector

	// ExtendedResourceName 是本类设备的扩展资源名称
	// 本类设备可用于满足 Pod 的扩展资源请求
	// 格式与 Pod 扩展资源名相同
	// 在集群所有设备类中应保持唯一
	// 若两个设备类同名，则后创建的类被用于满足 Pod 的扩展资源请求
	// 若两个类同时创建，则选择名称字典序较小的类
	// +optional
	// +featureGate=DRAExtendedResource
	ExtendedResourceName *string
}

// DeviceClassConfiguration 用于 DeviceClass
type DeviceClassConfiguration struct {
	DeviceConfiguration // inline
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeviceClassList 是设备类的集合
type DeviceClassList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items is the list of resource classes.
	Items []DeviceClass
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaimTemplate 用于生成 ResourceClaim 对象
//
// 这是 alpha 类型，需要开启 DynamicResourceAllocation 特性开关。
type ResourceClaimTemplate struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta

	// 描述将要生成的 ResourceClaim。
	Spec ResourceClaimTemplateSpec
}

// ResourceClaimTemplateSpec 包含 ResourceClaim 的元数据与字段
type ResourceClaimTemplateSpec struct {
	// ObjectMeta 可以包含将在创建 ResourceClaim 时复制到其中的标签和注解。
	// 不允许其他字段，并且会在校验时被拒绝。
	// +optional
	metav1.ObjectMeta

	// ResourceClaim 的 Spec，全部内容原样复制到由本模板创建的 ResourceClaim 中。
	// ResourceClaim 中有效的字段在这里同样有效。
	Spec ResourceClaimSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceClaimTemplateList 是 claim 模板的集合
type ResourceClaimTemplateList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items is the list of resource claim templates.
	Items []ResourceClaimTemplate
}

const (
	// AllocatedDeviceStatusMaxConditions 表示设备状态中条件的最大数量。
	AllocatedDeviceStatusMaxConditions int = 8
	// AllocatedDeviceStatusDataMaxLength 表示设备状态中 Data 字段原始数据的最大长度
	AllocatedDeviceStatusDataMaxLength int = 10 * 1024
	// NetworkDeviceDataMaxIPs represents the maximum number of IPs in the networkData
	// field in a device status.
	NetworkDeviceDataMaxIPs int = 16
	// NetworkDeviceDataInterfaceNameMaxLength 表示设备状态中 networkData.interfaceName
	// 字段的最大字符数。
	NetworkDeviceDataInterfaceNameMaxLength int = 256
	// NetworkDeviceDataHardwareAddressMaxLength 表示设备状态中 networkData.hardwareAddress 字段的最大字符数。
	NetworkDeviceDataHardwareAddressMaxLength int = 128
)

// AllocatedDeviceStatus contains the status of an allocated device, if the
// driver chooses to report it. This may include driver-specific information.
//
// The combination of Driver, Pool, Device, and ShareID must match the corresponding key
// in Status.Allocation.Devices.
type AllocatedDeviceStatus struct {
	// Driver 指定当 claim 在节点上需要时，应调用哪个 DRA 驱动的
	// kubelet 插件来处理分配。
	//
	// 必须是 DNS 子域，并应以所属的 DNS 域结尾，
	// 驱动供应商。
	//
	// +required
	Driver string

	// 该名称与驱动名和设备名字段共同标识分配的是哪个设备
	// （`<driver name>/<pool name>/<device name>`）。
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域。
	// +required
	Pool string

	// Device 通过设备在驱动资源池中的名称引用某一个设备实例，必须是 DNS 标签。
	// +required
	Device string

	// ShareID 唯一标识设备的一次独立分配份额。
	// +optional
	// +featureGate=DRAConsumableCapacity
	ShareID *string

	// Conditions 包含对设备状态的最新观测。如果设备已按类与声明的配置引用完成配置，
	// 则 `Ready` 条件应为 True。
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition

	// Data 包含任意驱动专有数据
	// 原始数据长度不得超过 10 Ki
	// +optional
	Data *runtime.RawExtension

	// NetworkData 包含与设备相关的网络信息。
	// +optional
	NetworkData *NetworkDeviceData
}

// NetworkDeviceData provides network-related details for the allocated device.
// This information may be filled by drivers or other components to configure
// or identify the device within a network context.
type NetworkDeviceData struct {
	// InterfaceName 指定与已分配设备关联的网络接口名称。
	// 它可能是正在 Pod 中配置的物理或虚拟网络接口的名称。
	// +optional
	InterfaceName string

	// IPs lists the network addresses assigned to the device's network interface.
	// This can include both IPv4 and IPv6 addresses.
	// The IPs are in the CIDR notation, which includes both the address and the
	// associated subnet mask.
	// e.g.: "192.0.2.5/24" for IPv4 and "2001:db8::5/64" for IPv6.
	//
	// Must not contain more than 16 entries.
	//
	// +optional
	// +listType=atomic
	IPs []string

	// HardwareAddress represents the hardware address (e.g. MAC Address) of the device's network interface.
	//
	// Must not be longer than 128 characters.
	//
	// +optional
	HardwareAddress string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeviceTaintRule adds one taint to all devices which match the selector.
// This has the same effect as if the taint was specified directly
// in the ResourceSlice by the DRA driver.
type DeviceTaintRule struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +optional
	metav1.ObjectMeta

	// Spec 定义选择器和一个污点。
	//
	// 修改 spec 会自动递增 metadata.generation。
	Spec DeviceTaintRuleSpec

	// Status 提供 spec 中所请求内容的执行状态信息
	// +optional
	Status DeviceTaintRuleStatus
}

// DeviceTaintRuleSpec 指定选择器和一个污点
type DeviceTaintRuleSpec struct {
	// DeviceSelector 定义污点施加到哪些设备上
	// 设备必须满足所有选择条件才能
	// 匹配；空选择器匹配所有设备。没有选择器时不匹配任何设备
	// +optional
	DeviceSelector *DeviceTaintSelector

	// 应用到匹配设备上的 taint。
	// +required
	Taint DeviceTaint
}

// DeviceTaintSelector defines which device(s) a DeviceTaintRule applies to.
// The empty selector matches all devices. Without a selector, no devices
// are matched.
type DeviceTaintSelector struct {
	// 若设置 driver，只选择该驱动的设备
	// 本字段对应 slice.spec.driver
	// +optional
	Driver *string

	// 若设置 pool，只选择该池中的设备
	// 同时设置驱动名有助于避免不同驱动使用同一池名时的歧义，
	// 但并非必须，因为跨驱动选择池也可能有用，
	// 例如节点本地设备的驱动常以节点名作为池名
	// +optional
	Pool *string

	// 若设置 device，只选择具有该名称的设备
	// 本字段对应 slice.spec.devices[].name
	// 可能还需要同时设置 driver 和 pool 以避免歧义，但非必须
	// +optional
	Device *string
}

// DeviceTaintRuleStatus 提供正在进行的 Pod 驱逐的相关信息。
type DeviceTaintRuleStatus struct {
	// Conditions provide information about the state of the DeviceTaintRule
	// and the cluster at some point in time,
	// in a machine-readable and human-readable format.
	//
	// The following condition is currently defined as part of this API, more may
	// get added:
	// - Type: EvictionInProgress
	// - Status: True if there are currently pods which need to be evicted, False otherwise
	//   (includes the effects which don't cause eviction).
	// - Reason: not specified, may change
	// - Message: includes information about number of pending pods and already evicted pods
	//   in a human-readable format, updated periodically, may change
	//
	// For `effect: None`, the condition above gets set once for each change to
	// the spec, with the message containing information about what would happen
	// if the effect was `NoExecute`. This feedback can be used to decide whether
	// changing the effect to `NoExecute` will work as intended. It only gets
	// set once to avoid having to constantly update the status.
	//
	// Must have 8 or less entries.
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	Conditions []metav1.Condition
}

// DeviceTaintRuleStatusMaxConditions 是 DeviceTaintRuleStatus 中 condition 的最大数量
const DeviceTaintRuleStatusMaxConditions = 8

// DeviceTaintConditionEvictionInProgress 是 DeviceTaintRuleStatus 对外公开的 condition 类型
const DeviceTaintConditionEvictionInProgress = "EvictionInProgress"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeviceTaintRuleList 是 DeviceTaintRule 的集合。
type DeviceTaintRuleList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items is the list of DeviceTaintRules.
	Items []DeviceTaintRule
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourcePoolStatusRequest triggers a one-time calculation of resource pool status
// based on the provided filters. Once status is set, the request is considered complete and will not be reprocessed.
// Users should delete and recreate requests to get updated information.
type ResourcePoolStatusRequest struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +required
	metav1.ObjectMeta

	// Spec defines the filters for which pools to include in the status.
	// +required
	Spec ResourcePoolStatusRequestSpec

	// Status is populated by the controller with the calculated pool status.
	// +optional
	Status *ResourcePoolStatusRequestStatus
}

// ResourcePoolStatusRequestSpec 定义池状态请求的过滤条件。
type ResourcePoolStatusRequestSpec struct {
	// Driver specifies the DRA driver name to filter pools.
	// Only pools from ResourceSlices with this driver will be included.
	// This field is required to bound the scope of the request.
	Driver string

	// PoolName optionally filters to a specific pool name.
	// If not specified, all pools from the specified driver are included.
	// When specified, must be a valid resource pool name (DNS subdomains separated by "/").
	// +optional
	PoolName *string

	// Limit optionally specifies the maximum number of pools to return in the status.
	// If more pools match the filter criteria, the response will be truncated.
	//
	// Default: 100
	// Maximum: 1000
	//
	// +optional
	Limit *int32

	// DefaultPartitionTypeAttribute optionally names a device attribute (by its
	// fully qualified name) to use as the default grouping attribute for
	// partitionable devices whose slice has not declared one themselves. A
	// slice's own PartitionTypeAttribute always takes precedence. When neither
	// the slice nor this default names an attribute, a partitionable pool
	// reports no partitionSummary.
	// +optional
	DefaultPartitionTypeAttribute *string
}

// ResourcePoolStatusRequestLimitDefault 是 spec.limit 的默认值。
const ResourcePoolStatusRequestLimitDefault int32 = 100

// ResourcePoolStatusRequestLimitMax 是 spec.limit 允许的最大值。
const ResourcePoolStatusRequestLimitMax int32 = 1000

// ResourcePoolStatusRequestStatus 包含计算得到的池状态信息。
type ResourcePoolStatusRequestStatus struct {
	// PoolCount is the total number of pools that matched the filter criteria,
	// regardless of truncation.
	// +optional
	PoolCount *int32

	// Pools contains the first `spec.limit` matching pools, sorted by driver
	// then pool name. If len(pools) < poolCount, the list was truncated.
	// +optional
	Pools []PoolStatus

	// Conditions provide information about the state of the request.
	// A condition with type=Complete or type=Failed will always be set
	// when the status is populated.
	// +optional
	Conditions []metav1.Condition
}

// PoolStatus 包含单个资源池的状态信息。
type PoolStatus struct {
	// Driver is the DRA driver name for this pool.
	Driver string

	// PoolName is the name of the pool.
	PoolName string

	// Generation is the pool generation observed across all ResourceSlices
	// in this pool.
	Generation int64

	// ResourceSliceCount is the number of ResourceSlices that make up this pool.
	// +optional
	ResourceSliceCount *int32

	// TotalDevices is the total number of devices in the pool across all slices.
	// +optional
	TotalDevices *int32

	// AllocatedDevices is the number of devices currently allocated to claims.
	// +optional
	AllocatedDevices *int32

	// AvailableDevices is the number of devices available for allocation.
	// This equals TotalDevices - AllocatedDevices - UnavailableDevices.
	// +optional
	AvailableDevices *int32

	// UnavailableDevices is the number of devices that are not available
	// due to taints or other conditions, but are not allocated.
	// +optional
	UnavailableDevices *int32

	// NodeName is the node this pool is associated with.
	// When omitted, the pool is not associated with a specific node.
	// +optional
	NodeName *string

	// ValidationError is set when the pool's data could not be fully validated.
	// When set, device count fields and ResourceSliceCount may be unset.
	// +optional
	ValidationError *string

	// PartitionSummary reports allocatability per (attribute, partition type)
	// for a partitionable pool. Each entry names the grouping attribute it was
	// resolved from: the one declared by a device's own slice, or for devices
	// whose slice declares none, the default named in the request.
	// +optional
	PartitionSummary []PartitionTypeStatus

	// ShareableSummary reports aggregate capacity for a pool that contains
	// devices with AllowMultipleAllocations.
	// +optional
	ShareableSummary *ShareableSummaryStatus
}

// PartitionTypeStatus reports allocatability for a single partition type,
// identified by the value of a grouping attribute.
type PartitionTypeStatus struct {
	// Attribute is the fully qualified name of the device attribute whose value
	// groups this entry. It is the PartitionTypeAttribute declared by the
	// devices' own slice, or the default named in the request when their slice
	// declares none.
	// +required
	Attribute string

	// Type is the partition type value (e.g. "Full" or "Half").
	// +required
	Type string

	// Total is the number of devices of this partition type in the pool.
	// +required
	Total *int32

	// Allocatable is the number of additional devices of this partition type
	// that could still be allocated given current shared-counter consumption.
	// +required
	Allocatable *int32
}

// ShareableSummaryStatus reports aggregate capacity for a pool that contains
// devices with AllowMultipleAllocations.
type ShareableSummaryStatus struct {
	// FullyAvailableDevices is the number of shareable devices with no
	// capacity consumed.
	// +required
	FullyAvailableDevices *int32

	// PartiallyAvailableDevices is the number of shareable devices with some
	// but not all capacity consumed.
	// +required
	PartiallyAvailableDevices *int32

	// Capacity reports aggregate total, consumed, and available amounts per
	// shareable capacity key across the pool.
	// +optional
	Capacity []ShareableCapacityStatus
}

// ShareableCapacityStatus reports aggregate amounts for a single shareable
// capacity key.
type ShareableCapacityStatus struct {
	// Name is the capacity name.
	// +required
	Name string

	// Total is the sum of this capacity across shareable devices in the pool.
	// +required
	Total *resource.Quantity

	// Consumed is the amount drawn by current allocations.
	// +required
	Consumed *resource.Quantity

	// Available is Total minus Consumed, never negative.
	// +required
	Available *resource.Quantity
}

// ResourcePoolStatusRequestConditionComplete 是已完成请求的条件类型。
const ResourcePoolStatusRequestConditionComplete = "Complete"

// ResourcePoolStatusRequestConditionFailed 是失败请求的条件类型。
const ResourcePoolStatusRequestConditionFailed = "Failed"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourcePoolStatusRequestList 是 ResourcePoolStatusRequest 的集合。
type ResourcePoolStatusRequestList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// +optional
	metav1.ListMeta

	// Items 是 ResourcePoolStatusRequest 列表。
	Items []ResourcePoolStatusRequest
}
