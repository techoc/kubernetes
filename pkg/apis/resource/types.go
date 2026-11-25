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
	// Finalizer 是针对通过内置控制器完成分配的 claim 所设置的 finalizer。
	// 该名称保留给 Kubernetes 使用，DRA 驱动控制器必须使用自己的 finalizer。
	Finalizer = "resource.kubernetes.io/delete-protection"
	// ExtendedResourceClaimAnnotation 是加在生成的特殊 ResourceClaim 上的注解。
	// 其唯一合法值为 "true"，且仅在调度器内部使用。
	ExtendedResourceClaimAnnotation = "resource.kubernetes.io/extended-resource-claim"
	// PodResourceClaimAnnotation 是 ResourceClaim 控制器打在由模板生成的
	// ResourceClaim 上的注解。其值为生成该 claim 所对应的 Pod 或 PodGroup 的
	// spec.resourceClaims[].name。Pod 与其 PodGroup 保证使用相同名称。
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

	// PartitionTypeAttribute 指定一个字符串设备属性（以完全限定名，
	// 例如 "gpu.example.com/profile"），其值用于标记每个设备的分区类型，
	// 例如 MIG 风格 GPU 的 "Full" 或 "Half"。
	//
	// 设置该字段后，分片中每个可分区设备都必须携带该属性，
	// 且共享同一属性值的设备必须具有相同的 ConsumesCounters 消耗量。
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

// CounterSet 定义一组具名计数器，可供 ResourcePool 中定义的设备使用。
//
// 计数器本身不可被单独分配，但可以被设备引用。当某个设备被分配后，
// 它使用的那部分计数器将不再可供其他设备使用。
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

	// Generation 跟踪池随时间的变化。每当驱动改变池中一个或多个资源的
	// 任何内容时，都必须修改该池所有 ResourceSlice 中的 generation。
	// ResourceSlice 的使用者应当只考虑 generation 编号最高的池中的资源。
	// 驱动可以重置 generation，这对使用者应当没有影响，前提是池中的
	// 所有 ResourceSlice 都被更新为匹配或被删除。
	//
	// 与 ResourceSliceCount 结合后，此机制使使用者能够检测由多个
	// ResourceSlice 组成、尚处于不完整状态的池。
	//
	// +required
	Generation int64

	// ResourceSliceCount 是该 generation 编号下池中 ResourceSlice 的总数，
	// 必须大于零。
	//
	// 使用者可以用它检查自己是否已看到属于同一池的所有 ResourceSlice。
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

// Device 表示一个可以基于其属性进行选择的独立硬件实例。
// 除名称外，必须且只能设置一个字段。
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

	// AllowMultipleAllocations 标记该设备是否允许被分配给多个 DeviceRequests。
	//
	// 若将 AllowMultipleAllocations 设为 true，该设备可以被分配多次，
	// 并且其全部容量均可被消费，无论是否定义了 requestPolicy。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	AllowMultipleAllocations *bool

	// NodeAllocatableResources 定义由此设备暴露的 DRA 驱动所管理的节点资源映射。
	// 这包括当前在 v1.Node `status.allocatable` 中报告、且不属于扩展资源的资源
	// （参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources）。
	// 例如 "cpu"、"memory"、"ephemeral-storage" 和大页（hugepages）。
	// 除了通过 Pod `spec` 发起的标准请求外，这些资源也可以通过 claim 请求
	// 并由 DRA 驱动分配。
	// 例如，CPU DRA 驱动可能分配独占式 CPU，或加速器设备的辅助节点内存依赖。
	// 该映射的键是节点可分配资源名（例如 "cpu"、"memory"）。
	// 不允许使用扩展资源名作为键。
	// +optional
	// +featureGate=DRANodeAllocatableResources
	NodeAllocatableResources map[v1.ResourceName]NodeAllocatableResource
}

// NodeAllocatableResource 定义如何将所请求的 DRA 设备/容量单位
// 换算为对应数量的节点可分配资源。
// Mapping 与 Overhead 中至少要指定一个。
type NodeAllocatableResource struct {
	// Mapping 用于设备直接建模标准 CPU 或内存等节点可分配资源的场景
	// （例如 CPU DRA 驱动）。计算出的数量在节点上每个 claim 实例只计入一次。
	// 为避免节点 cgroup 隔离方面的冲突，调度器会显式禁止跨多个 Pod
	// 共享带映射的设备 claim。
	// +optional
	Mapping *NodeAllocatableMapping

	// Overhead 包含用于建模辅助开销的字段：当分配的设备本身并不建模节点
	// 可分配资源时，会在节点可分配资源上产生该开销（例如 GPU 的主机内存开销）。
	// 允许跨多个 Pod 共享带开销映射的 claim。节点可分配开销会为每个
	// 引用该 claim 的 Pod 分别计入。
	// 即使为同一资源指定了 mapping，Overhead 也总是从节点该资源的可分配容量中扣除。
	// 例如：若设备通过 Mapping（带 CapacityKey）将每个 socket 的内存容量建模为
	// 可消费容量池，则为同一资源指定的任何开销都将从节点的一般可分配容量中扣除，
	// 而不是从 Mapping 中的每 socket 容量池中扣除。
	// +optional
	Overhead *NodeAllocatableOverhead
}

// NodeAllocatableMapping 定义 DRA 分配如何直接换算为节点可分配资源数量。
// 映射可由已分配设备数（通过 deviceMultiplier）或具体消耗的容量
// （通过 capacityKey 和 capacityMultiplier）推导，二者互斥。
// Kubelet 会将该从 claim 映射出的资源数量同时加入 Pod 级 cgroup 的
// requests 和 limits，并对每个引用该 claim 的容器将其加入容器级 cgroup 的 limits。
type NodeAllocatableMapping struct {
	// CapacityKey 引用在 `spec.devices[*].capacity` 映射中定义为键的容量名称。
	// 设置该字段后，对于特定 claim 分配，`status.allocation.devices.results[*].consumedCapacity`
	// 映射中与该键关联的值将决定节点可分配资源的基准数量。
	// `capacityMultiplier` 也必须设置，并与基准数量相乘。
	// 例如，若 `spec.devices[*].capacity` 中有条目 "dra.example.com/memory": "128Gi"，
	// 且该字段设为 "dra.example.com/memory"，则消耗
	// { "dra.example.com/memory": "4Gi" } 的 claim 分配，其节点可分配资源映射的
	// 基准数量为 "4Gi"。
	// 最终的节点可分配资源数量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// +optional
	CapacityKey *QualifiedName

	// CapacityMultiplier 用作已分配且被消耗容量的乘数，仅在设置了 `capacityKey` 时有效。
	// 最终节点可分配资源量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// 例如，若消耗了某设备的容量 "dra.example.com/cores"，且每个 "core" 提供 2 个 "cpu"，
	// 映射为：{ResourceName: "cpu", capacityKey: "dra.example.com/cores", capacityMultiplier: "2"}。
	// 若声明消耗 8 个 "dra.example.com/cores"，则 CPU 占用为 8 * 2 = 16。
	// +optional
	CapacityMultiplier *resource.Quantity

	// DeviceMultiplier 用作 claim 中已分配设备数量的乘数。
	// 最终的节点可分配资源数量为 `deviceCount` * `deviceMultiplier`。
	// 例如，将每个缓存复合体（CCX）表示为一个设备的 DRA 驱动，
	// 其 `nodeAllocatableResources` 中会包含 {ResourceName: "cpu", deviceMultiplier: "8"}。
	// 若为该 claim 分配了 2 个设备（CCX），则视为分配了 2 * 8 = 16 个 CPU。
	// 仅当 `capacityKey` 和 `capacityMultiplier` 未设置时才有效。
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

	// PerContainer 按每个引用该 claim 的容器施加。
	// 这建模的是与正在使用该设备的容器数量呈线性关系的开销。
	// 当同时指定 PerPod 和 PerContainer 时，每个引用该 claim 的 Pod
	// 分配的总开销按如下方式计算：
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet 在 cgroups 中计入此开销：
	// - Pod 级 cgroup（requests 和 limits）：Kubelet 加入 PerPod + (PerContainer * NumReferences)。
	// - 容器级 cgroup（仅 limits）：Kubelet 为每个引用该 claim 的容器加入 PerPod + PerContainer。
	// 这样任意单个容器都可以访问 Pod 级的开销，同时父 cgroup 将总用量封顶，
	// 使 PerPod 恰好只计入一次。
	// +optional
	PerContainer *resource.Quantity
}

// DeviceCounterConsumption 定义设备将从某个 CounterSet 中消费的计数器集合。
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

// Counter 描述与设备关联的某个数量。
type Counter struct {
	// Value 定义某项设备计数器可用的数量。
	// +required
	Value resource.Quantity
}

// CapacityRequestPolicyDiscreteMaxOptions 限定 requestPolicy 中允许的离散容量值数量上限。
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

// FullyQualifiedName 是指已设置域的 QualifiedName。
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

	// IntValue 是数字。
	//
	// +optional
	// +oneOf=ValueType
	IntValue *int64

	// BoolValue 是布尔值（true/false）。
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

	// BoolValues 是一组非空的布尔值（true/false）列表。
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

// 挂载该污点的设备，会对所有不容忍该污点的 claim 产生相应的 "effect"
// 影响，并通过这些 claim 波及使用它们的 Pod。
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
	// 这样才能在单个版本内新增枚举值，而无需依赖 ratcheting。

	// TimeAdded 表示添加该污点的时间，或（仅在 DeviceTaintRule 中）
	// 修改 effect 的时间。若未设置，会在创建或更新时自动添加。
	// 此外，对于 DeviceTaintRule，更新时若提供的值与旧值相同而新
	// effect 不同，则该值会被替换为当前时间。
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

	// Items 是 ResourceSlice 的列表。
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

// DeviceRequest 是对 claim 所需设备的请求。
// 这通常是对单个资源（如某个设备）的请求，但也可以请求多个相同设备。
// 借助 FirstAvailable，还可以提供按优先级排列的请求列表。
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

// DeviceSubRequest 描述在 claim.spec.devices.requests[].firstAvailable
// 数组中提供的设备请求。每一项通常是对单个资源（如某个设备）的请求，
// 但也可以请求多个相同设备。
//
// DeviceSubRequest 与 ExactDeviceRequest 类似，但不暴露 AdminAccess 字段，
// 因为该字段仅在请求特定设备时受支持。
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
	// Requests 表示针对不同资源的独立设备资源请求，设备必须满足全部请求。
	//
	// 该值用作针对设备可用容量的附加过滤条件。
	// 其语义等价于如下 CEL selector：
	// `device.capacity[<domain>].<name>.compareTo(quantity(<request quantity>)) >= 0`。
	// 例如，device.capacity['test-driver.cdi.k8s.io'].counters.compareTo(quantity('2')) >= 0。
	//
	// 若定义了 requestPolicy，请求量会依据该策略向上调整至最近的合法值。
	// 如果请求量无法被调整为合法值——因为其超出了 requestPolicy 允许的范围——
	// 则该设备被视为不可分配。
	//
	// 对于任何未被显式请求的容量：
	// - 若未设置 requestPolicy，默认消费的容量等于设备的全部容量
	//   （即整台设备被占用）。
	// - 若设置了 requestPolicy，默认消费的容量由该策略决定。
	//
	// 若设备允许多次分配，所有请求的累计消耗量不得超过该容量值。
	// 实际消费的容量（若定义了 requestPolicy，可能已被调整）会记录在
	// resource claim 的 status.devices[*].consumedCapacity 字段中。
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

// [DeviceRequest.CountMode] 的合法取值。
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
	// Expression 是一个对单个设备求值的 CEL 表达式。当所考虑的设备
	// 满足期望条件时它必须求值为 true，不满足时必须为 false。
	// 任何其他结果都视为错误，并导致设备分配中止。
	//
	// 表达式的输入是一个名为 "device" 的对象，其携带以下属性：
	//  - driver (string): 定义该设备的驱动名称。
	//  - attributes (map[string]object): 设备的属性，按前缀分组
	//    （例如 device.attributes["dra.example.com"] 求值为由所有以
	//    "dra.example.com" 为前缀的属性组成的对象。
	//  - capacity (map[string]object): 设备的容量，按前缀分组。
	//  - allowMultipleAllocations (bool): 设备的 allowMultipleAllocations 属性
	//    （v1.34+，且需启用 DRAConsumableCapacity 特性）。
	//
	// 示例：考虑一个 driver="dra.example.com" 的设备，它暴露两个名为
	// "model" 和 "ext.example.com/family" 的属性，以及一个名为 "modules"
	// 的容量。该表达式的输入将包含以下字段：
	//
	//     device.driver
	//     device.attributes["dra.example.com"].model
	//     device.attributes["ext.example.com"].family
	//     device.capacity["dra.example.com"].modules
	//
	// device.driver 字段可用于检查特定驱动，既可作为高层前置条件
	// （即只考虑该驱动的设备），也可作为考虑不同驱动设备的多子句
	// 表达式的一部分。
	//
	// 每个属性的值类型由设备定义决定，编写这些表达式的用户必须查阅
	// 其特定驱动的文档。每个容量的值类型为 Quantity。
	//
	// 如果在 device.attributes 或 device.capacity 中使用未知前缀进行查找，
	// 将返回空映射。对未知字段的任何引用都会导致求值错误并使分配中止。
	//
	// 健壮的表达式应当在引用属性之前先检查其是否存在。
	//
	// 为方便使用，已启用 cel.bind() 函数，可用于简化访问同一域下多个
	// 属性的表达式。例如：
	//
	//     cel.bind(dra, device.attributes["dra.example.com"], dra.someBool && dra.anotherBool)
	//
	// 当启用 DRAListTypeAttributes 特性门控时，可使用 includes() 辅助函数，
	// 它对标量属性和列表属性均适用。引入它是为了支持从标量属性平滑迁移到
	// 列表属性，同时保持 CEL 表达式简洁。例如：
	//
	//     device.attributes["dra.example.com"].models.includes("some-model")
	//
	// 表达式的长度必须小于或等于 10 Ki。其求值成本也根据估算的逻辑
	// 步骤数加以限制。
	//
	// +required
	Expression string
}

// DeviceDerivedAttribute 定义通过 CEL 计算出的派生属性。
type DeviceDerivedAttribute struct {
	// Name 是该派生属性的标识符，用于约束（constraints）中。
	//
	// 它必须是一个 DNS 子域，后接斜杠（"/"），再接一个 C 标识符
	// （例如 "example.com/numaNode" 或 "derived/numaNode"）。
	//
	// 若所选名称与驱动已有的物理属性匹配，派生属性的表达式将遮蔽
	// （shadow）该物理属性，约束中改用其求值结果。如果定义派生属性
	// 仅是为了在 ResourceClaim 内部使用、并不想遮蔽已有属性，请使用
	// 任何 DRA 驱动都不应使用的域前缀（例如 "derived/myAttribute"）。
	//
	// 定义的派生属性若未被至少一个约束引用，则不合法。
	//
	// +required
	Name FullyQualifiedName

	// Expression 是针对每个候选设备求值的 CEL 表达式。
	// 表达式必须求值为原始标量（字符串、整数、布尔或 semver），
	// 或由这些标量组成的列表（[]string、[]int64、[]bool、[]semver），
	// 以充当虚拟分组键。任何其他返回类型都是错误，并导致该设备的
	// CEL 求值失败。
	//
	// 表达式的输入是一个名为 "device" 的对象，其携带的属性与
	// CELDeviceSelector 中的完全相同。
	//
	// 当 Pod 调度过程中对某些设备的 CEL 求值遇到运行时错误
	// （如查找未定义的属性）时，会中止分配并使该 Pod 调度失败。
	// 立即暴露求值错误可以防止静默的拓扑匹配失败——这种失败极难察觉。
	// 健壮的表达式应当在引用属性之前先检查其是否存在，以避免运行时
	// 求值错误。
	//
	// 该表达式在设备通过本请求中其他 selector 表达式之后才会被求值。
	// 这使得可以编写针对所请求设备特化的表达式（例如假定设备来自
	// 某个特定厂商，从而跳过相关检查）。
	//
	// 表达式的长度必须小于或等于 10 Ki。其求值成本也根据估算的逻辑
	// 步骤数加以限制；claim 中所有派生属性的总成本受共享的 CEL 成本
	// 预算约束。
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

	// MatchAttribute 要求所有相关设备都具有该属性，且其类型和值
	// 在这些设备之间完全相同。
	//
	// 例如，若指定 "dra.example.com/numa"（假设性示例！），则只会选择
	// 同一 NUMA 节点上的设备。不具有该属性的设备不会被选择。按照规范，
	// 所有设备对该属性都应当使用相同类型的值，但若某设备不满足，
	// 它同样不会被选择。
	//
	// 当启用 DRAListTypeAttributes 特性门控时，比较采用集合语义
	// （即忽略元素顺序与重复项）：当所有设备上列表属性值的交集非空时视为匹配。
	// 为向后兼容，标量值被视为单元素集合。
	//
	// 必须包含域限定符。
	//
	// +optional
	// +oneOf=ConstraintType
	MatchAttribute *FullyQualifiedName

	// 未来可能的扩展，不属于当前设计：
	// 一个比较不同设备、匹配时返回 true 的 CEL 表达式。
	//
	// 由于它将是 one-of 的一部分，旧调度器不会意外忽略这一
	// 对它们而言未知的额外匹配条件。
	//
	// MatchExpression string

	// DistinctAttribute 要求所有相关设备都具有该属性，且其类型和值
	// 在这些设备之间互不相同。
	//
	// 当启用 DRAListTypeAttributes 特性门控时，比较采用集合语义
	// （即忽略元素顺序与重复项）：列表属性值在各设备之间必须两两不相交。
	// 为向后兼容，标量值被视为单元素集合。
	//
	// 它的作用与 MatchAttribute 相反。
	//
	// 此约束通过确保属性级别的差异化，避免将多个请求分配到同一设备。
	//
	// 这适用于资源请求必须由独立物理设备满足的场景。
	// 例如：一个容器请求两个网卡，且必须从两块不同的物理 NIC 中分配。
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

// DeviceConfiguration 必须且只能设置一个字段。它以内联方式嵌入其他
// 含有其他字段的结构体中，因此字段名不得与那些字段冲突。
type DeviceConfiguration struct {
	// Opaque 提供驱动特有的配置参数。
	// +optional
	// +oneOf=ConfigurationType
	Opaque *OpaqueDeviceConfiguration
}

// OpaqueDeviceConfiguration 以驱动供应商定义的格式保存该驱动的配置参数。
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

// ResourceClaimStatus 跟踪资源是否已分配以及分配结果如何。
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

// ResourceClaimConsumerReference 包含定位 ResourceClaim 消费者所需的
// 信息。user 必须是与 ResourceClaim 处于同一命名空间的资源。
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

	// 该名称与驱动名、设备名字段共同标识被分配的是哪个设备
	// （`<driver name>/<pool name>/<device name>`）。
	//
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域。
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

// [DeviceAllocationConfiguration.Source] 的合法取值。
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

// DeviceClassSpec 用于 [DeviceClass]，定义可以分配什么以及如何配置。
type DeviceClassSpec struct {
	// 通过本类声明的设备必须满足每个 selector。
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

	// Items 是资源类的列表。
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

	// Items 是资源 claim 模板的列表。
	Items []ResourceClaimTemplate
}

const (
	// AllocatedDeviceStatusMaxConditions 表示设备状态中条件的最大数量。
	AllocatedDeviceStatusMaxConditions int = 8
	// AllocatedDeviceStatusDataMaxLength 表示设备状态中 Data 字段原始数据的最大长度
	AllocatedDeviceStatusDataMaxLength int = 10 * 1024
	// NetworkDeviceDataMaxIPs 表示设备状态中 networkData 字段里 IP 地址的最大数量。
	NetworkDeviceDataMaxIPs int = 16
	// NetworkDeviceDataInterfaceNameMaxLength 表示设备状态中 networkData.interfaceName
	// 字段的最大字符数。
	NetworkDeviceDataInterfaceNameMaxLength int = 256
	// NetworkDeviceDataHardwareAddressMaxLength 表示设备状态中 networkData.hardwareAddress 字段的最大字符数。
	NetworkDeviceDataHardwareAddressMaxLength int = 128
)

// AllocatedDeviceStatus 在驱动选择上报时包含已分配设备的状态，
// 其中可能包括驱动特有的信息。
//
// Driver、Pool、Device 和 ShareID 的组合必须与 Status.Allocation.Devices
// 中的对应键匹配。
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

// NetworkDeviceData 提供已分配设备的网络相关信息。驱动或其他组件
// 可填写该信息，用于在网络上下文中配置或标识该设备。
type NetworkDeviceData struct {
	// InterfaceName 指定与已分配设备关联的网络接口名称。
	// 它可能是正在 Pod 中配置的物理或虚拟网络接口的名称。
	// +optional
	InterfaceName string

	// IPs 列出分配给该设备网络接口的网络地址。
	// 可同时包含 IPv4 和 IPv6 地址。
	// IP 采用 CIDR 表示法，同时包含地址和关联的子网掩码。
	// 例如：IPv4 为 "192.0.2.5/24"，IPv6 为 "2001:db8::5/64"。
	//
	// 条目数不得超过 16 个。
	//
	// +optional
	// +listType=atomic
	IPs []string

	// HardwareAddress 表示设备网络接口的硬件地址（例如 MAC 地址）。
	//
	// 长度不得超过 128 个字符。
	//
	// +optional
	HardwareAddress string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// DeviceTaintRule 向所有匹配 selector 的设备添加一个污点。
// 其效果等同于 DRA 驱动在 ResourceSlice 中直接指定该污点。
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

// DeviceTaintSelector 定义 DeviceTaintRule 适用于哪些设备。
// 空 selector 匹配所有设备；不提供 selector 时不匹配任何设备。
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
	// Conditions 以机器可读和人类可读的格式，提供某个时间点上
	// DeviceTaintRule 和集群的状态信息。
	//
	// 本 API 目前定义了以下 condition，未来可能添加更多：
	// - Type: EvictionInProgress
	// - Status: 若当前有需要驱逐的 Pod 则为 True，否则为 False
	//   （包含不会导致驱逐的 effect）。
	// - Reason: 未指定，可能变化
	// - Message: 以人类可读格式包含待驱逐 Pod 与已驱逐 Pod 的数量信息，
	//   定期更新，内容可能变化
	//
	// 对于 `effect: None`，上述 condition 会在每次 spec 变更时设置一次，
	// message 中包含如果 effect 为 `NoExecute` 将会发生什么的信息。
	// 可借助这一反馈来判断将 effect 改为 `NoExecute` 是否会按预期工作。
	// 为避免不断更新 status，它只会被设置一次。
	//
	// 条目数必须不超过 8 个。
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

	// Items 是 DeviceTaintRule 的列表。
	Items []DeviceTaintRule
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourcePoolStatusRequest 基于所提供的过滤条件触发一次性的资源池状态计算。
// 一旦设置了 status，该请求即视为完成，不会重新处理。
// 用户应删除并重建请求以获取更新的信息。
type ResourcePoolStatusRequest struct {
	metav1.TypeMeta
	// 标准的对象元数据。
	// +required
	metav1.ObjectMeta

	// Spec 定义在 status 中包含哪些池的过滤条件。
	// +required
	Spec ResourcePoolStatusRequestSpec

	// Status 由控制器填充为计算出的资源池状态。
	// +optional
	Status *ResourcePoolStatusRequestStatus
}

// ResourcePoolStatusRequestSpec 定义池状态请求的过滤条件。
type ResourcePoolStatusRequestSpec struct {
	// Driver 指定用于过滤资源池的 DRA 驱动名。
	// 只包含来自使用该驱动的 ResourceSlice 的池。
	// 该字段为必填，用于限定请求的作用范围。
	Driver string

	// PoolName 可选地过滤到特定池名。
	// 未指定时，包含指定驱动的所有池。
	// 指定时，必须是合法的资源池名（以 "/" 分隔的 DNS 子域）。
	// +optional
	PoolName *string

	// Limit 可选地指定 status 中返回的池数量上限。
	// 若匹配过滤条件的池更多，响应将被截断。
	//
	// Default: 100
	// Maximum: 1000
	//
	// +optional
	Limit *int32

	// DefaultPartitionTypeAttribute 可选地指定一个设备属性（以其完全限定名），
	// 作为自身未声明分组属性的可分区设备的默认分组属性。分片自身的
	// PartitionTypeAttribute 始终优先。若分片与本默认值均未指定属性，
	// 可分区池将不上报 partitionSummary。
	// +optional
	DefaultPartitionTypeAttribute *string
}

// ResourcePoolStatusRequestLimitDefault 是 spec.limit 的默认值。
const ResourcePoolStatusRequestLimitDefault int32 = 100

// ResourcePoolStatusRequestLimitMax 是 spec.limit 允许的最大值。
const ResourcePoolStatusRequestLimitMax int32 = 1000

// ResourcePoolStatusRequestStatus 包含计算得到的池状态信息。
type ResourcePoolStatusRequestStatus struct {
	// PoolCount 是匹配过滤条件的池总数，不受截断影响。
	// +optional
	PoolCount *int32

	// Pools 按驱动名、池名排序，包含前 `spec.limit` 个匹配的池。
	// 若 len(pools) < poolCount，说明列表被截断。
	// +optional
	Pools []PoolStatus

	// Conditions 提供该请求状态的信息。
	// status 被填充时，总会设置一个 type=Complete 或 type=Failed 的条件。
	// +optional
	Conditions []metav1.Condition
}

// PoolStatus 包含单个资源池的状态信息。
type PoolStatus struct {
	// Driver 是该池对应的 DRA 驱动名。
	Driver string

	// PoolName 是该池的名称。
	PoolName string

	// Generation 是在该池所有 ResourceSlice 中观察到的池 generation。
	Generation int64

	// ResourceSliceCount 是构成该池的 ResourceSlice 数量。
	// +optional
	ResourceSliceCount *int32

	// TotalDevices 是该池跨所有分片的设备总数。
	// +optional
	TotalDevices *int32

	// AllocatedDevices 是当前已分配给 claim 的设备数量。
	// +optional
	AllocatedDevices *int32

	// AvailableDevices 是可分配的设备数量。
	// 其值等于 TotalDevices - AllocatedDevices - UnavailableDevices。
	// +optional
	AvailableDevices *int32

	// UnavailableDevices 是因污点或其他条件而不可用、但并未被分配的设备数量。
	// +optional
	UnavailableDevices *int32

	// NodeName 是该池关联的节点。
	// 省略时表示该池不与特定节点关联。
	// +optional
	NodeName *string

	// ValidationError 在池的数据无法完全校验时设置。
	// 设置后，设备计数字段与 ResourceSliceCount 可能未填充。
	// +optional
	ValidationError *string

	// PartitionSummary 针对可分区池，按（属性，分区类型）报告可分配性。
	// 每个条目命名其解析来源的分组属性：设备所在分片自身声明的属性；
	// 若分片未声明，则为请求中指定的默认属性。
	// +optional
	PartitionSummary []PartitionTypeStatus

	// ShareableSummary 针对包含带 AllowMultipleAllocations 设备的池，
	// 报告聚合容量。
	// +optional
	ShareableSummary *ShareableSummaryStatus
}

// PartitionTypeStatus 报告由分组属性值标识的单一分区类型的可分配性。
type PartitionTypeStatus struct {
	// Attribute 是其值用于对本条目分组的设备属性的完全限定名。
	// 它是设备所在分片自身声明的 PartitionTypeAttribute；若分片未声明，
	// 则为请求中指定的默认属性。
	// +required
	Attribute string

	// Type 是分区类型的值（例如 "Full" 或 "Half"）。
	// +required
	Type string

	// Total 是该池中此分区类型的设备数量。
	// +required
	Total *int32

	// Allocatable 是在当前共享计数器消耗情况下，仍可分配的该分区类型
	// 的设备数量。
	// +required
	Allocatable *int32
}

// ShareableSummaryStatus 针对包含带 AllowMultipleAllocations 设备的池，
// 报告聚合容量。
type ShareableSummaryStatus struct {
	// FullyAvailableDevices 是未消耗任何容量的可共享设备数量。
	// +required
	FullyAvailableDevices *int32

	// PartiallyAvailableDevices 是消耗了部分（但非全部）容量的
	// 可共享设备数量。
	// +required
	PartiallyAvailableDevices *int32

	// Capacity 报告整个池中每个可共享容量键的总量、已消耗量与可用量汇总。
	// +optional
	Capacity []ShareableCapacityStatus
}

// ShareableCapacityStatus 报告单个可共享容量键的汇总数量。
type ShareableCapacityStatus struct {
	// Name 是容量名称。
	// +required
	Name string

	// Total 是该池中所有可共享设备上此容量的总和。
	// +required
	Total *resource.Quantity

	// Consumed 是当前分配所消耗的数量。
	// +required
	Consumed *resource.Quantity

	// Available 是 Total 减去 Consumed，结果不会为负。
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
