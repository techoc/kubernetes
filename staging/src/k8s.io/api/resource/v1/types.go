/*
Copyright 2025 The Kubernetes Authors.

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

package v1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// Finalizer 是内置控制器分配的 ResourceClaim 上设置的 finalizer
	//
	// 此 finalizer 保留给 Kubernetes 使用，DRA 驱动控制器必须使用
	// 自己的 finalizer
	Finalizer = "resource.kubernetes.io/delete-protection"
	// ExtendedResourceClaimAnnotation 是设置在生成的特殊 ResourceClaim 上的注解
	// 其唯一合法值为 "true"
	// 仅在调度器内部使用
	ExtendedResourceClaimAnnotation = "resource.kubernetes.io/extended-resource-claim"
	// PodResourceClaimAnnotation 由 ResourceClaim 控制器设置在模板生成的 ResourceClaim 上
	// 其值为生成该 claim 的 Pod 或 PodGroup 的 spec.resourceClaims[].name。
	//
	// Pod 与其 PodGroup 保证同名
	PodResourceClaimAnnotation = "resource.kubernetes.io/pod-claim-name"
	// 资源设备类前缀用于当设备类未指定 ExtendedResourceName 时，生成隐式的扩展资源名。
	//
	// 生成名为该前缀加上设备类名
	// 生成的名称可能不是 pod.Spec.Resources.Requests 中合法的扩展资源名，
	// 此时必须在设备类中显式指定合法名称
	//
	ResourceDeviceClassPrefix string = "deviceclass.resource.kubernetes.io/"

	// 以下常量均与 resourceclaims.status 写入的合成授权检查相关

	// SubresourceBinding 是用于授权 status.allocation 和 status.reservedFor 更新的合成子资源
	//
	SubresourceBinding = "binding"
	// SubresourceDriver 是用于按驱动授权 status.devices 更新的合成子资源
	//
	SubresourceDriver = "driver"
	// VerbPrefixAssociatedNode 是来自与 claim 分配所在节点相同的节点上的服务账号请求所使用的动词前缀。
	// 完整动词为
	// "associated-node:<request-verb>"，例如 "associated-node:update"
	VerbPrefixAssociatedNode = "associated-node:"
	// VerbPrefixArbitraryNode 是不与特定节点关联的请求（如控制器）所用的动词前缀。
	// 完整动词为
	// "arbitrary-node:<request-verb>"，例如 "arbitrary-node:update"
	VerbPrefixArbitraryNode = "arbitrary-node:"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// ResourceSlice 表示由同一驱动管理的、同类资源池中的一个或多个资源。
// 一个池可能跨多个 ResourceSlice，具体由多少 ResourceSlice 构成由驱动决定。
//
// 目前仅支持带属性和容量的设备资源
// 同一池中的每个设备，无论分布在多少个 ResourceSlice 中，都必须有唯一名称
// 设备发布所在的 ResourceSlice 可能随时间变化。设备的唯一标识是
// 元组 <driver name>, <pool name>, <device name>
//
// 驱动更新池时，会递增 pool.Spec.Pool.Generation，
// 并用该新值和新资源定义更新所有 ResourceSlice。消费者
// 只能使用 generation 最大的 ResourceSlice，忽略其他版本
//
// 在分配池中符合特定条件的全部资源，或在多种方案中寻找最优解时，
// 消费者应检查池中 ResourceSlice 的数量（每个 ResourceSlice 中都带有该值），
//
// 以判断自己对池的视图是否完整；若不完整，
// 应等待驱动完成池的更新
//
// 对于非节点本地资源，不设置节点名，而是
// 由驱动使用节点选择器指明设备可用的位置
type ResourceSlice struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 保存驱动发布的信息
	//
	// 修改 spec 会自动递增 metadata.generation
	// +required
	Spec ResourceSliceSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

const (
	// ResourceSliceSelectorNodeName 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.NodeName] 过滤
	ResourceSliceSelectorNodeName = "spec.nodeName"
	// ResourceSliceSelectorDriver 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.Driver] 过滤
	ResourceSliceSelectorDriver = "spec.driver"
	// ResourceSliceSelectorPoolName 可用于 [metav1.ListOptions] 的字段选择器，
	// 按 [ResourceSliceSpec.Pool.Name] 过滤
	ResourceSliceSelectorPoolName = "spec.pool.name"
)

// ResourceSliceSpec 保存单个 ResourceSlice 中由驱动发布的信息
type ResourceSliceSpec struct {
	// Driver 标识提供容量信息的 DRA 驱动
	// 可用字段选择器只列出指定驱动的 ResourceSlice 对象
	//
	// 必须是 DNS 子域，且最好以驱动供应商拥有的 DNS 域结尾，
	// 并且只使用小写字符
	// 本字段不可变
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Pool 描述本 ResourceSlice 所属的池
	//
	// +required
	Pool ResourcePool `json:"pool" protobuf:"bytes,2,name=pool"`

	// NodeName 标识提供池中资源的节点
	// 可用字段选择器只列出指定驱动的 ResourceSlice 对象
	// 属于特定节点的 ResourceSlice 对象
	//
	// 该字段可用于限制节点只能访问节点名相同的 ResourceSlice。
	// 它还向自动扩缩容器表明，新增与某个旧节点同类型的新节点
	// 也可能带来新的可用资源
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个
	// 本字段不可变
	//
	// +optional
	// +oneOf=NodeSelection
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// NodeSelector 定义哪些节点可以访问池中的资源（当池不限于单个节点时）
	//
	// 必须且只能使用一个匹配项（term）
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个
	//
	// +optional
	// +oneOf=NodeSelection
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,4,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问池中的资源
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个
	//
	// +optional
	// +oneOf=NodeSelection
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"bytes,5,opt,name=allNodes"`

	// Devices 列出池中的部分或全部设备
	//
	// 不得超过 128 项。若任一设备使用污点或消费计数器，上限为 64
	//
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +zeroOrOneOf=ResourceSliceType
	Devices []Device `json:"devices,omitempty" protobuf:"bytes,6,name=devices"`

	// PerDeviceNodeSelection 定义节点对池中资源的访问限制是在 ResourceSlice 层设置，
	// 还是逐设备设置。
	// 设为 true 时，ResourceSlice 中定义的每个设备
	// 都必须单独指定该设置
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个
	//
	// +optional
	// +oneOf=NodeSelection
	// +featureGate=DRAPartitionableDevices
	PerDeviceNodeSelection *bool `json:"perDeviceNodeSelection,omitempty" protobuf:"bytes,7,name=perDeviceNodeSelection"`

	// SharedCounters 定义计数器集合的列表，每个集合
	// 都有一个名称和一组可用计数器
	//
	// 计数器集合的名称在 ResourcePool 中必须唯一
	//
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一
	//
	// 计数器集合最多 8 个
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=name
	// +featureGate=DRAPartitionableDevices
	// +zeroOrOneOf=ResourceSliceType
	// +k8s:beta(since: "1.37")=+k8s:maxItems=8
	SharedCounters []CounterSet `json:"sharedCounters,omitempty" protobuf:"bytes,8,name=sharedCounters"`

	// PartitionTypeAttribute 指定一个字符串类型的设备属性（以完整限定名给出，
	// 如 "gpu.example.com/profile"），其值用于标记每个设备的分区类型，
	// 例如 MIG 风格 GPU 的 "Full" 或 "Half"
	//
	// 设置后，slice 中每个可分区设备都必须带有该属性，
	// 且共享同一属性值的设备必须拥有相同的 ConsumesCounters 开销
	//
	// +optional
	// +featureGate=DRAPartitionableDevicesType
	// +k8s:ifDisabled(DRAPartitionableDevicesType)=+k8s:forbidden
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:optional
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:format=k8s-resource-fully-qualified-name
	PartitionTypeAttribute *FullyQualifiedName `json:"partitionTypeAttribute,omitempty" protobuf:"bytes,9,opt,name=partitionTypeAttribute"`

	// SkipNodeOperations 列出此已分配设备跳过哪些节点本地资源操作（gRPC 调用）。
	// 即在判断节点上是否需要执行操作时跳过这些调用。
	// 则该 gRPC 调用会被跳过。该字段是分配时 ResourceSlice.spec.skipNodeOperations 的副本
	// 则该 gRPC 调用会被跳过。合法值为：
	//
	// - "NodePrepareResources"：跳过 NodePrepareResources gRPC 调用。
	// 除非同时列出 "NodeUnprepareResources"（或指定 "*"），
	// 否则不能单独指定该值
	// - "NodeUnprepareResources"：跳过 NodeUnprepareResources gRPC 调用
	// - "*"：跳过所有节点本地资源操作
	//
	// 未来可能增加其他值，kubelet 必须忽略未知值
	//
	// +optional
	// +listType=set
	// +k8s:listType=set
	// +featureGate=DRAOptionalNodeOperations
	// +k8s:optional
	SkipNodeOperations []SkipNodeOperation `json:"skipNodeOperations,omitempty" protobuf:"bytes,10,rep,name=skipNodeOperations,casttype=SkipNodeOperation"`
}

// +enum
// +k8s:enum
// SkipNodeOperation 表示可跳过的节点本地资源操作
type SkipNodeOperation string

const (
	// SkipNodeOperationNodePrepareResources 表示跳过 NodePrepareResources gRPC 调用
	SkipNodeOperationNodePrepareResources SkipNodeOperation = "NodePrepareResources"

	// SkipNodeOperationNodeUnprepareResources 表示跳过 NodeUnprepareResources gRPC 调用
	SkipNodeOperationNodeUnprepareResources SkipNodeOperation = "NodeUnprepareResources"

	// SkipNodeOperationAll 表示跳过所有节点本地资源操作
	SkipNodeOperationAll SkipNodeOperation = "*"
)

// CounterSet 定义一个具名的计数器集合，
// 供 ResourcePool 中定义的设备使用
//
// 计数器本身不可分配，但可被设备引用。
// 设备被分配时，其使用的计数器部分
// 将不再可供其他设备使用
type CounterSet struct {
	// Name 定义计数器集合的名称
	// 必须是 DNS label
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Counters 定义该 CounterSet 的计数器集合
	// 每个计数器的名称在集合内必须唯一，且必须是 DNS label
	//
	// 计数器最多 32 个
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,name=counters"`
}

// DriverNameMaxLength 是 ResourceSliceSpec 及其他位置中驱动名的最大合法长度，
// 与 CSI 驱动名的限制相同
const DriverNameMaxLength = 63

// ResourcePool 描述 ResourceSlice 所属的池
type ResourcePool struct {
	// Name 用于标识池。对于节点本地设备，
	// 通常取节点名，但非强制
	// 可用字段选择器只列出属于特定池的 ResourceSlice 对象
	//
	// 长度不得超过 253 个字符，且必须由一个或多个以斜杠分隔的 DNS 子域组成。
	// 本字段不可变
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Generation 记录池随时间的变化。驱动
	// 每次改动池中一个或多个资源时，
	// 都必须更新该池所有 ResourceSlice 的 generation。
	// ResourceSlice 的消费者只应考虑
	// generation 最大的池资源。
	// 驱动可以重置 generation；只要池中所有 ResourceSlice 都被更新
	// 或删除，这对消费者没有影响
	//
	// 与 ResourceSliceCount 结合，该机制使消费者能够
	// 检测出由多个 ResourceSlice 组成但尚不完整的池
	//
	// +required
	Generation int64 `json:"generation" protobuf:"bytes,2,name=generation"`

	// ResourceSliceCount 是该 generation 下池中 ResourceSlice 的总数，必须大于零
	//
	// 消费者可用它判断是否已经看到同一池的所有 ResourceSlice
	//
	// +required
	ResourceSliceCount int64 `json:"resourceSliceCount" protobuf:"bytes,3,name=resourceSliceCount"`
}

// ResourceSliceMaxSharedCapacity 是单个 ResourceSlice 可上报的最大共享容量数量
const ResourceSliceMaxSharedCapacity = 128

// ResourceSliceMaxDevices 是单个 ResourceSlice 中设备的最大数量
const ResourceSliceMaxDevices = 128

// ResourceSliceMaxDevicesWithAdvancedFeatures 定义当任一设备使用高级特性时，ResourceSlice 中设备的最大数量：
//
// - 设备污点（DRADeviceTaints 特性门控）
// - 消费计数器（DRAPartitionableDevices 特性门控）
// - 列表属性（DRAListTypeAttributes 特性门控）
const ResourceSliceMaxDevicesWithAdvancedFeatures = 64

// PoolNameMaxLength 是池名的最大长度，与单个节点名相同
const PoolNameMaxLength = validation.DNS1123SubdomainMaxLength // Same as for a single node name.
// BindingConditionsMaxSize 是绑定条件的最大数量
const BindingConditionsMaxSize = 4

// BindingFailureConditionsMaxSize 是绑定失败条件的最大数量
const BindingFailureConditionsMaxSize = 4

// 定义 ResourceSlice 中通过 SharedCounters 字段
// 可定义的计数器集合的最大数量
const ResourceSliceMaxCounterSets = 8

// 定义每次设备计数器消费中可定义的计数器最大数量
//
// ResourceSliceMaxCountersPerCounterSet 是单个计数器集合中计数器的最大数量
const ResourceSliceMaxCountersPerCounterSet = 32

// 定义每个设备通过 ConsumesCounters 字段
// 可定义的计数器消费记录的最大数量
//
// ResourceSliceMaxDeviceCounterConsumptionsPerDevice 是单个设备的计数器消费记录最大数量
const ResourceSliceMaxDeviceCounterConsumptionsPerDevice = 2

// 定义每次设备计数器消费中可定义的计数器最大数量
//
// ResourceSliceMaxCountersPerDeviceCounterConsumption 是单次计数器消费中计数器的最大数量
const ResourceSliceMaxCountersPerDeviceCounterConsumption = 32

// 定义每次设备计数器消费中可声明的兼容组最大数量
//
// DeviceCompatibilityGroupsMaxSize 是单次计数器消费中兼容组的最大数量
const DeviceCompatibilityGroupsMaxSize = 2

// Device 表示一个可根据属性选择的独立硬件实例。除名称外，必须且只能设置一个字段
type Device struct {
	// Name 在驱动管理的池中所有设备间唯一标识该设备，
	// 必须是 DNS label
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Attributes 定义该设备的属性集合
	// 每个属性的名称在集合内必须唯一
	//
	// 属性与容量的总数最多 32 个
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Attributes map[QualifiedName]DeviceAttribute `json:"attributes,omitempty" protobuf:"bytes,2,rep,name=attributes"`

	// Capacity 定义该设备的容量集合
	// 每个容量的名称在集合内必须唯一
	//
	// 属性与容量的总数最多 32 个
	//
	// +optional
	Capacity map[QualifiedName]DeviceCapacity `json:"capacity,omitempty" protobuf:"bytes,3,rep,name=capacity"`

	// ConsumesCounters 定义对 sharedCounters 的引用列表，
	// 以及设备将从这些计数器集合中消费的计数器集合
	//
	// 每个 counterSet 只能有一条记录
	//
	// 每个设备最多 2 条计数器消费记录
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=counterSet
	// +featureGate=DRAPartitionableDevices
	// +k8s:beta(since: "1.37")=+k8s:maxItems=2
	ConsumesCounters []DeviceCounterConsumption `json:"consumesCounters,omitempty" protobuf:"bytes,4,rep,name=consumesCounters"`

	// NodeName 标识设备可用的节点
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置
	// NodeName、NodeSelector 和 AllNodes 最多设置一个
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,5,opt,name=nodeName"`

	// NodeSelector 定义设备可用的节点
	//
	// 必须且只能使用一个匹配项（term）
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置
	// NodeName、NodeSelector 和 AllNodes 最多设置一个
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,6,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问该设备
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置
	// NodeName、NodeSelector 和 AllNodes 最多设置一个
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"bytes,7,opt,name=allNodes"`

	// 若指定，这些是驱动定义的污点
	//
	// 污点最多 16 个。若 ResourceSlice 中任一设备设置了污点，
	// 则该 ResourceSlice 允许的设备数上限从 128 降为 64
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Taints []DeviceTaint `json:"taints,omitempty" protobuf:"bytes,8,rep,name=taints"`

	// BindsToNode 表示涉及该设备的分配的使用是否必须限制在分配时选定的那个节点。
	//
	// 设为 true 时，调度器会将 ResourceClaim.Status.Allocation.NodeSelector 设置为匹配
	// 执行分配所在的节点
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindsToNode *bool `json:"bindsToNode,omitempty" protobuf:"varint,9,opt,name=bindsToNode"`

	// BindingConditions 定义继续绑定所需满足的条件
	// 调度 Pod 时，这些条件必须在各设备的 status.conditions 中
	// 全部为 True，才能继续将 Pod 绑定到节点
	//
	// 绑定条件最多 4 个
	//
	// 条件必须是合法的 condition 类型字符串
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,10,rep,name=bindingConditions"`

	// BindingFailureConditions 定义绑定失败的条件
	// 这些条件可设置在各设备的 status.conditions 中
	// 任一条件为 "True" 即表示发生了绑定失败
	//
	// 绑定失败条件最多 4 个
	//
	// 条件必须是合法的 condition 类型字符串
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,11,rep,name=bindingFailureConditions"`

	// AllowMultipleAllocations 标记该设备是否允许被分配给多个 DeviceRequest
	//
	// 设为 true 时，设备可被多次分配，
	// 且无论是否定义 requestPolicy，其全部容量都可被消费
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	AllowMultipleAllocations *bool `json:"allowMultipleAllocations,omitempty" protobuf:"bytes,12,opt,name=allowMultipleAllocations"`

	// NodeAllocatableResourceMappings 已废弃（tombstone），由 NodeAllocatableResources 取代
	// NodeAllocatableResourceMappings map[v1.ResourceName]NodeAllocatableResourceMapping `json:"nodeAllocatableResourceMappings,omitempty" protobuf:"bytes,13,opt,name=nodeAllocatableResourceMappings"`

	// NodeAllocatableResources 定义由暴露该设备的 DRA 驱动所管理的节点资源映射，
	// 包括目前上报在 v1.Node status.allocatable 中且不属于扩展资源的资源
	//
	// （参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources）
	// 例如 "cpu"、"memory"、"ephemeral-storage" 和大页
	// 除了通过 Pod `spec` 发起的标准请求外，这些资源
	// 也可以通过 claim 请求并由 DRA 驱动分配
	// 例如，CPU DRA 驱动可以分配独占 CPU，或分配加速器设备的辅助节点内存
	//
	// 该映射的键是节点可分配资源名（如 "cpu"、"memory"）
	// 不允许使用扩展资源名作为键
	// +optional
	// +k8s:optional
	// +featureGate=DRANodeAllocatableResources
	NodeAllocatableResources map[v1.ResourceName]NodeAllocatableResource `json:"nodeAllocatableResources,omitempty" protobuf:"bytes,14,opt,name=nodeAllocatableResources"`
}

// NodeAllocatableResource 定义请求的 DRA 设备/容量单位与对应节点可分配资源数量之间的换算。
//
// Mapping 和 Overhead 至少指定一个，都不指定属于非法配置
type NodeAllocatableResource struct {
	// 当设备直接建模节点可分配资源（如标准 CPU 或内存）时使用 Mapping（例如 CPU DRA 驱动）。
	// 计算出的数量在每个 claim 实例中只计入一次。
	// 为避免节点 cgroup 隔离冲突，调度器明确
	// 禁止跨多个 Pod 共享映射设备 claim
	// +optional
	// +k8s:optional
	Mapping *NodeAllocatableMapping `json:"mapping,omitempty" protobuf:"bytes,3,opt,name=mapping"`

	// Overhead 的字段用于建模分配那些本身不建模节点可分配资源的设备时，在节点可分配资源上产生的辅助开销
	// （例如 GPU 的主机内存开销）
	// 允许跨多个 Pod 共享开销映射的 claim。节点可分配开销按每个引用该 claim 的 Pod 单独计入
	//
	// 即使对同一资源指定了 mapping，Overhead 也始终从该资源的节点可分配容量中扣除
	//
	// 例如：若设备通过 Mapping（带 CapacityKey）将每个 socket 的内存容量建模为可消费容量池，
	// 则为同一资源指定的任何开销都会从节点通用可分配容量中扣除，
	// 而不是从 Mapping 中每 socket 的容量池扣除
	// +optional
	// +k8s:optional
	Overhead *NodeAllocatableOverhead `json:"overhead,omitempty" protobuf:"bytes,4,opt,name=overhead"`
}

// NodeAllocatableMapping 定义 DRA 分配如何直接换算为节点可分配资源数量。
// 映射可以由已分配设备数（通过 deviceMultiplier）或消费的具体容量（通过 capacityKey 和 capacityMultiplier）得出，二者互斥
// Kubelet 将 claim 的该映射资源数量同时加到 Pod 级 cgroup 的 requests 和 limits，以及每个引用该 claim 的容器级 cgroup 的 limits 上
type NodeAllocatableMapping struct {
	// CapacityKey 引用 `spec.devices[*].capacity` 映射中作为键定义的容量名。
	// 设置后，对某次具体的 claim 分配，
	// `status.allocation.devices.results[*].consumedCapacity` 映射中该键对应的值
	// 决定节点可分配资源的基础数量
	// 还必须设置 `capacityMultiplier`，
	// 它会与该基础数量相乘
	// 例如，若 `spec.devices[*].capacity` 有条目 "dra.example.com/memory": "128Gi"，
	// 且本字段设为 "dra.example.com/memory"，则对于消费了
	// { "dra.example.com/memory": "4Gi" } 的 claim 分配，节点可分配资源映射的基础数量为 "4Gi"
	//
	// 最终节点可分配资源数量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityMultiplier")
	CapacityKey *QualifiedName `json:"capacityKey,omitempty" protobuf:"bytes,1,opt,name=capacityKey"`

	// CapacityMultiplier 用作已消费分配容量的乘数
	// 仅在设置 `capacityKey` 时有效
	// 最终节点可分配资源数量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`
	// 例如，若消费了设备的 "dra.example.com/cores" 容量，
	// 而每个 "core" 提供 2 个 "cpu"，映射为：
	//
	// 若 claim 消费 8 个 "dra.example.com/cores"，CPU 占用为 8 * 2 = 16
	// +optional
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityKey")
	CapacityMultiplier *resource.Quantity `json:"capacityMultiplier,omitempty" protobuf:"bytes,2,opt,name=capacityMultiplier"`

	// DeviceMultiplier 用作 claim 中已分配设备数的乘数
	// 最终节点可分配资源数量为 `deviceCount` * `deviceMultiplier`
	// 例如，将每个 cache complex（CCX）表示为设备的 DRA 驱动，其 `nodeAllocatableResources` 中会有
	//
	// 若给 claim 分配 2 个设备（CCX），则视为分配了 2 * 8 = 16 个 CPU
	// 仅在未设置 `capacityKey` 和 `capacityMultiplier` 时有效
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	DeviceMultiplier *resource.Quantity `json:"deviceMultiplier,omitempty" protobuf:"bytes,3,opt,name=deviceMultiplier"`
}

// NodeAllocatableOverhead 定义分配设备时产生的辅助资源开销
// 开销可以指定为引用该 claim 的每个 Pod 的固定成本、每次容器引用的可变成本，或两者兼有
// Kubelet 通过将该开销加到引用容器的 Pod 级和容器级 cgroup 来进行计量
type NodeAllocatableOverhead struct {
	// PerPod 是对本节点上每个引用该 claim 的 Pod 施加一次的开销
	// 它是每个引用 claim 的 Pod 都产生的固定开销
	// +optional
	// +k8s:optional
	PerPod *resource.Quantity `json:"perPod,omitempty" protobuf:"bytes,1,opt,name=perPod"`

	// PerContainer 按每次容器对 claim 的引用施加
	// 它建模随实际使用该设备的容器数量线性增长的开销
	// 同时指定 PerPod 和 PerContainer 时，每个引用 claim 的 Pod 分配的总开销计算为：
	//
	// Kubelet 在 cgroup 中按如下方式计量该开销：
	// - Pod 级 cgroup（requests 和 limits）：Kubelet 加上 PerPod + (PerContainer * NumReferences)
	// - 容器级 cgroup（仅 limits）：Kubelet 为每个引用容器加上 PerPod + PerContainer
	// 这样任何单个容器都可以使用 Pod 级开销，而父 cgroup 限制总用量，确保 PerPod 只计入一次
	// +optional
	// +k8s:optional
	PerContainer *resource.Quantity `json:"perContainer,omitempty" protobuf:"bytes,2,opt,name=perContainer"`
}

// DeviceCounterConsumption 定义设备将从一个 CounterSet 中消费的一组计数器
type DeviceCounterConsumption struct {
	// CounterSet 是要消费的计数器集合的名称
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	CounterSet string `json:"counterSet" protobuf:"bytes,1,opt,name=counterSet"`

	// Counters 定义设备将消费的计数器
	//
	// 计数器最多 32 个
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,opt,name=counters"`

	// CompatibilityGroups 是本次计数器集合消费的不透明组名列表
	//
	// 从同一计数器集合消费计数器的设备，只有当它们至少共享一个公共组时才能
	// 同时分配（"co-allocated"）：
	// 该计数器集合上所有共同分配设备的 CompatibilityGroups 交集必须非空。
	// 从不同计数器集合消费的设备之间不通过该字段比较
	//
	// 未设置、显式 nil 和空列表等价，均表示"无组"：
	// 这样的设备只能与同一计数器集合上同样无组的设备共同分配，
	// 绝不能与声明了一个或多个组的设备共同分配
	//
	// 组名是不透明的，仅在发布驱动的池内有意义
	//
	// 组数最多 2 个，名称必须唯一
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceCompatibilityGroups
	// +k8s:listType=atomic
	// +k8s:optional
	// +k8s:maxItems=2
	// +k8s:unique=set
	// +k8s:eachVal=+k8s:format=k8s-short-name
	CompatibilityGroups []string `json:"compatibilityGroups,omitempty" protobuf:"bytes,3,rep,name=compatibilityGroups"`
}

// DeviceCapacity 描述与设备关联的数量
type DeviceCapacity struct {
	// Value 定义设备拥有某种容量的数量
	//
	// 本字段反映固定的总容量，不会改变
	// 已消费的数量由调度器单独跟踪，
	// 不影响本值
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`

	// RequestPolicy 定义设备允许被多次分配时，该容量如何被消费
	//
	// 要设置 requestPolicy，设备必须设置 allowMultipleAllocations 为 true
	//
	// 未设置时，容量请求不受约束：
	// 请求可以消费任意数量的容量，只要所有分配中消费的总量
	// 不超过设备定义的容量
	// 若请求也未设置，默认为全部容量
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	RequestPolicy *CapacityRequestPolicy `json:"requestPolicy,omitempty" protobuf:"bytes,2,opt,name=requestPolicy"`
}

// Counter 描述与设备关联的数量
type Counter struct {
	// Value 定义某种设备计数器的可用数量
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`
}

// CapacityRequestPolicy 定义请求如何消费设备容量
//
// ValidRequestValues 中最多设置一个
type CapacityRequestPolicy struct {
	// Default 指定当 DeviceRequest 的 Capacity 中没有对应条目时，请求消费多少该容量
	//
	// +optional
	Default *resource.Quantity `json:"default" protobuf:"bytes,1,opt,name=default"`

	// ValidValues 定义消费请求中可接受的数量值集合
	//
	// 不得超过 10 项
	// 必须按升序排列
	//
	// 设置本字段时，
	// 必须定义 Default，且 Default 必须包含在 ValidValues 列表中
	//
	// 若请求量不匹配任何合法值但小于某些合法值，
	// 调度器会计算出大于等于请求量的最小合法值
	// 即：min(ceil(requestedValue) ∈ validValues), 其中 requestedValue ≤ max(validValues)
	//
	// 若请求量超过所有合法值，则违反策略，
	// 该设备不可被分配
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValidRequestValues
	ValidValues []resource.Quantity `json:"validValues,omitempty" protobuf:"bytes,3,opt,name=validValues"`

	// ValidRange 定义消费请求中可接受的数量范围
	//
	// 设置本字段时，
	// 必须定义 Default，且 Default 必须落在 ValidRange 范围内
	//
	// 若请求量不在定义范围内，则违反策略，
	// 该设备不可被分配
	//
	// 若请求未包含该容量条目，则使用 Default 值
	//
	// +optional
	// +oneOf=ValidRequestValues
	ValidRange *CapacityRequestPolicyRange `json:"validRange,omitempty" protobuf:"bytes,4,opt,name=validRange"`
}

// CapacityRequestPolicyRange 定义可消费容量值的合法范围
//
// 若启用 DRAFractionalCapacityRange 特性门控，
// 且 Min、Max、Step 中至少有一个是分数量（即其值不是整数），
// 则改用毫单位算术，
// 支持最多 3 位小数（例如 100m = 0.1）
// 此时支持的最大值比使用 64 位整数时小 1000 倍
// 否则，所有比较都通过 resource.Quantity.Value() 使用 64 位整数算术
//
//   - 若请求量小于 Min，向上取整到 Min
//   - 若设置了 Step，且请求量在 Min 与 Max 之间但未与 Step 对齐，
//     则向上取整到下一个等于 Min + (n * Step) 的值
//   - 若未设置 Step，请求量在 Min 到 Max（如设置）范围内时原样使用
//   - 若请求量或取整后的值超过 Max（如设置），则请求不满足策略，
//
// 设备不可被分配
type CapacityRequestPolicyRange struct {
	// Min 指定消费请求允许的最小容量
	//
	// Min 必须大于等于零，
	// 且小于等于容量值
	// requestPolicy.default 必须大于等于该最小值
	//
	// +required
	Min *resource.Quantity `json:"min,omitempty" protobuf:"bytes,1,opt,name=min"`

	// Max 定义可请求容量的上限
	//
	// Max 必须小于等于容量值
	// Min 和 requestPolicy.default 必须小于等于该最大值
	//
	// +optional
	Max *resource.Quantity `json:"max,omitempty" protobuf:"bytes,2,opt,name=max"`

	// Step 定义范围内合法容量值之间的步长
	//
	// Max（如设置）和 requestPolicy.default 必须是 Step 的倍数
	// Min + Step 必须小于等于容量值
	//
	// +optional
	Step *resource.Quantity `json:"step,omitempty" protobuf:"bytes,3,opt,name=step"`
}

// 属性与容量条目数之和的上限
const ResourceSliceMaxAttributesAndCapacitiesPerDevice = 32

// 每个设备中，列表与非列表属性里字符串、版本、布尔或整数值的总数上限
//
// ResourceSliceMaxAttributeValuesPerDevice 是单个设备中属性值的最大数量
const ResourceSliceMaxAttributeValuesPerDevice = 48

// QualifiedName 是设备属性或容量的名称
//
// 属性和容量由具体驱动的拥有者（通常是供应商）或第三方（如 Kubernetes 项目）定义。
//
// 由于它们有时需要跨设备比较，同一名称在所有设备上
// 应具有相同含义和相同类型
//
// 名称必须是 C 标识符（如 "theName"），或 DNS 子域加斜杠（"/"）再加 C 标识符
//
// （如 "dra.example.com/theName"）。不含域前缀的名称
// 视为属于驱动的域；第三方定义的属性
// 或容量必须包含域前缀
//
// DNS 子域最长 63 个字符（与驱动名相同），C 标识符最长 32 个字符
type QualifiedName string

// FullyQualifiedName 是设置了域的 QualifiedName
// 该类型无法添加格式校验，因为其用途之一 DistinctAttribute 是有条件校验的，
// 而这种条件校验无法用声明式规则表达
type FullyQualifiedName string

// DeviceMaxDomainLength 是完全限定名中域前缀的最大长度
const DeviceMaxDomainLength = 63

// DeviceMaxIDLength 是设备属性或容量名（`<domain>/<ID>`）中标识符的最大长度
const DeviceMaxIDLength = 32

// DeviceAttribute 必须且只能设置一个字段
type DeviceAttribute struct {
	// 下面的 Go 字段名带 Value 后缀，是为了避免字段 "String" 与对应方法名冲突；
	// 该方法是必需的
	// Kubernetes API 定义中不带该后缀，以保持自然

	// IntValue 是数字
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	IntValue *int64 `json:"int,omitempty" protobuf:"varint,2,opt,name=int"`

	// BoolValue 是布尔值
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	BoolValue *bool `json:"bool,omitempty" protobuf:"varint,3,opt,name=bool"`

	// StringValue 是字符串，长度不得超过 64 个字符
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	StringValue *string `json:"string,omitempty" protobuf:"bytes,4,opt,name=string"`

	// VersionValue 是符合 semver.org 2.0.0 规范的语义化版本
	// 长度不得超过 64 个字符
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	VersionValue *string `json:"version,omitempty" protobuf:"bytes,5,opt,name=version"`

	// IntValues 是非空数字列表
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	IntValues []int64 `json:"ints,omitempty" protobuf:"varint,6,opt,name=ints"`

	// BoolValues 是非空布尔值列表
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	BoolValues []bool `json:"bools,omitempty" protobuf:"varint,7,opt,name=bools"`

	// StringValues 是非空字符串列表
	// 每个字符串不得超过 64 个字符
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:maxBytes=64
	// +featureGate=DRAListTypeAttributes
	StringValues []string `json:"strings,omitempty" protobuf:"bytes,8,opt,name=strings"`

	// VersionValues 是符合 semver.org 2.0.0 规范的语义化版本非空列表
	// 每个版本字符串不得超过 64 个字符
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	VersionValues []string `json:"versions,omitempty" protobuf:"bytes,9,opt,name=versions"`
}

// DeviceAttributeMaxValueLength 是字符串或版本属性值的最大长度
const DeviceAttributeMaxValueLength = 64

// DeviceTaintsMaxLength 是每个设备污点的最大数量
const DeviceTaintsMaxLength = 16

// 该污点所附着设备会对所有不容忍该污点的 claim 产生 "effect" 指定的效果，
// 并通过该 claim 传导到使用它的 Pod
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type DeviceTaint struct {
	// 施加到设备上的污点键
	// 必须是 label 名
	//
	// +required
	Key string `json:"key" protobuf:"bytes,1,name=key"`

	// 与污点键对应的污点值
	// 必须是 label 值
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`

	// 污点对不容忍它的 claim，以及通过这些 claim 对使用它们的 Pod 的效果
	//
	// 合法效果为 None、NoSchedule 和 NoExecute。节点污点使用的 PreferNoSchedule 在此无效。
	// 未来可能增加更多效果
	// 消费者必须将未知效果视为 None
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Effect DeviceTaintEffect `json:"effect" protobuf:"bytes,3,name=effect,casttype=DeviceTaintEffect"`

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
	// 渐进式（ratcheting）处理

	// TimeAdded 表示污点被添加的时间，
	// （仅在 DeviceTaintRule 中）也表示效果被修改的时间
	// 未设置时会在创建或更新时自动填入
	//
	// 此外，在 DeviceTaintRule 中，若更新时提供的值
	// 与旧值相同且新效果不同，则该值会被替换为当前时间
	//
	// 仅修改键和/或值而保持效果不变是允许的，且不会更新时间戳，
	// 因为使用时间戳的驱逐流程要么已开始（NoExecute），要么尚未开始
	//
	// （NoEffect、NoSchedule）
	//
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty" protobuf:"bytes,4,opt,name=timeAdded"`

	// ^^^
	//
	// 节点污点中该字段被定义为"仅对 NoExecute 污点写入"
	// 但实际上 Kubernetes 从未使用它（无校验、无默认值、在 pkg/controller/tainteviction 的 Pod 驱逐中被忽略）
	//
}

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// DeviceTaintEffect 表示设备污点的效果
type DeviceTaintEffect string

const (
	// 无效果，污点仅提供信息
	DeviceTaintEffectNone DeviceTaintEffect = "None"

	// 不允许新的 Pod 调度到使用带污点设备的场景，除非它们容忍该污点；
	// 但允许所有绕过调度器直接提交给 Kubelet 的 Pod 启动，
	// 并允许所有已运行的 Pod 继续运行
	DeviceTaintEffectNoSchedule DeviceTaintEffect = "NoSchedule"

	// 驱逐所有不容忍该设备污点的已运行 Pod
	DeviceTaintEffectNoExecute DeviceTaintEffect = "NoExecute"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// DeviceTaintRule 向所有匹配选择器的设备添加一个污点
// 其效果等同于 DRA 驱动直接在 ResourceSlice 中指定该污点
//
// +k8s:supportsSubresource="/status"
type DeviceTaintRule struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 指定选择器和一个污点
	//
	// 修改 spec 会自动递增 metadata.generation
	// +required
	Spec DeviceTaintRuleSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 提供 spec 中所请求内容的执行状态信息
	//
	// +optional
	Status DeviceTaintRuleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeviceTaintRuleSpec 指定选择器和一个污点
type DeviceTaintRuleSpec struct {
	// DeviceSelector 定义污点施加到哪些设备上
	// 设备必须满足所有选择条件才能
	// 匹配；空选择器匹配所有设备。没有选择器时不匹配任何设备
	//
	// +optional
	DeviceSelector *DeviceTaintSelector `json:"deviceSelector,omitempty" protobuf:"bytes,1,opt,name=deviceSelector"`

	// 施加到匹配设备上的污点
	//
	// +required
	Taint DeviceTaint `json:"taint,omitempty" protobuf:"bytes,2,rep,name=taint"`
}

// DeviceTaintSelector 定义 DeviceTaintRule 作用于哪些设备
// 空选择器匹配所有设备。没有选择器时不匹配任何设备
type DeviceTaintSelector struct {
	// 若设置 DeviceClassName，设备必须满足该类中定义的选择器才会被选中。
	// 本字段对应 class.metadata.name
	//
	// +optional
	//
	// 自 1.35 起废弃（tombstone）：要全面支持该功能，需要把设备属性复制到 ResourceClaim 的分配结果中，
	// 否则驱逐控制器无法计算这些 CEL 表达式
	//
	// DeviceClassName *string `json:"deviceClassName,omitempty" protobuf:"bytes,1,opt,name=deviceClassName"`

	// 若设置 driver，只选择该驱动的设备
	// 本字段对应 slice.spec.driver
	//
	// +optional
	Driver *string `json:"driver,omitempty" protobuf:"bytes,2,opt,name=driver"`

	// 若设置 pool，只选择该池中的设备
	//
	// 同时设置驱动名有助于避免不同驱动使用同一池名时的歧义，
	// 但并非必须，因为跨驱动选择池也可能有用，
	// 例如节点本地设备的驱动常以节点名作为池名
	//
	// +optional
	Pool *string `json:"pool,omitempty" protobuf:"bytes,3,opt,name=pool"`

	// 若设置 device，只选择具有该名称的设备
	// 本字段对应 slice.spec.devices[].name
	//
	// 可能还需要同时设置 driver 和 pool 以避免歧义，但非必须
	//
	// +optional
	Device *string `json:"device,omitempty" protobuf:"bytes,4,opt,name=device"`

	// Selectors 包含与 ResourceClaim 相同的选择条件
	// 目前支持 CEL 表达式，所有选择器都必须满足
	//
	// +optional
	// +listType=atomic
	//
	// 自 1.35 起废弃（tombstone）：要全面支持该功能，需要把设备属性复制到 ResourceClaim 的分配结果中，
	// 否则驱逐控制器无法计算这些 CEL 表达式
	//
	// Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,5,rep,name=selectors"`
}

// DeviceTaintRuleStatus 提供正在进行的 Pod 驱逐的信息
type DeviceTaintRuleStatus struct {
	// Conditions 以机器可读和人类可读的格式，提供 DeviceTaintRule 与集群
	// 在某一时刻的状态信息
	//
	// 本 API 目前定义了以下 condition，未来可能增加：
	//
	// - Type: EvictionInProgress
	// - Status: 当前存在需要驱逐的 Pod 时为 True，否则为 False
	// （包括不会导致驱逐的效果）
	// - Reason: 未指定，可能变化
	// - Message: 包含待驱逐和已驱逐 Pod 数量的信息
	// 采用人类可读格式，定期更新，可能变化
	//
	// 对于 `effect: None`，每次修改 spec 都会设置一次上述 condition，
	// 其 message 描述若效果为 `NoExecute` 会发生什么。
	// 该反馈可用于判断将效果改为 `NoExecute` 是否能达到预期。
	// 它只设置一次，以避免持续更新 status
	//
	// 不得超过 8 项
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,1,rep,name=conditions"`
}

// DeviceTaintRuleStatusMaxConditions 是 DeviceTaintRuleStatus 中 condition 的最大数量
const DeviceTaintRuleStatusMaxConditions = 8

// DeviceTaintConditionEvictionInProgress 是 DeviceTaintRuleStatus 对外公开的 condition 类型
const DeviceTaintConditionEvictionInProgress = "EvictionInProgress"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.37

// DeviceTaintRuleList 是 DeviceTaintRule 的集合
type DeviceTaintRuleList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 DeviceTaintRule 列表
	Items []DeviceTaintRule `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// ResourceSliceList 是 ResourceSlice 的集合
type ResourceSliceList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceSlice 列表
	Items []ResourceSlice `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:supportsSubresource="/status"

// ResourceClaim 描述工作负载对集群资源的访问请求。
// 例如，若工作负载需要具有特定属性的加速设备，
// 就用该对象表达该请求。status 部分记录该 claim 是否已满足以及
// 分配了哪些具体资源
type ResourceClaim struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 描述请求的内容及配置方式
	// spec 不可变
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 描述 claim 是否可用以及已分配的内容
	// +optional
	Status ResourceClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourceClaimSpec 定义 ResourceClaim 请求的内容及配置方式
type ResourceClaimSpec struct {
	// Devices 定义如何请求设备
	//
	// +optional
	Devices DeviceClaim `json:"devices" protobuf:"bytes,1,name=devices"`

	// Controller 自 Kubernetes 1.32 起废弃（tombstone）并移除。
	// 待不再支持解码 v1alpha3 后可复用该字段
	//
	// Controller string `json:"controller,omitempty" protobuf:"bytes,2,opt,name=controller"`
}

// DeviceClaim 定义如何通过 ResourceClaim 请求设备
type DeviceClaim struct {
	// Requests 表示对不同设备的独立请求，所有请求都必须被满足。
	// 为空时无需分配任何资源
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=name
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []DeviceRequest `json:"requests" protobuf:"bytes,1,name=requests"`

	// 分配给该 claim 的设备集合必须满足这些约束
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Constraints []DeviceConstraint `json:"constraints,omitempty" protobuf:"bytes,2,opt,name=constraints"`

	// 该字段保存若干潜在驱动的配置，这些驱动都可能满足本 claim 中的请求。
	// 分配 claim 时会忽略该字段
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClaimConfiguration `json:"config,omitempty" protobuf:"bytes,3,opt,name=config"`

	// 未来可能的扩展，旧调度器会忽略。这是可行的，
	// 因为评分只表达用户偏好，
	// 不会构成硬性要求
	//
	// Score *SomeScoringStruct
}

const (
	// DeviceRequestsMaxSize 是单个 DeviceClaim 中请求的最大数量
	DeviceRequestsMaxSize = AllocationResultsMaxSize
	// DeviceConstraintsMaxSize 是单个 DeviceClaim 中约束的最大数量
	DeviceConstraintsMaxSize = 32
	// DeviceConfigMaxSize 是单个 DeviceClaim 中配置的最大数量
	DeviceConfigMaxSize = 32
)

// DRAAdminNamespaceLabelKey 是用于授予命名空间内某些 resource.k8s.io API 类型管理访问权限的标签键。
// 当命名空间上该标签的值（区分大小写）为 "true" 时，
// 允许在任何命名空间级的 resource.k8s.io API 类型中使用 adminAccess: true。
// 目前该权限适用于
// ResourceClaim 和 ResourceClaimTemplate 对象
const (
	// DRAAdminNamespaceLabelKey 是授予命名空间管理访问权限的标签键
	DRAAdminNamespaceLabelKey = "resource.kubernetes.io/admin-access"
)

// DeviceRequest 表示 claim 所需设备的请求
// 通常是对单个设备资源的请求，也可以请求多个相同设备。
// 通过 FirstAvailable 还可以提供按优先级排列的请求列表
type DeviceRequest struct {
	// Name 可用于在 pod.spec.containers[].resources.claims 条目及 claim 的约束中引用本请求
	//
	// 设置 Exactly 时，通过 DeviceRequest 名称的引用能唯一标识一个请求；
	// 设置 FirstAvailable 时，对 DeviceRequest 名称的引用
	// 会匹配调度器最终选中的任意子请求
	//
	// 必须是 DNS label
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Exactly 指定单个请求的细节，必须完全满足这些细节
	//
	// Exactly 或 FirstAvailable 必须设置一个
	//
	// +optional
	// +oneOf=deviceRequestType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Exactly *ExactDeviceRequest `json:"exactly,omitempty" protobuf:"bytes,2,name=exactly"`

	// FirstAvailable 包含若干子请求，调度器会从中选择一个。
	// 它按此处列出的顺序依次尝试满足，
	// 因此若列表中有两项，只有判定第一项不可用后，
	// 调度器才会检查第二项
	//
	// DRA 尚未实现评分，因此调度器会
	// 选择第一组满足 claim 中所有请求的设备。
	// 若需求可以在多个节点上满足，
	// 则由其他调度特性决定选择哪个节点。
	// 这意味着分配给 claim 的设备集合
	// 可能不是集群中的最优选择。评分功能将在以后实现
	//
	// +optional
	// +oneOf=deviceRequestType
	// +listType=atomic
	// +featureGate=DRAPrioritizedList
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=name
	// +k8s:beta(since: "1.37")=+k8s:maxItems=8
	FirstAvailable []DeviceSubRequest `json:"firstAvailable,omitempty" protobuf:"bytes,3,name=firstAvailable"`
}

// ExactDeviceRequest 表示对一个或多个相同设备的请求
type ExactDeviceRequest struct {
	// DeviceClassName 引用一个具体的 DeviceClass，该类可定义本请求继承的额外配置与选择器
	//
	// DeviceClassName 必填
	//
	// 管理员可以只安装带有允许设备选择器的类，以限制用户可请求的设备。
	// 若允许用户无限制地请求任意资源，
	// 管理员可以创建一个空的 DeviceClass 供用户引用
	//
	// +required
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,1,name=deviceClassName"`

	// Selectors 定义某个设备要被本请求考虑所必须满足的条件，所有选择器都必须满足
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,2,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求。支持的值为：
	//
	// - ExactCount：请求指定数量的设备
	// 这是默认值，具体数量在 count 字段中给出
	//
	// - All：请求池中全部匹配的设备
	// 节点上至少需要一个设备，分配才能成功
	// 若部分设备已被分配，分配将失败，
	// 除非请求了 adminAccess
	//
	// 未指定 AllocationMode 时默认为 ExactCount。若模式为 ExactCount 且未指定 count，默认为 1。
	// 其他子请求必须指定本字段
	//
	// 未来可能增加更多模式。客户端必须拒绝处理未知模式的请求
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,3,opt,name=allocationMode"`

	// Count 仅在模式为 "ExactCount" 时使用，必须大于零
	// 若 AllocationMode 为 ExactCount 且未指定本字段，默认为 1
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,4,opt,name=count"`

	// AdminAccess 表示这是一个请求设备管理员访问权限的 claim。
	// 带 AdminAccess 的 claim 预计用于设备的监控或其他管理服务。
	// 它们在访问模式与资源分配方面
	// 忽略设备上所有普通 claim
	//
	// 未设置或为 false 时禁用管理员访问，否则启用
	//
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,5,opt,name=adminAccess"`

	// 若指定，表示本请求的容忍规则
	//
	// 要分配带 NoSchedule 效果污点的设备，需要对应的容忍规则；NoExecute 同理
	//
	// 此外，若分配后任一设备被加上 NoExecute 污点而未被容忍，
	// 所有消费该 ResourceClaim 的 Pod 都会被删除以完成驱逐。
	// 在 claim 带有这些污点设备期间，调度器不会让新 Pod 预留该 claim。
	// 所有 Pod 被驱逐后，claim 会被解除分配
	//
	// 容忍规则最多 16 条
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// Capacity 定义针对各容量的资源需求
	//
	// 未设置本字段且设备支持多次分配时，将按 requestPolicy 对各容量应用默认值。
	// 没有 requestPolicy 的容量，默认消费全部容量
	//
	// 适用于每一次设备分配
	// 若 Count > 1，
	// 没有足够满足要求的设备时请求失败
	// 若 AllocationMode 为 All，
	// 存在其他方面匹配请求、具备该容量且容量值 >= 请求量，但无法分配给本请求的设备时，请求失败
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,7,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备
	// （例如将 GPU 和 NIC 共同分配到同一 NUMA 节点），即使它们的驱动发布不同属性。
	// 在求值选择器表达式时，派生属性无法通过 CEL 环境中的 `device.attributes` 访问
	//
	// 派生属性允许在调度时从复杂拓扑字符串中提取 NUMA 索引，
	// 或重命名供应商专有属性，从而提取、转换或规范化拓扑信息，
	// 形成统一的虚拟属性名
	// 调度器随后在匹配约束时像对待静态属性一样求值这些虚拟属性
	//
	// 列表中定义的每个派生属性必须被 `.devices.constraints` 列表中至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用
	//
	// 派生属性最多 32 个
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,8,rep,name=derivedAttributes"`
}

// DeviceSubRequest 描述 claim.spec.devices.requests[].firstAvailable 数组中提供的设备请求。
// 每项通常是对单个资源的请求，
// 也可以请求多个相同设备
//
// DeviceSubRequest 与 ExactDeviceRequest 类似，但不暴露 AdminAccess 字段，
// 因为该字段仅在请求特定设备时支持
type DeviceSubRequest struct {
	// Name 可用于在 claim 的约束列表或配置列表中引用本子请求。
	// 引用必须使用 <main request>/<subrequest> 格式
	//
	// 必须是 DNS label
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// DeviceClassName 引用一个具体的 DeviceClass，该类可定义本请求继承的额外配置与选择器
	//
	// 类必填，可用类取决于集群配置
	//
	// 管理员可以只安装带有允许设备选择器的类，以限制用户可请求的设备。
	// 若允许用户无限制地请求任意资源，
	// 管理员可以创建一个空的 DeviceClass 供用户引用
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,2,name=deviceClassName"`

	// Selectors 定义某个设备要被本请求考虑所必须满足的条件，所有选择器都必须满足
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,3,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求。支持的值为：
	//
	// - ExactCount：请求指定数量的设备
	// 这是默认值，具体数量在 count 字段中给出
	//
	// - All：请求池中全部匹配的设备
	// 若部分设备已被分配，分配将失败，
	// 除非请求了 adminAccess
	//
	// 未指定 AllocationMode 时默认为 ExactCount。若模式为 ExactCount 且未指定 count，默认为 1。
	// 其他子请求必须指定本字段
	//
	// 未来可能增加更多模式。客户端必须拒绝处理未知模式的请求
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,4,opt,name=allocationMode"`

	// Count 仅在模式为 "ExactCount" 时使用，必须大于零
	// 若 AllocationMode 为 ExactCount 且未指定本字段，默认为 1
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,5,opt,name=count"`

	// 若指定，表示本请求的容忍规则
	//
	// 要分配带 NoSchedule 效果污点的设备，需要对应的容忍规则；NoExecute 同理
	//
	// 此外，若分配后任一设备被加上 NoExecute 污点而未被容忍，
	// 所有消费该 ResourceClaim 的 Pod 都会被删除以完成驱逐。
	// 在 claim 带有这些污点设备期间，调度器不会让新 Pod 预留该 claim。
	// 所有 Pod 被驱逐后，claim 会被解除分配
	//
	// 容忍规则最多 16 条
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// Capacity 定义针对各容量的资源需求
	//
	// 未设置本字段且设备支持多次分配时，将按 requestPolicy 对各容量应用默认值。
	// 没有 requestPolicy 的容量，默认消费全部容量
	//
	// 适用于每一次设备分配
	// 若 Count > 1，
	// 没有足够满足要求的设备时请求失败
	// 若 AllocationMode 为 All，
	// 存在其他方面匹配请求、具备该容量且容量值 >= 请求量，但无法分配给本请求的设备时，请求失败
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,7,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备
	// （例如将 GPU 和 NIC 共同分配到同一 NUMA 节点），即使它们的驱动发布不同属性。
	// 在求值选择器表达式时，派生属性无法通过 CEL 环境中的 `device.attributes` 访问
	//
	// 派生属性允许在调度时从复杂拓扑字符串中提取 NUMA 索引，
	// 或重命名供应商专有属性，从而提取、转换或规范化拓扑信息，
	// 形成统一的虚拟属性名
	// 调度器随后在匹配约束时像对待静态属性一样求值这些虚拟属性
	//
	// 列表中定义的每个派生属性必须被 `.devices.constraints` 列表中至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用
	//
	// 派生属性最多 32 个
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,8,rep,name=derivedAttributes"`
}

// CapacityRequirements 定义特定设备请求的容量需求
type CapacityRequirements struct {
	// Requests 表示对不同资源的独立设备资源请求，设备必须全部提供
	//
	// 该值用作针对设备上可用容量的额外过滤条件
	// 它在语义上等价于如下 CEL 选择器：
	// `device.capacity[<domain>].<name>.compareTo(quantity(<request quantity>)) >= 0`
	// 例如 device.capacity['test-driver.cdi.k8s.io'].counters.compareTo(quantity('2')) >= 0
	//
	// 定义 requestPolicy 时，请求量会按策略向上调整到最近的合法值
	//
	// 若请求量因超过 requestPolicy 允许的范围而无法调整为合法值，
	// 则该设备被视为不适合分配
	//
	// 对于任何未显式请求的容量：
	// - 未设置 requestPolicy 时，默认消费全部设备容量
	// （即占用整个设备）
	// - 设置 requestPolicy 时，默认消费容量由该策略决定
	//
	// 若设备允许多次分配，
	// 所有请求累计的量不得超过容量值
	// 消费容量（如定义了 requestPolicy 可能已被调整）会记录在
	// resource claim 的 status.devices[*].consumedCapacity 字段中
	//
	// +optional
	Requests map[QualifiedName]resource.Quantity `json:"requests,omitempty" protobuf:"bytes,1,rep,name=requests,castkey=QualifiedName"`
}

const (
	// DeviceSelectorsMaxSize 是设备选择器的最大数量
	DeviceSelectorsMaxSize = 32
	// FirstAvailableDeviceRequestMaxSize 是 FirstAvailable 子请求的最大数量
	FirstAvailableDeviceRequestMaxSize = 8
	// DeviceTolerationsMaxLength 是设备容忍规则的最大数量
	DeviceTolerationsMaxLength = 16
	// DeviceDerivedAttributesMaxSize 是派生属性的最大数量
	DeviceDerivedAttributesMaxSize = 32
)

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// DeviceAllocationMode 表示设备分配模式
type DeviceAllocationMode string

// [DeviceRequest.CountMode] 的合法值
const (
	// DeviceAllocationModeExactCount 表示请求指定数量的设备
	DeviceAllocationModeExactCount = DeviceAllocationMode("ExactCount")
	// DeviceAllocationModeAll 表示请求池中全部匹配设备
	DeviceAllocationModeAll = DeviceAllocationMode("All")
)

// DeviceSelector 必须且只能设置一个字段
type DeviceSelector struct {
	// CEL 包含用于选择设备的 CEL 表达式
	//
	// +optional
	// +oneOf=SelectorType
	CEL *CELDeviceSelector `json:"cel,omitempty" protobuf:"bytes,1,opt,name=cel"`
}

// CELDeviceSelector 包含用于选择设备的 CEL 表达式
type CELDeviceSelector struct {
	// Expression 是对单个设备求值的 CEL 表达式。当被考察设备满足条件时
	// 必须求值为 true，不满足时为 false；
	// 其他结果均视为错误并中止设备分配
	//
	// 表达式的输入是名为 "device" 的对象，具有如下属性：
	//
	// - driver (string)：定义该设备的驱动名
	// - attributes (map[string]object)：设备属性，按前缀分组
	// （例如 device.attributes["dra.example.com"] 得到包含所有以 "dra.example.com" 为前缀属性的对象）
	//
	// - capacity (map[string]object)：设备容量，按前缀分组
	// - allowMultipleAllocations (bool)：设备的 allowMultipleAllocations 属性
	// （v1.34+，需启用 DRAConsumableCapacity 特性）
	//
	// 例如：某设备 driver="dra.example.com"，暴露两个属性 "model" 和 "ext.example.com/family"，
	// 以及一个名为 "modules" 的容量。
	// 该表达式的输入具有以下字段：
	//
	//     device.driver
	//     device.attributes["dra.example.com"].model
	//     device.attributes["ext.example.com"].family
	//     device.capacity["dra.example.com"].modules
	//
	// device.driver 可用于检查特定驱动，
	// 既可以作为高层前置条件（只考虑该驱动的设备），
	// 也可以作为考虑多个驱动设备的复合表达式的一部分
	//
	// 每个属性的值类型由设备定义决定，编写表达式的用户须查阅对应驱动的文档。
	// 每个容量的值类型为 Quantity
	//
	// 在 device.attributes 或 device.capacity 中查找未知前缀会返回空 map。
	// 引用未知字段会导致求值错误并中止分配
	//
	// 健壮的表达式应在引用属性前检查其是否存在
	//
	// 常见错误：
	// - "no such key"：对可选字段使用可选链（.? 后接 orValue()）
	// 或用 has() 做前置检查
	// 详见 CEL Optional Types：
	//   https://pkg.go.dev/github.com/google/cel-go@v0.17.4/cel#OptionalTypes
	//
	// 更多 CEL 表达式语法与示例参见：
	// https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 为方便使用，已启用 cel.bind() 函数，可简化访问同一域多个属性的表达式。例如：
	//
	//     cel.bind(dra, device.attributes["dra.example.com"], dra.someBool && dra.anotherBool)
	//
	// 启用 DRAListTypeAttributes 特性门控后，
	// includes() 辅助函数可用于标量和列表类型属性。
	// 引入它是为了在保持 CEL 表达式简洁的同时，支持从标量属性平滑迁移到列表属性。例如：
	//
	//     device.attributes["dra.example.com"].models.includes("some-model")
	//
	// 表达式长度不得超过 10 Ki。求值成本同样受逻辑步骤估算数限制；
	// 一个 claim 中所有派生属性的总成本共享同一个 CEL 成本预算
	//
	// +required
	Expression string `json:"expression" protobuf:"bytes,1,name=expression"`
}

// DeviceDerivedAttribute 定义通过 CEL 计算的派生属性
type DeviceDerivedAttribute struct {
	// Name 是本派生属性在约束中使用的标识符
	//
	// 必须是 DNS 子域加斜杠（"/"）再加 C 标识符（如 "example.com/numaNode" 或 "derived/numaNode"）
	//
	// 若所选名称与驱动已有的物理属性同名，派生属性的表达式会遮蔽该物理属性，
	// 并在约束中使用其求值结果。
	// 若只想定义仅在 ResourceClaim 内部使用、不遮蔽现有属性的派生属性，
	// 应使用任何 DRA 驱动都不会使用的域前缀
	// （如 "derived/myAttribute"）
	//
	// 未被至少一个约束使用的派生属性是无效的
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-resource-fully-qualified-name
	Name FullyQualifiedName `json:"name" protobuf:"bytes,1,name=name"`

	// Expression 是对每个候选设备求值的 CEL 表达式
	// 表达式必须求值为基本标量（string、integer、boolean 或 semver）或这些标量的列表
	// （[]string、[]int64、[]bool、[]semver），作为虚拟分组键
	// 其他返回类型均视为错误，导致该设备的 CEL 求值失败
	//
	// 表达式的输入是名为 "device" 的对象，其属性与 CELDeviceSelector 中相同
	//
	// 当 Pod 调度过程中某些设备出现 CEL 运行时错误（如查找未定义的属性）时，
	// 会中止分配并使 Pod 调度失败。
	// 立即暴露求值错误，可避免极难察觉的静默拓扑匹配失败。
	// 例如，健壮的表达式应在引用属性前检查其存在性，
	// 以避免运行时求值错误
	//
	// 表达式在设备通过本请求中其他选择器表达式之后求值，
	// 因此可以针对具体被请求设备编写表达式
	// （例如假定设备来自某供应商而跳过相应检查）
	//
	// 表达式长度不得超过 10 Ki。求值成本同样受逻辑步骤估算数限制；
	// 一个 claim 中所有派生属性的总成本共享同一个 CEL 成本预算
	//
	// +required
	// +k8s:required
	Expression string `json:"expression" protobuf:"bytes,2,name=expression"`
}

// CELSelectorExpressionMaxCost 指定单次 CEL 选择器求值的成本上限
//
// 选择设备没有总体预算，因此实际耗时与 CEL 选择器数量及求值频率成正比，
// 而后两者取决于设备数量、集群利用率和附加约束等因素
//
// 只有在首次设置表达式或修改表达式时，才会按此上限和 [CELSelectorExpressionMaxLength] 校验。
// 若未来 Kubernetes 版本调整上限，现有用户可确保其已有表达式仍然有效
//
// 但 kube-scheduler 在运行时同样施加该成本上限，因此升级或降级后，
// 原本合法的表达式可能在运行时失败。
// 无版本差异时也可能发生：成本估算低于实际成本。
// 正因如此，kube-scheduler 选择在运行时强制该上限，而不只依赖校验
//
// 根据
// https://github.com/kubernetes/kubernetes/blob/4aeaf1e99e82da8334c0d6dddd848a194cd44b4f/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go#L20-L22,
// 每次表达式求值大约需要 0.1 秒
// 但这取决于机器性能
const CELSelectorExpressionMaxCost = 1000000

// DeviceClaimDerivedAttributeCELMaxCost 是单个 DeviceClaim 中所有派生属性 CEL 表达式允许的最大合计执行成本
//
// 校验期间，API server 会计算每个派生属性表达式的估算执行成本。
// claim 中所有请求的这些成本之和不得超过该预算，否则 claim 会被拒绝
//
// 为控制在预算内，使用者应简化 CEL 表达式，
// 避免深层嵌套或迭代（如 `.all()` 或 `.exists()`）等高开销操作，
// 并尽量减少 claim 中派生属性的总数
//
// 该共享预算可防止过于复杂的 claim 降低 kube-scheduler 的性能。
// 上限设为 1,000,000 条指令执行（约 0.1 秒求值时间），
// 使所有派生属性的合计成本与单个 CEL 选择器允许的最大成本一致
//
// DeviceClaimDerivedAttributeCELMaxCost 是单个 DeviceClaim 中所有派生属性表达式的合计成本上限
const DeviceClaimDerivedAttributeCELMaxCost = 1000000

// CELSelectorExpressionMaxLength 是 CEL 选择器表达式字符串的最大长度
const CELSelectorExpressionMaxLength = 10 * 1024

// 除 Requests 外，DeviceConstraint 必须且只能设置一个字段
type DeviceConstraint struct {
	// Requests 列出本 claim 中必须共同满足该约束的一个或多个请求。
	// 若某请求由多个设备满足，则所有这些设备都必须满足该约束。
	// 未指定时，约束适用于 claim 中所有请求
	//
	// 对子请求的引用必须包含主请求名，并可采用 <main request>[/<subrequest>] 格式附带子请求名。
	// 引用子请求必须包含主请求名，并可附带 <main request>[/<subrequest>] 格式的子请求名。
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,1,opt,name=requests"`

	// MatchAttribute 要求所有相关设备都具备该属性，且属性值类型和取值在这些设备上相同
	//
	// 例如指定 "dra.example.com/numa"（假设的例子），则只会选择同一 NUMA 节点上的设备。
	// 没有该属性的设备不会被选中。
	// 所有设备原则上应为该属性使用相同的值类型，
	// 因为这是属性规范的一部分；若某设备不同，它同样不会被选中
	//
	// 启用 DRAListTypeAttributes 特性门控后，比较使用集合语义（忽略元素顺序与重复）：
	// 列表类型属性在所有设备上的交集非空时即视为匹配。
	// 为向后兼容，标量值视为单元素列表
	//
	// 必须包含域限定符
	//
	// +optional
	// +oneOf=ConstraintType
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	MatchAttribute *FullyQualifiedName `json:"matchAttribute,omitempty" protobuf:"bytes,2,opt,name=matchAttribute"`

	// 未来可能的扩展，不属于当前设计：
	// 一个比较不同设备并在匹配时返回 true 的 CEL 表达式
	//
	// 由于它属于 one-of 字段，旧调度器不会因不了解该匹配条件而意外忽略它
	//
	// MatchExpression string

	// DistinctAttribute 要求所有相关设备都具备该属性，且属性值类型和取值在这些设备上互不相同
	//
	// 启用 DRAListTypeAttributes 特性门控后，比较使用集合语义（忽略元素顺序与重复）：
	// 列表类型属性在各设备之间必须两两不相交
	// 为向后兼容，标量值视为单元素集合
	//
	// 它与 MatchAttribute 互为反向
	//
	// 该约束通过属性层面的区分，避免把多个请求分配到同一个设备上
	//
	// 适用于资源请求必须由不同物理设备满足的场景
	// 例如容器请求两个网络接口，必须分别来自两块不同的物理网卡
	//
	// +optional
	// +oneOf=ConstraintType
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	DistinctAttribute *FullyQualifiedName `json:"distinctAttribute,omitempty" protobuf:"bytes,3,opt,name=distinctAttribute"`
}

// DeviceClaimConfiguration 用于 DeviceClaim 中的配置参数
type DeviceClaimConfiguration struct {
	// Requests 列出该配置适用的请求名称
	// 为空时适用于所有请求
	//
	// 对子请求的引用必须包含主请求名，并可采用 <main request>[/<subrequest>] 格式附带子请求名。
	// 引用子请求必须包含主请求名，并可附带 <main request>[/<subrequest>] 格式的子请求名。
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,1,opt,name=requests"`

	DeviceConfiguration `json:"" protobuf:"bytes,2,name=deviceConfiguration"`
}

// DeviceConfiguration 必须且只能设置一个字段。它会被内联嵌入其他含额外字段的结构体，
// 因此字段名不得与那些字段冲突
type DeviceConfiguration struct {
	// Opaque 提供驱动专有的配置参数
	//
	// +optional
	// +oneOf=ConfigurationType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Opaque *OpaqueDeviceConfiguration `json:"opaque,omitempty" protobuf:"bytes,1,opt,name=opaque"`
}

// OpaqueDeviceConfiguration 包含由驱动供应商定义格式的配置参数
type OpaqueDeviceConfiguration struct {
	// Driver 用于确定需要接收这些配置参数的 kubelet 插件
	//
	// 驱动开发者提供的准入策略可用它判断是否需要校验这些参数
	//
	// 必须是 DNS 子域，且最好以驱动供应商拥有的 DNS 域结尾，
	// 并且只使用小写字符
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Parameters 可包含任意数据。校验与版本管理由驱动开发者负责，
	// 通常包括自标识与版本号（Kubernetes 类型使用 "kind" + "apiVersion"），
	// 以及不同版本之间的转换
	//
	// 原始数据长度不得超过 10 Ki
	//
	// +required
	Parameters runtime.RawExtension `json:"parameters" protobuf:"bytes,2,name=parameters"`
}

// OpaqueParametersMaxLength 是 [OpaqueDeviceConfiguration.Parameters] 字段中原始数据的最大长度
//
// OpaqueParametersMaxLength 是不透明配置参数原始数据的最大长度
const OpaqueParametersMaxLength = 10 * 1024

// DeviceToleration 所附着的 ResourceClaim 容忍任何与三元组 <key,value,effect> 匹配且符合指定 operator 的污点
type DeviceToleration struct {
	// Key 是容忍规则适用的污点键。空表示匹配所有污点键
	// 若键为空，operator 必须为 Exists，此组合表示匹配所有值和所有键
	// 必须是 label 名
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-label-key
	Key string `json:"key,omitempty" protobuf:"bytes,1,opt,name=key"`

	// Operator 表示键与值的关系
	// 合法值为 Exists 和 Equal，默认为 Equal
	// Exists 相当于值的通配符，使 ResourceClaim 能容忍某一类别的所有污点
	//
	// +optional
	// +default="Equal"
	// +k8s:beta(since: "1.37")=+k8s:optional
	Operator DeviceTolerationOperator `json:"operator,omitempty" protobuf:"bytes,2,opt,name=operator,casttype=DeviceTolerationOperator"`

	// Value 是容忍规则匹配的污点值
	// operator 为 Exists 时值必须为空，否则为普通字符串
	// 必须是 label 值
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,3,opt,name=value"`

	// Effect 表示要匹配的污点效果。空表示匹配所有污点效果
	// 指定时，允许值为 NoSchedule 和 NoExecute
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Effect DeviceTaintEffect `json:"effect,omitempty" protobuf:"bytes,4,opt,name=effect,casttype=DeviceTaintEffect"`

	// TolerationSeconds 表示容忍规则容忍污点的时长（效果必须为 NoExecute，否则忽略该字段）。
	// 默认情况下
	// 未设置该字段，表示永久容忍（不驱逐）。零值和
	// 负值会被系统视为 0（立即驱逐）
	// 大于零时，Pod 需要被驱逐的时间计算为 <污点添加时间> + <容忍秒数>
	//
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty" protobuf:"varint,5,opt,name=tolerationSeconds"`
}

// 容忍操作符是可在容忍规则中使用的 operator 集合
//
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// DeviceTolerationOperator 表示设备容忍操作符
type DeviceTolerationOperator string

const (
	// DeviceTolerationOpExists 表示容忍规则匹配所有值（Exists 操作符）
	DeviceTolerationOpExists DeviceTolerationOperator = "Exists"
	// DeviceTolerationOpEqual 表示容忍规则按值精确匹配（Equal 操作符）
	DeviceTolerationOpEqual DeviceTolerationOperator = "Equal"
)

// ResourceClaimStatus 记录资源是否已分配以及分配结果
type ResourceClaimStatus struct {
	// claim 分配成功后设置 Allocation
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:update=NoModify
	Allocation *AllocationResult `json:"allocation,omitempty" protobuf:"bytes,1,opt,name=allocation"`

	// ReservedFor 表示当前允许哪些实体使用该 claim。引用了未为本 Pod 预留的 ResourceClaim 的 Pod 不会启动。
	// 已被使用或因预留而可能正在使用的 claim 不得被解除分配
	//
	// 在有多个调度器实例的集群中，两个 Pod 可能被不同调度器并发调度。
	// 若它们引用同一个已达到最大消费者数的 ResourceClaim，只有一个 Pod 能成功调度
	//
	// 两个调度器都会尝试把各自的 Pod 加入 claim.status.reservedFor，
	// 但只有先到达 API server 的更新会被保存。
	// 另一个更新会失败，发起它的调度器据此知道
	// 必须把 Pod 放回队列，
	// 等待 ResourceClaim 再次可用
	//
	// 此类预留最多 256 个，未来可能增加，但不会减少
	//
	// +optional
	// +listType=map
	// +listMapKey=uid
	// +patchStrategy=merge
	// +patchMergeKey=uid
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:listType=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=uid
	// +k8s:beta(since: "1.37")=+k8s:maxItems=256
	ReservedFor []ResourceClaimConsumerReference `json:"reservedFor,omitempty" protobuf:"bytes,2,opt,name=reservedFor" patchStrategy:"merge" patchMergeKey:"uid"`

	// DeallocationRequested 自 Kubernetes 1.32 起废弃（tombstone）并移除。
	// 待不再支持解码 v1alpha3 后可复用该字段
	//
	// DeallocationRequested bool `json:"deallocationRequested,omitempty" protobuf:"bytes,3,opt,name=deallocationRequested"`

	// Devices 保存驱动上报的、为本 claim 分配的每个设备的状态，
	// 其中可包含驱动专有信息
	// 各条目由对应的驱动负责维护
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +listType=map
	// +listMapKey=driver
	// +listMapKey=device
	// +listMapKey=pool
	// +listMapKey=shareID
	// +featureGate=DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:listType=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=driver
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=device
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=pool
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=shareID
	Devices []AllocatedDeviceStatus `json:"devices,omitempty" protobuf:"bytes,4,opt,name=devices"`
}

// ResourceClaimReservedForMaxSize 是 claim.status.reservedFor 中条目的最大数量
//
// ResourceClaimReservedForMaxSize 是 claim.status.reservedFor 的最大条目数
const ResourceClaimReservedForMaxSize = 256

// ResourceClaimConsumerReference 提供定位 ResourceClaim 消费者所需的足够信息。
// 消费者必须是与 ResourceClaim 同一命名空间中的资源
type ResourceClaimConsumerReference struct {
	// APIGroup 是被引用资源所属的组。核心 API 中为空，
	// 与创建资源时使用的 APIVersion 中的组一致
	//
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
	// Resource 是被引用资源的类型，例如 "pods"
	// +required
	Resource string `json:"resource" protobuf:"bytes,3,name=resource"`
	// Name 是被引用资源的名称
	// +required
	Name string `json:"name" protobuf:"bytes,4,name=name"`
	// UID 唯一标识该资源的一次实例
	// +required
	UID types.UID `json:"uid" protobuf:"bytes,5,name=uid"`
}

// AllocationResult 包含已分配资源的属性
type AllocationResult struct {
	// Devices 是设备分配的结果
	//
	// +optional
	Devices DeviceAllocationResult `json:"devices,omitempty" protobuf:"bytes,1,opt,name=devices"`

	// NodeSelector 定义已分配资源在哪些节点可用。未设置时表示所有节点均可用
	//
	// +optional
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,3,opt,name=nodeSelector"`

	// Controller 自 Kubernetes 1.32 起废弃（tombstone）并移除。
	// 待不再支持解码 v1alpha3 后可复用该字段
	//
	// Controller string `json:"controller,omitempty" protobuf:"bytes,4,opt,name=controller"`

	// AllocationTimestamp 记录资源分配的时间
	// 该字段不保证被设置，此时分配时间未知
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	AllocationTimestamp *metav1.Time `json:"allocationTimestamp,omitempty" protobuf:"bytes,5,opt,name=allocationTimestamp"`
}

// DeviceAllocationResult 是设备分配的结果
type DeviceAllocationResult struct {
	// Results 列出所有已分配设备
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Results []DeviceRequestAllocationResult `json:"results,omitempty" protobuf:"bytes,1,opt,name=results"`

	// 该字段是 claim 与 class 中所有配置参数的合并结果
	// 驱动可根据一个标志区分两者
	//
	// 它也包含那些在结果中没有已分配设备的驱动的配置参数，
	// 因为支持哪些配置参数由驱动自行决定。
	// 驱动可以静默忽略未知配置参数
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=64
	Config []DeviceAllocationConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`
}

// AllocationResultsMaxSize 表示 allocation.devices.results 中的最大条目数
//
// AllocationResultsMaxSize 是分配结果中设备的最大数量
const AllocationResultsMaxSize = 32

// DeviceRequestAllocationResult 包含一个请求的设备分配结果
type DeviceRequestAllocationResult struct {
	// Request 是导致该设备被分配的 claim 中的请求名称。
	// 若它引用 DeviceRequest 的 firstAvailable 列表中的子请求，
	// 则本字段必须同时包含主请求名和子请求名
	//
	// 一个请求可能分配到多个设备
	//
	// +required
	Request string `json:"request" protobuf:"bytes,1,name=request"`

	// Driver 指定 DRA 驱动的名称，当 claim 需要在某节点上使用时，
	// 应调用该驱动的 kubelet 插件处理此次分配
	//
	// 必须是 DNS 子域，且最好以驱动供应商拥有的 DNS 域结尾，
	// 并且只使用小写字符
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	Driver string `json:"driver" protobuf:"bytes,2,name=driver"`

	// 本名称与驱动名、设备名共同标识被分配的设备
	//
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-pool-name
	Pool string `json:"pool" protobuf:"bytes,3,name=pool"`

	// Device 通过设备在驱动资源池中的名称引用一个设备实例，必须是 DNS label
	//
	// +required
	Device string `json:"device" protobuf:"bytes,4,name=device"`

	// AdminAccess 表示该设备是为管理员访问而分配的，定义见对应的请求字段
	//
	// 未设置或为 false 时禁用管理员访问，否则启用
	//
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,5,opt,name=adminAccess"`

	// 分配设备时请求中所有容忍规则的副本
	//
	// 容忍规则最多 16 条
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// BindingConditions 是分配时对应 ResourceSlice 中 BindingConditions 的副本
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,7,rep,name=bindingConditions"`

	// BindingFailureConditions 是分配时对应 ResourceSlice 中 BindingFailureConditions 的副本
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus 特性门控
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,8,rep,name=bindingFailureConditions"`

	// ShareID 唯一标识设备的一次独立分配份额，用于设备支持多个并发分配的场景。
	// 它作为额外的 map 键，区分同一设备的不同并发份额
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *types.UID `json:"shareID,omitempty" protobuf:"bytes,9,opt,name=shareID"`

	// ConsumedCapacity 记录作为 claim 请求一部分、每台设备消费的容量
	// 消费量可能与请求量不同：如适用，会按设备的 requestPolicy 向上取整到最近的合法值
	//
	// 每台设备的消费容量总和不得超过 DeviceCapacity 的 Value
	//
	// 仅对允许多次分配的设备填充该字段
	// 所有容量条目都会被包含，即使消费量为零
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	ConsumedCapacity map[QualifiedName]resource.Quantity `json:"consumedCapacity,omitempty" protobuf:"bytes,10,rep,name=consumedCapacity"`

	// SkipNodeOperations 列出此已分配设备跳过哪些节点本地资源操作（gRPC 调用）。
	// 其规则与 ResourceSlice 中的同名字段一致：若某 claim 中某驱动的所有已分配设备都跳过某操作，
	// 则该 gRPC 调用会被跳过。该字段是分配时 ResourceSlice.spec.skipNodeOperations 的副本
	//
	// +optional
	// +listType=set
	// +k8s:listType=set
	// +featureGate=DRAOptionalNodeOperations
	// +k8s:optional
	SkipNodeOperations []SkipNodeOperation `json:"skipNodeOperations,omitempty" protobuf:"bytes,11,rep,name=skipNodeOperations,casttype=SkipNodeOperation"`
}

// DeviceAllocationConfiguration 被嵌入 AllocationResult
type DeviceAllocationConfiguration struct {
	// Source 记录配置是来自 class（普通用户无法设置），还是来自 claim
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Source AllocationConfigSource `json:"source" protobuf:"bytes,1,name=source"`

	// Requests 列出该配置适用的请求名称
	// 为空时适用于所有请求
	//
	// 对子请求的引用必须包含主请求名，并可采用 <main request>[/<subrequest>] 格式附带子请求名。
	// 引用子请求必须包含主请求名，并可附带 <main request>[/<subrequest>] 格式的子请求名。
	// 若只给出主请求名，则配置/约束适用于其所有子请求
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,2,opt,name=requests"`

	DeviceConfiguration `json:"" protobuf:"bytes,3,name=deviceConfiguration"`
}

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// AllocationConfigSource 表示设备配置的来源
type AllocationConfigSource string

// [DeviceAllocationConfiguration.Source] 的合法值
const (
	// AllocationConfigSourceClass 表示配置来自 DeviceClass
	AllocationConfigSourceClass AllocationConfigSource = "FromClass"
	// AllocationConfigSourceClaim 表示配置来自 ResourceClaim
	AllocationConfigSourceClaim AllocationConfigSource = "FromClaim"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// ResourceClaimList 是 claim 的集合
type ResourceClaimList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 resource claim 列表
	Items []ResourceClaim `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// DeviceClass 是由供应商或管理员提供的资源，包含设备配置与选择器。
// 可在 claim 的设备请求中引用以套用这些预设
//
// 集群作用域
type DeviceClass struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义可分配的内容及配置方式
	//
	// 本字段可变。消费者必须做好类随时被更新或替换的准备。
	// claim 的分配只在分配发生时依据类中当时的设置执行一次
	//
	// 修改 spec 会自动递增 metadata.generation
	// +optional
	Spec DeviceClassSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// DeviceClassSpec 用于 [DeviceClass] 中，定义可分配的内容及配置方式
type DeviceClassSpec struct {
	// 通过本类申请的设备必须满足每个选择器
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,1,opt,name=selectors"`

	// Config 定义适用于通过本类申请的每个设备的配置参数
	// 某些类可能由多个驱动满足，因此每个供应商配置实例只作用于一个驱动
	//
	// 这些配置会传给驱动，但在分配 claim 时不予考虑
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClassConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`

	// SuitableNodes 自 Kubernetes 1.32 起废弃（tombstone）并移除。
	// 待不再支持解码 v1alpha3 后可复用该字段
	//
	// SuitableNodes *v1.NodeSelector `json:"suitableNodes,omitempty" protobuf:"bytes,3,opt,name=suitableNodes"`

	// ExtendedResourceName 是本类设备的扩展资源名称
	// 本类设备可用于满足 Pod 的扩展资源请求
	// 格式与 Pod 扩展资源名相同
	// 在集群所有设备类中应保持唯一
	// 若两个设备类同名，则后创建的类被用于满足 Pod 的扩展资源请求
	//
	// 若两个类同时创建，则选择名称字典序较小的类
	//
	// +optional
	// +featureGate=DRAExtendedResource
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-extended-resource-name
	ExtendedResourceName *string `json:"extendedResourceName,omitempty" protobuf:"bytes,4,opt,name=extendedResourceName"`
}

// DeviceClassConfiguration 用于 DeviceClass
type DeviceClassConfiguration struct {
	DeviceConfiguration `json:"" protobuf:"bytes,1,opt,name=deviceConfiguration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// DeviceClassList 是设备类的集合
type DeviceClassList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是资源类列表
	Items []DeviceClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// ResourceClaimTemplate 用于生成 ResourceClaim 对象
type ResourceClaimTemplate struct {
	metav1.TypeMeta `json:""`
	// 对象的标准元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 描述将要生成的 ResourceClaim
	//
	// 本字段不可变。控制平面会在需要时为 Pod 创建 ResourceClaim，之后不再更新
	//
	// +optional
	Spec ResourceClaimTemplateSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// ResourceClaimTemplateSpec 包含 ResourceClaim 的元数据与字段
type ResourceClaimTemplateSpec struct {
	// ObjectMeta 可包含创建时复制到 ResourceClaim 的标签和注解
	// 不允许其他字段，校验时会拒绝
	//
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ResourceClaim 的 Spec。全部内容原样复制到由该模板创建的 ResourceClaim 中
	// ResourceClaim 中合法的字段在这里同样合法
	//
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.34

// ResourceClaimTemplateList 是 claim 模板的集合
type ResourceClaimTemplateList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 resource claim 模板列表
	Items []ResourceClaimTemplate `json:"items" protobuf:"bytes,2,rep,name=items"`
}

const (
	// AllocatedDeviceStatusMaxConditions 表示设备状态中 condition 的最大数量
	//
	AllocatedDeviceStatusMaxConditions int = 8
	// AllocatedDeviceStatusDataMaxLength 表示设备状态中 Data 字段原始数据的最大长度
	//
	AllocatedDeviceStatusDataMaxLength int = 10 * 1024
	// NetworkDeviceDataMaxIPs 表示设备状态中 networkData 字段中 IP 地址的最大数量
	//
	NetworkDeviceDataMaxIPs int = 16
	// NetworkDeviceDataInterfaceNameMaxLength 表示设备状态中 networkData.interfaceName 字段的最大字符数
	//
	NetworkDeviceDataInterfaceNameMaxLength int = 256
	// NetworkDeviceDataHardwareAddressMaxLength 表示设备状态中 networkData.hardwareAddress 字段的最大字符数
	//
	NetworkDeviceDataHardwareAddressMaxLength int = 128
)

// AllocatedDeviceStatus 保存已分配设备的状态（如果驱动选择上报），可包含驱动专有信息
//
// Driver、Pool、Device 和 ShareID 的组合必须与 Status.Allocation.Devices 中对应的键一致
type AllocatedDeviceStatus struct {
	// Driver 指定 DRA 驱动的名称，当 claim 需要在某节点上使用时，
	// 应调用该驱动的 kubelet 插件处理此次分配
	//
	// 必须是 DNS 子域，且最好以驱动供应商拥有的 DNS 域结尾，
	// 并且只使用小写字符
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,rep,name=driver"`

	// 本名称与驱动名、设备名共同标识被分配的设备
	//
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域
	//
	// +required
	Pool string `json:"pool" protobuf:"bytes,2,rep,name=pool"`

	// Device 通过设备在驱动资源池中的名称引用一个设备实例，必须是 DNS label
	//
	// +required
	Device string `json:"device" protobuf:"bytes,3,rep,name=device"`

	// ShareID 唯一标识设备的一次独立分配份额
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *string `json:"shareID,omitempty" protobuf:"bytes,7,opt,name=shareID"`

	// Conditions 保存对设备状态的最新观测
	// 若设备已按类和 claim 的配置引用完成配置，`Ready` condition 应为 True
	//
	// 不得超过 8 项
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions" protobuf:"bytes,4,opt,name=conditions"`

	// Data 包含任意驱动专有数据
	//
	// 原始数据长度不得超过 10 Ki
	//
	// +optional
	Data *runtime.RawExtension `json:"data,omitempty" protobuf:"bytes,5,opt,name=data"`

	// NetworkData 包含该设备特有的网络相关信息
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	NetworkData *NetworkDeviceData `json:"networkData,omitempty" protobuf:"bytes,6,opt,name=networkData"`
}

// NetworkDeviceData 提供已分配设备的网络相关细节
// 这些信息可由驱动或其他组件填写，用于在网络环境中配置或标识该设备
type NetworkDeviceData struct {
	// InterfaceName 指定与已分配设备关联的网络接口名称。
	// 它可以是 Pod 中正在配置的物理或虚拟网络接口名
	//
	// 长度不得超过 256 字节
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=256
	InterfaceName string `json:"interfaceName,omitempty" protobuf:"bytes,1,opt,name=interfaceName"`

	// IPs 列出分配给设备网络接口的网络地址，可包含 IPv4 和 IPv6 地址。
	// 地址为 CIDR 格式，同时包含地址和子网掩码
	//
	// 例如 IPv4 的 "192.0.2.5/24" 和 IPv6 的 "2001:db8::5/64"
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=16
	IPs []string `json:"ips,omitempty" protobuf:"bytes,2,opt,name=ips"`

	// HardwareAddress 表示设备网络接口的硬件地址（如 MAC 地址）
	//
	// 长度不得超过 128 字节
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=128
	HardwareAddress string `json:"hardwareAddress,omitempty" protobuf:"bytes,3,opt,name=hardwareAddress"`
}
