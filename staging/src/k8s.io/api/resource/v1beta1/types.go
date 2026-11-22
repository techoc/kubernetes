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

package v1beta1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// Finalizer 是给通过内置控制器分配的声明设置的终结器。
	// 该值保留给 Kubernetes 使用，DRA 驱动控制器必须使用自己的终结器。
	Finalizer = "resource.kubernetes.io/delete-protection"
	// ExtendedResourceClaimAnnotation 是加在生成的特殊 ResourceClaim 上的注解。
	// 其唯一合法值为 "true"，且仅在调度器内部使用。
	ExtendedResourceClaimAnnotation = "resource.kubernetes.io/extended-resource-claim"
	// PodResourceClaimAnnotation 是 ResourceClaim 控制器在模板生成的 ResourceClaim 上
	// 设置的注解，值为生成该声明的 pod.spec.resourceClaims[].name。
	PodResourceClaimAnnotation = "resource.kubernetes.io/pod-claim-name"
	// ResourceDeviceClassPrefix 是设备类前缀，当设备类未指定 ExtendedResourceName 字段时，
	// 用于生成隐式的扩展资源名。生成的名称为：本前缀 + 设备类名。
	// 生成的名称可能不是 pod.Spec.Resources.Requests 中合法的扩展资源名；
	// 这种情况下必须在设备类中显式指定合法名称。
	ResourceDeviceClassPrefix string = "deviceclass.resource.kubernetes.io/"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// ResourceSlice 表示由同一个驱动管理的、同类资源池中的一项或多项资源。
// 一个池可以横跨多个 ResourceSlice，具体由多少个 ResourceSlice 构成一个池由驱动决定。
//
// 目前仅支持带属性与容量的设备。同一池中的每个设备，无论分布在多少个 ResourceSlice 中，
// 名称都必须唯一。设备被发布在哪个 ResourceSlice 中可能随时间变化，
// 设备的唯一标识是 <驱动名>、<池名>、<设备名> 三元组。
//
// 驱动每次需要更新池时，都会递增 pool.Spec.Pool.Generation，并用新的编号与资源定义
// 更新所有 ResourceSlice。消费者只能使用 generation 最大的 ResourceSlice，忽略其余的。
//
// 在分配池中符合特定条件的全部资源，或在多个备选方案中寻找最佳方案时，
// 消费者应检查池中的 ResourceSlice 数量（每个 ResourceSlice 中都包含该数值），
// 以判断自己对池的视图是否完整；如果不完整，应等待驱动完成池的更新。
//
// 对于非节点本地的资源，不会设置节点名；驱动可以通过节点选择器指明设备在哪些节点可用。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation 特性门控。
type ResourceSlice struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 包含驱动发布的资源信息。
	//
	// 修改 spec 会自动递增 metadata.generation。
	// +required
	Spec ResourceSliceSpec `json:"spec" protobuf:"bytes,2,name=spec"`
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

// ResourceSliceSpec 包含驱动在某个 ResourceSlice 中发布的资源信息。
type ResourceSliceSpec struct {
	// Driver 标识提供容量信息的 DRA 驱动。
	// 可以使用字段选择器只列出具有特定驱动名的 ResourceSlice 对象。
	//
	// 必须是 DNS 子域，并且最好以驱动供应商拥有的 DNS 域结尾，
	// 只应使用小写字符。本字段不可变。
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Pool 描述本 ResourceSlice 所属的池。
	//
	// +required
	Pool ResourcePool `json:"pool" protobuf:"bytes,2,name=pool"`

	// NodeName 标识在本池中提供资源的节点。
	// 可以使用字段选择器只列出属于某个节点的 ResourceSlice 对象。
	//
	// 该字段可用于限制节点只能访问节点名相同的 ResourceSlice，
	// 同时也向自动扩缩容组件表明：增加与某个旧节点同类型的新节点，
	// 可能也会带来新的资源。
	//
	// NodeName、NodeSelector、AllNodes 与 PerDeviceNodeSelection 必须恰好设置一个。
	// 本字段不可变。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// NodeSelector 定义当池不限于单个节点时，哪些节点可以访问池中的资源。
	//
	// 必须恰好使用一个 term。
	//
	// NodeName、NodeSelector、AllNodes 与 PerDeviceNodeSelection 必须恰好设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,4,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问池中的资源。
	//
	// NodeName、NodeSelector、AllNodes 与 PerDeviceNodeSelection 必须恰好设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	AllNodes bool `json:"allNodes,omitempty" protobuf:"bytes,5,opt,name=allNodes"`

	// Devices 列出本池中的部分或全部设备。
	//
	// 条目不得超过 128 个；如果任一设备使用污点或消耗计数器，上限为 64。
	//
	// 在同一个 ResourceSlice 中，Devices 与 SharedCounters 只能设置其中一个。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +zeroOrOneOf=ResourceSliceType
	Devices []Device `json:"devices,omitempty" protobuf:"bytes,6,name=devices"`

	// PerDeviceNodeSelection 定义节点对池中资源的访问是在 ResourceSlice 层级设置，
	// 还是在每个设备上单独设置。设为 true 时，ResourceSlice 中定义的每个设备
	// 都必须单独指定该设置。
	//
	// NodeName、NodeSelector、AllNodes 与 PerDeviceNodeSelection 必须恰好设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	// +featureGate=DRAPartitionableDevices
	PerDeviceNodeSelection *bool `json:"perDeviceNodeSelection,omitempty" protobuf:"bytes,7,name=perDeviceNodeSelection"`

	// SharedCounters 定义计数器集合的列表，每个集合都有名称及可用的计数器列表。
	//
	// 计数器集合的名称在 ResourcePool 内必须唯一。
	//
	// 在同一个 ResourceSlice 中，Devices 与 SharedCounters 只能设置其中一个。
	//
	// 计数器集合的最大数量为 8。
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

	// PartitionTypeAttribute 指定一个字符串类型的设备属性（使用全限定名，
	// 例如 "gpu.example.com/profile"），其值用于标记每台设备的分区类型，
	// 例如 MIG 风格 GPU 的 "Full" 或 "Half"。
	//
	// 设置后，分片中每个可分区设备都必须带有该属性，且共享同一属性值的设备
	// 必须共享相同的 ConsumesCounters 开销。
	//
	// +optional
	// +featureGate=DRAPartitionableDevicesType
	// +k8s:ifDisabled(DRAPartitionableDevicesType)=+k8s:forbidden
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:optional
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:format=k8s-resource-fully-qualified-name
	PartitionTypeAttribute *FullyQualifiedName `json:"partitionTypeAttribute,omitempty" protobuf:"bytes,9,opt,name=partitionTypeAttribute"`

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，本分片中设备将跳过的
	// 节点本地资源操作（gRPC 调用）。如果某驱动在声明中的所有已分配设备都跳过某操作，
	// 该 gRPC 调用就会被跳过。合法取值为：
	//
	// - "NodePrepareResources"：跳过 NodePrepareResources gRPC 调用。除非同时列出
	//   "NodeUnprepareResources"（或指定 "*"），否则不能指定该值。
	// - "NodeUnprepareResources"：跳过 NodeUnprepareResources gRPC 调用。
	// - "*"：跳过所有节点本地资源操作。
	//
	// 未来可能增加其他取值，kubelet 必须忽略未知取值。
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
// SkipNodeOperation 表示节点本地资源操作的类型。
type SkipNodeOperation string

const (
	// SkipNodeOperationNodePrepareResources 表示跳过 NodePrepareResources gRPC 调用。
	SkipNodeOperationNodePrepareResources SkipNodeOperation = "NodePrepareResources"

	// SkipNodeOperationNodeUnprepareResources 表示跳过 NodeUnprepareResources gRPC 调用。
	SkipNodeOperationNodeUnprepareResources SkipNodeOperation = "NodeUnprepareResources"

	// SkipNodeOperationAll 表示跳过所有节点本地资源操作。
	SkipNodeOperationAll SkipNodeOperation = "*"
)

// CounterSet 定义一个具名的计数器集合，可供 ResourcePool 中定义的设备使用。
//
// 计数器本身不可分配，但可以被设备引用。设备被分配后，它所使用的那部分计数器
// 将不再可供其他设备使用。
type CounterSet struct {
	// Name 定义计数器集合的名称，必须是 DNS 标签。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Counters 定义本 CounterSet 的计数器集合。
	// 每个计数器的名称在集合内必须唯一，且必须是 DNS 标签。
	//
	// 计数器最大数量为 32。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,name=counters"`
}

// Counter 描述与设备关联的一个数量值。
type Counter struct {
	// Value 定义某设备计数器可用数量。
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`
}

// DriverNameMaxLength 是 ResourceSliceSpec 等位置中驱动名的最大合法长度。
// 与 CSI 驱动名的长度限制相同。
const DriverNameMaxLength = 63

// ResourcePool 描述 ResourceSlice 所属的池。
type ResourcePool struct {
	// Name 用于标识池。对于节点本地设备，这通常是节点名，但不是必须的。
	// 可以使用字段选择器只列出属于某个池的 ResourceSlice 对象。
	//
	// 长度不得超过 253 个字符，且必须由一个或多个用斜杠分隔的 DNS 子域组成。
	// 本字段不可变。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Generation 跟踪池随时间发生的变化。驱动每次改动池中一项或多项资源时，
	// 都必须更新该池所有 ResourceSlice 中的 generation。ResourceSlice 的消费者
	// 只能考虑 generation 最大的池中的资源。驱动可以重置 generation，
	// 只要池中所有 ResourceSlice 都被更新或删除，这对消费者来说没有问题。
	//
	// 与 ResourceSliceCount 配合，该机制使消费者能够检测由多个 ResourceSlice 组成、
	// 但当前处于不完整状态的池。
	//
	// +required
	Generation int64 `json:"generation" protobuf:"bytes,2,name=generation"`

	// ResourceSliceCount 是本 generation 下池中 ResourceSlice 的总数，必须大于零。
	//
	// 消费者可以用它检查是否已看到同一池的所有 ResourceSlice。
	//
	// +required
	ResourceSliceCount int64 `json:"resourceSliceCount" protobuf:"bytes,3,name=resourceSliceCount"`
}

// ResourceSliceMaxSharedCapacity 是 ResourceSlice 可上报的最大共享容量。
const ResourceSliceMaxSharedCapacity = 128

// ResourceSliceMaxDevices 是 ResourceSlice 中设备的最大数量。
const ResourceSliceMaxDevices = 128

// ResourceSliceMaxDevicesWithAdvancedFeatures 定义当任意设备使用高级特性时，
// 一个 ResourceSlice 中设备的最大数量。高级特性包括：
// - 设备污点（DRADeviceTaints 特性门控）
// - 消耗计数器（DRAPartitionableDevices 特性门控）
// - 列表类型属性（DRAListTypeAttributes 特性门控）
const ResourceSliceMaxDevicesWithAdvancedFeatures = 64

// PoolNameMaxLength 是资源池名称的最大长度，与单个节点名相同。
const PoolNameMaxLength = validation.DNS1123SubdomainMaxLength // 与单个节点名相同。
// BindingConditionsMaxSize 是绑定条件的最大数量。
const BindingConditionsMaxSize = 4

// BindingFailureConditionsMaxSize 是绑定失败条件的最大数量。
const BindingFailureConditionsMaxSize = 4

// 定义在 ResourceSlice 中通过 SharedCounters 字段可定义的计数器集合的最大数量。
const ResourceSliceMaxCounterSets = 8

// 定义在一个计数器集合中可定义的计数器的最大数量。
const ResourceSliceMaxCountersPerCounterSet = 32

// 定义每台设备通过 ConsumesCounters 字段可定义的计数器消耗的最大数量。
const ResourceSliceMaxDeviceCounterConsumptionsPerDevice = 2

// 定义每次设备计数器消耗中可定义的计数器的最大数量。
const ResourceSliceMaxCountersPerDeviceCounterConsumption = 32

// 定义每次设备计数器消耗中可声明的兼容性组的最大数量。
const DeviceCompatibilityGroupsMaxSize = 2

// Device 表示一个可以根据属性选择的独立硬件实例。除名称外，必须恰好设置一个字段。
type Device struct {
	// Name 是驱动在池中管理的所有设备中的唯一标识，必须是 DNS 标签。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Basic 定义一个设备实例。
	//
	// +optional
	// +oneOf=deviceType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Basic *BasicDevice `json:"basic,omitempty" protobuf:"bytes,2,opt,name=basic"`
}

// BasicDevice 定义一个设备实例。
type BasicDevice struct {
	// Attributes 定义本设备的属性集合，每个属性名在集合内必须唯一。
	//
	// 属性与容量的总数不得超过 32。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Attributes map[QualifiedName]DeviceAttribute `json:"attributes,omitempty" protobuf:"bytes,1,rep,name=attributes"`

	// Capacity 定义本设备的容量集合，每个容量名在集合内必须唯一。
	//
	// 属性与容量的总数不得超过 32。
	//
	// +optional
	Capacity map[QualifiedName]DeviceCapacity `json:"capacity,omitempty" protobuf:"bytes,2,rep,name=capacity"`

	// ConsumesCounters 定义一组对 sharedCounters 的引用，以及设备将从这些计数器集合
	// 中消耗的计数器。
	//
	// 每个 counterSet 只能有一条记录。
	//
	// 每台设备的计数器消耗记录最多 2 条。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=counterSet
	// +featureGate=DRAPartitionableDevices
	// +k8s:beta(since: "1.37")=+k8s:maxItems=2
	ConsumesCounters []DeviceCounterConsumption `json:"consumesCounters,omitempty" protobuf:"bytes,3,rep,name=consumesCounters"`

	// NodeName 标识设备可用的节点。
	//
	// 仅当 Spec.PerDeviceNodeSelection 设为 true 时才能设置。
	// NodeName、NodeSelector 与 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,4,opt,name=nodeName"`

	// NodeSelector 定义设备可用的节点。
	//
	// 必须恰好使用一个 term。
	//
	// 仅当 Spec.PerDeviceNodeSelection 设为 true 时才能设置。
	// NodeName、NodeSelector 与 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,5,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问该设备。
	//
	// 仅当 Spec.PerDeviceNodeSelection 设为 true 时才能设置。
	// NodeName、NodeSelector 与 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"bytes,6,opt,name=allNodes"`

	// 若指定，这些是驱动定义的污点。
	//
	// 污点最多 16 个。如果 ResourceSlice 中任一设备设置了污点，
	// 每个 ResourceSlice 允许的设备数上限会从 128 降为 64。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Taints []DeviceTaint `json:"taints,omitempty" protobuf:"bytes,7,rep,name=taints"`

	// BindsToNode 表示涉及本设备的分配的使用是否必须限定在分配声明时选中的那个节点。
	// 设为 true 时，调度器会将 ResourceClaim.Status.Allocation.NodeSelector 设置为匹配
	// 执行分配的节点。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// 特性门控。
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindsToNode *bool `json:"bindsToNode,omitempty" protobuf:"varint,8,opt,name=bindsToNode"`

	// BindingConditions 定义继续绑定的条件。调度 Pod 时，只有当逐设备状态条件中
	// 所有这些条件都为 True，才能把 Pod 绑定到节点。
	//
	// 绑定条件最多 4 个，且必须是合法的 condition 类型字符串。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,9,rep,name=bindingConditions"`

	// BindingFailureConditions 定义绑定失败的条件，可设置在逐设备状态条件中。
	// 其中任一条件为 true 即表示发生了绑定失败。
	//
	// 绑定失败条件最多 4 个，且必须是合法的 condition 类型字符串。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,10,rep,name=bindingFailureConditions"`

	// AllowMultipleAllocations 标记本设备是否允许被分配到多个 DeviceRequest。
	//
	// 设为 true 时，设备可以被多次分配，并且无论是否定义 requestPolicy，
	// 其全部容量都可以被消耗。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	AllowMultipleAllocations *bool `json:"allowMultipleAllocations,omitempty" protobuf:"bytes,11,opt,name=allowMultipleAllocations"`

	// NodeAllocatableResourceMappings 已被替换为 NodeAllocatableResources，此处保留墓碑。

	// NodeAllocatableResources 定义由暴露本设备的 DRA 驱动管理的节点资源映射。
	// 包括当前在 v1.Node `status.allocatable` 中上报的、非扩展资源
	// （参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources）
	// 在内的资源，例如 "cpu"、"memory"、"ephemeral-storage" 以及大页内存。
	// 除了通过 Pod `spec` 发出的标准请求外，这些资源也可以通过声明请求，
	// 并由 DRA 驱动分配。例如，CPU DRA 驱动可以分配独占 CPU，
	// 或者分配加速器设备所依赖的辅助节点内存。
	// 该 map 的键是节点可分配资源名（如 "cpu"、"memory"），不允许使用扩展资源名。
	// +optional
	// +k8s:optional
	// +featureGate=DRANodeAllocatableResources
	NodeAllocatableResources map[v1.ResourceName]NodeAllocatableResource `json:"nodeAllocatableResources,omitempty" protobuf:"bytes,14,opt,name=nodeAllocatableResources"`
}

// NodeAllocatableResource 定义请求的 DRA 设备/容量单位与对应节点可分配资源数量之间的换算。
// Mapping 与 Overhead 必须至少指定一个，两者都不指定是非法配置。
type NodeAllocatableResource struct {
	// 当设备直接建模节点可分配资源（如标准 CPU 或内存，例如 CPU DRA 驱动）时使用 Mapping。
	// 计算得到的数量在节点上按声明实例只计入一次。为避免节点 cgroup 隔离冲突，
	// 调度器明确禁止将映射设备声明在多个 Pod 之间共享。
	// +optional
	// +k8s:optional
	Mapping *NodeAllocatableMapping `json:"mapping,omitempty" protobuf:"bytes,3,opt,name=mapping"`

	// Overhead 包含用于建模辅助开销的字段：当分配的设备本身并不建模节点可分配资源时
	// （例如 GPU 的主机内存开销），这些开销会作用在节点可分配资源上。
	// 允许在多个 Pod 之间共享开销映射的声明，节点可分配开销会针对每个引用该声明的 Pod
	// 分别计入。即使同一资源同时指定了 Mapping，Overhead 也始终从该资源的节点可分配容量中扣除。
	// 例如：若某设备通过 Mapping（配合 CapacityKey）将每个 socket 的内存容量建模为可消耗容量池，
	// 同一资源上指定的任何开销都会从节点通用可分配容量中扣除，而不是从 Mapping 的每 socket 容量池中扣除。
	// +optional
	// +k8s:optional
	Overhead *NodeAllocatableOverhead `json:"overhead,omitempty" protobuf:"bytes,4,opt,name=overhead"`
}

// NodeAllocatableMapping 定义 DRA 分配如何直接换算为节点可分配资源数量。
// 换算可以基于已分配设备数量，也可以基于消耗的具体容量，两者互斥。
// Kubelet 会把该映射数量从声明加到引用它的 Pod 级 cgroup 的 requests 与 limits，
// 以及每个引用该声明的容器级 cgroup 的 limits 中。
type NodeAllocatableMapping struct {
	// CapacityKey 引用 `spec.devices[*].capacity` map 中定义的容量名。设置该字段后，
	// 对应声明分配在 `status.allocation.devices.results[*].consumedCapacity` map 中
	// 该键的值将作为节点可分配资源的基础数量，同时还必须设置 capacityMultiplier，
	// 最终数量为基础数量乘以 capacityMultiplier。
	// 例如 `spec.devices[*].capacity` 中有条目 "dra.example.com/memory": "128Gi"，
	// 且本字段设为 "dra.example.com/memory"，那么对于消耗 { "dra.example.com/memory": "4Gi" }
	// 的声明分配，节点可分配资源映射的基础数量为 "4Gi"。
	// 最终节点可分配资源量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityMultiplier")
	CapacityKey *QualifiedName `json:"capacityKey,omitempty" protobuf:"bytes,1,opt,name=capacityKey"`

	// CapacityMultiplier 用作已分配且被消耗容量的乘数，仅在设置了 `capacityKey` 时有效。
	// 最终节点可分配资源量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// 例如，若消耗了某设备的容量 "dra.example.com/cores"，且每个 "core" 提供 2 个 "cpu"，
	// 映射为：{ResourceName: "cpu", capacityKey: "dra.example.com/cores", capacityMultiplier: "2"}。
	// 若声明消耗 8 个 "dra.example.com/cores"，则 CPU 占用为 8 * 2 = 16。
	// +optional
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityKey")
	CapacityMultiplier *resource.Quantity `json:"capacityMultiplier,omitempty" protobuf:"bytes,2,opt,name=capacityMultiplier"`

	// DeviceMultiplier 用作声明中已分配设备数量的乘数。
	// 最终节点可分配资源量为 `deviceCount` * `deviceMultiplier`。
	// 例如，将每个缓存复合体（CCX）建模为设备的 DRA 驱动，其 `nodeAllocatableResources`
	// 中会有 {ResourceName: "cpu", deviceMultiplier: "8"}。若声明分配了 2 个设备（CCX），
	// 则视为分配了 2 * 8 = 16 个 CPU。
	// 仅在未设置 `capacityKey` 与 `capacityMultiplier` 时有效。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	DeviceMultiplier *resource.Quantity `json:"deviceMultiplier,omitempty" protobuf:"bytes,3,opt,name=deviceMultiplier"`
}

// NodeAllocatableOverhead 定义分配设备时产生的辅助资源开销。
// 开销可以指定为每个引用该声明的 Pod 的固定成本、每个容器引用的可变成本，或两者兼有。
// Kubelet 会将该开销加入引用容器的 Pod 级与容器级 cgroup 中进行核算。
type NodeAllocatableOverhead struct {
	// PerPod 是引用本节点上该声明的每个 Pod 各应用一次的开销。
	// 这是每个引用该声明的 Pod 都会产生的固定开销。
	// +optional
	// +k8s:optional
	PerPod *resource.Quantity `json:"perPod,omitempty" protobuf:"bytes,1,opt,name=perPod"`

	// PerContainer 按容器对该声明的每次引用应用。
	// 它建模随实际使用该设备的容器数量线性增长的开销。
	// 当同时指定 PerPod 与 PerContainer 时，每个引用该声明的 Pod 分配的总开销计算为：
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet 在 cgroup 中核算该开销：
	// - Pod 级 cgroup（requests 与 limits）：加入 PerPod + (PerContainer * NumReferences)。
	// - 容器级 cgroup（仅 limits）：每个引用容器加入 PerPod + PerContainer。
	// 这样任何单个容器都可以访问 Pod 级开销，同时父 cgroup 将总用量限制为
	// 恰好计入一次 PerPod。
	// +optional
	// +k8s:optional
	PerContainer *resource.Quantity `json:"perContainer,omitempty" protobuf:"bytes,2,opt,name=perContainer"`
}

// DeviceCounterConsumption 定义设备将从某个 CounterSet 中消耗的一组计数器。
type DeviceCounterConsumption struct {
	// CounterSet 是要消耗的计数器所属集合的名称。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	CounterSet string `json:"counterSet" protobuf:"bytes,1,opt,name=counterSet"`

	// Counters 定义设备将消耗的计数器。
	//
	// 计数器最大数量为 32。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,opt,name=counters"`

	// CompatibilityGroups 是本次计数器消耗的不透明组名列表。
	//
	// 从同一计数器集合消耗计数的设备，只有当它们至少共享一个公共组时才能同时分配
	// （即“共同分配”）：所有共同分配设备在该计数器集合上的 CompatibilityGroups 交集必须非空。
	// 从不同计数器集合消耗计数的设备之间不会通过该字段比较。
	//
	// 未设置、显式 nil 与空列表等价，都表示“无组”：这样的设备只能与同一计数器集合上
	// 同样没有组的同级设备共同分配，且永远不能与声明了一个或多个组的设备共同分配。
	//
	// 组名是不透明的，仅在发布驱动的池内有意义。
	//
	// 组的最大数量为 2，且名称必须唯一。
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

// DeviceCapacity 描述与设备关联的数量。
type DeviceCapacity struct {
	// Value 定义设备拥有多少某种容量。
	//
	// 该字段反映固定的总容量，不会变化。
	// 已消耗的数量由调度器单独跟踪，不影响该值。
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`

	// RequestPolicy 定义当设备允许被多个分配共享时，本 DeviceCapacity 应如何被消耗。
	//
	// 要设置 requestPolicy，设备必须将 allowMultipleAllocations 设为 true。
	//
	// 未设置时，容量请求没有约束：请求可以消耗任意数量的容量，
	// 只要所有分配消耗的总量不超过设备定义的容量即可。若请求也未设置，默认值为全部容量。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	RequestPolicy *CapacityRequestPolicy `json:"requestPolicy,omitempty" protobuf:"bytes,2,opt,name=requestPolicy"`
}

// CapacityRequestPolicy 定义请求如何消耗设备容量。
//
// 最多只能设置一个 ValidRequestValues。
type CapacityRequestPolicy struct {
	// Default 指定当请求在 DeviceRequest 的 Capacity 中不含该容量条目时，
	// 该请求消耗的容量。
	//
	// +optional
	Default *resource.Quantity `json:"default" protobuf:"bytes,1,opt,name=default"`

	// ValidValues 定义请求中可接受的数量值集合。
	//
	// 不得超过 10 个条目，且必须按升序排列。
	//
	// 设置该字段时，必须定义 Default 且 Default 必须包含在 ValidValues 列表中。
	//
	// 如果请求量不匹配任何有效值但小于某些有效值，调度器会计算出大于等于请求量的最小有效值。
	// 即：min(ceil(requestedValue) ∈ validValues)，其中 requestedValue ≤ max(validValues)。
	//
	// 如果请求量超过所有有效值，则该请求违反策略，设备不可被分配。
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValidRequestValues
	ValidValues []resource.Quantity `json:"validValues,omitempty" protobuf:"bytes,3,opt,name=validValues"`

	// ValidRange 定义请求中可接受的数量范围。
	//
	// 设置该字段时，必须定义 Default 且 Default 必须落在 ValidRange 范围内。
	//
	// 如果请求量不在定义的范围内，则该请求违反策略，设备不可被分配。
	//
	// 如果请求不含该容量条目，则使用 Default 值。
	//
	// +optional
	// +oneOf=ValidRequestValues
	ValidRange *CapacityRequestPolicyRange `json:"validRange,omitempty" protobuf:"bytes,4,opt,name=validRange"`
}

// CapacityRequestPolicyRange 定义可消耗容量值的合法范围。
//
// 如果启用了 DRAFractionalCapacityRange 特性门控，且 Min、Max 或 Step 中至少有一个是小数数量
// （即其值不是整数），则改用毫单位算术，支持最多 3 位小数的值（例如 100m = 0.1）。
// 此时支持的最大值比使用 64 位整数时小 1000 倍。
// 否则，所有比较都通过 resource.Quantity.Value() 使用 64 位整数算术。
type CapacityRequestPolicyRange struct {
	// Min 指定消耗请求允许的最小容量。
	//
	// Min 必须大于等于零，且小于等于容量值。
	// requestPolicy.default 必须不小于最小值。
	//
	// +required
	Min *resource.Quantity `json:"min,omitempty" protobuf:"bytes,1,opt,name=min"`

	// Max 定义可请求容量的上限。
	//
	// Max 必须小于等于容量值，且 Min 与 requestPolicy.default 都必须小于等于最大值。
	//
	// +optional
	Max *resource.Quantity `json:"max,omitempty" protobuf:"bytes,2,opt,name=max"`

	// Step 定义范围内合法容量值之间的步长。
	//
	// Max（若设置）与 requestPolicy.default 必须是 Step 的整数倍。
	// Min + Step 必须小于等于容量值。
	//
	// +optional
	Step *resource.Quantity `json:"step,omitempty" protobuf:"bytes,3,opt,name=step"`
}

// attributes 与 capacity 两个 map 条目数量之和的上限。
const ResourceSliceMaxAttributesAndCapacitiesPerDevice = 32

// 每台设备中，列表与非列表属性里字符串、版本、布尔或整数值的总数上限。
const ResourceSliceMaxAttributeValuesPerDevice = 48

// QualifiedName 是设备属性或容量的名称。
//
// 属性和容量由特定驱动的拥有者（通常是供应商）或第三方（例如 Kubernetes 项目）定义。
// 由于有时会在不同设备之间比较，给定名称在所有设备上应当含义相同、类型一致。
//
// 名称要么是 C 标识符（如 "theName"），要么是 DNS 子域后跟斜杠（"/"）再跟 C 标识符
// （如 "dra.example.com/theName"）。不含域前缀的名称被视为属于驱动的域。
// 第三方定义的属性或容量必须包含域前缀。
//
// DNS 子域的最大长度为 63 个字符（与驱动名相同），C 标识符的最大长度为 32。
type QualifiedName string

// FullyQualifiedName 是设置了域的 QualifiedName。
// 无法为该类型添加格式校验，因为它的用途之一 DistinctAttribute 是条件校验，
// 而这种条件校验无法用声明式方式表达。
type FullyQualifiedName string

// DeviceMaxDomainLength 是全限定名称中域前缀的最大长度。
const DeviceMaxDomainLength = 63

// DeviceMaxIDLength 是设备属性或容量名（`<domain>/<ID>`）中标识符的最大长度。
const DeviceMaxIDLength = 32

// DeviceAttribute 必须恰好设置一个字段。
type DeviceAttribute struct {
	// 下面的 Go 字段名带有 Value 后缀，是为了避免字段 "String" 与对应方法重名，
	// 而该方法是必需的。Kubernetes API 中的定义不带这个后缀，以保持自然。
	//

	// IntValue 是一个数值。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	IntValue *int64 `json:"int,omitempty" protobuf:"varint,2,opt,name=int"`

	// BoolValue 是 true/false 值。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	BoolValue *bool `json:"bool,omitempty" protobuf:"varint,3,opt,name=bool"`

	// StringValue 是字符串，长度不得超过 64 个字符。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	StringValue *string `json:"string,omitempty" protobuf:"bytes,4,opt,name=string"`

	// VersionValue 是符合 semver.org 2.0.0 规范的语义版本，长度不得超过 64 个字符。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	VersionValue *string `json:"version,omitempty" protobuf:"bytes,5,opt,name=version"`

	// IntValues 是非空的数值列表。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	IntValues []int64 `json:"ints,omitempty" protobuf:"varint,6,opt,name=ints"`

	// BoolValues 是非空的 true/false 值列表。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	BoolValues []bool `json:"bools,omitempty" protobuf:"varint,7,opt,name=bools"`

	// StringValues 是非空的字符串列表，每个字符串长度不得超过 64 个字符。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:maxBytes=64
	// +featureGate=DRAListTypeAttributes
	StringValues []string `json:"strings,omitempty" protobuf:"bytes,8,opt,name=strings"`

	// VersionValues 是非空的语义版本列表，版本需符合 semver.org 2.0.0 规范，
	// 每个版本字符串长度不得超过 64 个字符。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes 特性门控。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	VersionValues []string `json:"versions,omitempty" protobuf:"bytes,9,opt,name=versions"`
}

// DeviceAttributeMaxValueLength 是字符串或版本属性值的最大长度。
const DeviceAttributeMaxValueLength = 64

// DeviceTaintsMaxLength 是每台 Device 的污点最大数量。
const DeviceTaintsMaxLength = 16

// 污点所附着的设备会对任何不容忍该污点的声明产生 "effect" 影响，
// 并通过该声明影响到使用该声明的 Pod。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type DeviceTaint struct {
	// 要施加到设备上的污点键，必须是标签名。
	//
	// +required
	Key string `json:"key" protobuf:"bytes,1,name=key"`

	// 与污点键对应的污点值，必须是标签值。
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`

	// 该污点对所有不容忍它的声明、以及通过这些声明对使用它们的 Pod 的影响。
	//
	// 合法 effect 为 None、NoSchedule 和 NoExecute。节点使用的 PreferNoSchedule 在这里无效。
	// 未来可能增加更多 effect，消费者必须把未知 effect 当作 None 处理。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Effect DeviceTaintEffect `json:"effect" protobuf:"bytes,3,name=effect,casttype=DeviceTaintEffect"`

	// ^^^^
	//
	// 实现 PreferNoSchedule 取决于 DRA 的打分方案，可能会随该方案一起加入。
	//
	// 未来可能新增的 effect 是 NoExecuteWithPodDisruptionBudget：
	// 遵循 Pod 中断预算，而不是直接删除 Pod。目前尚未决定，也可能做成一个单独的字段。
	//
	// 校验必须允许存储对象中出现未知枚举值，这样才能在单个发布版本内新增枚举，
	// 而无需 ratcheting 机制。
	//

	// TimeAdded 表示污点被添加的时间，或者（仅在 DeviceTaintRule 中）effect 被修改的时间。
	// 如果未设置，会在创建或更新时自动添加。
	//
	// 此外，在 DeviceTaintRule 中，如果更新时提供的值与旧值相同而 effect 不同，
	// 提供的值会被替换为当前时间。保持 effect 不变而只修改 key 和/或 value 是允许的，
	// 且不会更新时间戳，因为使用它的驱逐要么已经开始（NoExecute），要么尚未开始
	//
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty" protobuf:"bytes,4,opt,name=timeAdded"`

	// ^^^
	//
	// 对于节点污点，该字段原先的定义是“只对 NoExecute 污点写入”。
	// 但实际上 Kubernetes 从未对它做过任何处理（无校验、无默认值，
	// 在 pkg/controller/tainteviction 的 Pod 驱逐中被忽略）。
}

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// DeviceTaintEffect 描述设备污点对不容忍它的声明的效果。
type DeviceTaintEffect string

const (
	// 无影响，污点纯粹是信息性的。
	DeviceTaintEffectNone DeviceTaintEffect = "None"

	// 不允许新的 Pod 调度到使用被污染设备的节点上，除非它们容忍该污点；
	// 但允许所有未经调度器、直接提交给 Kubelet 的 Pod 启动，
	// 也允许所有已经在运行的 Pod 继续运行。
	DeviceTaintEffectNoSchedule DeviceTaintEffect = "NoSchedule"

	// 驱逐所有不容忍该设备污点的、已经在运行的 Pod。
	DeviceTaintEffectNoExecute DeviceTaintEffect = "NoExecute"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// ResourceSliceList 是 ResourceSlice 的集合。
type ResourceSliceList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceSlice 的列表。
	Items []ResourceSlice `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32
// +k8s:supportsSubresource="/status"

// ResourceClaim 描述工作负载对集群资源的访问请求。
// 例如，如果某个工作负载需要具有特定属性的加速卡设备，就用它来表达该请求。
// status 部分记录该声明是否已满足，以及分配到了哪些具体资源。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation 特性门控。
type ResourceClaim struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 描述请求的内容及配置方式，spec 不可变。
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 描述声明是否可用以及已分配的内容。
	// +optional
	Status ResourceClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourceClaimSpec 定义 ResourceClaim 中请求什么以及如何配置。
type ResourceClaimSpec struct {
	// Devices 定义如何请求设备。
	//
	// +optional
	Devices DeviceClaim `json:"devices" protobuf:"bytes,1,name=devices"`

	// Controller 自 Kubernetes 1.32 起已被移除，此处保留墓碑。
	// 当不再需要解码 v1alpha3 时，可以复用该名称。
}

// DeviceClaim 定义如何用 ResourceClaim 请求设备。
type DeviceClaim struct {
	// Requests 表示对互不相同的设备的单独请求，所有请求都必须被满足。
	// 若为空，则无需分配任何资源。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=map
	// +k8s:beta(since: "1.37")=+k8s:listMapKey=name
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []DeviceRequest `json:"requests" protobuf:"bytes,1,name=requests"`

	// 为本声明分配的设备集合必须满足这些约束。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Constraints []DeviceConstraint `json:"constraints,omitempty" protobuf:"bytes,2,opt,name=constraints"`

	// 本字段保存多个潜在驱动的配置，这些驱动都可能满足本声明中的请求。
	// 分配声明时会忽略这些配置。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClaimConfiguration `json:"config,omitempty" protobuf:"bytes,3,opt,name=config"`

	// 潜在的未来扩展，旧调度器会忽略。这是可行的，因为打分只让用户表达偏好，
	// 而不会使其成为硬性要求。
	//
	// Score *SomeScoringStruct
}

const (
	DeviceRequestsMaxSize    = AllocationResultsMaxSize
	DeviceConstraintsMaxSize = 32
	DeviceConfigMaxSize      = 32
)

// DRAAdminNamespaceLabelKey 是一个标签键，用于在命名空间内授予对某些 resource.k8s.io API 类型的管理访问权限。
// 当命名空间上该标签的值设为 "true"（区分大小写）时，允许在任何命名空间级的 resource.k8s.io API 类型中
// 使用 adminAccess: true。目前该权限适用于 ResourceClaim 与 ResourceClaimTemplate 对象。
const (
	DRAAdminNamespaceLabelKey = "resource.kubernetes.io/admin-access"
)

// DeviceRequest 是声明对所需设备的请求。
// 通常是请求单个资源（如一个设备），但也可以请求多个相同的设备。
type DeviceRequest struct {
	// Name 可用于在 pod.spec.containers[].resources.claims 条目以及声明的约束中引用本请求。
	//
	// 必须是 DNS 标签，且在 ResourceClaim 的所有 DeviceRequest 中唯一。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// DeviceClassName 引用特定的 DeviceClass，后者可以定义供本请求继承的附加配置与选择器。
	//
	// 如果 firstAvailable 列表中未指定子请求，则必须指定类；如果 firstAvailable 列表中指定了子请求，
	// 则不能设置类。哪些类可用取决于集群。
	//
	// 管理员可以只安装带有允许设备选择器的类，以此限制用户可以请求哪些设备。
	// 若允许用户不受限制地请求任何设备，管理员可以创建一个空的 DeviceClass 供用户引用。
	//
	// +optional
	// +oneOf=deviceRequestType
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,2,name=deviceClassName"`

	// Selectors 定义某设备被本请求考虑所必须满足的条件，设备必须满足所有选择器。
	//
	// 只有当 deviceClassName 已设置且 firstAvailable 列表中未指定子请求时，才能设置该字段。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,3,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本请求，支持的取值为：
	//
	// - ExactCount：请求指定数量的设备，这是默认值，具体数量在 count 字段中给出。
	//
	// - All：请求池中全部匹配的设备。分配成功要求节点上至少存在一台设备。
	//   如果部分设备已被分配，分配会失败，除非请求了 adminAccess。
	//
	// 未指定 AllocationMode 时默认使用 ExactCount；模式为 ExactCount 且未指定 count 时默认数量为 1。
	// 其他请求必须显式指定该字段。
	//
	// 只有当 deviceClassName 已设置且 firstAvailable 列表中未指定子请求时，才能设置该字段。
	//
	// 未来可能增加更多模式，客户端必须拒绝处理未知模式的请求。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,4,opt,name=allocationMode"`

	// Count 仅在计数模式为 "ExactCount" 时使用，必须大于零。
	// 如果 AllocationMode 为 ExactCount 且未指定该字段，默认值为 1。
	//
	// 只有当 deviceClassName 已设置且 firstAvailable 列表中未指定子请求时，才能设置该字段。
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,5,opt,name=count"`

	// AdminAccess 表示本声明是对设备的管理访问的声明。
	// 带 AdminAccess 的声明预期用于设备的监控或其他管理服务，在访问模式和资源分配方面
	// 会忽略设备上所有普通声明。
	//
	// 只有当 deviceClassName 已设置且 firstAvailable 列表中未指定子请求时，才能设置该字段。
	//
	// 这是 alpha 字段，需要启用 DRAAdminAccess 特性门控。未设置或设为 false 时禁用管理访问，
	// 否则启用。
	//
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,6,opt,name=adminAccess"`

	// FirstAvailable 包含多个子请求，调度器会恰好满足其中一个。它按此处列出的顺序尝试满足，
	// 因此如果列表中有两条记录，调度器只有在确定第一条不可用时才会检查第二条。
	//
	// 该字段只能在 DeviceClaim.Requests 的条目中设置。
	//
	// DRA 目前尚未实现打分，因此调度器会选择第一组能满足声明中所有请求的设备。
	// 如果需求可以在多个节点上满足，则由其他调度特性决定选择哪个节点。这意味着分配给声明的
	// 设备集合可能不是集群中的最优解，打分功能会在以后实现。
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
	FirstAvailable []DeviceSubRequest `json:"firstAvailable,omitempty" protobuf:"bytes,7,name=firstAvailable"`

	// 若指定，这是请求的容忍列表。
	//
	// 要分配带 NoSchedule 效果污点的设备，必须有对应的 NoSchedule 容忍，NoExecute 同理。
	//
	// 此外，如果任何已分配设备在分配后被加上 NoExecute 污点且该效果未被容忍，
	// 所有使用该 ResourceClaim 的 Pod 都会被删除以进行驱逐。在设备带这些污点期间，
	// 调度器不会让新 Pod 预留该声明。所有 Pod 被驱逐后，声明会被解除分配。
	//
	// 容忍的最大数量为 16。
	//
	// 只有当 deviceClassName 已设置且 firstAvailable 列表中未指定子请求时，才能设置该字段。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,8,opt,name=tolerations"`

	// Capacity 定义针对每种容量的资源需求。
	//
	// 如果未设置该字段且设备支持多次分配，则按 requestPolicy 为每种容量应用默认值；
	// 没有 requestPolicy 的容量默认取全部容量值。
	//
	// 适用于每次设备分配。如果 Count > 1，但没有足够满足需求的设备，请求会失败。
	// 如果 AllocationMode 设为 All，且存在其他都匹配该请求、拥有该容量且容量值 >= 请求量、
	// 但无法分配给本请求的设备，请求也会失败。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,9,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算出的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备（例如在同一 NUMA 节点上
	// 共同分配 GPU 与 NIC），即使它们的驱动发布的是不同属性。在计算选择器表达式时，
	// 派生属性在 CEL 环境中无法通过 `device.attributes` 访问。
	//
	// 派生属性允许在调度时提取、转换或规范化拓扑信息（例如从复杂的拓扑字符串中抽取 NUMA 索引，
	// 或重命名供应商特有属性）为统一的虚拟属性名。随后调度器会像匹配静态属性一样匹配这些虚拟属性。
	//
	// 列表中定义的每个派生属性都必须被 `.devices.constraints` 列表中的至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性的最大数量为 32。
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,10,rep,name=derivedAttributes"`
}

// DeviceSubRequest 描述 claim.spec.devices.requests[].firstAvailable 数组中提供的设备请求。
// 每个请求通常是请求单个资源（如一个设备），但也可以请求多个相同的设备。
//
// DeviceSubRequest 与 Request 类似，但不暴露 AdminAccess 与 FirstAvailable 字段，
// 因为它们只能在顶层请求中设置。优先级列表不支持管理访问，也不支持递归的 FirstAvailable 字段。
type DeviceSubRequest struct {
	// Name 可用于在声明的约束列表或配置列表中引用本子请求，引用格式为 <main request>/<subrequest>。
	//
	// 必须是 DNS 标签。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// DeviceClassName 引用特定的 DeviceClass，后者可以定义供本子请求继承的附加配置与选择器。
	//
	// 必须指定类，哪些类可用取决于集群。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,2,name=deviceClassName"`

	// Selectors 定义某设备被本子请求考虑所必须满足的条件，设备必须满足所有选择器。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	// +k8s:beta(since: "1.37")=+k8s:optional
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,3,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求，支持的取值为：
	//
	// - ExactCount：请求指定数量的设备，这是默认值，具体数量在 count 字段中给出。
	//
	// - All：请求池中全部匹配的设备。如果部分设备已被分配，分配会失败，除非请求了 adminAccess。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,4,opt,name=allocationMode"`

	// Count 仅在计数模式为 "ExactCount" 时使用，必须大于零。
	// 如果 AllocationMode 为 ExactCount 且未指定该字段，默认值为 1。
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,5,opt,name=count"`

	// 若指定，这是请求的容忍列表。
	//
	// 要分配带 NoSchedule 效果污点的设备，必须有对应的 NoSchedule 容忍，NoExecute 同理。
	//
	// 此外，如果任何已分配设备在分配后被加上 NoExecute 污点且该效果未被容忍，
	// 所有使用该 ResourceClaim 的 Pod 都会被删除以进行驱逐。在设备带这些污点期间，
	// 调度器不会让新 Pod 预留该声明。所有 Pod 被驱逐后，声明会被解除分配。
	//
	// 容忍的最大数量为 16。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,7,opt,name=tolerations"`

	// Capacity 定义针对每种容量的资源需求。
	//
	// 如果未设置该字段且设备支持多次分配，则按 requestPolicy 为每种容量应用默认值；
	// 没有 requestPolicy 的容量默认取全部容量值。
	//
	// 适用于每次设备分配。如果 Count > 1，但没有足够满足需求的设备，请求会失败。
	// 如果 AllocationMode 设为 All，且存在其他都匹配该请求、拥有该容量且容量值 >= 请求量、
	// 但无法分配给本请求的设备，请求也会失败。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,8,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算出的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备（例如在同一 NUMA 节点上
	// 共同分配 GPU 与 NIC），即使它们的驱动发布的是不同属性。在计算选择器表达式时，
	// 派生属性在 CEL 环境中无法通过 `device.attributes` 访问。
	//
	// 派生属性允许在调度时提取、转换或规范化拓扑信息（例如从复杂的拓扑字符串中抽取 NUMA 索引，
	// 或重命名供应商特有属性）为统一的虚拟属性名。随后调度器会像匹配静态属性一样匹配这些虚拟属性。
	//
	// 列表中定义的每个派生属性都必须被 `.devices.constraints` 列表中的至少一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性的最大数量为 32。
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes 特性门控。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,9,rep,name=derivedAttributes"`
}

// CapacityRequirements 定义特定设备请求的容量需求。
type CapacityRequirements struct {
	// Requests 表示对不同资源的单独设备资源请求，设备必须提供所有这些资源。
	// +optional
	Requests map[QualifiedName]resource.Quantity `json:"requests,omitempty" protobuf:"bytes,1,rep,name=requests,castkey=QualifiedName"`
}

// DeviceDerivedAttribute 定义通过 CEL 计算出的派生属性。
type DeviceDerivedAttribute struct {
	// Name 是本次派生属性的标识符，用于约束中。
	// +required
	// +k8s:required
	// +k8s:format=k8s-resource-fully-qualified-name
	Name FullyQualifiedName `json:"name" protobuf:"bytes,1,name=name"`

	// Expression 是针对每个候选设备求值的 CEL 表达式，必须求值为基本标量（字符串、整数、
	// 布尔或 semver）或这些标量的列表（[]string、[]int64、[]bool、[]semver），以作为虚拟分组键。
	// 任何其他返回类型都是错误，会导致该设备的 CEL 求值失败。
	// +required
	// +k8s:required
	Expression string `json:"expression" protobuf:"bytes,2,name=expression"`
}

const (
	DeviceSelectorsMaxSize             = 32
	FirstAvailableDeviceRequestMaxSize = 8
	DeviceTolerationsMaxLength         = 16
	DeviceDerivedAttributesMaxSize     = 32
)

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
// DeviceAllocationMode 定义设备的分配模式。
type DeviceAllocationMode string

// [DeviceRequest.CountMode] 的合法取值。
const (
	DeviceAllocationModeExactCount = DeviceAllocationMode("ExactCount")
	DeviceAllocationModeAll        = DeviceAllocationMode("All")
)

// DeviceSelector 必须恰好设置一个字段。
type DeviceSelector struct {
	// CEL 包含用于选择设备的 CEL 表达式。
	// +optional
	// +oneOf=SelectorType
	CEL *CELDeviceSelector `json:"cel,omitempty" protobuf:"bytes,1,opt,name=cel"`
}

// CELDeviceSelector 包含用于选择设备的 CEL 表达式。
type CELDeviceSelector struct {
	// Expression 是对单台设备求值的 CEL 表达式：当被测设备满足期望条件时必须求值为 true，
	// 否则为 false。任何其他结果都是错误，会导致设备分配中止。
	// +required
	Expression string `json:"expression" protobuf:"bytes,1,name=expression"`
}

// CELSelectorExpressionMaxCost 指定单个 CEL 选择器求值的开销上限。
const CELSelectorExpressionMaxCost = 1000000

// DeviceClaimDerivedAttributeCELMaxCost 是单个 DeviceClaim 中所有派生属性 CEL 表达式允许的最大总执行开销。
const DeviceClaimDerivedAttributeCELMaxCost = 1000000

// CELSelectorExpressionMaxLength 是 CEL 选择器表达式字符串的最大长度。
const CELSelectorExpressionMaxLength = 10 * 1024

// DeviceConstraint 除 Requests 外必须恰好设置一个字段。
type DeviceConstraint struct {
	// Requests 列出本声明中必须共同满足该约束的一个或多个请求。
	// 如果一个请求由多台设备满足，则所有设备都必须满足该约束。
	// 未指定时，约束适用于本声明中的所有请求。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,1,opt,name=requests"`

	// MatchAttribute 要求所有相关设备都具有该属性，且各设备上该属性的类型与值相同。
	// +optional
	// +oneOf=ConstraintType
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	MatchAttribute *FullyQualifiedName `json:"matchAttribute,omitempty" protobuf:"bytes,2,opt,name=matchAttribute"`

	// 潜在的未来扩展，不属于当前设计：一个比较不同设备、匹配时返回 true 的 CEL 表达式。

	// DistinctAttribute 要求所有相关设备都具有该属性，且各设备上该属性的类型与值互不相同。
	// +optional
	// +oneOf=ConstraintType
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	DistinctAttribute *FullyQualifiedName `json:"distinctAttribute,omitempty" protobuf:"bytes,3,opt,name=distinctAttribute"`
}

// DeviceClaimConfiguration 用于 DeviceClaim 中的配置参数。
type DeviceClaimConfiguration struct {
	// Requests 列出配置适用的请求名称；为空时适用于所有请求。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,1,opt,name=requests"`

	DeviceConfiguration `json:"" protobuf:"bytes,2,name=deviceConfiguration"`
}

// DeviceConfiguration 必须恰好设置一个字段。它会被内联嵌入到其他含字段的结构体中，
// 因此字段名不能与这些字段冲突。
type DeviceConfiguration struct {
	// Opaque 提供驱动特有的配置参数。
	// +optional
	// +oneOf=ConfigurationType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Opaque *OpaqueDeviceConfiguration `json:"opaque,omitempty" protobuf:"bytes,1,opt,name=opaque"`
}

// OpaqueDeviceConfiguration 包含由驱动供应商定义格式的驱动配置参数。
type OpaqueDeviceConfiguration struct {
	// Driver 用于确定需要把配置参数传给哪个 kubelet 插件。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Parameters 可以包含任意数据，校验与版本管理由驱动开发者负责。通常包括自标识与版本
	// （Kubernetes 类型中为 "kind" + "apiVersion"），以及不同版本之间的转换。
	// +required
	Parameters runtime.RawExtension `json:"parameters" protobuf:"bytes,2,name=parameters"`
}

// OpaqueParametersMaxLength 是 [OpaqueDeviceConfiguration.Parameters] 字段中原始数据的最大长度。
const OpaqueParametersMaxLength = 10 * 1024

// 附着本 DeviceToleration 的 ResourceClaim 会容忍所有按 <key,value,effect> 三元组及
// <operator> 匹配运算符匹配成功的污点。
type DeviceToleration struct {
	// Key 是容忍适用的污点键，为空表示匹配所有污点键。
	// 如果键为空，operator 必须为 Exists，该组合表示匹配所有值与所有键。必须是标签名。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-label-key
	Key string `json:"key,omitempty" protobuf:"bytes,1,opt,name=key"`

	// Operator 表示键与值的关系。合法运算符为 Exists 与 Equal，默认为 Equal。
	// Exists 等价于值的通配符，使 ResourceClaim 能容忍某一类别的所有污点。
	// +optional
	// +default="Equal"
	// +k8s:beta(since: "1.37")=+k8s:optional
	Operator DeviceTolerationOperator `json:"operator,omitempty" protobuf:"bytes,2,opt,name=operator,casttype=DeviceTolerationOperator"`

	// Value 是容忍匹配的污点值。operator 为 Exists 时该值必须为空，否则为普通字符串。
	// 必须是标签值。
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,3,opt,name=value"`

	// Effect 表示要匹配的污点效果，为空表示匹配所有效果。
	// 指定时合法取值为 NoSchedule 与 NoExecute。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Effect DeviceTaintEffect `json:"effect,omitempty" protobuf:"bytes,4,opt,name=effect,casttype=DeviceTaintEffect"`

	// TolerationSeconds 表示容忍（必须是 NoExecute 效果，否则忽略该字段）容忍污点的时长。
	// 默认未设置，表示永久容忍（不驱逐）。零值与负值会被系统按 0 处理（立即驱逐）。
	// 若大于零，Pod 需要被驱逐的时间计算为 <污点添加时间> + <容忍秒数>。
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty" protobuf:"varint,5,opt,name=tolerationSeconds"`
}

// DeviceTolerationOperator 是容忍中可以使用的运算符集合。
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type DeviceTolerationOperator string

const (
	DeviceTolerationOpExists DeviceTolerationOperator = "Exists"
	DeviceTolerationOpEqual  DeviceTolerationOperator = "Equal"
)

// ResourceClaimStatus 记录资源是否已分配以及分配结果。
type ResourceClaimStatus struct {
	// 声明成功分配后会设置 Allocation。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:update=NoModify
	Allocation *AllocationResult `json:"allocation,omitempty" protobuf:"bytes,1,opt,name=allocation"`

	// ReservedFor 指明当前允许哪些实体使用该声明。引用了未为其预留的 ResourceClaim 的 Pod 不会启动。
	// 正在使用或因被预留而可能正在使用的声明不得被解除分配。
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

	// DeallocationRequested 自 Kubernetes 1.32 起已被移除，此处保留墓碑。
	// 当不再需要解码 v1alpha3 时，可以复用该名称。

	// Devices 包含驱动上报的、为本声明分配的每个设备的状态。其中可能包含驱动特有的信息，
	// 各条目由相应驱动负责维护。
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

// ResourceClaimReservedForMaxSize 是 claim.status.reservedFor 的最大条目数。
const ResourceClaimReservedForMaxSize = 256

// ResourceClaimConsumerReference 包含定位 ResourceClaim 消费者所需的足够信息。
// 该使用者必须是与 ResourceClaim 同一命名空间下的资源。
type ResourceClaimConsumerReference struct {
	// APIGroup 是被引用资源所属的组；核心 API 下为空。
	// 它与创建资源时使用的 APIVersion 中的组名一致。
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
	// Resource 是被引用资源的类型，例如 "pods"。
	// +required
	Resource string `json:"resource" protobuf:"bytes,3,name=resource"`
	// Name 是被引用资源的名称。
	// +required
	Name string `json:"name" protobuf:"bytes,4,name=name"`
	// UID 精确标识资源的某一次实例。
	// +required
	UID types.UID `json:"uid" protobuf:"bytes,5,name=uid"`
}

// AllocationResult 包含已分配资源的属性。
type AllocationResult struct {
	// Devices 是设备分配的结果。
	// +optional
	Devices DeviceAllocationResult `json:"devices,omitempty" protobuf:"bytes,1,opt,name=devices"`

	// NodeSelector 定义已分配资源在哪些节点上可用；未设置时处处可用。
	// +optional
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,3,opt,name=nodeSelector"`

	// Controller 自 Kubernetes 1.32 起已被移除，此处保留墓碑。
	// 当不再需要解码 v1alpha3 时，可以复用该名称。
	// Controller string `json:"controller,omitempty" protobuf:"bytes,4,opt,name=controller"`

	// AllocationTimestamp 保存资源分配发生的时间。该字段不保证一定被设置，未设置时该时间未知。
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	AllocationTimestamp *metav1.Time `json:"allocationTimestamp,omitempty" protobuf:"bytes,5,opt,name=allocationTimestamp"`
}

// DeviceAllocationResult 是设备分配的结果。
type DeviceAllocationResult struct {
	// Results 列出所有已分配的设备。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Results []DeviceRequestAllocationResult `json:"results,omitempty" protobuf:"bytes,1,opt,name=results"`

	// 本字段是声明与类所有配置参数的合并结果。驱动可以根据标志区分二者的来源。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=64
	Config []DeviceAllocationConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`
}

// AllocationResultsMaxSize 表示 allocation.devices.results 中的最大条目数。
const AllocationResultsMaxSize = 32

// DeviceRequestAllocationResult 包含单个请求对应的分配结果。
type DeviceRequestAllocationResult struct {
	// Request 是声明中导致本设备被分配的请求名称。如果它引用 DeviceRequest.firstAvailable 中的子请求，
	// 本字段必须使用 <main request>/<subrequest> 格式同时给出主请求与子请求名。
	// +required
	Request string `json:"request" protobuf:"bytes,1,name=request"`

	// Driver 指定 DRA 驱动的名称；当声明需要在某节点上使用时，会调用该驱动的 kubelet 插件来处理分配。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	Driver string `json:"driver" protobuf:"bytes,2,name=driver"`

	// 该名称与驱动名、设备名共同标识分配的是哪个设备（`<driver name>/<pool name>/<device name>`）。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-pool-name
	Pool string `json:"pool" protobuf:"bytes,3,name=pool"`

	// Device 通过设备在驱动资源池中的名称引用某一个设备实例，必须是 DNS 标签。
	// +required
	Device string `json:"device" protobuf:"bytes,4,name=device"`

	// AdminAccess 表示本设备是为管理访问而分配的，模式定义见请求中对应字段。
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,5,opt,name=adminAccess"`

	// 这是设备分配时请求中所有容忍的副本。
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// BindingConditions 是分配时对应 ResourceSlice 中 BindingConditions 的副本。
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,7,rep,name=bindingConditions"`

	// BindingFailureConditions 是分配时对应 ResourceSlice 中 BindingFailureConditions 的副本。
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,8,rep,name=bindingFailureConditions"`

	// ShareID 唯一标识设备的某一次独立分配份额，用于设备支持并发多次分配的场景。
	// 它作为附加的 map 键，用于区分同一设备的不同并发份额。
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *types.UID `json:"shareID,omitempty" protobuf:"bytes,9,opt,name=shareID"`

	// ConsumedCapacity 记录本次声明请求中每个设备实际消耗的容量。
	// 实际消耗量可能与请求量不同：若设备定义了 requestPolicy，会向上取整到最近的有效值
	// （即不会小于请求量）。
	// +optional
	// +featureGate=DRAConsumableCapacity
	ConsumedCapacity map[QualifiedName]resource.Quantity `json:"consumedCapacity,omitempty" protobuf:"bytes,10,rep,name=consumedCapacity"`

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，本已分配设备将跳过的节点本地资源操作
	// （gRPC 调用）。如果某驱动在声明中的所有已分配设备都跳过某操作，该 gRPC 调用就会被跳过。
	// 它是分配时 ResourceSlice.spec.skipNodeOperations 的副本。
	//
	// +optional
	// +listType=set
	// +k8s:listType=set
	// +featureGate=DRAOptionalNodeOperations
	// +k8s:optional
	SkipNodeOperations []SkipNodeOperation `json:"skipNodeOperations,omitempty" protobuf:"bytes,11,rep,name=skipNodeOperations,casttype=SkipNodeOperation"`
}

// DeviceAllocationConfiguration 会被嵌入到 AllocationResult 中。
type DeviceAllocationConfiguration struct {
	// Source 记录配置是来自类（因此普通用户无法设置）还是来自声明。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Source AllocationConfigSource `json:"source" protobuf:"bytes,1,name=source"`

	// Requests 列出配置适用的请求名称；为空时适用于所有请求。
	//
	// 引用子请求时必须包含主请求名，并可使用 <main request>[/<subrequest>] 格式附带子请求名。
	// 若只给出主请求名，则配置适用于其所有子请求。
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
// AllocationConfigSource 表示分配配置的来源。
type AllocationConfigSource string

// [DeviceAllocationConfiguration.Source] 的合法取值。
const (
	AllocationConfigSourceClass AllocationConfigSource = "FromClass"
	AllocationConfigSourceClaim AllocationConfigSource = "FromClaim"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// ResourceClaimList 是声明的集合。
type ResourceClaimList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是资源声明的列表。
	Items []ResourceClaim `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// DeviceClass 是由供应商或管理员提供的资源，包含设备配置与选择器。
// 声明的设备请求中可以引用它来套用这些预置设置。集群作用域。
type DeviceClass struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义可以分配什么以及如何配置。
	// +optional
	Spec DeviceClassSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// DeviceClassSpec 用于 [DeviceClass] 中定义可以分配什么以及如何配置。
type DeviceClassSpec struct {
	// 通过本类申领设备的请求必须满足每一个选择器。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,1,opt,name=selectors"`

	// Config 定义适用于通过本类申领的每台设备的配置参数，
	// 这些参数会传递给驱动，但在分配声明时不予考虑。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClassConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`

	// SuitableNodes 自 Kubernetes 1.32 起已被移除，此处保留墓碑。
	// 当不再需要解码 v1alpha3 时，可以复用该名称。

	// ExtendedResourceName 是本类设备的扩展资源名称。本类的设备可用于满足 Pod 的扩展资源请求。
	// 其格式与 Pod 扩展资源名称相同，在集群内所有设备类中应当保持唯一。
	// 若两个设备类同名，则后创建的类会被选中用于满足 Pod 的扩展资源请求；
	// 若两个类同时创建，则按名称字典序靠前的那个被选中。
	// +optional
	// +featureGate=DRAExtendedResource
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-extended-resource-name
	ExtendedResourceName *string `json:"extendedResourceName,omitempty" protobuf:"bytes,4,opt,name=extendedResourceName"`
}

// DeviceClassConfiguration 用于 DeviceClass。
type DeviceClassConfiguration struct {
	DeviceConfiguration `json:"" protobuf:"bytes,1,opt,name=deviceConfiguration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// DeviceClassList 是类的集合。
type DeviceClassList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是资源类的列表。
	Items []DeviceClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// ResourceClaimTemplate 用于生成 ResourceClaim 对象。
type ResourceClaimTemplate struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 描述将要生成的 ResourceClaim。
	// +optional
	Spec ResourceClaimTemplateSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// ResourceClaimTemplateSpec 包含 ResourceClaim 的元数据与字段。
type ResourceClaimTemplateSpec struct {
	// ObjectMeta 可以包含在创建时复制到 ResourceClaim 的标签与注解。
	// 不允许使用其他字段，校验时会拒绝。
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ResourceClaim 的 Spec，全部内容原样复制到由本模板创建的 ResourceClaim 中。
	// ResourceClaim 中有效的字段在这里同样有效。
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.32

// ResourceClaimTemplateList 是声明模板的集合。
type ResourceClaimTemplateList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是资源声明模板的列表。
	Items []ResourceClaimTemplate `json:"items" protobuf:"bytes,2,rep,name=items"`
}

const (
	// AllocatedDeviceStatusMaxConditions 表示设备状态中条件的最大数量。
	AllocatedDeviceStatusMaxConditions int = 8
	// AllocatedDeviceStatusDataMaxLength 表示设备状态中 Data 字段原始数据的最大长度。
	AllocatedDeviceStatusDataMaxLength int = 10 * 1024
	// NetworkDeviceDataMaxIPs 表示设备状态中 networkData 字段的最大 IP 数量。
	NetworkDeviceDataMaxIPs int = 16
	// NetworkDeviceDataInterfaceNameMaxLength 表示设备状态中 networkData.interfaceName 字段的最大字符数。
	NetworkDeviceDataInterfaceNameMaxLength int = 256
	// NetworkDeviceDataHardwareAddressMaxLength 表示设备状态中 networkData.hardwareAddress 字段的最大字符数。
	NetworkDeviceDataHardwareAddressMaxLength int = 128
)

// AllocatedDeviceStatus 包含已分配设备的状态（如果驱动选择上报），其中可能包含驱动特有的信息。
type AllocatedDeviceStatus struct {
	// Driver 指定 DRA 驱动的名称；当声明需要在某节点上使用时，会调用该驱动的 kubelet 插件来处理分配。
	//
	// 必须是 DNS 子域，并且最好以驱动供应商拥有的 DNS 域结尾，只应使用小写字符。
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,rep,name=driver"`

	// 该名称与驱动名、设备名共同标识分配的是哪个设备（`<driver name>/<pool name>/<device name>`）。
	//
	// 长度不得超过 253 个字符，可以包含多个由斜杠分隔的 DNS 子域。
	//
	// +required
	Pool string `json:"pool" protobuf:"bytes,2,rep,name=pool"`

	// Device 通过设备在驱动资源池中的名称引用某一个设备实例，必须是 DNS 标签。
	//
	// +required
	Device string `json:"device" protobuf:"bytes,3,rep,name=device"`

	// ShareID 唯一标识设备的某一次独立分配份额。
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *string `json:"shareID,omitempty" protobuf:"bytes,7,opt,name=shareID"`

	// Conditions 包含对设备状态的最新观测。如果设备已按类与声明的配置引用完成配置，
	// 则 `Ready` 条件应为 True。
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions" protobuf:"bytes,4,opt,name=conditions"`

	// Data 包含任意驱动特有的数据。
	// +optional
	Data *runtime.RawExtension `json:"data,omitempty" protobuf:"bytes,5,opt,name=data"`

	// NetworkData 包含与设备相关的网络信息。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	NetworkData *NetworkDeviceData `json:"networkData,omitempty" protobuf:"bytes,6,opt,name=networkData"`
}

// NetworkDeviceData 为已分配设备提供网络相关的详细信息。
// 驱动或其他组件可以填写这些信息，用于在网络环境中配置或识别该设备。
type NetworkDeviceData struct {
	// InterfaceName 指定与已分配设备关联的网络接口名称。
	// 它可能是正在 Pod 中配置的物理或虚拟网络接口的名称。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=256
	InterfaceName string `json:"interfaceName,omitempty" protobuf:"bytes,1,opt,name=interfaceName"`

	// IPs 列出分配给设备网络接口的网络地址，可同时包含 IPv4 与 IPv6 地址。
	// 地址采用 CIDR 表示法，同时包含地址本身与相应的子网掩码，
	// 例如 IPv4 的 "192.0.2.5/24" 和 IPv6 的 "2001:db8::5/64"。
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=16
	IPs []string `json:"ips,omitempty" protobuf:"bytes,2,opt,name=ips"`

	// HardwareAddress 表示设备网络接口的硬件地址（例如 MAC 地址）。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=128
	HardwareAddress string `json:"hardwareAddress,omitempty" protobuf:"bytes,3,opt,name=hardwareAddress"`
}
