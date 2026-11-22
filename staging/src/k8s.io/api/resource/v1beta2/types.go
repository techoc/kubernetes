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

package v1beta2

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
)

const (
	// Finalizer 是由内置控制器分配的 ResourceClaim 上设置的 finalizer。
	// 该 finalizer 保留给 Kubernetes 使用，DRA 驱动控制器必须使用自己的 finalizer。
	Finalizer = "resource.kubernetes.io/delete-protection"
	// ExtendedResourceClaimAnnotation 是添加到生成的特殊 ResourceClaim 上的注解，
	// 唯一合法值为 "true"。该注解仅在调度器内部使用。
	ExtendedResourceClaimAnnotation = "resource.kubernetes.io/extended-resource-claim"
	// PodResourceClaimAnnotation 由 ResourceClaim 控制器设置在模板生成的 ResourceClaim 上，
	// 其值为生成该 claim 所对应的 pod.spec.resourceClaims[].name。
	PodResourceClaimAnnotation = "resource.kubernetes.io/pod-claim-name"
	// 设备类资源前缀用于在没有指定 ExtendedResourceName 字段时，为设备类生成隐式的扩展资源名。
	// 生成的名字为该前缀加上设备类名。生成的名字可能不是 pod.Spec.Resources.Requests 中合法的
	// 扩展资源名，此时必须在设备类中显式指定一个合法名称。
	ResourceDeviceClassPrefix string = "deviceclass.resource.kubernetes.io/"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ResourceSlice 表示由同一个驱动管理的、由相似资源组成的资源池中的一个或多个资源。
// 一个池可能跨多个 ResourceSlice，具体由多少个 ResourceSlice 组成由驱动决定。
//
// 目前仅支持带属性和容量的设备。同一池中的每个设备，无论分布在多少个 ResourceSlice 上，
// 都必须有唯一名称。设备发布在哪个 ResourceSlice 中可能随时间变化；设备的唯一标识是
// <driver name>、<pool name>、<device name> 三元组。
//
// 驱动更新池时，会递增 pool.Spec.Pool.Generation，并以新的编号和资源定义更新所有 ResourceSlice。
// 消费方只能使用具有最高 generation 的 ResourceSlice，忽略其他所有版本。
//
// 当分配池中满足特定条件的所有资源，或在多个备选方案中寻找最佳方案时，消费方应检查池中的
// ResourceSlice 数量（每个 ResourceSlice 中都包含该数量），以判断自己对池的视图是否完整；
// 若不完整，应等待驱动完成池的更新。
//
// 对于非节点本地资源，不设置节点名；驱动可以使用节点选择器指定设备的可用位置。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation feature gate。
type ResourceSlice struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 包含驱动发布的信息。
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

// ResourceSliceSpec 包含驱动在一个 ResourceSlice 中发布的信息。
type ResourceSliceSpec struct {
	// Driver 标识提供容量信息的 DRA 驱动。可以使用字段选择器只列出
	// 具有特定驱动名的 ResourceSlice 对象。
	//
	// 必须是 DNS 子域，且应以驱动供应商拥有的 DNS 域结尾；只能使用小写字符。
	// 该字段不可变。
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Pool 描述该 ResourceSlice 所属的资源池。
	//
	// +required
	Pool ResourcePool `json:"pool" protobuf:"bytes,2,name=pool"`

	// NodeName 标识提供该池中资源的节点。可以使用字段选择器只列出
	// 属于某个节点的 ResourceSlice 对象。
	//
	// 该字段可用于限制节点只能访问具有相同节点名的 ResourceSlice。它也向自动扩缩容器表明：
	// 增加与现有节点同类型的新节点，可能也会带来新的可用资源。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	// 该字段不可变。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// NodeSelector 定义当池不限于单个节点时，哪些节点可以访问池中的资源。
	//
	// 必须且只能使用一个 term。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,4,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问池中的资源。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"bytes,5,opt,name=allNodes"`

	// Devices 列出该池中部分或全部设备。
	//
	// 不得超过 128 项。如果任一设备使用了 taint 或消费计数器，则上限为 64。
	//
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +zeroOrOneOf=ResourceSliceType
	Devices []Device `json:"devices,omitempty" protobuf:"bytes,6,name=devices"`

	// PerDeviceNodeSelection 定义节点对池中资源的访问是在 ResourceSlice 级别设置，
	// 还是在每个设备上单独设置。若为 true，ResourceSlice 中定义的每个设备都必须单独指定。
	//
	// NodeName、NodeSelector、AllNodes 和 PerDeviceNodeSelection 必须且只能设置一个。
	//
	// +optional
	// +oneOf=NodeSelection
	// +featureGate=DRAPartitionableDevices
	PerDeviceNodeSelection *bool `json:"perDeviceNodeSelection,omitempty" protobuf:"bytes,7,name=perDeviceNodeSelection"`

	// SharedCounters 定义计数器集合列表，每个集合都有一个名称和可用的计数器列表。
	//
	// 计数器集合的名称在 ResourcePool 中必须唯一。
	//
	// 一个 ResourceSlice 中只能设置 Devices 或 SharedCounters 之一。
	//
	// 计数器集合最多 8 个。
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

	// PartitionTypeAttribute 按完全限定名（例如 "gpu.example.com/profile"）指定一个字符串类型的
	// 设备属性，其值用于标记每个设备的分区类型，例如 MIG 风格 GPU 的 "Full" 或 "Half"。
	//
	// 设置后，slice 中的每个可分区设备都必须带有该属性，且属性值相同的设备必须共享相同的
	// ConsumesCounters 开销。
	//
	// +optional
	// +featureGate=DRAPartitionableDevicesType
	// +k8s:ifDisabled(DRAPartitionableDevicesType)=+k8s:forbidden
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:optional
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:format=k8s-resource-fully-qualified-name
	PartitionTypeAttribute *FullyQualifiedName `json:"partitionTypeAttribute,omitempty" protobuf:"bytes,9,opt,name=partitionTypeAttribute"`

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，对该 slice 中设备跳过的
	// 节点本地资源操作（gRPC 调用）。如果某个 claim 中某驱动的所有已分配设备都跳过某项操作，
	// 该 gRPC 调用就会被跳过。有效值为：
	//
	// - "NodePrepareResources"：跳过 NodePrepareResources gRPC 调用。除非同时列出
	//   "NodeUnprepareResources"（或指定 "*"），否则不能指定该值。
	// - "NodeUnprepareResources"：跳过 NodeUnprepareResources gRPC 调用。
	// - "*"：跳过所有节点本地资源操作。
	//
	// 未来可能添加其他值。kubelet 必须忽略未知值。
	//
	// +optional
	// +listType=set
	// +k8s:listType=set
	// +featureGate=DRAOptionalNodeOperations
	// +k8s:optional
	SkipNodeOperations []SkipNodeOperation `json:"skipNodeOperations,omitempty" protobuf:"bytes,10,rep,name=skipNodeOperations,casttype=SkipNodeOperation"`
}

// SkipNodeOperation 表示可跳过的节点本地资源操作。
//
// +enum
// +k8s:enum
type SkipNodeOperation string

const (
	// SkipNodeOperationNodePrepareResources 表示跳过 NodePrepareResources gRPC 调用。
	SkipNodeOperationNodePrepareResources SkipNodeOperation = "NodePrepareResources"

	// SkipNodeOperationNodeUnprepareResources 表示跳过 NodeUnprepareResources gRPC 调用。
	SkipNodeOperationNodeUnprepareResources SkipNodeOperation = "NodeUnprepareResources"

	// SkipNodeOperationAll 表示跳过所有节点本地资源操作。
	SkipNodeOperationAll SkipNodeOperation = "*"
)

// CounterSet 定义一个具名计数器集合，供 ResourcePool 中定义的设备使用。
//
// 计数器本身不可分配，但可被设备引用。设备被分配后，它所占用的那部分计数器
// 将不再可供其他设备使用。
type CounterSet struct {
	// Name 定义计数器集合的名称，必须是 DNS label。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Counters 定义该 CounterSet 中的计数器集合。每个计数器的名称在集合内必须唯一，
	// 且必须是 DNS label。
	//
	// 计数器最多 32 个。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,name=counters"`
}

// DriverNameMaxLength 是 ResourceSliceSpec 等处驱动名的最大合法长度，与 CSI 驱动名相同。
const DriverNameMaxLength = 63

// ResourcePool 描述 ResourceSlice 所属的资源池。
type ResourcePool struct {
	// Name 用于标识池。对于节点本地设备，它通常是节点名，但并非必须。
	// 可以使用字段选择器只列出属于某个池的 ResourceSlice 对象。
	//
	// 长度不得超过 253 个字符，且必须由一个或多个以斜杠分隔的 DNS 子域组成。
	// 该字段不可变。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Generation 跟踪池随时间的变化。驱动每次更改池中一个或多个资源时，都必须更新该池所有
	// ResourceSlice 的 generation。ResourceSlice 的消费方只应考虑 generation 最大的池资源。
	// 驱动可以重置 generation，只要池中所有 ResourceSlice 都被更新或删除，消费方就不受影响。
	//
	// 与 ResourceSliceCount 结合后，该机制使消费方能够识别出由多个 ResourceSlice 组成、
	// 但处于不完整状态的池。
	//
	// +required
	Generation int64 `json:"generation" protobuf:"bytes,2,name=generation"`

	// ResourceSliceCount 是该 generation 下池中 ResourceSlice 的总数，必须大于零。
	//
	// 消费方可以用它检查是否已看到同一池中的所有 ResourceSlice。
	//
	// +required
	ResourceSliceCount int64 `json:"resourceSliceCount" protobuf:"bytes,3,name=resourceSliceCount"`
}

// ResourceSliceMaxSharedCapacity 是单个 ResourceSlice 可声明的最大共享容量条目数。
const ResourceSliceMaxSharedCapacity = 128

// ResourceSliceMaxDevices 是单个 ResourceSlice 中允许的最大设备数。
const ResourceSliceMaxDevices = 128

// ResourceSliceMaxDevicesWithAdvancedFeatures 定义当任一设备使用高级特性时，
// 一个 ResourceSlice 中允许的最大设备数。高级特性包括：
// - 设备 taint（DRADeviceTaints feature gate）
// - 消费计数器（DRAPartitionableDevices feature gate）
// - 列表属性（DRAListTypeAttributes feature gate）
const ResourceSliceMaxDevicesWithAdvancedFeatures = 64

// PoolNameMaxLength 是池名的最大长度，与单个节点名相同。
const PoolNameMaxLength = validation.DNS1123SubdomainMaxLength // 与单个节点名相同。
// BindingConditionsMaxSize 是每设备绑定条件的最大数量。
const BindingConditionsMaxSize = 4

// BindingFailureConditionsMaxSize 是每设备绑定失败条件的最大数量。
const BindingFailureConditionsMaxSize = 4

// ResourceSliceMaxCounterSets 定义一个 ResourceSlice 中可定义的计数器集合
// （通过 SharedCounters 字段）的最大数量。
const ResourceSliceMaxCounterSets = 8

// ResourceSliceMaxCountersPerCounterSet 定义一个计数器集合中可定义的计数器最大数量。
const ResourceSliceMaxCountersPerCounterSet = 32

// ResourceSliceMaxDeviceCounterConsumptionsPerDevice 定义每个设备可声明的
// 计数器消费（通过 ConsumesCounters 字段）的最大数量。
const ResourceSliceMaxDeviceCounterConsumptionsPerDevice = 2

// ResourceSliceMaxCountersPerDeviceCounterConsumption 定义每次设备计数器消费中
// 可声明的计数器最大数量。
const ResourceSliceMaxCountersPerDeviceCounterConsumption = 32

// DeviceCompatibilityGroupsMaxSize 定义每次设备计数器消费中可声明的兼容组最大数量。
const DeviceCompatibilityGroupsMaxSize = 2

// Device 表示一个可根据属性选择的独立硬件实例。除 name 外，必须且只能设置一个字段。
type Device struct {
	// Name 是池中该驱动管理的所有设备之间的唯一标识，必须是 DNS label。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Attributes 定义该设备的属性集合。每个属性的名称在集合内必须唯一。
	//
	// 属性与容量的总数最多为 32。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Attributes map[QualifiedName]DeviceAttribute `json:"attributes,omitempty" protobuf:"bytes,2,rep,name=attributes"`

	// Capacity 定义该设备的容量集合。每个容量的名称在集合内必须唯一。
	//
	// 属性与容量的总数最多为 32。
	//
	// +optional
	Capacity map[QualifiedName]DeviceCapacity `json:"capacity,omitempty" protobuf:"bytes,3,rep,name=capacity"`

	// ConsumesCounters 定义对 sharedCounters 的引用列表，以及设备将从这些计数器集合中
	// 消费的计数器集合。
	//
	// 每个 counterSet 只能有一个条目。
	//
	// 每个设备最多可有 2 次计数器消费。
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

	// NodeName 标识设备可用的节点。
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,5,opt,name=nodeName"`

	// NodeSelector 定义设备可用的节点。
	//
	// 必须且只能使用一个 term。
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,6,opt,name=nodeSelector"`

	// AllNodes 表示所有节点都可以访问该设备。
	//
	// 仅当 Spec.PerDeviceNodeSelection 为 true 时才能设置。
	// NodeName、NodeSelector 和 AllNodes 最多只能设置一个。
	//
	// +optional
	// +oneOf=DeviceNodeSelection
	// +featureGate=DRAPartitionableDevices
	AllNodes *bool `json:"allNodes,omitempty" protobuf:"bytes,7,opt,name=allNodes"`

	// 若指定，这些是驱动定义的 taint。
	//
	// taint 最多 16 个。如果 ResourceSlice 中任一设备设置了 taint，
	// 则该 ResourceSlice 允许的最大设备数从 128 降为 64。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Taints []DeviceTaint `json:"taints,omitempty" protobuf:"bytes,8,rep,name=taints"`

	// BindsToNode 表示涉及该设备的分配的使用是否必须限定在分配 claim 时所选的那个节点上。
	// 若设为 true，调度器会设置 ResourceClaim.Status.Allocation.NodeSelector，
	// 使其匹配进行分配的节点。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	BindsToNode *bool `json:"bindsToNode,omitempty" protobuf:"varint,9,opt,name=bindsToNode"`

	// BindingConditions 定义继续进行绑定的条件。在调度 Pod 时，必须在每设备状态条件中
	// 将所有这些条件设为 True，才能继续将 Pod 绑定到节点。
	//
	// 绑定条件最多 4 个。
	//
	// 条件必须是合法的 condition type 字符串。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,10,rep,name=bindingConditions"`

	// BindingFailureConditions 定义绑定失败的条件。这些条件可在每设备状态条件中设置。
	// 若其中任意一个为 "True"，则表示发生了绑定失败。
	//
	// 绑定失败条件最多 4 个。
	//
	// 条件必须是合法的 condition type 字符串。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,11,rep,name=bindingFailureConditions"`

	// AllowMultipleAllocations 标记该设备是否允许被分配到多个 DeviceRequest。
	//
	// 若设为 true，该设备可以被多次分配，且无论是否定义 requestPolicy，其全部容量都可被消费。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	AllowMultipleAllocations *bool `json:"allowMultipleAllocations,omitempty" protobuf:"bytes,12,opt,name=allowMultipleAllocations"`

	// NodeAllocatableResourceMappings 已废止，被 NodeAllocatableResources 取代。
	// NodeAllocatableResourceMappings map[v1.ResourceName]NodeAllocatableResourceMapping `json:"nodeAllocatableResourceMappings,omitempty" protobuf:"bytes,13,opt,name=nodeAllocatableResourceMappings"`

	// NodeAllocatableResources 定义由暴露该设备的 DRA 驱动管理的节点资源映射。
	// 包括 v1.Node `status.allocatable` 中当前上报的非扩展资源
	// （参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources），
	// 例如 "cpu"、"memory"、"ephemeral-storage" 和 hugepages。
	// 除了通过 Pod `spec` 发起的标准请求外，这些资源也可通过 claim 请求并由 DRA 驱动分配。
	// 例如 CPU DRA 驱动可分配独占 CPU，或分配加速器设备所需的辅助节点内存依赖。
	// 该 map 的键是节点可分配资源名（如 "cpu"、"memory"），不允许使用扩展资源名作为键。
	// +optional
	// +k8s:optional
	// +featureGate=DRANodeAllocatableResources
	NodeAllocatableResources map[v1.ResourceName]NodeAllocatableResource `json:"nodeAllocatableResources,omitempty" protobuf:"bytes,14,opt,name=nodeAllocatableResources"`
}

// NodeAllocatableResource 定义请求的 DRA 设备/容量单位与对应节点可分配资源数量之间的换算。
// Mapping 或 Overhead 至少指定一个；两者都不指定属于非法配置。
type NodeAllocatableResource struct {
	// 当设备直接建模节点可分配资源（如标准 CPU 或内存，例如 CPU DRA 驱动）时使用 Mapping。
	// 计算出的数量在节点上按每个 claim 实例只计一次。为避免节点 cgroup 隔离冲突，
	// 调度器明确禁止在多个 Pod 之间共享映射设备的 claim。
	// +optional
	// +k8s:optional
	Mapping *NodeAllocatableMapping `json:"mapping,omitempty" protobuf:"bytes,3,opt,name=mapping"`

	// 当分配的设备本身不建模节点可分配资源（例如 GPU 的主机内存开销）时，
	// Overhead 中的字段用于建模节点可分配资源上的辅助开销。
	// 允许在多个 Pod 之间共享带有开销映射的 claim；节点可分配开销会按引用该 claim 的每个 Pod 分别计算。
	// 即使对同一资源指定了 mapping，开销也始终从该资源的节点可分配容量中扣除。
	// 例如：若设备通过 Mapping（带 CapacityKey）将每个 socket 的内存容量建模为可消费容量池，
	// 则对同一资源指定的任何开销都会从节点总体可分配容量中扣除，而非从 Mapping 中的每 socket 容量池扣除。
	// +optional
	// +k8s:optional
	Overhead *NodeAllocatableOverhead `json:"overhead,omitempty" protobuf:"bytes,4,opt,name=overhead"`
}

// NodeAllocatableMapping 定义 DRA 分配如何直接换算为节点可分配资源数量。
// 映射可来自已分配设备数量（通过 deviceMultiplier）或所消费的具体容量（通过 capacityKey 和 capacityMultiplier），
// 两者互斥。
// Kubelet 会将 claim 的映射资源数量同时加到 Pod 级 cgroup 的 requests 和 limits，
// 并加到每个引用该 claim 的容器的容器级 cgroup limits 上。
type NodeAllocatableMapping struct {
	// CapacityKey 引用 `spec.devices[*].capacity` map 中定义的容量名。
	// 设置该字段后，`status.allocation.devices.results[*].consumedCapacity` map 中该键对应的值
	// （针对特定 claim 分配）决定节点可分配资源的基础数量。必须同时设置 `capacityMultiplier`，
	// 它会与该基础数量相乘。
	// 例如，若 `spec.devices[*].capacity` 中有条目 "dra.example.com/memory": "128Gi"，
	// 且本字段设为 "dra.example.com/memory"，则对消费了 { "dra.example.com/memory": "4Gi" } 的
	// claim 分配，节点可分配资源映射的基础数量为 "4Gi"。
	// 最终节点可分配资源数量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityMultiplier")
	CapacityKey *QualifiedName `json:"capacityKey,omitempty" protobuf:"bytes,1,opt,name=capacityKey"`

	// CapacityMultiplier 用作所消费已分配容量的乘数，仅在设置了 `capacityKey` 时有效。
	// 最终节点可分配资源数量为 `consumedCapacity[capacityKey]` * `capacityMultiplier`。
	// 例如，若消费了设备的 "dra.example.com/cores" 容量，且每个 "core" 提供 2 个 "cpu"，
	// 则映射为：{ResourceName: "cpu", capacityKey: "dra.example.com/cores", capacityMultiplier: "2"}。
	// 若 claim 消费了 8 个 "dra.example.com/cores"，则 CPU 占用为 8 * 2 = 16。
	// +optional
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:dependentRequired("capacityKey")
	CapacityMultiplier *resource.Quantity `json:"capacityMultiplier,omitempty" protobuf:"bytes,2,opt,name=capacityMultiplier"`

	// DeviceMultiplier 用作 claim 中已分配设备数量的乘数。
	// 最终节点可分配资源数量为 `deviceCount` * `deviceMultiplier`。
	// 例如，将每个 cache complex（CCX）表示为一个设备的 DRA 驱动，其 `nodeAllocatableResources` 中
	// 会有 {ResourceName: "cpu", deviceMultiplier: "8"}。
	// 若为该 claim 分配了 2 个设备（CCX），则认为分配了 2 * 8 = 16 个 CPU。
	// 仅在未设置 `capacityKey` 和 `capacityMultiplier` 时有效。
	// +optional
	// +k8s:optional
	// +k8s:unionMember
	DeviceMultiplier *resource.Quantity `json:"deviceMultiplier,omitempty" protobuf:"bytes,3,opt,name=deviceMultiplier"`
}

// NodeAllocatableOverhead 定义分配设备时产生的辅助资源开销。
// 开销可指定为引用该 claim 的每个 Pod 的固定成本、每次容器引用的可变成本，或两者兼有。
// Kubelet 通过将该开销加到引用容器的 Pod 级和容器级 cgroup 来进行记账。
type NodeAllocatableOverhead struct {
	// PerPod 是引用本节点上该 claim 的每个 Pod 各应用一次的开销，
	// 即每个引用该 claim 的 Pod 都会产生的固定开销。
	// +optional
	// +k8s:optional
	PerPod *resource.Quantity `json:"perPod,omitempty" protobuf:"bytes,1,opt,name=perPod"`

	// PerContainer 按每次容器对该 claim 的引用应用，用于建模随主动使用该设备的容器数量线性增长的开销。
	// 同时指定 PerPod 和 PerContainer 时，引用该 claim 的每个 Pod 分配的总开销计算为：
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet 在 cgroup 中这样记账：
	// - Pod 级 cgroup（requests 和 limits）：加上 PerPod + (PerContainer * NumReferences)。
	// - 容器级 cgroup（仅 limits）：每个引用容器加上 PerPod + PerContainer。
	// 这样任一容器都可访问 Pod 级开销，同时父 cgroup 限制总用量，确保 PerPod 只计一次。
	// +optional
	// +k8s:optional
	PerContainer *resource.Quantity `json:"perContainer,omitempty" protobuf:"bytes,2,opt,name=perContainer"`
}

// DeviceCounterConsumption 定义设备将从某个 CounterSet 消费的一组计数器。
type DeviceCounterConsumption struct {
	// CounterSet 是要消费的计数器所属集合的名称。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-short-name
	CounterSet string `json:"counterSet" protobuf:"bytes,1,opt,name=counterSet"`

	// Counters 定义设备将要消费的计数器。
	//
	// 计数器最多 32 个。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:eachKey=+k8s:format=k8s-short-name
	Counters map[string]Counter `json:"counters,omitempty" protobuf:"bytes,2,opt,name=counters"`

	// CompatibilityGroups 是本次计数器集合消费的不透明组名列表。
	//
	// 从同一计数器集合消费计数的设备只有在至少共享一个组时才能同时分配（"共同分配"）：
	// 在该计数器集合上所有共同分配设备的 CompatibilityGroups 交集必须非空。
	// 从不同计数器集合消费的设备永远不会通过本字段进行比较。
	//
	// 未设置、显式 nil 和空列表等价，均表示"无组"：此类设备只能与同一计数器集合上
	// 同样无组的同级设备共同分配，绝不会与声明了一个或多个组的设备共同分配。
	//
	// 组名是不透明的，仅在发布该资源的驱动池内有意义。
	//
	// 组最多 2 个，且名称必须唯一。
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
	// Value 定义设备拥有的某项容量的大小。
	//
	// 该字段反映固定的总容量，不会改变。已消费数量由调度器单独跟踪，不影响此值。
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`

	// RequestPolicy 定义当设备允许被多个分配共享时，该 DeviceCapacity 必须如何被消费。
	//
	// 只有设备的 allowMultipleAllocations 为 true 时才能设置 requestPolicy。
	//
	// 未设置时，容量请求不受约束：只要所有分配消费的总量不超过设备定义的容量，
	// 请求可消费任意数量的容量。若请求也未设置，则默认为全部容量值。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	RequestPolicy *CapacityRequestPolicy `json:"requestPolicy,omitempty" protobuf:"bytes,2,opt,name=requestPolicy"`
}

// Counter 描述与设备关联的数量。
type Counter struct {
	// Value 定义某项设备计数器可用的数量。
	//
	// +required
	Value resource.Quantity `json:"value" protobuf:"bytes,1,rep,name=value"`
}

// CapacityRequestPolicy 定义请求如何消费设备容量。
//
// 最多只能设置一个 ValidRequestValues。
type CapacityRequestPolicy struct {
	// Default 指定当请求在 DeviceRequest 的 Capacity 中没有对应条目时，
	// 该容量默认被消费的数量。
	//
	// +optional
	Default *resource.Quantity `json:"default" protobuf:"bytes,1,opt,name=default"`

	// ValidValues 定义消费请求中可接受的数量值集合。
	//
	// 不得超过 10 项，且必须按升序排列。
	//
	// 设置该字段时，必须定义 Default，且 Default 必须包含在 ValidValues 列表中。
	//
	// 若请求数量不匹配任何有效值但小于某些有效值，调度器会计算出大于等于该请求的最小有效值，
	// 即：min(ceil(requestedValue) ∈ validValues)，其中 requestedValue ≤ max(validValues)。
	//
	// 若请求数量超过所有有效值，则该请求违反策略，无法分配该设备。
	//
	// +optional
	// +listType=atomic
	// +oneOf=ValidRequestValues
	ValidValues []resource.Quantity `json:"validValues,omitempty" protobuf:"bytes,3,opt,name=validValues"`

	// ValidRange 定义消费请求中可接受的数量范围。
	//
	// 设置该字段时，必须定义 Default，且 Default 必须落在 ValidRange 范围内。
	//
	// 若请求数量不在该范围内，则请求违反策略，无法分配该设备。
	//
	// 若请求不包含该容量条目，则使用 Default 值。
	//
	// +optional
	// +oneOf=ValidRequestValues
	ValidRange *CapacityRequestPolicyRange `json:"validRange,omitempty" protobuf:"bytes,4,opt,name=validRange"`
}

// CapacityRequestPolicyRange 定义可消费容量值的有效范围。
//
// 若启用了 DRAFractionalCapacityRange feature gate，且 Min、Max 或 Step 中至少有一个是分数数量
// （即其值不是整数），则改用毫单位算术，支持最多 3 位小数（例如 100m = 0.1）。
// 此时支持的最大值比使用 64 位整数时小 1000 倍。否则所有比较都通过 resource.Quantity.Value()
// 使用 64 位整数算术。
//
//   - 若请求数量小于 Min，则向上取整为 Min。
//   - 若设置了 Step，且请求数量在 Min 与 Max 之间但未与 Step 对齐，则向上取整到 Min + (n * Step)。
//   - 若未设置 Step，且请求数量落在 Min 到 Max（若设置）范围内，则原样使用。
//   - 若请求或取整后的数量超过 Max（若设置），则请求不满足策略，无法分配该设备。
type CapacityRequestPolicyRange struct {
	// Min 指定消费请求允许的最小容量。
	//
	// Min 必须大于等于零，且小于等于容量值。
	// requestPolicy.default 必须大于等于该最小值。
	//
	// +required
	Min *resource.Quantity `json:"min,omitempty" protobuf:"bytes,1,opt,name=min"`

	// Max 定义可请求容量的上限。
	//
	// Max 必须小于等于容量值。Min 和 requestPolicy.default 必须小于等于该上限。
	//
	// +optional
	Max *resource.Quantity `json:"max,omitempty" protobuf:"bytes,2,opt,name=max"`

	// Step 定义范围内有效容量值之间的步长。
	//
	// Max（若设置）和 requestPolicy.default 必须是 Step 的倍数。
	// Min + Step 必须小于等于容量值。
	//
	// +optional
	Step *resource.Quantity `json:"step,omitempty" protobuf:"bytes,3,opt,name=step"`
}

// 属性与容量条目数之和的上限。
const ResourceSliceMaxAttributesAndCapacitiesPerDevice = 32

// 每个设备上列表属性和非列表属性中 string、version、bool 或 int 值的总数上限。
const ResourceSliceMaxAttributeValuesPerDevice = 48

// QualifiedName 是设备属性或容量的名称。
//
// 属性和容量由特定驱动的所有者（通常是供应商）或第三方（例如 Kubernetes 项目）定义。
// 由于有时会在设备之间比较它们，同一名称在所有设备上应含义相同、类型相同。
//
// 名称必须是 C 标识符（例如 "theName"），或 DNS 子域加斜杠（"/"）再加 C 标识符
// （例如 "dra.example.com/theName"）。不含域前缀的名称被视为驱动域的一部分。
// 第三方定义的属性或容量必须包含域前缀。
//
// DNS 子域最大长度为 63 个字符（与驱动名相同），C 标识符最大长度为 32。
type QualifiedName string

// FullyQualifiedName 是设置了域的 QualifiedName。
// 无法为该类型添加格式校验，因为其用途之一 DistinctAttribute 是有条件校验的，
// 而这种条件校验无法用声明式方式表达。
type FullyQualifiedName string

// DeviceMaxDomainLength 是完全限定名中域前缀的最大长度。
const DeviceMaxDomainLength = 63

// DeviceMaxIDLength 是设备属性或容量名（`<domain>/<ID>`）中标识符的最大长度。
const DeviceMaxIDLength = 32

// DeviceAttribute 必须且只能设置一个字段。
type DeviceAttribute struct {
	// 下面的 Go 字段名带有 Value 后缀，是为了避免字段 "String" 与同名方法冲突；该方法是必需的。
	// Kubernetes API 中的定义不带该后缀，以保持更自然的命名。

	// IntValue 是数字。
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

	// VersionValue 是符合 semver.org 2.0.0 规范的语义化版本，长度不得超过 64 个字符。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	VersionValue *string `json:"version,omitempty" protobuf:"bytes,5,opt,name=version"`

	// IntValues 是非空数字列表。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes feature gate。
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	IntValues []int64 `json:"ints,omitempty" protobuf:"varint,6,opt,name=ints"`

	// BoolValues 是非空的 true/false 值列表。
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	BoolValues []bool `json:"bools,omitempty" protobuf:"varint,7,opt,name=bools"`

	// StringValues 是非空字符串列表，每个字符串长度不得超过 64 个字符。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes feature gate。
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:maxBytes=64
	// +featureGate=DRAListTypeAttributes
	StringValues []string `json:"strings,omitempty" protobuf:"bytes,8,opt,name=strings"`

	// VersionValues 是符合 semver.org 2.0.0 规范的语义化版本非空列表，
	// 每个版本字符串长度不得超过 64 个字符。
	//
	// 这是 alpha 字段，需要启用 DRAListTypeAttributes feature gate。
	//
	// +optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:unionMember
	// +featureGate=DRAListTypeAttributes
	VersionValues []string `json:"versions,omitempty" protobuf:"bytes,9,opt,name=versions"`
}

// DeviceAttributeMaxValueLength 是字符串或版本属性值的最大长度。
const DeviceAttributeMaxValueLength = 64

// DeviceTaintsMaxLength 是每个 Device 的最大 taint 数量。
const DeviceTaintsMaxLength = 16

// 该 taint 所附加的设备会对任何不容忍该 taint 的 claim 产生相应的 "effect"，
// 并通过该 claim 影响到使用它的 Pod。
//
// +protobuf.options.(gogoproto.goproto_stringer)=false
type DeviceTaint struct {
	// 要应用到设备上的 taint 键，必须是 label 名。
	//
	// +required
	Key string `json:"key" protobuf:"bytes,1,name=key"`

	// 与 taint 键对应的 taint 值，必须是 label 值。
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`

	// taint 对不容忍它的 claim 的影响，并通过这些 claim 影响使用它们的 Pod。
	//
	// 有效 effect 为 None、NoSchedule 和 NoExecute。节点上使用的 PreferNoSchedule 在这里无效。
	// 未来可能添加更多 effect。消费方必须将未知 effect 当作 None 处理。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Effect DeviceTaintEffect `json:"effect" protobuf:"bytes,3,name=effect,casttype=DeviceTaintEffect"`

	// ^^^^
	//
	// 实现 PreferNoSchedule 依赖 DRA 的评分方案，可能会随之一起加入。
	//
	// 未来可能新增 effect NoExecuteWithPodDisruptionBudget：
	// 遵循 PodDisruptionBudget，而非直接删除 Pod。目前尚未决定，也可能作为独立字段。
	//
	// 校验必须允许存储对象中出现未知枚举值，这样才能在单个版本内添加新枚举而无需渐进式放开校验。

	// TimeAdded 表示添加 taint 的时间，或（仅在 DeviceTaintRule 中）修改 effect 的时间。
	// 未设置时会在创建或更新时自动添加。
	//
	// 此外，在 DeviceTaintRule 中，若更新时提供的值与旧值相同而新 effect 不同，
	// 则提供的值会被替换为当前时间。可以在保持 effect 不变的情况下修改键和/或值，
	// 此时不会更新时间戳，因为使用该时间戳的驱逐要么已开始（NoExecute），
	// 要么尚未开始（NoEffect、NoSchedule）。
	//
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty" protobuf:"bytes,4,opt,name=timeAdded"`

	// ^^^
	//
	// 节点 taint 中该字段定义为"仅对 NoExecute taint 写入"。但实际上 Kubernetes 从未使用它
	// （没有校验、没有默认值，在 pkg/controller/tainteviction 的 Pod 驱逐中也被忽略）。
}

// DeviceTaintEffect 表示设备 taint 对不容忍它的 claim 及 Pod 的影响。
//
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type DeviceTaintEffect string

const (
	// 无影响，该 taint 仅提供信息。
	DeviceTaintEffectNone DeviceTaintEffect = "None"

	// 不允许新的 Pod 调度到使用带 taint 设备的节点，除非它们容忍该 taint；
	// 但允许所有未经调度器直接提交给 Kubelet 的 Pod 启动，并允许所有已运行的 Pod 继续运行。
	DeviceTaintEffectNoSchedule DeviceTaintEffect = "NoSchedule"

	// 驱逐所有不容忍该设备 taint 的已运行 Pod。
	DeviceTaintEffectNoExecute DeviceTaintEffect = "NoExecute"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// DeviceTaintRule 向所有匹配选择器的设备添加一个 taint。
// 其效果与 DRA 驱动直接在 ResourceSlice 中指定该 taint 相同。
// +k8s:supportsSubresource="/status"
type DeviceTaintRule struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 指定选择器和一个 taint。
	//
	// 修改 spec 会自动递增 metadata.generation。
	// +required
	Spec DeviceTaintRuleSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 提供 spec 中所请求内容的相关信息。
	//
	// +optional
	Status DeviceTaintRuleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeviceTaintRuleSpec 指定选择器和一个 taint。
type DeviceTaintRuleSpec struct {
	// DeviceSelector 定义 taint 应用到哪些设备上。设备必须满足所有选择条件才算匹配。
	// 空选择器匹配所有设备；没有选择器则不匹配任何设备。
	//
	// +optional
	DeviceSelector *DeviceTaintSelector `json:"deviceSelector,omitempty" protobuf:"bytes,1,opt,name=deviceSelector"`

	// 应用到匹配设备上的 taint。
	//
	// +required
	Taint DeviceTaint `json:"taint,omitempty" protobuf:"bytes,2,rep,name=taint"`
}

// DeviceTaintSelector 定义 DeviceTaintRule 应用到哪些设备上。
// 空选择器匹配所有设备；没有选择器则不匹配任何设备。
type DeviceTaintSelector struct {
	// 若设置了 DeviceClassName，设备必须满足其中定义的选择器才能被选中。
	// 该字段对应 class.metadata.name。
	//
	// +optional
	//
	// 自 1.35 起废止，因为要全面支持它需要把设备属性复制到 ResourceClaim 分配结果中，
	// 否则驱逐控制器无法计算这些 CEL 表达式。
	//
	// DeviceClassName *string `json:"deviceClassName,omitempty" protobuf:"bytes,1,opt,name=deviceClassName"`

	// 若设置了 driver，则只选择该驱动的设备，对应 slice.spec.driver。
	//
	// +optional
	Driver *string `json:"driver,omitempty" protobuf:"bytes,2,opt,name=driver"`

	// 若设置了 pool，则只选择该池中的设备。
	//
	// 当不同驱动使用相同的池名时，同时设置驱动名有助于避免歧义，但并非必须，
	// 因为选择来自不同驱动的池也可能有用，例如使用节点本地设备的驱动会以节点名作为池名。
	//
	// +optional
	Pool *string `json:"pool,omitempty" protobuf:"bytes,3,opt,name=pool"`

	// 若设置了 device，则只选择具有该名称的设备，对应 slice.spec.devices[].name。
	//
	// 可能还需同时设置 driver 和 pool 以避免歧义，但并非必须。
	//
	// +optional
	Device *string `json:"device,omitempty" protobuf:"bytes,4,opt,name=device"`

	// Selectors 包含与 ResourceClaim 相同的选择条件，目前支持 CEL 表达式，且必须满足所有选择器。
	//
	// +optional
	// +listType=atomic
	//
	// 自 1.35 起废止，因为要全面支持它需要把设备属性复制到 ResourceClaim 分配结果中，
	// 否则驱逐控制器无法计算这些 CEL 表达式。
	//
	// Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,5,rep,name=selectors"`
}

// DeviceTaintRuleStatus 提供正在进行的 Pod 驱逐的相关信息。
type DeviceTaintRuleStatus struct {
	// Conditions 以机器可读和人类可读的格式，提供某一时刻 DeviceTaintRule 和集群的状态信息。
	//
	// 本 API 目前定义了以下条件，未来可能增加更多：
	// - Type: EvictionInProgress
	// - Status: 当前存在需要驱逐的 Pod 时为 True，否则为 False（包括不会导致驱逐的 effect）。
	// - Reason: 未指定，可能变化。
	// - Message: 以人类可读格式包含待驱逐 Pod 和已驱逐 Pod 的数量信息，定期更新，可能变化。
	//
	// 对于 `effect: None`，每次 spec 变更时上述条件只设置一次，消息中说明若 effect 为
	// `NoExecute` 会发生什么。该反馈可用于判断将 effect 改为 `NoExecute` 是否达到预期效果。
	// 只设置一次是为了避免持续更新 status。
	//
	// 条目数不得超过 8。
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

// DeviceTaintRuleStatusMaxConditions 是 DeviceTaintRuleStatus 中条件的最大数量。
const DeviceTaintRuleStatusMaxConditions = 8

// DeviceTaintConditionEvictionInProgress 是 DeviceTaintRuleStatus 公开文档化的条件类型。
const DeviceTaintConditionEvictionInProgress = "EvictionInProgress"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// DeviceTaintRuleList 是 DeviceTaintRule 的集合。
type DeviceTaintRuleList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 DeviceTaintRule 列表。
	Items []DeviceTaintRule `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ResourceSliceList 是 ResourceSlice 的集合。
type ResourceSliceList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceSlice 列表。
	Items []ResourceSlice `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33
// +k8s:supportsSubresource="/status"

// ResourceClaim 描述工作负载对集群资源的访问请求。例如，若工作负载需要具有特定属性的
// 加速器设备，就通过它来表达该请求。status 部分记录该 claim 是否已得到满足，
// 以及具体分配了哪些资源。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation feature gate。
type ResourceClaim struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 描述请求的内容及配置方式，该 spec 不可变。
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 描述该 claim 是否可用以及已分配的资源。
	// +optional
	Status ResourceClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourceClaimSpec 定义 ResourceClaim 中的请求内容及配置方式。
type ResourceClaimSpec struct {
	// Devices 定义如何请求设备。
	//
	// +optional
	Devices DeviceClaim `json:"devices" protobuf:"bytes,1,name=devices"`

	// Controller 自 Kubernetes 1.32 起废止，当时该字段被移除。
	// 待不再支持解码 v1alpha3 后可复用。
	// Controller string `json:"controller,omitempty" protobuf:"bytes,2,opt,name=controller"`
}

// DeviceClaim 定义如何通过 ResourceClaim 请求设备。
type DeviceClaim struct {
	// Requests 表示对不同设备的独立请求，所有请求都必须得到满足。
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

	// 为该 claim 分配的设备集合必须满足这些约束。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Constraints []DeviceConstraint `json:"constraints,omitempty" protobuf:"bytes,2,opt,name=constraints"`

	// 该字段保存多个可能满足本 claim 请求的驱动的配置，在分配 claim 时会被忽略。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClaimConfiguration `json:"config,omitempty" protobuf:"bytes,3,opt,name=config"`

	// 未来可能的扩展，旧调度器会忽略它。这没问题，因为评分只让用户表达偏好，
	// 不构成硬性要求。
	//
	// Score *SomeScoringStruct
}

const (
	// DeviceRequestsMaxSize 是设备请求的最大数量，与分配结果条目数上限一致。
	DeviceRequestsMaxSize = AllocationResultsMaxSize
	// DeviceConstraintsMaxSize 是设备约束的最大数量。
	DeviceConstraintsMaxSize = 32
	// DeviceConfigMaxSize 是设备配置的最大数量。
	DeviceConfigMaxSize = 32
)

// DRAAdminNamespaceLabelKey 是用于在命名空间内授予对某些 resource.k8s.io API 类型
// 管理访问权限的 label 键。当命名空间上该 label 值为 "true"（区分大小写）时，
// 允许在任何命名空间级 resource.k8s.io API 类型中使用 adminAccess: true。
// 目前该权限适用于 ResourceClaim 和 ResourceClaimTemplate 对象。
const (
	// DRAAdminNamespaceLabelKey 是授予 DRA 管理访问权限的命名空间 label 键。
	DRAAdminNamespaceLabelKey = "resource.kubernetes.io/admin-access"
)

// DeviceRequest 是 claim 所需设备的请求。它通常是对单个设备这类资源的请求，
// 但也可请求多个相同设备。通过 FirstAvailable，还可以提供带优先级的需求列表。
type DeviceRequest struct {
	// Name 可用于在 pod.spec.containers[].resources.claims 条目和 claim 的约束中引用该请求。
	//
	// 设置 Exactly 字段时，使用 DeviceRequest 中 name 的引用能唯一标识一个请求。
	// 设置 FirstAvailable 字段时，对 DeviceRequest name 的引用会匹配调度器选中的任意子请求。
	//
	// 必须是 DNS label。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Exactly 指定单个请求的细节，必须完全满足该请求才算成功。
	//
	// Exactly 或 FirstAvailable 必须设置一个。
	//
	// +optional
	// +oneOf=deviceRequestType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Exactly *ExactDeviceRequest `json:"exactly,omitempty" protobuf:"bytes,2,name=exactly"`

	// FirstAvailable 包含若干子请求，调度器只会选择其中一个。它按此处列出的顺序尝试满足，
	// 因此若列表中有两个条目，只有判断第一个不可用时才会检查第二个。
	//
	// DRA 尚未实现评分，因此调度器会选择第一个满足 claim 中所有请求的设备集合。
	// 若多个节点都能满足要求，则由其他调度特性决定选择哪个节点。这意味着分配给 claim
	// 的设备集合可能并非集群中的最优集合。评分功能将在后续实现。
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

// ExactDeviceRequest 是对一个或多个相同设备的请求。
type ExactDeviceRequest struct {
	// DeviceClassName 引用特定的 DeviceClass，该类可定义本请求要继承的附加配置和选择器。
	//
	// 必须指定 DeviceClassName。
	//
	// 管理员可只安装带允许设备选择器的类，以此限制用户可请求的设备。
	// 若允许用户无限制请求任何设备，管理员可创建一个空的 DeviceClass 供用户引用。
	//
	// +required
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,1,name=deviceClassName"`

	// Selectors 定义特定设备被本请求考虑前必须满足的条件。设备必须满足所有选择器才会被考虑。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,2,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本请求。支持的值包括：
	//
	// - ExactCount：请求特定数量的设备，这是默认值，具体数量在 count 字段中给出。
	//
	// - All：请求池中全部匹配的设备。分配成功的前提是节点上至少存在一个设备。
	//   若部分设备已被分配，分配会失败，除非请求了 adminAccess。
	//
	// 未指定 AllocationMode 时默认为 ExactCount；若模式为 ExactCount 且未指定 count，
	// 则默认数量为 1。其他任何请求都必须指定该字段。
	//
	// 未来可能添加更多模式。客户端必须拒绝处理未知模式的请求。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,3,opt,name=allocationMode"`

	// Count 仅在计数模式为 "ExactCount" 时使用，必须大于零。
	// 若 AllocationMode 为 ExactCount 且未指定本字段，则默认为 1。
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,4,opt,name=count"`

	// AdminAccess 表示这是对设备的管理访问请求。带 AdminAccess 的 claim 预期用于
	// 设备监控或其他管理服务。在访问模式和资源分配方面，它们会忽略设备上所有普通 claim。
	//
	// 这是 alpha 字段，需要启用 DRAAdminAccess feature gate。未设置或设为 false 时
	// 禁用管理访问，否则启用。
	//
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,5,opt,name=adminAccess"`

	// 若指定，这些是请求的 toleration。
	//
	// 要分配带有 NoSchedule effect taint 的设备，必须有对应的 toleration；NoExecute 同理。
	//
	// 此外，若已分配设备在分配后被加上 NoExecute taint 且不被容忍，
	// 所有消费该 ResourceClaim 的 Pod 都会被删除以进行驱逐。
	// 在 claim 带有这些带 taint 设备期间，调度器不会让新 Pod 预留该 claim。
	// 所有 Pod 被驱逐后，claim 会被解除分配。
	//
	// toleration 最多 16 个。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// Capacity 定义针对每项容量的资源需求。
	//
	// 若未设置该字段且设备支持多次分配，则根据 requestPolicy 对每项容量应用默认值。
	// 没有 requestPolicy 的容量，默认值为全部容量。
	//
	// 该配置适用于每次设备分配。若 Count > 1，当没有足够设备满足要求时请求失败。
	// 若 AllocationMode 为 All，当存在其他方面匹配请求、具备该容量且值 >= 请求量、
	// 但无法分配给本请求的设备时，请求失败。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,7,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算出的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备
	// （例如在同一 NUMA 节点上共同分配 GPU 和 NIC），即使它们的驱动发布的属性不同。
	// 在计算选择器表达式时，CEL 环境中无法通过 `device.attributes` 访问派生属性。
	//
	// 派生属性可在调度时提取、转换或规范化拓扑信息（例如从复杂拓扑字符串中提取 NUMA 索引，
	// 或重命名供应商特有属性），统一为通用虚拟属性名。随后调度器在匹配约束时
	// 会像处理静态属性一样处理这些虚拟属性。
	//
	// 此列表中定义的每个派生属性都必须至少被 `.devices.constraints` 列表中的一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性最多 32 个。
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,8,rep,name=derivedAttributes"`
}

// DeviceSubRequest 描述 claim.spec.devices.requests[].firstAvailable 数组中提供的设备请求。
// 每个请求通常是对单个设备这类资源的请求，但也可请求多个相同设备。
//
// DeviceSubRequest 与 ExactDeviceRequest 类似，但不暴露 AdminAccess 字段，
// 因为该字段仅在请求特定设备时支持。
type DeviceSubRequest struct {
	// Name 可用于在 claim 的约束列表或配置列表中引用该子请求。
	// 引用必须使用 <main request>/<subrequest> 格式。
	//
	// 必须是 DNS label。
	//
	// +required
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// DeviceClassName 引用特定的 DeviceClass，该类可定义本子请求要继承的附加配置和选择器。
	//
	// 必须指定一个类，可用的类取决于集群。
	//
	// 管理员可只安装带允许设备选择器的类，以此限制用户可请求的设备。
	// 若允许用户无限制请求任何设备，管理员可创建一个空的 DeviceClass 供用户引用。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name
	DeviceClassName string `json:"deviceClassName" protobuf:"bytes,2,name=deviceClassName"`

	// Selectors 定义特定设备被本子请求考虑前必须满足的条件。设备必须满足所有选择器才会被考虑。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,3,name=selectors"`

	// AllocationMode 及其相关字段定义如何分配设备以满足本子请求。支持的值包括：
	//
	// - ExactCount：请求特定数量的设备，这是默认值，具体数量在 count 字段中给出。
	//
	// - All：本子请求需要池中全部匹配的设备。若部分设备已被分配，分配会失败，
	//   除非请求了 adminAccess。
	//
	// 未指定 AllocationMode 时默认为 ExactCount；若模式为 ExactCount 且未指定 count，
	// 则默认数量为 1。其他任何子请求都必须指定该字段。
	//
	// 未来可能添加更多模式。客户端必须拒绝处理未知模式的请求。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	AllocationMode DeviceAllocationMode `json:"allocationMode,omitempty" protobuf:"bytes,4,opt,name=allocationMode"`

	// Count 仅在计数模式为 "ExactCount" 时使用，必须大于零。
	// 若 AllocationMode 为 ExactCount 且未指定本字段，则默认为 1。
	//
	// +optional
	// +oneOf=AllocationMode
	Count int64 `json:"count,omitempty" protobuf:"bytes,5,opt,name=count"`

	// 若指定，这些是请求的 toleration。
	//
	// 要分配带有 NoSchedule effect taint 的设备，必须有对应的 toleration；NoExecute 同理。
	//
	// 此外，若已分配设备在分配后被加上 NoExecute taint 且不被容忍，
	// 所有消费该 ResourceClaim 的 Pod 都会被删除以进行驱逐。
	// 在 claim 带有这些带 taint 设备期间，调度器不会让新 Pod 预留该 claim。
	// 所有 Pod 被驱逐后，claim 会被解除分配。
	//
	// toleration 最多 16 个。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// Capacity 定义针对每项容量的资源需求。
	//
	// 若未设置该字段且设备支持多次分配，则根据 requestPolicy 对每项容量应用默认值。
	// 没有 requestPolicy 的容量，默认值为全部容量。
	//
	// 该配置适用于每次设备分配。若 Count > 1，当没有足够设备满足要求时请求失败。
	// 若 AllocationMode 为 All，当存在其他方面匹配请求、具备该容量且值 >= 请求量、
	// 但无法分配给本请求的设备时，请求失败。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	Capacity *CapacityRequirements `json:"capacity,omitempty" protobuf:"bytes,7,opt,name=capacity"`

	// DerivedAttributes 定义一组通过 CEL 表达式为每个候选设备计算出的虚拟属性。
	// 这些虚拟属性可在 `.devices.constraints` 中引用，用于对齐和匹配不同设备
	// （例如在同一 NUMA 节点上共同分配 GPU 和 NIC），即使它们的驱动发布的属性不同。
	// 在计算选择器表达式时，CEL 环境中无法通过 `device.attributes` 访问派生属性。
	//
	// 派生属性可在调度时提取、转换或规范化拓扑信息（例如从复杂拓扑字符串中提取 NUMA 索引，
	// 或重命名供应商特有属性），统一为通用虚拟属性名。随后调度器在匹配约束时
	// 会像处理静态属性一样处理这些虚拟属性。
	//
	// 此列表中定义的每个派生属性都必须至少被 `.devices.constraints` 列表中的一个
	// MatchAttribute 或 DistinctAttribute 约束引用。
	//
	// 派生属性最多 32 个。
	//
	// 这是 alpha 字段，需要启用 DRADerivedAttributes feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADerivedAttributes
	// +k8s:optional
	// +k8s:maxItems=32
	DerivedAttributes []DeviceDerivedAttribute `json:"derivedAttributes,omitempty" protobuf:"bytes,8,rep,name=derivedAttributes"`
}

// CapacityRequirements 定义特定设备请求的容量需求。
type CapacityRequirements struct {
	// Requests 表示对不同资源的独立设备资源请求，所有请求都必须由该设备满足。
	//
	// 该值用作针对设备可用容量的额外过滤条件，语义上等价于如下 CEL 选择器：
	// `device.capacity[<domain>].<name>.compareTo(quantity(<request quantity>)) >= 0`。
	// 例如 device.capacity['test-driver.cdi.k8s.io'].counters.compareTo(quantity('2')) >= 0。
	//
	// 定义 requestPolicy 时，请求数量会根据策略向上调整到最近的有效值。
	// 若请求数量因超出 requestPolicy 允许的范围而无法调整为有效值，则认为该设备不适合分配。
	//
	// 对于未显式请求的任何容量：
	// - 未设置 requestPolicy 时，默认消费容量等于设备全部容量（即占用整个设备）。
	// - 设置了 requestPolicy 时，默认消费容量由该策略决定。
	//
	// 若设备允许多次分配，所有请求的总量不得超过容量值。
	// 消费容量（若定义了 requestPolicy 则可能已按其调整）记录在 claim 的
	// status.devices[*].consumedCapacity 字段中。
	//
	// +optional
	Requests map[QualifiedName]resource.Quantity `json:"requests,omitempty" protobuf:"bytes,1,rep,name=requests,castkey=QualifiedName"`
}

// DeviceDerivedAttribute 定义通过 CEL 计算出的派生属性。
type DeviceDerivedAttribute struct {
	// Name 是该派生属性在约束中使用的标识符。
	//
	// 必须是 DNS 子域加斜杠（"/"）再加 C 标识符，例如 "example.com/numaNode" 或 "derived/numaNode"。
	//
	// 若所选名称与驱动提供的现有物理属性同名，派生属性的表达式会遮蔽该物理属性，
	// 约束中将改用其计算值。若只想定义仅在 ResourceClaim 内部使用、不遮蔽现有属性的派生属性，
	// 应使用任何 DRA 驱动都不会使用的域前缀（例如 "derived/myAttribute"）。
	//
	// 定义未被至少一个约束使用的派生属性是非法的。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-resource-fully-qualified-name
	Name FullyQualifiedName `json:"name" protobuf:"bytes,1,name=name"`

	// Expression 是针对每个候选设备计算的 CEL 表达式。表达式必须求值为基本标量
	// （string、integer、boolean 或 semver）或这些标量的列表（[]string、[]int64、
	// []bool、[]semver），以充当虚拟分组键。任何其他返回类型都是错误，会导致该设备的
	// CEL 求值失败。
	//
	// 表达式的输入是名为 "device" 的对象，其属性与 CELDeviceSelector 中的相同。
	//
	// Pod 调度时，若某些设备出现 CEL 运行时错误（例如查找未定义的属性），将中止分配
	// 并使 Pod 调度失败。立即暴露求值错误可避免极难察觉的静默拓扑匹配失败。
	// 健壮的表达式应在引用属性前检查其是否存在，以避免运行时求值错误。
	//
	// 表达式会在设备通过该请求的其他选择器表达式之后才求值。这允许编写针对特定
	// 目标设备的表达式（例如假定设备来自某供应商而跳过相关检查）。
	//
	// 表达式长度不得超过 10 Ki。其求值成本也受预估逻辑步骤数限制；一个 claim 中所有
	// 派生属性的总成本受共享 CEL 成本预算约束。
	//
	// +required
	// +k8s:required
	Expression string `json:"expression" protobuf:"bytes,2,name=expression"`
}

const (
	// DeviceSelectorsMaxSize 是设备选择器的最大数量。
	DeviceSelectorsMaxSize = 32
	// FirstAvailableDeviceRequestMaxSize 是 FirstAvailable 子请求的最大数量。
	FirstAvailableDeviceRequestMaxSize = 8
	// DeviceTolerationsMaxLength 是设备容忍的最大数量。
	DeviceTolerationsMaxLength = 16
	// DeviceDerivedAttributesMaxSize 是派生属性的最大数量。
	DeviceDerivedAttributesMaxSize = 32
)

// DeviceAllocationMode 定义如何分配设备以满足请求。
//
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type DeviceAllocationMode string

// [DeviceRequest.CountMode] 的有效值。
const (
	// DeviceAllocationModeExactCount 表示请求特定数量的设备。
	DeviceAllocationModeExactCount = DeviceAllocationMode("ExactCount")
	// DeviceAllocationModeAll 表示请求池中全部匹配的设备。
	DeviceAllocationModeAll = DeviceAllocationMode("All")
)

// DeviceSelector 必须且只能设置一个字段。
type DeviceSelector struct {
	// CEL 包含用于选择设备的 CEL 表达式。
	//
	// +optional
	// +oneOf=SelectorType
	CEL *CELDeviceSelector `json:"cel,omitempty" protobuf:"bytes,1,opt,name=cel"`
}

// CELDeviceSelector 包含用于选择设备的 CEL 表达式。
type CELDeviceSelector struct {
	// Expression 是对单个设备求值的 CEL 表达式。当被评估设备满足期望条件时必须求值为 true，
	// 否则为 false。任何其他结果都是错误，会导致设备分配中止。
	//
	// 表达式的输入是名为 "device" 的对象，具有以下属性：
	//  - driver (string)：定义该设备的驱动名。
	//  - attributes (map[string]object)：设备属性，按前缀分组
	//    （例如 device.attributes["dra.example.com"] 得到包含所有以 "dra.example.com"
	//    为前缀属性的对象）。
	//  - capacity (map[string]object)：设备容量，按前缀分组。
	//  - allowMultipleAllocations (bool)：设备的 allowMultipleAllocations 属性
	//    （v1.34+，需启用 DRAConsumableCapacity）。
	//
	// 示例：某设备 driver="dra.example.com"，暴露两个属性 "model" 和 "ext.example.com/family"，
	// 以及一个容量 "modules"。该表达式输入将具有以下字段：
	//
	//     device.driver
	//     device.attributes["dra.example.com"].model
	//     device.attributes["ext.example.com"].family
	//     device.capacity["dra.example.com"].modules
	//
	// device.driver 字段可用于检查特定驱动，既可作为高层前置条件（即只考虑该驱动的设备），
	// 也可作为考虑不同驱动设备的多子句表达式的一部分。
	//
	// 每个属性的值类型由设备定义决定，编写表达式的用户必须查阅具体驱动的文档。
	// 每个容量的值类型为 Quantity。
	//
	// 在 device.attributes 或 device.capacity 中查找未知前缀时返回空 map。
	// 引用未知字段会导致求值错误并中止分配。
	//
	// 健壮的表达式应在引用属性前检查其是否存在。
	//
	// 常见错误：
	// - "no such key"：使用可选链（.? 后接 orValue()），或用 has() 对可选字段做保护。
	//   详见 CEL Optional Types：
	//   https://pkg.go.dev/github.com/google/cel-go@v0.17.4/cel#OptionalTypes
	//
	// 更多 CEL 表达式语法和示例参见：
	// https://kubernetes.io/docs/reference/using-api/cel/
	//
	// 为方便使用，已启用 cel.bind() 函数，可简化访问同一域下多个属性的表达式。例如：
	//
	//     cel.bind(dra, device.attributes["dra.example.com"], dra.someBool && dra.anotherBool)
	//
	// 启用 DRAListTypeAttributes feature gate 后，可使用 includes() 辅助函数，
	// 它同时适用于标量属性和列表属性。引入该函数是为了在保持 CEL 表达式简单的同时，
	// 支持从标量属性平滑迁移到列表属性。例如：
	//
	//     device.attributes["dra.example.com"].models.includes("some-model")
	//
	// 表达式长度不得超过 10 Ki，求值成本同样受预估逻辑步骤数限制。
	//
	// +required
	Expression string `json:"expression" protobuf:"bytes,1,name=expression"`
}

// CELSelectorExpressionMaxCost 指定单个 CEL 选择器求值的成本上限。
//
// 选择设备没有总体预算，因此实际耗时
// 与 CEL 选择器的数量及求值频率成正比，
// 而这取决于设备数量、集群利用率、附加约束等多个因素。
//
// 仅在首次设置表达式或修改表达式时，才会针对该上限和 [CELSelectorExpressionMaxLength]
// 进行校验。若未来 Kubernetes 版本调整了这些上限，
// 现有用户可保证已有表达式继续有效。
//
// 但 kube-scheduler 在运行时也会应用该成本上限，因此升级或降级后，
// 一个原本有效的表达式可能在运行时失败。
// 即使没有版本差异，若成本估算低估了实际成本也可能发生这种情况。
// 正是出于这个原因，
// kube-scheduler 强制执行运行时限制，而不只依赖校验。
//
// 根据
// https://github.com/kubernetes/kubernetes/blob/4aeaf1e99e82da8334c0d6dddd848a194cd44b4f/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go#L20-L22，
// 每次表达式求值大约耗时 0.1 秒，
// 但这取决于机器速度。
const CELSelectorExpressionMaxCost = 1000000

// DeviceClaimDerivedAttributeCELMaxCost 是单个 DeviceClaim 中所有派生属性 CEL 表达式
// 允许的总执行成本上限。
//
// 校验时，API 服务器会计算每个派生属性表达式的预估执行成本。所有请求的成本总和
// 不得超过该预算，否则 claim 会被拒绝。
//
// 为控制在预算内，使用方应简化 CEL 表达式，避免深度嵌套或迭代（如 `.all()` 或 `.exists()`）
// 等高成本操作，并尽量减少 claim 中派生属性的总数。
//
// 该共享预算可防止过于复杂的 claim 拖慢 kube-scheduler 性能。上限设为 1,000,000 条指令执行
// （约 0.1 秒求值时间），使所有派生属性的总成本与单个 CEL 选择器的最大成本保持一致。
const DeviceClaimDerivedAttributeCELMaxCost = 1000000

// CELSelectorExpressionMaxLength 是 CEL 选择器表达式字符串的最大长度。
const CELSelectorExpressionMaxLength = 10 * 1024

// DeviceConstraint 除 Requests 外必须且只能设置一个字段。
type DeviceConstraint struct {
	// Requests 是本 claim 中必须共同满足该约束的一个或多个请求的列表。
	// 若一个请求由多个设备满足，则所有设备都必须满足该约束。未指定时，
	// 该约束适用于 claim 中的所有请求。
	//
	// 对子请求的引用必须包含主请求名，并可按照 <main request>[/<subrequest>] 格式包含子请求。
	// 若只给出主请求，则约束适用于所有子请求。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Requests []string `json:"requests,omitempty" protobuf:"bytes,1,opt,name=requests"`

	// MatchAttribute 要求所有相关设备都具有该属性，且属性和值在这些设备之间相同。
	//
	// 例如指定 "dra.example.com/numa"（仅是假设示例），则只会选择同一 NUMA 节点上的设备。
	// 不具备该属性的设备不会被选中。所有设备对该属性应使用相同类型的值，这是其定义的一部分；
	// 若有设备不满足，也不会被选中。
	//
	// 启用 DRAListTypeAttributes feature gate 后，比较使用集合语义（忽略元素顺序和重复项）：
	// 列表属性在所有设备上的交集非空时即匹配。为保持向后兼容，标量值视为单元素集合。
	//
	// 必须包含域限定符。
	//
	// +optional
	// +oneOf=ConstraintType
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	MatchAttribute *FullyQualifiedName `json:"matchAttribute,omitempty" protobuf:"bytes,2,opt,name=matchAttribute"`

	// 未来可能的扩展，不属于当前设计：用一个 CEL 表达式比较不同设备，匹配时返回 true。
	//
	// 由于它将成为 one-of 的一部分，旧调度器不会意外忽略这一对其未知的新匹配条件。
	//
	// MatchExpression string

	// DistinctAttribute 要求所有相关设备都具有该属性，且其类型和值在这些设备之间互不相同。
	//
	// 启用 DRAListTypeAttributes feature gate 后，比较使用集合语义（忽略元素顺序和重复项）：
	// 列表属性在各设备之间必须两两不相交。为保持向后兼容，标量值视为单元素集合。
	//
	// 其作用与 MatchAttribute 相反。
	//
	// 该约束通过确保属性层面的差异，避免将多个请求分配到同一设备上。
	//
	// 适用于资源请求必须由不同物理设备满足的场景，例如容器请求两个必须来自不同物理 NIC
	// 的网络接口。
	//
	// +optional
	// +oneOf=ConstraintType
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-fully-qualified-name
	DistinctAttribute *FullyQualifiedName `json:"distinctAttribute,omitempty" protobuf:"bytes,3,opt,name=distinctAttribute"`
}

// DeviceClaimConfiguration 用于 DeviceClaim 中的配置参数。
type DeviceClaimConfiguration struct {
	// Requests 列出该配置适用的请求名，为空时适用于所有请求。
	//
	// 对子请求的引用必须包含主请求名，并可按照 <main request>[/<subrequest>] 格式包含子请求。
	// 若只给出主请求，则配置适用于所有子请求。
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

// DeviceConfiguration 必须且只能设置一个字段。它以内联方式嵌入其他含有字段的结构体，
// 因此字段名不得与那些字段冲突。
type DeviceConfiguration struct {
	// Opaque 提供驱动特有的配置参数。
	//
	// +optional
	// +oneOf=ConfigurationType
	// +k8s:beta(since: "1.37")=+k8s:optional
	Opaque *OpaqueDeviceConfiguration `json:"opaque,omitempty" protobuf:"bytes,1,opt,name=opaque"`
}

// OpaqueDeviceConfiguration 以驱动供应商定义的格式保存驱动的配置参数。
type OpaqueDeviceConfiguration struct {
	// Driver 用于确定需要将这些配置参数传给哪个 kubelet 插件。
	//
	// 驱动开发者提供的准入策略可用它决定是否需要校验这些参数。
	//
	// 必须是 DNS 子域，且应以驱动供应商拥有的 DNS 域结尾，只能使用小写字符。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// Parameters 可包含任意数据。校验和版本管理由驱动开发者负责，通常包括自标识和版本信息
	// （Kubernetes 类型使用 "kind" + "apiVersion"），以及不同版本之间的转换。
	//
	// 原始数据长度不得超过 10 Ki。
	//
	// +required
	Parameters runtime.RawExtension `json:"parameters" protobuf:"bytes,2,name=parameters"`
}

// OpaqueParametersMaxLength 是 [OpaqueDeviceConfiguration.Parameters] 字段中原始数据的最大长度。
const OpaqueParametersMaxLength = 10 * 1024

// 附加该 DeviceToleration 的 ResourceClaim 可容忍任何通过匹配运算符 <operator>
// 与三元组 <key,value,effect> 匹配的 taint。
type DeviceToleration struct {
	// Key 是 toleration 适用的 taint 键，为空表示匹配所有 taint 键。
	// 键为空时 operator 必须为 Exists，该组合表示匹配所有值和所有键。
	// 必须是 label 名。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-label-key
	Key string `json:"key,omitempty" protobuf:"bytes,1,opt,name=key"`

	// Operator 表示键与值之间的关系。有效运算符为 Exists 和 Equal，默认为 Equal。
	// Exists 相当于值的通配符，使 ResourceClaim 能容忍某一类别的所有 taint。
	//
	// +optional
	// +default="Equal"
	// +k8s:beta(since: "1.37")=+k8s:optional
	Operator DeviceTolerationOperator `json:"operator,omitempty" protobuf:"bytes,2,opt,name=operator,casttype=DeviceTolerationOperator"`

	// Value 是 toleration 匹配的 taint 值。operator 为 Exists 时该值必须为空，
	// 否则为普通字符串。必须是 label 值。
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,3,opt,name=value"`

	// Effect 表示要匹配的 taint effect，为空表示匹配所有 effect。
	// 指定时允许的值为 NoSchedule 和 NoExecute。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	Effect DeviceTaintEffect `json:"effect,omitempty" protobuf:"bytes,4,opt,name=effect,casttype=DeviceTaintEffect"`

	// TolerationSeconds 表示 toleration 容忍 taint 的时长（effect 必须是 NoExecute，
	// 否则忽略该字段）。默认未设置，表示永久容忍（不驱逐）。零和负值会被系统当作 0
	// （立即驱逐）。大于零时，Pod 需要被驱逐的时间计算为 <taint 添加时间> + <toleration 秒数>。
	//
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty" protobuf:"varint,5,opt,name=tolerationSeconds"`
}

// TolerationOperator 是 toleration 中可用的运算符集合。
//
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type DeviceTolerationOperator string

const (
	DeviceTolerationOpExists DeviceTolerationOperator = "Exists"
	DeviceTolerationOpEqual  DeviceTolerationOperator = "Equal"
)

// ResourceClaimStatus 跟踪资源是否已分配以及分配结果。
type ResourceClaimStatus struct {
	// claim 成功分配后会设置 Allocation。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:update=NoModify
	Allocation *AllocationResult `json:"allocation,omitempty" protobuf:"bytes,1,opt,name=allocation"`

	// ReservedFor 表示当前允许使用本 claim 的实体。引用了未为其预留的 ResourceClaim 的 Pod
	// 不会启动。正在使用或因已被预留而可能在使用中的 claim 不得被解除分配。
	//
	// 在有多个调度器实例的集群中，两个 Pod 可能被不同调度器同时调度。当它们引用同一个
	// 已达到最大消费者数量的 ResourceClaim 时，只有一个 Pod 能被调度。
	//
	// 两个调度器都会尝试把自己的 Pod 加入 claim.status.reservedFor 字段，但只有先到达
	// API 服务器的更新会被保存。另一个更新失败并返回错误，发出该更新的调度器便知道
	// 必须把 Pod 放回队列，等待 ResourceClaim 再次可用。
	//
	// 此类预留最多 256 个，未来可能增加但不会减少。
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

	// DeallocationRequested 自 Kubernetes 1.32 起废止，当时该字段被移除。
	// 待不再支持解码 v1alpha3 后可复用。
	// DeallocationRequested bool `json:"deallocationRequested,omitempty" protobuf:"bytes,3,opt,name=deallocationRequested"`

	// Devices 包含驱动上报的、为本 claim 分配的每个设备的状态，可能包含驱动特有信息。
	// 各条目由对应驱动所有。
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

// ResourceClaimReservedForMaxSize 是 claim.status.reservedFor 中条目的最大数量。
const ResourceClaimReservedForMaxSize = 256

// ResourceClaimConsumerReference 包含定位 ResourceClaim 消费者所需的足够信息。
// 消费者必须是与 ResourceClaim 处于同一命名空间的资源。
type ResourceClaimConsumerReference struct {
	// APIGroup 是被引用资源所属的组，核心 API 为空字符串，与创建资源时使用的
	// APIVersion 中的组一致。
	// +optional
	APIGroup string `json:"apiGroup,omitempty" protobuf:"bytes,1,opt,name=apiGroup"`
	// Resource 是被引用资源的类型，例如 "pods"。
	// +required
	Resource string `json:"resource" protobuf:"bytes,3,name=resource"`
	// Name 是被引用资源的名称。
	// +required
	Name string `json:"name" protobuf:"bytes,4,name=name"`
	// UID 唯一标识该资源的一个实例。
	// +required
	UID types.UID `json:"uid" protobuf:"bytes,5,name=uid"`
}

// AllocationResult 包含已分配资源的属性。
type AllocationResult struct {
	// Devices 是设备分配的结果。
	//
	// +optional
	Devices DeviceAllocationResult `json:"devices,omitempty" protobuf:"bytes,1,opt,name=devices"`

	// NodeSelector 定义已分配资源在哪些节点上可用，未设置时表示到处都可用。
	//
	// +optional
	NodeSelector *v1.NodeSelector `json:"nodeSelector,omitempty" protobuf:"bytes,3,opt,name=nodeSelector"`

	// Controller 自 Kubernetes 1.32 起废止，当时该字段被移除。
	// 待不再支持解码 v1alpha3 后可复用。
	// Controller string `json:"controller,omitempty" protobuf:"bytes,4,opt,name=controller"`

	// AllocationTimestamp 保存资源分配的时间。该字段不保证被设置，未设置时时间未知。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	AllocationTimestamp *metav1.Time `json:"allocationTimestamp,omitempty" protobuf:"bytes,5,opt,name=allocationTimestamp"`
}

// DeviceAllocationResult 是设备分配的结果。
type DeviceAllocationResult struct {
	// Results 列出所有已分配的设备。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Results []DeviceRequestAllocationResult `json:"results,omitempty" protobuf:"bytes,1,opt,name=results"`

	// 该字段是 claim 和 class 的所有配置参数的组合，驱动可通过标志区分两者来源。
	//
	// 其中包含结果中没有已分配设备的驱动的配置参数，因为支持哪些配置参数由驱动决定，
	// 驱动可以静默忽略未知的配置参数。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=64
	Config []DeviceAllocationConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`
}

// AllocationResultsMaxSize 表示 allocation.devices.results 中的最大条目数。
const AllocationResultsMaxSize = 32

// DeviceRequestAllocationResult 包含一个请求的分配结果。
type DeviceRequestAllocationResult struct {
	// Request 是 claim 中导致该设备被分配的请求名。若引用 DeviceRequest 的 firstAvailable
	// 列表中的子请求，本字段必须同时包含主请求名和子请求，格式为 <main request>/<subrequest>。
	//
	// 每个请求可能分配了多个设备。
	//
	// +required
	Request string `json:"request" protobuf:"bytes,1,name=request"`

	// Driver 指定 DRA 驱动的名称，当 claim 需要在节点上使用时，将调用该驱动的 kubelet
	// 插件来处理分配。
	//
	// 必须是 DNS 子域，且应以驱动供应商拥有的 DNS 域结尾，只能使用小写字符。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-long-name-caseless
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	// +k8s:beta(since: "1.37")=+k8s:required
	Driver string `json:"driver" protobuf:"bytes,2,name=driver"`

	// 该名称与驱动名和设备名字段共同标识分配的是哪个设备
	// （`<driver name>/<pool name>/<device name>`）。
	//
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-resource-pool-name
	Pool string `json:"pool" protobuf:"bytes,3,name=pool"`

	// Device 通过设备名引用驱动资源池中的一个设备实例，必须是 DNS label。
	//
	// +required
	Device string `json:"device" protobuf:"bytes,4,name=device"`

	// AdminAccess 表示该设备是为管理访问而分配的。模式定义参见对应的请求字段。
	//
	// 这是 alpha 字段，需要启用 DRAAdminAccess feature gate。未设置或设为 false 时
	// 禁用管理访问，否则启用。
	//
	// +optional
	// +featureGate=DRAAdminAccess
	AdminAccess *bool `json:"adminAccess,omitempty" protobuf:"bytes,5,opt,name=adminAccess"`

	// 分配设备时请求中所有 toleration 的副本。
	//
	// toleration 最多 16 个。
	//
	// 这是 beta 字段，需要启用 DRADeviceTaints feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceTaints
	// +k8s:beta(since: "1.37")=+k8s:optional
	Tolerations []DeviceToleration `json:"tolerations,omitempty" protobuf:"bytes,6,opt,name=tolerations"`

	// BindingConditions 包含分配时对应 ResourceSlice 中 BindingConditions 的副本。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingConditions []string `json:"bindingConditions,omitempty" protobuf:"bytes,7,rep,name=bindingConditions"`

	// BindingFailureConditions 包含分配时对应 ResourceSlice 中 BindingFailureConditions 的副本。
	//
	// 这是 beta 字段，需要启用 DRADeviceBindingConditions 和 DRAResourceClaimDeviceStatus
	// feature gate。
	//
	// +optional
	// +listType=atomic
	// +featureGate=DRADeviceBindingConditions,DRAResourceClaimDeviceStatus
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=4
	BindingFailureConditions []string `json:"bindingFailureConditions,omitempty" protobuf:"bytes,8,rep,name=bindingFailureConditions"`

	// ShareID 唯一标识设备的一次独立分配份额，用于设备支持多个并发分配的场景。
	// 它作为额外的 map 键，用于区分同一设备的并发份额。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *types.UID `json:"shareID,omitempty" protobuf:"bytes,9,opt,name=shareID"`

	// ConsumedCapacity 记录作为 claim 请求一部分、每台设备消费的容量数量。
	// 消费量可能与请求量不同：若适用，会根据设备的 requestPolicy 向上取整到最近的有效值
	// （即不会小于请求量）。
	//
	// 每台设备的总消费容量不得超过 DeviceCapacity 的 Value。
	//
	// 该字段仅对允许多次分配的设备填充，包含所有容量条目，即使消费量为零。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	ConsumedCapacity map[QualifiedName]resource.Quantity `json:"consumedCapacity,omitempty" protobuf:"bytes,10,rep,name=consumedCapacity"`

	// SkipNodeOperations 列出在判断节点上是否需要执行操作时，对该已分配设备跳过的
	// 节点本地资源操作（gRPC 调用）。若某个 claim 中某驱动的所有已分配设备都跳过某项操作，
	// 该 gRPC 调用就会被跳过。它是分配设备时 ResourceSlice.spec.skipNodeOperations 值的副本。
	//
	// +optional
	// +listType=set
	// +k8s:listType=set
	// +featureGate=DRAOptionalNodeOperations
	// +k8s:optional
	SkipNodeOperations []SkipNodeOperation `json:"skipNodeOperations,omitempty" protobuf:"bytes,11,rep,name=skipNodeOperations,casttype=SkipNodeOperation"`
}

// DeviceAllocationConfiguration 被嵌入 AllocationResult 中。
type DeviceAllocationConfiguration struct {
	// Source 记录配置是来自 class（因此普通用户无法设置），还是来自 claim。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Source AllocationConfigSource `json:"source" protobuf:"bytes,1,name=source"`

	// Requests 列出该配置适用的请求名，为空时适用于所有请求。
	//
	// 对子请求的引用必须包含主请求名，并可按照 <main request>[/<subrequest>] 格式包含子请求。
	// 若只给出主请求，则配置适用于所有子请求。
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

// AllocationConfigSource 表示配置来源是 class 还是 claim。
//
// +enum
// +k8s:beta(since: "1.37")=+k8s:enum
type AllocationConfigSource string

// [DeviceAllocationConfiguration.Source] 的有效值。
const (
	// AllocationConfigSourceClass 表示配置来自 DeviceClass。
	AllocationConfigSourceClass AllocationConfigSource = "FromClass"
	// AllocationConfigSourceClaim 表示配置来自 ResourceClaim。
	AllocationConfigSourceClaim AllocationConfigSource = "FromClaim"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ResourceClaimList 是 claim 的集合。
type ResourceClaimList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceClaim 列表。
	Items []ResourceClaim `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// DeviceClass 是由供应商或管理员提供的资源，包含设备配置和选择器。
// 它可在 claim 的设备请求中引用，以应用这些预设配置。该资源为集群作用域。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation feature gate。
type DeviceClass struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义可分配的内容及配置方式。
	//
	// 它是可变的。消费方必须做好类随时变化的准备，无论类是更新还是被替换。
	// claim 的分配只在分配时根据类中当时的设置进行一次。
	//
	// 修改 spec 会自动递增 metadata.generation。
	// +optional
	Spec DeviceClassSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// DeviceClassSpec 用于 [DeviceClass] 中定义可分配的内容及配置方式。
type DeviceClassSpec struct {
	// 通过该类申请的每个设备都必须满足每个选择器。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,1,opt,name=selectors"`

	// Config 定义适用于通过该类的每个设备的配置参数。
	// 某些类可能由多个驱动满足，因此每个供应商配置实例只适用于一个驱动。
	//
	// 这些参数会传给驱动，但在分配 claim 时不会被考虑。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxItems=32
	Config []DeviceClassConfiguration `json:"config,omitempty" protobuf:"bytes,2,opt,name=config"`

	// SuitableNodes 自 Kubernetes 1.32 起废止，当时该字段被移除。
	// 待不再支持解码 v1alpha3 后可复用。
	// SuitableNodes *v1.NodeSelector `json:"suitableNodes,omitempty" protobuf:"bytes,3,opt,name=suitableNodes"`

	// ExtendedResourceName 是该类设备的扩展资源名。该类设备可用于满足 Pod 的扩展资源请求。
	// 其格式与 Pod 扩展资源名相同，在集群所有设备类中应唯一。
	// 若两个设备类同名，则选择后创建的类来满足 Pod 的扩展资源请求。
	// 若两个类同时创建，则选择名称按字典序靠前的类。
	//
	// +optional
	// +featureGate=DRAExtendedResource
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-extended-resource-name
	ExtendedResourceName *string `json:"extendedResourceName,omitempty" protobuf:"bytes,4,opt,name=extendedResourceName"`
}

// DeviceClassConfiguration 用于 DeviceClass 中。
type DeviceClassConfiguration struct {
	DeviceConfiguration `json:"" protobuf:"bytes,1,opt,name=deviceConfiguration"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// DeviceClassList 是 DeviceClass 的集合。
type DeviceClassList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 DeviceClass 列表。
	Items []DeviceClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ResourceClaimTemplate 用于生成 ResourceClaim 对象。
//
// 这是 alpha 类型，需要启用 DynamicResourceAllocation feature gate。
type ResourceClaimTemplate struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据。
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 描述将要生成的 ResourceClaim。
	//
	// 该字段不可变。控制平面会在需要时为 Pod 创建 ResourceClaim，之后不再更新。
	// +optional
	Spec ResourceClaimTemplateSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// ResourceClaimTemplateSpec 包含 ResourceClaim 的元数据和字段。
type ResourceClaimTemplateSpec struct {
	// ObjectMeta 可包含创建 ResourceClaim 时会复制进去的 label 和注解。
	// 不允许其他字段，否则会在校验时被拒绝。
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ResourceClaim 的 Spec，全部内容会原样复制到由该模板创建的 ResourceClaim 中。
	// ResourceClaim 中合法的字段在这里同样合法。
	// +optional
	Spec ResourceClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// ResourceClaimTemplateList 是 claim 模板的集合。
type ResourceClaimTemplateList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceClaimTemplate 列表。
	Items []ResourceClaimTemplate `json:"items" protobuf:"bytes,2,rep,name=items"`
}

const (
	// AllocatedDeviceStatusMaxConditions 表示设备状态中 condition 的最大数量。
	AllocatedDeviceStatusMaxConditions int = 8
	// AllocatedDeviceStatusDataMaxLength 表示设备状态中 Data 字段原始数据的最大长度。
	AllocatedDeviceStatusDataMaxLength int = 10 * 1024
	// NetworkDeviceDataMaxIPs 表示设备状态中 networkData 字段的最大 IP 数量。
	NetworkDeviceDataMaxIPs int = 16
	// NetworkDeviceDataInterfaceNameMaxLength 表示设备状态中 networkData.interfaceName
	// 字段的最大字符数。
	NetworkDeviceDataInterfaceNameMaxLength int = 256
	// NetworkDeviceDataHardwareAddressMaxLength 表示设备状态中 networkData.hardwareAddress
	// 字段的最大字符数。
	NetworkDeviceDataHardwareAddressMaxLength int = 128
)

// AllocatedDeviceStatus 包含已分配设备的状态（若驱动选择上报），可能包含驱动特有信息。
//
// Driver、Pool、Device 和 ShareID 的组合必须与 Status.Allocation.Devices 中的对应键匹配。
type AllocatedDeviceStatus struct {
	// Driver 指定 DRA 驱动的名称，当 claim 需要在节点上使用时，将调用该驱动的 kubelet
	// 插件来处理分配。
	//
	// 必须是 DNS 子域，且应以驱动供应商拥有的 DNS 域结尾，只能使用小写字符。
	//
	// +required
	Driver string `json:"driver" protobuf:"bytes,1,rep,name=driver"`

	// 该名称与驱动名和设备名字段共同标识分配的是哪个设备
	// （`<driver name>/<pool name>/<device name>`）。
	//
	// 长度不得超过 253 个字符，可包含一个或多个以斜杠分隔的 DNS 子域。
	//
	// +required
	Pool string `json:"pool" protobuf:"bytes,2,rep,name=pool"`

	// Device 通过设备名引用驱动资源池中的一个设备实例，必须是 DNS label。
	//
	// +required
	Device string `json:"device" protobuf:"bytes,3,rep,name=device"`

	// ShareID 唯一标识设备的一次独立分配份额。
	//
	// +optional
	// +featureGate=DRAConsumableCapacity
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:format=k8s-uuid
	ShareID *string `json:"shareID,omitempty" protobuf:"bytes,7,opt,name=shareID"`

	// Conditions 包含对设备状态的最新观察结果。
	// 若设备已按 class 和 claim 的配置引用完成配置，`Ready` 条件应为 True。
	//
	// 条目数不得超过 8。
	//
	// +optional
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions" protobuf:"bytes,4,opt,name=conditions"`

	// Data 包含任意驱动特有数据。
	//
	// 原始数据长度不得超过 10 Ki。
	//
	// +optional
	Data *runtime.RawExtension `json:"data,omitempty" protobuf:"bytes,5,opt,name=data"`

	// NetworkData 包含该设备特有的网络相关信息。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	NetworkData *NetworkDeviceData `json:"networkData,omitempty" protobuf:"bytes,6,opt,name=networkData"`
}

// NetworkDeviceData 提供已分配设备的网络相关细节。
// 这些信息可由驱动或其他组件填写，用于在网络环境中配置或识别设备。
type NetworkDeviceData struct {
	// InterfaceName 指定与已分配设备关联的网络接口名，可以是在 Pod 中配置的
	// 物理或虚拟网络接口的名称。
	//
	// 长度不得超过 256 字节。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=256
	InterfaceName string `json:"interfaceName,omitempty" protobuf:"bytes,1,opt,name=interfaceName"`

	// IPs 列出分配给设备网络接口的网络地址，可同时包含 IPv4 和 IPv6 地址。
	// IP 使用 CIDR 表示法，同时包含地址和对应的子网掩码。
	// 例如 IPv4 为 "192.0.2.5/24"，IPv6 为 "2001:db8::5/64"。
	//
	// +optional
	// +listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:listType=atomic
	// +k8s:beta(since: "1.37")=+k8s:unique=set
	// +k8s:beta(since: "1.37")=+k8s:maxItems=16
	IPs []string `json:"ips,omitempty" protobuf:"bytes,2,opt,name=ips"`

	// HardwareAddress 表示设备网络接口的硬件地址（如 MAC 地址）。
	//
	// 长度不得超过 128 字节。
	//
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:maxBytes=128
	HardwareAddress string `json:"hardwareAddress,omitempty" protobuf:"bytes,3,opt,name=hardwareAddress"`
}
