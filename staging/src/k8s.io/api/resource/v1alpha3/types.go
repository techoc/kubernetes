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

package v1alpha3

import (
	resource "k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// Expression 是针对单个设备求值的 CEL 表达式。当待考察设备满足条件时必须求值为 true，
	// 否则为 false；任何其他结果都是错误，会导致设备分配中止。
	//
	// 表达式的输入是名为 "device" 的对象，具有以下属性：
	//  - driver (string)：定义此设备的驱动名称。
	//  - attributes (map[string]object)：设备的属性，按前缀分组
	//    （例如 device.attributes["dra.example.com"] 求值得到包含所有以
	//    "dra.example.com" 为前缀的属性的对象）。
	//  - capacity (map[string]object)：设备的容量，按前缀分组。
	//
	// 示例：某设备 driver="dra.example.com"，暴露两个属性 "model" 和
	// "ext.example.com/family"，以及一个容量 "modules"。该表达式的输入具有以下字段：
	//
	//     device.driver
	//     device.attributes["dra.example.com"].model
	//     device.attributes["ext.example.com"].family
	//     device.capacity["dra.example.com"].modules
	//
	// device.driver 字段可用于检查特定驱动：既可作为高层前置条件（即只考虑该驱动的设备），
	// 也可作为多子句表达式的一部分，用于同时考虑来自不同驱动的设备。
	//
	// 每个属性的值类型由设备定义决定，编写表达式的用户必须查阅具体驱动的文档。
	// 每个容量的值类型为 Quantity。
	//
	// 若在 device.attributes 或 device.capacity 中以前缀查不到内容，将返回空 map；
	// 引用未知字段则会导致求值错误并中止分配。
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
	// 表达式长度不得超过 10 Ki，求值代价也受限于估算的逻辑步数。
	//
	// +required
	Expression string `json:"expression" protobuf:"bytes,1,name=expression"`
}

// CELSelectorExpressionMaxCost 指定单次 CEL 选择器求值的代价上限。
//
// 选择设备没有总预算，因此实际耗时与 CEL 选择器的数量及其求值频率成正比，
// 而后者取决于设备数量、集群利用率、附加约束等多个因素。
//
// 对该上限和 [CELSelectorExpressionMaxLength] 的校验仅在首次设置表达式或修改表达式时进行。
// 若未来 Kubernetes 版本调整了这些上限，现有用户可确保其已有表达式仍然有效。
//
// 但 kube-scheduler 在运行时也会施加此代价上限，因此升级或降级后，一个原本有效的表达式
// 可能在运行时失败。即使没有版本差异，当代价估算低估了实际代价时也可能发生这种情况。
// 正是由于这种可能性，kube-scheduler 选择强制执行运行时上限，而不是仅依赖校验。
//
// 依据
// https://github.com/kubernetes/kubernetes/blob/4aeaf1e99e82da8334c0d6dddd848a194cd44b4f/staging/src/k8s.io/apiserver/pkg/apis/cel/config.go#L20-L22，
// 每次表达式求值大约为 0.1 秒，具体取决于机器性能。
const CELSelectorExpressionMaxCost = 1000000

// CELSelectorExpressionMaxLength 是 CEL 选择器表达式字符串的最大长度。
const CELSelectorExpressionMaxLength = 10 * 1024

// 附加该污点的设备会对任何不容忍该污点的 ResourceClaim 产生“effect”影响，
// 并经由该申领影响使用它的 Pod。
//

// +protobuf.options.(gogoproto.goproto_stringer)=false
type DeviceTaint struct {
	// 要施加到设备上的污点键，必须是标签名（label name）。
	//
	// +required
	Key string `json:"key" protobuf:"bytes,1,name=key"`

	// 与污点键对应的污点值，必须是标签值（label value）。
	//
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`

	// 污点对不容忍它的申领、并经由此类申领对使用它们的 Pod 产生的影响。
	//
	// 有效影响为 None、NoSchedule 和 NoExecute。节点污点使用的 PreferNoSchedule 在此无效。
	// 将来可能增加更多影响。使用方必须将未知影响按 None 处理。
	//
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	Effect DeviceTaintEffect `json:"effect" protobuf:"bytes,3,name=effect,casttype=DeviceTaintEffect"`

	// ^^^^
	//
	// 实现 PreferNoSchedule 需要依赖 DRA 的打分机制，可能会作为该机制的一部分加入。
	//
	// 未来可能新增影响 NoExecuteWithPodDisruptionBudget：遵循 Pod 中断预算，而非直接删除 Pod。
	// 目前尚未决定，也可能作为一个独立字段。
	//
	// 校验必须允许已存储对象中出现未知枚举值，以便在不使用 ratcheting 的情况下
	// 在单个版本内新增枚举。

	// TimeAdded 表示添加污点的时间；在 DeviceTaintRule 中也表示 effect 被修改的时间。
	// 未设置时会在创建或更新时自动添加。
	//
	// 此外，在 DeviceTaintRule 中，如果更新时提供的值与旧值相同而新 effect 不同，
	// 则该值会被替换为当前时间。仅修改键和/或值而保持 effect 不变是允许的，且不会更新时间戳，
	// 因为使用时间戳的驱逐要么已经启动（NoExecute），要么尚未启动（NoEffect、NoSchedule）。
	//
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty" protobuf:"bytes,4,opt,name=timeAdded"`

	// ^^^
	//
	// 节点污点的对应字段定义为“仅对 NoExecute 污点写入”。但实际上 Kubernetes 从未使用它
	// （无校验、无默认值设定，pkg/controller/tainteviction 中的 Pod 驱逐也忽略它）。
}

// +enum
// +k8s:beta(since: "1.37")=+k8s:enum

// DeviceTaintEffect 描述设备污点的影响类型。
type DeviceTaintEffect string

const (
	// 无影响，该污点纯粹是提示性的。
	DeviceTaintEffectNone DeviceTaintEffect = "None"

	// 不允许新的 Pod 调度到使用带污点设备的节点，除非它们容忍该污点；但允许所有
	// 未经调度器、直接提交给 Kubelet 的 Pod 启动，也允许所有已在运行的 Pod 继续运行。
	DeviceTaintEffectNoSchedule DeviceTaintEffect = "NoSchedule"

	// 驱逐所有不容忍该设备污点的、已在运行的 Pod。
	DeviceTaintEffectNoExecute DeviceTaintEffect = "NoExecute"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// DeviceTaintRule 为所有匹配选择器的设备添加一条污点。
// 其效果等同于 DRA 驱动直接在 ResourceSlice 中指定该污点。
// +k8s:supportsSubresource="/status"
type DeviceTaintRule struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 指定选择器和一条污点。
	//
	// 修改 spec 会自动递增 metadata.generation。
	// +required
	Spec DeviceTaintRuleSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 提供 spec 中所请求内容的相关信息。
	//
	// +optional
	Status DeviceTaintRuleStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// DeviceTaintRuleSpec 指定选择器和一条污点。
type DeviceTaintRuleSpec struct {
	// DeviceSelector 定义污点施加到哪些设备上。设备必须满足所有选择条件才算匹配。
	// 空选择器匹配所有设备；没有选择器时不匹配任何设备。
	//
	// +optional
	DeviceSelector *DeviceTaintSelector `json:"deviceSelector,omitempty" protobuf:"bytes,1,opt,name=deviceSelector"`

	// 施加到匹配设备上的污点。
	//
	// +required
	Taint DeviceTaint `json:"taint,omitempty" protobuf:"bytes,2,rep,name=taint"`
}

// DeviceTaintSelector 定义 DeviceTaintRule 施加到哪些设备上。
// 空选择器匹配所有设备；没有选择器时不匹配任何设备。
type DeviceTaintSelector struct {
	// 若设置了 DeviceClassName，设备必须满足其中定义的选择器才会被选中。
	// 此字段对应 class.metadata.name。
	//
	// +optional
	//
	// 自 1.35 起废弃（tombstoned）：事实证明，要在所有情况下支持它，需要将设备属性
	// 复制到 ResourceClaim 分配结果中，否则驱逐控制器无法求值这些 CEL 表达式。
	//
	// DeviceClassName *string `json:"deviceClassName,omitempty" protobuf:"bytes,1,opt,name=deviceClassName"`

	// 若设置了 driver，则只选择该驱动的设备。
	// 此字段对应 slice.spec.driver。
	//
	// +optional
	Driver *string `json:"driver,omitempty" protobuf:"bytes,2,opt,name=driver"`

	// 若设置了 pool，则只选择该池中的设备。
	//
	// 同时设置驱动名有助于避免不同驱动使用相同池名时的歧义，但并非必须，
	// 因为选择来自不同驱动的池有时也很有用，例如使用节点名作为池名的
	// 节点本地设备驱动。
	//
	// +optional
	Pool *string `json:"pool,omitempty" protobuf:"bytes,3,opt,name=pool"`

	// 若设置了 device，则只选择具有该名称的设备。
	// 此字段对应 slice.spec.devices[].name。
	//
	// 为避免歧义可能需要同时设置 driver 和 pool，但并非强制要求。
	//
	// +optional
	Device *string `json:"device,omitempty" protobuf:"bytes,4,opt,name=device"`

	// Selectors 包含与 ResourceClaim 相同的选择条件，目前支持 CEL 表达式。
	// 所有选择器都必须满足。
	//
	// +optional
	// +listType=atomic
	//
	// 自 1.35 起废弃（tombstoned）：事实证明，要在所有情况下支持它，需要将设备属性
	// 复制到 ResourceClaim 分配结果中，否则驱逐控制器无法求值这些 CEL 表达式。
	//
	// Selectors []DeviceSelector `json:"selectors,omitempty" protobuf:"bytes,5,rep,name=selectors"`

}

// DeviceTaintRuleStatus 提供正在进行的 Pod 驱逐的相关信息。
type DeviceTaintRuleStatus struct {
	// Conditions 以机器可读和人类可读的格式，提供某时刻 DeviceTaintRule 和集群的状态信息。
	//
	// 当前此 API 定义了以下条件，将来可能增加：
	// - Type: EvictionInProgress
	// - Status: 当前存在需要驱逐的 Pod 时为 True，否则为 False（包括不会导致驱逐的 effect）。
	// - Reason: 未指定，可能变化
	// - Message: 以人类可读格式包含待驱逐和已驱逐 Pod 的数量，定期更新，可能变化
	//
	// 对于 `effect: None`，每次修改 spec 后上述条件会被设置一次，消息说明若 effect 为
	// `NoExecute` 会发生什么。此反馈可用于判断将 effect 改为 `NoExecute` 是否能达到预期。
	// 该条件只设置一次，以避免持续更新状态。
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

// DeviceTaintRuleStatusMaxConditions 是 DeviceTaintRuleStatus 中条件数的最大值。
const DeviceTaintRuleStatusMaxConditions = 8

// DeviceTaintConditionEvictionInProgress 是 DeviceTaintRuleStatus 对外公开的条件类型。
const DeviceTaintConditionEvictionInProgress = "EvictionInProgress"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.33

// DeviceTaintRuleList 是 DeviceTaintRule 的集合。
type DeviceTaintRuleList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 DeviceTaintRule 列表。
	Items []DeviceTaintRule `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36
// +k8s:supportsSubresource="/status"

// ResourcePoolStatusRequest 根据给定的过滤器触发一次资源池状态计算。状态一旦设置，请求即视为
// 完成，不会重新处理。用户需要删除并重建请求以获取更新后的信息。
type ResourcePoolStatusRequest struct {
	metav1.TypeMeta `json:""`
	// 标准对象元数据
	// +required
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义要在状态中包含哪些池的过滤条件。spec 创建后不可变。
	//
	// +required
	// +k8s:immutable
	Spec ResourcePoolStatusRequestSpec `json:"spec" protobuf:"bytes,2,name=spec"`

	// Status 由控制器填充计算出的池状态。status 非 nil 时请求视为完成，
	// 整个对象变为不可变。
	//
	// +optional
	// +k8s:optional
	Status *ResourcePoolStatusRequestStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// ResourcePoolStatusRequestSpec 定义池状态请求的过滤条件。
type ResourcePoolStatusRequestSpec struct {
	// Driver 指定用于过滤池的 DRA 驱动名称，只包含具有该驱动的 ResourceSlice 中的池。
	// 必须是 DNS 子域名（例如 "gpu.example.com"）。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name-caseless
	Driver string `json:"driver" protobuf:"bytes,1,name=driver"`

	// PoolName 可选，用于过滤特定池名称。未指定时包含指定驱动的所有池。
	// 指定时必须是非空的有效资源池名称（以 "/" 分隔的 DNS 子域名）。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-resource-pool-name
	PoolName *string `json:"poolName,omitempty" protobuf:"bytes,2,opt,name=poolName"`

	// Limit 可选，指定状态中返回池数量的上限。若匹配的池更多，响应会被截断
	// （即 len(status.pools) < status.poolCount）。
	//
	// 默认值：100；最小值：1；最大值：1000。
	//
	// +optional
	// +k8s:optional
	// +default=100
	// +k8s:minimum=1
	// +k8s:maximum=1000
	Limit *int32 `json:"limit,omitempty" protobuf:"varint,3,opt,name=limit"`

	// DefaultPartitionTypeAttribute 可选，通过完全限定名（例如 "gpu.example.com/profile"）
	// 指定一个设备属性，作为未自行声明属性的可分区设备的默认分组属性。
	//
	// ResourceSlice 自身声明的 PartitionTypeAttribute 始终优先。此默认值仅适用于其 slice
	// 未声明属性的设备，使请求仍能从尚未更新以声明该属性的驱动获得准确的 partitionSummary。
	// 当 slice 和此默认值都未指定属性时，可分区池不报告 partitionSummary。
	//
	// 必须包含域限定符。
	//
	// +optional
	// +featureGate=DRAPartitionableDevicesType
	// +k8s:ifDisabled(DRAPartitionableDevicesType)=+k8s:forbidden
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:optional
	// +k8s:ifEnabled(DRAPartitionableDevicesType)=+k8s:format=k8s-resource-fully-qualified-name
	DefaultPartitionTypeAttribute *string `json:"defaultPartitionTypeAttribute,omitempty" protobuf:"bytes,4,opt,name=defaultPartitionTypeAttribute"`
}

// ResourcePoolStatusRequestLimitDefault 是 spec.limit 的默认值。
const ResourcePoolStatusRequestLimitDefault int32 = 100

// ResourcePoolStatusRequestLimitMax 是 spec.limit 允许的最大值。
const ResourcePoolStatusRequestLimitMax int32 = 1000

// ResourcePoolStatusRequestStatus 包含计算得到的池状态信息。
type ResourcePoolStatusRequestStatus struct {
	// PoolCount 是匹配过滤条件的池总数，不受截断影响。即使响应被截断，用户也能据此
	// 了解池的总数。值为 0 表示没有池匹配过滤条件。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	PoolCount *int32 `json:"poolCount,omitempty" protobuf:"varint,6,opt,name=poolCount"`

	// Pools 包含前 `spec.limit` 个匹配的池，按驱动名再按池名排序。若
	// `len(pools) < poolCount`，说明列表已被截断。省略时表示没有池匹配请求过滤条件。
	//
	// +optional
	// +k8s:optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:maxItems=1000
	Pools []PoolStatus `json:"pools,omitempty" protobuf:"bytes,2,rep,name=pools"`

	// Conditions 提供请求状态的有关信息。填充状态时始终会设置 type=Complete 或
	// type=Failed 的条件。
	//
	// 已知条件类型：
	// - "Complete"：请求已成功处理时为 True
	// - "Failed"：请求无法处理时为 True
	//

	// +optional
	// +k8s:optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	// +k8s:maxItems=10
	// +k8s:alpha(since: "1.37")=+k8s:listType=map
	// +k8s:alpha(since: "1.37")=+k8s:listMapKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,3,rep,name=conditions"`
}

// PoolStatus 包含单个资源池的状态信息。
type PoolStatus struct {
	// Driver 是此池的 DRA 驱动名称，必须是 DNS 子域名（例如 "gpu.example.com"）。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-long-name-caseless
	Driver string `json:"driver,omitempty" protobuf:"bytes,1,name=driver"`

	// PoolName 是池的名称，必须是有效的资源池名称（以 "/" 分隔的 DNS 子域名）。
	//
	// +required
	// +k8s:required
	// +k8s:format=k8s-resource-pool-name
	PoolName string `json:"poolName,omitempty" protobuf:"bytes,2,name=poolName"`

	// Generation 是在此池所有 ResourceSlice 上观测到的池版本，只报告最新版本。
	// 在版本滚动升级过程中，若最新版本的所有 slice 尚未全部发布，则会带上 validationError
	// 并返回此池，但设备计数不设置。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	Generation int64 `json:"generation" protobuf:"varint,9,opt,name=generation"`

	// ResourceSliceCount 是组成此池的 ResourceSlice 数量。设置 validationError 时可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=1
	ResourceSliceCount *int32 `json:"resourceSliceCount,omitempty" protobuf:"varint,8,opt,name=resourceSliceCount"`

	// TotalDevices 是池中所有 slice 的设备总数。值为 0 表示池中没有设备。
	// 设置 validationError 时可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=0
	TotalDevices *int32 `json:"totalDevices,omitempty" protobuf:"varint,4,opt,name=totalDevices"`

	// AllocatedDevices 是当前已分配给申领的设备数。值为 0 表示没有设备被分配。
	// 设置 validationError 时可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=0
	AllocatedDevices *int32 `json:"allocatedDevices,omitempty" protobuf:"varint,5,opt,name=allocatedDevices"`

	// AvailableDevices 是可分配的设备数，等于 TotalDevices - AllocatedDevices - UnavailableDevices。
	// 值为 0 表示当前没有可用设备。设置 validationError 时可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=0
	AvailableDevices *int32 `json:"availableDevices,omitempty" protobuf:"varint,6,opt,name=availableDevices"`

	// UnavailableDevices 是因污点或其他条件而不可用、但尚未分配的设备数。
	// 值为 0 表示所有未分配的设备均可用。设置 validationError 时可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:minimum=0
	UnavailableDevices *int32 `json:"unavailableDevices,omitempty" protobuf:"varint,7,opt,name=unavailableDevices"`

	// NodeName 是此池关联的节点。省略时表示池不关联特定节点。
	// 必须是有效的 DNS 子域名（RFC1123）。
	//
	// +optional
	// +k8s:optional
	// +k8s:format=k8s-long-name
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,3,opt,name=nodeName"`

	// ValidationError 在池数据无法完全校验（如 slice 发布不完整）时设置。设置后，
	// 设备计数字段和 ResourceSliceCount 可能未设置。
	//
	// +optional
	// +k8s:optional
	// +k8s:maxBytes=256
	ValidationError *string `json:"validationError,omitempty" protobuf:"bytes,10,opt,name=validationError"`

	// PartitionSummary 针对发布 SharedCounters 的可分区池，按（属性，分区类型）报告可分配情况。
	// 每个条目注明解析所用的分组属性：设备自身 slice 声明的 PartitionTypeAttribute，
	// 或 slice 未声明属性时请求中指定的默认值。若池中混合了在不同属性下声明的分区，
	// 则分别报告；若没有 slice 声明属性且请求也未指定默认值，则池不报告分区摘要。
	//
	// +optional
	// +k8s:optional
	// +listType=atomic
	// +k8s:listType=atomic
	// +k8s:unique=map
	// +k8s:listMapKey=attribute
	// +k8s:listMapKey=type
	// +k8s:maxItems=32
	PartitionSummary []PartitionTypeStatus `json:"partitionSummary,omitempty" protobuf:"bytes,11,rep,name=partitionSummary"`

	// ShareableSummary 报告包含 AllowMultipleAllocations 设备的池的聚合容量。
	// 仅当池中至少有一个可共享设备时才填充。
	//
	// +optional
	// +k8s:optional
	ShareableSummary *ShareableSummaryStatus `json:"shareableSummary,omitempty" protobuf:"bytes,13,opt,name=shareableSummary"`
}

// PartitionTypeStatus 报告单个分区类型的可分配情况，由分组属性的值标识。
type PartitionTypeStatus struct {
	// Attribute 是对该条目进行分组的设备属性的完全限定名。它取自设备自身 slice 声明的
	// PartitionTypeAttribute；若 slice 未声明，则取自请求中指定的默认值。
	//
	// +required
	// +k8s:required
	Attribute string `json:"attribute,omitempty" protobuf:"bytes,4,name=attribute"`

	// Type 是分区类型的值（例如 "Full" 或 "Half"）。
	//
	// +required
	// +k8s:required
	Type string `json:"type,omitempty" protobuf:"bytes,1,name=type"`

	// Total 是池中此分区类型的设备数量。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	Total *int32 `json:"total,omitempty" protobuf:"varint,2,opt,name=total"`

	// Allocatable 是在当前共享计数器消耗下，仍可分配的此分区类型的额外设备数。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	Allocatable *int32 `json:"allocatable,omitempty" protobuf:"varint,3,opt,name=allocatable"`
}

// ShareableSummaryStatus 报告包含 AllowMultipleAllocations 设备的池的聚合容量。
type ShareableSummaryStatus struct {
	// FullyAvailableDevices 是尚未消耗任何容量的可共享设备数量。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	FullyAvailableDevices *int32 `json:"fullyAvailableDevices,omitempty" protobuf:"varint,1,opt,name=fullyAvailableDevices"`

	// PartiallyAvailableDevices 是已消耗部分但未耗尽容量的可共享设备数量。
	//
	// +required
	// +k8s:required
	// +k8s:minimum=0
	PartiallyAvailableDevices *int32 `json:"partiallyAvailableDevices,omitempty" protobuf:"varint,2,opt,name=partiallyAvailableDevices"`

	// Capacity 按可共享容量键报告整个池的聚合总量、已消耗量和可用量。
	//
	// +optional
	// +k8s:optional
	// +listType=atomic
	// +k8s:maxItems=32
	Capacity []ShareableCapacityStatus `json:"capacity,omitempty" protobuf:"bytes,3,rep,name=capacity"`
}

// ShareableCapacityStatus 报告单个可共享容量键的聚合数值。
type ShareableCapacityStatus struct {
	// Name 是容量名称。
	//
	// +required
	// +k8s:required
	Name string `json:"name,omitempty" protobuf:"bytes,1,name=name"`

	// Total 是池中所有可共享设备上该容量的总和。
	//
	// +required
	// +k8s:required
	Total *resource.Quantity `json:"total,omitempty" protobuf:"bytes,2,opt,name=total"`

	// Consumed 是当前分配已占用的量。
	//
	// +required
	// +k8s:required
	Consumed *resource.Quantity `json:"consumed,omitempty" protobuf:"bytes,3,opt,name=consumed"`

	// Available 等于 Total 减去 Consumed，永不为负。
	//
	// +required
	// +k8s:required
	Available *resource.Quantity `json:"available,omitempty" protobuf:"bytes,4,opt,name=available"`
}

// ResourcePoolStatusRequestConditionComplete 是已完成请求的条件类型。
const ResourcePoolStatusRequestConditionComplete = "Complete"

// ResourcePoolStatusRequestConditionFailed 是失败请求的条件类型。
const ResourcePoolStatusRequestConditionFailed = "Failed"

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.36

// ResourcePoolStatusRequestList 是 ResourcePoolStatusRequest 的集合。
type ResourcePoolStatusRequestList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourcePoolStatusRequest 列表。
	Items []ResourcePoolStatusRequest `json:"items" protobuf:"bytes,2,rep,name=items"`
}
