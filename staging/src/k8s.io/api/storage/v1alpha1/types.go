/*
Copyright 2017 The Kubernetes Authors.

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

package v1alpha1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,VolumeAttachment
// +k8s:supportsSubresource="/status"

// VolumeAttachment 表达将指定卷附加到指定节点或从指定节点分离卷的意图。
//
// VolumeAttachment 对象是非命名空间对象。
type VolumeAttachment struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 表示期望的卷附加/分离行为，由 Kubernetes 系统填充。
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +required
	Spec VolumeAttachmentSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示 VolumeAttachment 请求的状态，由完成附加或分离操作的实体
	// （即 external-attacher）填充。
	// +optional
	Status VolumeAttachmentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.9
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,VolumeAttachmentList

// VolumeAttachmentList 是 VolumeAttachment 对象的集合。
type VolumeAttachmentList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 VolumeAttachment 列表。
	Items []VolumeAttachment `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// VolumeAttachmentSpec 是 VolumeAttachment 请求的规格。
type VolumeAttachmentSpec struct {
	// attacher 表示必须处理此请求的卷驱动名称，即 GetPluginName() 返回的名称。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:format="k8s-long-name-caseless"
	// +k8s:beta(since: "1.37")=+k8s:maxLength=63
	Attacher string `json:"attacher" protobuf:"bytes,1,opt,name=attacher"`

	// source 表示应附加的卷。
	Source VolumeAttachmentSource `json:"source" protobuf:"bytes,2,opt,name=source"`

	// nodeName 表示卷应附加到的节点。
	NodeName string `json:"nodeName" protobuf:"bytes,3,opt,name=nodeName"`
}

// VolumeAttachmentSource 表示应附加的卷。目前通过 external attacher 只能附加 PersistentVolume，
// 将来可能也支持 Pod 中的内联卷。必须且只能设置一个成员。
type VolumeAttachmentSource struct {
	// persistentVolumeName 表示要附加的持久卷的名称。
	// +optional
	PersistentVolumeName *string `json:"persistentVolumeName,omitempty" protobuf:"bytes,1,opt,name=persistentVolumeName"`

	// inlineVolumeSpec 包含附加 Pod 内联 VolumeSource 所定义的持久卷所需的全部信息。
	// 此字段仅为 CSIMigration 特性填充，包含从 Pod 内联 VolumeSource 转换到
	// PersistentVolumeSpec 的字段。这是 alpha 字段，只有启用 CSIMigration 特性的服务端才处理。
	// +optional
	InlineVolumeSpec *v1.PersistentVolumeSpec `json:"inlineVolumeSpec,omitempty" protobuf:"bytes,2,opt,name=inlineVolumeSpec"`
}

// VolumeAttachmentStatus 是 VolumeAttachment 请求的状态。
type VolumeAttachmentStatus struct {
	// attached 表示卷已成功附加。此字段只能由完成附加操作的实体（即 external-attacher）设置。
	Attached bool `json:"attached" protobuf:"varint,1,opt,name=attached"`

	// attachmentMetadata 在附加成功后由附加操作返回的信息填充，必须传递给后续的
	// WaitForAttach 或 Mount 调用。此字段只能由完成附加操作的实体（即 external-attacher）设置。
	// +optional
	AttachmentMetadata map[string]string `json:"attachmentMetadata,omitempty" protobuf:"bytes,2,rep,name=attachmentMetadata"`

	// attachError 表示附加操作中遇到的最后一个错误（若有）。此字段只能由完成附加操作的
	// 实体（即 external-attacher）设置。
	// +optional
	AttachError *VolumeError `json:"attachError,omitempty" protobuf:"bytes,3,opt,name=attachError,casttype=VolumeError"`

	// detachError 表示分离操作中遇到的最后一个错误（若有）。此字段只能由完成分离操作的
	// 实体（即 external-attacher）设置。
	// +optional
	DetachError *VolumeError `json:"detachError,omitempty" protobuf:"bytes,4,opt,name=detachError,casttype=VolumeError"`
}

// VolumeError 记录卷操作期间遇到的错误。
type VolumeError struct {
	// time 表示遇到错误的时间。
	// +optional
	Time metav1.Time `json:"time,omitempty" protobuf:"bytes,1,opt,name=time"`

	// message 表示 Attach 或 Detach 操作期间遇到的错误。此字符串可能被记录到日志，
	// 因此不应包含敏感信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`

	// errorCode 是表示 Attach 或 Detach 操作期间遇到错误的数字 gRPC 码。
	//
	// 这是可选的 alpha 字段，需要启用 MutableCSINodeAllocatableCount 特性门控才可设置。
	//
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	ErrorCode *int32 `json:"errorCode,omitempty" protobuf:"varint,3,opt,name=errorCode"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1beta1,CSIStorageCapacity

// CSIStorageCapacity 保存一次 CSI GetCapacity 调用的结果，描述给定 StorageClass 在某个
// 拓扑分段中的可用容量，可用于决定在何处创建新的 PersistentVolume。
//
// 例如可表达如下信息：
// - 存储类别 "standard" 在 "topology.kubernetes.io/zone=us-east1" 有 "1234 GiB" 可用
// - 存储类别 "localssd" 在 "kubernetes.io/hostname=knode-abc123" 有 "10 GiB" 可用
//
// 以下三种情况都表示某个组合没有可用容量：
// - 不存在拓扑和存储类别名称匹配的对象
// - 存在这样的对象，但其 capacity 未设置
// - 存在这样的对象，但其 capacity 为零
//
// 这些对象的生产者可自行决定采用哪种方式。
//
// 当 CSI 驱动通过 CSIDriverSpec.StorageCapacity 选择容量感知调度时，kube-scheduler 会消费
// 这些对象：将 MaximumVolumeSize 与待处理卷的请求大小比较，过滤不合适的节点。若
// MaximumVolumeSize 未设置，则退而使用精度较低的 Capacity 比较；若后者也未设置，
// 调度器认为容量不足并尝试其他节点。
type CSIStorageCapacity struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。名称无特殊含义，但必须是 DNS 子域（允许点，最多 253 个字符）。
	// 为避免与集群中其他 CSI 驱动冲突，建议使用 csisc-<uuid>、生成的名称，或以唯一 CSI 驱动名
	// 结尾的反向域名。
	//
	// 对象是有命名空间的。
	//
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// nodeTopology 定义哪些节点可访问所报告容量的存储。未设置时集群中任何节点都无法访问该存储；
	// 为空时所有节点都可访问。此字段不可变。
	//
	// +optional
	NodeTopology *metav1.LabelSelector `json:"nodeTopology,omitempty" protobuf:"bytes,2,opt,name=nodeTopology"`

	// storageClassName 表示所报告容量适用的 StorageClass 名称，必须满足 StorageClass 对象名称的
	// 要求（非空、DNS 子域）。若该对象已不存在，则 CSIStorageCapacity 对象已过时，应由其创建者删除。
	// 此字段不可变。
	StorageClassName string `json:"storageClassName" protobuf:"bytes,3,name=storageClassName"`

	// capacity 是 CSI 驱动在 GetCapacityResponse 中针对与前述字段匹配的拓扑和参数所报告的容量值。
	//
	// 当前语义（CSI 规范 1.2）定义为：可用于供给卷的存储可用容量（字节）。未设置时表示该信息
	// 当前不可用。
	//
	// +optional
	Capacity *resource.Quantity `json:"capacity,omitempty" protobuf:"bytes,4,opt,name=capacity"`

	// maximumVolumeSize 是 CSI 驱动在 GetCapacityResponse 中针对与前述字段匹配的拓扑和参数所报告的
	// 值。自 CSI 规范 1.4.0 起定义为 CreateVolumeRequest.capacity_range.required_bytes 字段中
	// 可用于创建与 GetCapacityRequest 参数相同的卷的最大尺寸。Kubernetes API 中的对应值是
	// 卷声明中的 ResourceRequirements.Requests。
	//
	// +optional
	MaximumVolumeSize *resource.Quantity `json:"maximumVolumeSize,omitempty" protobuf:"bytes,5,opt,name=maximumVolumeSize"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.19
// +k8s:prerelease-lifecycle-gen:deprecated=1.21
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1beta1,CSIStorageCapacityList

// CSIStorageCapacityList 是 CSIStorageCapacity 对象的集合。
type CSIStorageCapacityList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSIStorageCapacity 对象列表。
	Items []CSIStorageCapacity `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:prerelease-lifecycle-gen:introduced=1.29
// +k8s:prerelease-lifecycle-gen:deprecated=1.32
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,VolumeAttributesClass
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClass 表示 CSI 驱动定义的可变卷属性规格。该类可在 PersistentVolumeClaim
// 动态供给时指定，并在供给后通过 PersistentVolumeClaim spec 更改。
type VolumeAttributesClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// driverName 是 CSI 驱动的名称，此字段不可变。
	DriverName string `json:"driverName" protobuf:"bytes,2,opt,name=driverName"`

	// parameters 保存 CSI 驱动定义的卷属性，这些值对 Kubernetes 是不透明的，直接传给 CSI 驱动。
	// 底层存储供应商支持修改已有卷的这些属性，但 parameters 字段本身不可变。要触发卷更新，
	// 应创建带有新参数的新 VolumeAttributesClass，并更新 PersistentVolumeClaim 以引用新类。
	//
	// 此字段为必填，至少包含一个键/值对。键不能为空，参数最多 512 个，累计大小上限 256K。
	// 若 CSI 驱动拒绝无效参数，目标 PersistentVolumeClaim 的 modifyVolumeStatus 字段会被
	// 设为 "Infeasible" 状态。
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,3,rep,name=parameters"`
}

// +k8s:prerelease-lifecycle-gen:introduced=1.29
// +k8s:prerelease-lifecycle-gen:deprecated=1.32
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,VolumeAttributesClassList
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClassList 是 VolumeAttributesClass 对象的集合。
type VolumeAttributesClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 VolumeAttributesClass 对象列表。
	Items []VolumeAttributesClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}
