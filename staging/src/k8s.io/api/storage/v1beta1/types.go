/*
Copyright 2016 The Kubernetes Authors.

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
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.4
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,StorageClass

// StorageClass 描述可动态供给 PersistentVolume 的存储类别参数。
//
// StorageClass 是非命名空间对象，其在 etcd 中的名称保存在 ObjectMeta.Name 中。
type StorageClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// provisioner 表示供给方的类型。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:immutable
	Provisioner string `json:"provisioner" protobuf:"bytes,2,opt,name=provisioner"`

	// parameters 保存创建该存储类别卷的供给方所需参数。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,3,rep,name=parameters"`

	// reclaimPolicy 控制本存储类别动态供给的 PersistentVolume 的回收策略，默认为 Delete。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	ReclaimPolicy *v1.PersistentVolumeReclaimPolicy `json:"reclaimPolicy,omitempty" protobuf:"bytes,4,opt,name=reclaimPolicy,casttype=k8s.io/api/core/v1.PersistentVolumeReclaimPolicy"`

	// mountOptions 控制本存储类别动态供给的 PersistentVolume 的挂载选项，例如 ["ro", "soft"]。
	// 这些选项不做校验，若存在无效项，PV 挂载会直接失败。
	// +optional
	// +listType=atomic
	MountOptions []string `json:"mountOptions,omitempty" protobuf:"bytes,5,opt,name=mountOptions"`

	// allowVolumeExpansion 表示此存储类别是否允许卷扩容。
	// +optional
	AllowVolumeExpansion *bool `json:"allowVolumeExpansion,omitempty" protobuf:"varint,6,opt,name=allowVolumeExpansion"`

	// volumeBindingMode 指示 PersistentVolumeClaim 应如何供给和绑定，未设置时使用 VolumeBindingImmediate。
	// 只有启用了 VolumeScheduling 特性的服务端才会处理此字段。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	VolumeBindingMode *VolumeBindingMode `json:"volumeBindingMode,omitempty" protobuf:"bytes,7,opt,name=volumeBindingMode"`

	// allowedTopologies 限制可动态供给卷的节点拓扑范围。各卷插件自行定义支持的拓扑规格。
	// TopologySelectorTerm 列表为空表示没有拓扑限制。只有启用了 VolumeScheduling 特性的
	// 服务端才会处理此字段。
	// +optional
	// +listType=atomic
	AllowedTopologies []v1.TopologySelectorTerm `json:"allowedTopologies,omitempty" protobuf:"bytes,8,rep,name=allowedTopologies"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.4
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,StorageClassList

// StorageClassList 是存储类别的集合。
type StorageClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 StorageClass 列表。
	Items []StorageClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// VolumeBindingMode 指示 PersistentVolumeClaim 应如何绑定。
type VolumeBindingMode string

const (
	// VolumeBindingImmediate 表示应立即供给并绑定 PersistentVolumeClaim，这是默认模式。
	VolumeBindingImmediate VolumeBindingMode = "Immediate"

	// VolumeBindingWaitForFirstConsumer 表示在首个引用该 PersistentVolumeClaim 的 Pod 创建前，
	// 不应供给和绑定卷。卷的供给与绑定将在 Pod 调度期间进行。
	VolumeBindingWaitForFirstConsumer VolumeBindingMode = "WaitForFirstConsumer"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.10
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
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
// +k8s:prerelease-lifecycle-gen:introduced=1.10
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
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
	// PersistentVolumeSpec 的字段。这是 beta 字段，只有启用 CSIMigration 特性的服务端才处理。
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
	// 这是可选的 beta 字段，需要启用 MutableCSINodeAllocatableCount 特性门控才可设置。
	//
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	ErrorCode *int32 `json:"errorCode,omitempty" protobuf:"varint,3,opt,name=errorCode"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSIDriver

// CSIDriver 记录部署在集群中的容器存储接口（CSI）卷驱动的信息。
// CSI 驱动无需直接创建 CSIDriver 对象，可使用 cluster-driver-registrar sidecar 容器：
// 与 CSI 驱动一起部署时，它会自动创建代表该驱动的 CSIDriver 对象。
// Kubernetes 的 attach detach 控制器据此判断是否需要附加卷，Kubelet 据此判断挂载时是否
// 需要传递 Pod 信息。CSIDriver 对象是非命名空间对象。
type CSIDriver struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据。metadata.Name 表示本对象所指 CSI 驱动的名称，
	// 必须与该驱动 CSI GetPluginName() 调用返回的名称一致。驱动名称不得超过 63 个字符，
	// 以字母数字字符（[a-z0-9A-Z]）开头和结尾，中间可含连字符（-）、点（.）和字母数字。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 表示 CSI 驱动的规格。
	Spec CSIDriverSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.19
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSIDriverList

// CSIDriverList 是 CSIDriver 对象的集合。
type CSIDriverList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSIDriver 列表。
	Items []CSIDriver `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CSIDriverSpec 是 CSIDriver 的规格。
type CSIDriverSpec struct {
	// attachRequired 表示此 CSI 卷驱动需要附加操作（因其实现了 CSI ControllerPublishVolume()
	// 方法），Kubernetes attach detach 控制器应调用附加卷接口，检查 volumeattachment 状态并
	// 等待卷附加完成后再进行挂载。CSI external-attacher 与卷驱动协作，在附加操作完成时
	// 更新 volumeattachment 状态。设为 false 时跳过附加操作，否则调用附加操作。
	//
	// 此字段不可变。
	//
	// +optional
	AttachRequired *bool `json:"attachRequired,omitempty" protobuf:"varint,1,opt,name=attachRequired"`

	// podInfoOnMount 设为 true 时表示此 CSI 卷驱动在挂载操作中需要额外的 Pod 信息
	// （如 podName、podUID 等）；设为 false 时挂载时不传递 Pod 信息。默认为 false。
	//
	// CSI 驱动在部署时指定 podInfoOnMount。若为 true，Kubelet 会在 CSI NodePublishVolume()
	// 调用中把 Pod 信息作为 VolumeContext 传入，驱动负责解析和校验这些信息。
	//
	// podInfoOnMount 为 true 时将传递如下 VolumeContext。此列表可能增长，但前缀保持不变。
	// "csi.storage.k8s.io/pod.name": pod.Name
	// "csi.storage.k8s.io/pod.namespace": pod.Namespace
	// "csi.storage.k8s.io/pod.uid": string(pod.UID)
	// "csi.storage.k8s.io/ephemeral": 卷是由 CSIVolumeSource 定义的临时内联卷时为 "true"，否则为 "false"
	//
	// "csi.storage.k8s.io/ephemeral" 是 Kubernetes 1.16 引入的新特性，仅同时支持
	// "Persistent" 和 "Ephemeral" 两种 VolumeLifecycleMode 的驱动才需要它。其他驱动可
	// 保持 Pod 信息传递关闭和/或忽略此字段。由于 Kubernetes 1.15 不支持该字段，驱动在这类
	// 集群上部署时只能支持一种模式，具体模式由部署方式（例如驱动的命令行参数）决定。
	//
	// 此字段不可变。
	//
	// +optional
	PodInfoOnMount *bool `json:"podInfoOnMount,omitempty" protobuf:"bytes,2,opt,name=podInfoOnMount"`

	// volumeLifecycleModes 定义此 CSI 卷驱动支持哪类卷。列表为空时默认为 "Persistent"，
	// 即 CSI 规范定义、Kubernetes 通过常规 PV/PVC 机制实现的用法。
	//
	// 另一种模式为 "Ephemeral"。此模式下卷通过 CSIVolumeSource 内联定义在 Pod spec 中，
	// 其生命周期与 Pod 绑定。驱动必须知晓这一点，因为此类卷只会收到 NodePublishVolume 调用。
	//
	// 实现此模式的更多信息参见 https://kubernetes-csi.github.io/docs/ephemeral-local-volumes.html
	// 一个驱动可支持一种或多种模式，未来可能新增更多模式。
	//
	// 此字段不可变。
	//
	// +optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	VolumeLifecycleModes []VolumeLifecycleMode `json:"volumeLifecycleModes,omitempty" protobuf:"bytes,3,opt,name=volumeLifecycleModes"`

	// storageCapacity 设为 true 时表示 CSI 卷驱动希望 Pod 调度考虑其通过创建带有容量信息的
	// CSIStorageCapacity 对象所报告的存储容量。
	//
	// 部署驱动时可立即启用此检查，此时采用延迟绑定的新卷供给会暂停，直到驱动部署发布了
	// 合适的 CSIStorageCapacity 对象。也可先不设置该字段或设为 false 部署驱动，待容量信息
	// 发布后再切换。
	//
	// 此字段在 Kubernetes <= 1.22 中不可变，现在可变。
	//
	// +optional
	StorageCapacity *bool `json:"storageCapacity,omitempty" protobuf:"bytes,4,opt,name=storageCapacity"`

	// fsGroupPolicy 定义底层卷是否支持在挂载前修改卷的所有权和权限，具体细节参见各 FSGroupPolicy 取值。
	//
	// 此字段不可变。
	//
	// 默认为 ReadWriteOnceWithFSType：逐个检查卷以决定是否由 Kubernetes 修改其所有权和权限。
	// 使用默认策略时，只有在定义了 fstype 且卷的访问模式包含 ReadWriteOnce 时才会应用 fsGroup。
	//
	// +optional
	FSGroupPolicy *FSGroupPolicy `json:"fsGroupPolicy,omitempty" protobuf:"bytes,5,opt,name=fsGroupPolicy"`

	// tokenRequests 表示 CSI 驱动需要其所挂载卷对应 Pod 的 service account token 来完成必要的认证。
	// Kubelet 会在 CSI NodePublishVolume 调用中把 token 放入 VolumeContext 传递。
	// CSI 驱动应解析并校验如下 VolumeContext：
	// "csi.storage.k8s.io/serviceAccount.tokens": {
	//   "<audience>": {
	//     "token": <token>,
	//     "expirationTimestamp": <expiration timestamp in RFC3339>,
	//   },
	//   ...
	// }
	//
	// 注意：每个 TokenRequest 的 audience 应互不相同，且最多只有一个 token 为空字符串。
	// 若要在过期后获取新 token，可使用 RequiresRepublish 定期触发 NodePublishVolume。
	//
	// +optional
	// +listType=atomic
	TokenRequests []TokenRequest `json:"tokenRequests,omitempty" protobuf:"bytes,6,opt,name=tokenRequests"`

	// requiresRepublish 表示 CSI 驱动希望定期调用 `NodePublishVolume`，以反映已挂载卷中可能发生的
	// 变化，默认为 false。
	//
	// 注意：首次 NodePublishVolume 调用成功后，后续调用只应更新卷内容，运行中的容器看不到新的挂载点。
	//
	// +optional
	RequiresRepublish *bool `json:"requiresRepublish,omitempty" protobuf:"varint,7,opt,name=requiresRepublish"`

	// seLinuxMount 指定 CSI 驱动是否支持 "-o context" 挂载选项。
	//
	// 为 "true" 时，驱动必须确保其提供的所有卷都能分别使用不同的 `-o context` 选项挂载，
	// 这通常见于将卷作为块设备上的文件系统或独立共享卷提供的存储后端。当挂载显式设置了
	// SELinux 上下文的 Pod 所使用的 ReadWriteOncePod 卷时，Kubernetes 会在 NodeStage /
	// NodePublish 调用中传入 "-o context=xyz" 挂载选项。未来可能扩展到其他卷 AccessMode。
	// 无论如何，Kubernetes 会确保卷只以单一 SELinux 上下文挂载。
	//
	// 为 "false" 时，Kubernetes 不会向驱动传递任何特殊的 SELinux 挂载选项，这通常见于
	// 表示较大共享文件系统子目录的卷。
	//
	// 默认为 "false"。
	//
	// +featureGate=SELinuxMountReadWriteOncePod
	// +optional
	SELinuxMount *bool `json:"seLinuxMount,omitempty" protobuf:"varint,8,opt,name=seLinuxMount"`

	// nodeAllocatableUpdatePeriodSeconds 指定此驱动的 CSINode 可分配容量周期性更新的间隔。
	// 设置后，周期性更新和由容量相关故障触发的更新都会启用。未设置时两类更新均不发生，
	// allocatable.count 保持静态。此字段最小允许值为 10 秒。
	//
	// 这是 beta 特性，需要启用 MutableCSINodeAllocatableCount 特性门控。
	//
	// 此字段可变。
	//
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	NodeAllocatableUpdatePeriodSeconds *int64 `json:"nodeAllocatableUpdatePeriodSeconds,omitempty" protobuf:"varint,9,opt,name=nodeAllocatableUpdatePeriodSeconds"`

	// serviceAccountTokenInSecrets 是 CSI 驱动的可选项，表示 service account token 应通过
	// NodePublishVolumeRequest 的 Secrets 字段而非 VolumeContext 字段传递。CSI 规范为 token
	// 等敏感信息提供了专门的 Secrets 字段，这是处理凭据的合适机制，可避免敏感 token 作为
	// 卷上下文的一部分被记录到日志。
	//
	// 为 "true" 时，kubelet 只把 token 放在 Secrets 字段中，键为
	// "csi.storage.k8s.io/serviceAccount.tokens"。CSI 驱动必须相应更新为从 Secrets 字段
	// （而非 VolumeContext）读取 token。
	//
	// 为 "false" 或未设置时，kubelet 仍把 token 放在 VolumeContext 中，键为
	// "csi.storage.k8s.io/serviceAccount.tokens"（既有行为），以保持与现有 CSI 驱动的向后兼容。
	//
	// 只有配置了 TokenRequests 时才可设置此字段；未配置 TokenRequests 却设置该字段的
	// CSIDriver spec 会被 API server 拒绝。未设置时默认在 VolumeContext 字段中传递 token。
	//
	// +featureGate=CSIServiceAccountTokenSecrets
	// +optional
	ServiceAccountTokenInSecrets *bool `json:"serviceAccountTokenInSecrets,omitempty" protobuf:"varint,10,opt,name=serviceAccountTokenInSecrets"`

	// preventPodSchedulingIfMissing 表示 CSI 驱动希望在节点上缺少该驱动时阻止 Pod 调度。
	//
	// 启用此选项会阻止调度器（或任何内嵌默认调度器的组件，如 cluster-autoscaler）把 Pod
	// 调度到未安装该 CSI 驱动的节点上。
	//
	// 对内嵌调度器并使用调度器插件模拟 Pod 放置的组件（如 cluster-autoscaler），必须通过
	// CSINode 对象感知 CSI 驱动的注册信息。它们在调度模拟中除创建 Node 对象外还必须创建
	// 模拟的 CSINode 对象；否则当 CSIDriver 对象全局启用了 PreventPodSchedulingIfMissing 时，
	// 任何新建节点都可能因缺少 CSI 驱动信息而被调度器拒绝。
	//
	// 这是 beta 特性，需要启用 VolumeLimitScaling 特性门控，默认为 "false"。
	// +featureGate=VolumeLimitScaling
	// +optional
	PreventPodSchedulingIfMissing *bool `json:"preventPodSchedulingIfMissing,omitempty" protobuf:"varint,11,opt,name=preventPodSchedulingIfMissing"`
}

// FSGroupPolicy 指定 CSI 驱动是否支持修改待挂载卷的所有权和权限，将来可能新增更多模式。
type FSGroupPolicy string

const (
	// ReadWriteOnceWithFSTypeFSGroupPolicy 表示逐个检查卷以决定是否修改其所有权和权限。
	// 若定义了 fstype 且卷的访问模式包含 ReadWriteOnce，则应用定义的 fsGroup。
	// 未定义其他 FSGroupPolicy 时此为默认行为。
	ReadWriteOnceWithFSTypeFSGroupPolicy FSGroupPolicy = "ReadWriteOnceWithFSType"

	// FileFSGroupPolicy 表示 CSI 驱动支持通过 fsGroup 修改卷的所有权和权限。无论 fstype 或
	// 访问模式如何，Kubernetes 都会把卷中每个文件的权限和所有权改为与 Pod SecurityPolicy
	// 中用户请求的 fsGroup 一致。若希望由 Kubernetes 修改卷的权限和所有权，请使用此模式。
	FileFSGroupPolicy FSGroupPolicy = "File"

	// None 表示挂载卷时不修改任何所有权或权限，因为 CSIDriver 不支持这些操作。
	NoneFSGroupPolicy FSGroupPolicy = "None"
)

// VolumeLifecycleMode 枚举 CSI 驱动提供的卷的可能使用模式，将来可能新增更多模式。
type VolumeLifecycleMode string

// TokenRequest 包含 service account token 的参数。
type TokenRequest struct {
	// audience 是 "TokenRequestSpec" 中 token 的目标受众，默认为 kube apiserver 的受众。
	Audience string `json:"audience" protobuf:"bytes,1,opt,name=audience"`

	// expirationSeconds 是 "TokenRequestSpec" 中 token 的有效时长，与 "TokenRequestSpec" 中
	// "ExpirationSeconds" 的默认值相同。
	//
	// +optional
	ExpirationSeconds *int64 `json:"expirationSeconds,omitempty" protobuf:"varint,2,opt,name=expirationSeconds"`
}

const (
	// VolumeLifecyclePersistent 明确确认驱动实现了完整 CSI 规范。当
	// CSIDriverSpec.VolumeLifecycleModes 未设置时此为默认值。此类卷在 Kubernetes 中通过持久卷
	// 声明机制管理，其生命周期独立于使用它们的 Pod。
	VolumeLifecyclePersistent VolumeLifecycleMode = "Persistent"

	// VolumeLifecycleEphemeral 表示驱动可用于临时内联卷。此类卷通过 CSIVolumeSource 内联定义在
	// Pod spec 中，就 Kubernetes 而言其生命周期与 Pod 绑定。例如这类卷可能包含专门为某个 Pod
	// 创建的数据（如 secret）。但卷实际如何创建和管理完全由驱动决定；若多个 Pod 的
	// CSIVolumeSource 相同，驱动也可通过引用计数在这些 Pod 间共享同一卷实例。
	VolumeLifecycleEphemeral VolumeLifecycleMode = "Ephemeral"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSINode
// +k8s:supportsSubresource="/status"

// 已弃用 - 本组版本的 CSINode 已被 storage/v1/CSINode 取代，详情参见发布说明。
// CSINode 保存节点上安装的所有 CSI 驱动的信息。CSI 驱动无需直接创建 CSINode 对象：
// 只要使用 node-driver-registrar sidecar 容器，kubelet 就会在插件注册过程中自动为该驱动
// 填充 CSINode 对象。CSINode 与节点同名；对象缺失意味着节点上没有可用 CSI 驱动，或 Kubelet
// 版本过低未能创建此对象。CSINode 带有指向对应 Node 对象的 OwnerReference。
type CSINode struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准对象元数据，其中 metadata.name 必须是 Kubernetes 节点名称。
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 是 CSINode 的规格。
	Spec CSINodeSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 包含节点存储的健康和状态信息。
	// +optional
	Status CSINodeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// CSINodeSpec 保存节点上所有已安装 CSI 驱动的规格信息。
type CSINodeSpec struct {
	// drivers 是节点上所有现存 CSI 驱动的信息列表；若所有驱动都被卸载，此列表可为空。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Drivers []CSINodeDriver `json:"drivers" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,1,rep,name=drivers"`
}

// CSINodeDriver 保存节点上一个已安装 CSI 驱动的规格信息。
type CSINodeDriver struct {
	// name 表示本对象所指 CSI 驱动的名称，必须与该驱动 CSI GetPluginName() 调用返回的名称一致。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// nodeID 是从驱动视角看到的节点 ID。此字段使 Kubernetes 能与节点命名规则不同的存储系统通信。
	// 例如 Kubernetes 称某节点为 "node1"，而存储系统称同一节点为 "nodeA"；当 Kubernetes 向存储
	// 系统下发将卷附加到特定节点的命令时，可用此字段以存储系统可识别的 ID（如 "nodeA" 而非
	// "node1"）指代节点。此字段为必填。
	NodeID string `json:"nodeID" protobuf:"bytes,2,opt,name=nodeID"`

	// topologyKeys 是驱动支持的键列表。驱动在集群上初始化时提供一组它理解的拓扑键
	// （如 "company.com/zone"、"company.com/region"）；在节点上初始化时提供相同的拓扑键及取值。
	// Kubelet 会把这些拓扑键作为标签暴露在自己的 Node 对象上。Kubernetes 做拓扑感知供给时，
	// 可据此列表决定从节点对象取哪些标签并回传给驱动。不同节点可能使用不同的拓扑键；
	// 若驱动不支持拓扑，此列表可为空。
	// +optional
	// +listType=atomic
	TopologyKeys []string `json:"topologyKeys" protobuf:"bytes,3,rep,name=topologyKeys"`

	// allocatable 表示节点上可供调度使用的卷资源。
	// +optional
	Allocatable *VolumeNodeResources `json:"allocatable,omitempty" protobuf:"bytes,4,opt,name=allocatable"`
}

// VolumeNodeResources 是卷调度用的一组资源限制。
type VolumeNodeResources struct {
	// count 表示 CSI 驱动管理的、可在节点上使用的唯一卷的最大数量。同时附加并挂载到节点上的卷
	// 只算一次使用，而非两次；同一唯一卷在同一节点上被多个 Pod 共享时也遵循此规则。
	// 若此字段为 nil，则此节点支持的卷数量无上限。
	// +optional
	Count *int32 `json:"count,omitempty" protobuf:"varint,1,opt,name=count"`
}

// StorageHealthStatusType 描述存储后端的健康状态类别。
// +enum
type StorageHealthStatusType string

const (
	// StorageUnreachable 表示存储后端不可达。
	StorageUnreachable StorageHealthStatusType = "StorageUnreachable"
	// StorageDegraded 表示存储后端以降级能力运行。
	StorageDegraded StorageHealthStatusType = "StorageDegraded"
)

// StorageHealthCondition 表示 CSI 驱动报告的、其节点上存储后端的异常健康状态。
type StorageHealthCondition struct {
	// status 是健康状态类别，取值为 "StorageUnreachable" 或 "StorageDegraded"。
	// +required
	Status StorageHealthStatusType `json:"status" protobuf:"bytes,1,opt,name=status,casttype=StorageHealthStatusType"`
	// reason 是简短的 CamelCase 格式、可供机器解析的原因。
	// +required
	// reason 允许的最大长度为 256 个字符。
	Reason string `json:"reason" protobuf:"bytes,2,opt,name=reason"`
	// message 是人类可读的描述。
	// +optional
	// message 允许的最大长度为 1024 个字符。
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// accessMode 是受影响的访问模式，nil 表示所有访问模式均受影响。
	// +optional
	AccessMode *v1.PersistentVolumeAccessMode `json:"accessMode,omitempty" protobuf:"bytes,4,opt,name=accessMode,casttype=k8s.io/api/core/v1.PersistentVolumeAccessMode"`
	// volumeMode 是受影响的卷模式，nil 表示两种模式均受影响。
	// +optional
	VolumeMode *v1.PersistentVolumeMode `json:"volumeMode,omitempty" protobuf:"bytes,5,opt,name=volumeMode,casttype=k8s.io/api/core/v1.PersistentVolumeMode"`
	// lastTransitionTime 是本条件首次以当前状态出现的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,6,opt,name=lastTransitionTime"`
}

// StorageHealth 包含 CSI 驱动在节点上报告的存储后端健康信息。
type StorageHealth struct {
	// name 是 CSI 驱动名称，与 CSINodeDriver.name 一致。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// healthConditions 是 CSI 驱动报告的异常存储后端状态，最多可报告 16 条。
	// +optional
	// +listType=atomic
	HealthConditions []StorageHealthCondition `json:"healthConditions,omitempty" protobuf:"bytes,2,rep,name=healthConditions"`
}

// CSINodeStatus 包含节点上存储的健康和状态信息。
type CSINodeStatus struct {
	// storageHealth 包含节点上已注册 CSI 驱动的后端健康报告。
	// +optional
	// +listType=map
	// +listMapKey=name
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +featureGate=CSIVolumeHealth
	StorageHealth []StorageHealth `json:"storageHealth,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,1,rep,name=storageHealth"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.14
// +k8s:prerelease-lifecycle-gen:deprecated=1.17
// +k8s:prerelease-lifecycle-gen:removed=1.22
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSINode

// CSINodeList 是 CSINode 对象的集合。
type CSINodeList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSINode 列表。
	Items []CSINode `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.21
// +k8s:prerelease-lifecycle-gen:deprecated=1.24
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSIStorageCapacity

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
// +k8s:prerelease-lifecycle-gen:introduced=1.21
// +k8s:prerelease-lifecycle-gen:deprecated=1.24
// +k8s:prerelease-lifecycle-gen:replacement=storage.k8s.io,v1,CSIStorageCapacityList

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
// +k8s:prerelease-lifecycle-gen:introduced=1.31
// +k8s:prerelease-lifecycle-gen:deprecated=1.34
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

// +k8s:prerelease-lifecycle-gen:introduced=1.31
// +k8s:prerelease-lifecycle-gen:deprecated=1.34
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
