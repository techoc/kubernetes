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

package v1

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6

// StorageClass 描述一类存储的参数，可为该类存储动态创建 PersistentVolume。
//
// StorageClass 是非命名空间级的；根据 etcd，存储类的名称保存在 ObjectMeta.Name 中。
type StorageClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// provisioner 表示 provisioner 的类型。
	// +required
	// +k8s:beta(since: "1.37")=+k8s:required
	// +k8s:beta(since: "1.37")=+k8s:immutable
	Provisioner string `json:"provisioner" protobuf:"bytes,2,opt,name=provisioner"`

	// parameters 保存 provisioner 的参数，该 provisioner 负责创建此存储类的卷。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,3,rep,name=parameters"`

	// reclaimPolicy 控制此存储类动态创建的 PersistentVolume 的回收策略。
	// 默认为 Delete。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	ReclaimPolicy *v1.PersistentVolumeReclaimPolicy `json:"reclaimPolicy,omitempty" protobuf:"bytes,4,opt,name=reclaimPolicy,casttype=k8s.io/api/core/v1.PersistentVolumeReclaimPolicy"`

	// mountOptions 控制此存储类动态创建的 PersistentVolume 的挂载选项，
	// 例如 ["ro", "soft"]。这些选项不经过校验；若有无效选项，PV 挂载就会失败。
	// +optional
	// +listType=atomic
	MountOptions []string `json:"mountOptions,omitempty" protobuf:"bytes,5,opt,name=mountOptions"`

	// allowVolumeExpansion 表示此存储类是否允许卷扩容。
	// +optional
	AllowVolumeExpansion *bool `json:"allowVolumeExpansion,omitempty" protobuf:"varint,6,opt,name=allowVolumeExpansion"`

	// volumeBindingMode 表示 PersistentVolumeClaim 应如何被创建和绑定。
	// 未设置时使用 VolumeBindingImmediate。
	// 此字段仅在启用了 VolumeScheduling 特性的服务端上生效。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +k8s:beta(since: "1.37")=+k8s:optional
	VolumeBindingMode *VolumeBindingMode `json:"volumeBindingMode,omitempty" protobuf:"bytes,7,opt,name=volumeBindingMode"`

	// allowedTopologies 限制可动态创建卷的节点拓扑范围。
	// 每个卷插件定义自己支持的拓扑规范。
	// 空的 TopologySelectorTerm 列表表示没有拓扑限制。
	// 此字段仅在启用了 VolumeScheduling 特性的服务端上生效。
	// +optional
	// +listType=atomic
	AllowedTopologies []v1.TopologySelectorTerm `json:"allowedTopologies,omitempty" protobuf:"bytes,8,rep,name=allowedTopologies"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6

// StorageClassList 是存储类的集合。
type StorageClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 StorageClass 的列表。
	Items []StorageClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// VolumeBindingMode 表示 PersistentVolumeClaim 应如何被绑定。
// +enum
type VolumeBindingMode string

const (
	// VolumeBindingImmediate 表示 PersistentVolumeClaim 应立即被创建并绑定。这是默认模式。
	VolumeBindingImmediate VolumeBindingMode = "Immediate"

	// VolumeBindingWaitForFirstConsumer 表示直到第一个引用该 PersistentVolumeClaim 的
	// Pod 被创建时，才创建并绑定 PersistentVolumeClaim。卷的创建和绑定会在 Pod 调度期间发生。
	VolumeBindingWaitForFirstConsumer VolumeBindingMode = "WaitForFirstConsumer"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.13
// +k8s:supportsSubresource="/status"

// VolumeAttachment 表达将指定卷挂载到或卸载自指定节点的意图。
//
// VolumeAttachment 对象是非命名空间级的。
type VolumeAttachment struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 表示期望的卷挂载/卸载行为的规格。
	// 由 Kubernetes 系统填充。
	// +k8s:beta(since: "1.37")=+k8s:immutable
	// +required
	Spec VolumeAttachmentSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 表示 VolumeAttachment 请求的状态。
	// 由完成挂载或卸载操作的实体（即 external-attacher）填充。
	// +optional
	Status VolumeAttachmentStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.13

// VolumeAttachmentList 是 VolumeAttachment 对象的集合。
type VolumeAttachmentList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 VolumeAttachment 的列表。
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

	// source 表示应被挂载的卷。
	Source VolumeAttachmentSource `json:"source" protobuf:"bytes,2,opt,name=source"`

	// nodeName 表示卷应被挂载到的节点。
	NodeName string `json:"nodeName" protobuf:"bytes,3,opt,name=nodeName"`
}

// VolumeAttachmentSource 表示应被挂载的卷。
// 目前只有 PersistentVolume 可以通过 external attacher 挂载，将来可能也允许 pod 中的内联卷。
// 只能设置其中一个成员。
type VolumeAttachmentSource struct {
	// persistentVolumeName 表示要挂载的 persistent volume 的名称。
	// +optional
	PersistentVolumeName *string `json:"persistentVolumeName,omitempty" protobuf:"bytes,1,opt,name=persistentVolumeName"`

	// inlineVolumeSpec 包含挂载由 pod 内联 VolumeSource 定义的持久卷所需的全部信息。
	// 此字段仅为 CSIMigration 特性填充，包含从 pod 内联 VolumeSource
	// 转换到 PersistentVolumeSpec 的字段。此字段处于 beta 级别，
	// 仅在启用了 CSIMigration 特性的服务端上生效。
	// +optional
	InlineVolumeSpec *v1.PersistentVolumeSpec `json:"inlineVolumeSpec,omitempty" protobuf:"bytes,2,opt,name=inlineVolumeSpec"`
}

// VolumeAttachmentStatus 是 VolumeAttachment 请求的状态。
type VolumeAttachmentStatus struct {
	// attached 表示卷已成功挂载。
	// 此字段只能由完成挂载操作的实体（即 external-attacher）设置。
	Attached bool `json:"attached" protobuf:"varint,1,opt,name=attached"`

	// attachmentMetadata 保存挂载操作成功后返回的任何信息，
	// 这些信息必须传递给后续的 WaitForAttach 或 Mount 调用。
	// 此字段只能由完成挂载操作的实体（即 external-attacher）设置。
	// +optional
	AttachmentMetadata map[string]string `json:"attachmentMetadata,omitempty" protobuf:"bytes,2,rep,name=attachmentMetadata"`

	// attachError 表示挂载操作期间遇到的最后一个错误（如有）。
	// 此字段只能由完成挂载操作的实体（即 external-attacher）设置。
	// +optional
	AttachError *VolumeError `json:"attachError,omitempty" protobuf:"bytes,3,opt,name=attachError,casttype=VolumeError"`

	// detachError 表示卸载操作期间遇到的最后一个错误（如有）。
	// 此字段只能由完成卸载操作的实体（即 external-attacher）设置。
	// +optional
	DetachError *VolumeError `json:"detachError,omitempty" protobuf:"bytes,4,opt,name=detachError,casttype=VolumeError"`
}

// VolumeError 捕获卷操作期间遇到的错误。
type VolumeError struct {
	// time 表示遇到错误的时间。
	// +optional
	Time metav1.Time `json:"time,omitempty" protobuf:"bytes,1,opt,name=time"`

	// message 表示 Attach 或 Detach 操作期间遇到的错误。
	// 此字符串可能被记录到日志中，因此不应包含敏感信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`

	// errorCode 是用数字 gRPC 码表示的 Attach 或 Detach 操作期间遇到的错误。
	//
	// 设置此字段需要启用 MutableCSINodeAllocatableCount 特性门控。
	//
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	ErrorCode *int32 `json:"errorCode,omitempty" protobuf:"varint,3,opt,name=errorCode"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.18

// CSIDriver 保存部署在集群中的容器存储接口（CSI）卷驱动的信息。
// Kubernetes 挂载/卸载控制器使用此对象判断是否需要执行挂载。
// Kubelet 使用此对象判断挂载时是否需要传递 pod 信息。
// CSIDriver 对象是非命名空间级的。
type CSIDriver struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// metadata.Name 表示此对象所指的 CSI 驱动名称，必须与对该驱动调用
	// CSI GetPluginName() 返回的名称相同。
	// 驱动名称不得超过 63 个字符，以字母数字字符（[a-z0-9A-Z]）开头和结尾，
	// 中间可包含短横线（-）、点（.）和字母数字字符。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 表示 CSI 驱动的规格。
	Spec CSIDriverSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.18

// CSIDriverList 是 CSIDriver 对象的集合。
type CSIDriverList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSIDriver 的列表。
	Items []CSIDriver `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// CSIDriverSpec 是 CSIDriver 的规格。
type CSIDriverSpec struct {
	// attachRequired 表示此 CSI 卷驱动需要执行挂载操作（因为它实现了 CSI
	// ControllerPublishVolume() 方法），且 Kubernetes 挂载/卸载控制器应调用
	// 挂载卷接口，检查 volumeattachment 状态并等待卷挂载完成后再进行挂载。
	// CSI external-attacher 与 CSI 卷驱动协作，在挂载操作完成时更新
	// volumeattachment 状态。
	// 若此值设为 false，则跳过挂载操作，否则会调用挂载操作。
	//
	// 此字段不可变。
	//
	// +optional
	AttachRequired *bool `json:"attachRequired,omitempty" protobuf:"varint,1,opt,name=attachRequired"`

	// podInfoOnMount 设为 true 表示此 CSI 卷驱动在挂载操作期间需要额外的
	// pod 信息（如 podName、podUID 等）。
	// 设为 false 时，挂载时不会传递 pod 信息。
	// 默认为 false。
	//
	// CSI 驱动在部署时指定 podInfoOnMount。若为 true，Kubelet 会在 CSI
	// NodePublishVolume() 调用中将 pod 信息作为 VolumeContext 传递。
	// CSI 驱动负责解析和校验作为 VolumeContext 传入的信息。
	//
	// 若 podInfoOnMount 设为 true，将传递以下 VolumeContext。
	// 此列表可能增长，但会使用统一前缀。
	// "csi.storage.k8s.io/pod.name": pod.Name
	// "csi.storage.k8s.io/pod.namespace": pod.Namespace
	// "csi.storage.k8s.io/pod.uid": string(pod.UID)
	// "csi.storage.k8s.io/ephemeral": 卷是由 CSIVolumeSource 定义的临时内联卷时为 "true"，否则为 "false"
	//
	// "csi.storage.k8s.io/ephemeral" 是 Kubernetes 1.16 的新特性，只有同时支持
	// "Persistent" 和 "Ephemeral" VolumeLifecycleMode 的驱动才需要它。
	// 其他驱动可以禁用 pod 信息和/或忽略此字段。
	// 由于 Kubernetes 1.15 不支持此字段，驱动部署在这种集群上时只能支持一种模式，
	// 具体模式由部署方式决定，例如通过驱动的命令行参数。
	//
	// 此字段在 Kubernetes < 1.29 中不可变，现在可变。
	//
	// +optional
	PodInfoOnMount *bool `json:"podInfoOnMount,omitempty" protobuf:"bytes,2,opt,name=podInfoOnMount"`

	// volumeLifecycleModes 定义此 CSI 卷驱动支持哪些类型的卷。
	// 列表为空时默认为 "Persistent"，即 CSI 规范定义并由 Kubernetes 通过常规
	// PV/PVC 机制实现的用法。
	//
	// 另一种模式是 "Ephemeral"。此模式下，卷通过 CSIVolumeSource 在 pod spec 中
	// 内联定义，其生命周期与 pod 绑定。驱动必须了解这一点，因为这类卷只会收到
	// NodePublishVolume 调用。
	//
	// 有关实现此模式的更多信息，参见
	// https://kubernetes-csi.github.io/docs/ephemeral-local-volumes.html
	// 一个驱动可以支持一种或多种模式，将来可能新增更多模式。
	//
	// 此字段处于 beta 阶段。
	// 此字段不可变。
	//
	// +optional
	// +listType=set
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	VolumeLifecycleModes []VolumeLifecycleMode `json:"volumeLifecycleModes,omitempty" protobuf:"bytes,3,opt,name=volumeLifecycleModes"`

	// storageCapacity 设为 true 表示 CSI 卷驱动希望 pod 调度考虑驱动部署通过
	// 创建带容量信息的 CSIStorageCapacity 对象所报告的存储容量。
	//
	// 部署驱动时可立即启用此检查。此时，使用延迟绑定创建新卷会暂停，
	// 直到驱动部署发布了某个合适的 CSIStorageCapacity 对象。
	//
	// 或者，驱动部署时可以未设置此字段或设为 false，待存储容量信息发布后再打开。
	//
	// 此字段在 Kubernetes <= 1.22 中不可变，现在可变。
	//
	// +optional
	// +featureGate=CSIStorageCapacity
	StorageCapacity *bool `json:"storageCapacity,omitempty" protobuf:"bytes,4,opt,name=storageCapacity"`

	// fsGroupPolicy 定义底层卷是否支持在挂载前更改卷的所有权和权限。
	// 更多细节参见具体的 FSGroupPolicy 取值说明。
	//
	// 此字段在 Kubernetes < 1.29 中不可变，现在可变。
	//
	// 默认为 ReadWriteOnceWithFSType，它会检查每个卷以确定 Kubernetes
	// 是否应修改卷的所有权和权限。使用默认策略时，只有在定义了 fstype
	// 且卷的访问模式包含 ReadWriteOnce 时，才会应用定义的 fsGroup。
	//
	// +optional
	FSGroupPolicy *FSGroupPolicy `json:"fsGroupPolicy,omitempty" protobuf:"bytes,5,opt,name=fsGroupPolicy"`

	// tokenRequests 表示 CSI 驱动需要其为挂载卷的 pod 的 service account token
	// 来完成必要的认证。Kubelet 会在 CSI NodePublishVolume 调用中通过 VolumeContext
	// 传递这些 token。CSI 驱动应解析并校验如下 VolumeContext：
	// "csi.storage.k8s.io/serviceAccount.tokens": {
	//   "<audience>": {
	//     "token": <token>,
	//     "expirationTimestamp": <expiration timestamp in RFC3339>,
	//   },
	//   ...
	// }
	//
	// 注意：每个 TokenRequest 的 audience 应不同，且最多只有一个 token 为空字符串。
	// 要在 token 过期后获取新 token，可以使用 RequiresRepublish 定期触发 NodePublishVolume。
	//
	// +optional
	// +listType=atomic
	TokenRequests []TokenRequest `json:"tokenRequests,omitempty" protobuf:"bytes,6,opt,name=tokenRequests"`

	// requiresRepublish 表示 CSI 驱动希望定期调用 `NodePublishVolume`，
	// 以反映已挂载卷中可能发生的任何变化。此字段默认为 false。
	//
	// 注意：初次 NodePublishVolume 调用成功后，后续对 NodePublishVolume 的调用
	// 应只更新卷的内容，运行中的容器不会看到新的挂载点。
	//
	// +optional
	RequiresRepublish *bool `json:"requiresRepublish,omitempty" protobuf:"varint,7,opt,name=requiresRepublish"`

	// seLinuxMount 指定 CSI 驱动是否支持 "-o context" 挂载选项。
	//
	// 为 "true" 时，CSI 驱动必须确保其提供的所有卷都能用不同的 `-o context`
	// 选项分别挂载。这对于将卷作为块设备上的文件系统或独立共享卷提供的
	// 存储后端是典型情况。
	// 当挂载被显式设置了 SELinux context 的 Pod 使用的 ReadWriteOncePod 卷时，
	// Kubernetes 会使用 "-o context=xyz" 挂载选项调用 NodeStage / NodePublish。
	// 将来这可能扩展到其他卷 AccessMode。无论如何，Kubernetes 会确保卷只使用
	// 单个 SELinux context 挂载。
	//
	// 为 "false" 时，Kubernetes 不会向驱动传递任何特殊的 SELinux 挂载选项。
	// 这对于表示更大共享文件系统子目录的卷是典型情况。
	//
	// 默认为 "false"。
	//
	// +featureGate=SELinuxMountReadWriteOncePod
	// +optional
	SELinuxMount *bool `json:"seLinuxMount,omitempty" protobuf:"varint,8,opt,name=seLinuxMount"`

	// nodeAllocatableUpdatePeriodSeconds 指定此驱动的 CSINode 可分配容量
	// 周期性更新的间隔。设置后，会同时启用周期性更新和由容量相关失败触发的更新。
	// 未设置时不会进行任何更新（既不周期性更新，也不在检测到容量相关失败时更新），
	// allocatable.count 保持静态。此字段允许的最小值为 10 秒。
	//
	// 此特性需要启用 MutableCSINodeAllocatableCount 特性门控。
	//
	// 此字段可变。
	//
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	NodeAllocatableUpdatePeriodSeconds *int64 `json:"nodeAllocatableUpdatePeriodSeconds,omitempty" protobuf:"varint,9,opt,name=nodeAllocatableUpdatePeriodSeconds"`

	// serviceAccountTokenInSecrets 是 CSI 驱动的可选配置，表示 service account token
	// 应通过 NodePublishVolumeRequest 的 Secrets 字段而非 VolumeContext 字段传递。
	// CSI 规范为 token 等敏感信息提供了专门的 Secrets 字段，这是处理凭据的合适机制。
	// 这解决了敏感 token 作为卷上下文一部分被记录到日志中的安全问题。
	//
	// 为 "true" 时，kubelet 仅在 Secrets 字段中以键
	// "csi.storage.k8s.io/serviceAccount.tokens" 传递 token。CSI 驱动必须更新为
	// 从 Secrets 字段而非 VolumeContext 读取 token。
	//
	// 为 "false" 或未设置时，kubelet 在 VolumeContext 中以键
	// "csi.storage.k8s.io/serviceAccount.tokens" 传递 token（现有行为），
	// 以保持与现有 CSI 驱动的向后兼容。
	//
	// 此字段只能在配置了 TokenRequests 时设置。API server 会拒绝未配置
	// TokenRequests 却设置此字段的 CSIDriver 规格。
	//
	// 未设置时的默认行为是在 VolumeContext 字段中传递 token。
	//
	// +featureGate=CSIServiceAccountTokenSecrets
	// +optional
	ServiceAccountTokenInSecrets *bool `json:"serviceAccountTokenInSecrets,omitempty" protobuf:"varint,10,opt,name=serviceAccountTokenInSecrets"`

	// preventPodSchedulingIfMissing 表示当节点上缺少 CSI 驱动时，CSI 驱动希望阻止 pod 调度。
	//
	// 启用此选项将阻止调度器（或任何其他嵌入默认调度器的组件，如 cluster-autoscaler）
	// 将 pod 调度到未安装 CSI 驱动的节点。
	//
	// 对于嵌入调度器并使用调度器插件运行 pod 放置模拟的组件（如 cluster-autoscaler），
	// 它们必须通过 CSINode 对象感知 CSI 驱动注册信息。它们必须在调度模拟中
	// 除 Node 对象外还创建模拟的 CSINode 对象，否则如果对 CSIDriver 对象
	// 全局启用了 PreventPodSchedulingIfMissing，任何新建节点都可能因缺少
	// CSI 驱动信息而被调度器拒绝。
	//
	// 这是 beta 特性，需要启用 VolumeLimitScaling 特性门控。默认为 "false"。
	// +featureGate=VolumeLimitScaling
	// +optional
	PreventPodSchedulingIfMissing *bool `json:"preventPodSchedulingIfMissing,omitempty" protobuf:"varint,11,opt,name=preventPodSchedulingIfMissing"`
}

// FSGroupPolicy 指定 CSI 驱动是否支持修改待挂载卷的所有权和权限。
// 将来可能新增更多模式。
type FSGroupPolicy string

// FSGroupPolicy 的取值。
const (
	// ReadWriteOnceWithFSTypeFSGroupPolicy 表示会检查每个卷，以确定是否应修改
	// 卷的所有权和权限。若定义了 fstype 且卷的访问模式包含 ReadWriteOnce
	// 或 ReadWriteOncePod，则会应用定义的 fsGroup。
	// 如果预期 fsGroup 可能需要根据 pod 的 SecurityPolicy 修改，应使用此模式。
	// 若未定义其他 FSGroupPolicy，这是默认行为。
	ReadWriteOnceWithFSTypeFSGroupPolicy FSGroupPolicy = "ReadWriteOnceWithFSType"

	// FileFSGroupPolicy 表示 CSI 驱动支持通过 fsGroup 更改卷的所有权和权限，
	// Kubernetes 会将卷中每个文件的权限和所有权更改为与 pod 的 SecurityPolicy 中
	// 用户请求的 fsGroup 匹配，而不管 fstype 或访问模式如何。
	// 如果希望 Kubernetes 修改卷的权限和所有权，使用此模式。
	FileFSGroupPolicy FSGroupPolicy = "File"

	// NoneFSGroupPolicy 表示挂载卷时不执行任何所有权或权限修改，因为 CSIDriver
	// 不支持这些操作。
	// 当 CSIDriver 不支持 fsGroup 修改时应选择此模式，例如由于 NFS 卷上的
	// root-squash 设置导致 Kubernetes 无法更改卷的所有权和权限。
	NoneFSGroupPolicy FSGroupPolicy = "None"
)

// VolumeLifecycleMode 是 CSI 驱动提供的卷的可能使用模式的枚举。
// 将来可能新增更多模式。
type VolumeLifecycleMode string

// TokenRequest 包含 service account token 的参数。
type TokenRequest struct {
	// audience 是 "TokenRequestSpec" 中 token 的目标受众。
	// 默认为 kube apiserver 的 audience。
	Audience string `json:"audience" protobuf:"bytes,1,opt,name=audience"`

	// expirationSeconds 是 "TokenRequestSpec" 中 token 的有效期。
	// 默认值与 "TokenRequestSpec" 中的 "ExpirationSeconds" 相同。
	//
	// +optional
	ExpirationSeconds *int64 `json:"expirationSeconds,omitempty" protobuf:"varint,2,opt,name=expirationSeconds"`
}

// VolumeLifecycleMode 的取值。
const (
	// VolumeLifecyclePersistent 明确确认驱动实现了完整的 CSI 规范。
	// 当 CSIDriverSpec.VolumeLifecycleModes 未设置时，这是默认模式。
	// 这类卷在 Kubernetes 中通过 persistent volume claim 机制管理，
	// 其生命周期与使用它们的 pod 无关。
	VolumeLifecyclePersistent VolumeLifecycleMode = "Persistent"

	// VolumeLifecycleEphemeral 表示驱动可用于临时内联卷。
	// 这类卷在 pod spec 中通过 CSIVolumeSource 指定，就 Kubernetes 而言，
	// 其生命周期与 pod 的生命周期绑定。例如，这类卷可能包含专门为某个 pod
	// 创建的数据，如 secret。
	// 但卷实际如何创建和管理完全由驱动决定。如果不同 pod 的 CSIVolumeSource
	// 相同，驱动还可以使用引用计数在这些 pod 之间共享同一卷实例。
	VolumeLifecycleEphemeral VolumeLifecycleMode = "Ephemeral"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.17
// +k8s:supportsSubresource="/status"

// CSINode 保存节点上安装的所有 CSI 驱动的信息。
// CSI 驱动无需直接创建 CSINode 对象。只要它们使用 node-driver-registrar sidecar 容器，
// kubelet 就会作为 kubelet 插件注册的一部分，自动为该 CSI 驱动填充 CSINode 对象。
// CSINode 与节点同名。若对象缺失，要么表示节点上没有可用的 CSI 驱动，
// 要么表示 Kubelet 版本过低，不会创建此对象。
// CSINode 有一个指向对应 node 对象的 OwnerReference。
type CSINode struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// metadata.name 必须是 Kubernetes 节点名称。
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 是 CSINode 的规格。
	Spec CSINodeSpec `json:"spec" protobuf:"bytes,2,opt,name=spec"`

	// status 包含节点存储的健康和状态信息。
	// +optional
	Status CSINodeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// CSINodeSpec 保存节点上安装的所有 CSI 驱动的规格信息。
type CSINodeSpec struct {
	// drivers 是节点上存在的所有 CSI 驱动的信息列表。
	// 若列表中所有驱动都被卸载，此列表可以为空。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Drivers []CSINodeDriver `json:"drivers" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,1,rep,name=drivers"`
}

// CSINodeDriver 保存节点上安装的某个 CSI 驱动的规格信息。
type CSINodeDriver struct {
	// name 表示此对象所指的 CSI 驱动名称。
	// 必须与对该驱动调用 CSI GetPluginName() 返回的名称相同。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// nodeID 是从驱动角度看到的节点 ID。
	// 此字段使 Kubernetes 能够与节点命名法不同的存储系统通信。例如，Kubernetes
	// 可能将某节点称为 "node1"，而存储系统可能将同一节点称为 "nodeA"。
	// 当 Kubernetes 向存储系统发出将卷挂载到特定节点的命令时，可以使用此字段
	// 以存储系统能理解的 ID 来引用节点名，例如用 "nodeA" 代替 "node1"。此字段必填。
	NodeID string `json:"nodeID" protobuf:"bytes,2,opt,name=nodeID"`

	// topologyKeys 是驱动支持的键列表。
	// 驱动在集群上初始化时，会提供一组它理解的拓扑键
	// （如 "company.com/zone"、"company.com/region"）。
	// 驱动在节点上初始化时，会提供相同的拓扑键及其值。Kubelet 会将这些拓扑键
	// 作为标签暴露在自己的 node 对象上。
	// 当 Kubernetes 进行拓扑感知创建时，可以使用此列表确定应从 node 对象
	// 获取哪些标签并传回驱动。
	// 不同节点可以使用不同的拓扑键。若驱动不支持拓扑，此列表可为空。
	// +optional
	// +listType=atomic
	TopologyKeys []string `json:"topologyKeys" protobuf:"bytes,3,rep,name=topologyKeys"`

	// allocatable 表示节点上可供调度使用的卷资源。
	// 此字段处于 beta 阶段。
	// +optional
	Allocatable *VolumeNodeResources `json:"allocatable,omitempty" protobuf:"bytes,4,opt,name=allocatable"`
}

// VolumeNodeResources 是用于卷调度的一组资源限制。
type VolumeNodeResources struct {
	// count 表示 CSI 驱动管理的、可在节点上使用的唯一卷的最大数量。
	// 同时在节点上挂载和附着的卷只算使用一次，而不是两次。
	// 同一节点上多个 pod 共享的唯一卷也适用相同规则。
	// 若未指定此字段，则该节点支持的卷数量无上限。
	// +optional
	Count *int32 `json:"count,omitempty" protobuf:"varint,1,opt,name=count"`
}

// StorageHealthStatusType 描述存储后端的健康状态类别。
// +enum
type StorageHealthStatusType string

// StorageHealthStatusType 的取值。
const (
	// StorageUnreachable 表示存储后端不可达。
	StorageUnreachable StorageHealthStatusType = "StorageUnreachable"
	// StorageDegraded 表示存储后端功能降级。
	StorageDegraded StorageHealthStatusType = "StorageDegraded"
)

// StorageHealthCondition 表示 CSI 驱动报告的其存储后端在节点上的不良健康状况。
type StorageHealthCondition struct {
	// status 是健康状态类别，取值为 "StorageUnreachable" 或 "StorageDegraded"。
	// +required
	Status StorageHealthStatusType `json:"status" protobuf:"bytes,1,opt,name=status,casttype=StorageHealthStatusType"`
	// reason 是简短的 CamelCase 格式、可由机器解析的原因。
	// reason 最大允许长度为 256 个字符。
	// +required
	Reason string `json:"reason" protobuf:"bytes,2,opt,name=reason"`
	// message 是人类可读的描述。
	// +optional
	// message 最大允许长度为 1024 个字符。
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// accessMode 是受影响的访问模式。nil 表示所有访问模式都受影响。
	// +optional
	AccessMode *v1.PersistentVolumeAccessMode `json:"accessMode,omitempty" protobuf:"bytes,4,opt,name=accessMode,casttype=k8s.io/api/core/v1.PersistentVolumeAccessMode"`
	// volumeMode 是受影响的卷模式。nil 表示两种模式都受影响。
	// +optional
	VolumeMode *v1.PersistentVolumeMode `json:"volumeMode,omitempty" protobuf:"bytes,5,opt,name=volumeMode,casttype=k8s.io/api/core/v1.PersistentVolumeMode"`
	// lastTransitionTime 是本条件首次以当前状态出现的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,6,opt,name=lastTransitionTime"`
}

// StorageHealth 包含 CSI 驱动在节点上报告的存储后端健康状况。
type StorageHealth struct {
	// name 是 CSI 驱动名称，与 CSINodeDriver.name 匹配。
	// +required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// healthConditions 是 CSI 驱动报告的存储后端不良状况。
	// 最多可报告 16 条条件。
	// +optional
	// +listType=atomic
	HealthConditions []StorageHealthCondition `json:"healthConditions,omitempty" protobuf:"bytes,2,rep,name=healthConditions"`
}

// CSINodeStatus 包含节点上存储的健康和状态信息。
type CSINodeStatus struct {
	// storageHealth 包含节点上注册的 CSI 驱动的后端健康报告。
	// +optional
	// +listType=map
	// +listMapKey=name
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +featureGate=CSIVolumeHealth
	StorageHealth []StorageHealth `json:"storageHealth,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,1,rep,name=storageHealth"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.17

// CSINodeList 是 CSINode 对象的集合。
type CSINodeList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSINode 的列表。
	Items []CSINode `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.24

// CSIStorageCapacity 保存一次 CSI GetCapacity 调用的结果。
// 对于给定的 StorageClass，它描述某个特定拓扑分段中的可用容量，
// 可用于决定在何处创建新的 PersistentVolume。
//
// 例如它可以表达这样的信息：
// - StorageClass "standard" 在 "topology.kubernetes.io/zone=us-east1" 中有 "1234 GiB" 可用
// - StorageClass "localssd" 在 "kubernetes.io/hostname=knode-abc123" 中有 "10 GiB" 可用
//
// 以下三种情况都表示某个组合没有可用容量：
// - 不存在具有合适拓扑和存储类名的对象
// - 存在这样的对象，但未设置容量
// - 存在这样的对象，但容量为零
//
// 这些对象的生产者可以决定采用哪种方式。
//
// 当 CSI 驱动通过 CSIDriverSpec.StorageCapacity 选择启用容量感知调度时，
// kube-scheduler 会使用这些对象。调度器将 MaximumVolumeSize 与待处理卷的
// 请求大小比较，以过滤掉不合适的节点。若 MaximumVolumeSize 未设置，
// 则回退为与精度较低的 Capacity 比较。若 Capacity 也未设置，
// 调度器认为容量不足，转而尝试其他节点。
type CSIStorageCapacity struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// 名称没有特殊含义，但必须是 DNS 子域（允许点号，253 个字符）。
	// 为确保与集群中其他 CSI 驱动不冲突，建议使用 csisc-<uuid>、
	// 生成的名称，或以唯一 CSI 驱动名称结尾的反向域名。
	//
	// 对象为命名空间级。
	//
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// nodeTopology 定义哪些节点可以访问为其报告容量的存储。
	// 未设置时，集群中任何节点都不能访问该存储。为空时，所有节点都可访问。
	// 此字段不可变。
	//
	// +optional
	NodeTopology *metav1.LabelSelector `json:"nodeTopology,omitempty" protobuf:"bytes,2,opt,name=nodeTopology"`

	// storageClassName 表示所报告容量适用的 StorageClass 名称。
	// 它必须满足与 StorageClass 对象名称相同的要求（非空、DNS 子域）。
	// 若该对象已不存在，则 CSIStorageCapacity 对象已过时，应由其创建者删除。
	// 此字段不可变。
	StorageClassName string `json:"storageClassName" protobuf:"bytes,3,name=storageClassName"`

	// capacity 是 CSI 驱动在其 GetCapacityResponse 中报告的值，
	// 该响应对应于拓扑和参数与前述字段匹配的 GetCapacityRequest。
	//
	// 当前（CSI 规范 1.2）定义其语义为：
	// 可用于创建卷的存储可用容量（以字节为单位）。若未设置，则该信息当前不可用。
	//
	// +optional
	Capacity *resource.Quantity `json:"capacity,omitempty" protobuf:"bytes,4,opt,name=capacity"`

	// maximumVolumeSize 是 CSI 驱动在其 GetCapacityResponse 中报告的值，
	// 该响应对应于拓扑和参数与前述字段匹配的 GetCapacityRequest。
	//
	// 自 CSI 规范 1.4.0 起，它定义为可在
	// CreateVolumeRequest.capacity_range.required_bytes 字段中使用的最大大小，
	// 用于创建与 GetCapacityRequest 中参数相同的卷。Kubernetes API 中
	// 对应的值是卷声明中的 ResourceRequirements.Requests。
	//
	// +optional
	MaximumVolumeSize *resource.Quantity `json:"maximumVolumeSize,omitempty" protobuf:"bytes,5,opt,name=maximumVolumeSize"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.24

// CSIStorageCapacityList 是 CSIStorageCapacity 对象的集合。
type CSIStorageCapacityList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 CSIStorageCapacity 对象的列表。
	Items []CSIStorageCapacity `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClass 表示 CSI 驱动定义的可变卷属性规格。
// 该类可在 PersistentVolumeClaim 动态创建时指定，并在创建后通过
// PersistentVolumeClaim 的 spec 修改。
type VolumeAttributesClass struct {
	metav1.TypeMeta `json:""`

	// metadata 是标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// driverName 是 CSI 驱动的名称。
	// 此字段不可变。
	DriverName string `json:"driverName" protobuf:"bytes,2,opt,name=driverName"`

	// parameters 保存 CSI 驱动定义的卷属性。这些值对 Kubernetes 不透明，
	// 会直接传递给 CSI 驱动。底层存储提供程序支持修改现有卷上的这些属性，
	// 但 parameters 字段本身不可变。要触发卷更新，应使用新参数创建一个新的
	// VolumeAttributesClass，并更新 PersistentVolumeClaim 以引用新的类。
	//
	// 此字段必填，且必须至少包含一对键/值。键不能为空，参数最多 512 个，
	// 累计最大大小为 256K。若 CSI 驱动拒绝无效参数，目标 PersistentVolumeClaim
	// 会在 modifyVolumeStatus 字段中被设为 "Infeasible" 状态。
	Parameters map[string]string `json:"parameters,omitempty" protobuf:"bytes,3,rep,name=parameters"`
}

// +k8s:prerelease-lifecycle-gen:introduced=1.34
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClassList 是 VolumeAttributesClass 对象的集合。
type VolumeAttributesClassList struct {
	metav1.TypeMeta `json:""`

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// items 是 VolumeAttributesClass 对象的列表。
	Items []VolumeAttributesClass `json:"items" protobuf:"bytes,2,rep,name=items"`
}
