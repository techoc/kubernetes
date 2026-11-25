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

package storage

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageClass 描述集群中提供的某个具名存储“类”。
// 不同的类可能对应不同的服务质量级别、备份策略，
// 或由集群管理员决定的任意策略。Kubernetes 本身对类代表什么
// 不作立场设定。这一概念在其他存储系统中有时被称为“profile（配置文件）”。
// StorageClass 对象的名称具有重要意义，用户通过它来请求特定的类。
type StorageClass struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// provisioner 是预期负责处理该 StorageClass 的驱动。
	// 这是一个可带前缀的名称，类似 label 键。
	// 例如："kubernetes.io/gce-pd" 或 "kubernetes.io/aws-ebs"。
	// 该值不得为空。
	Provisioner string

	// parameters 保存 provisioner 的参数。
	// 这些值对系统而言是不透明的，会直接传给
	// provisioner。对键唯一的校验是键不得
	// 为空。参数数量上限为
	// 512，累计大小上限为 256K。
	// +optional
	Parameters map[string]string

	// reclaimPolicy 是该存储类动态预置的 PersistentVolume 创建时所采用的
	// 回收策略。
	// +optional
	ReclaimPolicy *api.PersistentVolumeReclaimPolicy

	// mountOptions 是该存储类动态预置的 PersistentVolume 创建时所采用的
	// 挂载选项。
	// +optional
	MountOptions []string

	// AllowVolumeExpansion 表示该存储类是否允许卷扩容。
	// 若该字段为 nil 或未设置，则等同于禁用
	// 由该 storageclass 创建的所有 PV 的扩容功能。
	// +optional
	AllowVolumeExpansion *bool

	// VolumeBindingMode 表示 PersistentVolumeClaim 应以何种方式被预置
	// 和绑定。未设置时使用 VolumeBindingImmediate。
	// 仅在启用了 VolumeScheduling 特性的服务器上生效。
	// +optional
	VolumeBindingMode *VolumeBindingMode

	// 限制可动态预置卷的节点拓扑。
	// 每个卷插件定义自己支持的拓扑规格。
	// TopologySelectorTerm 列表为空表示不做拓扑限制。
	// 仅在启用了 VolumeScheduling 特性的服务器上生效。
	// +optional
	AllowedTopologies []api.TopologySelectorTerm
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// StorageClassList 是存储类的集合。
type StorageClassList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// Items 是 StorageClass 的列表。
	Items []StorageClass
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttachment 记录将指定 volume 挂载（attach）或卸载（detach）到
// 指定 node 的意图。
//
// VolumeAttachment 对象是非命名空间对象。
type VolumeAttachment struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// spec 表示期望的卷挂载/卸载行为的规格。
	// 由 Kubernetes 系统填充。
	Spec VolumeAttachmentSpec

	// VolumeAttachment 请求的状态。
	// 由完成挂载或卸载操作的实体填充，
	// 即外部 attacher 执行的操作。
	// +optional
	Status VolumeAttachmentStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttachmentList 是 VolumeAttachment 对象的集合。
type VolumeAttachmentList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// Items 是 VolumeAttachment 的列表。
	Items []VolumeAttachment
}

// VolumeAttachment 请求的规格。
type VolumeAttachmentSpec struct {
	// Attacher 表示必须处理该请求的卷驱动名称。
	// 即 GetPluginName() 返回的名称。
	Attacher string

	// source 表示应附加的卷。
	Source VolumeAttachmentSource

	// 卷应当被 attach 到的节点。
	NodeName string
}

// VolumeAttachmentSource 表示一个应当被 attach 的卷。
// 目前，持久卷以及内联卷（仅在 CSI Migration 场景下）
// 都可以通过外部 attacher 进行 attach。
// 只能且必须设置一个成员。
type VolumeAttachmentSource struct {
	// 要 attach 的持久卷的名称。
	// +optional
	PersistentVolumeName *string

	// inlineVolumeSpec 包含挂载由 pod 内联 VolumeSource 定义的持久卷所需的全部信息。
	// 此字段仅为 CSIMigration 特性填充，包含从 pod 内联 VolumeSource
	// 转换到 PersistentVolumeSpec 的字段。此字段处于 beta 级别，
	// 仅在启用了 CSIMigration 特性的服务端上生效。
	// +optional
	InlineVolumeSpec *api.PersistentVolumeSpec
}

// VolumeAttachment 请求的状态。
type VolumeAttachmentStatus struct {
	// 表示 volume 已成功挂载。
	// 该字段只能由完成 attach 的实体设置，
	// 即外部 attacher 执行的操作。
	Attached bool

	// 挂载成功后，此字段会被填入挂载操作返回的、
	// 必须传递给后续 WaitForAttach 或 Mount 调用的信息。
	// 该字段只能由完成 attach 的实体设置，
	// 即外部 attacher 执行的操作。
	// +optional
	AttachmentMetadata map[string]string

	// 挂载操作期间遇到的最后一个错误（如有）。
	// 该字段只能由完成 attach 的实体设置，
	// 即外部 attacher 执行的操作。
	// +optional
	AttachError *VolumeError

	// 卸载操作期间遇到的最后一个错误（如有）。
	// 该字段只能由完成 detach 的实体设置，
	// 即外部 attacher 执行的操作。
	// +optional
	DetachError *VolumeError
}

// VolumeError 记录卷操作期间遇到的错误。
type VolumeError struct {
	// 遇到该错误的时间。
	// +optional
	Time metav1.Time

	// message 表示 Attach 或 Detach 操作期间遇到的错误。此字符串可能被记录到日志，
	// 因此不应包含敏感信息。
	// +optional
	Message string

	// errorCode 是表示 Attach 或 Detach 操作期间遇到错误的数字 gRPC 码。
	// 这是可选的 beta 字段，需要启用 MutableCSINodeAllocatableCount 特性门控才可设置。
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	ErrorCode *int32
}

// VolumeBindingMode 表示 PersistentVolumeClaim 应如何被绑定。
type VolumeBindingMode string

const (
	// VolumeBindingImmediate 表示 PersistentVolumeClaims 应当被立即
	// 预置并绑定。
	VolumeBindingImmediate VolumeBindingMode = "Immediate"

	// VolumeBindingWaitForFirstConsumer 表示 PersistentVolumeClaims
	// 在第一个引用其的 Pod 创建之前不应被预置和绑定。
	// 卷的预置与绑定将在 Pod 调度期间进行。
	VolumeBindingWaitForFirstConsumer VolumeBindingMode = "WaitForFirstConsumer"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSIDriver 捕获部署在集群上的容器存储接口（CSI）卷驱动的信息。
// CSI 驱动无需直接创建 CSIDriver 对象，可以使用
// cluster-driver-registrar sidecar 容器。与 CSI 驱动一起部署时，它会自动
// 创建表示该驱动的 CSIDriver 对象。
// Kubernetes 的 attach/detach 控制器使用该对象判断是否需要 attach。
// Kubelet 使用该对象判断挂载时是否需要传递 Pod 信息。
// CSIDriver 对象是非命名空间对象。
type CSIDriver struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// metadata.Name 表示此对象所引用的 CSI 驱动名称；
	// 它必须与该驱动 CSI GetPluginName() 调用返回的名称一致。
	// 驱动名必须为 63 个字符或更少，以字母数字字符（[a-z0-9A-Z]）开头和结尾，
	// 中间可包含短横线（-）、点（.）和字母数字字符。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta

	// CSI 驱动的规格。
	Spec CSIDriverSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSIDriverList 是 CSIDriver 对象的集合。
type CSIDriverList struct {
	metav1.TypeMeta

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 CSIDriver 列表。
	Items []CSIDriver
}

// CSIDriverSpec 是 CSIDriver 的规格。
type CSIDriverSpec struct {
	// attachRequired 表示该 CSI volume 驱动需要挂载（attach）操作
	// （因为它实现了 CSI ControllerPublishVolume() 方法），并且 Kubernetes
	// 的挂载/卸载控制器应调用挂载 volume 的接口，由该接口检查 volumeattachment
	// 状态，并等待 volume 挂载完成后再继续执行挂载（mount）。
	// CSI external-attacher 与 CSI volume 驱动协调，并在挂载操作完成时
	// 更新 volumeattachment 状态。
	// 若该值显式设为 false，则跳过挂载操作。
	// 否则会调用挂载操作。
	//
	// 该字段不可变。
	//
	// +optional
	AttachRequired *bool

	// 若设为 true，podInfoOnMount 表示该 CSI 卷驱动在挂载操作期间
	// 需要额外的 Pod 信息（如 podName、podUID 等）。
	// 若设为 false，挂载时不传递 Pod 信息。
	// 默认为 false。
	// CSI 驱动在部署时指定 podInfoOnMount。
	// 若为 true，Kubelet 将在 CSI NodePublishVolume() 调用中
	// 把 Pod 信息作为 VolumeContext 传递。
	// CSI 驱动负责解析并校验以 VolumeContext 形式传入的信息。
	// 若 podInfoOnMount 设为 true，将传递以下 VolumeContext。
	// 此列表可能会增加，但会沿用同样的前缀。
	// "csi.storage.k8s.io/pod.name": pod.Name
	// "csi.storage.k8s.io/pod.namespace": pod.Namespace
	// "csi.storage.k8s.io/pod.uid": string(pod.UID)
	// "csi.storage.k8s.io/ephemeral": 若该卷是由 CSIVolumeSource 定义的内联
	//                                 临时卷则为 "true"，否则为 "false"
	//
	// "csi.storage.k8s.io/ephemeral" 是 Kubernetes 1.16 的新特性，仅
	// 同时支持 "Persistent" 和 "Ephemeral" VolumeLifecycleMode 的驱动才需要。
	// 其他驱动可以保持 Pod 信息禁用，和/或忽略此字段。
	// 由于 Kubernetes 1.15 不支持此字段，驱动在此类集群上部署时只能支持
	// 一种模式，且由部署方式决定所支持的模式，例如通过驱动的命令行参数。
	//
	// 该字段在 Kubernetes < 1.29 中不可变，现已可变。
	//
	// +optional
	PodInfoOnMount *bool

	// VolumeLifecycleModes 定义该 CSI volume 驱动支持的 volume 类型。
	// 若列表为空，默认为 "Persistent"，即 CSI 规范定义的、
	// Kubernetes 通过常规 PV/PVC 机制实现的用法。
	// 另一种模式是 "Ephemeral"。在这种模式下，volume 以 CSIVolumeSource
	// 的形式内联定义在 Pod spec 中，其生命周期与该 Pod 的生命周期绑定。
	// 驱动必须意识到这一点，因为对这样的 volume，
	// 驱动只会收到一次 NodePublishVolume 调用。
	// 有关实现此模式的更多信息，参见
	// https://kubernetes-csi.github.io/docs/ephemeral-local-volumes.html
	// 一个驱动可以支持一种或多种这样的模式，
	// 未来可能添加更多模式。
	//
	// 该字段不可变。
	//
	// +optional
	VolumeLifecycleModes []VolumeLifecycleMode

	// storageCapacity 设为 true 时表示 CSI 卷驱动希望 Pod 调度考虑其通过创建带有容量信息的
	// CSIStorageCapacity 对象所报告的存储容量。
	// 部署驱动时可立即启用此检查，此时采用延迟绑定的新卷供给会暂停，直到驱动部署发布了
	// 合适的 CSIStorageCapacity 对象。也可先不设置该字段或设为 false 部署驱动，待容量信息
	// 发布后再切换。
	// 此字段在 Kubernetes <= 1.22 中不可变，现在可变。
	// +optional
	StorageCapacity *bool

	// 定义底层卷是否支持在挂载前更改卷的所有权和权限。
	// 更多细节参见具体的 FSGroupPolicy 取值。
	//
	// 该字段在 Kubernetes < 1.29 中不可变，现已可变。
	//
	// 默认为 ReadWriteOnceWithFSType，该策略会检查每个卷，
	// 以判断 Kubernetes 是否应修改卷的所有权和权限。
	// 在默认策略下，所定义的 fsGroup 仅在定义了 fstype
	// 且卷的访问模式包含 ReadWriteOnce 时才会应用。
	// +optional
	FSGroupPolicy *FSGroupPolicy

	// TokenRequests 表示 CSI 驱动需要为其挂载卷的 Pod 的服务账号
	// 令牌来完成必要的身份认证。Kubelet 会在 CSI NodePublishVolume 调用中
	// 通过 VolumeContext 传递这些令牌。
	// CSI 驱动应当解析并校验以下 VolumeContext：
	// "csi.storage.k8s.io/serviceAccount.tokens": {
	//   "<audience>": {
	//     "token": <token>,
	//     "expirationTimestamp": <expiration timestamp in RFC3339>,
	//   },
	//   ...
	// }
	//
	// 注意：每个 TokenRequest 中的 Audience 应当不同，且
	// 至多一个令牌的 audience 为空字符串。若要在令牌过期后获取新令牌，
	// 可使用 RequiresRepublish 定期触发 NodePublishVolume。
	//
	// +optional
	// +listType=atomic
	TokenRequests []TokenRequest

	// RequiresRepublish 表示 CSI 驱动希望 `NodePublishVolume`
	// 被周期性调用，以反映已挂载卷上可能发生的任何变化。
	// 该字段默认为 false。
	//
	// 注意：初次 NodePublishVolume 调用成功后，后续的
	// NodePublishVolume 调用应当只更新卷的内容。运行中的
	// 容器不会看到新的挂载点。
	//
	// +optional
	RequiresRepublish *bool

	// SELinuxMount 指定 CSI 驱动是否支持 "-o context" 挂载选项。
	//
	// 设为 "true" 时，CSI 驱动必须确保其提供的所有卷都能分别使用
	// 不同的 `-o context` 选项挂载。这典型适用于以块设备上的文件系统
	// 或独立共享卷形式提供卷的存储后端。
	// 当挂载在显式设置了 SELinux 上下文的 Pod 中使用的
	// ReadWriteOncePod 卷时，Kubernetes 会在 NodeStage / NodePublish
	// 调用中带上 "-o context=xyz" 挂载选项。未来可能扩展到其他
	// 卷 AccessModes。无论如何，Kubernetes 将确保该卷只以单一
	// SELinux 上下文挂载。
	//
	// 设为 "false" 时，Kubernetes 不会向驱动传递任何特殊的 SELinux
	// 挂载选项。这典型适用于表示更大共享文件系统中子目录的卷。
	//
	// 默认为 "false"。
	//
	// +featureGate=SELinuxMountReadWriteOncePod
	// +optional
	SELinuxMount *bool

	// nodeAllocatableUpdatePeriodSeconds 指定此驱动的 CSINode 可分配容量
	// 周期性更新的间隔。设置后，会同时启用周期性更新和由容量相关失败触发的更新。
	// 未设置时不会进行任何更新（既不周期性更新，也不在检测到容量相关失败时更新），
	// allocatable.count 保持静态。此字段允许的最小值为 10 秒。
	// 此特性需要启用 MutableCSINodeAllocatableCount 特性门控。
	// 此字段可变。
	// +featureGate=MutableCSINodeAllocatableCount
	// +optional
	NodeAllocatableUpdatePeriodSeconds *int64

	// serviceAccountTokenInSecrets 是 CSI 驱动的一项选择性启用（opt-in），
	// 表示服务账号令牌应通过 NodePublishVolumeRequest 的 Secrets 字段
	// 而非 VolumeContext 字段传递。CSI 规范提供了专门的 Secrets 字段
	// 用于承载令牌等敏感信息，这是处理凭据的恰当机制。这解决了敏感令牌
	// 作为卷上下文一部分被记录到日志的安全问题，此类问题曾导致
	// CVE-2023-2878 和 CVE-2024-3744 等漏洞。
	//
	// 设为 "true" 时，kubelet 将只在 Secrets 字段中以键
	// "csi.storage.k8s.io/serviceAccount.tokens" 传递令牌。CSI 驱动必须
	// 更新为从 Secrets 字段（而非 VolumeContext）读取令牌。
	//
	// 设为 "false" 或未设置时，kubelet 将在 VolumeContext 中以键
	// "csi.storage.k8s.io/serviceAccount.tokens" 传递令牌（现有行为）。
	// 这保持了对现有 CSI 驱动的向后兼容性。
	//
	// 该字段只有在配置了 TokenRequests 时才可设置。若 CSIDriver spec
	// 在未配置 TokenRequests 的情况下设置了该字段，API server 将拒绝。
	//
	// 默认为 "false"。
	//
	// +featureGate=CSIServiceAccountTokenSecrets
	// +optional
	ServiceAccountTokenInSecrets *bool

	// PreventPodSchedulingIfMissing 表示 CSI 驱动希望在节点上缺少该 CSI 驱动时
	// 阻止 Pod 调度到该节点。
	//
	// 启用此选项后，调度器（或任何内嵌默认调度器的其他组件，
	// 例如 cluster-autoscaler）将不会把 Pod 调度到未安装该
	// CSI 驱动的节点。
	//
	// 对于内嵌调度器并使用调度器插件运行 Pod 放置模拟的组件
	// （如 cluster-autoscaler），它们必须通过 CSINode 对象感知
	// CSI 驱动注册信息。在调度模拟期间，它们必须除 Node 对象外
	// 还创建模拟的 CSINode 对象；否则，若在 CSIDriver 对象上全局
	// 启用 PreventPodSchedulingIfMissing，任何新建的节点都可能因为
	// 缺少来自该节点的 CSI 驱动信息而被调度器拒绝。
	//
	// 这是一个 beta 特性，需要启用 VolumeLimitScaling 特性门控。
	// 默认为 "false"。
	// +featureGate=VolumeLimitScaling
	// +optional
	PreventPodSchedulingIfMissing *bool
}

// FSGroupPolicy 指定 CSI 驱动是否支持修改待挂载卷的所有权和权限。
// 未来可能添加更多模式。
type FSGroupPolicy string

const (
	// ReadWriteOnceWithFSTypeFSGroupPolicy 表示会检查每个卷，以确定是否应修改
	// 卷的所有权和权限。若定义了 fstype 且卷的访问模式包含 ReadWriteOnce
	// 或 ReadWriteOncePod，则会应用定义的 fsGroup。
	// 如果预期 fsGroup 可能需要根据 pod 的 SecurityPolicy 修改，应使用此模式。
	// 若未定义其他 FSGroupPolicy，这是默认行为。
	ReadWriteOnceWithFSTypeFSGroupPolicy FSGroupPolicy = "ReadWriteOnceWithFSType"

	// FileFSGroupPolicy 表示 CSI 驱动支持通过 fsGroup 修改卷的所有权和权限。无论 fstype 或
	// 访问模式如何，Kubernetes 都会把卷中每个文件的权限和所有权改为与 Pod SecurityPolicy
	// 中用户请求的 fsGroup 一致。若希望由 Kubernetes 修改卷的权限和所有权，请使用此模式。
	FileFSGroupPolicy FSGroupPolicy = "File"

	// NoneFSGroupPolicy 表示挂载卷时不执行任何所有权或权限修改，因为 CSIDriver
	// 不支持这些操作。
	// 当 CSIDriver 不支持 fsGroup 修改时应选择此模式，例如由于 NFS 卷上的
	// root-squash 设置导致 Kubernetes 无法更改卷的所有权和权限。
	NoneFSGroupPolicy FSGroupPolicy = "None"
)

// VolumeLifecycleMode 指定 CSI 卷在 Kubernetes 中的使用方式。
// 未来可能添加更多模式。
type VolumeLifecycleMode string

// TokenRequest 包含 service account token 的参数。
type TokenRequest struct {
	// audience 是 "TokenRequestSpec" 中 token 的目标受众。
	// 默认为 kube apiserver 的 audience。
	Audience string

	// ExpirationSeconds 是 "TokenRequestSpec" 中令牌的有效时长。
	// 其默认值与 "TokenRequestSpec" 中的 "ExpirationSeconds" 相同。
	//
	// +optional
	ExpirationSeconds *int64
}

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

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSINode 保存节点上安装的所有 CSI 驱动的信息。
// CSI 驱动无需直接创建 CSINode 对象。只要使用
// node-driver-registrar sidecar 容器，kubelet 就会在插件注册过程中
// 自动为该 CSI 驱动填充 CSINode 对象。
// CSINode 与节点同名。若该对象缺失，说明节点上要么没有可用的
// CSI 驱动，要么 Kubelet 版本过低而不会创建该对象。
// CSINode 带有一个指向对应 node 对象的 OwnerReference。
// 启用 MutableCSINodeAllocatableCount 特性门控后，可以更新
// CSINodeDriver 中的 allocatable.count 字段。
type CSINode struct {
	metav1.TypeMeta

	// metadata.name 必须是 Kubernetes 节点名。
	metav1.ObjectMeta

	// spec 是 CSINode 的规格。
	Spec CSINodeSpec

	// status 包含节点存储的健康和状态信息。
	// +optional
	Status CSINodeStatus
}

// CSINodeSpec 保存节点上安装的所有 CSI 驱动的规格信息。
type CSINodeSpec struct {
	// drivers 是节点上所有已存在 CSI 驱动信息的列表。
	// 若列表中的驱动全部被卸载，该列表可能变为空。
	// +patchMergeKey=name
	// +patchStrategy=merge
	Drivers []CSINodeDriver
}

// CSINodeDriver 保存节点上安装的某个 CSI 驱动的规格信息。
type CSINodeDriver struct {
	// 这是该对象所指的 CSI 驱动的名称。
	// 必须与该驱动的 CSI GetPluginName() 调用返回的名称一致。
	Name string

	// nodeID 是从驱动视角看到的节点 ID。
	// 该字段使 Kubernetes 能够与使用不同节点命名体系的存储系统
	// 通信。例如，Kubernetes 可能称某个节点为 "node1"，而存储系统
	// 可能称同一节点为 "nodeA"。当 Kubernetes 向存储系统发出将卷
	// attach 到特定节点的命令时，可以使用该字段以存储系统能理解的
	// ID（如 "nodeA" 而非 "node1"）引用节点名。该字段为必填。
	NodeID string

	// topologyKeys 是该驱动支持的键列表。
	// 驱动在集群上初始化时，会提供一组它能理解的拓扑键
	// （如 "company.com/zone"、"company.com/region"）。
	// 驱动在节点上初始化时，会提供相同的拓扑键及其值。
	// Kubelet 会将这些拓扑键作为 label 暴露在其 node 对象上。
	// Kubernetes 进行拓扑感知预置时，可用此列表确定应从节点对象
	// 获取哪些 label 并传回给驱动。
	// 不同节点可以使用不同的拓扑键。
	// 若驱动不支持拓扑，该列表可以为空。
	// +optional
	TopologyKeys []string

	// allocatable 表示节点上可供调度使用的卷资源。
	// +optional
	Allocatable *VolumeNodeResources
}

// VolumeNodeResources 是用于卷调度的一组资源限制。
type VolumeNodeResources struct {
	// count 表示 CSI 驱动管理的、可在节点上使用的唯一卷的最大数量。同时附加并挂载到节点上的卷
	// 只算一次使用，而非两次；同一唯一卷在同一节点上被多个 Pod 共享时也遵循此规则。
	// 若此字段为 nil，则此节点支持的卷数量无上限。
	// +optional
	Count *int32
}

// StorageHealthStatusType 描述存储后端的健康状态类别。
type StorageHealthStatusType string

const (
	// StorageUnreachable 表示存储后端不可达。
	StorageUnreachable StorageHealthStatusType = "StorageUnreachable"
	// StorageDegraded 表示存储后端功能降级。
	StorageDegraded StorageHealthStatusType = "StorageDegraded"
)

// StorageHealthCondition 表示 CSI 驱动为其存储后端在节点上报告的不良健康状况。
type StorageHealthCondition struct {
	// status 是健康状态类别。
	// 取值为 "StorageUnreachable"、"StorageDegraded" 之一。
	Status StorageHealthStatusType
	// reason 是简短的、机器可解析的 CamelCase 原因。
	Reason string
	// message 是人类可读的描述。
	// +optional
	Message string
	// accessMode 是受影响的访问模式，nil 表示所有访问模式均受影响。
	// +optional
	AccessMode *api.PersistentVolumeAccessMode
	// volumeMode 是受影响的卷模式，nil 表示两种模式均受影响。
	// +optional
	VolumeMode *api.PersistentVolumeMode
	// lastTransitionTime 是该 condition 首次以当前状态出现的时间。
	// +optional
	LastTransitionTime metav1.Time
}

// StorageHealth 包含 CSI 驱动在节点上报告的存储后端健康状况。
type StorageHealth struct {
	// name 是 CSI 驱动名称，与 CSINodeDriver.name 一致。
	Name string
	// healthConditions 是 CSI 驱动报告的存储后端不良状况。
	// 最多可报告 16 个 condition。
	// +optional
	// +listType=atomic
	HealthConditions []StorageHealthCondition
}

// CSINodeStatus 包含节点上存储的健康和状态信息。
type CSINodeStatus struct {
	// storageHealth 包含节点上已注册 CSI 驱动的存储后端健康报告。
	// +optional
	// +listType=map
	// +listMapKey=name
	// +featureGate=CSIVolumeHealth
	StorageHealth []StorageHealth
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSINodeList 是 CSINode 对象的集合。
type CSINodeList struct {
	metav1.TypeMeta

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 CSINode 列表。
	Items []CSINode
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSIStorageCapacity 存储一次 CSI GetCapacity 调用的结果。
// 对于给定的 StorageClass，它描述某个特定拓扑段中的可用容量。
// 在考虑将新的 PersistentVolume 实例化到何处时可以使用。
//
// 例如可表达如下信息：
// - 存储类别 "standard" 在 "topology.kubernetes.io/zone=us-east1" 有 "1234 GiB" 可用
// - 存储类别 "localssd" 在 "kubernetes.io/hostname=knode-abc123" 有 "10 GiB" 可用
//
// 以下三种情况都意味着某个特定组合没有可用容量：
// - 不存在具有合适拓扑和存储类名的对象
// - 这样的对象存在，但 capacity 未设置
// - 这样的对象存在，但 capacity 为零
//
// 这些对象的生产者可自行决定采用哪种方式。
//
// 当 CSI 驱动通过 CSIDriverSpec.StorageCapacity 选择启用
// 容量感知调度时，由 kube-scheduler 消费这些对象。调度器将
// MaximumVolumeSize 与待处理 volume 的请求大小进行比较，以过滤掉不合适的节点。
// 若 MaximumVolumeSize 未设置，则回退到与精度较低的 Capacity
// 进行比较。若该值也未设置，调度器认为容量不足，
// 转而尝试其他节点。
type CSIStorageCapacity struct {
	metav1.TypeMeta
	// 标准对象元数据。名称没有特定含义。它必须是
	// DNS 子域（允许点号，253 个字符）。为确保与
	// 集群中的其他 CSI 驱动不冲突，
	// 建议使用 csisc-<uuid>、生成名，或以唯一 CSI 驱动名结尾的反向域名。
	//
	// 对象是有命名空间的。
	//
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// NodeTopology 定义哪些节点可以访问所报告容量对应的存储。
	// 未设置时，集群中任何节点都无法访问该存储。若为空对象，
	// 则所有节点均可访问。该字段不可变。
	//
	// +optional
	NodeTopology *metav1.LabelSelector

	// 所报告容量适用的 StorageClass 名称。
	// 它必须满足与 StorageClass 对象名相同的要求（非空、DNS 子域）。
	// 若该对象已不存在，则此 CSIStorageCapacity 对象已过时，
	// 应由其创建者移除。
	// 该字段不可变。
	StorageClassName string

	// Capacity 是 CSI 驱动在其 GetCapacityResponse 中报告的值，对应的
	// GetCapacityRequest 的拓扑与参数需与前述字段匹配。
	//
	// 目前（CSI spec 1.2）其语义定义为：可用于预置卷的存储的
	// 可用容量（以字节计）。若未设置，表示该信息当前不可用。
	//
	// +optional
	Capacity *resource.Quantity

	// MaximumVolumeSize 是 CSI 驱动在其 GetCapacityResponse 中报告的值，
	// 对应的 GetCapacityRequest 的拓扑与参数需与前述字段匹配。
	//
	// 自 CSI spec 1.4.0 起，其定义为：可以在
	// CreateVolumeRequest.capacity_range.required_bytes 字段中使用的、
	// 以与 GetCapacityRequest 相同参数创建卷的最大尺寸。
	// Kubernetes API 中对应的值是卷 claim 中的
	// ResourceRequirements.Requests。
	//
	// +optional
	MaximumVolumeSize *resource.Quantity
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSIStorageCapacityList 是 CSIStorageCapacity 对象的集合。
type CSIStorageCapacityList struct {
	metav1.TypeMeta
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 CSIStorageCapacity 对象的列表。
	Items []CSIStorageCapacity
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClass 表示 CSI 驱动定义的可变卷属性规格。
// 该类可在 PersistentVolumeClaim 动态创建时指定，并在创建后通过
// PersistentVolumeClaim 的 spec 修改。
type VolumeAttributesClass struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta

	// Name 是 CSI 驱动名称。
	// 该字段不可变。
	DriverName string

	// parameters 保存 CSI 驱动定义的卷属性，这些值对 Kubernetes 是不透明的，直接传给 CSI 驱动。
	// 底层存储供应商支持修改已有卷的这些属性，但 parameters 字段本身不可变。要触发卷更新，
	// 应创建带有新参数的新 VolumeAttributesClass，并更新 PersistentVolumeClaim 以引用新类。
	// 此字段为必填，至少包含一个键/值对。键不能为空，参数最多 512 个，累计大小上限 256K。
	// 若 CSI 驱动拒绝无效参数，目标 PersistentVolumeClaim 的 modifyVolumeStatus 字段会被
	// 设为 "Infeasible" 状态。
	Parameters map[string]string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// VolumeAttributesClassList 是 VolumeAttributesClass 对象的集合。
type VolumeAttributesClassList struct {
	metav1.TypeMeta

	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta

	// items 是 VolumeAttributesClass 对象列表。
	// +listType=map
	// +listMapKey=name
	Items []VolumeAttributesClass
}
