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

// StorageClass describes a named "class" of storage offered in a cluster.
// Different classes might map to quality-of-service levels, or to backup policies,
// or to arbitrary policies determined by the cluster administrators.  Kubernetes
// itself is unopinionated about what classes represent.  This concept is sometimes
// called "profiles" in other storage systems.
// The name of a StorageClass object is significant, and is how users can request a particular class.
type StorageClass struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// provisioner is the driver expected to handle this StorageClass.
	// This is an optionally-prefixed name, like a label key.
	// For example: "kubernetes.io/gce-pd" or "kubernetes.io/aws-ebs".
	// This value may not be empty.
	Provisioner string

	// parameters holds parameters for the provisioner.
	// These values are opaque to the  system and are passed directly
	// to the provisioner.  The only validation done on keys is that they are
	// not empty.  The maximum number of parameters is
	// 512, with a cumulative max size of 256K
	// +optional
	Parameters map[string]string

	// reclaimPolicy is the reclaim policy that dynamically provisioned
	// PersistentVolumes of this storage class are created with
	// +optional
	ReclaimPolicy *api.PersistentVolumeReclaimPolicy

	// mountOptions are the mount options that dynamically provisioned
	// PersistentVolumes of this storage class are created with
	// +optional
	MountOptions []string

	// AllowVolumeExpansion shows whether the storage class allow volume expand
	// If the field is nil or not set, it would amount to expansion disabled
	// for all PVs created from this storageclass.
	// +optional
	AllowVolumeExpansion *bool

	// VolumeBindingMode indicates how PersistentVolumeClaims should be
	// provisioned and bound.  When unset, VolumeBindingImmediate is used.
	// This field is only honored by servers that enable the VolumeScheduling feature.
	// +optional
	VolumeBindingMode *VolumeBindingMode

	// Restrict the node topologies where volumes can be dynamically provisioned.
	// Each volume plugin defines its own supported topology specifications.
	// An empty TopologySelectorTerm list means there is no topology restriction.
	// This field is only honored by servers that enable the VolumeScheduling feature.
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

	// Items is the list of StorageClasses
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

	// Items is the list of VolumeAttachments
	Items []VolumeAttachment
}

// The specification of a VolumeAttachment request.
type VolumeAttachmentSpec struct {
	// Attacher indicates the name of the volume driver that MUST handle this
	// request. This is the name returned by GetPluginName().
	Attacher string

	// source 表示应附加的卷。
	Source VolumeAttachmentSource

	// The node that the volume should be attached to.
	NodeName string
}

// VolumeAttachmentSource represents a volume that should be attached.
// Right now persistent volumes as well as inline volumes (only in
// CSI Migration scenarios) can be attached via external attacher.
// Exactly one member can be set.
type VolumeAttachmentSource struct {
	// Name of the persistent volume to attach.
	// +optional
	PersistentVolumeName *string

	// inlineVolumeSpec 包含挂载由 pod 内联 VolumeSource 定义的持久卷所需的全部信息。
	// 此字段仅为 CSIMigration 特性填充，包含从 pod 内联 VolumeSource
	// 转换到 PersistentVolumeSpec 的字段。此字段处于 beta 级别，
	// 仅在启用了 CSIMigration 特性的服务端上生效。
	// +optional
	InlineVolumeSpec *api.PersistentVolumeSpec
}

// The status of a VolumeAttachment request.
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

// Captures an error encountered during a volume operation.
type VolumeError struct {
	// Time the error was encountered.
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
	// VolumeBindingImmediate indicates that PersistentVolumeClaims should be
	// immediately provisioned and bound.
	VolumeBindingImmediate VolumeBindingMode = "Immediate"

	// VolumeBindingWaitForFirstConsumer indicates that PersistentVolumeClaims
	// should not be provisioned and bound until the first Pod is created that
	// references the PeristentVolumeClaim.  The volume provisioning and
	// binding will occur during Pod scheduing.
	VolumeBindingWaitForFirstConsumer VolumeBindingMode = "WaitForFirstConsumer"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// CSIDriver captures information about a Container Storage Interface (CSI)
// volume driver deployed on the cluster.
// CSI drivers do not need to create the CSIDriver object directly. Instead they may use the
// cluster-driver-registrar sidecar container. When deployed with a CSI driver it automatically
// creates a CSIDriver object representing the driver.
// Kubernetes attach detach controller uses this object to determine whether attach is required.
// Kubelet uses this object to determine whether pod information needs to be passed on mount.
// CSIDriver objects are non-namespaced.
type CSIDriver struct {
	metav1.TypeMeta

	// 标准的对象元数据。
	// metadata.Name 表示此对象所引用的 CSI 驱动名称；
	// 它必须与该驱动 CSI GetPluginName() 调用返回的名称一致。
	// 驱动名必须为 63 个字符或更少，以字母数字字符（[a-z0-9A-Z]）开头和结尾，
	// 中间可包含短横线（-）、点（.）和字母数字字符。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta

	// Specification of the CSI Driver.
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

	// If set to true, podInfoOnMount indicates this CSI volume driver
	// requires additional pod information (like podName, podUID, etc.) during
	// mount operations.
	// If set to false, pod information will not be passed on mount.
	// Default is false.
	// The CSI driver specifies podInfoOnMount as part of driver deployment.
	// If true, Kubelet will pass pod information as VolumeContext in the CSI
	// NodePublishVolume() calls.
	// The CSI driver is responsible for parsing and validating the information
	// passed in as VolumeContext.
	// The following VolumeContext will be passed if podInfoOnMount is set to true.
	// This list might grow, but the prefix will be used.
	// "csi.storage.k8s.io/pod.name": pod.Name
	// "csi.storage.k8s.io/pod.namespace": pod.Namespace
	// "csi.storage.k8s.io/pod.uid": string(pod.UID)
	// "csi.storage.k8s.io/ephemeral": "true" if the volume is an ephemeral inline volume
	//                                 defined by a CSIVolumeSource, otherwise "false"
	//
	// "csi.storage.k8s.io/ephemeral" is a new feature in Kubernetes 1.16. It is only
	// required for drivers which support both the "Persistent" and "Ephemeral" VolumeLifecycleMode.
	// Other drivers can leave pod info disabled and/or ignore this field.
	// As Kubernetes 1.15 doesn't support this field, drivers can only support one mode when
	// deployed on such a cluster and the deployment determines which mode that is, for example
	// via a command line parameter of the driver.
	//
	// This field was immutable in Kubernetes < 1.29 and now is mutable.
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

	// Defines if the underlying volume supports changing ownership and
	// permission of the volume before being mounted.
	// Refer to the specific FSGroupPolicy values for additional details.
	//
	// This field was immutable in Kubernetes < 1.29 and now is mutable.
	//
	// Defaults to ReadWriteOnceWithFSType, which will examine each volume
	// to determine if Kubernetes should modify ownership and permissions of the volume.
	// With the default policy the defined fsGroup will only be applied
	// if a fstype is defined and the volume's access mode contains ReadWriteOnce.
	// +optional
	FSGroupPolicy *FSGroupPolicy

	// TokenRequests indicates the CSI driver needs pods' service account
	// tokens it is mounting volume for to do necessary authentication. Kubelet
	// will pass the tokens in VolumeContext in the CSI NodePublishVolume calls.
	// The CSI driver should parse and validate the following VolumeContext:
	// "csi.storage.k8s.io/serviceAccount.tokens": {
	//   "<audience>": {
	//     "token": <token>,
	//     "expirationTimestamp": <expiration timestamp in RFC3339>,
	//   },
	//   ...
	// }
	//
	// Note: Audience in each TokenRequest should be different and at
	// most one token is empty string. To receive a new token after expiry,
	// RequiresRepublish can be used to trigger NodePublishVolume periodically.
	//
	// +optional
	// +listType=atomic
	TokenRequests []TokenRequest

	// RequiresRepublish indicates the CSI driver wants `NodePublishVolume`
	// being periodically called to reflect any possible change in the mounted
	// volume. This field defaults to false.
	//
	// Note: After a successful initial NodePublishVolume call, subsequent calls
	// to NodePublishVolume should only update the contents of the volume. New
	// mount points will not be seen by a running container.
	//
	// +optional
	RequiresRepublish *bool

	// SELinuxMount specifies if the CSI driver supports "-o context"
	// mount option.
	//
	// When "true", the CSI driver must ensure that all volumes provided by this CSI
	// driver can be mounted separately with different `-o context` options. This is
	// typical for storage backends that provide volumes as filesystems on block
	// devices or as independent shared volumes.
	// Kubernetes will call NodeStage / NodePublish with "-o context=xyz" mount
	// option when mounting a ReadWriteOncePod volume used in Pod that has
	// explicitly set SELinux context. In the future, it may be expanded to other
	// volume AccessModes. In any case, Kubernetes will ensure that the volume is
	// mounted only with a single SELinux context.
	//
	// When "false", Kubernetes won't pass any special SELinux mount options to the driver.
	// This is typical for volumes that represent subdirectories of a bigger shared filesystem.
	//
	// Default is "false".
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

	// serviceAccountTokenInSecrets is an opt-in for CSI drivers to indicate that
	// service account tokens should be passed via the Secrets field in NodePublishVolumeRequest
	// instead of the VolumeContext field. The CSI specification provides a dedicated Secrets
	// field for sensitive information like tokens, which is the appropriate mechanism for
	// handling credentials. This addresses security concerns where sensitive tokens were being
	// logged as part of volume context, leading to vulnerabilities like CVE-2023-2878 and
	// CVE-2024-3744.
	//
	// When "true", kubelet will pass the tokens only in the Secrets field with the key
	// "csi.storage.k8s.io/serviceAccount.tokens". The CSI driver must be updated to read
	// tokens from the Secrets field instead of VolumeContext.
	//
	// When "false" or not set, kubelet will pass the tokens in VolumeContext with the key
	// "csi.storage.k8s.io/serviceAccount.tokens" (existing behavior). This maintains backward
	// compatibility with existing CSI drivers.
	//
	// This field can only be set when TokenRequests is configured. The API server will reject
	// CSIDriver specs that set this field without TokenRequests.
	//
	// Default is "false".
	//
	// +featureGate=CSIServiceAccountTokenSecrets
	// +optional
	ServiceAccountTokenInSecrets *bool

	// PreventPodSchedulingIfMissing indicates that the CSI driver wants to prevent pod
	// scheduling if the CSI driver on the node is missing.
	//
	// Enabling this option will prevent the scheduler (or any other
	// component which embeds default scheduler such as cluster-autoscaler) from
	// scheduling pods to nodes where CSI driver is not installed.
	//
	// For components(such as cluster-autoscaler) that embed the scheduler and run
	// pod placement simulations using scheduler plugins, they MUST be aware of
	// CSI driver registration information via CSINode object. They must create simulated
	// CSINode objects in addition to Node objects during scheduling simulation, otherwise
	// if PreventPodSchedulingIfMissing is enabled globally for CSIDriver object, any
	// newly created node may be rejected by the scheduler because of missing CSI driver
	// information from the node.
	//
	// This is a beta feature and requires the VolumeLimitScaling feature gate to be enabled.
	// Default is "false".
	// +featureGate=VolumeLimitScaling
	// +optional
	PreventPodSchedulingIfMissing *bool
}

// FSGroupPolicy specifies if a CSI Driver supports modifying
// volume ownership and permissions of the volume to be mounted.
// More modes may be added in the future.
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

// VolumeLifecycleMode specifies how a CSI volume is used in Kubernetes.
// More modes may be added in the future.
type VolumeLifecycleMode string

// TokenRequest 包含 service account token 的参数。
type TokenRequest struct {
	// audience 是 "TokenRequestSpec" 中 token 的目标受众。
	// 默认为 kube apiserver 的 audience。
	Audience string

	// ExpirationSeconds is the duration of validity of the token in "TokenRequestSpec".
	// It has the same default value of "ExpirationSeconds" in "TokenRequestSpec."
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

// CSINode holds information about all CSI drivers installed on a node.
// CSI drivers do not need to create the CSINode object directly. As long as
// they use the node-driver-registrar sidecar container, the kubelet will
// automatically populate the CSINode object for the CSI driver as part of
// kubelet plugin registration.
// CSINode has the same name as a node. If the object is missing, it means either
// there are no CSI Drivers available on the node, or the Kubelet version is low
// enough that it doesn't create this object.
// CSINode has an OwnerReference that points to the corresponding node object.
// When the MutableCSINodeAllocatableCount feature gate is enabled, the allocatable.count
// field in CSINodeDriver can be updated.
type CSINode struct {
	metav1.TypeMeta

	// metadata.name must be the Kubernetes node name.
	metav1.ObjectMeta

	// spec 是 CSINode 的规格。
	Spec CSINodeSpec

	// status 包含节点存储的健康和状态信息。
	// +optional
	Status CSINodeStatus
}

// CSINodeSpec 保存节点上安装的所有 CSI 驱动的规格信息。
type CSINodeSpec struct {
	// drivers is a list of information of all CSI Drivers existing on a node.
	// If all drivers in the list are uninstalled, this can become empty.
	// +patchMergeKey=name
	// +patchStrategy=merge
	Drivers []CSINodeDriver
}

// CSINodeDriver 保存节点上安装的某个 CSI 驱动的规格信息。
type CSINodeDriver struct {
	// This is the name of the CSI driver that this object refers to.
	// This MUST be the same name returned by the CSI GetPluginName() call for
	// that driver.
	Name string

	// nodeID of the node from the driver point of view.
	// This field enables Kubernetes to communicate with storage systems that do
	// not share the same nomenclature for nodes. For example, Kubernetes may
	// refer to a given node as "node1", but the storage system may refer to
	// the same node as "nodeA". When Kubernetes issues a command to the storage
	// system to attach a volume to a specific node, it can use this field to
	// refer to the node name using the ID that the storage system will
	// understand, e.g. "nodeA" instead of "node1". This field is required.
	NodeID string

	// topologyKeys is the list of keys supported by the driver.
	// When a driver is initialized on a cluster, it provides a set of topology
	// keys that it understands (e.g. "company.com/zone", "company.com/region").
	// When a driver is initialized on a node, it provides the same topology keys
	// along with values. Kubelet will expose these topology keys as labels
	// on its own node object.
	// When Kubernetes does topology aware provisioning, it can use this list to
	// determine which labels it should retrieve from the node object and pass
	// back to the driver.
	// It is possible for different nodes to use different topology keys.
	// This can be empty if driver does not support topology.
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

// StorageHealthCondition represents an adverse health condition reported
// by a CSI driver for its storage backend on a node.
type StorageHealthCondition struct {
	// status is the health status category.
	// One of "StorageUnreachable", "StorageDegraded".
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
	// lastTransitionTime is when this condition first appeared at its current state.
	// +optional
	LastTransitionTime metav1.Time
}

// StorageHealth 包含 CSI 驱动在节点上报告的存储后端健康状况。
type StorageHealth struct {
	// name 是 CSI 驱动名称，与 CSINodeDriver.name 一致。
	Name string
	// healthConditions are the adverse storage backend conditions reported by the CSI driver.
	// At most 16 conditions may be reported.
	// +optional
	// +listType=atomic
	HealthConditions []StorageHealthCondition
}

// CSINodeStatus 包含节点上存储的健康和状态信息。
type CSINodeStatus struct {
	// storageHealth contains backend health reports for CSI drivers registered on the node.
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

	// NodeTopology defines which nodes have access to the storage
	// for which capacity was reported. If not set, the storage is
	// not accessible from any node in the cluster. If empty, the
	// storage is accessible from all nodes. This field is
	// immutable.
	//
	// +optional
	NodeTopology *metav1.LabelSelector

	// 所报告容量适用的 StorageClass 名称。
	// 它必须满足与 StorageClass 对象名相同的要求（非空、DNS 子域）。
	// 若该对象已不存在，则此 CSIStorageCapacity 对象已过时，
	// 应由其创建者移除。
	// 该字段不可变。
	StorageClassName string

	// Capacity is the value reported by the CSI driver in its GetCapacityResponse
	// for a GetCapacityRequest with topology and parameters that match the
	// previous fields.
	//
	// The semantic is currently (CSI spec 1.2) defined as:
	// The available capacity, in bytes, of the storage that can be used
	// to provision volumes. If not set, that information is currently
	// unavailable.
	//
	// +optional
	Capacity *resource.Quantity

	// MaximumVolumeSize is the value reported by the CSI driver in its GetCapacityResponse
	// for a GetCapacityRequest with topology and parameters that match the
	// previous fields.
	//
	// This is defined since CSI spec 1.4.0 as the largest size
	// that may be used in a
	// CreateVolumeRequest.capacity_range.required_bytes field to
	// create a volume with the same parameters as those in
	// GetCapacityRequest. The corresponding value in the Kubernetes
	// API is ResourceRequirements.Requests in a volume claim.
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
