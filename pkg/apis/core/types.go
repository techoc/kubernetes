/*
Copyright 2014 The Kubernetes Authors.

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

package core

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// NamespaceDefault 表示对象位于默认命名空间，客户端未指定命名空间时会采用该值
	NamespaceDefault = "default"
	// NamespaceAll 是在上下文中指定跨所有命名空间列出或过滤资源时使用的默认参数
	NamespaceAll = ""
	// NamespaceNone 是无 namespace 场景下传给 context 的参数。
	NamespaceNone = ""
	// NamespaceSystem 是部署系统组件的 namespace。
	NamespaceSystem = "kube-system"
	// NamespacePublic 是存放公开信息（ConfigMap）的 namespace。
	NamespacePublic = "kube-public"
	// NamespaceNodeLease 是放置节点租约对象（用于节点心跳）的命名空间
	NamespaceNodeLease = "kube-node-lease"
	// TerminationMessagePathDefault 表示捕获容器中运行的应用程序终止消息的默认路径。
	TerminationMessagePathDefault = "/dev/termination-log"
)

// Volume represents a named volume in a pod that may be accessed by any containers in the pod.
type Volume struct {
	// Required: This must be a DNS_LABEL.  Each volume in a pod must have
	// a unique name.
	Name string
	// The VolumeSource represents the location and type of a volume to mount.
	// This is optional for now. If not specified, the Volume is implied to be an EmptyDir.
	// 此隐含行为已弃用，将在未来版本中移除。
	// +optional
	VolumeSource
}

// VolumeSource represents the source location of a volume to mount.
// 只能指定其中一个成员。
type VolumeSource struct {
	// hostPath represents file or directory on the host machine that is
	// directly exposed to the container. This is generally used for system
	// agents or other privileged things that are allowed to see the host
	// machine. Most containers will NOT need this.
	// ---
	// TODO(jonesdl) We need to restrict who can use host directory mounts and who can/can not
	// mount host directories as read/write.
	// +optional
	HostPath *HostPathVolumeSource
	// emptyDir 表示与 Pod 生命周期相同的临时目录。
	// +optional
	EmptyDir *EmptyDirVolumeSource
	// gcePersistentDisk 表示挂载到 kubelet 主机、再暴露给 Pod 的 GCE 磁盘资源。
	// Deprecated: GCEPersistentDisk 已弃用。树内 gcePersistentDisk 类型的所有操作
	// 都会重定向到 pd.csi.storage.gke.io CSI 驱动。
	// +optional
	GCEPersistentDisk *GCEPersistentDiskVolumeSource
	// awsElasticBlockStore represents an AWS EBS disk that is attached to a
	// kubelet's host machine and then exposed to the pod.
	// Deprecated: AWSElasticBlockStore is deprecated. All operations for the in-tree
	// awsElasticBlockStore type are redirected to the ebs.csi.aws.com CSI driver.
	// +optional
	AWSElasticBlockStore *AWSElasticBlockStoreVolumeSource
	// gitRepo 表示处于特定版本的 git 仓库。
	// Deprecated: GitRepo 已弃用。若要为容器提供 git 仓库，可将一个 EmptyDir 挂载到
	// InitContainer，用 git 克隆仓库，然后再将该 EmptyDir 挂载到 Pod 的容器中。
	// +optional
	GitRepo *GitRepoVolumeSource
	// secret 表示用于填充该卷的 secret。
	// +optional
	Secret *SecretVolumeSource
	// nfs 表示主机上的 NFS 挂载，其生命周期与 Pod 相同。
	// +optional
	NFS *NFSVolumeSource
	// iscsi 表示挂载到 kubelet 主机、再暴露给 Pod 的 ISCSI 磁盘资源。
	// +optional
	ISCSI *ISCSIVolumeSource
	// glusterfs 表示主机上与 Pod 生命周期相同的 Glusterfs 挂载。
	// Deprecated: Glusterfs 已弃用，不再支持树内 glusterfs 类型。
	// +optional
	Glusterfs *GlusterfsVolumeSource
	// persistentVolumeClaim represents a reference to a PersistentVolumeClaim in the same namespace
	// +optional
	PersistentVolumeClaim *PersistentVolumeClaimVolumeSource
	// rbd 表示主机上与 Pod 生命周期相同的 Rados Block Device 挂载。
	// Deprecated: RBD 已弃用，不再支持树内 rbd 类型。
	// +optional
	RBD *RBDVolumeSource

	// flexVolume 表示通过基于 exec 的插件供给/挂载的通用卷资源。
	// Deprecated: FlexVolume 已弃用。建议改用 CSIDriver。
	// +optional
	FlexVolume *FlexVolumeSource

	// cinder 表示挂载到 kubelet 主机的 cinder 卷。
	// Deprecated: Cinder 已弃用。树内 cinder 类型的所有操作
	// 都会重定向到 cinder.csi.openstack.org CSI 驱动。
	// +optional
	Cinder *CinderVolumeSource

	// cephFS represents a Cephfs mount on the host that shares a pod's lifetime.
	// Deprecated: CephFS is deprecated and the in-tree cephfs type is no longer supported.
	// +optional
	CephFS *CephFSVolumeSource

	// flocker 表示挂载到 kubelet 主机的 Flocker 卷。依赖 Flocker 控制服务正在运行。
	// Deprecated: Flocker 已弃用，不再支持树内 flocker 类型。
	// +optional
	Flocker *FlockerVolumeSource

	// downwardAPI represents metadata about the pod that should populate this volume
	// +optional
	DownwardAPI *DownwardAPIVolumeSource
	// fc 表示挂载到 kubelet 主机、再暴露给 Pod 的 Fibre Channel 资源。
	// +optional
	FC *FCVolumeSource
	// azureFile 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
	// Deprecated: AzureFile 已弃用。树内 azureFile 类型的所有操作
	// 都会重定向到 file.csi.azure.com CSI 驱动。
	// +optional
	AzureFile *AzureFileVolumeSource
	// ConfigMap represents a configMap that should populate this volume
	// +optional
	ConfigMap *ConfigMapVolumeSource
	// vsphereVolume 表示挂载到 kubelet 主机的 vSphere 卷。
	// Deprecated: VsphereVolume 已弃用。树内 vsphereVolume 类型的所有操作
	// 都会重定向到 csi.vsphere.vmware.com CSI 驱动。
	// +optional
	VsphereVolume *VsphereVirtualDiskVolumeSource
	// quobyte 表示主机上与 Pod 生命周期相同的 Quobyte 挂载。
	// Deprecated: Quobyte 已弃用，不再支持树内 quobyte 类型。
	// +optional
	Quobyte *QuobyteVolumeSource
	// azureDisk 表示主机上的 Azure 数据盘挂载，并绑定挂载到 Pod。
	// Deprecated: AzureDisk 已弃用。树内 azureDisk 类型的所有操作
	// 都会重定向到 disk.csi.azure.com CSI 驱动。
	// +optional
	AzureDisk *AzureDiskVolumeSource
	// photonPersistentDisk 表示挂载到 kubelet 主机的 PhotonController 持久盘。
	// Deprecated: PhotonPersistentDisk 已弃用，不再支持树内 photonPersistentDisk 类型。
	PhotonPersistentDisk *PhotonPersistentDiskVolumeSource
	// projected 将 secret、configmap 和 downward API 全部汇聚到一处。
	Projected *ProjectedVolumeSource
	// portworxVolume 表示挂载到 kubelet 主机的 portworx 卷。
	// Deprecated: PortworxVolume 已弃用。树内 portworxVolume 类型的所有操作
	// 都会重定向到 pxd.portworx.com CSI 驱动。
	// +optional
	PortworxVolume *PortworxVolumeSource
	// scaleIO 表示挂载到 Kubernetes 节点的 ScaleIO 持久卷。
	// Deprecated: ScaleIO 已弃用，不再支持树内 scaleIO 类型。
	// +optional
	ScaleIO *ScaleIOVolumeSource
	// storageOS represents a StorageOS volume that is attached to the kubelet's host machine and mounted into the pod.
	// 已弃用：StorageOS 已弃用，不再支持树内 storageos 类型。
	// +optional
	StorageOS *StorageOSVolumeSource
	// csi（容器存储接口）表示由某些外部 CSI 驱动处理的临时存储。
	// +optional
	CSI *CSIVolumeSource
	// ephemeral represents a volume that is handled by a cluster storage driver.
	// The volume's lifecycle is tied to the pod that defines it - it will be created before the pod starts,
	// and deleted when the pod is removed.
	//
	// Use this if:
	// a) the volume is only needed while the pod runs,
	// b) features of normal volumes like restoring from snapshot or capacity
	//    tracking are needed,
	// c) the storage driver is specified through a storage class, and
	// d) the storage driver supports dynamic volume provisioning through
	//    a PersistentVolumeClaim (see EphemeralVolumeSource for more
	//    information on the connection between this volume type
	//    and PersistentVolumeClaim).
	//
	// Use PersistentVolumeClaim or one of the vendor-specific
	// APIs for volumes that persist for longer than the lifecycle
	// of an individual pod.
	//
	// Use CSI for light-weight local ephemeral volumes if the CSI driver is meant to
	// be used that way - see the documentation of the driver for
	// more information.
	//
	// A pod can use both types of ephemeral volumes and
	// persistent volumes at the same time.
	//
	// +optional
	Ephemeral *EphemeralVolumeSource
	// image 表示在 kubelet 主机上拉取并挂载的 OCI 对象（容器镜像或制品）。
	// 卷在 Pod 启动时根据所提供的 PullPolicy 值进行解析：
	// - Always：kubelet 总是尝试拉取引用。拉取失败则容器创建失败。
	// - Never：kubelet 从不拉取引用，只使用本地镜像或制品。引用不存在则容器创建失败。
	// - IfNotPresent：若磁盘上尚无引用，kubelet 才会拉取。引用不存在且拉取失败则容器创建失败。
	// 若 Pod 被删除并重建，卷会重新解析，因此重建 Pod 后可使用新的远端内容。
	// Pod 启动期间解析或拉取镜像失败会阻塞容器启动，并可能带来显著延迟。
	// 失败会按常规卷退避策略重试，并反映在 Pod 的 reason 和 message 中。
	// 该卷可挂载的对象类型由主机上的容器运行时实现定义，至少须包含 container image 字段支持的所有有效类型。
	// OCI 对象以与容器镜像相同的方式合并清单层，挂载到单个目录（spec.containers[*].volumeMounts.mountPath）中。
	// 卷以只读（ro）方式挂载。
	// 1.33 之前不支持容器的子路径挂载（spec.containers[*].volumeMounts.subpath）。
	// 字段 spec.securityContext.fsGroupChangePolicy 对此卷类型无效。
	// +optional
	Image *ImageVolumeSource
}

// PersistentVolumeSource is similar to VolumeSource but meant for the administrator who creates PVs.
// Exactly one of its members must be set.
type PersistentVolumeSource struct {
	// gcePersistentDisk 表示挂载到 kubelet 主机、再暴露给 Pod 的 GCE 磁盘资源，由管理员供给。
	// Deprecated: GCEPersistentDisk 已弃用。树内 gcePersistentDisk 类型的所有操作
	// 都会重定向到 pd.csi.storage.gke.io CSI 驱动。
	// +optional
	GCEPersistentDisk *GCEPersistentDiskVolumeSource
	// awsElasticBlockStore 表示挂载到 kubelet 主机、再暴露给 Pod 的 AWS 磁盘资源。
	// Deprecated: AWSElasticBlockStore 已弃用。树内 awsElasticBlockStore 类型的所有操作
	// 都会重定向到 ebs.csi.aws.com CSI 驱动。
	// +optional
	AWSElasticBlockStore *AWSElasticBlockStoreVolumeSource
	// hostPath represents a directory on the host.
	// Provisioned by a developer or tester.
	// This is useful for single-node development and testing only!
	// On-host storage is not supported in any way and WILL NOT WORK in a multi-node cluster.
	// +optional
	HostPath *HostPathVolumeSource
	// glusterfs 表示挂载到主机、再暴露给 Pod 的 Glusterfs 卷，由管理员供给。
	// Deprecated: Glusterfs 已弃用，不再支持树内 glusterfs 类型。
	// +optional
	Glusterfs *GlusterfsPersistentVolumeSource
	// nfs 表示主机上的 NFS 挂载，其生命周期与 Pod 相同。
	// +optional
	NFS *NFSVolumeSource
	// rbd 表示主机上与 Pod 生命周期相同的 Rados Block Device 挂载。
	// Deprecated: RBD 已弃用，不再支持树内 rbd 类型。
	// +optional
	RBD *RBDPersistentVolumeSource
	// iscsi 表示挂载到 kubelet 主机、再暴露给 Pod 的 ISCSI 磁盘资源，由管理员供给。
	// +optional
	ISCSI *ISCSIPersistentVolumeSource
	// cinder 表示挂载到 kubelet 主机的 cinder 卷。
	// Deprecated: Cinder 已弃用。树内 cinder 类型的所有操作
	// 都会重定向到 cinder.csi.openstack.org CSI 驱动。
	// +optional
	Cinder *CinderPersistentVolumeSource
	// cephFS 表示主机上与 Pod 生命周期相同的 Ceph FS 挂载。
	// Deprecated: CephFS 已弃用，不再支持树内 cephfs 类型。
	// +optional
	CephFS *CephFSPersistentVolumeSource
	// fc 表示挂载到 kubelet 主机、再暴露给 Pod 的 Fibre Channel 资源。
	// +optional
	FC *FCVolumeSource
	// flocker 表示挂载到 kubelet 主机、供 Pod 使用的 Flocker 卷。依赖 Flocker 控制服务正在运行。
	// Deprecated: Flocker 已弃用，不再支持树内 flocker 类型。
	// +optional
	Flocker *FlockerVolumeSource
	// flexVolume 表示通过基于 exec 的插件供给/挂载的通用卷资源。
	// Deprecated: FlexVolume 已弃用。建议改用 CSIDriver。
	// +optional
	FlexVolume *FlexPersistentVolumeSource
	// azureFile 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
	// Deprecated: AzureFile 已弃用。树内 azureFile 类型的所有操作
	// 都会重定向到 file.csi.azure.com CSI 驱动。
	// +optional
	AzureFile *AzureFilePersistentVolumeSource
	// vsphereVolume 表示挂载到 kubelet 主机的 vSphere 卷。
	// Deprecated: VsphereVolume 已弃用。树内 vsphereVolume 类型的所有操作
	// 都会重定向到 csi.vsphere.vmware.com CSI 驱动。
	// +optional
	VsphereVolume *VsphereVirtualDiskVolumeSource
	// quobyte 表示主机上与 Pod 生命周期相同的 Quobyte 挂载。
	// Deprecated: Quobyte 已弃用，不再支持树内 quobyte 类型。
	// +optional
	Quobyte *QuobyteVolumeSource
	// azureDisk 表示主机上的 Azure 数据盘挂载，并绑定挂载到 Pod。
	// Deprecated: AzureDisk 已弃用。树内 azureDisk 类型的所有操作
	// 都会重定向到 disk.csi.azure.com CSI 驱动。
	// +optional
	AzureDisk *AzureDiskVolumeSource
	// photonPersistentDisk 表示挂载到 kubelet 主机的 PhotonController 持久盘。
	// Deprecated: PhotonPersistentDisk 已弃用，不再支持树内 photonPersistentDisk 类型。
	PhotonPersistentDisk *PhotonPersistentDiskVolumeSource
	// portworxVolume 表示挂载到 kubelet 主机的 portworx 卷。
	// Deprecated: PortworxVolume 已弃用。树内 portworxVolume 类型的所有操作
	// 都会重定向到 pxd.portworx.com CSI 驱动。
	// +optional
	PortworxVolume *PortworxVolumeSource
	// scaleIO 表示挂载到 Kubernetes 节点的 ScaleIO 持久卷。
	// Deprecated: ScaleIO 已弃用，不再支持树内 scaleIO 类型。
	// +optional
	ScaleIO *ScaleIOPersistentVolumeSource
	// local 表示带有节点亲和性的直接挂载存储。
	// +optional
	Local *LocalVolumeSource
	// storageOS 表示挂载到 kubelet 主机并挂载进 Pod 的 StorageOS 卷。
	// Deprecated: StorageOS 已弃用，不再支持树内 storageos 类型。
	// +optional
	StorageOS *StorageOSPersistentVolumeSource
	// csi 表示由外部 CSI 驱动处理的存储。
	// +optional
	CSI *CSIPersistentVolumeSource
}

// PersistentVolumeClaimVolumeSource represents a reference to a PersistentVolumeClaim in the same namespace
type PersistentVolumeClaimVolumeSource struct {
	// ClaimName is the name of a PersistentVolumeClaim in the same namespace as the pod using this volume
	ClaimName string
	// Optional: Defaults to false (read/write).  ReadOnly here
	// will force the ReadOnly setting in VolumeMounts
	// +optional
	ReadOnly bool
}

const (
	// BetaStorageClassAnnotation 表示 beta/旧版 StorageClass 注解。
	// It's deprecated and will be removed in a future release. (#51440)
	BetaStorageClassAnnotation = "volume.beta.kubernetes.io/storage-class"

	// MountOptionAnnotation defines mount option annotation used in PVs
	MountOptionAnnotation = "volume.beta.kubernetes.io/mount-options"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolume struct captures the details of the implementation of PV storage
type PersistentVolume struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines a persistent volume owned by the cluster
	// +optional
	Spec PersistentVolumeSpec

	// Status represents the current information about persistent volume.
	// +optional
	Status PersistentVolumeStatus
}

// PersistentVolumeSpec has most of the details required to define a persistent volume
type PersistentVolumeSpec struct {
	// Resources represents the actual resources of the volume
	Capacity ResourceList
	// Source represents the location and type of a volume to mount.
	PersistentVolumeSource
	// AccessModes contains all ways the volume can be mounted
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// ClaimRef is part of a bi-directional binding between PersistentVolume and PersistentVolumeClaim.
	// ClaimRef is expected to be non-nil when bound.
	// claim.VolumeName 才是 PV 与 PVC 绑定的权威依据。
	// When set to non-nil value, PVC.Spec.Selector of the referenced PVC is
	// ignored, i.e. labels of this PV do not need to match PVC selector.
	// +optional
	ClaimRef *ObjectReference
	// Optional: what happens to a persistent volume when released from its claim.
	// +optional
	PersistentVolumeReclaimPolicy PersistentVolumeReclaimPolicy
	// Name of StorageClass to which this persistent volume belongs. Empty value
	// means that this volume does not belong to any StorageClass.
	// +optional
	StorageClassName string
	// A list of mount options, e.g. ["ro", "soft"]. Not validated - mount will
	// simply fail if one is invalid.
	// +optional
	MountOptions []string
	// volumeMode 定义卷是应使用格式化文件系统，还是保持原始块状态。
	// spec 中未包含时隐含为 Filesystem。
	// +optional
	VolumeMode *PersistentVolumeMode
	// NodeAffinity defines constraints that limit what nodes this volume can be accessed from.
	// This field influences the scheduling of pods that use this volume.
	// This field is mutable if MutablePVNodeAffinity feature gate is enabled.
	// +optional
	NodeAffinity *VolumeNodeAffinity
	// Name of VolumeAttributesClass to which this persistent volume belongs. Empty value
	// is not allowed. When this field is not set, it indicates that this volume does not belong to any
	// VolumeAttributesClass. This field is mutable and can be changed by the CSI driver
	// after a volume has been updated successfully to a new class.
	// For an unbound PersistentVolume, the volumeAttributesClassName will be matched with unbound
	// PersistentVolumeClaims during the binding process.
	// +featureGate=VolumeAttributesClass
	// +optional
	VolumeAttributesClassName *string
}

// VolumeNodeAffinity 定义限制可从哪些节点访问该卷的约束。
type VolumeNodeAffinity struct {
	// required 指定必须满足的硬性节点约束。
	Required *NodeSelector
}

// PersistentVolumeReclaimPolicy describes a policy for end-of-life maintenance of persistent volumes
type PersistentVolumeReclaimPolicy string

const (
	// PersistentVolumeReclaimRecycle 表示卷从其声明释放后会被回收到未绑定持久卷池中。
	// 卷插件必须支持回收（Recycling）。
	// DEPRECATED: The PersistentVolumeReclaimRecycle called Recycle is being deprecated. See announcement here: https://groups.google.com/forum/#!topic/kubernetes-dev/uexugCza84I
	PersistentVolumeReclaimRecycle PersistentVolumeReclaimPolicy = "Recycle"
	// PersistentVolumeReclaimDelete 表示卷从其声明释放后会从 Kubernetes 中删除。
	// 卷插件必须支持删除（Deletion）。
	PersistentVolumeReclaimDelete PersistentVolumeReclaimPolicy = "Delete"
	// PersistentVolumeReclaimRetain 表示卷保持当前阶段（Released），由管理员手动回收。
	// 默认策略为 Retain。
	PersistentVolumeReclaimRetain PersistentVolumeReclaimPolicy = "Retain"
)

// PersistentVolumeMode 描述卷的使用方式：Block 或 Filesystem。
type PersistentVolumeMode string

const (
	// PersistentVolumeBlock 表示卷不会用文件系统格式化，而是保持为原始块设备。
	PersistentVolumeBlock PersistentVolumeMode = "Block"
	// PersistentVolumeFilesystem 表示卷将或已使用文件系统格式化。
	PersistentVolumeFilesystem PersistentVolumeMode = "Filesystem"
)

// PersistentVolumeStatus represents the status of PV storage
type PersistentVolumeStatus struct {
	// phase 表示卷是否可用、已绑定到声明或已被声明释放。
	// +optional
	Phase PersistentVolumePhase
	// message 是人类可读的消息，说明卷处于该状态的详细原因。
	// +optional
	Message string
	// reason 是简短的 CamelCase 字符串，描述任何故障，
	// 便于机器解析并在 CLI 中整洁展示。
	// +optional
	Reason string
	// LastPhaseTransitionTime is the time the phase transitioned from one to another
	// and automatically resets to current time everytime a volume phase transitions.
	// +optional
	LastPhaseTransitionTime *metav1.Time
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolumeList represents a list of PVs
type PersistentVolumeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []PersistentVolume
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolumeClaim 是用户对持久卷的请求和声明
type PersistentVolumeClaim struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the volume requested by a pod author
	// +optional
	Spec PersistentVolumeClaimSpec

	// Status represents the current information about a claim
	// +optional
	Status PersistentVolumeClaimStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolumeClaimList represents the list of PV claims
type PersistentVolumeClaimList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []PersistentVolumeClaim
}

// PersistentVolumeClaimSpec describes the common attributes of storage devices
// and allows a Source for provider-specific attributes
type PersistentVolumeClaimSpec struct {
	// Contains the types of access modes required
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// A label query over volumes to consider for binding. This selector is
	// ignored when VolumeName is set
	// +optional
	Selector *metav1.LabelSelector
	// Resources represents the minimum resources required
	// If RecoverVolumeExpansionFailure feature is enabled users are allowed to specify resource requirements
	// that are lower than previous value but must still be higher than capacity recorded in the
	// status field of the claim.
	// +optional
	Resources VolumeResourceRequirements
	// VolumeName is the binding reference to the PersistentVolume backing this
	// claim. When set to non-empty value Selector is not evaluated
	// +optional
	VolumeName string
	// Name of the StorageClass required by the claim.
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
	// +optional
	StorageClassName *string
	// volumeMode 定义声明所需的卷类型。
	// 声明 spec 中未包含时隐含为 Filesystem。
	// +optional
	VolumeMode *PersistentVolumeMode
	// dataSource 字段可用于指定以下之一：
	// * 现有的 VolumeSnapshot 对象（snapshot.storage.k8s.io/VolumeSnapshot）
	// * 现有的 PVC（PersistentVolumeClaim）
	// 如果供给器或外部控制器支持指定的数据源，它将基于该数据源的内容创建新卷。
	// dataSource 的内容会复制到 dataSourceRef；当 dataSourceRef.namespace 未指定时，
	// dataSourceRef 的内容也会复制到 dataSource。
	// 若指定了命名空间，则 dataSourceRef 不会复制到 dataSource。
	// +optional
	DataSource *TypedLocalObjectReference
	// Specifies the object from which to populate the volume with data, if a non-empty
	// volume is desired. This may be any object from a non-empty API group (non
	// core object) or a PersistentVolumeClaim object.
	// When this field is specified, volume binding will only succeed if the type of
	// the specified object matches some installed volume populator or dynamic
	// provisioner.
	// This field will replace the functionality of the dataSource field and as such
	// if both fields are non-empty, they must have the same value. For backwards
	// compatibility, when namespace isn't specified in dataSourceRef,
	// both fields (dataSource and dataSourceRef) will be set to the same
	// value automatically if one of them is empty and the other is non-empty.
	// When namespace is specified in dataSourceRef,
	// dataSource isn't set to the same value and must be empty.
	// There are three important differences between dataSource and dataSourceRef:
	// * While dataSource only allows two specific types of objects, dataSourceRef
	//   allows any non-core object, as well as PersistentVolumeClaim objects.
	// * While dataSource ignores disallowed values (dropping them), dataSourceRef
	//   preserves all values, and generates an error if a disallowed value is
	//   specified.
	// * While dataSource only allows local objects, dataSourceRef allows objects
	//   in any namespaces.
	// +optional
	DataSourceRef *TypedObjectReference
	// volumeAttributesClassName 可用于设置此声明使用的 VolumeAttributesClass。
	// 若指定，CSI 驱动会使用相应 VolumeAttributesClass 中定义的属性创建或更新卷。
	// 它的用途与 storageClassName 不同，可在声明创建后修改。空字符串或 nil 表示不会对该声明应用任何 VolumeAttributesClass。
	// 如果声明进入 Infeasible 错误状态，可将此字段重置为之前的值（包括 nil）以取消修改。
	// 如果 volumeAttributesClass 引用的资源不存在，此 PersistentVolumeClaim 会被置为 Pending 状态，
	// 如 modifyVolumeStatus 字段所示，直至该资源存在为止。
	// More info: https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/
	// +featureGate=VolumeAttributesClass
	// +optional
	VolumeAttributesClassName *string
}

type TypedObjectReference struct {
	// APIGroup 是被引用资源所属的组。
	// 若未指定 APIGroup，则指定的 Kind 必须属于核心 API 组。
	// 对于其他任何第三方类型，APIGroup 为必填项。
	// +optional
	APIGroup *string
	// Kind 是被引用资源的类型
	Kind string
	// Name 是被引用资源的名称
	Name string
	// Namespace 是被引用资源所在的命名空间
	// 注意，指定命名空间时，被引用命名空间中必须存在一个 gateway.networking.k8s.io/ReferenceGrant 对象，
	// 以允许该命名空间的所有者接受此引用。详情参见 ReferenceGrant 文档。
	// (Alpha) 此字段需要启用 CrossNamespaceVolumeDataSource 特性门控。
	// +featureGate=CrossNamespaceVolumeDataSource
	// +optional
	Namespace *string
}

// PersistentVolumeClaimConditionType 定义 PVC 的条件类型。
// Valid values are:
//   - "Resizing", "FileSystemResizePending"
//
// If RecoverVolumeExpansionFailure feature gate is enabled, then following additional values can be expected:
//   - "ControllerResizeError", "NodeResizeError"
//
// 若启用 VolumeAttributesClass 特性门控，则可能还有以下额外值：
//   - "ModifyVolumeError", "ModifyingVolume"
type PersistentVolumeClaimConditionType string

// These are valid conditions of PVC
const (
	// An user trigger resize of pvc has been started
	PersistentVolumeClaimResizing PersistentVolumeClaimConditionType = "Resizing"
	// PersistentVolumeClaimFileSystemResizePending 表示控制器扩容已完成，节点上仍待进行文件系统扩容
	PersistentVolumeClaimFileSystemResizePending PersistentVolumeClaimConditionType = "FileSystemResizePending"

	// PersistentVolumeClaimControllerResizeError 表示控制器扩容卷容量时发生错误
	PersistentVolumeClaimControllerResizeError PersistentVolumeClaimConditionType = "ControllerResizeError"
	// PersistentVolumeClaimNodeResizeError 表示节点扩容卷容量时发生错误。
	PersistentVolumeClaimNodeResizeError PersistentVolumeClaimConditionType = "NodeResizeError"

	// 应用目标 VolumeAttributesClass 时遇到错误
	PersistentVolumeClaimVolumeModifyVolumeError PersistentVolumeClaimConditionType = "ModifyVolumeError"
	// Volume is being modified
	PersistentVolumeClaimVolumeModifyingVolume PersistentVolumeClaimConditionType = "ModifyingVolume"

	// PersistentVolumeClaimUnused 表示该 PVC 当前是否未被任何 Pod 使用。
	// 状态为 True 时，该 PVC 未被任何非终止态 Pod 引用。
	// lastTransitionTime 表示 PVC 上次转变为未使用状态的时间。
	// 由于处理延迟或集群中启用该功能的时间点不同，此条件所指示的使用时长和未使用时长
	// 可能短于或略长于实际的使用/未使用时长。
	// 需要 PersistentVolumeClaimUnusedSinceTime beta 特性门控
	PersistentVolumeClaimUnused PersistentVolumeClaimConditionType = "Unused"
)

// +enum
// When a controller receives persistentvolume claim update with ClaimResourceStatus for a resource
// that it does not recognizes, then it should ignore that update and let other controllers
// handle it.
type ClaimResourceStatus string

const (
	// State set when resize controller starts resizing the volume in control-plane
	PersistentVolumeClaimControllerResizeInProgress ClaimResourceStatus = "ControllerResizeInProgress"

	// 当调整容量控制器因不可恢复的终结性错误而扩容失败时设置的状态。
	// 超时等瞬时错误不应设置此状态，也不应修改 allocatedResourceStatus，
	// 以便调整容量控制器可以继续卷扩容。
	PersistentVolumeClaimControllerResizeInfeasible ClaimResourceStatus = "ControllerResizeInfeasible"

	// State set when resize controller has finished resizing the volume but further resizing of volume
	// is needed on the node.
	PersistentVolumeClaimNodeResizePending ClaimResourceStatus = "NodeResizePending"
	// 当 kubelet 开始扩容卷时设置的状态
	PersistentVolumeClaimNodeResizeInProgress ClaimResourceStatus = "NodeResizeInProgress"
	// 当 kubelet 扩容因不可恢复的终结性错误而失败时设置的状态。瞬时错误不应设置此状态
	PersistentVolumeClaimNodeResizeInfeasible ClaimResourceStatus = "NodeResizeInfeasible"
)

// +enum
// New statuses can be added in the future. Consumers should check for unknown statuses and fail appropriately
type PersistentVolumeClaimModifyVolumeStatus string

const (
	// Pending 表示因未满足条件（例如指定的 VolumeAttributesClass 不存在），PersistentVolumeClaim 无法被修改
	PersistentVolumeClaimModifyVolumePending PersistentVolumeClaimModifyVolumeStatus = "Pending"
	// InProgress 表示卷正在被修改
	PersistentVolumeClaimModifyVolumeInProgress PersistentVolumeClaimModifyVolumeStatus = "InProgress"
	// Infeasible 表示请求被 CSI 驱动判定为无效而拒绝。要解决该错误，需指定有效的 VolumeAttributesClass
	PersistentVolumeClaimModifyVolumeInfeasible PersistentVolumeClaimModifyVolumeStatus = "Infeasible"
)

// ModifyVolumeStatus 表示 ControllerModifyVolume 操作的状态对象
type ModifyVolumeStatus struct {
	// targetVolumeAttributesClassName 是当前正在进行协调的 PVC 所使用的 VolumeAttributesClass 名称
	TargetVolumeAttributesClassName string
	// status 是 ControllerModifyVolume 操作的状态，可以是以下任一状态：
	// - Pending
	// Pending 表示因未满足条件（例如指定的 VolumeAttributesClass 不存在），PersistentVolumeClaim 无法被修改。
	// - InProgress
	// InProgress 表示卷正在被修改。
	// - Infeasible
	// Infeasible 表示请求被 CSI 驱动判定为无效而拒绝。要解决该错误，需指定有效的 VolumeAttributesClass。
	// 注意：未来可能新增状态。消费方应检查未知状态并作适当处理。
	Status PersistentVolumeClaimModifyVolumeStatus
}

// PersistentVolumeClaimCondition represents the current condition of PV claim
type PersistentVolumeClaimCondition struct {
	Type   PersistentVolumeClaimConditionType
	Status ConditionStatus
	// +optional
	LastProbeTime metav1.Time
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// VolumeHealthStatusType 描述卷的健康状态类别。
type VolumeHealthStatusType string

const (
	// VolumeHealthInaccessible 表示无法访问该卷。
	VolumeHealthInaccessible VolumeHealthStatusType = "Inaccessible"
	// VolumeHealthDataLoss 表示检测到该卷发生数据丢失。
	VolumeHealthDataLoss VolumeHealthStatusType = "DataLoss"
	// VolumeHealthDegraded 表示卷仍可工作，但能力有所下降。
	VolumeHealthDegraded VolumeHealthStatusType = "Degraded"
)

// VolumeHealthCondition represents an adverse health condition reported for a volume.
type VolumeHealthCondition struct {
	// status is the machine-parseable health category.
	// Possible values:
	// - "Inaccessible": the volume cannot be accessed.
	// - "DataLoss": data loss has been detected on the volume.
	// - "Degraded": the volume is functioning with reduced capability.
	Status VolumeHealthStatusType
	// reason 是简短的、机器可解析的 CamelCase 原因。
	// 与 status 一起构成某个条件条目的唯一标识。
	Reason string
	// message 是人类可读的描述。
	// +optional
	Message string
}

// VolumeHealthStatus contains health information for a volume reported
// by the CSI controller plugin.
type VolumeHealthStatus struct {
	// conditions is the set of adverse conditions reported by
	// the CSI controller plugin. An empty list means no adverse condition.
	// At most 16 conditions may be reported.
	// +optional
	// +listType=map
	// +listMapKey=status
	// +listMapKey=reason
	HealthConditions []VolumeHealthCondition
	// lastTransitionTime is when the current set of conditions first appeared.
	// +optional
	LastTransitionTime metav1.Time
}

// PodVolumeHealth contains health information for a volume used by a pod,
// reported by the CSI node plugin via the kubelet.
type PodVolumeHealth struct {
	// name matches an entry in pod.spec.volumes.
	Name string
	// conditions is the set of adverse conditions reported by
	// the CSI node plugin for this volume on this node.
	// At most 16 conditions may be reported.
	// +optional
	// +listType=map
	// +listMapKey=status
	// +listMapKey=reason
	HealthConditions []VolumeHealthCondition
	// lastTransitionTime is when the current set of conditions first appeared.
	// +optional
	LastTransitionTime metav1.Time
}

// PersistentVolumeClaimStatus represents the status of PV claim
type PersistentVolumeClaimStatus struct {
	// Phase represents the current phase of PersistentVolumeClaim
	// +optional
	Phase PersistentVolumeClaimPhase
	// AccessModes contains all ways the volume backing the PVC can be mounted
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// Represents the actual resources of the underlying volume
	// +optional
	Capacity ResourceList
	// +optional
	Conditions []PersistentVolumeClaimCondition
	// AllocatedResources tracks the resources allocated to a PVC including its capacity.
	// Key names follow standard Kubernetes label syntax. Valid values are either:
	// 	* Un-prefixed keys:
	//		- storage - the capacity of the volume.
	//	* Custom resources must use implementation-defined prefixed names such as "example.com/my-custom-resource"
	// Apart from above values - keys that are unprefixed or have kubernetes.io prefix are considered
	// reserved and hence may not be used.
	//
	// Capacity reported here may be larger than the actual capacity when a volume expansion operation
	// is requested.
	// For storage quota, the larger value from allocatedResources and PVC.spec.resources is used.
	// If allocatedResources is not set, PVC.spec.resources alone is used for quota calculation.
	// If a volume expansion capacity request is lowered, allocatedResources is only
	// lowered if there are no expansion operations in progress and if the actual volume capacity
	// is equal or lower than the requested capacity.
	//
	// A controller that receives PVC update with previously unknown resourceName
	// should ignore the update for the purpose it was designed. For example - a controller that
	// only is responsible for resizing capacity of the volume, should ignore PVC updates that change other valid
	// resources associated with PVC.
	// +optional
	AllocatedResources ResourceList
	// AllocatedResourceStatuses stores status of resource being resized for the given PVC.
	// Key names follow standard Kubernetes label syntax. Valid values are either:
	// 	* Un-prefixed keys:
	//		- storage - the capacity of the volume.
	//	* Custom resources must use implementation-defined prefixed names such as "example.com/my-custom-resource"
	// Apart from above values - keys that are unprefixed or have kubernetes.io prefix are considered
	// reserved and hence may not be used.
	//
	// ClaimResourceStatus can be in any of following states:
	//	- ControllerResizeInProgress:
	//		State set when resize controller starts resizing the volume in control-plane.
	// 	- ControllerResizeFailed:
	//		State set when resize has failed in resize controller with a terminal error.
	//	- NodeResizePending:
	//		State set when resize controller has finished resizing the volume but further resizing of
	//		volume is needed on the node.
	//	- NodeResizeInProgress:
	//		State set when kubelet starts resizing the volume.
	//	- NodeResizeFailed:
	//		State set when resizing has failed in kubelet with a terminal error. Transient errors don't set
	//		NodeResizeFailed.
	// For example: if expanding a PVC for more capacity - this field can be one of the following states:
	// 	- pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeFailed"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizePending"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeFailed"
	// When this field is not set, it means that no resize operation is in progress for the given PVC.
	//
	// A controller that receives PVC update with previously unknown resourceName or ClaimResourceStatus
	// should ignore the update for the purpose it was designed. For example - a controller that
	// only is responsible for resizing capacity of the volume, should ignore PVC updates that change other valid
	// resources associated with PVC.
	// +mapType=granular
	// +optional
	AllocatedResourceStatuses map[ResourceName]ClaimResourceStatus
	// currentVolumeAttributesClassName 是 PVC 当前使用的 VolumeAttributesClass 名称。
	// 未设置时，表示此 PersistentVolumeClaim 未应用任何 VolumeAttributeClass
	// +featureGate=VolumeAttributesClass
	// +optional
	CurrentVolumeAttributesClassName *string
	// ModifyVolumeStatus 表示 ControllerModifyVolume 操作的状态对象。
	// 未设置时，表示没有正在尝试的 ModifyVolume 操作。
	// +featureGate=VolumeAttributesClass
	// +optional
	ModifyVolumeStatus *ModifyVolumeStatus
	// healthStatus contains the latest controller-reported health information
	// for the volume bound to this claim.
	// +featureGate=CSIVolumeHealth
	// +optional
	HealthStatus *VolumeHealthStatus
}

// PersistentVolumeAccessMode defines various access modes for PV.
type PersistentVolumeAccessMode string

// These are the valid values for PersistentVolumeAccessMode
const (
	// can be mounted read/write mode to exactly 1 host
	ReadWriteOnce PersistentVolumeAccessMode = "ReadWriteOnce"
	// 可以以只读模式挂载到多台主机
	ReadOnlyMany PersistentVolumeAccessMode = "ReadOnlyMany"
	// 可以以读写模式挂载到多台主机
	ReadWriteMany PersistentVolumeAccessMode = "ReadWriteMany"
	// 可以以读写模式挂载到恰好 1 个 Pod
	// 不能与其他访问模式组合使用
	ReadWriteOncePod PersistentVolumeAccessMode = "ReadWriteOncePod"
)

// PersistentVolumePhase defines the phase in which a PV is
type PersistentVolumePhase string

// These are the valid values for PersistentVolumePhase
const (
	// used for PersistentVolumes that are not available
	VolumePending PersistentVolumePhase = "Pending"
	// used for PersistentVolumes that are not yet bound
	// Available volumes are held by the binder and matched to PersistentVolumeClaims
	VolumeAvailable PersistentVolumePhase = "Available"
	// used for PersistentVolumes that are bound
	VolumeBound PersistentVolumePhase = "Bound"
	// used for PersistentVolumes where the bound PersistentVolumeClaim was deleted
	// released volumes must be recycled before becoming available again
	// this phase is used by the persistent volume claim binder to signal to another process to reclaim the resource
	VolumeReleased PersistentVolumePhase = "Released"
	// 用于从声明释放后未能正确回收或删除的持久卷
	VolumeFailed PersistentVolumePhase = "Failed"
)

// PersistentVolumeClaimPhase defines the phase of PV claim
type PersistentVolumeClaimPhase string

// These are the valid value for PersistentVolumeClaimPhase
const (
	// 用于尚未绑定的持久卷声明
	ClaimPending PersistentVolumeClaimPhase = "Pending"
	// used for PersistentVolumeClaims that are bound
	ClaimBound PersistentVolumeClaimPhase = "Bound"
	// used for PersistentVolumeClaims that lost their underlying
	// PersistentVolume. The claim was bound to a PersistentVolume and this
	// volume does not exist any longer and all data on it was lost.
	ClaimLost PersistentVolumeClaimPhase = "Lost"
)

// HostPathType defines the type of host path for PV
type HostPathType string

// These are the valid values for HostPathType
const (
	// 为向后兼容，未设置时留空
	HostPathUnset HostPathType = ""
	// 若给定路径下不存在任何内容，将按需创建一个空目录，
	// 文件模式为 0755，且与 Kubelet 具有相同的组和所有权。
	HostPathDirectoryOrCreate HostPathType = "DirectoryOrCreate"
	// 给定路径下必须存在一个目录
	HostPathDirectory HostPathType = "Directory"
	// 若给定路径下不存在任何内容，将按需创建一个空文件，
	// 文件模式为 0644，且与 Kubelet 具有相同的组和所有权。
	HostPathFileOrCreate HostPathType = "FileOrCreate"
	// 给定路径下必须存在一个文件
	HostPathFile HostPathType = "File"
	// 给定路径下必须存在一个 UNIX socket
	HostPathSocket HostPathType = "Socket"
	// 给定路径下必须存在一个字符设备
	HostPathCharDev HostPathType = "CharDevice"
	// 给定路径下必须存在一个块设备
	HostPathBlockDev HostPathType = "BlockDevice"
)

// HostPathVolumeSource represents a host path mapped into a pod.
// 主机路径卷不支持所有权管理或 SELinux 重标记。
type HostPathVolumeSource struct {
	// If the path is a symlink, it will follow the link to the real path.
	Path string
	// Defaults to ""
	Type *HostPathType
}

// EmptyDirVolumeSource represents an empty directory for a pod.
// 空目录卷支持所有权管理和 SELinux 重标记。
type EmptyDirVolumeSource struct {
	// TODO: Longer term we want to represent the selection of underlying
	// media more like a scheduling problem - user says what traits they
	// need, we give them a backing store that satisfies that.  For now
	// this will cover the most common needs.
	// Optional: what type of storage medium should back this directory.
	// 默认为 ""，表示使用节点的默认介质。
	// +optional
	Medium StorageMedium
	// Total amount of local storage required for this EmptyDir volume.
	// The size limit is also applicable for memory medium.
	// The maximum usage on memory medium EmptyDir would be the minimum value between
	// the SizeLimit specified here and the sum of memory limits of all containers in a pod.
	// The default is nil which means that the limit is undefined.
	// More info: https://kubernetes.io/docs/concepts/storage/volumes#emptydir
	// +optional
	SizeLimit *resource.Quantity
	// mode specifies the permission bits for the emptyDir directory, in numeric
	// notation (e.g., 0755, 01777). Must be a value between 0000 and 01777.
	// If not specified, defaults to 0777.
	// This might be in conflict with other options that affect the file
	// mode, like fsGroup. If fsGroup is specified, the fsGroup permissions
	// will override the mode specified here.
	// This field has no effect on Windows.
	// +optional
	Mode *int32
}

// StorageMedium 定义卷的存储分配方式。
type StorageMedium string

// These are the valid value for StorageMedium
const (
	StorageMediumDefault         StorageMedium = ""           // use whatever the default is for the node
	StorageMediumMemory          StorageMedium = "Memory"     // use memory (tmpfs)
	StorageMediumHugePages       StorageMedium = "HugePages"  // use hugepages
	StorageMediumHugePagesPrefix StorageMedium = "HugePages-" // 完整介质表示法 HugePages-<size> 的前缀
)

// Protocol 定义容器端口等所使用的网络协议。
type Protocol string

const (
	// ProtocolTCP 表示 TCP 协议。
	ProtocolTCP Protocol = "TCP"
	// ProtocolUDP 表示 UDP 协议。
	ProtocolUDP Protocol = "UDP"
	// ProtocolSCTP 表示 SCTP 协议。
	ProtocolSCTP Protocol = "SCTP"
)

// GCEPersistentDiskVolumeSource represents a Persistent Disk resource in Google Compute Engine.
//
// A GCE PD must exist before mounting to a container. The disk must
// also be in the same GCE project and zone as the kubelet. A GCE PD
// can only be mounted as read/write once or read-only many times. GCE
// PDs support ownership management and SELinux relabeling.
type GCEPersistentDiskVolumeSource struct {
	// Unique name of the PD resource. Used to identify the disk in GCE
	PDName string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: Partition on the disk to mount.
	// If omitted, kubelet will attempt to mount the device name.
	// Ex. For /dev/sda1, this field is "1", for /dev/sda, this field is 0 or empty.
	// +optional
	Partition int32
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// ISCSIVolumeSource represents an ISCSI disk.
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIVolumeSource struct {
	// Required: iSCSI target portal
	// the portal is either an IP or ip_addr:port if port is other than default (typically TCP ports 860 and 3260)
	// +optional
	TargetPortal string
	// Required:  target iSCSI Qualified Name
	// +optional
	IQN string
	// Required: iSCSI target lun number
	// +optional
	Lun int32
	// Optional: Defaults to 'default' (tcp). iSCSI interface name that uses an iSCSI transport.
	// +optional
	ISCSIInterface string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// Optional: list of iSCSI target portal ips for high availability.
	// the portal is either an IP or ip_addr:port if port is other than default (typically TCP ports 860 and 3260)
	// +optional
	Portals []string
	// Optional: whether support iSCSI Discovery CHAP authentication
	// +optional
	DiscoveryCHAPAuth bool
	// Optional: whether support iSCSI Session CHAP authentication
	// +optional
	SessionCHAPAuth bool
	// Optional: CHAP secret for iSCSI target and initiator authentication.
	// The secret is used if either DiscoveryCHAPAuth or SessionCHAPAuth is true
	// +optional
	SecretRef *LocalObjectReference
	// Optional: Custom initiator name per volume.
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name> will be created for the connection.
	// +optional
	InitiatorName *string
}

// ISCSIPersistentVolumeSource 表示 ISCSI 磁盘。
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIPersistentVolumeSource struct {
	// Required: iSCSI target portal
	// the portal is either an IP or ip_addr:port if port is other than default (typically TCP ports 860 and 3260)
	// +optional
	TargetPortal string
	// Required:  target iSCSI Qualified Name
	// +optional
	IQN string
	// Required: iSCSI target lun number
	// +optional
	Lun int32
	// Optional: Defaults to 'default' (tcp). iSCSI interface name that uses an iSCSI transport.
	// +optional
	ISCSIInterface string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// Optional: list of iSCSI target portal ips for high availability.
	// the portal is either an IP or ip_addr:port if port is other than default (typically TCP ports 860 and 3260)
	// +optional
	Portals []string
	// Optional: whether support iSCSI Discovery CHAP authentication
	// +optional
	DiscoveryCHAPAuth bool
	// Optional: whether support iSCSI Session CHAP authentication
	// +optional
	SessionCHAPAuth bool
	// Optional: CHAP secret for iSCSI target and initiator authentication.
	// The secret is used if either DiscoveryCHAPAuth or SessionCHAPAuth is true
	// +optional
	SecretRef *SecretReference
	// Optional: Custom initiator name per volume.
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name> will be created for the connection.
	// +optional
	InitiatorName *string
}

// FCVolumeSource represents a Fibre Channel volume.
// Fibre Channel 卷只能以读写方式挂载一次。
// Fibre Channel 卷支持所有权管理和 SELinux 重新标记。
type FCVolumeSource struct {
	// Optional: FC target worldwide names (WWNs)
	// +optional
	TargetWWNs []string
	// Optional: FC target lun number
	// +optional
	Lun *int32
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// TODO: how do we prevent errors in the filesystem from compromising the machine
	// +optional
	FSType string
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// Optional: FC volume World Wide Identifiers (WWIDs)
	// Either WWIDs or TargetWWNs and Lun must be set, but not both simultaneously.
	// +optional
	WWIDs []string
}

// FlexPersistentVolumeSource represents a generic persistent volume resource that is
// provisioned/attached using an exec based plugin.
type FlexPersistentVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". The default filesystem depends on FlexVolume script.
	// +optional
	FSType string
	// Optional: SecretRef is reference to the secret object containing
	// sensitive information to pass to the plugin scripts. This may be
	// empty if no secret object is specified. If the secret object
	// contains more than one secret, all secrets are passed to the plugin
	// scripts.
	// +optional
	SecretRef *SecretReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// Optional: Extra driver options if any.
	// +optional
	Options map[string]string
}

// FlexVolumeSource represents a generic volume resource that is
// provisioned/attached using an exec based plugin.
type FlexVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". The default filesystem depends on FlexVolume script.
	// +optional
	FSType string
	// Optional: SecretRef is reference to the secret object containing
	// sensitive information to pass to the plugin scripts. This may be
	// empty if no secret object is specified. If the secret object
	// contains more than one secret, all secrets are passed to the plugin
	// scripts.
	// +optional
	SecretRef *LocalObjectReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// Optional: Extra driver options if any.
	// +optional
	Options map[string]string
}

// AWSElasticBlockStoreVolumeSource represents a Persistent Disk resource in AWS.
//
// An AWS EBS disk must exist before mounting to a container. The disk
// must also be in the same AWS zone as the kubelet. An AWS EBS disk
// can only be mounted as read/write once. AWS EBS volumes support
// ownership management and SELinux relabeling.
type AWSElasticBlockStoreVolumeSource struct {
	// Unique id of the persistent disk resource. Used to identify the disk in AWS
	VolumeID string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: Partition on the disk to mount.
	// If omitted, kubelet will attempt to mount the device name.
	// Ex. For /dev/sda1, this field is "1", for /dev/sda, this field is 0 or empty.
	// +optional
	Partition int32
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// GitRepoVolumeSource represents a volume that is populated with the contents of a git repository.
// Git 仓库卷不支持所有权管理。
// Git 仓库卷支持 SELinux 重新标记。
//
// DEPRECATED: GitRepo is deprecated. To provision a container with a git repo, mount an
// InitContainer 中，由后者使用 git 克隆仓库，然后将该 EmptyDir 挂载到
// Pod 的容器中。
type GitRepoVolumeSource struct {
	// Repository URL
	Repository string
	// Commit hash, this is optional
	// +optional
	Revision string
	// directory 是目标目录名称。
	// 不得包含或以 '..' 开头。若提供 '.'，卷目录即为 Git 仓库本身。
	// 否则，若指定了该字段，卷将在具有给定名称的子目录中包含 Git 仓库。
	// +optional
	Directory string
	// TODO: Consider credentials here.
}

// SecretVolumeSource adapts a Secret into a volume.
//
// The contents of the target Secret's Data field will be presented in a volume
// as files using the keys in the Data field as the file names.
// Secret volumes support ownership management and SELinux relabeling.
type SecretVolumeSource struct {
	// Name of the secret in the pod's namespace to use.
	// +optional
	SecretName string
	// items 若未指定，所引用 Secret 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 Secret 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// Mode bits to use on created files by default. Must be a value between
	// 0 and 0777.
	// Directories within the path are not affected by this setting.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	DefaultMode *int32
	// Specify whether the Secret or its key must be defined
	// +optional
	Optional *bool
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64
}

// SecretProjection adapts a secret into a projected volume.
//
// The contents of the target Secret's Data field will be presented in a
// projected volume as files using the keys in the Data field as the file names.
// Note that this is identical to a secret volume source without the default
// mode.
type SecretProjection struct {
	LocalObjectReference
	// items 若未指定，所引用 Secret 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 Secret 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// optional 字段指定是否必须定义 Secret 或其键。
	// +optional
	Optional *bool
}

// NFSVolumeSource represents an NFS mount that lasts the lifetime of a pod.
// NFS 卷不支持所有权管理或 SELinux 重新标记。
type NFSVolumeSource struct {
	// server 是 NFS 服务器的主机名或 IP 地址。
	Server string

	// Path is the exported NFS share
	Path string

	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the NFS export to be mounted with read-only permissions
	// +optional
	ReadOnly bool
}

// QuobyteVolumeSource represents a Quobyte mount that lasts the lifetime of a pod.
// Quobyte 卷不支持所有权管理或 SELinux 重新标记。
type QuobyteVolumeSource struct {
	// registry 表示一个或多个 Quobyte Registry 服务，
	// 以 host:port 形式的字符串指定（多个条目以逗号分隔），
	// 作为卷的中央注册中心。
	Registry string

	// volume 是按名称引用已创建 Quobyte 卷的字符串。
	Volume string

	// Defaults to false (read/write). ReadOnly here will force
	// the Quobyte to be mounted with read-only permissions
	// +optional
	ReadOnly bool

	// User to map volume access to
	// Defaults to the root user
	// +optional
	User string

	// group 指定卷访问映射到的组。
	// 默认不指定组。
	// +optional
	Group string

	// tenant 是后端中拥有该 Quobyte 卷的租户。
	// 用于动态配置的 Quobyte 卷，其值由插件设置。
	// +optional
	Tenant string
}

// GlusterfsVolumeSource represents a Glusterfs mount that lasts the lifetime of a pod.
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsVolumeSource struct {
	// Required: EndpointsName is the endpoint name that details Glusterfs topology
	EndpointsName string

	// Required: Path is the Glusterfs volume path
	Path string

	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the Glusterfs to be mounted with read-only permissions
	// +optional
	ReadOnly bool
}

// GlusterfsPersistentVolumeSource represents a Glusterfs mount that lasts the lifetime of a pod.
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsPersistentVolumeSource struct {
	// EndpointsName is the endpoint name that details Glusterfs topology.
	// More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	EndpointsName string

	// path 是 Glusterfs 卷路径。
	// More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	Path string

	// ReadOnly here will force the Glusterfs volume to be mounted with read-only permissions.
	// Defaults to false.
	// More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	ReadOnly bool

	// EndpointsNamespace is the namespace that contains Glusterfs endpoint.
	// If this field is empty, the EndpointNamespace defaults to the same namespace as the bound PVC.
	// More info: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	EndpointsNamespace *string
}

// RBDVolumeSource represents a Rados Block Device mount that lasts the lifetime of a pod.
// RBD 卷支持所有权管理和 SELinux 重标记。
type RBDVolumeSource struct {
	// Required: CephMonitors is a collection of Ceph monitors
	CephMonitors []string
	// Required: RBDImage is the rados image name
	RBDImage string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: RadosPool is the rados pool name,default is rbd
	// +optional
	RBDPool string
	// Optional: RBDUser is the rados user name, default is admin
	// +optional
	RadosUser string
	// Optional: Keyring is the path to key ring for RBDUser, default is /etc/ceph/keyring
	// +optional
	Keyring string
	// Optional: SecretRef is name of the authentication secret for RBDUser, default is nil.
	// +optional
	SecretRef *LocalObjectReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// RBDPersistentVolumeSource represents a Rados Block Device mount that lasts the lifetime of a pod.
// RBD 卷支持所有权管理和 SELinux 重标记。
type RBDPersistentVolumeSource struct {
	// Required: CephMonitors is a collection of Ceph monitors
	CephMonitors []string
	// Required: RBDImage is the rados image name
	RBDImage string
	// Filesystem type to mount.
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// Optional: RadosPool is the rados pool name,default is rbd
	// +optional
	RBDPool string
	// Optional: RBDUser is the rados user name, default is admin
	// +optional
	RadosUser string
	// Optional: Keyring is the path to key ring for RBDUser, default is /etc/ceph/keyring
	// +optional
	Keyring string
	// Optional: SecretRef is reference to the authentication secret for User, default is empty.
	// +optional
	SecretRef *SecretReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// CinderVolumeSource represents a cinder volume resource in Openstack. A Cinder volume
// must exist before mounting to a container. The volume must also be
// in the same region as the kubelet. Cinder volumes support ownership
// management and SELinux relabeling.
type CinderVolumeSource struct {
	// Unique id of the volume used to identify the cinder volume.
	VolumeID string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// Optional: points to a secret object containing parameters used to connect
	// to OpenStack.
	// +optional
	SecretRef *LocalObjectReference
}

// CinderPersistentVolumeSource represents a cinder volume resource in Openstack. A Cinder volume
// must exist before mounting to a container. The volume must also be
// in the same region as the kubelet. Cinder volumes support ownership
// management and SELinux relabeling.
type CinderPersistentVolumeSource struct {
	// Unique id of the volume used to identify the cinder volume.
	VolumeID string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// Optional: points to a secret object containing parameters used to connect
	// to OpenStack.
	// +optional
	SecretRef *SecretReference
}

// CephFSVolumeSource represents a Ceph Filesystem mount that lasts the lifetime of a pod
// CephFS 卷不支持所有权管理或 SELinux 重新标记。
type CephFSVolumeSource struct {
	// monitors 为必填项：Ceph monitor 的集合。
	Monitors []string
	// path 为可选项：用作挂载根目录，而非整个 Ceph 目录树，默认值为 /。
	// +optional
	Path string
	// user 为可选项：rados 用户名，默认值为 admin。
	// +optional
	User string
	// Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret
	// +optional
	SecretFile string
	// Optional: SecretRef is reference to the authentication secret for User, default is empty.
	// +optional
	SecretRef *LocalObjectReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// SecretReference represents a Secret Reference. It has enough information to retrieve secret
// in any namespace
type SecretReference struct {
	// name 在命名空间内唯一，用于引用 Secret 资源。
	// +optional
	Name string
	// namespace 定义了 Secret 名称必须唯一的作用域。
	// +optional
	Namespace string
}

// CephFSPersistentVolumeSource represents a Ceph Filesystem mount that lasts the lifetime of a pod
// CephFS 卷不支持所有权管理或 SELinux 重新标记。
type CephFSPersistentVolumeSource struct {
	// monitors 为必填项：Ceph monitor 的集合。
	Monitors []string
	// path 为可选项：用作挂载根目录，而非整个 Ceph 目录树，默认值为 /。
	// +optional
	Path string
	// user 为可选项：rados 用户名，默认值为 admin。
	// +optional
	User string
	// Optional: SecretFile is the path to key ring for User, default is /etc/ceph/user.secret
	// +optional
	SecretFile string
	// Optional: SecretRef is reference to the authentication secret for User, default is empty.
	// +optional
	SecretRef *SecretReference
	// Optional: Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// FlockerVolumeSource represents a Flocker volume mounted by the Flocker agent.
// datasetName 与 datasetUUID 必须且只能设置其中一个。
// Flocker 卷不支持所有权管理或 SELinux 重新标记。
type FlockerVolumeSource struct {
	// Name of the dataset stored as metadata -> name on the dataset for Flocker
	// should be considered as deprecated
	// +optional
	DatasetName string
	// UUID of the dataset. This is unique identifier of a Flocker dataset
	// +optional
	DatasetUUID string
}

// DownwardAPIVolumeSource 表示包含 downward API 信息的卷。
// downward API 卷支持所有权管理和 SELinux 重新标记。
type DownwardAPIVolumeSource struct {
	// Items is a list of DownwardAPIVolume file
	// +optional
	Items []DownwardAPIVolumeFile
	// Mode bits to use on created files by default. Must be a value between
	// 0 and 0777.
	// Directories within the path are not affected by this setting.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	DefaultMode *int32
	// defaultUser 可选：默认情况下所创建文件的所有者 UID。
	// 仅当条目级别的 user 字段未设置时，才使用 defaultUser 作为回退值。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64
}

// DownwardAPIVolumeFile represents a single file containing information from the downward API
type DownwardAPIVolumeFile struct {
	// Required: Path is  the relative path name of the file to be created. Must not be absolute or contain the '..' path. Must be utf-8 encoded. The first item of the relative path must not start with '..'
	Path string
	// 必填：选择 Pod 的一个字段：目前仅支持 annotations、labels、name、namespace 和 uid。
	// +optional
	FieldRef *ObjectFieldSelector
	// 选择容器的一种资源：目前仅支持资源限制和请求
	// （limits.cpu、limits.memory、requests.cpu 和 requests.memory）。
	// +optional
	ResourceFieldRef *ResourceFieldSelector
	// Optional: mode bits to use on this file, must be a value between 0
	// and 0777. If not specified, the volume defaultMode will be used.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	Mode *int32
	// user 可选：所创建文件的所有者 UID。
	// 若指定，条目级别的 user 字段优先于 defaultUser。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// DownwardAPIProjection represents downward API info for projecting into a projected volume.
// Note that this is identical to a downwardAPI volume source without the default
// mode.
type DownwardAPIProjection struct {
	// Items 是 DownwardAPIVolume 文件的列表。
	// +optional
	Items []DownwardAPIVolumeFile
}

// AzureFileVolumeSource azureFile represents an Azure File Service mount on the host and bind mount to the pod.
type AzureFileVolumeSource struct {
	// the name of secret that contains Azure Storage Account Name and Key
	SecretName string
	// Share Name
	ShareName string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// AzureFilePersistentVolumeSource represents an Azure File Service mount on the host and bind mount to the pod.
type AzureFilePersistentVolumeSource struct {
	// the name of secret that contains Azure Storage Account Name and Key
	SecretName string
	// Share Name
	ShareName string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// the namespace of the secret that contains Azure Storage Account Name and Key
	// default is the same as the Pod
	// +optional
	SecretNamespace *string
}

// VsphereVirtualDiskVolumeSource represents a vSphere volume resource.
type VsphereVirtualDiskVolumeSource struct {
	// Path that identifies vSphere volume vmdk
	VolumePath string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Storage Policy Based Management (SPBM) profile name.
	// +optional
	StoragePolicyName string
	// Storage Policy Based Management (SPBM) profile ID associated with the StoragePolicyName.
	// +optional
	StoragePolicyID string
}

// PhotonPersistentDiskVolumeSource represents a Photon Controller persistent disk resource.
type PhotonPersistentDiskVolumeSource struct {
	// ID that identifies Photon Controller persistent disk
	PdID string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	FSType string
}

// PortworxVolumeSource 表示 Portworx 卷资源。
type PortworxVolumeSource struct {
	// VolumeID uniquely identifies a Portworx volume
	VolumeID string
	// FSType represents the filesystem type to mount
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// AzureDataDiskCachingMode defines the caching mode for Azure data disk
type AzureDataDiskCachingMode string

// AzureDataDiskKind defines the kind of Azure data disk
type AzureDataDiskKind string

// Defines cache mode and kinds for Azure data disk
const (
	AzureDataDiskCachingNone      AzureDataDiskCachingMode = "None"
	AzureDataDiskCachingReadOnly  AzureDataDiskCachingMode = "ReadOnly"
	AzureDataDiskCachingReadWrite AzureDataDiskCachingMode = "ReadWrite"

	AzureSharedBlobDisk    AzureDataDiskKind = "Shared"
	AzureDedicatedBlobDisk AzureDataDiskKind = "Dedicated"
	AzureManagedDisk       AzureDataDiskKind = "Managed"
)

// AzureDiskVolumeSource represents an Azure Data Disk mount on the host and bind mount to the pod.
type AzureDiskVolumeSource struct {
	// The Name of the data disk in the blob storage
	DiskName string
	// The URI of the data disk in the blob storage
	DataDiskURI string
	// Host Caching mode: None, Read Only, Read Write.
	// +optional
	CachingMode *AzureDataDiskCachingMode
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType *string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly *bool
	// kind 的预期值为 Shared（每个存储账户多个 Blob 磁盘）、Dedicated（每个存储账户单个 Blob 磁盘）、
	// Managed（Azure 托管数据磁盘，仅适用于托管可用性集）。默认值为 Shared。
	Kind *AzureDataDiskKind
}

// ScaleIOVolumeSource represents a persistent ScaleIO volume
type ScaleIOVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string
	// system 是 ScaleIO 中配置的存储系统名称。
	System string
	// SecretRef references to the secret for ScaleIO user and other
	// sensitive information. If this is not provided, Login operation will fail.
	SecretRef *LocalObjectReference
	// Flag to enable/disable SSL communication with Gateway, default false
	// +optional
	SSLEnabled bool
	// The name of the ScaleIO Protection Domain for the configured storage.
	// +optional
	ProtectionDomain string
	// The ScaleIO Storage Pool associated with the protection domain.
	// +optional
	StoragePool string
	// Indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned.
	// Default is ThinProvisioned.
	// +optional
	StorageMode string
	// The name of a volume already created in the ScaleIO system
	// that is associated with this volume source.
	VolumeName string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs".
	// Default is "xfs".
	// +optional
	FSType string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// ScaleIOPersistentVolumeSource represents a persistent ScaleIO volume that can be defined
// by a an admin via a storage class, for instance.
type ScaleIOPersistentVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string
	// system 是 ScaleIO 中配置的存储系统名称。
	System string
	// SecretRef references to the secret for ScaleIO user and other
	// sensitive information. If this is not provided, Login operation will fail.
	SecretRef *SecretReference
	// Flag to enable/disable SSL communication with Gateway, default false
	// +optional
	SSLEnabled bool
	// The name of the ScaleIO Protection Domain for the configured storage.
	// +optional
	ProtectionDomain string
	// The ScaleIO Storage Pool associated with the protection domain.
	// +optional
	StoragePool string
	// Indicates whether the storage for a volume should be ThickProvisioned or ThinProvisioned.
	// Default is ThinProvisioned.
	// +optional
	StorageMode string
	// The name of a volume created in the ScaleIO system
	// that is associated with this volume source.
	VolumeName string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs".
	// Default is "xfs".
	// +optional
	FSType string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
}

// StorageOSVolumeSource represents a StorageOS persistent volume resource.
type StorageOSVolumeSource struct {
	// VolumeName is the human-readable name of the StorageOS volume.  Volume
	// names are only unique within a namespace.
	VolumeName string
	// VolumeNamespace specifies the scope of the volume within StorageOS.  If no
	// namespace is specified then the Pod's namespace will be used.  This allows the
	// Kubernetes name scoping to be mirrored within StorageOS for tighter integration.
	// Set VolumeName to any name to override the default behaviour.
	// Set to "default" if you are not using namespaces within StorageOS.
	// Namespaces that do not pre-exist within StorageOS will be created.
	// +optional
	VolumeNamespace string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// SecretRef specifies the secret to use for obtaining the StorageOS API
	// credentials.  If not specified, default values will be attempted.
	// +optional
	SecretRef *LocalObjectReference
}

// StorageOSPersistentVolumeSource represents a StorageOS persistent volume resource.
type StorageOSPersistentVolumeSource struct {
	// VolumeName is the human-readable name of the StorageOS volume.  Volume
	// names are only unique within a namespace.
	VolumeName string
	// VolumeNamespace specifies the scope of the volume within StorageOS.  If no
	// namespace is specified then the Pod's namespace will be used.  This allows the
	// Kubernetes name scoping to be mirrored within StorageOS for tighter integration.
	// Set VolumeName to any name to override the default behaviour.
	// Set to "default" if you are not using namespaces within StorageOS.
	// Namespaces that do not pre-exist within StorageOS will be created.
	// +optional
	VolumeNamespace string
	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". Implicitly inferred to be "ext4" if unspecified.
	// +optional
	FSType string
	// Defaults to false (read/write). ReadOnly here will force
	// the ReadOnly setting in VolumeMounts.
	// +optional
	ReadOnly bool
	// SecretRef specifies the secret to use for obtaining the StorageOS API
	// credentials.  If not specified, default values will be attempted.
	// +optional
	SecretRef *ObjectReference
}

// ConfigMapVolumeSource adapts a ConfigMap into a volume.
//
// The contents of the target ConfigMap's Data field will be presented in a
// volume as files using the keys in the Data field as the file names, unless
// the items element is populated with specific mappings of keys to paths.
// ConfigMap volumes support ownership management and SELinux relabeling.
type ConfigMapVolumeSource struct {
	LocalObjectReference
	// items 若未指定，所引用 ConfigMap 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 ConfigMap 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// Mode bits to use on created files by default. Must be a value between
	// 0 and 0777.
	// Directories within the path are not affected by this setting.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	DefaultMode *int32
	// optional 指定是否必须定义 ConfigMap 或其键。
	// +optional
	Optional *bool
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64
}

// ConfigMapProjection adapts a ConfigMap into a projected volume.
//
// The contents of the target ConfigMap's Data field will be presented in a
// projected volume as files using the keys in the Data field as the file names,
// unless the items element is populated with specific mappings of keys to paths.
// Note that this is identical to a configmap volume source without the default
// mode.
type ConfigMapProjection struct {
	LocalObjectReference
	// items 若未指定，所引用 ConfigMap 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 ConfigMap 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// optional 指定是否必须定义 ConfigMap 或其键。
	// +optional
	Optional *bool
}

// ServiceAccountTokenProjection represents a projected service account token
// volume. This projection can be used to insert a service account token into
// the pods runtime filesystem for use against APIs (Kubernetes API Server or
// otherwise).
type ServiceAccountTokenProjection struct {
	// audience 是令牌的目标受众。令牌接收方必须使用令牌受众中指定的标识符表明自身身份，
	// 否则应拒绝该令牌。受众默认为 apiserver 的标识符。
	Audience string
	// ExpirationSeconds is the requested duration of validity of the service
	// account token. As the token approaches expiration, the kubelet volume
	// plugin will proactively rotate the service account token. The kubelet will
	// start trying to rotate the token if the token is older than 80 percent of
	// its time to live or if the token is older than 24 hours.Defaults to 1 hour
	// and must be at least 10 minutes.
	// +optional
	ExpirationSeconds *int64
	// path 是令牌投影文件的路径，相对于挂载点。
	Path string
	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// ClusterTrustBundleProjection allows a pod to access the
// `.spec.trustBundle` field of a ClusterTrustBundle object in an auto-updating
// file.
type ClusterTrustBundleProjection struct {
	// Select a single ClusterTrustBundle by object name.   Mutually-exclusive
	// with SignerName and LabelSelector.
	Name *string

	// Select all ClusterTrustBundles for this signer that match LabelSelector.
	// Mutually-exclusive with Name.
	SignerName *string

	// Select all ClusterTrustBundles that match this LabelSelecotr.
	// Mutually-exclusive with Name.
	LabelSelector *metav1.LabelSelector

	// Block pod startup if the selected ClusterTrustBundle(s) aren't available?
	Optional *bool

	// 写入 bundle 的路径，相对于卷根目录。
	Path string

	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// PodCertificateProjection provides a private key and X.509 certificate in
// a combined file.
type PodCertificateProjection struct {
	// Kubelet's generated CSRs will be addressed to this signer.
	SignerName string

	// The type of keypair Kubelet will generate for the pod.
	//
	// Valid values are "RSA3072", "RSA4096", "ECDSAP256", "ECDSAP384",
	// "ECDSAP521", and "ED25519".
	KeyType string

	// maxExpirationSeconds is the maximum lifetime permitted for the
	// certificate.
	//
	// Kubelet copies this value verbatim into the PodCertificateRequests it
	// generates for this projection.
	//
	// If omitted, kube-apiserver will set it to 86400(24 hours). kube-apiserver
	// will reject values shorter than 3600 (1 hour).  The maximum allowable
	// value is 7862400 (91 days).
	//
	// The signer implementation is then free to issue a certificate with any
	// lifetime *shorter* than MaxExpirationSeconds, but no shorter than 3600
	// seconds (1 hour).  This constraint is enforced by kube-apiserver.
	// `kubernetes.io` signers will never issue certificates with a lifetime
	// longer than 24 hours.
	MaxExpirationSeconds *int32

	// 将凭据包写入投影卷中的此路径。
	// 凭据包是单个文件，包含多个 PEM 块。第一个 PEM 块是 PRIVATE KEY 块，
	// 其中包含 PKCS#8 私钥。
	// 其余块为 CERTIFICATE 块，包含签名者签发的证书链（叶子证书及所有中间证书）。
	// 使用 credentialBundlePath 可让 Pod 中的应用代码通过一次原子读取获得一致的
	// 私钥和证书链。若将它们投影到不同文件，应用代码还需额外检查叶子证书是否签发给该私钥。
	CredentialBundlePath string

	// Write the key at this path in the projected volume.
	//
	// When using keyPath and certificateChainPath, your application needs to check
	// that the key and leaf certificate are consistent, because it is possible to
	// read the files mid-rotation.
	KeyPath string

	// Write the certificate chain at this path in the projected volume.
	CertificateChainPath string

	// userAnnotations allow pod authors to pass additional information to
	// the signer implementation.  Kubernetes does not restrict or validate this
	// metadata in any way.
	//
	// These values are copied verbatim into the `spec.unverifiedUserAnnotations` field of
	// the PodCertificateRequest objects that Kubelet creates.
	//
	// Entries are subject to the same validation as object metadata annotations,
	// with the addition that all keys must be domain-prefixed. No restrictions
	// are placed on values, except an overall size limitation on the entire field.
	//
	// Signers should document the keys and values they support. Signers should
	// deny requests that contain keys they do not recognize.
	UserAnnotations map[string]string

	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// ProjectedVolumeSource represents a projected volume source
type ProjectedVolumeSource struct {
	// list of volume projections
	Sources []VolumeProjection
	// Mode bits to use on created files by default. Must be a value between
	// 0 and 0777.
	// Directories within the path are not affected by this setting.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	DefaultMode *int32
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64
}

// VolumeProjection that may be projected along with other supported volume types
type VolumeProjection struct {
	// all types below are the supported types for projection into the same volume

	// secret 是要投影的 Secret 数据的相关信息。
	Secret *SecretProjection
	// downwardAPI 是要投影的 downwardAPI 数据的相关信息。
	DownwardAPI *DownwardAPIProjection
	// information about the configMap data to project
	ConfigMap *ConfigMapProjection
	// information about the serviceAccountToken data to project
	ServiceAccountToken *ServiceAccountTokenProjection
	// information about the ClusterTrustBundle data to project
	ClusterTrustBundle *ClusterTrustBundleProjection
	// information about the pod certificate to project.
	PodCertificate *PodCertificateProjection
}

// KeyToPath maps a string key to a path within a volume.
type KeyToPath struct {
	// The key to project.
	Key string

	// path 是键所映射到的文件的相对路径。
	// 不能是绝对路径。
	// 不能包含路径元素 '..'。
	// 不能以字符串 '..' 开头。
	Path string
	// Optional: mode bits to use on this file, should be a value between 0
	// and 0777. If not specified, the volume defaultMode will be used.
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	Mode *int32
	// user 是可选的：所创建文件的所有者 UID。
	// 如果指定，条目级别的 user 字段优先于 defaultUser。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// LocalVolumeSource represents directly-attached storage with node affinity
type LocalVolumeSource struct {
	// path 是节点上卷的完整路径。
	// 它可以是目录或块设备（磁盘、分区等）。
	Path string

	// Filesystem type to mount.
	// It applies only when the Path is a block device.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs". The default value is to auto-select a filesystem if unspecified.
	// +optional
	FSType *string
}

// CSIPersistentVolumeSource represents storage that is managed by an external CSI volume driver.
type CSIPersistentVolumeSource struct {
	// driver 是要用于此卷的驱动名称。
	// 必填。
	Driver string

	// VolumeHandle is the unique volume name returned by the CSI volume
	// plugin’s CreateVolume to refer to the volume on all subsequent calls.
	// Required.
	VolumeHandle string

	// Optional: The value to pass to ControllerPublishVolumeRequest.
	// Defaults to false (read/write).
	// +optional
	ReadOnly bool

	// Filesystem type to mount.
	// Must be a filesystem type supported by the host operating system.
	// Ex. "ext4", "xfs", "ntfs".
	// +optional
	FSType string

	// Attributes of the volume to publish.
	// +optional
	VolumeAttributes map[string]string

	// ControllerPublishSecretRef is a reference to the secret object containing
	// sensitive information to pass to the CSI driver to complete the CSI
	// ControllerPublishVolume and ControllerUnpublishVolume calls.
	// This field is optional, and may be empty if no secret is required. If the
	// secret object contains more than one secret, all secrets are passed.
	// +optional
	ControllerPublishSecretRef *SecretReference

	// NodeStageSecretRef is a reference to the secret object containing sensitive
	// information to pass to the CSI driver to complete the CSI NodeStageVolume
	// and NodeStageVolume and NodeUnstageVolume calls.
	// This field is optional, and may be empty if no secret is required. If the
	// secret object contains more than one secret, all secrets are passed.
	// +optional
	NodeStageSecretRef *SecretReference

	// NodePublishSecretRef is a reference to the secret object containing
	// sensitive information to pass to the CSI driver to complete the CSI
	// NodePublishVolume and NodeUnpublishVolume calls.
	// This field is optional, and may be empty if no secret is required. If the
	// secret object contains more than one secret, all secrets are passed.
	// +optional
	NodePublishSecretRef *SecretReference

	// ControllerExpandSecretRef is a reference to the secret object containing
	// sensitive information to pass to the CSI driver to complete the CSI
	// ControllerExpandVolume call.
	// This field is optional, and may be empty if no secret is required. If the
	// secret object contains more than one secret, all secrets are passed.
	// +optional
	ControllerExpandSecretRef *SecretReference

	// NodeExpandSecretRef is a reference to the secret object containing
	// sensitive information to pass to the CSI driver to complete the CSI
	// NodeExpandVolume call.
	// This field is optional, may be omitted if no secret is required. If the
	// secret object contains more than one secret, all secrets are passed.
	// +optional
	NodeExpandSecretRef *SecretReference
}

// CSIVolumeSource represents a source location of a volume to mount, managed by an external CSI driver
type CSIVolumeSource struct {
	// driver 是处理此卷的 CSI 驱动名称。
	// 请向管理员咨询集群中注册的正确名称。
	Driver string

	// Specifies a read-only configuration for the volume.
	// Defaults to false (read/write).
	// +optional
	ReadOnly *bool

	// Filesystem type to mount. Ex. "ext4", "xfs", "ntfs".
	// If not provided, the empty value is passed to the associated CSI driver
	// which will determine the default filesystem to apply.
	// +optional
	FSType *string

	// VolumeAttributes stores driver-specific properties that are passed to the CSI
	// driver. Consult your driver's documentation for supported values.
	// +optional
	VolumeAttributes map[string]string

	// NodePublishSecretRef is a reference to the secret object containing
	// sensitive information to pass to the CSI driver to complete the CSI
	// NodePublishVolume and NodeUnpublishVolume calls.
	// This field is optional, and  may be empty if no secret is required. If the
	// secret object contains more than one secret, all secret references are passed.
	// +optional
	NodePublishSecretRef *LocalObjectReference
}

// EphemeralVolumeSource represents an ephemeral volume that is handled by a normal storage driver.
type EphemeralVolumeSource struct {
	// VolumeClaimTemplate will be used to create a stand-alone PVC to provision the volume.
	// The pod in which this EphemeralVolumeSource is embedded will be the
	// owner of the PVC, i.e. the PVC will be deleted together with the
	// pod.  The name of the PVC will be `<pod name>-<volume name>` where
	// `<volume name>` is the name from the `PodSpec.Volumes` array
	// entry. Pod validation will reject the pod if the concatenated name
	// is not valid for a PVC (for example, too long).
	//
	// An existing PVC with that name that is not owned by the pod
	// will *not* be used for the pod to avoid using an unrelated
	// volume by mistake. Starting the pod is then blocked until
	// the unrelated PVC is removed. If such a pre-created PVC is
	// meant to be used by the pod, the PVC has to updated with an
	// owner reference to the pod once the pod exists. Normally
	// this should not be necessary, but it may be useful when
	// manually reconstructing a broken cluster.
	//
	// This field is read-only and no changes will be made by Kubernetes
	// to the PVC after it has been created.
	//
	// Required, must not be nil.
	VolumeClaimTemplate *PersistentVolumeClaimTemplate
}

// PersistentVolumeClaimTemplate is used to produce
// PersistentVolumeClaim 对象。
type PersistentVolumeClaimTemplate struct {
	// ObjectMeta may contain labels and annotations that will be copied into the PVC
	// when creating it. No other fields are allowed and will be rejected during
	// validation.
	// +optional
	metav1.ObjectMeta

	// PersistentVolumeClaim 的规格。全部内容会原样复制到
	// 由此模板创建的 PVC 中。PersistentVolumeClaim 中的字段
	// 在此处同样有效。
	Spec PersistentVolumeClaimSpec
}

// ContainerPort represents a network port in a single container
type ContainerPort struct {
	// Optional: If specified, this must be an IANA_SVC_NAME  Each named port
	// in a pod must have a unique name.
	// +optional
	Name string
	// Optional: If specified, this must be a valid port number, 0 < x < 65536.
	// 如果指定了 HostNetwork，此值必须与 ContainerPort 一致。
	// +optional
	HostPort int32
	// Required: This must be a valid port number, 0 < x < 65536.
	ContainerPort int32
	// Required: Supports "TCP", "UDP" and "SCTP"
	// +optional
	Protocol Protocol
	// Optional: What host IP to bind the external port to.
	// +optional
	HostIP string
}

// VolumeMount 描述将卷挂载到容器中的一个挂载点。
type VolumeMount struct {
	// Required: This must match the Name of a Volume [above].
	Name string
	// Optional: Defaults to false (read-write).
	// +optional
	ReadOnly bool
	// RecursiveReadOnly 指定是否以递归方式处理只读挂载。
	// 如果 ReadOnly 为 false，此字段无意义且必须不指定。
	// 如果 ReadOnly 为 true 且此字段设为 Disabled，则挂载不会递归只读。
	// 如果设为 IfPossible，且容器运行时支持，则挂载会递归只读。
	// 如果设为 Enabled，且容器运行时支持，则挂载会递归只读；
	// 否则 Pod 不会启动，并会生成指明原因的错误。
	// 如果此字段设为 IfPossible 或 Enabled，MountPropagation 必须设为
	// None（或不指定，默认为 None）。
	// 如果未指定此字段，则视为等同于 Disabled。
	// +optional
	RecursiveReadOnly *RecursiveReadOnlyMode
	// Required. If the path is not an absolute path (e.g. some/path) it
	// will be prepended with the appropriate root prefix for the operating
	// system.  On Linux this is '/', on Windows this is 'C:\'.
	MountPath string
	// Path within the volume from which the container's volume should be mounted.
	// 默认为 ""（卷的根目录）。
	// +optional
	SubPath string
	// mountPropagation 决定挂载如何从主机传播到容器，以及反向传播。
	// 未设置时使用 MountPropagationNone。
	// 此字段在 1.10 中为 beta。
	// 当 RecursiveReadOnly 设为 IfPossible 或 Enabled 时，MountPropagation 必须为 None 或不指定
	// （默认为 None）。
	// +optional
	MountPropagation *MountPropagationMode
	// 卷内要挂载到容器的扩展路径。
	// 行为类似 SubPath，但环境变量引用 $(VAR_NAME) 会使用容器的环境变量展开。
	// 默认为 ""（卷的根目录）。
	// SubPathExpr 与 SubPath 互斥。
	// +optional
	SubPathExpr string
	// bindMountOptions 是挂载此卷到容器时要应用的额外绑定挂载选项列表。
	// 允许的值为 noexec、nodev 和 nosuid。这些是 Linux 挂载选项，
	// 在 Windows 节点上无效。
	// 此字段不支持镜像卷。
	// 这是 alpha 字段，需要启用 VolumeBindMountOptions 特性门控。
	// +featureGate=VolumeBindMountOptions
	// +optional
	BindMountOptions []string
}

// BindMountOption 定义支持的绑定挂载选项。
type BindMountOption string

const (
	// BindMountOptionNoExec 阻止在挂载的卷上执行二进制文件。
	BindMountOptionNoExec BindMountOption = "noexec"
	// BindMountOptionNoDev 忽略挂载的卷上的设备特殊文件。
	BindMountOptionNoDev BindMountOption = "nodev"
	// BindMountOptionNoSUID 忽略挂载的卷上的 set-user-identifier 或 set-group-identifier 位。
	BindMountOptionNoSUID BindMountOption = "nosuid"
)

// MountPropagationMode describes mount propagation.
type MountPropagationMode string

const (
	// MountPropagationNone 表示容器中的卷
	// 不会接收来自主机或其他容器的新挂载，容器内挂载的文件系统
	// 也不会传播到主机或其他容器。
	// 注意此模式对应 Linux 术语中的 "private"。
	MountPropagationNone MountPropagationMode = "None"
	// MountPropagationHostToContainer 表示容器中的卷
	// 会接收来自主机或其他容器的新挂载，但容器内挂载的文件系统
	// 不会传播到主机或其他容器。
	// 注意此模式会递归应用于卷中的所有挂载
	// （Linux 术语中的 "rslave"）。
	MountPropagationHostToContainer MountPropagationMode = "HostToContainer"
	// MountPropagationBidirectional 表示容器中的卷
	// 会接收来自主机或其他容器的新挂载，其自身的挂载
	// 也会从容器传播到主机或其他容器。
	// 注意此模式会递归应用于卷中的所有挂载
	// （Linux 术语中的 "rshared"）。
	MountPropagationBidirectional MountPropagationMode = "Bidirectional"
)

// RecursiveReadOnlyMode 描述递归只读模式。
type RecursiveReadOnlyMode string

const (
	// RecursiveReadOnlyDisabled 禁用递归只读模式。
	RecursiveReadOnlyDisabled RecursiveReadOnlyMode = "Disabled"
	// RecursiveReadOnlyIfPossible 在可能的情况下启用递归只读模式。
	RecursiveReadOnlyIfPossible RecursiveReadOnlyMode = "IfPossible"
	// RecursiveReadOnlyEnabled 启用递归只读模式，否则报错。
	RecursiveReadOnlyEnabled RecursiveReadOnlyMode = "Enabled"
)

// VolumeDevice describes a mapping of a raw block device within a container.
type VolumeDevice struct {
	// name 必须与 Pod 中某个 persistentVolumeClaim 的名称匹配。
	Name string
	// devicePath 是设备将映射到的容器内路径。
	DevicePath string
}

// EnvVar 表示容器中存在的一个环境变量。
type EnvVar struct {
	// 环境变量的名称。
	// 可以包含除 '=' 以外的任意可打印 ASCII 字符。
	Name string
	// Optional: no more than one of the following may be specified.
	// Optional: Defaults to ""; variable references $(VAR_NAME) are expanded
	// using the previously defined environment variables in the container and
	// any service environment variables.  If a variable cannot be resolved,
	// the reference in the input string will be unchanged.  Double $$ are
	// reduced to a single $, which allows for escaping the $(VAR_NAME)
	// syntax: i.e. "$$(VAR_NAME)" will produce the string literal
	// "$(VAR_NAME)".  Escaped references will never be expanded,
	// regardless of whether the variable exists or not.
	// +optional
	Value string
	// Optional: Specifies a source the value of this var should come from.
	// +optional
	ValueFrom *EnvVarSource
}

// EnvVarSource 表示 EnvVar 值的来源。
// Only one of its fields may be set.
type EnvVarSource struct {
	// Selects a field of the pod: supports metadata.name, metadata.namespace, `metadata.labels['<KEY>']`, `metadata.annotations['<KEY>']`,
	// metadata.uid, spec.nodeName, spec.serviceAccountName, status.hostIP, status.podIP, status.podIPs.
	// +optional
	FieldRef *ObjectFieldSelector
	// 选择容器的一个资源：目前仅支持资源限制和请求
	// （limits.cpu、limits.memory、limits.ephemeral-storage、requests.cpu、requests.memory 和 requests.ephemeral-storage）。
	// +optional
	ResourceFieldRef *ResourceFieldSelector
	// 选择 ConfigMap 的一个键。
	// +optional
	ConfigMapKeyRef *ConfigMapKeySelector
	// 选择 Pod 命名空间中某个 Secret 的一个键。
	// +optional
	SecretKeyRef *SecretKeySelector
	// FileKeyRef 选择 env 文件中的一个键。
	// 需要启用 EnvFiles 特性门控。
	// +featureGate=EnvFiles
	// +optional
	FileKeyRef *FileKeySelector
}

// FileKeySelector 选择 env 文件中的一个键。
type FileKeySelector struct {
	// 包含该 env 文件的卷挂载的名称。
	// +required
	VolumeName string
	// The path within the volume from which to select the file.
	// 必须是相对路径，且不能包含 '..' 路径或以 '..' 开头。
	// +required
	Path string
	// env 文件中的键。无效的键会阻止 Pod 启动。
	// 来源中定义的键可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// 在 EnvFiles 特性门控的 Alpha 阶段，键的长度限制为 128 个字符。
	// +required
	Key string
	// 指定是否必须定义文件或其键。如果文件或键不存在，
	// 则不会发布该环境变量。
	// 如果 optional 设为 true 且指定的键不存在，
	// 则不会在 Pod 的容器中设置该环境变量。
	// 如果 optional 设为 false 且指定的键不存在，
	// 则在创建 Pod 时会返回错误。
	// +optional
	// +default=false
	Optional *bool
}

// ObjectFieldSelector 选择对象的一个带 API 版本的字段。
type ObjectFieldSelector struct {
	// Required: Version of the schema the FieldPath is written in terms of.
	// If no value is specified, it will be defaulted to the APIVersion of the
	// enclosing object.
	APIVersion string
	// Required: Path of the field to select in the specified API version
	FieldPath string
}

// ResourceFieldSelector 表示容器资源（cpu、memory）及其输出格式。
type ResourceFieldSelector struct {
	// Container name: required for volumes, optional for env vars
	// +optional
	ContainerName string
	// Required: resource to select
	Resource string
	// Specifies the output format of the exposed resources, defaults to "1"
	// +optional
	Divisor resource.Quantity
}

// ConfigMapKeySelector selects a key from a ConfigMap.
type ConfigMapKeySelector struct {
	// 要选择的 ConfigMap。
	LocalObjectReference
	// The key to select.
	Key string
	// Specify whether the ConfigMap or its key must be defined
	// +optional
	Optional *bool
}

// SecretKeySelector 选择 Secret 的一个键。
type SecretKeySelector struct {
	// 要选择 Pod 命名空间中 Secret 的名称。
	LocalObjectReference
	// 要选择的 Secret 的键。必须是有效的 Secret 键。
	Key string
	// Specify whether the Secret or its key must be defined
	// +optional
	Optional *bool
}

// EnvFromSource 表示一组 ConfigMap 或 Secret 的来源。
type EnvFromSource struct {
	// 可选：在每个环境变量名称前添加的前缀文本。
	// 可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// +optional
	Prefix string
	// 要选择的 ConfigMap。
	// +optional
	ConfigMapRef *ConfigMapEnvSource
	// 要选择的 Secret。
	// +optional
	SecretRef *SecretEnvSource
}

// ConfigMapEnvSource selects a ConfigMap to populate the environment
// variables with.
//
// The contents of the target ConfigMap's Data field will represent the
// key-value pairs as environment variables.
type ConfigMapEnvSource struct {
	// 要选择的 ConfigMap。
	LocalObjectReference
	// Specify whether the ConfigMap must be defined
	// +optional
	Optional *bool
}

// SecretEnvSource selects a Secret to populate the environment
// variables with.
//
// The contents of the target Secret's Data field will represent the
// key-value pairs as environment variables.
type SecretEnvSource struct {
	// 要选择的 Secret。
	LocalObjectReference
	// Specify whether the Secret must be defined
	// +optional
	Optional *bool
}

// HTTPHeader 描述要在 HTTP 探针中使用的自定义请求头。
type HTTPHeader struct {
	// The header field name.
	// 输出时会被规范化，因此大小写不同的名称会被视为同一个请求头。
	Name string
	// The header field value
	Value string
}

// HTTPProtocol selects the wire protocol for the HTTP probe,
// independently of the URI scheme.
type HTTPProtocol string

const (
	// HTTPProtocolHTTP1 使用 HTTP/1.1（现有默认行为）。
	HTTPProtocolHTTP1 HTTPProtocol = "HTTP1"
	// HTTPProtocolHTTP2 使用 HTTP/2。
	// 目前仅支持明文且预先知晓（h2c）的方式，且必须与 HTTP scheme 一起使用。
	HTTPProtocolHTTP2 HTTPProtocol = "HTTP2"
)

// HTTPGetAction 描述基于 HTTP Get 请求的动作。
type HTTPGetAction struct {
	// Optional: Path to access on the HTTP server.
	// +optional
	Path string
	// Required: Name or number of the port to access on the container.
	// +optional
	Port intstr.IntOrString
	// 要连接到的主机名，默认为 Pod IP。通常你可能想在 httpHeaders 中设置 "Host"。
	// +optional
	Host string
	// Optional: Scheme to use for connecting to the host, defaults to HTTP.
	// +optional
	Scheme URIScheme
	// Optional: Custom headers to set in the request. HTTP allows repeated headers.
	// +optional
	HTTPHeaders []HTTPHeader
	// Protocol 选择探针连接的线协议。
	// 为 nil 时默认为 HTTP/1.1。
	// +optional
	Protocol *HTTPProtocol
}

// URIScheme 标识 Get 动作连接主机时使用的 scheme。
type URIScheme string

const (
	// URISchemeHTTP means that the scheme used will be http://
	URISchemeHTTP URIScheme = "HTTP"
	// URISchemeHTTPS means that the scheme used will be https://
	URISchemeHTTPS URIScheme = "HTTPS"
)

// TCPSocketAction 描述基于打开 socket 的动作。
type TCPSocketAction struct {
	// Required: Port to connect to.
	// +optional
	Port intstr.IntOrString
	// 可选：要连接到的主机名，默认为 Pod IP。
	// +optional
	Host string
}

// ExecAction 描述"在容器内运行"的动作。
type ExecAction struct {
	// Command 是要在容器内执行的命令行，命令的工作目录
	// 是容器文件系统的根目录（'/'）。命令只是被直接 exec，
	// 并不在 shell 中运行，因此传统的 shell 指令（'|' 等）不起作用。
	// 若要使用 shell，需要显式调用该 shell。
	// 退出状态 0 视为存活/健康，非零视为不健康。
	// +optional
	Command []string
}

// SleepAction 描述"休眠"动作。
type SleepAction struct {
	// Seconds 是要休眠的秒数。
	Seconds int64
}

// Probe describes a health check to be performed against a container to determine whether it is
// alive or ready to receive traffic.
type Probe struct {
	// 用于判断容器健康状况的动作
	ProbeHandler
	// Length of time before health checking is activated.  In seconds.
	// +optional
	InitialDelaySeconds int32
	// Length of time before health checking times out.  In seconds.
	// +optional
	TimeoutSeconds int32
	// How often (in seconds) to perform the probe.
	// +optional
	PeriodSeconds int32
	// Minimum consecutive successes for the probe to be considered successful after having failed.
	// Must be 1 for liveness and startup.
	// +optional
	SuccessThreshold int32
	// Minimum consecutive failures for the probe to be considered failed after having succeeded.
	// +optional
	FailureThreshold int32
	// Optional duration in seconds the pod needs to terminate gracefully upon probe failure.
	// The grace period is the duration in seconds after the processes running in the pod are sent
	// a termination signal and the time when the processes are forcibly halted with a kill signal.
	// Set this value longer than the expected cleanup time for your process.
	// If this value is nil, the pod's terminationGracePeriodSeconds will be used. Otherwise, this
	// value overrides the value provided by the pod spec.
	// Value must be non-negative integer. The value zero indicates stop immediately via
	// the kill signal (no opportunity to shut down).
	// This is a beta field and requires enabling ProbeTerminationGracePeriod feature gate.
	// +optional
	TerminationGracePeriodSeconds *int64
}

// PullPolicy 描述是否/何时拉取容器镜像的策略。
type PullPolicy string

const (
	// PullAlways 表示 kubelet 总是尝试拉取最新镜像。如果拉取失败，容器将启动失败。
	PullAlways PullPolicy = "Always"
	// PullNever 表示 kubelet 从不拉取镜像，只使用本地镜像。如果镜像不存在，容器将启动失败。
	PullNever PullPolicy = "Never"
	// PullIfNotPresent 表示如果磁盘上没有该镜像，kubelet 就会拉取。如果镜像不存在且拉取失败，容器将启动失败。
	PullIfNotPresent PullPolicy = "IfNotPresent"
)

// ResourceResizeRestartPolicy 指定如何处理容器资源调整。
type ResourceResizeRestartPolicy string

// 以下是有效的资源调整重启策略值：
const (
	// 'NotRequired' means Kubernetes will try to resize the container
	// without restarting it, if possible. Kubernetes may however choose to
	// restart the container if it is unable to actuate resize without a
	// restart. For e.g. the runtime doesn't support restart-free resizing.
	NotRequired ResourceResizeRestartPolicy = "NotRequired"
	// 'RestartContainer' means Kubernetes will resize the container in-place
	// by stopping and starting the container when new resources are applied.
	// This is needed for legacy applications. For e.g. java apps using the
	// -xmxN flag which are unable to use resized memory without restarting.
	RestartContainer ResourceResizeRestartPolicy = "RestartContainer"
)

// ContainerResizePolicy 表示容器的资源调整策略。
type ContainerResizePolicy struct {
	// 此资源调整策略适用的资源名称。
	// 支持的值：cpu、memory。
	ResourceName ResourceName
	// 调整指定资源时要应用的重启策略。
	// 如果未指定，默认为 NotRequired。
	RestartPolicy ResourceResizeRestartPolicy
}

// PreemptionPolicy 描述是否/何时抢占 Pod 的策略。
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可以抢占优先级更低的 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他优先级更低的 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// TerminationMessagePolicy 描述如何从容器获取终止消息。
type TerminationMessagePolicy string

const (
	// TerminationMessageReadFile 是默认行为，容器退出时会将
	// 容器的 terminationMessagePath 内容设为容器状态消息。
	TerminationMessageReadFile TerminationMessagePolicy = "File"
	// TerminationMessageFallbackToLogsOnError 会在容器以错误退出且
	// terminationMessagePath 没有内容时，读取容器日志的最新内容
	// 作为容器状态消息。
	TerminationMessageFallbackToLogsOnError TerminationMessagePolicy = "FallbackToLogsOnError"
)

// Capability represent POSIX capabilities type
type Capability string

// Capabilities represent POSIX capabilities that can be added or removed to a running container.
type Capabilities struct {
	// Added capabilities
	// +optional
	Add []Capability
	// Removed capabilities
	// +optional
	Drop []Capability
}

// ResourceRequirements 描述计算资源需求。
type ResourceRequirements struct {
	// Limits describes the maximum amount of compute resources allowed.
	// +optional
	Limits ResourceList
	// Requests 描述所需计算资源的最小值。
	// 如果容器省略 Requests，则默认为显式指定的 Limits，
	// 否则为具体实现定义的值。Requests 不能超过 Limits。
	// +optional
	Requests ResourceList
	// Claims 列出了 spec.resourceClaims 中定义的、
	// 由此容器使用的资源名称。
	// 此字段依赖 DynamicResourceAllocation 特性门控。
	// 此字段不可变，且只能为容器设置。
	// +featureGate=DynamicResourceAllocation
	// +optional
	Claims []ResourceClaim
}

// VolumeResourceRequirements 描述卷的存储资源需求。
type VolumeResourceRequirements struct {
	// Limits 描述允许使用的计算资源上限。
	// +optional
	Limits ResourceList
	// Requests 描述所需计算资源的最小值。
	// 如果容器省略 Requests，则默认为显式指定的 Limits，
	// 否则为具体实现定义的值。Requests 不能超过 Limits。
	// +optional
	Requests ResourceList
}

// ResourceClaim 引用 PodSpec.ResourceClaims 中的一个条目。
type ResourceClaim struct {
	// Name 必须与使用该字段的 Pod 的 pod.spec.resourceClaims 中某个条目的名称匹配。
	// 它使该资源在容器内可用。
	Name string

	// Request 是所引用 claim 中某个请求的名称。
	// 如果为空，则 claim 中的全部内容均可用；否则只有该请求的结果可用。
	// +optional
	Request string
}

// Container represents a single container that is expected to be run on the host.
type Container struct {
	// Required: This must be a DNS_LABEL.  Each container in a pod must
	// have a unique name.
	Name string
	// Required.
	Image string
	// Optional: The container image's entrypoint is used if this is not provided; cannot be updated.
	// Variable references $(VAR_NAME) are expanded using the container's environment.  If a variable
	// cannot be resolved, the reference in the input string will be unchanged.  Double $$ are reduced
	// to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will
	// produce the string literal "$(VAR_NAME)".  Escaped references will never be expanded, regardless
	// of whether the variable exists or not.
	// +optional
	Command []string
	// Optional: The container image's cmd is used if this is not provided; cannot be updated.
	// Variable references $(VAR_NAME) are expanded using the container's environment.  If a variable
	// cannot be resolved, the reference in the input string will be unchanged.  Double $$ are reduced
	// to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will
	// produce the string literal "$(VAR_NAME)".  Escaped references will never be expanded, regardless
	// of whether the variable exists or not.
	// +optional
	Args []string
	// Optional: Defaults to the container runtime's default working directory.
	// +optional
	WorkingDir string
	// +optional
	Ports []ContainerPort
	// 用于填充容器中环境变量的来源列表。
	// 来源中定义的键可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// 当某个键存在于多个来源时，
	// 最后一个来源关联的值优先。
	// 由带有重复键的 Env 定义的值优先。
	// 不可更新。
	// +optional
	EnvFrom []EnvFromSource
	// +optional
	Env []EnvVar
	// Compute resource requirements.
	// +optional
	Resources ResourceRequirements
	// Resources resize policy for the container.
	// This field cannot be set on ephemeral containers.
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	ResizePolicy []ContainerResizePolicy
	// RestartPolicy defines the restart behavior of individual containers in a pod.
	// This overrides the pod-level restart policy. When this field is not specified,
	// the restart behavior is defined by the Pod's restart policy and the container type.
	// If restartPolicyRules are specified, the container-level restartPolicy is also
	// required as the "default" policy if no rules matched. The restartPolicyRules takes
	// precedence over the container-level restart policies. This is an alpha-level feature
	// and is controlled by the feature gate "ContainerRestartRules". If the feature is
	// not enabled, the only allowed value is "Always".
	// Additionally, setting the RestartPolicy as "Always" for the init container will
	// have the following effect:
	// this init container will be continually restarted on
	// exit until all regular containers have terminated. Once all regular
	// containers have completed, all init containers with restartPolicy "Always"
	// will be shut down. This lifecycle differs from normal init containers and
	// is often referred to as a "sidecar" container. Although this init
	// container still starts in the init container sequence, it does not wait
	// for the container to complete before proceeding to the next init
	// container. Instead, the next init container starts immediately after this
	// init container is started, or after any startupProbe has successfully
	// completed.
	// +optional
	RestartPolicy *ContainerRestartPolicy
	// Represents a list of rules to be checked to determine if the
	// container should be restarted on exit. The rules are evaluated in
	// order. Once a rule matches a container exit condition, the remaining
	// rules are ignored. If no rule matches the container exit condition,
	// the Container-level restart policy determines the whether the container
	// is restarted or not. This is an alpha-level feature, and controlled by
	// the feature gate "ContainerRestartRules". If the feature is not enabled,
	// this field is not allowed. Constraints on the rules:
	// - At most 20 rules are allowed.
	// - Rules can have the same action.
	// - Identical rules are not forbidden in validations.
	// When rules are specified, container MUST set RestartPolicy explicitly
	// even it if matches the Pod's RestartPolicy.
	// +featureGate=ContainerRestartRules
	// +optional
	// +listType=atomic
	RestartPolicyRules []ContainerRestartRule
	// +optional
	VolumeMounts []VolumeMount
	// volumeDevices 是容器要使用的块设备列表。
	// +optional
	VolumeDevices []VolumeDevice
	// +optional
	LivenessProbe *Probe
	// +optional
	ReadinessProbe *Probe
	// +optional
	StartupProbe *Probe
	// +optional
	Lifecycle *Lifecycle
	// Required.
	// +optional
	TerminationMessagePath string
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy
	// Required: Policy for pulling images for this container
	ImagePullPolicy PullPolicy
	// SecurityContext 定义容器运行时应使用的安全选项。
	// 如果设置，SecurityContext 中的字段会覆盖 PodSecurityContext 中的对应字段。
	// +optional
	SecurityContext *SecurityContext

	// 交互式容器的变量，这些有非常专门的用例（例如调试），
	// 不应用于通用容器。
	// +optional
	Stdin bool
	// +optional
	StdinOnce bool
	// +optional
	TTY bool
}

// ProbeHandler 定义探针中要采取的具体动作。
// 必须指定且只能指定其中一个字段。
type ProbeHandler struct {
	// Exec specifies the action to take.
	// +optional
	Exec *ExecAction
	// HTTPGet specifies the http request to perform.
	// +optional
	HTTPGet *HTTPGetAction
	// TCPSocket specifies an action involving a TCP port.
	// +optional
	TCPSocket *TCPSocketAction

	// GRPC specifies an action involving a GRPC port.
	// +optional
	GRPC *GRPCAction
}

// LifecycleHandler 定义生命周期钩子中要采取的具体动作。
// 除 TCPSocket 外，必须指定且只能指定其中一个字段。
type LifecycleHandler struct {
	// Exec specifies the action to take.
	// +optional
	Exec *ExecAction
	// HTTPGet specifies the http request to perform.
	// +optional
	HTTPGet *HTTPGetAction
	// Deprecated. TCPSocket is NOT supported as a LifecycleHandler and kept
	// for the backward compatibility. There are no validation of this field and
	// lifecycle hooks will fail in runtime when tcp handler is specified.
	// +optional
	TCPSocket *TCPSocketAction
	// Sleep represents the duration that the container should sleep before being terminated.
	// +optional
	Sleep *SleepAction
}

type GRPCAction struct {
	// gRPC 服务的端口号。数字必须在 1 到 65535 范围内。
	Port int32

	// Service is the name of the service to place in the gRPC HealthCheckRequest
	// (see https://github.com/grpc/grpc/blob/master/doc/health-checking.md).
	//
	// If this is not specified, the default behavior is to probe the server's overall health status.
	// +optional
	Service *string

	// mode 指定 gRPC 健康检查探针的连接模式。
	// 设为 "TLS" 时使用 TLS 但不校验证书。
	// 设为 "Plaintext" 时显式使用明文（不安全）连接。
	// 如果未指定，探针使用明文（不安全）连接。
	// +featureGate=GRPCContainerProbeTLS
	// +optional
	Mode *GRPCProbeMode
}

// GRPCProbeMode 描述 gRPC 探针的连接模式。
// +enum
type GRPCProbeMode string

const (
	// GRPCProbeModePlaintext 表示探针应使用明文（不安全的）gRPC 连接。
	GRPCProbeModePlaintext GRPCProbeMode = "Plaintext"
	// GRPCProbeModeTLS 表示探针应使用 TLS 连接但不校验证书。
	GRPCProbeModeTLS GRPCProbeMode = "TLS"
)

// Signal defines the stop signal of containers
// +enum
type Signal string

const (
	SIGABRT         Signal = "SIGABRT"
	SIGALRM         Signal = "SIGALRM"
	SIGBUS          Signal = "SIGBUS"
	SIGCHLD         Signal = "SIGCHLD"
	SIGCLD          Signal = "SIGCLD"
	SIGCONT         Signal = "SIGCONT"
	SIGFPE          Signal = "SIGFPE"
	SIGHUP          Signal = "SIGHUP"
	SIGILL          Signal = "SIGILL"
	SIGINT          Signal = "SIGINT"
	SIGIO           Signal = "SIGIO"
	SIGIOT          Signal = "SIGIOT"
	SIGKILL         Signal = "SIGKILL"
	SIGPIPE         Signal = "SIGPIPE"
	SIGPOLL         Signal = "SIGPOLL"
	SIGPROF         Signal = "SIGPROF"
	SIGPWR          Signal = "SIGPWR"
	SIGQUIT         Signal = "SIGQUIT"
	SIGSEGV         Signal = "SIGSEGV"
	SIGSTKFLT       Signal = "SIGSTKFLT"
	SIGSTOP         Signal = "SIGSTOP"
	SIGSYS          Signal = "SIGSYS"
	SIGTERM         Signal = "SIGTERM"
	SIGTRAP         Signal = "SIGTRAP"
	SIGTSTP         Signal = "SIGTSTP"
	SIGTTIN         Signal = "SIGTTIN"
	SIGTTOU         Signal = "SIGTTOU"
	SIGURG          Signal = "SIGURG"
	SIGUSR1         Signal = "SIGUSR1"
	SIGUSR2         Signal = "SIGUSR2"
	SIGVTALRM       Signal = "SIGVTALRM"
	SIGWINCH        Signal = "SIGWINCH"
	SIGXCPU         Signal = "SIGXCPU"
	SIGXFSZ         Signal = "SIGXFSZ"
	SIGRTMIN        Signal = "SIGRTMIN"
	SIGRTMINPLUS1   Signal = "SIGRTMIN+1"
	SIGRTMINPLUS2   Signal = "SIGRTMIN+2"
	SIGRTMINPLUS3   Signal = "SIGRTMIN+3"
	SIGRTMINPLUS4   Signal = "SIGRTMIN+4"
	SIGRTMINPLUS5   Signal = "SIGRTMIN+5"
	SIGRTMINPLUS6   Signal = "SIGRTMIN+6"
	SIGRTMINPLUS7   Signal = "SIGRTMIN+7"
	SIGRTMINPLUS8   Signal = "SIGRTMIN+8"
	SIGRTMINPLUS9   Signal = "SIGRTMIN+9"
	SIGRTMINPLUS10  Signal = "SIGRTMIN+10"
	SIGRTMINPLUS11  Signal = "SIGRTMIN+11"
	SIGRTMINPLUS12  Signal = "SIGRTMIN+12"
	SIGRTMINPLUS13  Signal = "SIGRTMIN+13"
	SIGRTMINPLUS14  Signal = "SIGRTMIN+14"
	SIGRTMINPLUS15  Signal = "SIGRTMIN+15"
	SIGRTMAXMINUS14 Signal = "SIGRTMAX-14"
	SIGRTMAXMINUS13 Signal = "SIGRTMAX-13"
	SIGRTMAXMINUS12 Signal = "SIGRTMAX-12"
	SIGRTMAXMINUS11 Signal = "SIGRTMAX-11"
	SIGRTMAXMINUS10 Signal = "SIGRTMAX-10"
	SIGRTMAXMINUS9  Signal = "SIGRTMAX-9"
	SIGRTMAXMINUS8  Signal = "SIGRTMAX-8"
	SIGRTMAXMINUS7  Signal = "SIGRTMAX-7"
	SIGRTMAXMINUS6  Signal = "SIGRTMAX-6"
	SIGRTMAXMINUS5  Signal = "SIGRTMAX-5"
	SIGRTMAXMINUS4  Signal = "SIGRTMAX-4"
	SIGRTMAXMINUS3  Signal = "SIGRTMAX-3"
	SIGRTMAXMINUS2  Signal = "SIGRTMAX-2"
	SIGRTMAXMINUS1  Signal = "SIGRTMAX-1"
	SIGRTMAX        Signal = "SIGRTMAX"
)

// Lifecycle 描述管理系统应针对容器生命周期事件采取的动作。
// events.  For the PostStart and PreStop lifecycle handlers, management of the container blocks
// 否则对容器的管理会阻塞直到动作完成。
type Lifecycle struct {
	// PostStart is called immediately after a container is created.  If the handler fails, the container
	// is terminated and restarted.
	// More info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PostStart *LifecycleHandler
	// PreStop 在容器因 API 请求或管理事件（如存活/启动探针失败、
	// 抢占、资源争用等）而即将被终止前立即调用。如果容器崩溃或退出，则不会调用此钩子。
	// Pod 的终止宽限期倒计时在 PreStop 钩子执行之前就开始。
	// 无论钩子的结果如何，容器最终都会在 Pod 的终止宽限期内终止
	// （除非被 finalizer 延迟）。对容器的其他管理会阻塞直到钩子完成
	// 或达到终止宽限期。
	// More info: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PreStop *LifecycleHandler
	// StopSignal 定义容器被停止时要发送的信号。
	// 如果未指定，默认值由所使用的容器运行时定义。
	// StopSignal 只能为 .spec.os.name 非空的 Pod 设置。
	// +optional
	StopSignal *Signal
}

// The below types are used by kube_client and api_server.

// ConditionStatus defines conditions of resources
type ConditionStatus string

// These are valid condition statuses. "ConditionTrue" means a resource is in the condition;
// "ConditionFalse" means a resource is not in the condition; "ConditionUnknown" means kubernetes
// 无法确定资源是否处于该条件。将来，我们可能会添加其他
// 中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// ContainerStateWaiting represents the waiting state of a container
type ContainerStateWaiting struct {
	// A brief CamelCase string indicating details about why the container is in waiting state.
	// +optional
	Reason string
	// A human-readable message indicating details about why the container is in waiting state.
	// +optional
	Message string
}

// ContainerStateRunning represents the running state of a container
type ContainerStateRunning struct {
	// +optional
	StartedAt metav1.Time
}

// ContainerStateTerminated represents the terminated state of a container
type ContainerStateTerminated struct {
	ExitCode int32
	// +optional
	Signal int32
	// +optional
	Reason string
	// +optional
	Message string
	// +optional
	StartedAt metav1.Time
	// +optional
	FinishedAt metav1.Time
	// +optional
	ContainerID string
}

// ContainerState 表示容器可能处于的状态。
// 只能指定其中一个成员。
// 如果都未指定，则默认为 ContainerStateWaiting。
type ContainerState struct {
	// +optional
	Waiting *ContainerStateWaiting
	// +optional
	Running *ContainerStateRunning
	// +optional
	Terminated *ContainerStateTerminated
}

// ContainerStatus 包含此容器当前状态的详细信息。
type ContainerStatus struct {
	// Name 是表示容器唯一名称的 DNS_LABEL。
	// Pod 中的每个容器在所有容器类型中都必须具有唯一的名称。
	// 不可更新。
	Name string
	// State 保存容器当前状况的详细信息。
	// +optional
	State ContainerState
	// LastTerminationState 保存容器的上一次终止状态，
	// 用于帮助调试容器崩溃和重启。如果容器仍在运行且 RestartCount 为 0，
	// 则不会填充该字段。
	// +optional
	LastTerminationState ContainerState
	// Ready 指定容器当前是否通过就绪性检查。
	// 该值会随着就绪性探针持续执行而变化。如果未指定就绪性探针，
	// 则在容器完全启动后该字段默认为 true（参见 Started 字段）。
	// 该值通常用于判断容器是否已准备好接收流量。
	Ready bool
	// RestartCount 保存容器被重启的次数。
	// Kubelet 会努力始终递增该值，但在某些情况下状态可能因节点重启而丢失，
	// 此时该值可能被重置为 0。该值永不为负数。
	RestartCount int32
	// Image 是容器正在运行的容器镜像的名称。
	// 该镜像可能与 PodSpec 中指定的镜像不同，
	// 因为它可能已被运行时解析。
	// More info: https://kubernetes.io/docs/concepts/containers/images.
	Image string
	// ImageID 是容器镜像的镜像 ID。该镜像 ID 可能与
	// PodSpec 中使用的镜像的 ID 不同，因为它可能已被运行时解析。
	ImageID string
	// 其中 type 是容器运行时标识符，由 CRI API 的 Version 调用返回
	// （例如 "containerd"）。
	// ContainerID is the ID of the container in the format '<type>://<container_id>'.
	// +optional
	ContainerID string
	// Started 表示容器是否已完成 postStart 生命周期钩子
	// 并通过了启动探针。
	// 初始值为 false，在 startupProbe 被认为成功后变为 true。
	// 当容器重启或 kubelet 临时丢失状态时重置为 false；这两种情况下启动探针都会重新运行。
	// 如果未定义 startupProbe 且容器正在运行并已通过 postStart 生命周期钩子，则该值始终为 true。
	// null 值必须按 false 处理。
	// +optional
	Started *bool
	// AllocatedResources 表示节点为此容器分配的计算资源。
	// Kubelet 在 Pod 准入成功以及期望的 Pod 扩缩容准入成功后，
	// 将此值设置为 Container.Resources.Requests。
	// +optional
	AllocatedResources ResourceList
	// Resources 表示在容器启动后或成功扩缩容后，
	// 已在此运行容器上成功生效的计算资源请求和限制。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	Resources *ResourceRequirements
	// Status of volume mounts.
	// +listType=atomic
	// +optional
	VolumeMounts []VolumeMountStatus
	// User 表示最初附加到容器第一个进程的用户身份信息。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	User *ContainerUser
	// AllocatedResourcesStatus 表示为此 Pod 分配的各种资源的状态。
	// +featureGate=ResourceHealthStatus
	// +optional
	AllocatedResourcesStatus []ResourceStatus
	// StopSignal is the signal which will be sent to this container when it is stopped.  This might be
	// the stop signal specified by the user, the signal specified in the container image, or the default
	// stop signal of the container runtime on this node.
	// +featureGate=ContainerStopSignals
	// +optional
	StopSignal *Signal
}

type ResourceStatus struct {
	// 资源名称。在 Pod 内必须唯一；对于非 DRA 资源，必须与 Pod spec 中的某个资源匹配。
	// 对于 DRA 资源，当设置了 container.resources.claims[*].request 时，
	// 值必须为 "claim:<claim_name>/<request>"；当 container.resources.claims[*].request 为空时，
	// 值必须为 "claim:<claim_name>"。
	// 对于 DRA 支持的扩展资源，当声明名称和请求名称记录在 pod.status.extendedResourceClaimStatus 中时，
	// 使用 "claim:<claim_name>/<request>"。
	// 当报告的是关于某个容器的状态时，"claim_name" 和 "request"
	// 必须与该容器的某个声明匹配。
	// +required
	Name ResourceName
	// 资源健康状态的唯一列表。列表中的每个元素包含一个唯一的资源 ID 及其健康状况。
	// 至少在 Pod 的生命周期内，资源 ID 必须唯一标识节点上分配给该 Pod 的资源。
	// 如果同一节点上的其他 Pod 报告了具有相同资源 ID 的状态，则它们必须是共享的同一资源。
	// 有关其在各种使用场景下的具体格式，请参见 ResourceID 类型定义。
	// +listType=map
	// +listMapKey=resourceID
	Resources []ResourceHealth

	// allow to extend this struct in future with the overall health fields or things like Device Plugin version
}

// ResourceID 根据此资源健康信息的来源计算得出。
// For DevicePlugin:
//
//	DeviceID, where DeviceID is how device plugin identifies the device. The same DeviceID can be found in PodResources API.
//
// DevicePlugin ID 通常在节点的生命周期内保持不变，通常可用于唯一标识节点上的设备。
//
// For DRA:
//
// <driver name>/<pool name>/<device name>：可以在该 DRA 驱动程序发布的信息中查找此类设备以了解更多信息。它被设计为在集群内全局唯一。
type ResourceID string

type ResourceHealthStatus string

const (
	ResourceHealthStatusHealthy   ResourceHealthStatus = "Healthy"
	ResourceHealthStatusUnhealthy ResourceHealthStatus = "Unhealthy"
	ResourceHealthStatusUnknown   ResourceHealthStatus = "Unknown"
)

// ResourceHealth 表示资源的健康状况，包含最新的设备健康信息。
// This is a part of KEP https://kep.k8s.io/4680.
type ResourceHealth struct {
	// ResourceID 是资源的唯一标识符。有关更多信息，请参见 ResourceID 类型。
	ResourceID ResourceID
	// 资源的健康状况。
	// 可以是以下之一：
	// - Healthy：正常运行
	// - Unhealthy：报告为不健康。我们认为这是暂时的健康问题，
	// 因为我们目前没有机制区分暂时性和永久性问题。
	// - Unknown：无法确定状态。
	// 例如，Device Plugin 已注销且此后未重新注册。
	// 将来我们可能会引入 PermanentlyUnhealthy 状态。
	Health ResourceHealthStatus
	// Message provides human-readable context for Health (e.g. "ECC error count exceeded threshold").
	// This field is populated by the kubelet when ResourceHealthStatusMessage is enabled.
	Message *string
}

// ContainerUser represents user identity information
type ContainerUser struct {
	// Linux 保存最初附加到 Linux 容器中容器第一个进程的用户身份信息。
	// 请注意，如果进程有足够权限，实际运行时的身份可以更改。
	// +optional
	Linux *LinuxContainerUser

	// Windows holds user identity information of the first process of the containers in Windows
	// 这只是为将来使用而保留。
	// Windows *WindowsContainerUser
}

// LinuxContainerUser 表示 Linux 容器中的用户身份信息。
type LinuxContainerUser struct {
	// UID 是最初附加到容器中第一个进程的主 uid。
	UID int64
	// GID 是最初附加到容器中第一个进程的主 gid。
	GID int64
	// SupplementalGroups 是最初附加到容器中第一个进程的附加组。
	SupplementalGroups []int64
}

// PodPhase 是当前时刻 Pod 所处状况的标签。
type PodPhase string

// 以下是 Pod 的有效状态。
const (
	// PodPending 表示系统已接受该 Pod，但一个或多个容器
	// 尚未启动。这包括绑定到节点之前的时间，以及
	// 将镜像拉取到主机上所花费的时间。
	PodPending PodPhase = "Pending"
	// PodRunning 表示 Pod 已绑定到节点且所有容器都已启动。
	// 至少有一个容器仍在运行或正在重新启动。
	PodRunning PodPhase = "Running"
	// PodSucceeded 表示 Pod 中的所有容器都已自愿终止，
	// 退出码为 0，且系统不会再重启其中任何容器。
	PodSucceeded PodPhase = "Succeeded"
	// PodFailed 表示 Pod 中的所有容器都已终止，且至少有一个容器
	// 以失败状态终止（以非零退出码退出或被系统停止）。
	PodFailed PodPhase = "Failed"
	// PodUnknown means that for some reason the state of the pod could not be obtained, typically due
	// to an error in communicating with the host of the pod.
	// Deprecated in v1.21: It isn't being set since 2015 (74da3b14b0c0f658b3bb8d2def5094686d0e9095)
	PodUnknown PodPhase = "Unknown"
)

// PodConditionType defines the condition of pod
type PodConditionType string

// These are valid conditions of pod.
const (
	// PodScheduled 表示此 Pod 的调度过程的状态。
	PodScheduled PodConditionType = "PodScheduled"
	// PodReady 表示 Pod 能够处理请求，应将其加入
	// 所有匹配服务的负载均衡池。
	PodReady PodConditionType = "Ready"
	// PodInitialized 表示 Pod 中的所有 init 容器都已成功启动。
	PodInitialized PodConditionType = "Initialized"
	// ContainersReady 表示 Pod 中的所有容器是否都已就绪。
	ContainersReady PodConditionType = "ContainersReady"
	// DisruptionTarget 表示该 Pod 由于中断（如抢占、驱逐 API 或垃圾回收）
	// 即将被终止。
	DisruptionTarget PodConditionType = "DisruptionTarget"
	// PodResizePending 表示 Pod 已完成调整大小，但 kubelet 尚未
	// 分配资源。如果同时设置了 PodResizePending 和 PodResizeInProgress，
	// 表示在上一次仍在进行的 Pod 调整过程中又请求了一次新的调整。
	PodResizePending PodConditionType = "PodResizePending"
	// PodResizeInProgress 表示调整正在进行中，只要 kubelet 已为调整分配资源
	// 但尚未完成所有必需变更，就会存在此状况。
	// 如果同时设置了 PodResizePending 和 PodResizeInProgress，
	// 表示在上一次仍在进行的 Pod 调整过程中又请求了一次新的调整。
	PodResizeInProgress PodConditionType = "PodResizeInProgress"
	// AllContainersRestarting 表示 Pod 中的所有容器正在重启。
	AllContainersRestarting PodConditionType = "AllContainersRestarting"
)

// PodCondition represents pod's condition
type PodCondition struct {
	Type PodConditionType
	// +optional
	ObservedGeneration int64
	Status             ConditionStatus
	// +optional
	LastProbeTime metav1.Time
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// Deprecated: PodResizeStatus 表示 Pod 容器期望调整大小的状态。
type PodResizeStatus string

const (
	// Pod 资源调整已被节点接受，正在执行。
	PodResizeStatusInProgress PodResizeStatus = "InProgress"
	// 节点当前无法调整 Pod 的大小，将继续重试。
	PodResizeStatusDeferred PodResizeStatus = "Deferred"
	// 请求的 Pod 调整不可行，不会重新评估。
	PodResizeStatusInfeasible PodResizeStatus = "Infeasible"
)

// VolumeMountStatus 显示卷挂载的状态。
type VolumeMountStatus struct {
	// Name 对应原始 VolumeMount 的名称。
	Name string
	// MountPath 对应原始 VolumeMount。
	MountPath string
	// ReadOnly 对应原始 VolumeMount。
	// +optional
	ReadOnly bool
	// RecursiveReadOnly 必须设置为 Disabled、Enabled 或未指定（对于非只读挂载）。
	// 原始 VolumeMount 中的 IfPossible 值必须根据挂载结果转换为 Disabled 或 Enabled。
	// +optional
	RecursiveReadOnly *RecursiveReadOnlyMode
	// volumeStatus 表示已挂载卷的特定于卷类型的状态。
	// +optional
	VolumeStatus *VolumeStatus
}

// VolumeStatus 表示已挂载卷的状态。
// 最多只能指定其中一个成员。
type VolumeStatus struct {
	// image 表示在 kubelet 主机上拉取并挂载的 OCI 对象（容器镜像或制品）。
	// +featureGate=ImageVolumeWithDigest
	// +optional
	Image *ImageVolumeStatus
}

type ImageVolumeStatus struct {
	// ImageRef 是此卷所用镜像的摘要。
	// 其值应类似于 Pod 的 status.containerStatuses[i].imageID。
	// ImageRef 长度不应超过 256 个字符。
	// +kubebuilder:validation:MaxLength=256
	// +required
	ImageRef string
}

// RestartPolicy describes how the container should be restarted.
// Only one of the following restart policies may be specified.
// If none of the following policies is specified, the default one
// is RestartPolicyAlways.
type RestartPolicy string

// These are valid restart policies
const (
	RestartPolicyAlways    RestartPolicy = "Always"
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	RestartPolicyNever     RestartPolicy = "Never"
)

// ContainerRestartPolicy 是单个容器的重启策略。
// 唯一允许的值为 "Always"、"Never" 和 "OnFailure"。
type ContainerRestartPolicy string

const (
	ContainerRestartPolicyAlways    ContainerRestartPolicy = "Always"
	ContainerRestartPolicyNever     ContainerRestartPolicy = "Never"
	ContainerRestartPolicyOnFailure ContainerRestartPolicy = "OnFailure"
)

// ContainerRestartRule 描述如何处理容器退出。
type ContainerRestartRule struct {
	// 指定在满足条件时对容器退出采取的操作。唯一可能的值为 "Restart"，
	// 表示重启容器。
	// +required
	Action ContainerRestartRuleAction

	// Represents the exit codes to check on container exits.
	// +optional
	ExitCodes *ContainerRestartRuleOnExitCodes
}

// ContainerRestartRuleAction describes the action to take when the
// container exits.
type ContainerRestartRuleAction string

// These are valid restart rule actions.
const (
	// The container will be restarted if the rule matches. Only valid on normal init container and
	// regular containers. Not valid on sidecar containers and ephemeral containers.
	ContainerRestartRuleActionRestart ContainerRestartRuleAction = "Restart"
	// All containers (except ephemeral containers) inside the pod will be terminated and restarted.
	// Valid on normal init container, sidecar containers, and regular containers. Not valid on
	// ephemeral containers.
	ContainerRestartRuleActionRestartAllContainers ContainerRestartRuleAction = "RestartAllContainers"
)

// ContainerRestartRuleOnExitCodes describes the condition
// for handling an exited container based on its exit codes.
type ContainerRestartRuleOnExitCodes struct {
	// 表示容器退出码与指定值之间的关系。可能的值为：
	// - In：如果容器退出码在指定值集合中，则满足条件。
	// - NotIn：如果容器退出码不在指定值集合中，则满足条件。
	// +required
	Operator ContainerRestartRuleOnExitCodesOperator

	// Specifies the set of values to check for container exit codes.
	// 最多允许 255 个元素。
	// +optional
	// +listType=set
	Values []int32
}

// ContainerRestartRuleOnExitCodesOperator describes the operator
// to take for the exit codes.
type ContainerRestartRuleOnExitCodesOperator string

const (
	ContainerRestartRuleOnExitCodesOpIn    ContainerRestartRuleOnExitCodesOperator = "In"
	ContainerRestartRuleOnExitCodesOpNotIn ContainerRestartRuleOnExitCodesOperator = "NotIn"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodList 是 Pod 的列表。
type PodList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Pod
}

// DNSPolicy 定义 Pod 的 DNS 配置方式。
type DNSPolicy string

const (
	// DNSClusterFirstWithHostNet 表示 Pod 应优先使用集群 DNS（如果可用），
	// 然后回退到默认（由 kubelet 确定）DNS 设置。
	DNSClusterFirstWithHostNet DNSPolicy = "ClusterFirstWithHostNet"

	// DNSClusterFirst 表示除非 hostNetwork 为 true，否则 Pod 应优先使用集群 DNS
	// （如果可用），然后回退到默认（由 kubelet 确定）DNS 设置。
	DNSClusterFirst DNSPolicy = "ClusterFirst"

	// DNSDefault 表示 Pod 应使用默认（由 kubelet 确定）DNS 设置。
	DNSDefault DNSPolicy = "Default"

	// DNSNone 表示 Pod 应使用空的 DNS 设置。nameservers 和 search paths 等 DNS
	// 参数应通过 DNSConfig 定义。
	DNSNone DNSPolicy = "None"
)

// NodeSelector represents the union of the results of one or more label queries
// over a set of nodes; that is, it represents the OR of the selectors represented
// by the node selector terms.
type NodeSelector struct {
	// Required. A list of node selector terms. The terms are ORed.
	NodeSelectorTerms []NodeSelectorTerm
}

// NodeSelectorTerm represents expressions and fields required to select nodes.
// A null or empty node selector term matches no objects. The requirements of
// them are ANDed.
// The TopologySelectorTerm type implements a subset of the NodeSelectorTerm.
type NodeSelectorTerm struct {
	// A list of node selector requirements by node's labels.
	MatchExpressions []NodeSelectorRequirement
	// A list of node selector requirements by node's fields.
	MatchFields []NodeSelectorRequirement
}

// NodeSelectorRequirement is a selector that contains values, a key, and an operator
// that relates the key and values.
type NodeSelectorRequirement struct {
	// The label key that the selector applies to.
	Key string
	// 表示键与一组值之间的关系。
	// 有效的操作符有 In、NotIn、Exists、DoesNotExist、Gt 和 Lt。
	Operator NodeSelectorOperator
	// 字符串值数组。如果操作符是 In 或 NotIn，则 values 数组必须非空。
	// 如果操作符是 Exists 或 DoesNotExist，则 values 数组必须为空。
	// 如果操作符是 Gt 或 Lt，则 values 数组必须只有一个元素，该元素将被解释为整数。
	// 在 strategic merge patch 期间会替换此数组。
	// +optional
	Values []string
}

// NodeSelectorOperator is the set of operators that can be used in
// a node selector requirement.
type NodeSelectorOperator string

// These are valid values of NodeSelectorOperator
const (
	NodeSelectorOpIn           NodeSelectorOperator = "In"
	NodeSelectorOpNotIn        NodeSelectorOperator = "NotIn"
	NodeSelectorOpExists       NodeSelectorOperator = "Exists"
	NodeSelectorOpDoesNotExist NodeSelectorOperator = "DoesNotExist"
	NodeSelectorOpGt           NodeSelectorOperator = "Gt"
	NodeSelectorOpLt           NodeSelectorOperator = "Lt"
)

// TopologySelectorTerm represents the result of label queries.
// 为 null 或空的拓扑选择器项不匹配任何对象。
// 其中的要求为 AND 关系。
// 它提供 NodeSelectorTerm 功能的子集。
// 这是一个 alpha 特性，将来可能会改变。
type TopologySelectorTerm struct {
	// A list of topology selector requirements by labels.
	// +optional
	MatchLabelExpressions []TopologySelectorLabelRequirement
}

// TopologySelectorLabelRequirement is a selector that matches given label.
// 这是一个 alpha 特性，将来可能会改变。
type TopologySelectorLabelRequirement struct {
	// The label key that the selector applies to.
	Key string
	// An array of string values. One value must match the label to be selected.
	// Each entry in Values is ORed.
	Values []string
}

// Affinity 是一组亲和性调度规则。
type Affinity struct {
	// 描述 Pod 的节点亲和性调度规则。
	// +optional
	NodeAffinity *NodeAffinity
	// Describes pod affinity scheduling rules (e.g. co-locate this pod in the same node, zone, etc. as some other pod(s)).
	// +optional
	PodAffinity *PodAffinity
	// Describes pod anti-affinity scheduling rules (e.g. avoid putting this pod in the same node, zone, etc. as some other pod(s)).
	// +optional
	PodAntiAffinity *PodAntiAffinity
}

// PodAffinity is a group of inter pod affinity scheduling rules.
type PodAffinity struct {
	// NOT YET IMPLEMENTED. TODO: Uncomment field once it is implemented.
	// If the affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to a pod label update), the
	// system will try to eventually evict the pod from its node.
	// When there are multiple elements, the lists of nodes corresponding to each
	// podAffinityTerm are intersected, i.e. all terms must be satisfied.
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm

	// If the affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to a pod label update), the
	// system may or may not try to eventually evict the pod from its node.
	// When there are multiple elements, the lists of nodes corresponding to each
	// podAffinityTerm are intersected, i.e. all terms must be satisfied.
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm
	// The scheduler will prefer to schedule pods to nodes that satisfy
	// the affinity expressions specified by this field, but it may choose
	// a node that violates one or more of the expressions. The node that is
	// most preferred is the one with the greatest sum of weights, i.e.
	// for each node that meets all of the scheduling requirements (resource
	// request, requiredDuringScheduling affinity expressions, etc.),
	// compute a sum by iterating through the elements of this field and adding
	// "weight" to the sum if the node has pods which matches the corresponding podAffinityTerm; the
	// node(s) with the highest sum are the most preferred.
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm
}

// PodAntiAffinity is a group of inter pod anti affinity scheduling rules.
type PodAntiAffinity struct {
	// NOT YET IMPLEMENTED. TODO: Uncomment field once it is implemented.
	// If the anti-affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the anti-affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to a pod label update), the
	// system will try to eventually evict the pod from its node.
	// When there are multiple elements, the lists of nodes corresponding to each
	// podAffinityTerm are intersected, i.e. all terms must be satisfied.
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm

	// If the anti-affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the anti-affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to a pod label update), the
	// system may or may not try to eventually evict the pod from its node.
	// When there are multiple elements, the lists of nodes corresponding to each
	// podAffinityTerm are intersected, i.e. all terms must be satisfied.
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm
	// The scheduler will prefer to schedule pods to nodes that satisfy
	// the anti-affinity expressions specified by this field, but it may choose
	// a node that violates one or more of the expressions. The node that is
	// most preferred is the one with the greatest sum of weights, i.e.
	// for each node that meets all of the scheduling requirements (resource
	// request, requiredDuringScheduling anti-affinity expressions, etc.),
	// compute a sum by iterating through the elements of this field and subtracting
	// "weight" from the sum if the node has pods which matches the corresponding podAffinityTerm; the
	// node(s) with the highest sum are the most preferred.
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm
}

// WeightedPodAffinityTerm represents the weights of all of the matched WeightedPodAffinityTerm
// fields are added per-node to find the most preferred node(s)
type WeightedPodAffinityTerm struct {
	// 与匹配相应 podAffinityTerm 关联的权重，范围为 1-100。
	Weight int32
	// 必需。与相应权重关联的 Pod 亲和性项。
	PodAffinityTerm PodAffinityTerm
}

// PodAffinityTerm defines a set of pods (namely those matching the labelSelector
// relative to the given namespace(s)) that this pod should be
// co-located (affinity) or not co-located (anti-affinity) with,
// where co-located is defined as running on a node whose value of
// the label with key <topologyKey> matches that of any node on which
// a pod of the set of pods is running.
type PodAffinityTerm struct {
	// 对一组资源（此处为 Pod）的标签查询。
	// 如果为 null，则此 PodAffinityTerm 不匹配任何 Pod。
	// +optional
	LabelSelector *metav1.LabelSelector
	// namespaces 指定此条件适用的一组命名空间的静态列表。
	// 该条件应用于此字段列出的命名空间与 namespaceSelector 选择的命名空间的并集。
	// namespaces 列表为 null 或为空且 namespaceSelector 为 null 时表示“此 Pod 所在的命名空间”。
	// +optional
	Namespaces []string
	// 此 Pod 应与指定命名空间中匹配 labelSelector 的 Pod 共置（亲和性）或不共置（反亲和性），
	// 其中共置定义为运行在一个节点上，该节点上键为 topologyKey 的标签值
	// 与任一被选中 Pod 所在节点的对应标签值匹配。
	// topologyKey 不得为空。
	TopologyKey string
	// 对此条件适用的一组命名空间的标签查询。
	// 该条件应用于此字段选择的命名空间与 namespaces 字段中列出的命名空间的并集。
	// null 选择器且 namespaces 列表为 null 或为空时表示“此 Pod 所在的命名空间”。
	// 空选择器（{}）匹配所有命名空间。
	// +optional
	NamespaceSelector *metav1.LabelSelector
	// MatchLabelKeys 是一组 Pod 标签键，用于选择要考虑的 Pod。
	// 这些键用于从新传入 Pod 的标签中查找值，这些键值标签与 `labelSelector` 合并为 `key in (value)`，
	// 以选出在新传入 Pod 的 Pod（反）亲和性中需要考虑的现有 Pod 组。
	// 新传入 Pod 标签中不存在的键将被忽略。默认值为空。
	// 同一键不得同时存在于 matchLabelKeys 和 labelSelector 中。
	// 此外，如果未设置 labelSelector，则不能设置 matchLabelKeys。
	// +listType=atomic
	// +optional
	MatchLabelKeys []string
	// MismatchLabelKeys 是一组 Pod 标签键，用于选择要考虑的 Pod。
	// 这些键用于从新传入 Pod 的标签中查找值，这些键值标签与 `labelSelector` 合并为 `key notin (value)`，
	// 以选出在新传入 Pod 的 Pod（反）亲和性中需要考虑的现有 Pod 组。
	// 新传入 Pod 标签中不存在的键将被忽略。默认值为空。
	// 同一键不得同时存在于 mismatchLabelKeys 和 labelSelector 中。
	// 此外，如果未设置 labelSelector，则不能设置 mismatchLabelKeys。
	// +listType=atomic
	// +optional
	MismatchLabelKeys []string
}

// NodeAffinity is a group of node affinity scheduling rules.
type NodeAffinity struct {
	// NOT YET IMPLEMENTED. TODO: Uncomment field once it is implemented.
	// If the affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to an update), the system
	// will try to eventually evict the pod from its node.
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution *NodeSelector

	// If the affinity requirements specified by this field are not met at
	// scheduling time, the pod will not be scheduled onto the node.
	// If the affinity requirements specified by this field cease to be met
	// at some point during pod execution (e.g. due to an update), the system
	// may or may not try to eventually evict the pod from its node.
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution *NodeSelector
	// The scheduler will prefer to schedule pods to nodes that satisfy
	// the affinity expressions specified by this field, but it may choose
	// a node that violates one or more of the expressions. The node that is
	// most preferred is the one with the greatest sum of weights, i.e.
	// for each node that meets all of the scheduling requirements (resource
	// request, requiredDuringScheduling affinity expressions, etc.),
	// compute a sum by iterating through the elements of this field and adding
	// "weight" to the sum if the node matches the corresponding matchExpressions; the
	// node(s) with the highest sum are the most preferred.
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []PreferredSchedulingTerm
}

// PreferredSchedulingTerm represents an empty preferred scheduling term matches all objects with implicit weight 0
// 为 null 时不匹配任何对象（同样是无操作）。
type PreferredSchedulingTerm struct {
	// 与匹配相应 nodeSelectorTerm 关联的权重，范围为 1-100。
	Weight int32
	// A node selector term, associated with the corresponding weight.
	Preference NodeSelectorTerm
}

// Taint represents taint that can be applied to the node.
// The node this Taint is attached to has the "effect" on
// any pod that does not tolerate the Taint.
type Taint struct {
	// Required. The taint key to be applied to a node.
	Key string
	// Required. The taint value corresponding to the taint key.
	// +optional
	Value string
	// 必需。此污点对不容忍它的 Pod 的影响。
	// 有效的影响有 NoSchedule、PreferNoSchedule 和 NoExecute。
	Effect TaintEffect
	// TimeAdded 表示添加此污点的时间。
	// +optional
	TimeAdded *metav1.Time
}

// TaintEffect defines the effects of Taint
type TaintEffect string

// These are valid values for TaintEffect
const (
	// 不允许新的 Pod 调度到该节点上，除非它们容忍此污点，
	// 但允许所有未经过调度器而直接提交给 Kubelet 的 Pod 启动，
	// 并允许所有已在运行的 Pod 继续运行。
	// 由调度器强制执行。
	TaintEffectNoSchedule TaintEffect = "NoSchedule"
	// 与 TaintEffectNoSchedule 类似，但调度器会尽量不将新 Pod 调度到该节点上，
	// 而不是完全禁止新 Pod 调度到该节点。由调度器强制执行。
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule"
	// NOT YET IMPLEMENTED. TODO: Uncomment field once it is implemented.
	// Like TaintEffectNoSchedule, but additionally do not allow pods submitted to
	// Kubelet without going through the scheduler to start.
	// Enforced by Kubelet and the scheduler.
	// TaintEffectNoScheduleNoAdmit TaintEffect = "NoScheduleNoAdmit"

	// 驱逐所有不容忍此污点的、已在运行的 Pod。
	// 当前由 NodeController 强制执行。
	TaintEffectNoExecute TaintEffect = "NoExecute"
)

// Toleration represents the toleration object that can be attached to a pod.
// Toleration 所附加到的 Pod 会容忍任何与三元组 <key,value,effect> 匹配的污点，
// 匹配使用操作符 <operator>。
type Toleration struct {
	// Key 是此容忍所适用的污点键。空表示匹配所有污点键。
	// 如果键为空，则操作符必须为 Exists；此组合表示匹配所有的值和所有的键。
	// +optional
	Key string
	// Operator 表示键与值之间的关系。
	// 有效的操作符有 Exists、Equal、Lt 和 Gt。默认为 Equal。
	// Exists 等价于值的通配符，因此 Pod 可以容忍某一类别的所有污点。
	// Lt 和 Gt 执行数值比较（需要启用特性门控 TaintTolerationComparisonOperators）。
	// +optional
	Operator TolerationOperator
	// Value 是此容忍匹配的污点值。
	// 如果操作符为 Exists，则值应为空，否则为普通字符串。
	// +optional
	Value string
	// Effect 指示要匹配的污点影响。空表示匹配所有污点影响。
	// 指定时，允许的值为 NoSchedule、PreferNoSchedule 和 NoExecute。
	// +optional
	Effect TaintEffect
	// TolerationSeconds 表示容忍（其影响必须为 NoExecute，否则忽略此字段）
	// 容忍该污点的时长。默认情况下未设置，表示永久容忍该污点（不驱逐）。
	// 零值和负值将被系统视为 0（立即驱逐）。
	// +optional
	TolerationSeconds *int64
}

// TolerationOperator is the set of operators that can be used in a toleration.
type TolerationOperator string

// These are valid values for TolerationOperator
const (
	TolerationOpExists TolerationOperator = "Exists"
	TolerationOpEqual  TolerationOperator = "Equal"
	TolerationOpLt     TolerationOperator = "Lt"
	TolerationOpGt     TolerationOperator = "Gt"
)

// PodReadinessGate 包含对 Pod 状况的引用。
type PodReadinessGate struct {
	// ConditionType 引用 Pod 状况列表中类型匹配的状况。
	ConditionType PodConditionType
}

// PodSpec is a description of a pod
type PodSpec struct {
	Volumes []Volume
	// List of initialization containers belonging to the pod.
	InitContainers []Container
	// List of containers belonging to the pod.
	Containers []Container
	// 在此 Pod 中运行的临时容器列表。临时容器可以在现有 Pod 中运行，
	// 以执行用户发起的操作（例如调试）。创建 Pod 时不能指定此列表，
	// 也不能通过更新 Pod spec 来修改它。要向现有 Pod 添加临时容器，
	// 请使用该 Pod 的 ephemeralcontainers 子资源。
	// +optional
	EphemeralContainers []EphemeralContainer
	// +optional
	RestartPolicy RestartPolicy
	// Pod 优雅终止所需的可选秒数，可在删除请求中减小。
	// 值必须为非负整数。值为 0 表示通过 kill 信号立即停止（没有关停机会）。
	// 如果此值为 nil，则使用默认的优雅终止时间。
	// 优雅终止时间是指从向 Pod 中运行的进程发送终止信号，
	// 到进程被 kill 信号强制停止之间的秒数。
	// 请将此值设置为长于进程的预期清理时间。
	// 默认为 30 秒。
	// +optional
	TerminationGracePeriodSeconds *int64
	// Optional duration in seconds relative to the StartTime that the pod may be active on a node
	// before the system actively tries to terminate the pod; value must be positive integer
	// +optional
	ActiveDeadlineSeconds *int64
	// 设置 Pod 的 DNS 策略。
	// 默认为 "ClusterFirst"。
	// 有效值为 'ClusterFirstWithHostNet'、'ClusterFirst'、'Default' 或 'None'。
	// DNSConfig 中给定的 DNS 参数将与 DNSPolicy 选择的策略合并。
	// 要与 hostNetwork 一起设置 DNS 选项，必须显式将 DNS 策略指定为 'ClusterFirstWithHostNet'。
	// +optional
	DNSPolicy DNSPolicy
	// NodeSelector is a selector which must be true for the pod to fit on a node
	// +optional
	NodeSelector map[string]string

	// ServiceAccountName is the name of the ServiceAccount to use to run this pod
	// The pod will be allowed to use secrets referenced by the ServiceAccount
	ServiceAccountName string
	// DeprecatedServiceAccount 是 ServiceAccountName 的已弃用别名。
	// Deprecated: 请改用 serviceAccountName。
	// +optional
	DeprecatedServiceAccount string
	// AutomountServiceAccountToken 表示是否应自动挂载 service account token。
	// +optional
	AutomountServiceAccountToken *bool

	// NodeName 表示此 Pod 被调度到哪个节点上。
	// 如果为空，则此 Pod 是由 schedulerName 指定的调度器的候选调度对象。
	// 一旦设置此字段，该节点的 kubelet 就负责此 Pod 的生命周期。
	// 此字段不应用于表达希望将 Pod 调度到特定节点的意愿。
	// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodename
	// +optional
	NodeName string
	// Use the host's network namespace.  If this option is set, the ports that will be
	// used must be specified.
	// Optional: Default to false
	// +optional
	HostNetwork bool
	// Use the host's pid namespace.
	// Optional: Default to false.
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	HostPID bool
	// Use the host's ipc namespace.
	// Optional: Default to false.
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	HostIPC bool
	// Share a single process namespace between all of the containers in a pod.
	// When this is set containers will be able to view and signal processes from other containers
	// in the same pod, and the first process in each container will not be assigned PID 1.
	// HostPID and ShareProcessNamespace cannot both be set.
	// Note that this field cannot be set when spec.os.name is windows.
	// Optional: Default to false.
	// +optional
	ShareProcessNamespace *bool
	// SecurityContext 保存 Pod 级别的安全属性和通用容器设置。
	// 可选：默认为空。各字段的默认值请参见类型说明。
	// +optional
	SecurityContext *PodSecurityContext
	// ImagePullSecrets 是同一命名空间中 secret 引用的可选列表，用于拉取此 PodSpec 中使用的任何镜像。
	// 如果指定，这些 secret 将传递给各个拉取器实现供其使用。
	// +optional
	ImagePullSecrets []LocalObjectReference
	// 指定 Pod 的主机名。
	// 如果未指定，Pod 的主机名将设置为系统定义的值。
	// +optional
	Hostname string
	// 如果指定，Pod 的完整限定主机名为 "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>"。
	// 如果未指定，Pod 将完全没有域名。
	// +optional
	Subdomain string
	// If specified, the pod's scheduling constraints
	// +optional
	Affinity *Affinity
	// If specified, the pod will be dispatched by specified scheduler.
	// 如果未指定，Pod 将由默认调度器分派。
	// +optional
	SchedulerName string
	// If specified, the pod's tolerations.
	// +optional
	Tolerations []Toleration
	// HostAliases is an optional list of hosts and IPs that will be injected into the pod's hosts
	// file if specified.
	// +optional
	HostAliases []HostAlias
	// 如果指定，表示 Pod 的优先级。"system-node-critical" 和
	// "system-cluster-critical" 是两个特殊关键字，表示最高优先级，前者优先级最高。
	// 任何其他名称必须通过创建具有该名称的 PriorityClass 对象来定义。
	// 如果未指定，Pod 优先级将为默认值；如果没有默认值则为 0。
	// +optional
	PriorityClassName string
	// 优先级值。各种系统组件使用此字段查找 Pod 的优先级。
	// 启用 Priority Admission Controller 时，会阻止用户设置此字段。
	// 该准入控制器会根据 PriorityClassName 填充此字段。
	// 值越高，优先级越高。
	// +optional
	Priority *int32
	// 指定 Pod 的 DNS 参数。
	// 此处指定的参数将与基于 DNSPolicy 生成的 DNS 配置合并。
	// +optional
	DNSConfig *PodDNSConfig
	// If specified, all readiness gates will be evaluated for pod readiness.
	// A pod is ready when all its containers are ready AND
	// all conditions specified in the readiness gates have status equal to "True"
	// More info: https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates
	// +optional
	ReadinessGates []PodReadinessGate
	// RuntimeClassName 引用 node.k8s.io 组中的 RuntimeClass 对象，应用于运行此 Pod。
	// 如果没有与指定名称匹配的 RuntimeClass 资源，则不会运行该 Pod。
	// 如果未设置或为空，将使用 "legacy" RuntimeClass，这是一个隐式类，
	// 其定义为空，使用默认的运行时处理程序。
	// More info: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class
	// +optional
	RuntimeClassName *string
	// EnableServiceLinks indicates whether information about services should be injected into pod's
	// 其语法与 Docker links 匹配。
	// If not specified, the default is true.
	// +optional
	EnableServiceLinks *bool
	// PreemptionPolicy 是抢占较低优先级 Pod 的策略。
	// 取 Never、PreemptLowerPriority 之一。
	// 启用 Priority Admission Controller 时，会阻止用户设置此字段。
	// 该准入控制器会根据 PriorityClassName 填充此字段。
	// 如果未设置，默认为 PreemptLowerPriority。
	// +optional
	PreemptionPolicy *PreemptionPolicy
	// Overhead 表示在给定 RuntimeClass 下运行 Pod 相关的资源开销。
	// 此字段将在准入时由 RuntimeClass 准入控制器自动填充。如果启用了
	// RuntimeClass 准入控制器，则 Pod 创建请求中不得设置 overhead。
	// RuntimeClass 准入控制器将拒绝已设置 overhead 的 Pod 创建请求。
	// 如果在 PodSpec 中配置并选择了 RuntimeClass，Overhead 将设置为
	// 相应 RuntimeClass 中定义的值，否则保持未设置并视为 0。
	// More info: https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead
	// +optional
	Overhead ResourceList
	// TopologySpreadConstraints describes how a group of pods ought to spread across topology
	// domains. Scheduler will schedule pods in a way which abides by the constraints.
	// All topologySpreadConstraints are ANDed.
	// +optional
	TopologySpreadConstraints []TopologySpreadConstraint
	// 如果为 true，Pod 的主机名将配置为 Pod 的 FQDN，而不是叶子名称（默认）。
	// 在 Linux 容器中，这意味着在内核的 hostname 字段（struct utsname 的 nodename 字段）中设置 FQDN。
	// 在 Windows 容器中，这意味着将注册表键 HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters 下的 hostname 注册表值设置为 FQDN。
	// 如果 Pod 没有 FQDN，则此设置无效。
	// 默认为 false。
	// +optional
	SetHostnameAsFQDN *bool
	// Specifies the OS of the containers in the pod.
	// Some pod and container fields are restricted if this is set.
	//
	// If the OS field is set to linux, the following fields must be unset:
	// - securityContext.windowsOptions
	//
	// If the OS field is set to windows, following fields must be unset:
	// - spec.hostPID
	// - spec.hostIPC
	// - spec.hostUsers
	// - spec.resources
	// - spec.securityContext.appArmorProfile
	// - spec.securityContext.seLinuxOptions
	// - spec.securityContext.seccompProfile
	// - spec.securityContext.fsGroup
	// - spec.securityContext.fsGroupChangePolicy
	// - spec.securityContext.sysctls
	// - spec.shareProcessNamespace
	// - spec.securityContext.runAsUser
	// - spec.securityContext.runAsGroup
	// - spec.securityContext.supplementalGroups
	// - spec.securityContext.supplementalGroupsPolicy
	// - spec.containers[*].securityContext.appArmorProfile
	// - spec.containers[*].securityContext.seLinuxOptions
	// - spec.containers[*].securityContext.seccompProfile
	// - spec.containers[*].securityContext.capabilities
	// - spec.containers[*].securityContext.readOnlyRootFilesystem
	// - spec.containers[*].securityContext.privileged
	// - spec.containers[*].securityContext.allowPrivilegeEscalation
	// - spec.containers[*].securityContext.procMount
	// - spec.containers[*].securityContext.runAsUser
	// - spec.containers[*].securityContext.runAsGroup
	// +optional
	OS *PodOS

	// Use the host's user namespace.
	// Optional: Default to true.
	// If set to true or not present, the pod will be run in the host user namespace, useful
	// for when the pod needs a feature only available to the host user namespace, such as
	// loading a kernel module with CAP_SYS_MODULE.
	// When set to false, a new user namespace is created for the pod. Setting false is useful
	// for mitigating container breakout vulnerabilities even allowing users to run their
	// containers as root without actually having root privileges on the host.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	HostUsers *bool

	// SchedulingGates 是一个不透明的列表，如果指定将阻止 Pod 被调度。
	// 如果 schedulingGates 非空，Pod 将保持在 SchedulingGated 状态，
	// 调度器不会尝试调度该 Pod。
	// SchedulingGates 只能在 Pod 创建时设置，之后只能删除。
	// +optional
	SchedulingGates []PodSchedulingGate
	// ResourceClaims defines which ResourceClaims must be allocated
	// and reserved before the Pod is allowed to start. The resources
	// will be made available to those containers which consume them
	// by name.
	//
	// This is a stable field but requires that the
	// DynamicResourceAllocation feature gate is enabled.
	//
	// This field is immutable.
	//
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaims []PodResourceClaim
	// Resources is the total amount of CPU and Memory resources required by all
	// 它仅支持为 "cpu"、"memory" 和 "hugepages-" 资源名称指定 Requests 和 Limits。
	// 不支持 ResourceClaims。
	//
	// 此字段可对整个 Pod 的资源分配进行细粒度控制，允许 Pod 内容器之间共享资源。
	// TODO: 进入 beta 阶段时，请扩展此注释以提供更详细的说明。
	// TODO: For beta graduation, expand this comment with a detailed explanation.
	//
	// This is an alpha field and requires enabling the PodLevelResources feature
	// gate.
	//
	// +featureGate=PodLevelResources
	// +optional
	Resources *ResourceRequirements
	// HostnameOverride 为 Pod 自身感知到的主机名指定显式覆盖值。
	// 此字段仅指定 Pod 的主机名，不影响其 DNS 记录。
	// 当此字段设置为非空字符串时：
	// - 它优先于 `hostname` 和 `subdomain` 中设置的值。
	// - Pod 的主机名将设置为此值。
	// - `setHostnameAsFQDN` 必须为 nil 或设置为 false。
	// - `hostNetwork` 必须设置为 false。
	// 此字段必须是 RFC 1123 定义的有效 DNS 子域，且最多包含 64 个字符。
	// +featureGate=HostnameOverride
	// +optional
	HostnameOverride *string
	// SchedulingGroup provides a reference to the immediate scheduling runtime
	// grouping object that this Pod belongs to.
	// This field is used by the scheduler to identify the group and apply the
	// correct group scheduling policies. The association with a group also
	// impacts other lifecycle aspects of a Pod that are relevant in a wider context
	// of scheduling like preemption, resource attachment, etc. If not specified,
	// the Pod is treated as a single unit in all of these aspects.
	// The group object referenced by this field may not exist at the time the
	// Pod is created.
	// This field is immutable, but a group object with the same name may be
	// recreated with different policies. Doing this during pod scheduling
	// may result in the placement not conforming to the expected policies.
	//
	// +featureGate=GenericWorkload
	// +optional
	SchedulingGroup *PodSchedulingGroup

	// evictionResponders 引用基于 EvictionRequests 对驱逐做出响应的响应器。
	// 响应器应通过观察并通过 Eviction Resource API 通信，协助 Pod 的优雅终止。
	// 响应器按其指定优先级依次选择。
	// 响应器应通过更新 Eviction 对象的 .status.responders[].heartbeatTime 字段
	// 定期报告驱逐进度。如果未在 Eviction API 定义的心跳截止时间（目前为 20 分钟）内
	// 更新此字段，驱逐将移交给下一个优先级较低的响应器。如果没有其他响应器，
	// 最后的默认 imperative-eviction.k8s.io/evictor 响应器（优先级为 100）将使用
	// 命令式 Eviction API（pods/<name>/eviction 子资源）驱逐该 Pod。
	// 响应器列表的最大长度为 10。
	// 当 Pod 属于 PodGroup（设置了 .spec.schedulingGroup）时，不支持响应器。
	// 此字段只能在创建时设置，之后不可变。
	// +featureGate=EvictionRequestAPI
	// +optional
	EvictionResponders []EvictionResponder
}

// PodResourceClaim references exactly one ResourceClaim through a ClaimSource.
// 它会添加一个名称，在 Pod 内唯一标识该 ResourceClaim。
// 需要访问该 ResourceClaim 的容器使用此名称引用它。
//
// When the DRAWorkloadResourceClaims feature gate is enabled and this Pod
// belongs to a PodGroup, a PodResourceClaim is matched to a
// PodGroupResourceClaim if all of their fields are equal (Name,
// ResourceClaimName, and ResourceClaimTemplateName). A matched claim references
// a single ResourceClaim shared across all Pods in the PodGroup, reserved for
// the PodGroup in ResourceClaimStatus.ReservedFor rather than for individual
// Pods.
type PodResourceClaim struct {
	// Name 在该 Pod 内唯一标识此资源声明。
	// 必须是 DNS_LABEL。
	Name string

	// ResourceClaimName 是与该 Pod 处于同一命名空间中的 ResourceClaim 对象的名称。
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其中一个。
	ResourceClaimName *string

	// ResourceClaimTemplateName is the name of a ResourceClaimTemplate
	// object in the same namespace as this pod.
	//
	// The template will be used to create a new ResourceClaim, which will
	// be bound to this pod. When this pod is deleted, the ResourceClaim
	// will also be deleted. The pod name and resource name, along with a
	// generated component, will be used to form a unique name for the
	// ResourceClaim, which will be recorded in pod.status.resourceClaimStatuses.
	//
	// When the DRAWorkloadResourceClaims feature gate is enabled and the pod
	// belongs to a PodGroup that defines a PodGroupResourceClaim with the same
	// Name and ResourceClaimTemplateName, this PodResourceClaim resolves to the
	// ResourceClaim generated for the PodGroup. All pods in the group that
	// define an equivalent PodResourceClaim matching the
	// PodGroupResourceClaim's Name and ResourceClaimTemplateName share the same
	// generated ResourceClaim. ResourceClaims generated for a PodGroup are
	// owned by the PodGroup and their lifecycles are tied to the PodGroup
	// instead of any individual pod.
	//
	// This field is immutable and no changes will be made to the
	// corresponding ResourceClaim by the control plane after creating the
	// ResourceClaim.
	//
	// Exactly one of ResourceClaimName and ResourceClaimTemplateName must
	// be set.
	ResourceClaimTemplateName *string
}

// PodResourceClaimStatus is stored in the PodStatus for each PodResourceClaim
// which references a ResourceClaimTemplate. It stores the generated name for
// the corresponding ResourceClaim.
type PodResourceClaimStatus struct {
	// Name 在该 Pod 内唯一标识此资源声明。
	// 必须与 pod.spec.resourceClaims 中某个条目的名称一致，
	// 这意味着该字符串必须是 DNS_LABEL。
	Name string

	// ResourceClaimName 是在 Pod 所在命名空间中为该 Pod 生成的 ResourceClaim 的名称。
	// 当 DRAWorkloadResourceClaims 特性启用，且对应的 PodResourceClaim 与该 Pod 所属
	// PodGroup 创建的 PodGroupResourceClaim 匹配时，此值为为 PodGroup 生成并预留的
	// ResourceClaim 的名称。
	// 如果未设置此值，说明无需生成 ResourceClaim。此时可以忽略 pod.spec.resourceClaims
	// 中的相应条目。
	ResourceClaimName *string
}

// PodExtendedResourceClaimStatus is stored in the PodStatus for the extended
// resource requests backed by DRA. It stores the generated name for
// the corresponding special ResourceClaim created by the scheduler.
type PodExtendedResourceClaimStatus struct {
	// RequestMapping identifies the mapping of <container, extended resource backed by DRA> to  device request
	// in the generated ResourceClaim.
	RequestMappings []ContainerExtendedResourceRequest

	// ResourceClaimName 是在 Pod 所在命名空间中为该 Pod 生成的 ResourceClaim 的名称。
	ResourceClaimName string
}

type ContainerExtendedResourceRequest struct {
	// The name of the container requesting resources.
	ContainerName string
	// The name of the extended resource in that container which gets backed by DRA.
	ResourceName string
	// The name of the request in the special ResourceClaim which corresponds to the extended resource.
	RequestName string
}

// OSName 是可在 OS 中使用的操作系统类型集合。
type OSName string

// These are valid values for OSName
const (
	Linux   OSName = "linux"
	Windows OSName = "windows"
)

// PodOS 定义 Pod 的操作系统参数。
type PodOS struct {
	// Name 是操作系统名称。当前支持的值为 linux 和 windows。
	// 未来可能定义其他值，示例参见：
	// 客户端应能处理其他取值，并将此字段中无法识别的值视为 os: null。
	// https://github.com/opencontainers/runtime-spec/blob/master/config.md#platform-specific-configuration
	Name OSName
}

// PodSchedulingGate 与 Pod 关联，用于阻止其调度。
type PodSchedulingGate struct {
	// Name 是调度门控的名称。
	// 每个调度门控的名称字段必须唯一。
	Name string
}

// PodSchedulingGroup 标识 Pod 所属的运行时调度组实例。
// 调度器使用此信息来应用考虑工作负载的调度语义。
// 必须且只能指定一个字段。
// +union
type PodSchedulingGroup struct {
	// PodGroupName 指定独立 PodGroup 对象的名称，
	// 该对象代表此调度组的运行时实例。
	// 必须是 DNS 子域名。
	// +optional
	// +oneOf=GroupSelection
	PodGroupName *string
}

// HostAlias holds the mapping between IP and hostnames that will be injected as an entry in the
// pod's hosts file.
type HostAlias struct {
	IP        string
	Hostnames []string
}

// Sysctl 定义要设置的内核参数。
type Sysctl struct {
	// Name of a property to set
	Name string
	// Value of a property to set
	Value string
}

// PodFSGroupChangePolicy holds policies that will be used for applying fsGroup to a volume
// when volume is mounted.
type PodFSGroupChangePolicy string

const (
	// FSGroupChangeOnRootMismatch 表示仅当卷根目录的权限和所有权与卷上预期的权限不匹配时，
	// 才更改卷的所有权和权限。这有助于缩短更改卷所有权和权限所需的时间。
	FSGroupChangeOnRootMismatch PodFSGroupChangePolicy = "OnRootMismatch"
	// FSGroupChangeAlways 表示每当卷在 Pod 内挂载时，都应更改其所有权和权限。
	// 这是默认行为。
	FSGroupChangeAlways PodFSGroupChangePolicy = "Always"
)

// SupplementalGroupsPolicy defines how supplemental groups
// of the first container processes are calculated.
type SupplementalGroupsPolicy string

const (
	// SupplementalGroupsPolicyMerge 表示容器提供的 SupplementalGroups 和 FsGroup
	// （在 SecurityContext 中指定）将与容器镜像中定义的主用户组（/etc/group）合并。
	SupplementalGroupsPolicyMerge SupplementalGroupsPolicy = "Merge"
	// SupplementalGroupsPolicyStrict 表示使用容器提供的 SupplementalGroups 和 FsGroup
	// （在 SecurityContext 中指定），而不使用容器镜像中定义的任何组。
	SupplementalGroupsPolicyStrict SupplementalGroupsPolicy = "Strict"
)

// PodSELinuxChangePolicy 定义容器的 SELinux 标签如何应用到 Pod 使用的所有卷上。
type PodSELinuxChangePolicy string

const (
	// Recursive relabeling of all Pod volumes by the container runtime.
	// This may be slow for large volumes, but allows mixing privileged and unprivileged Pods sharing the same volume on the same node.
	SELinuxChangePolicyRecursive PodSELinuxChangePolicy = "Recursive"
	// MountOption mounts all eligible Pod volumes with `-o context` mount option.
	// This requires all Pods that share the same volume to use the same SELinux label.
	// It is not possible to share the same volume among privileged and unprivileged Pods.
	// Eligible volumes are in-tree FibreChannel and iSCSI volumes, and all CSI volumes
	// whose CSI driver announces SELinux support by setting spec.seLinuxMount: true in their
	// CSIDriver instance. Other volumes are always re-labelled recursively.
	SELinuxChangePolicyMountOption PodSELinuxChangePolicy = "MountOption"
)

// PodSecurityContext 保存 Pod 级别的安全属性和通用容器设置。
// 部分字段也存在于 container.securityContext 中。
// container.securityContext 的字段值优先于 PodSecurityContext 的字段值。
type PodSecurityContext struct {
	// The SELinux context to be applied to all containers.
	// If unspecified, the container runtime will allocate a random SELinux context for each
	// container.  May also be set in SecurityContext.  If set in
	// both SecurityContext and PodSecurityContext, the value specified in SecurityContext
	// takes precedence for that container.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	SELinuxOptions *SELinuxOptions
	// The Windows specific settings applied to all containers.
	// If unspecified, the options within a container's SecurityContext will be used.
	// If set in both SecurityContext and PodSecurityContext, the value specified in SecurityContext takes precedence.
	// Note that this field cannot be set when spec.os.name is linux.
	// +optional
	WindowsOptions *WindowsSecurityContextOptions
	// The UID to run the entrypoint of the container process.
	// Defaults to user specified in image metadata if unspecified.
	// May also be set in SecurityContext.  If set in both SecurityContext and
	// PodSecurityContext, the value specified in SecurityContext takes precedence
	// for that container.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	RunAsUser *int64
	// The GID to run the entrypoint of the container process.
	// Uses runtime default if unset.
	// May also be set in SecurityContext.  If set in both SecurityContext and
	// PodSecurityContext, the value specified in SecurityContext takes precedence
	// for that container.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	RunAsGroup *int64
	// Indicates that the container must run as a non-root user.
	// If true, the Kubelet will validate the image at runtime to ensure that it
	// does not run as UID 0 (root) and fail to start the container if it does.
	// If unset or false, no such validation will be performed.
	// May also be set in SecurityContext.  If set in both SecurityContext and
	// PodSecurityContext, the value specified in SecurityContext takes precedence
	// for that container.
	// +optional
	RunAsNonRoot *bool
	// A list of groups applied to the first process run in each container, in
	// addition to the container's primary GID and fsGroup (if specified).  If
	// the SupplementalGroupsPolicy feature is enabled, the
	// supplementalGroupsPolicy field determines whether these are in addition
	// to or instead of any group memberships defined in the container image.
	// If unspecified, no additional groups are added, though group memberships
	// defined in the container image may still be used, depending on the
	// supplementalGroupsPolicy field.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	SupplementalGroups []int64
	// Defines how supplemental groups of the first container processes are calculated.
	// Valid values are "Merge" and "Strict". If not specified, "Merge" is used.
	// (Alpha) Using the field requires the SupplementalGroupsPolicy feature gate to be enabled
	// and the container runtime must implement support for this feature.
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// TODO: update the default value to "Merge" when spec.os.name is not windows in v1.34
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	SupplementalGroupsPolicy *SupplementalGroupsPolicy
	// A special supplemental group that applies to all containers in a pod.
	// Some volume types allow the Kubelet to change the ownership of that volume
	// to be owned by the pod:
	//
	// 1. The owning GID will be the FSGroup
	// 2. The setgid bit is set (new files created in the volume will be owned by FSGroup)
	// 3. The permission bits are OR'd with rw-rw----
	//
	// If unset, the Kubelet will not modify the ownership and permissions of any volume.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	FSGroup *int64
	// Sysctls 保存该 Pod 使用的命名空间级 sysctl 列表。
	// 如果 Pod 使用了容器运行时不支持的 sysctl，可能无法启动。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Sysctls []Sysctl
	// fsGroupChangePolicy defines behavior of changing ownership and permission of the volume
	// before being exposed inside Pod. This field will only apply to
	// volume types which support fsGroup based ownership(and permissions).
	// It will have no effect on ephemeral volume types such as: secret, configmaps
	// and emptydir.
	// Valid values are "OnRootMismatch" and "Always". If not specified, "Always" is used.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	FSGroupChangePolicy *PodFSGroupChangePolicy
	// The seccomp options to use by the containers in this pod.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	SeccompProfile *SeccompProfile
	// appArmorProfile is the AppArmor options to use by the containers in this pod.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	AppArmorProfile *AppArmorProfile
	// SELinuxChangePolicy 定义容器的 SELinux 标签如何应用到 Pod 使用的所有卷上。
	// 对不支持 SELinux 的节点或不支持 SELinux 的卷无效。
	// 有效值为 "MountOption" 和 "Recursive"。
	// "Recursive" 表示由容器运行时对所有 Pod 卷上的所有文件重新打标签。
	// 对于大容量卷这可能较慢，但允许在同一节点上共享同一卷的特权 Pod 与非特权 Pod 混用。
	// "MountOption" 使用 `-o context` 挂载选项挂载所有符合条件的 Pod 卷。
	// 这要求共享同一卷的所有 Pod 使用相同的 SELinux 标签。
	// 特权 Pod 与非特权 Pod 之间无法共享同一卷。
	// 符合条件的卷包括树内 FibreChannel 和 iSCSI 卷，以及所有在其 CSIDriver 实例中
	// 通过设置 spec.seLinuxMount: true 声明支持 SELinux 的 CSI 卷。
	// 其他卷始终递归重新打标签。
	// 如果未指定，则使用 "MountOption"。
	// 此字段仅影响设置了 SELinux 标签的 Pod，无论是在 PodSecurityContext 中，
	// 还是在所有容器的 SecurityContext 中设置的标签。
	// 使用同一卷的所有 Pod 应使用相同的 seLinuxChangePolicy，
	// 否则某些 Pod 可能卡在 ContainerCreating 状态。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +featureGate=SELinuxChangePolicy
	// +optional
	SELinuxChangePolicy *PodSELinuxChangePolicy
}

// SeccompProfile 定义 Pod/容器的 seccomp 配置设置。
// 只能设置一种配置来源。
// +union
type SeccompProfile struct {
	// +unionDiscriminator
	Type SeccompProfileType
	// Load a profile defined in static file on the node.
	// 该配置必须预先在节点上配置好才能生效。
	// LocalhostProfile cannot be an absolute nor a descending path.
	// +optional
	LocalhostProfile *string
}

// SeccompProfileType 定义支持的 seccomp 配置类型。
type SeccompProfileType string

const (
	// SeccompProfileTypeUnconfined is when no seccomp profile is applied (A.K.A. unconfined).
	SeccompProfileTypeUnconfined SeccompProfileType = "Unconfined"
	// SeccompProfileTypeRuntimeDefault 表示容器运行时的默认 seccomp 配置。
	SeccompProfileTypeRuntimeDefault SeccompProfileType = "RuntimeDefault"
	// SeccompProfileTypeLocalhost represents custom made profiles stored on the node's disk.
	SeccompProfileTypeLocalhost SeccompProfileType = "Localhost"
)

// AppArmorProfile 定义 Pod 或容器的 AppArmor 设置。
// +union
type AppArmorProfile struct {
	// Type 指示将应用哪种 AppArmor 配置。
	// 有效选项为：
	// Localhost - 节点上预加载的配置。
	// RuntimeDefault - 容器运行时的默认配置。
	// Unconfined - 不实施 AppArmor 限制。
	// +unionDiscriminator
	Type AppArmorProfileType

	// localhostProfile indicates a profile loaded on the node that should be used.
	// The profile must be preconfigured on the node to work.
	// Must match the loaded name of the profile.
	// Must be set if and only if type is "Localhost".
	// +optional
	LocalhostProfile *string
}

// +enum
type AppArmorProfileType string

const (
	// AppArmorProfileTypeUnconfined 表示不实施任何 AppArmor 配置。
	AppArmorProfileTypeUnconfined AppArmorProfileType = "Unconfined"
	// AppArmorProfileTypeRuntimeDefault 表示使用容器运行时的默认 AppArmor 配置。
	AppArmorProfileTypeRuntimeDefault AppArmorProfileType = "RuntimeDefault"
	// AppArmorProfileTypeLocalhost 表示使用节点上预加载的配置。
	AppArmorProfileTypeLocalhost AppArmorProfileType = "Localhost"
)

// PodQOSClass 定义 Pod 支持的 QoS 类别。
type PodQOSClass string

// These are valid values for PodQOSClass
const (
	// PodQOSGuaranteed is the Guaranteed qos class.
	PodQOSGuaranteed PodQOSClass = "Guaranteed"
	// PodQOSBurstable is the Burstable qos class.
	PodQOSBurstable PodQOSClass = "Burstable"
	// PodQOSBestEffort is the BestEffort qos class.
	PodQOSBestEffort PodQOSClass = "BestEffort"
)

// PodDNSConfig defines the DNS parameters of a pod in addition to
// those generated from DNSPolicy.
type PodDNSConfig struct {
	// Nameservers 是 DNS 名称服务器 IP 地址列表。
	// 会追加到由 DNSPolicy 生成的基础名称服务器列表之后。
	// 重复的名称服务器会被去除。
	// +optional
	Nameservers []string
	// Searches 是用于主机名查找的 DNS 搜索域列表。
	// 会追加到由 DNSPolicy 生成的基础搜索路径之后。
	// 重复的搜索路径会被去除。
	// +optional
	Searches []string
	// Options 是 DNS 解析器选项列表。
	// 会与由 DNSPolicy 生成的基础选项合并。
	// 重复的条目会被去除。Options 中给出的解析选项
	// 会覆盖基础 DNSPolicy 中出现的选项。
	// +optional
	Options []PodDNSConfigOption
}

// PodDNSConfigOption 定义 Pod 的 DNS 解析器选项。
type PodDNSConfigOption struct {
	// Required.
	Name string
	// +optional
	Value *string
}

// PodIP 表示分配给 Pod 的单个 IP 地址。
type PodIP struct {
	// IP 是分配给 Pod 的 IP 地址。
	IP string
}

// HostIP 表示分配给主机的单个 IP 地址。
type HostIP struct {
	// IP is the IP address assigned to the host
	IP string
}

// EphemeralContainerCommon 是 Container 中所有字段的副本，用于内联到 EphemeralContainer 中。
// 这个独立类型便于将 EphemeralContainer 转换为 Container，
// 也便于单独为 EphemeralContainer 的字段编写文档。
// 向 Container 添加新字段时，也必须在此处添加。
type EphemeralContainerCommon struct {
	// Required: This must be a DNS_LABEL.  Each container in a pod must
	// have a unique name.
	Name string
	// Required.
	Image string
	// Optional: The container image's entrypoint is used if this is not provided; cannot be updated.
	// Variable references $(VAR_NAME) are expanded using the container's environment.  If a variable
	// cannot be resolved, the reference in the input string will be unchanged.  Double $$ are reduced
	// to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will
	// produce the string literal "$(VAR_NAME)".  Escaped references will never be expanded, regardless
	// of whether the variable exists or not.
	// +optional
	Command []string
	// Optional: The container image's cmd is used if this is not provided; cannot be updated.
	// Variable references $(VAR_NAME) are expanded using the container's environment.  If a variable
	// cannot be resolved, the reference in the input string will be unchanged.  Double $$ are reduced
	// to a single $, which allows for escaping the $(VAR_NAME) syntax: i.e. "$$(VAR_NAME)" will
	// produce the string literal "$(VAR_NAME)".  Escaped references will never be expanded, regardless
	// of whether the variable exists or not.
	// +optional
	Args []string
	// Optional: Defaults to the container runtime's default working directory.
	// +optional
	WorkingDir string
	// Ports 不允许为临时容器设置端口。
	// +optional
	Ports []ContainerPort
	// List of sources to populate environment variables in the container.
	// The keys defined within a source may consist of any printable ASCII characters except '='.
	// All invalid keys
	// will be reported as an event when the container is starting. When a key exists in multiple
	// sources, the value associated with the last source will take precedence.
	// Values defined by an Env with a duplicate key will take precedence.
	// Cannot be updated.
	// +optional
	EnvFrom []EnvFromSource
	// +optional
	Env []EnvVar
	// Resources 不允许为临时容器设置资源。临时容器使用已分配给 Pod 的空闲资源。
	// +optional
	Resources ResourceRequirements
	// Resources resize policy for the container.
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	ResizePolicy []ContainerResizePolicy
	// Restart policy for the container to manage the restart behavior of each
	// container within a pod. Must be specified if restartPolicyRules are used.
	// You cannot set this field on ephemeral containers.
	// +optional
	RestartPolicy *ContainerRestartPolicy
	// Represents a list of rules to be checked to determine if the
	// container should be restarted on exit. The rules are evaluated in
	// order. Once a rule matches a container exit condition, the remaining
	// rules are ignored. If no rule matches the container exit condition,
	// the Pod-level restart policy determines the whether the container
	// is restarted or not. Constraints on the rules:
	// - At most 20 rules are allowed.
	// - Rules can have the same action.
	// - Identical rules are not forbidden in validations.
	// +featureGate=ContainerRestartRules
	// +optional
	// +listType=atomic
	RestartPolicyRules []ContainerRestartRule
	// Pod volumes to mount into the container's filesystem. Subpath mounts are not allowed for ephemeral containers.
	// +optional
	VolumeMounts []VolumeMount
	// volumeDevices is the list of block devices to be used by the container.
	// +optional
	VolumeDevices []VolumeDevice
	// Probes are not allowed for ephemeral containers.
	// +optional
	LivenessProbe *Probe
	// Probes are not allowed for ephemeral containers.
	// +optional
	ReadinessProbe *Probe
	// Probes are not allowed for ephemeral containers.
	// +optional
	StartupProbe *Probe
	// Lifecycle 不允许为临时容器设置生命周期钩子。
	// +optional
	Lifecycle *Lifecycle
	// Required.
	// +optional
	TerminationMessagePath string
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy
	// Required: Policy for pulling images for this container
	ImagePullPolicy PullPolicy
	// SecurityContext 定义临时容器运行时的安全选项（可选）。
	// 如果设置，SecurityContext 中的字段会覆盖 PodSecurityContext 中的对应字段。
	// +optional
	SecurityContext *SecurityContext

	// 交互式容器的变量，这些有非常专门的用例（例如调试），
	// 不应用于通用容器。
	// +optional
	Stdin bool
	// +optional
	StdinOnce bool
	// +optional
	TTY bool
}

// EphemeralContainerCommon converts to Container. All fields must be kept in sync between
// these two types.
var _ = Container(EphemeralContainerCommon{})

// An EphemeralContainer is a temporary container that you may add to an existing Pod for
// user-initiated activities such as debugging. Ephemeral containers have no resource or
// scheduling guarantees, and they will not be restarted when they exit or when a Pod is
// removed or restarted. The kubelet may evict a Pod if an ephemeral container causes the
// Pod to exceed its resource allocation.
//
// 要添加临时容器，请使用现有 Pod 的 ephemeralcontainers 子资源。
// 临时容器不能被移除或重启。
type EphemeralContainer struct {
	// Ephemeral containers have all of the fields of Container, plus additional fields
	// specific to ephemeral containers. Fields in common with Container are in the
	// following inlined struct so than an EphemeralContainer may easily be converted
	// to a Container.
	EphemeralContainerCommon

	// If set, the name of the container from PodSpec that this ephemeral container targets.
	// The ephemeral container will be run in the namespaces (IPC, PID, etc) of this container.
	// If not set then the ephemeral container uses the namespaces configured in the Pod spec.
	//
	// The container runtime must implement support for this feature. If the runtime does not
	// support namespace targeting then the result of setting this field is undefined.
	// +optional
	TargetContainerName string
}

// EvictionResponder allows you to specify the responder reacting to an Eviction.
// Responders should observe and communicate through the Eviction Resource API to help with
// the graceful eviction of a target (e.g. termination of a pod).
// +structType=atomic
type EvictionResponder struct {
	// Name 用于标识响应 Eviction 的响应者。
	// 必须是有效的带域名前缀的键（例如 "acme.io/foo"）。
	// 域名 *.k8s.io 和 *.kubernetes.io 保留。
	// 每个响应者的此字段必须唯一。
	// 此字段必填。
	// +required
	Name string

	// Priority 是此响应者的优先级。evictionrequest-controller 会优先选择优先级更高的响应者。
	// 如果存在优先级相同的响应者，则选择域名按字母序靠前的响应者。
	// 这意味着先按字母序比较顶级域名标签，再比较下级域名标签，最后比较键。
	// 作为 Pod 管理控制器的响应者应将此字段值设为 10000，
	// 以便其他响应者进行抢占或回退注册。
	// 最小值为 0，最大值为 100000。
	// 0-999 区间保留给后缀为 *.k8s.io 的响应者。
	// 此字段必填。
	// +required
	Priority *int32
}

// PodStatus represents information about the status of a pod. Status may trail the actual
// state of a system.
type PodStatus struct {
	// ObservedGeneration 如果设置，表示 Pod 状态是基于哪个 .metadata.generation 设置的。
	// +optional
	ObservedGeneration int64
	// +optional
	Phase PodPhase
	// +optional
	Conditions []PodCondition
	// Message 是人类可读的消息，说明 Pod 处于当前状态的详细原因。
	// +optional
	Message string
	// A brief CamelCase message indicating details about why the pod is in this state. e.g. 'Evicted'
	// +optional
	Reason string
	// nominatedNodeName is set when this pod preempts other pods on the node, but it cannot be
	// scheduled right away as preemption victims receive their graceful termination periods.
	// This field does not guarantee that the pod will be scheduled on this node. Scheduler may decide
	// to place the pod elsewhere if other nodes become available sooner. Scheduler may also decide to
	// give the resources on this node to a higher priority pod that is created after preemption.
	// +optional
	NominatedNodeName string

	// HostIP 保存 Pod 被分配到的主机的 IP 地址。Pod 尚未启动时为空。
	// Pod 可能被分配到 kubelet 有问题的节点，这意味着即使 Pod 已分配到节点，
	// HostIP 也不会更新。
	// +optional
	HostIP string

	// HostIPs holds the IP addresses allocated to the host. If this field is specified, the first entry must
	// match the hostIP field. This list is empty if the pod has not started yet.
	// A pod can be assigned to a node that has a problem in kubelet which in turns means that HostIPs will
	// not be updated even if there is a node is assigned to this pod.
	// match the hostIP field. This list is empty if no IPs have been allocated yet.
	// +optional
	HostIPs []HostIP

	// PodIP address allocated to the pod, kept in sync with PodIPs[0] by the
	// pod registry strategy. Routable at least within the cluster. Empty if
	// not yet allocated.
	// +optional
	PodIP string
	// PodIPs holds all of the known IP addresses allocated to the pod. Pods may be assigned AT MOST
	// one value for each of IPv4 and IPv6.
	// +optional
	PodIPs []PodIP

	// Date and time at which the object was acknowledged by the Kubelet.
	// This is before the Kubelet pulled the container image(s) for the pod.
	// +optional
	StartTime *metav1.Time

	// Statuses of init containers in this pod. The most recent successful non-restartable
	// init container will have ready = true, the most recently started container will have
	// startTime set.
	// Each init container in the pod should have at most one status in this list,
	// and all statuses should be for containers in the pod.
	// However this is not enforced.
	// If a status for a non-existent container is present in the list, or the list has duplicate names,
	// the behavior of various Kubernetes components is not defined and those statuses might be
	// ignored.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
	InitContainerStatuses []ContainerStatus

	// Statuses of containers in this pod.
	// Each container in the pod should have at most one status in this list,
	// and all statuses should be for containers in the pod.
	// However this is not enforced.
	// If a status for a non-existent container is present in the list, or the list has duplicate names,
	// the behavior of various Kubernetes components is not defined and those statuses might be
	// ignored.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
	// +optional
	ContainerStatuses []ContainerStatus
	// +optional
	QOSClass PodQOSClass

	// Statuses for any ephemeral containers that have run in this pod.
	// Each ephemeral container in the pod should have at most one status in this list,
	// and all statuses should be for containers in the pod.
	// However this is not enforced.
	// If a status for a non-existent container is present in the list, or the list has duplicate names,
	// the behavior of various Kubernetes components is not defined and those statuses might be
	// ignored.
	// More info: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
	// +optional
	EphemeralContainerStatuses []ContainerStatus

	// Resize 是 Pod 容器期望的资源调整状态。
	// 如果没有待处理的资源调整，则为空。
	// 对容器资源的任何更改都会自动将其设置为 "Proposed"。
	// Deprecated: 调整状态已迁移到两个 Pod 条件：PodResizePending 和 PodResizeInProgress。
	// PodResizePending 用于跟踪 spec 已调整但 Kubelet 尚未分配资源的状态。
	// PodResizeInProgress 用于跟踪进行中的调整，当已分配资源 != 已确认资源时应存在该条件。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	Resize PodResizeStatus

	// Status of resource claims.
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaimStatuses []PodResourceClaimStatus

	// Status of claim of extended resource backed by DRA.
	// +featureGate=DRAExtendedResource
	// +optional
	ExtendedResourceClaimStatus *PodExtendedResourceClaimStatus

	// AllocatedResources 是节点为此 Pod 分配的总请求量。
	// 如果未设置 Pod 级别请求，则为 Pod 内各容器请求量的总和。
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	AllocatedResources ResourceList

	// Resources represents the compute resource requests and limits that have been
	// applied at the pod level
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	Resources *ResourceRequirements

	// NodeAllocatableResourceClaimStatuses contains the status of node-allocatable resources
	// that were allocated for this pod through DRA claims. This includes resources currently
	// reported in v1.Node `status.allocatable` that are not extended resources
	// (see https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources).
	// Examples include "cpu", "memory", "ephemeral-storage", and hugepages.
	// +featureGate=DRANodeAllocatableResources
	// +optional
	// +listType=map
	// +listMapKey=resourceClaimName
	NodeAllocatableResourceClaimStatuses []NodeAllocatableResourceClaimStatus

	// volumeHealth contains node-reported health for each volume the pod is using.
	// Populated by the kubelet on the pod's node.
	// +featureGate=CSIVolumeHealth
	// +optional
	// +listType=map
	// +listMapKey=name
	VolumeHealth []PodVolumeHealth
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pod is a collection of containers, used as either input (create, update) or as output (list, get).
type Pod struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the behavior of a pod.
	// +optional
	Spec PodSpec

	// Status represents the current information about a pod. This data may not be up
	// to date.
	// +optional
	Status PodStatus
}

// PodTemplateSpec 描述从模板创建 Pod 时该 Pod 应具备的数据。
type PodTemplateSpec struct {
	// Metadata of the pods created from this template.
	// +optional
	metav1.ObjectMeta

	// Spec defines the behavior of a pod.
	// +optional
	Spec PodSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTemplate 描述用于创建预定义 Pod 副本的模板。
type PodTemplate struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Template 定义将从此 Pod 模板创建的 Pod。
	// +optional
	Template PodTemplateSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodTemplateList 是 PodTemplate 的列表。
type PodTemplateList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []PodTemplate
}

// ReplicationControllerSpec 是副本控制器的规格说明。
// As the internal representation of a replication controller, it may have either
// a TemplateRef or a Template set.
type ReplicationControllerSpec struct {
	// Replicas is the number of desired replicas.
	Replicas *int32

	// 新创建的 Pod 在被视为可用之前，需要保持就绪且容器不崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32

	// Selector is a label query over pods that should match the Replicas count.
	Selector map[string]string

	// TemplateRef is a reference to an object that describes the pod that will be created if
	// insufficient replicas are detected. This reference is ignored if a Template is set.
	// Must be set before converting to a versioned API object
	// +optional
	// TemplateRef *ObjectReference

	// Template 是描述当检测到副本不足时要创建的 Pod 的对象。其优先级高于 TemplateRef。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +optional
	Template *PodTemplateSpec
}

// ReplicationControllerStatus represents the current status of a replication
// controller.
type ReplicationControllerStatus struct {
	// Replicas is the number of actual replicas.
	Replicas int32

	// 标签与副本控制器 Pod 模板的标签相匹配的 Pod 数量。
	// +optional
	FullyLabeledReplicas int32

	// 此副本控制器中就绪的副本数。
	// +optional
	ReadyReplicas int32

	// 此副本控制器中可用的副本数（至少就绪 minReadySeconds 秒）。
	// +optional
	AvailableReplicas int32

	// ObservedGeneration is the most recent generation observed by the controller.
	// +optional
	ObservedGeneration int64

	// 表示对副本控制器当前状态的最新可用观测。
	// +optional
	Conditions []ReplicationControllerCondition
}

// ReplicationControllerConditionType defines the conditions of a replication controller.
type ReplicationControllerConditionType string

// 以下是副本控制器的有效条件。
const (
	// 当副本控制器的某个 Pod 因配额不足、限制范围、Pod 安全策略、节点选择器等原因
	// 无法创建，或因 kubelet 宕机、finalizer 失败而被删除时，
	// 会为该副本控制器添加 ReplicationControllerReplicaFailure 条件。
	ReplicationControllerReplicaFailure ReplicationControllerConditionType = "ReplicaFailure"
)

// ReplicationControllerCondition 描述副本控制器在某一时刻的状态。
type ReplicationControllerCondition struct {
	// Type of replication controller condition.
	Type ReplicationControllerConditionType
	// 条件的状态，取值为 True、False、Unknown 之一。
	Status ConditionStatus
	// The last time the condition transitioned from one status to another.
	// +optional
	LastTransitionTime metav1.Time
	// 条件最近一次状态变化的原因。
	// +optional
	Reason string
	// 用自然语言描述状态变化的详细信息。
	// +optional
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicationController 表示一个副本控制器的配置。
type ReplicationController struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired behavior of this replication controller.
	// +optional
	Spec ReplicationControllerSpec

	// Status is the current status of this replication controller. This data may be
	// 在一段时间内处于过期状态。
	// +optional
	Status ReplicationControllerStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ReplicationControllerList 是副本控制器的集合。
type ReplicationControllerList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []ReplicationController
}

const (
	// ClusterIPNone - 不分配集群 IP。
	// 无需代理，也不应为 Pod 创建环境变量。
	ClusterIPNone = "None"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceList 保存 Service 的列表。
type ServiceList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Service
}

// ServiceAffinity Type string
type ServiceAffinity string

const (
	// ServiceAffinityClientIP 表示基于客户端 IP 的会话亲和。
	ServiceAffinityClientIP ServiceAffinity = "ClientIP"

	// ServiceAffinityNone 表示无会话亲和。
	ServiceAffinityNone ServiceAffinity = "None"
)

const (
	// DefaultClientIPServiceAffinitySeconds is the default timeout seconds
	// of Client IP based session affinity - 3 hours.
	DefaultClientIPServiceAffinitySeconds int32 = 10800
	// MaxClientIPServiceAffinitySeconds is the max timeout seconds
	// of Client IP based session affinity - 1 day.
	MaxClientIPServiceAffinitySeconds int32 = 86400
)

// SessionAffinityConfig 表示会话亲和的配置。
type SessionAffinityConfig struct {
	// clientIP 包含基于客户端 IP 的会话亲和配置。
	// +optional
	ClientIP *ClientIPConfig
}

// ClientIPConfig 表示基于客户端 IP 的会话亲和配置。
type ClientIPConfig struct {
	// timeoutSeconds 指定 ClientIP 类型会话保持的秒数。
	// 当 ServiceAffinity == "ClientIP" 时，该值必须 >0 && <=86400（1 天）。
	// 默认值为 10800（3 小时）。
	// +optional
	TimeoutSeconds *int32
}

// ServiceType string describes ingress methods for a service
type ServiceType string

const (
	// ServiceTypeClusterIP means a service will only be accessible inside the
	// cluster, via the ClusterIP.
	ServiceTypeClusterIP ServiceType = "ClusterIP"

	// ServiceTypeNodePort 表示在 'ClusterIP' 类型的基础上，在每个节点的某个端口上暴露 Service。
	ServiceTypeNodePort ServiceType = "NodePort"

	// ServiceTypeLoadBalancer 表示在 'NodePort' 类型的基础上，通过外部负载均衡器
	// （如果云厂商支持）暴露 Service。
	ServiceTypeLoadBalancer ServiceType = "LoadBalancer"

	// ServiceTypeExternalName 表示 Service 仅包含一个对外部名称的引用，
	// kubedns 或类似组件会将其作为 CNAME 记录返回，不涉及任何 Pod 的暴露或代理。
	ServiceTypeExternalName ServiceType = "ExternalName"
)

// ServiceInternalTrafficPolicy describes the endpoint-selection policy for
// traffic sent to the ClusterIP.
type ServiceInternalTrafficPolicy string

const (
	// ServiceInternalTrafficPolicyCluster 将流量路由到所有端点。
	ServiceInternalTrafficPolicyCluster ServiceInternalTrafficPolicy = "Cluster"

	// ServiceInternalTrafficPolicyLocal 仅将流量路由到与客户端 Pod 位于同一节点的端点
	// （如果没有本地端点则丢弃流量）。
	ServiceInternalTrafficPolicyLocal ServiceInternalTrafficPolicy = "Local"
)

// ServiceExternalTrafficPolicy describes the endpoint-selection policy for
// traffic to external service entrypoints (NodePorts, ExternalIPs, and
// LoadBalancer IPs).
type ServiceExternalTrafficPolicy string

const (
	// ServiceExternalTrafficPolicyCluster 将流量路由到所有端点。
	ServiceExternalTrafficPolicyCluster ServiceExternalTrafficPolicy = "Cluster"

	// ServiceExternalTrafficPolicyLocal 仅将流量路由到接收该流量的节点上的端点，
	// 从而保留流量的源 IP（如果没有本地端点则丢弃流量）。
	ServiceExternalTrafficPolicyLocal ServiceExternalTrafficPolicy = "Local"
)

// 以下是 Service 的 TrafficDistribution 字段的有效值。
const (
	// ServiceTrafficDistributionPreferSameZone 表示倾向于将流量路由到与客户端位于同一可用区的端点。
	// 用户只有在确保客户端与端点的分布不会因"同可用区"偏好导致端点过载时，才应设置此值。
	ServiceTrafficDistributionPreferSameZone = "PreferSameZone"

	// ServiceTrafficDistributionPreferSameNode 表示倾向于将流量路由到与客户端位于同一节点的端点。
	// 用户只有在确保客户端与端点的分布不会因"同节点"偏好导致端点过载时，才应设置此值。
	ServiceTrafficDistributionPreferSameNode = "PreferSameNode"

	// ServiceTrafficDistributionPreferClose 是 "PreferSameZone" 的原名。
	// 尽管名称听起来很宽泛，其含义与 "PreferSameZone" 完全相同。
	// Deprecated: 请改用 "PreferSameZone"。
	ServiceTrafficDistributionPreferClose = "PreferClose"
)

// 以下是 Service 的有效条件。
const (
	// LoadBalancerPortsError 表示云负载均衡器实例上所请求端口的条件。
	LoadBalancerPortsError = "LoadBalancerPortsError"
)

// ServiceStatus represents the current status of a service
type ServiceStatus struct {
	// LoadBalancer 包含负载均衡器的当前状态（如果存在）。
	// +optional
	LoadBalancer LoadBalancerStatus

	// Current service condition
	// +optional
	Conditions []metav1.Condition
}

// LoadBalancerStatus represents the status of a load-balancer
type LoadBalancerStatus struct {
	// Ingress 是包含负载均衡器入口点的列表。
	// 发往该 Service 的流量应发送到这些入口点。
	// +optional
	Ingress []LoadBalancerIngress
}

// LoadBalancerIngress 表示负载均衡器入口点的状态：
// 发往该 Service 的流量应发送到某个入口点。
type LoadBalancerIngress struct {
	// IP 用于基于 IP 的负载均衡器入口点（通常是 GCE 或 OpenStack 负载均衡器）。
	// +optional
	IP string

	// Hostname 用于基于 DNS 的负载均衡器入口点（通常是 AWS 负载均衡器）。
	// +optional
	Hostname string

	// IPMode specifies how the load-balancer IP behaves, and may only be specified when the ip field is specified.
	// Setting this to "VIP" indicates that traffic is delivered to the node with
	// the destination set to the load-balancer's IP and port.
	// Setting this to "Proxy" indicates that traffic is delivered to the node or pod with
	// the destination set to the node's IP and node port or the pod's IP and port.
	// Service implementations may use this information to adjust traffic routing.
	// +optional
	IPMode *LoadBalancerIPMode

	// Ports 是 Service 端口记录的列表。
	// 如果使用该字段，Service 中定义的每个端口都应有一条对应记录。
	// +optional
	Ports []PortStatus
}

// IPFamily 表示 IP 协议族（IPv4 或 IPv6）。该类型用于表达某个类型所描述的 IP 的协议族
// （例如 service.spec.ipFamilies）。
type IPFamily string

const (
	// IPv4Protocol 表示此 IP 为 IPv4 协议。
	IPv4Protocol IPFamily = "IPv4"
	// IPv6Protocol 表示此 IP 为 IPv6 协议。
	IPv6Protocol IPFamily = "IPv6"
)

// IPFamilyPolicy 表示 Service 请求或要求的双栈特性。
type IPFamilyPolicy string

const (
	// IPFamilyPolicySingleStack indicates that this service is required to have a single IPFamily.
	// The IPFamily assigned is based on the default IPFamily used by the cluster
	// or as identified by service.spec.ipFamilies field
	IPFamilyPolicySingleStack IPFamilyPolicy = "SingleStack"
	// IPFamilyPolicyPreferDualStack indicates that this service prefers dual-stack when
	// the cluster is configured for dual-stack. If the cluster is not configured
	// for dual-stack the service will be assigned a single IPFamily. If the IPFamily is not
	// set in service.spec.ipFamilies then the service will be assigned the default IPFamily
	// configured on the cluster
	IPFamilyPolicyPreferDualStack IPFamilyPolicy = "PreferDualStack"
	// IPFamilyPolicyRequireDualStack indicates that this service requires dual-stack. Using
	// IPFamilyPolicyRequireDualStack on a single stack cluster will result in validation errors. The
	// IPFamilies (and their order) assigned  to this service is based on service.spec.ipFamilies. If
	// service.spec.ipFamilies was not provided then it will be assigned according to how they are
	// configured on the cluster. If service.spec.ipFamilies has only one entry then the alternative
	// IPFamily will be added by apiserver
	IPFamilyPolicyRequireDualStack IPFamilyPolicy = "RequireDualStack"
)

// ServiceSpec describes the attributes that a user creates on a service
type ServiceSpec struct {

	// 此 Service 暴露的端口列表。
	Ports []ServicePort

	// 将 Service 流量路由到标签键和值与此 selector 匹配的 Pod。
	// 如果为空或不存在，则认为该 Service 由外部进程管理其端点，Kubernetes 不会修改。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。
	// 当类型为 ExternalName 时忽略。
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/
	Selector map[string]string

	// ClusterIP is the IP address of the service and is usually assigned
	// randomly by the master. If an address is specified manually and is not in
	// use by others, it will be allocated to the service; otherwise, creation
	// of the service will fail. This field can not be changed through updates.
	// Valid values are "None", empty string (""), or a valid IP address. "None"
	// can be specified for headless services when proxying is not required.
	// Only applies to types ClusterIP, NodePort, and LoadBalancer. Ignored if
	// type is ExternalName.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +optional
	ClusterIP string

	// ClusterIPs identifies all the ClusterIPs assigned to this
	// service. ClusterIPs are assigned or reserved based on the values of
	// service.spec.ipFamilies. A maximum of two entries (dual-stack IPs) are
	// allowed in ClusterIPs. The IPFamily of each ClusterIP must match
	// values provided in service.spec.ipFamilies. Clients using ClusterIPs must
	// keep it in sync with ClusterIP (if provided) by having ClusterIP matching
	// first element of ClusterIPs.
	// +optional
	ClusterIPs []string
	// Type determines how the Service is exposed. Defaults to ClusterIP. Valid
	// options are ExternalName, ClusterIP, NodePort, and LoadBalancer.
	// "ExternalName" maps to the specified externalName.
	// "ClusterIP" allocates a cluster-internal IP address for load-balancing to
	// endpoints. Endpoints are determined by the selector or if that is not
	// specified, by manual construction of an Endpoints object. If clusterIP is
	// "None", no virtual IP is allocated and the endpoints are published as a
	// set of endpoints rather than a stable IP.
	// "NodePort" builds on ClusterIP and allocates a port on every node which
	// routes to the clusterIP.
	// "LoadBalancer" builds on NodePort and creates an
	// external load-balancer (if supported in the current cloud) which routes
	// to the clusterIP.
	// More info: https://kubernetes.io/docs/concepts/services-networking/service/
	// +optional
	Type ServiceType

	// ExternalIPs are used by external load balancers, or can be set by
	// users to handle external traffic that arrives at a node.
	//
	// Deprecated: ExternalIPs is deprecated and may be removed in a future version.
	// +optional
	ExternalIPs []string

	// Optional: Supports "ClientIP" and "None".  Used to maintain session affinity.
	// +optional
	SessionAffinity ServiceAffinity

	// Only applies to Service Type: LoadBalancer
	// LoadBalancer will get created with the IP specified in this field.
	// This feature depends on whether the underlying cloud-provider supports specifying
	// the loadBalancerIP when a load balancer is created.
	// This field will be ignored if the cloud-provider does not support the feature.
	// Deprecated: This field was under-specified and its meaning varies across implementations.
	// Using it is non-portable and it may not support dual-stack.
	// Users are encouraged to use implementation-specific annotations when available.
	// +optional
	LoadBalancerIP string

	// Optional: If specified and supported by the platform, this will restrict traffic through the cloud-provider
	// load-balancer will be restricted to the specified client IPs. This field will be ignored if the
	// cloud-provider does not support the feature."
	// +optional
	LoadBalancerSourceRanges []string

	// ExternalName is the external reference that kubedns or equivalent will
	// return as a CNAME record for this service. No proxying will be involved.
	// Must be a valid RFC-1123 hostname (https://tools.ietf.org/html/rfc1123)
	// and requires Type to be ExternalName.
	ExternalName string

	// externalTrafficPolicy describes how nodes distribute service traffic they
	// receive on one of the Service's "externally-facing" addresses (NodePorts,
	// ExternalIPs, and LoadBalancer IPs). If set to "Local", the proxy will configure
	// the service in a way that assumes that external load balancers will take care
	// of balancing the service traffic between nodes, and so each node will deliver
	// traffic only to the node-local endpoints of the service, without masquerading
	// the client source IP. (Traffic mistakenly sent to a node with no endpoints will
	// be dropped.) The default value, "Cluster", uses the standard behavior of
	// routing to all endpoints evenly (possibly modified by topology and other
	// features). Note that traffic sent to an External IP or LoadBalancer IP from
	// within the cluster will always get "Cluster" semantics, but clients sending to
	// a NodePort from within the cluster may need to take traffic policy into account
	// when picking a node.
	// +optional
	ExternalTrafficPolicy ServiceExternalTrafficPolicy

	// healthCheckNodePort specifies the healthcheck nodePort for the service.
	// If not specified, HealthCheckNodePort is created by the service api
	// backend with the allocated nodePort. Will use user-specified nodePort value
	// if specified by the client. Only effects when Type is set to LoadBalancer
	// and ExternalTrafficPolicy is set to Local.
	// +optional
	HealthCheckNodePort int32

	// publishNotReadyAddresses 表示处理此 Service 端点的任何代理都应忽略就绪/未就绪的标记。
	// 设置此字段的主要场景是 StatefulSet 的无头 Service 传播其 Pod 的 SRV DNS 记录，用于对等发现。
	// 为 Service 生成 Endpoints 和 EndpointSlice 资源的 Kubernetes 控制器会将其理解为：
	// 即使 Pod 本身未就绪，所有端点也视为"就绪"。
	// 仅通过 Endpoints 或 EndpointSlice 资源消费 Kubernetes 生成端点的代理，可以安全地依赖此行为。
	// +optional
	PublishNotReadyAddresses bool

	// sessionAffinityConfig 包含会话亲和的配置。
	// +optional
	SessionAffinityConfig *SessionAffinityConfig

	// IPFamilies identifies all the IPFamilies assigned for this Service. If a value
	// was not provided for IPFamilies it will be defaulted based on the cluster
	// configuration and the value of service.spec.ipFamilyPolicy. A maximum of two
	// values (dual-stack IPFamilies) are allowed in IPFamilies. IPFamilies field is
	// conditionally mutable: it allows for adding or removing a secondary IPFamily,
	// but it does not allow changing the primary IPFamily of the service.
	// +optional
	IPFamilies []IPFamily

	// IPFamilyPolicy represents the dual-stack-ness requested or required by this
	// Service. If there is no value provided, then this Service will be considered
	// SingleStack (single IPFamily). Services can be SingleStack (single IPFamily),
	// PreferDualStack (two dual-stack IPFamilies on dual-stack clusters or single
	// IPFamily on single-stack clusters), or RequireDualStack (two dual-stack IPFamilies
	// on dual-stack configured clusters, otherwise fail). The IPFamilies and ClusterIPs assigned
	// to this service can be controlled by service.spec.ipFamilies and service.spec.clusterIPs
	// respectively.
	// +optional
	IPFamilyPolicy *IPFamilyPolicy

	// allocateLoadBalancerNodePorts defines if NodePorts will be automatically
	// allocated for services with type LoadBalancer.  Default is "true". It
	// may be set to "false" if the cluster load-balancer does not rely on
	// NodePorts.  If the caller requests specific NodePorts (by specifying a
	// value), those requests will be respected, regardless of this field.
	// This field may only be set for services with type LoadBalancer and will
	// be cleared if the type is changed to any other type.
	// +optional
	AllocateLoadBalancerNodePorts *bool

	// loadBalancerClass is the class of the load balancer implementation this Service belongs to.
	// If specified, the value of this field must be a label-style identifier, with an optional prefix,
	// e.g. "internal-vip" or "example.com/internal-vip". Unprefixed names are reserved for end-users.
	// This field can only be set when the Service type is 'LoadBalancer'. If not set, the default load
	// balancer implementation is used, today this is typically done through the cloud provider integration,
	// but should apply for any default implementation. If set, it is assumed that a load balancer
	// implementation is watching for Services with a matching class. Any default load balancer
	// implementation (e.g. cloud providers) should ignore Services that set this field.
	// This field can only be set when creating or updating a Service to type 'LoadBalancer'.
	// Once set, it can not be changed. This field will be wiped when a service is updated to a non 'LoadBalancer' type.
	// +optional
	LoadBalancerClass *string

	// InternalTrafficPolicy 描述节点如何分发其在 ClusterIP 上接收到的 Service 流量。
	// 若设为 "Local"，代理会假定 Pod 只希望与同一节点上的 Service 端点通信，
	// 如果没有本地端点则丢弃流量。默认值 "Cluster" 使用标准行为：
	// 均匀路由到所有端点（可能受拓扑及其他特性影响）。
	// +optional
	InternalTrafficPolicy *ServiceInternalTrafficPolicy

	// TrafficDistribution 提供一种表达流量分发到 Service 端点的偏好的方式。
	// 实现可以将此字段作为提示，但无需保证严格遵循。
	// 如果未设置此字段，实现将采用其默认路由策略。
	// 如果设为 "PreferClose"，实现应优先选择同一可用区内的端点。
	// +optional
	TrafficDistribution *string
}

// ServicePort represents the port on which the service is exposed
type ServicePort struct {
	// Optional if only one ServicePort is defined on this service: The
	// name of this port within the service.  This must be a DNS_LABEL.
	// All ports within a ServiceSpec must have unique names.  This maps to
	// the 'Name' field in EndpointPort objects.
	Name string

	// The IP protocol for this port.  Supports "TCP", "UDP", and "SCTP".
	Protocol Protocol

	// The application protocol for this port.
	// This is used as a hint for implementations to offer richer behavior for protocols that they understand.
	// This field follows standard Kubernetes label syntax.
	// Valid values are either:
	//
	// * Un-prefixed protocol names - reserved for IANA standard service names (as per
	// RFC-6335 and https://www.iana.org/assignments/service-names).
	//
	// * Kubernetes-defined prefixed names:
	//   * 'kubernetes.io/h2c' - HTTP/2 prior knowledge over cleartext as described in https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - WebSocket over cleartext as described in https://www.rfc-editor.org/rfc/rfc6455
	//   * 'kubernetes.io/wss' - WebSocket over TLS as described in https://www.rfc-editor.org/rfc/rfc6455
	//
	// * Other protocols should use implementation-defined prefixed names such as
	// mycompany.com/my-custom-protocol.
	// +optional
	AppProtocol *string

	// The port that will be exposed on the service.
	Port int32

	// Optional: The target port on pods selected by this service.  If this
	// is a string, it will be looked up as a named port in the target
	// Pod's container ports.  If this is not specified, the value
	// of the 'port' field is used (an identity map).
	// This field is ignored for services with clusterIP=None, and should be
	// omitted or set equal to the 'port' field.
	TargetPort intstr.IntOrString

	// The port on each node on which this service is exposed.
	// Default is to auto-allocate a port if the ServiceType of this Service requires one.
	NodePort int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Service is a named abstraction of software service (for example, mysql) consisting of local port
// (for example 3306) that the proxy listens on, and the selector that determines which pods
// will answer requests sent through the proxy.
type Service struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Service 的行为。
	// +optional
	Spec ServiceSpec

	// Status represents the current status of a service.
	// +optional
	Status ServiceStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceAccount binds together:
// * 一个可供用户（可能还有外围系统）理解的名称，用于标识身份
// * 一个可被认证和授权的主体
// * a set of secrets
type ServiceAccount struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Secrets 是同一命名空间中的 Secret 列表，使用此 ServiceAccount 运行的 Pod 可以使用这些 Secret。
	// 只有当此 ServiceAccount 设置了 "kubernetes.io/enforce-mountable-secrets" 注解为 "true" 时，
	// Pod 才受此列表限制。
	// "kubernetes.io/enforce-mountable-secrets" 注解自 v1.32 起已弃用。
	// 建议使用独立的命名空间来隔离对已挂载 Secret 的访问。
	// 不应使用此字段查找自动生成的 ServiceAccount 令牌 Secret 以供 Pod 之外使用。
	// 应改用 TokenRequest API 直接请求令牌，或手动创建 ServiceAccount 令牌 Secret。
	Secrets []ObjectReference

	// ImagePullSecrets 是对同一命名空间中 Secret 的引用列表，用于拉取引用此 ServiceAccount 的 Pod 中的任何镜像。
	// ImagePullSecrets 与 Secrets 不同：Secret 可以挂载到 Pod 中，而 ImagePullSecrets 只有 kubelet 访问。
	// +optional
	ImagePullSecrets []LocalObjectReference

	// AutomountServiceAccountToken indicates whether pods running as this service account should have an API token automatically mounted.
	// Can be overridden at the pod level.
	// +optional
	AutomountServiceAccountToken *bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceAccountList is a list of ServiceAccount objects
type ServiceAccountList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []ServiceAccount
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Endpoints is a collection of endpoints that implement the actual service.  Example:
//
//	 Name: "mysvc",
//	 Subsets: [
//	   {
//	     Addresses: [{"ip": "10.10.1.1"}, {"ip": "10.10.2.2"}],
//	     Ports: [{"name": "a", "port": 8675}, {"name": "b", "port": 309}]
//	   },
//	   {
//	     Addresses: [{"ip": "10.10.3.3"}],
//	     Ports: [{"name": "a", "port": 93}, {"name": "b", "port": 76}]
//	   },
//	]
type Endpoints struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// The set of all endpoints is the union of all subsets.
	Subsets []EndpointSubset
}

// EndpointSubset is a group of addresses with a common set of ports.  The
// 展开后的端点集合是 Addresses × Ports 的笛卡尔积。
// For example, given:
//
//	{
//	  Addresses: [{"ip": "10.10.1.1"}, {"ip": "10.10.2.2"}],
//	  Ports:     [{"name": "a", "port": 8675}, {"name": "b", "port": 309}]
//	}
//
// 得到的端点集合可视为：
//
//	a: [ 10.10.1.1:8675, 10.10.2.2:8675 ],
//	b: [ 10.10.1.1:309, 10.10.2.2:309 ]
type EndpointSubset struct {
	Addresses         []EndpointAddress
	NotReadyAddresses []EndpointAddress
	Ports             []EndpointPort
}

// EndpointAddress is a tuple that describes single IP address.
type EndpointAddress struct {
	// 此端点的 IP。
	// 不得为回环地址（127.0.0.0/8 或 ::1）、链路本地地址（169.254.0.0/16 或 fe80::/10），
	// 或链路本地组播地址（224.0.0.0/24 或 ff02::/16）。
	IP string
	// Optional: Hostname of this endpoint
	// Meant to be used by DNS servers etc.
	// +optional
	Hostname string
	// 可选：承载此端点的节点。可用于确定某个节点上的本地端点。
	// +optional
	NodeName *string
	// Optional: The kubernetes object related to the entry point.
	TargetRef *ObjectReference
}

// EndpointPort is a tuple that describes a single port.
type EndpointPort struct {
	// The name of this port (corresponds to ServicePort.Name).  Optional
	// if only one port is defined.  Must be a DNS_LABEL.
	Name string

	// The port number.
	Port int32

	// 此端口的 IP 协议。
	Protocol Protocol

	// The application protocol for this port.
	// This is used as a hint for implementations to offer richer behavior for protocols that they understand.
	// This field follows standard Kubernetes label syntax.
	// Valid values are either:
	//
	// * Un-prefixed protocol names - reserved for IANA standard service names (as per
	// RFC-6335 and https://www.iana.org/assignments/service-names).
	//
	// * Kubernetes-defined prefixed names:
	//   * 'kubernetes.io/h2c' - HTTP/2 prior knowledge over cleartext as described in https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - WebSocket over cleartext as described in https://www.rfc-editor.org/rfc/rfc6455
	//   * 'kubernetes.io/wss' - WebSocket over TLS as described in https://www.rfc-editor.org/rfc/rfc6455
	//
	// * Other protocols should use implementation-defined prefixed names such as
	// mycompany.com/my-custom-protocol.
	// +optional
	AppProtocol *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EndpointsList is a list of endpoints.
type EndpointsList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Endpoints
}

// NodeSpec 描述创建节点时所使用的属性。
type NodeSpec struct {
	// PodCIDRs represents the IP ranges assigned to the node for usage by Pods on that node. It may
	// contain AT MOST one value for each of IPv4 and IPv6.
	// Note: assigning IP ranges to nodes might need to be revisited when we support migratable IPs.
	// +optional
	PodCIDRs []string

	// ID of the node assigned by the cloud provider
	// Note: format is "<ProviderName>://<ProviderSpecificNodeID>"
	// +optional
	ProviderID string

	// Unschedulable 控制新 Pod 是否可调度到该节点。默认情况下节点是可调度的。
	// +optional
	Unschedulable bool

	// If specified, the node's taints.
	// +optional
	Taints []Taint

	// Deprecated: 曾用于指定 DynamicKubeletConfig 特性中节点配置的来源。该特性已被移除。
	// +optional
	ConfigSource *NodeConfigSource

	// 已弃用。并非所有 kubelet 都会设置此字段。1.13 之后将移除该字段。
	// see: https://issues.k8s.io/61966
	// +optional
	DoNotUseExternalID string

	// PodPreemptionPolicy controls the node-level preemption behaviors for pods on this node.
	// This is an alpha field and requires enabling the InPlacePodVerticalScalingSchedulerPreemption feature gate.
	// +featureGate=InPlacePodVerticalScalingSchedulerPreemption
	// +optional
	PodPreemptionPolicy *NodePodPreemptionPolicy
}

// NodePodPreemptionPolicy 定义管理此节点上 Pod 抢占行为的节点级策略。
type NodePodPreemptionPolicy struct {
	// DisableResizePreemption lists the owners that have requested to disable scheduler and Kubelet preemption for in-place pod resize on this node.
	// This is an alpha field and requires enabling the InPlacePodVerticalScalingSchedulerPreemption feature gate.
	// +optional
	DisableResizePreemption []string
}

// Deprecated: NodeConfigSource specifies a source of node configuration. Exactly one subfield must be non-nil.
type NodeConfigSource struct {
	ConfigMap *ConfigMapNodeConfigSource
}

// Deprecated: ConfigMapNodeConfigSource represents the config map of a node
type ConfigMapNodeConfigSource struct {
	// Namespace 是被引用 ConfigMap 的 metadata.namespace。
	// 此字段在任何情况下都是必填的。
	Namespace string

	// Name 是被引用 ConfigMap 的 metadata.name。
	// 此字段在任何情况下都是必填的。
	Name string

	// UID 是被引用 ConfigMap 的 metadata.UID。
	// 此字段在 Node.Spec 中禁止使用，在 Node.Status 中为必填。
	// +optional
	UID types.UID

	// ResourceVersion 是被引用 ConfigMap 的 metadata.ResourceVersion。
	// 此字段在 Node.Spec 中禁止使用，在 Node.Status 中为必填。
	// +optional
	ResourceVersion string

	// KubeletConfigKey 声明被引用 ConfigMap 中哪个键对应 KubeletConfiguration 结构。
	// 此字段在任何情况下都是必填的。
	KubeletConfigKey string
}

// DaemonEndpoint 包含单个守护进程端点的信息。
type DaemonEndpoint struct {
	/*
		The port tag was not properly in quotes in earlier releases, so it must be
		uppercase for backwards compatibility (since it was falling back to var name of
		'Port').
	*/

	// 给定端点的端口号。
	Port int32
}

// NodeDaemonEndpoints 列出节点上运行的守护进程所开放的端口。
type NodeDaemonEndpoints struct {
	// Kubelet 监听的端点。
	// +optional
	KubeletEndpoint DaemonEndpoint
}

// NodeRuntimeHandlerFeatures 是运行时处理器实现的一组特性。
type NodeRuntimeHandlerFeatures struct {
	// 如果运行时处理器支持 RecursiveReadOnlyMounts，则 RecursiveReadOnlyMounts 设为 true。
	// +optional
	RecursiveReadOnlyMounts *bool
	// 如果运行时处理器支持 UserNamespaces（包括卷），则 UserNamespaces 设为 true。
	// +optional
	UserNamespaces *bool
}

// NodeRuntimeHandler 是一组运行时处理器信息。
type NodeRuntimeHandler struct {
	// Runtime handler name.
	// 默认运行时处理器时为空。
	// +optional
	Name string
	// Supported features.
	// +optional
	Features *NodeRuntimeHandlerFeatures
}

// NodeFeatures 描述 CRI 实现所具备的特性集合。
// 其中包含的特性应仅取决于 CRI 实现，
// independent of runtime handlers.
type NodeFeatures struct {
	// 如果运行时支持 SupplementalGroupsPolicy 和 ContainerUser，则 SupplementalGroupsPolicy 设为 true。
	// +optional
	SupplementalGroupsPolicy *bool
}

// NodeSystemInfo 是一组用于唯一标识节点的 id/uuid。
type NodeSystemInfo struct {
	// 节点上报的 MachineID。在集群中唯一标识机器时
	// 首选此字段。更多信息参见 man(5)
	// machine-id: http://man7.org/linux/man-pages/man5/machine-id.5.html
	MachineID string
	// 节点上报的 SystemUUID。如需唯一标识机器，
	// 首选 MachineID。此字段仅适用于 Red Hat 主机
	// https://access.redhat.com/documentation/en-us/red_hat_subscription_management/1/html/rhsm/uuid
	SystemUUID string
	// Boot ID reported by the node.
	BootID string
	// Kernel Version reported by the node.
	KernelVersion string
	// OS Image reported by the node.
	OSImage string
	// ContainerRuntime Version reported by the node.
	ContainerRuntimeVersion string
	// Kubelet Version reported by the node.
	KubeletVersion string
	// Deprecated: KubeProxy Version reported by the node.
	KubeProxyVersion string
	// The Operating System reported by the node
	OperatingSystem string
	// The Architecture reported by the node
	Architecture string
	// Swap Info reported by the node.
	Swap *NodeSwapStatus
	// Whether the node is running in a user namespace.
	// +featureGate=KubeletInUserNamespace
	// +optional
	RunningInUserNamespace *bool
}

// NodeSwapStatus 表示交换内存信息。
type NodeSwapStatus struct {
	// Total amount of swap memory in bytes.
	// +optional
	Capacity *int64
}

// NodeConfigStatus 描述由 Node.Spec.ConfigSource 分配的配置的状态。
type NodeConfigStatus struct {
	// Assigned 记录节点将尝试使用的已存档配置。
	// 当 Node.Spec.ConfigSource 更新时，节点会将相应的配置载荷以及一条表明目标配置的记录
	// 存档到本地磁盘。节点依据该记录选择配置存档点，并在 Assigned 中上报这条记录。
	// 只有在记录已存档到磁盘后，Assigned 才会更新到 status 中。
	// Kubelet 重启时，会尝试通过加载并校验 Assigned 所指向的存档配置载荷，
	// 将 Assigned 配置作为 Active 配置。
	// +optional
	Assigned *NodeConfigSource
	// Active 记录节点当前实际使用的已存档配置。
	// Active 可能表示 Assigned 配置的当前版本，也可能表示 LastKnownGood 配置的当前版本，
	// 具体取决于尝试使用 Assigned 配置时是否出错。
	// +optional
	Active *NodeConfigSource
	// LastKnownGood 记录节点在尝试使用 Assigned 配置出错时将回退到的已存档配置。
	// 当节点判定 Assigned 配置稳定且正确时，该配置就会成为 LastKnownGood 配置。
	// 目前的实现方式是：从本地 Assigned 配置记录更新开始，进行 10 分钟的观察期。
	// 若观察期结束时 Assigned 配置仍处于 Active 状态，它便成为 LastKnownGood。
	// 注意，如果 Spec.ConfigSource 被重置为 nil（使用本地默认配置），LastKnownGood 也会立即重置为 nil，
	// 因为本地默认配置始终被认为是正确的。
	// 不要对节点判断配置稳定性和正确性的方式做任何假设，
	// 因为将来这种方式可能会改变或变得可配置。
	// +optional
	LastKnownGood *NodeConfigSource
	// Error describes any problems reconciling the Spec.ConfigSource to the Active config.
	// Errors may occur, for example, attempting to checkpoint Spec.ConfigSource to the local Assigned
	// record, attempting to checkpoint the payload associated with Spec.ConfigSource, attempting
	// to load or validate the Assigned config, etc.
	// Errors may occur at different points while syncing config. Earlier errors (e.g. download or
	// checkpointing errors) will not result in a rollback to LastKnownGood, and may resolve across
	// Kubelet retries. Later errors (e.g. loading or validating a checkpointed config) will result in
	// a rollback to LastKnownGood. In the latter case, it is usually possible to resolve the error
	// by fixing the config assigned in Spec.ConfigSource.
	// You can find additional information for debugging by searching the error message in the Kubelet log.
	// Error is a human-readable description of the error state; machines can check whether or not Error
	// is empty, but should not rely on the stability of the Error text across Kubelet versions.
	// +optional
	Error string
}

// NodeStatus 描述节点的当前状态信息。
type NodeStatus struct {
	// Capacity 表示节点的总资源量。
	// +optional
	Capacity ResourceList
	// Allocatable 表示节点上可供调度使用的资源。
	// 默认为 Capacity。
	// +optional
	Allocatable ResourceList
	// NodePhase is the current lifecycle phase of the node.
	// +optional
	Phase NodePhase
	// Conditions 是当前观察到的节点状况列表。
	// +optional
	Conditions []NodeCondition
	// Queried from cloud provider, if available.
	// +optional
	Addresses []NodeAddress
	// 节点上运行的守护进程的端点。
	// +optional
	DaemonEndpoints NodeDaemonEndpoints
	// 用于唯一标识节点的一组 id/uuid。
	// +optional
	NodeInfo NodeSystemInfo
	// List of container images on this node
	// +optional
	Images []ContainerImage
	// 节点正在使用（挂载）的可挂载卷列表。
	// +optional
	VolumesInUse []UniqueVolumeName
	// 已附加到节点的卷列表。
	// +optional
	VolumesAttached []AttachedVolume
	// 通过动态 Kubelet 配置功能分配给节点的配置的状态。
	// +optional
	Config *NodeConfigStatus
	// The available runtime handlers.
	// +optional
	RuntimeHandlers []NodeRuntimeHandler
	// Features 描述 CRI 实现所具备的特性集合。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	Features *NodeFeatures
	// DeclaredFeatures represents the declared features of a node.
	// +featureGate=NodeDeclaredFeatures
	// +optional
	DeclaredFeatures []string
}

// UniqueVolumeName defines the name of attached volume
type UniqueVolumeName string

// AttachedVolume 描述附加到节点的卷
type AttachedVolume struct {
	// Name of the attached volume
	Name UniqueVolumeName

	// DevicePath represents the device path where the volume should be available
	DevicePath string
}

// AvoidPods describes pods that should avoid this node. This is the value for a
// Node annotation with key scheduler.alpha.kubernetes.io/preferAvoidPods and
// will eventually become a field of NodeStatus.
type AvoidPods struct {
	// 应避开此节点的 Pod 的签名列表，数量有上限，按时间戳从旧到新排序。
	// 切片大小未作规定。
	// +optional
	PreferAvoidPods []PreferAvoidPodsEntry
}

// PreferAvoidPodsEntry describes a class of pods that should avoid this node.
type PreferAvoidPodsEntry struct {
	// The class of pods.
	PodSignature PodSignature
	// Time at which this entry was added to the list.
	// +optional
	EvictionTime metav1.Time
	// (brief) reason why this entry was added to the list.
	// +optional
	Reason string
	// Human readable message indicating why this entry was added to the list.
	// +optional
	Message string
}

// PodSignature describes the class of pods that should avoid this node.
// 应当只设置其中一个字段。
type PodSignature struct {
	// Reference to controller whose pods should avoid this node.
	// +optional
	PodController *metav1.OwnerReference
}

// ContainerImage describe a container image
type ContainerImage struct {
	// Names by which this image is known.
	// +optional
	Names []string
	// The size of the image in bytes.
	// +optional
	SizeBytes int64
}

// NodePhase defines the phase in which a node is in
type NodePhase string

// 以下是节点的有效阶段。
const (
	// NodePending 表示节点已被系统创建/加入，但尚未配置。
	NodePending NodePhase = "Pending"
	// NodeRunning 表示节点已完成配置，且 Kubernetes 组件正在运行。
	NodeRunning NodePhase = "Running"
	// NodeTerminated 表示节点已从集群中移除。
	NodeTerminated NodePhase = "Terminated"
)

// NodeConditionType defines node's condition
type NodeConditionType string

// 以下是节点的有效状况，但并不穷尽所有可能。云服务商可以设置此处未列出的状况。
// 相关事件包括 "NodeReady"、"NodeNotReady"、"NodeSchedulable" 和 "NodeNotSchedulable"。
const (
	// NodeReady 表示 kubelet 健康，可以接收 Pod。
	NodeReady NodeConditionType = "Ready"
	// NodeMemoryPressure 表示 kubelet 因可用内存不足而处于压力状态。
	NodeMemoryPressure NodeConditionType = "MemoryPressure"
	// NodeDiskPressure 表示 kubelet 因可用磁盘空间不足而处于压力状态。
	NodeDiskPressure NodeConditionType = "DiskPressure"
	// NodeNetworkUnavailable 表示节点的网络未正确配置。
	NodeNetworkUnavailable NodeConditionType = "NetworkUnavailable"
	// NodeGracefulNodeShutdownInProgress 报告此节点是否正在执行优雅关机。
	// 管理员负责设置和清除此状况。
	NodeGracefulNodeShutdownInProgress NodeConditionType = "GracefulNodeShutdownInProgress"
	// NodeDrainInProgress 报告此节点正在被排空，
	// 具体依据管理员对排空的定义。
	// 通常这意味着移除一些 Pod、卷和网络。
	// 管理员负责设置和清除此状况。
	NodeDrainInProgress NodeConditionType = "DrainInProgress"
	// NodeDrained 报告此节点已达到管理员选择的排空标准。
	// 管理员负责设置和清除此状况。
	NodeDrained NodeConditionType = "Drained"
	// NodeMaintenancePlanned 报告此节点预计在未来将发生变更。
	// 如果此变更会影响节点使用者，管理员应先排空该节点。
	// 管理员可将节点维护状况用于硬件或软件升级、
	// 修复、下线或调试等场景。
	// 管理员负责设置和清除此状况。
	NodeMaintenancePlanned NodeConditionType = "MaintenancePlanned"
	// NodeMaintenanceInProgress 报告此节点正在执行维护。
	// 管理员决定该变更是否会影响节点使用者并需要排空节点。
	// 例如，建议在 Kubernetes 升级前排空节点。
	// 相反，内核热补丁可能不需要排空节点，但通告维护正在进行
	// 仍然是有用的。
	// 管理员负责设置和清除此状况。
	NodeMaintenanceInProgress NodeConditionType = "MaintenanceInProgress"
)

// NodeCondition represents the node's condition
type NodeCondition struct {
	Type   NodeConditionType
	Status ConditionStatus
	// +optional
	LastHeartbeatTime metav1.Time
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// NodeAddressType defines the node's address type
type NodeAddressType string

// These are valid values of node address type
const (
	// NodeHostName 标识节点的名称。虽然可以认为每个节点都有此类型的 NodeAddress，
	// 但其确切的语法和语义并未定义，在不同集群之间也不一致。
	NodeHostName NodeAddressType = "Hostname"

	// NodeInternalIP identifies an IP address which is assigned to one of the node's
	// network interfaces. Every node should have at least one address of this type.
	//
	// An internal IP is normally expected to be reachable from every other node, but
	// may not be visible to hosts outside the cluster. By default it is assumed that
	// kube-apiserver can reach node internal IPs, though it is possible to configure
	// clusters where this is not the case.
	//
	// NodeInternalIP is the default type of node IP, and does not necessarily imply
	// that the IP is ONLY reachable internally. If a node has multiple internal IPs,
	// no specific semantics are assigned to the additional IPs.
	NodeInternalIP NodeAddressType = "InternalIP"

	// NodeExternalIP identifies an IP address which is, in some way, intended to be
	// more usable from outside the cluster then an internal IP, though no specific
	// semantics are defined. It may be a globally routable IP, though it is not
	// required to be.
	//
	// External IPs may be assigned directly to an interface on the node, like a
	// NodeInternalIP, or alternatively, packets sent to the external IP may be NAT'ed
	// to an internal node IP rather than being delivered directly (making the IP less
	// efficient for node-to-node traffic than a NodeInternalIP).
	NodeExternalIP NodeAddressType = "ExternalIP"

	// NodeInternalDNS 标识解析为具有 NodeInternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeInternalIP 地址。
	NodeInternalDNS NodeAddressType = "InternalDNS"

	// NodeExternalDNS 标识解析为具有 NodeExternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeExternalIP 地址。
	NodeExternalDNS NodeAddressType = "ExternalDNS"
)

// NodeAddress represents node's address
type NodeAddress struct {
	Type    NodeAddressType
	Address string
}

// ResourceName 是 ResourceList 中标识各种资源的名称。
type ResourceName string

// Resource names must be not more than 63 characters, consisting of upper- or lower-case alphanumeric characters,
// with the -, _, and . characters allowed anywhere, except the first or last character.
// The default convention, matching that for annotations, is to use lower-case names, with dashes, rather than
// camel case, separating compound words.
// Fully-qualified resource typenames are constructed from a DNS-style subdomain, followed by a slash `/` and a name.
const (
	// CPU，单位为核数。（500m = .5 核）
	ResourceCPU ResourceName = "cpu"
	// 内存，单位为字节。（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）
	ResourceMemory ResourceName = "memory"
	// 卷大小，单位为字节（例如 5Gi = 5GiB = 5 * 1024 * 1024 * 1024）
	ResourceStorage ResourceName = "storage"
	// 本地临时存储，单位为字节。（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）
	ResourceEphemeralStorage ResourceName = "ephemeral-storage"
)

const (
	// ResourceDefaultNamespacePrefix is the default namespace prefix.
	ResourceDefaultNamespacePrefix = "kubernetes.io/"
	// ResourceHugePagesPrefix is the name prefix for huge page resources (alpha).
	ResourceHugePagesPrefix = "hugepages-"
	// ResourceAttachableVolumesPrefix is the name prefix for storage resource limits
	ResourceAttachableVolumesPrefix = "attachable-volumes-"
)

// ResourceList 是一组（资源名称，数量）对。
type ResourceList map[ResourceName]resource.Quantity

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Node is a worker node in Kubernetes
// The name of the node according to etcd is in ObjectMeta.Name.
type Node struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义节点的行为。
	// +optional
	Spec NodeSpec

	// Status describes the current status of a Node
	// +optional
	Status NodeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeList is a list of nodes.
type NodeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Node
}

// NamespaceSpec describes the attributes on a Namespace
type NamespaceSpec struct {
	// Finalizers 是一个不透明的值列表，必须为空才能将对象从存储中永久删除。
	Finalizers []FinalizerName
}

// FinalizerName 是在命名空间生命周期中标识终结器的名称。
type FinalizerName string

// These are internal finalizer values to Kubernetes, must be qualified name unless defined here or
// in metav1.
const (
	FinalizerKubernetes FinalizerName = "kubernetes"
)

// NamespaceStatus 描述 Namespace 的当前状态信息。
type NamespaceStatus struct {
	// Phase 是命名空间当前的生命周期阶段。
	// +optional
	Phase NamespacePhase
	// +optional
	Conditions []NamespaceCondition
}

// NamespacePhase defines the phase in which the namespace is
type NamespacePhase string

// 以下是命名空间的有效阶段。
const (
	// NamespaceActive 表示命名空间可在系统中使用
	NamespaceActive NamespacePhase = "Active"
	// NamespaceTerminating 表示命名空间正在优雅终止
	NamespaceTerminating NamespacePhase = "Terminating"
)

// NamespaceConditionType defines constants reporting on status during namespace lifetime and deletion progress
type NamespaceConditionType string

// These are valid conditions of a namespace.
const (
	NamespaceDeletionDiscoveryFailure NamespaceConditionType = "NamespaceDeletionDiscoveryFailure"
	NamespaceDeletionContentFailure   NamespaceConditionType = "NamespaceDeletionContentFailure"
	NamespaceDeletionGVParsingFailure NamespaceConditionType = "NamespaceDeletionGroupVersionParsingFailure"
)

// NamespaceCondition 包含命名空间状态的详细信息。
type NamespaceCondition struct {
	// Type of namespace controller condition.
	Type NamespaceConditionType
	// 状况的状态，取值为 True、False 或 Unknown。
	Status ConditionStatus
	// +optional
	LastTransitionTime metav1.Time
	// +optional
	Reason string
	// +optional
	Message string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Namespace 为名称提供了一个作用域。
// Use of multiple namespaces is optional
type Namespace struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Namespace 的行为。
	// +optional
	Spec NamespaceSpec

	// Status 描述 Namespace 的当前状态。
	// +optional
	Status NamespaceStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NamespaceList 是 Namespace 的列表。
type NamespaceList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Namespace
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Binding ties one object to another; for example, a pod is bound to a node by a scheduler.
type Binding struct {
	metav1.TypeMeta
	// ObjectMeta describes the object that is being bound.
	// +optional
	metav1.ObjectMeta

	// Target is the object to bind to.
	Target ObjectReference
}

// 在执行操作（更新、删除等）之前必须满足的前置条件。
type Preconditions struct {
	// 指定目标 UID。
	// +optional
	UID *types.UID
}

const (
	// LogStreamStdout 表示 stdout 的流类型。
	LogStreamStdout = "Stdout"
	// LogStreamStderr 表示 stderr 的流类型。
	LogStreamStderr = "Stderr"
	// LogStreamAll 表示 stdout 和 stderr 的合并流。
	LogStreamAll = "All"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodLogOptions is the query options for a Pod's logs REST call
type PodLogOptions struct {
	metav1.TypeMeta

	// Container for which to return logs
	Container string
	// If true, follow the logs for the pod
	Follow bool
	// If true, return previous terminated container logs
	Previous bool
	// 相对于当前时间往前推的秒数，从该时间点开始显示日志。如果此值
	// 早于 Pod 启动时间，则只返回 Pod 启动以来的日志。
	// 如果此值在未来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中之一。
	SinceSeconds *int64
	// 从该 RFC3339 时间戳开始显示日志。如果此值
	// 早于 Pod 启动时间，则只返回 Pod 启动以来的日志。
	// 如果此值在未来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中之一。
	SinceTime *metav1.Time
	// If true, add an RFC 3339 timestamp with 9 digits of fractional seconds at the beginning of every line
	// of log output.
	Timestamps bool
	// 若设置，表示从日志末尾开始显示的行数。如果未指定，
	// 则从容器创建起，或从 sinceSeconds / sinceTime 指定的时间点起显示日志。
	// 注意，指定 "TailLines" 时，"Stream" 只能设为 nil 或 "All"。
	TailLines *int64
	// If set, the number of bytes to read from the server before terminating the
	// log output. This may not display a complete final line of logging, and may return
	// slightly more or slightly less than the specified limit.
	LimitBytes *int64

	// insecureSkipTLSVerifyBackend indicates that the apiserver should not confirm the validity of the
	// serving certificate of the backend it is connecting to.  This will make the HTTPS connection between the apiserver
	// and the backend insecure. This means the apiserver cannot verify the log data it is receiving came from the real
	// kubelet.  If the kubelet is configured to verify the apiserver's TLS credentials, it does not mean the
	// connection to the real kubelet is vulnerable to a man in the middle attack (e.g. an attacker could not intercept
	// the actual log data coming from the real kubelet).
	// +optional
	InsecureSkipTLSVerifyBackend bool

	// 指定向客户端返回哪个容器日志流。
	// 可接受的值为 "All"、"Stdout" 和 "Stderr"。若未指定，则使用 "All"，
	// 并将 stdout 和 stderr 交错返回。
	// 注意，指定 "TailLines" 时，"Stream" 只能设为 nil 或 "All"。
	// +featureGate=PodLogsQuerySplitStreams
	// +optional
	Stream *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodAttachOptions is the query options to a Pod's remote attach call
// TODO: 与下方的 PodExecOptions 合并，统一处理 stdin、stdout 等
type PodAttachOptions struct {
	metav1.TypeMeta

	// Stdin if true indicates that stdin is to be redirected for the attach call
	// +optional
	Stdin bool

	// Stdout if true indicates that stdout is to be redirected for the attach call
	// +optional
	Stdout bool

	// Stderr if true indicates that stderr is to be redirected for the attach call
	// +optional
	Stderr bool

	// TTY if true indicates that a tty will be allocated for the attach call
	// +optional
	TTY bool

	// Container to attach to.
	// +optional
	Container string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodExecOptions is the query options to a Pod's remote exec call
type PodExecOptions struct {
	metav1.TypeMeta

	// Stdin if true indicates that stdin is to be redirected for the exec call
	Stdin bool

	// Stdout if true indicates that stdout is to be redirected for the exec call
	Stdout bool

	// Stderr if true indicates that stderr is to be redirected for the exec call
	Stderr bool

	// TTY if true indicates that a tty will be allocated for the exec call
	TTY bool

	// Container in which to execute the command.
	Container string

	// Command is the remote command to execute; argv array; not executed within a shell.
	Command []string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodPortForwardOptions is the query options to a Pod's port forward call
type PodPortForwardOptions struct {
	metav1.TypeMeta

	// The list of ports to forward
	// +optional
	Ports []int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodProxyOptions is the query options to a Pod's proxy call
type PodProxyOptions struct {
	metav1.TypeMeta

	// Path 是本次到 Pod 的代理请求所使用的 URL 路径。
	Path string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeProxyOptions is the query options to a Node's proxy call
type NodeProxyOptions struct {
	metav1.TypeMeta

	// Path 是本次到 Node 的代理请求所使用的 URL 路径。
	Path string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceProxyOptions 是 Service 代理调用的查询选项。
type ServiceProxyOptions struct {
	metav1.TypeMeta

	// Path is the part of URLs that include service endpoints, suffixes,
	// and parameters to use for the current proxy request to service.
	// For example, the whole request URL is
	// http://localhost/api/v1/namespaces/kube-system/services/elasticsearch-logging/_search?q=user:kimchy.
	// Path is _search?q=user:kimchy.
	Path string
}

// ObjectReference 包含足够的信息，可用于检查或修改被引用的对象。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type ObjectReference struct {
	// +optional
	Kind string
	// +optional
	Namespace string
	// +optional
	Name string
	// +optional
	UID types.UID
	// +optional
	APIVersion string
	// +optional
	ResourceVersion string

	// Optional. If referring to a piece of an object instead of an entire object, this string
	// should contain information to identify the sub-object. For example, if the object
	// reference is to a container within a pod, this would take on a value like:
	// "spec.containers{name}" (where "name" refers to the name of the container that triggered
	// the event) or if no container name is specified "spec.containers[2]" (container with
	// index 2 in this pod). This syntax is chosen only to have some well-defined way of
	// referencing a part of an object.
	// TODO: this design is not final and this field is subject to change in the future.
	// +optional
	FieldPath string
}

// LocalObjectReference contains enough information to let you locate the referenced object inside the same namespace.
type LocalObjectReference struct {
	// TODO: Add other useful fields.  apiVersion, kind, uid?
	Name string
}

// TypedLocalObjectReference contains enough information to let you locate the typed referenced object inside the same namespace.
type TypedLocalObjectReference struct {
	// APIGroup 是被引用资源所属的组。
	// 如果未指定 APIGroup，则指定的 Kind 必须属于核心 API 组。
	// 对于其他第三方类型，APIGroup 是必填的。
	// +optional
	APIGroup *string
	// Kind 是被引用资源的类型
	Kind string
	// Name 是被引用资源的名称
	Name string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SerializedReference represents a serialized object reference
type SerializedReference struct {
	metav1.TypeMeta
	// +optional
	Reference ObjectReference
}

// EventSource represents the source from which an event is generated
type EventSource struct {
	// Component from which the event is generated.
	// +optional
	Component string
	// Node name on which the event is generated.
	// +optional
	Host string
}

// 事件类型的有效值（将来可能新增类型）
const (
	// 仅提供信息，不会导致任何问题
	EventTypeNormal string = "Normal"
	// 此类事件用于警告可能出问题
	EventTypeWarning string = "Warning"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Event is a report of an event somewhere in the cluster.  Events
// have a limited retention time and triggers and messages may evolve
// with time.  Event consumers should not rely on the timing of an event
// with a given Reason reflecting a consistent underlying trigger, or the
// continued existence of events with that Reason.  Events should be
// treated as informative, best-effort, supplemental data.
// TODO: Decide whether to store these separately or with the object they apply to.
type Event struct {
	metav1.TypeMeta

	metav1.ObjectMeta

	// The object that this event is about. Mapped to events.Event.regarding
	InvolvedObject ObjectReference

	// Optional; this should be a short, machine understandable string that gives the reason
	// for this event being generated. For example, if the event is reporting that a container
	// can't start, the Reason might be "ImageNotFound".
	// TODO: 提供确切的格式规范。
	// +optional
	Reason string

	// Optional. A human-readable description of the status of this operation.
	// TODO: decide on maximum length. Mapped to events.Event.note
	// +optional
	Message string

	// Optional. The component reporting this event. Should be a short machine understandable string.
	// +optional
	Source EventSource

	// 事件首次被记录的时间。（服务端接收时间见 TypeMeta。）
	// +optional
	FirstTimestamp metav1.Time

	// The time at which the most recent occurrence of this event was recorded.
	// +optional
	LastTimestamp metav1.Time

	// The number of times this event has occurred.
	// +optional
	Count int32

	// Type of this event (Normal, Warning), new types could be added in the future.
	// +optional
	Type string

	// Time when this Event was first observed.
	// +optional
	EventTime metav1.MicroTime

	// 此事件所属事件序列的数据；若为单次事件则为 nil。
	// +optional
	Series *EventSeries

	// 针对 Regarding 对象已执行/失败的动作。
	// +optional
	Action string

	// Optional secondary object for more complex actions.
	// +optional
	Related *ObjectReference

	// Name of the controller that emitted this Event, e.g. `kubernetes.io/kubelet`.
	// +optional
	ReportingController string

	// ID of the controller instance, e.g. `kubelet-xyzf`.
	// +optional
	ReportingInstance string
}

// EventSeries represents a series ov events
type EventSeries struct {
	// Number of occurrences in this series up to the last heartbeat time
	Count int32
	// Time of the last occurrence observed
	LastObservedTime metav1.MicroTime
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EventList 是事件列表。
type EventList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Event
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// List 保存一组对象，服务端可能并不知晓这些对象的类型。
type List metainternalversion.List

// LimitType defines a type of object that is limited
type LimitType string

const (
	// LimitTypePod defines limit that applies to all pods in a namespace
	LimitTypePod LimitType = "Pod"
	// LimitTypeContainer defines limit that applies to all containers in a namespace
	LimitTypeContainer LimitType = "Container"
	// LimitTypePersistentVolumeClaim defines limit that applies to all persistent volume claims in a namespace
	LimitTypePersistentVolumeClaim LimitType = "PersistentVolumeClaim"
)

// LimitRangeItem defines a min/max usage limit for any resource that matches on kind
type LimitRangeItem struct {
	// Type of resource that this limit applies to
	// +optional
	Type LimitType
	// Max usage constraints on this kind by resource name
	// +optional
	Max ResourceList
	// Min usage constraints on this kind by resource name
	// +optional
	Min ResourceList
	// Default resource requirement limit value by resource name.
	// +optional
	Default ResourceList
	// DefaultRequest resource requirement request value by resource name.
	// +optional
	DefaultRequest ResourceList
	// MaxLimitRequestRatio represents the max burst value for the named resource
	// +optional
	MaxLimitRequestRatio ResourceList
}

// LimitRangeSpec defines a min/max usage limit for resources that match on kind
type LimitRangeSpec struct {
	// Limits 是需要强制执行的 LimitRangeItem 对象列表。
	Limits []LimitRangeItem
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LimitRange sets resource usage limits for each kind of resource in a Namespace
type LimitRange struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the limits enforced
	// +optional
	Spec LimitRangeSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LimitRangeList 是 LimitRange 对象的列表。
type LimitRangeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items is a list of LimitRange objects
	Items []LimitRange
}

// 以下常量标识 Kubernetes 对象类型的资源。
const (
	// Pod 数量。
	ResourcePods ResourceName = "pods"
	// Service 数量。
	ResourceServices ResourceName = "services"
	// ReplicationControllers, number
	ResourceReplicationControllers ResourceName = "replicationcontrollers"
	// ResourceQuotas, number
	ResourceQuotas ResourceName = "resourcequotas"
	// ResourceSecrets, number
	ResourceSecrets ResourceName = "secrets"
	// ResourceConfigMaps, number
	ResourceConfigMaps ResourceName = "configmaps"
	// ResourcePersistentVolumeClaims, number
	ResourcePersistentVolumeClaims ResourceName = "persistentvolumeclaims"
	// ResourceServicesNodePorts, number
	ResourceServicesNodePorts ResourceName = "services.nodeports"
	// ResourceServicesLoadBalancers, number
	ResourceServicesLoadBalancers ResourceName = "services.loadbalancers"
	// CPU 请求，单位为核数（500m = 0.5 核）。
	ResourceRequestsCPU ResourceName = "requests.cpu"
	// 内存请求，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	ResourceRequestsMemory ResourceName = "requests.memory"
	// Storage request, in bytes
	ResourceRequestsStorage ResourceName = "requests.storage"
	// 本地临时存储请求，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	ResourceRequestsEphemeralStorage ResourceName = "requests.ephemeral-storage"
	// CPU 限制，单位为核数（500m = 0.5 核）。
	ResourceLimitsCPU ResourceName = "limits.cpu"
	// 内存限制，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	ResourceLimitsMemory ResourceName = "limits.memory"
	// 本地临时存储限制，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	ResourceLimitsEphemeralStorage ResourceName = "limits.ephemeral-storage"
	// 按指定 DeviceClass 请求的 resource.k8s.io 设备数量。
	ResourceClaimsPerClass string = ".deviceclass.resource.k8s.io/devices"
)

// 以下常量标识 Kubernetes 对象类型的资源前缀。
const (
	// HugePages 请求，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	// HugePages 不支持突发的用量，因此只对其请求值配额，忽略限制值。
	ResourceRequestsHugePagesPrefix = "requests.hugepages-"
	// Default resource requests prefix
	DefaultResourceRequestsPrefix = "requests."
)

// ResourceQuotaScope defines a filter that must match each object tracked by a quota
type ResourceQuotaScope string

// These are valid values for resource quota spec
const (
	// 匹配所有 spec.activeDeadlineSeconds >=0 的 Pod 对象。
	ResourceQuotaScopeTerminating ResourceQuotaScope = "Terminating"
	// 匹配所有 spec.activeDeadlineSeconds 为 nil 的 Pod 对象。
	ResourceQuotaScopeNotTerminating ResourceQuotaScope = "NotTerminating"
	// 匹配所有服务质量为 best effort 的 Pod 对象。
	ResourceQuotaScopeBestEffort ResourceQuotaScope = "BestEffort"
	// 匹配所有服务质量不为 best effort 的 Pod 对象。
	ResourceQuotaScopeNotBestEffort ResourceQuotaScope = "NotBestEffort"
	// 匹配所有指定了优先级类别的 Pod 对象。
	ResourceQuotaScopePriorityClass ResourceQuotaScope = "PriorityClass"
	// 匹配所有指定了跨命名空间 Pod（反）亲和性的 Pod 对象。
	ResourceQuotaScopeCrossNamespacePodAffinity ResourceQuotaScope = "CrossNamespacePodAffinity"

	// Match all pvc objects that have volume attributes class mentioned.
	ResourceQuotaScopeVolumeAttributesClass ResourceQuotaScope = "VolumeAttributesClass"
)

// ResourceQuotaSpec defines the desired hard limits to enforce for Quota
type ResourceQuotaSpec struct {
	// hard 是各命名资源所期望的硬性限制集合。
	// +optional
	Hard ResourceList
	// A collection of filters that must match each object tracked by a quota.
	// 若未指定，则配额匹配所有对象。
	// +optional
	Scopes []ResourceQuotaScope
	// ScopeSelector is also a collection of filters like Scopes that must match each object tracked by a quota
	// 但它通过 ScopeSelectorOperator 与可能的取值组合来表达。
	// +optional
	ScopeSelector *ScopeSelector
}

// ScopeSelector represents the AND of the selectors represented
// by the scoped-resource selector terms.
type ScopeSelector struct {
	// A list of scope selector requirements by scope of the resources.
	// +optional
	MatchExpressions []ScopedResourceSelectorRequirement
}

// ScopedResourceSelectorRequirement is a selector that contains values, a scope name, and an operator
// 以及用于关联作用域名称与这些值的操作符。
type ScopedResourceSelectorRequirement struct {
	// The name of the scope that the selector applies to.
	ScopeName ResourceQuotaScope
	// 表示作用域与一组值之间的关系。
	// 有效操作符为 In、NotIn、Exists、DoesNotExist。
	Operator ScopeSelectorOperator
	// 字符串值数组。当操作符为 In 或 NotIn 时，该数组必须非空；
	// 当操作符为 Exists 或 DoesNotExist 时，该数组必须为空。
	// 在 strategic merge patch 中会替换此数组。
	// +optional
	Values []string
}

// ScopeSelectorOperator is the set of operators that can be used in
// a scope selector requirement.
type ScopeSelectorOperator string

// These are the valid values for ScopeSelectorOperator
const (
	ScopeSelectorOpIn           ScopeSelectorOperator = "In"
	ScopeSelectorOpNotIn        ScopeSelectorOperator = "NotIn"
	ScopeSelectorOpExists       ScopeSelectorOperator = "Exists"
	ScopeSelectorOpDoesNotExist ScopeSelectorOperator = "DoesNotExist"
)

// ResourceQuotaStatus defines the enforced hard limits and observed use
type ResourceQuotaStatus struct {
	// Hard 是各命名资源已施加的硬性限制集合。
	// +optional
	Hard ResourceList
	// Used 是该命名空间中当前观测到的资源总使用量。
	// +optional
	Used ResourceList
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceQuota 设置每个命名空间所施加的聚合配额限制。
type ResourceQuota struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec defines the desired quota
	// +optional
	Spec ResourceQuotaSpec

	// Status 定义实际施加的配额及其当前使用量。
	// +optional
	Status ResourceQuotaStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceQuotaList is a list of ResourceQuota items
type ResourceQuotaList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items is a list of ResourceQuota objects
	Items []ResourceQuota
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Secret holds secret data of a certain type.  The total bytes of the values in
// the Data field must be less than MaxSecretSize bytes.
type Secret struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Immutable field, if set, ensures that data stored in the Secret cannot
	// （只能修改对象元数据）。
	// +optional
	Immutable *bool

	// Data 包含私密数据。每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 私密数据的序列化形式为 base64 编码字符串，表示此处任意（可能是非字符串）的数据值。
	// +optional
	Data map[string][]byte `datapolicy:"password,security-key,token"`

	// Used to facilitate programmatic handling of secret data.
	// More info: https://kubernetes.io/docs/concepts/configuration/secret/#secret-types
	// +optional
	Type SecretType
}

// MaxSecretSize represents the max secret size.
const MaxSecretSize = 1 * 1024 * 1024

// SecretType defines the types of secrets
type SecretType string

// These are the valid values for SecretType
const (
	// SecretTypeOpaque 是默认值，表示任意用户自定义数据。
	SecretTypeOpaque SecretType = "Opaque"

	// SecretTypeServiceAccountToken 包含一个用于向 API 标识服务账号的令牌。
	// 必填字段：
	// - Secret.Annotations["kubernetes.io/service-account.name"] - 令牌所标识的 ServiceAccount 的名称
	// - Secret.Annotations["kubernetes.io/service-account.uid"] - 令牌所标识的 ServiceAccount 的 UID
	// - Secret.Data["token"] - 向 API 标识该服务账号的令牌
	SecretTypeServiceAccountToken SecretType = "kubernetes.io/service-account-token"

	// ServiceAccountNameKey 是 SecretTypeServiceAccountToken 类型 Secret 必填注解的键。
	ServiceAccountNameKey = "kubernetes.io/service-account.name"
	// ServiceAccountUIDKey 是 SecretTypeServiceAccountToken 类型 Secret 必填注解的键。
	ServiceAccountUIDKey = "kubernetes.io/service-account.uid"
	// ServiceAccountTokenKey 是 SecretTypeServiceAccountToken 类型 Secret 必填数据的键。
	ServiceAccountTokenKey = "token"
	// ServiceAccountKubeconfigKey 是 SecretTypeServiceAccountToken 类型 Secret 可选 kubeconfig 数据的键。
	ServiceAccountKubeconfigKey = "kubernetes.kubeconfig"
	// ServiceAccountRootCAKey 是 SecretTypeServiceAccountToken 类型 Secret 可选根证书颁发机构的键。
	ServiceAccountRootCAKey = "ca.crt"
	// ServiceAccountNamespaceKey 是可选命名空间的键，该命名空间用作命名空间级 API 调用的默认值。
	ServiceAccountNamespaceKey = "namespace"

	// SecretTypeDockercfg 包含一个遵循 ~/.dockercfg 相同格式规则的 dockercfg 文件。
	// 必填字段：
	// - Secret.Data[".dockercfg"] - 序列化后的 ~/.dockercfg 文件
	SecretTypeDockercfg SecretType = "kubernetes.io/dockercfg"

	// DockerConfigKey 是 SecretTypeDockercfg 类型 Secret 必填数据的键。
	DockerConfigKey = ".dockercfg"

	// SecretTypeDockerConfigJSON contains a dockercfg file that follows the same format rules as ~/.docker/config.json
	//
	// Required fields:
	// - Secret.Data[".dockerconfigjson"] - 序列化后的 ~/.docker/config.json 文件
	SecretTypeDockerConfigJSON SecretType = "kubernetes.io/dockerconfigjson"

	// DockerConfigJSONKey is the key of the required data for SecretTypeDockerConfigJson secrets
	DockerConfigJSONKey = ".dockerconfigjson"

	// SecretTypeBasicAuth 包含基本认证所需的数据。
	// 至少要求以下字段之一：
	// - Secret.Data["username"] - 用于认证的用户名
	// - Secret.Data["password"] - 认证所需的密码或令牌
	SecretTypeBasicAuth SecretType = "kubernetes.io/basic-auth"

	// BasicAuthUsernameKey 是 SecretTypeBasicAuth 类型 Secret 用户名的键。
	BasicAuthUsernameKey = "username"
	// BasicAuthPasswordKey 是 SecretTypeBasicAuth 类型 Secret 密码或令牌的键。
	BasicAuthPasswordKey = "password"

	// SecretTypeSSHAuth 包含 SSH 认证所需的数据。
	// 必填字段：
	// - Secret.Data["ssh-privatekey"] - 认证所需的 SSH 私钥
	SecretTypeSSHAuth SecretType = "kubernetes.io/ssh-auth"

	// SSHAuthPrivateKey 是 SecretTypeSSHAuth 类型 Secret 必填 SSH 私钥的键。
	SSHAuthPrivateKey = "ssh-privatekey"

	// SecretTypeTLS 包含 TLS 客户端或服务端 Secret 的信息。它主要用于 Ingress 资源的 TLS 终止，
	// 也可用于其他类型。
	// 必填字段：
	// - Secret.Data["tls.key"] - TLS 私钥。
	// Secret.Data["tls.crt"] - TLS 证书。
	// TODO: 考虑支持不同格式，以及指定 CA/destinationCA。
	SecretTypeTLS SecretType = "kubernetes.io/tls"

	// TLSCertKey 是 TLS Secret 中 TLS 证书的键。
	TLSCertKey = "tls.crt"
	// TLSPrivateKeyKey 是 TLS Secret 中私钥字段的键。
	TLSPrivateKeyKey = "tls.key"
	// SecretTypeBootstrapToken is used during the automated bootstrap process (first
	// implemented by kubeadm). It stores tokens that are used to sign well known
	// ConfigMaps. They are used for authn.
	SecretTypeBootstrapToken SecretType = "bootstrap.kubernetes.io/token"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SecretList represents the list of secrets
type SecretList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Secret
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConfigMap holds configuration data for components or applications to consume.
type ConfigMap struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Immutable field, if set, ensures that data stored in the ConfigMap cannot
	// （只能修改对象元数据）。
	// +optional
	Immutable *bool

	// Data 包含配置数据。
	// 每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 含非 UTF-8 字节序列的值必须使用 BinaryData 字段。
	// Data 中的键不得与 BinaryData 字段中的键重叠，校验过程会强制检查这一点。
	// +optional
	Data map[string]string

	// BinaryData contains the binary data.
	// Each key must consist of alphanumeric characters, '-', '_' or '.'.
	// BinaryData can contain byte sequences that are not in the UTF-8 range.
	// The keys stored in BinaryData must not overlap with the ones in
	// the Data field, this is enforced during validation process.
	// Using this field will require 1.10+ apiserver and
	// kubelet.
	// +optional
	BinaryData map[string][]byte
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConfigMapList 是包含 ConfigMap 对象列表的资源。
type ConfigMapList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items is the list of ConfigMaps.
	Items []ConfigMap
}

// These constants are for remote command execution and port forwarding and are
// used by both the client side and server side components.
//
// This is probably not the ideal place for them, but it didn't seem worth it
// to create pkg/exec and pkg/portforward just to contain a single file with
// constants in it.  Suggestions for more appropriate alternatives are
// definitely welcome!
const (
	// 为远程命令执行启用 stdin。
	ExecStdinParam = "input"
	// 为远程命令执行启用 stdout。
	ExecStdoutParam = "output"
	// 为远程命令执行启用 stderr。
	ExecStderrParam = "error"
	// 为远程命令执行启用 TTY。
	ExecTTYParam = "tty"
	// Command to run for remote command execution
	ExecCommandParam = "command"

	// Name of header that specifies stream type
	StreamType = "streamType"
	// stdin 流的 streamType 请求头值。
	StreamTypeStdin = "stdin"
	// stdout 流的 streamType 请求头值。
	StreamTypeStdout = "stdout"
	// stderr 流的 streamType 请求头值。
	StreamTypeStderr = "stderr"
	// 数据流的 streamType 请求头值。
	StreamTypeData = "data"
	// 错误流的 streamType 请求头值。
	StreamTypeError = "error"
	// 终端调整大小流的 streamType 请求头值。
	StreamTypeResize = "resize"

	// 指定被转发端口的请求头名称。
	PortHeader = "port"
	// Name of header that specifies a request ID used to associate the error
	// and data streams for a single forwarded connection
	PortForwardRequestIDHeader = "requestID"
)

// ComponentConditionType defines type and constants for component health validation.
type ComponentConditionType string

// 以下是组件的有效状态。
const (
	ComponentHealthy ComponentConditionType = "Healthy"
)

// ComponentCondition represents the condition of a component
type ComponentCondition struct {
	Type   ComponentConditionType
	Status ConditionStatus
	// +optional
	Message string
	// +optional
	Error string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComponentStatus (and ComponentStatusList) holds the cluster validation info.
// Deprecated: This API is deprecated in v1.19+
type ComponentStatus struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// +optional
	Conditions []ComponentCondition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComponentStatusList represents the list of component statuses
// Deprecated: This API is deprecated in v1.19+
type ComponentStatusList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []ComponentStatus
}

// SecurityContext 保存将应用于容器的安全配置。
// 有些字段同时存在于 SecurityContext 和 PodSecurityContext 中。当两者都设置时，
// SecurityContext 中的值优先。
type SecurityContext struct {
	// 运行容器时添加/删除的能力（capabilities）。
	// 默认为容器运行时授予的默认能力集。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Capabilities *Capabilities
	// 以特权模式运行容器。
	// 特权容器中的进程基本上等同于宿主机上的 root。
	// 默认为 false。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Privileged *bool
	// 要应用于容器的 SELinux 上下文。
	// 若未指定，容器运行时会为每个容器分配一个随机的 SELinux 上下文。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SELinuxOptions *SELinuxOptions
	// 应用于所有容器的 Windows 特有设置。
	// 若未指定，则使用 PodSecurityContext 中的选项。
	// 若同时在 SecurityContext 和 PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 linux 时不能设置此字段。
	// +optional
	WindowsOptions *WindowsSecurityContextOptions
	// 运行容器进程入口点的 UID。
	// 若未指定，默认使用镜像元数据中指定的用户。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsUser *int64
	// 运行容器进程入口点的 GID。
	// 若未设置，则使用运行时默认值。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsGroup *int64
	// 表示容器必须以非 root 用户身份运行。
	// 若为 true，Kubelet 会在运行时校验镜像，确保不以 UID 0（root）运行；否则容器启动失败。
	// 若未设置或为 false，则不执行此类校验。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// +optional
	RunAsNonRoot *bool
	// The read-only root filesystem allows you to restrict the locations that an application can write
	// files to, ensuring the persistent data can only be written to mounts.
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	ReadOnlyRootFilesystem *bool
	// AllowPrivilegeEscalation controls whether a process can gain more
	// privileges than its parent process. This bool directly controls if
	// the no_new_privs flag will be set on the container process.
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AllowPrivilegeEscalation *bool
	// ProcMount denotes the type of proc mount to use for the containers.
	// The default value is Default which uses the container runtime defaults for
	// readonly paths and masked paths.
	// Note that this field cannot be set when spec.os.name is windows.
	// +optional
	ProcMount *ProcMountType
	// 此容器使用的 seccomp 选项。若同时在 Pod 和容器级别提供 seccomp 选项，
	// 容器级别的选项会覆盖 Pod 级别的选项。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SeccompProfile *SeccompProfile
	// appArmorProfile 是此容器使用的 AppArmor 选项。若设置，此配置会
	// 覆盖 Pod 的 appArmorProfile。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AppArmorProfile *AppArmorProfile
}

// ProcMountType defines the type of proc mount
type ProcMountType string

const (
	// DefaultProcMount 对 /proc 的只读路径和屏蔽路径使用容器运行时的默认设置。
	// 大多数容器运行时会屏蔽 /proc 中的某些路径，以避免特殊设备或信息被意外暴露。
	DefaultProcMount ProcMountType = "Default"

	// UnmaskedProcMount 绕过容器运行时的默认屏蔽行为，确保容器中新建的 /proc
	// 保持原样，不做任何修改。
	UnmaskedProcMount ProcMountType = "Unmasked"
)

// SELinuxOptions are the labels to be applied to the container.
type SELinuxOptions struct {
	// SELinux user label
	// +optional
	User string
	// SELinux role label
	// +optional
	Role string
	// SELinux type label
	// +optional
	Type string
	// SELinux level label.
	// +optional
	Level string
}

// WindowsSecurityContextOptions 包含 Windows 特有的选项和凭据。
type WindowsSecurityContextOptions struct {
	// GMSACredentialSpecName 是要使用的 GMSA 凭据规范的名称。
	// +optional
	GMSACredentialSpecName *string

	// GMSACredentialSpec 是 GMSA 准入 webhook
	// 该规范由 GMSACredentialSpecName 字段指定。
	// (https://github.com/kubernetes-sigs/windows-gmsa) inlines the contents of the
	// +optional
	GMSACredentialSpec *string

	// The UserName in Windows to run the entrypoint of the container process.
	// Defaults to the user specified in image metadata if unspecified.
	// May also be set in PodSecurityContext. If set in both SecurityContext and
	// PodSecurityContext, the value specified in SecurityContext takes precedence.
	// +optional
	RunAsUserName *string

	// HostProcess 决定容器是否应作为“Host Process”容器运行。
	// Pod 中所有容器的 HostProcess 有效值必须相同
	// （不允许混合使用 HostProcess 容器和非 HostProcess 容器）。
	// 此外，若 HostProcess 为 true，则 HostNetwork 也必须设为 true。
	// +optional
	HostProcess *bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// RangeAllocation is an opaque API object (not exposed to end users) that can be persisted to record
// the global allocation state of the cluster. The schema of Range and Data generic, in that Range
// should be a string representation of the inputs to a range (for instance, for IP allocation it
// might be a CIDR) and Data is an opaque blob understood by an allocator which is typically a
// binary range.  Consumers should use annotations to record additional information (schema version,
// data encoding hints). A range allocation should *ALWAYS* be recreatable at any time by observation
// of the cluster, thus the object is less strongly typed than most.
type RangeAllocation struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// A string representing a unique label for a range of resources, such as a CIDR "10.0.0.0/8" or
	// port range "10000-30000". Range is not strongly schema'd here. The Range is expected to define
	// a start and end unless there is an implicit end.
	Range string
	// A byte array representing the serialized state of a range allocation. Additional clarifiers on
	// the type or format of data should be represented with annotations. For IP allocations, this is
	// represented as a bit array starting at the base IP of the CIDR in Range, with each bit representing
	// a single allocated address (the fifth bit on CIDR 10.0.0.0/8 is 10.0.0.4).
	Data []byte
}

const (
	// DefaultHardPodAffinitySymmetricWeight is the weight of implicit PreferredDuringScheduling affinity rule.
	//
	// RequiredDuringScheduling affinity is not symmetric, but there is an implicit PreferredDuringScheduling affinity rule
	// corresponding to every RequiredDuringScheduling affinity rule.
	// When the --hard-pod-affinity-weight scheduler flag is not specified,
	// DefaultHardPodAffinityWeight defines the weight of the implicit PreferredDuringScheduling affinity rule.
	DefaultHardPodAffinitySymmetricWeight int32 = 1
)

// UnsatisfiableConstraintAction defines the actions that can be taken for an
// unsatisfiable constraint.
type UnsatisfiableConstraintAction string

const (
	// DoNotSchedule 指示调度器在约束未满足时不要调度该 Pod。
	DoNotSchedule UnsatisfiableConstraintAction = "DoNotSchedule"
	// ScheduleAnyway 指示调度器即使约束未满足也调度该 Pod。
	ScheduleAnyway UnsatisfiableConstraintAction = "ScheduleAnyway"
)

// NodeInclusionPolicy 定义节点纳入策略的类型。
// +enum
type NodeInclusionPolicy string

const (
	// NodeInclusionPolicyIgnore 表示在计算 Pod 拓扑分布倾斜时忽略此调度指令。
	NodeInclusionPolicyIgnore NodeInclusionPolicy = "Ignore"
	// NodeInclusionPolicyHonor 表示在计算 Pod 拓扑分布倾斜时使用此调度指令。
	NodeInclusionPolicyHonor NodeInclusionPolicy = "Honor"
)

// TopologySpreadConstraint 指定如何将匹配的 Pod 分布到给定的拓扑结构中。
type TopologySpreadConstraint struct {
	// MaxSkew 描述 Pod 分布不均匀的最大程度。
	// 当 `whenUnsatisfiable=DoNotSchedule` 时，它是目标拓扑中匹配 Pod 数量与全局最小值之间
	// 允许的最大差值。全局最小值是合格域中匹配 Pod 数量的最小值；如果合格域数量少于
	// MinDomains，则全局最小值为 0。
	// 例如，在一个有 3 个可用区的集群中，MaxSkew 设为 1，具有相同 labelSelector 的 Pod
	// 分布为 2/2/1：
	// 此时全局最小值为 1。
	// - 若 MaxSkew 为 1，新来的 Pod 只能调度到 zone3，使其变为 2/2/2；
	// 若调度到 zone1（zone2），则 zone1（zone2）上的 ActualSkew（3-1）会违反 MaxSkew（1）。
	// - 若 MaxSkew 为 2，新来的 Pod 可以调度到任意可用区。
	// 当 `whenUnsatisfiable=ScheduleAnyway` 时，该值用于对满足约束的拓扑赋予更高优先级。
	// 这是必填字段。默认值为 1，不允许为 0。
	// +-------+-------+-------+
	// +-------+-------+-------+
	// +-------+-------+-------+
	MaxSkew int32
	// TopologyKey is the key of node labels. Nodes that have a label with this key
	// and identical values are considered to be in the same topology.
	// We consider each <key, value> as a "bucket", and try to put balanced number
	// of pods into each bucket.
	// We define a domain as a particular instance of a topology.
	// Also, we define an eligible domain as a domain whose nodes meet the requirements of
	// nodeAffinityPolicy and nodeTaintsPolicy.
	// e.g. If TopologyKey is "kubernetes.io/hostname", each Node is a domain of that topology.
	// And, if TopologyKey is "topology.kubernetes.io/zone", each zone is a domain of that topology.
	// It's a required field.
	TopologyKey string
	// WhenUnsatisfiable 指示当 Pod 不满足分布约束时如何处理。
	// - DoNotSchedule（默认）告诉调度器不要调度该 Pod。
	// - ScheduleAnyway 告诉调度器可以将 Pod 调度到任意位置，
	// 但优先选择有助于降低倾斜度的拓扑。
	// 当且仅当对某个新来的 Pod，所有可能的节点分配都会在某一拓扑上违反 "MaxSkew" 时，
	// 才认为该约束对此 Pod "无法满足"。
	// 例如，在一个有 3 个可用区的集群中，MaxSkew 设为 1，具有相同 labelSelector 的 Pod
	// 分布为 3/1/1：
	// 若 WhenUnsatisfiable 设为 DoNotSchedule，新来的 Pod 只能调度到 zone2（zone3），
	// 使其变为 3/2/1（3/1/2），因为 zone2（zone3）上的 ActualSkew（2-1）满足 MaxSkew（1）。
	// 换句话说，集群仍可能是不均衡的，但调度器不会让它*更*不均衡。
	// 这是必填字段。
	// +-------+-------+-------+
	// +-------+-------+-------+
	// +-------+-------+-------+
	WhenUnsatisfiable UnsatisfiableConstraintAction
	// LabelSelector 用于查找匹配的 Pod。
	// 匹配该标签选择器的 Pod 会被计数，以确定其所在拓扑域中的 Pod 数量。
	// +optional
	LabelSelector *metav1.LabelSelector
	// MinDomains 表示合格域的最小数量。
	// 当具有匹配拓扑键的合格域数量少于 minDomains 时，Pod 拓扑分布会将"全局最小值"视为 0，
	// 然后进行倾斜度的计算。
	// 当具有匹配拓扑键的合格域数量大于或等于 minDomains 时，此值对调度没有影响。
	// 因此，当合格域数量少于 minDomains 时，调度器向这些域调度的 Pod 不会超过 maxSkew 个。
	// 若值为 nil，则该约束的行为等同于 MinDomains 等于 1。
	// 有效值为大于 0 的整数。
	// 当值不为 nil 时，WhenUnsatisfiable 必须为 DoNotSchedule。
	// 例如，在一个有 3 个可用区的集群中，MaxSkew 设为 2，MinDomains 设为 5，
	// 具有相同 labelSelector 的 Pod 分布为 2/2/2：
	// 域数量少于 5（MinDomains），因此"全局最小值"被视为 0。
	// 此时具有相同 labelSelector 的新 Pod 无法被调度，
	// 因为若将其调度到三个可用区中的任何一个，计算出的倾斜度将为 3（3 - 0），
	// 从而违反 MaxSkew。
	// +-------+-------+-------+
	// +-------+-------+-------+
	// +-------+-------+-------+
	// +optional
	MinDomains *int32
	// NodeAffinityPolicy 指示在计算 Pod 拓扑分布倾斜度时如何处理 Pod 的 nodeAffinity/nodeSelector。
	// 可选项为：
	// - Honor：仅将匹配 nodeAffinity/nodeSelector 的节点纳入计算。
	// - Ignore：忽略 nodeAffinity/nodeSelector，所有节点都纳入计算。
	// 若此值为 nil，则行为等同于 Honor 策略。
	// +optional
	NodeAffinityPolicy *NodeInclusionPolicy
	// NodeTaintsPolicy 指示在计算 Pod 拓扑分布倾斜度时如何处理节点污点。
	// 可选项为：
	// - Honor：包含无污点的节点，以及新来的 Pod 能够容忍其污点的节点。
	// - Ignore：忽略节点污点，包含所有节点。
	// 若此值为 nil，则行为等同于 Ignore 策略。
	// +optional
	NodeTaintsPolicy *NodeInclusionPolicy
	// MatchLabelKeys is a set of pod label keys to select the pods over which
	// spreading will be calculated. The keys are used to lookup values from the
	// incoming pod labels, those key-value labels are ANDed with labelSelector
	// to select the group of existing pods over which spreading will be calculated
	// for the incoming pod. The same key is forbidden to exist in both MatchLabelKeys and LabelSelector.
	// MatchLabelKeys cannot be set when LabelSelector isn't set.
	// Keys that don't exist in the incoming pod labels will
	// be ignored. A null or empty list means only match against labelSelector.
	//
	// This is a beta field and requires the MatchLabelKeysInPodTopologySpread feature gate to be enabled (enabled by default).
	// +listType=atomic
	// +optional
	MatchLabelKeys []string
}

// These are the built-in errors for PortStatus.
const (
	// MixedProtocolNotSupported 在 PortStatus 中表示云端提供商无法在负载均衡器上发布该端口，
	// 因为同一 LoadBalancer 类型 Service 上混合使用多种协议不受云端提供商支持。
	MixedProtocolNotSupported = "MixedProtocolNotSupported"
)

// PortStatus 表示 Service 端口的错误状态。
type PortStatus struct {
	// Port 是记录此处状态的 Service 端口的端口号。
	Port int32
	// Protocol is the protocol of the service port of which status is recorded here
	Protocol Protocol
	// Error 记录 Service 端口的问题。
	// 错误格式必须遵循以下规则：
	// - 内置错误值必须在本文件中指定，且使用 CamelCase 命名
	// - 云端提供商特有的错误值必须符合 foo.example.com/CamelCase 格式。
	// 其匹配的正则表达式为 (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// ---
	// +optional
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Error *string
}

// LoadBalancerIPMode 表示 LoadBalancer 入口 IP 的模式。
type LoadBalancerIPMode string

const (
	// LoadBalancerIPModeVIP 表示流量被送达节点，目标地址设为负载均衡器的 IP 和端口。
	LoadBalancerIPModeVIP LoadBalancerIPMode = "VIP"
	// LoadBalancerIPModeProxy 表示流量被送达节点或 Pod，目标地址设为节点的 IP 和端口或 Pod 的 IP 和端口。
	LoadBalancerIPModeProxy LoadBalancerIPMode = "Proxy"
)

// ImageVolumeSource 表示一个镜像卷资源。
type ImageVolumeSource struct {
	// Required: Image or artifact reference to be used.
	// Behaves in the same way as pod.spec.containers[*].image.
	// Pull secrets will be assembled in the same way as for the container image by looking up node credentials, SA image pull secrets, and pod spec image pull secrets.
	// More info: https://kubernetes.io/docs/concepts/containers/images
	// This field is optional to allow higher level config management to default or override
	// container images in workload controllers like Deployments and StatefulSets.
	// +optional
	Reference string

	// 拉取 OCI 对象的策略。可能的值为：
	// Always：kubelet 总是尝试拉取引用。若拉取失败，容器创建将失败。
	// Never：kubelet 从不拉取引用，只使用本地镜像或制品。若引用不存在，容器创建将失败。
	// IfNotPresent：若磁盘上不存在该引用，kubelet 才会拉取。若引用不存在且拉取失败，容器创建将失败。
	// 指定 :latest 标签时默认为 Always，否则默认为 IfNotPresent。
	// +optional
	PullPolicy PullPolicy
}

// NodeAllocatableResourceClaimStatus describes the status of node allocatable resources allocated via DRA.
type NodeAllocatableResourceClaimStatus struct {
	// ResourceClaimName is the resource claim referenced by the pod that resulted in this node allocatable resource allocation.
	// +required
	ResourceClaimName string
	// Containers lists the names of all containers in this pod that reference the claim.
	// +optional
	// +listType=set
	Containers []string
	// Mapping contains allocations through devices mapped in `device.nodeAllocatableResources.mapping`.
	// This is used by kubelet for pod level and container level cgroup enforcement.
	// +optional
	// +listType=map
	// +listMapKey=name
	Mapping []NodeAllocatableMappedResources
	// Overhead contains allocations through devices mapped in `device.nodeAllocatableResources.overhead`.
	// This is used by kubelet for pod level and container level cgroup enforcement.
	// +optional
	// +listType=map
	// +listMapKey=name
	Overhead []NodeAllocatableOverheadResources
}

// NodeAllocatableMappedResources describes mapped node allocatable resource allocations.
type NodeAllocatableMappedResources struct {
	// Name is the name of the resource (e.g., cpu, memory).
	// +required
	Name ResourceName
	// Quantity is the total node allocatable resource capacity allocated for the claim.
	// This claim's allocated devices is shared by all the containers referencing the claim.
	// Kubelet adds this value to both requests and limits at the pod-level cgroup, and to limits at the container-level cgroup for each container referencing the claim.
	// +required
	Quantity *resource.Quantity
}

// NodeAllocatableOverheadResources describes auxiliary overhead resource allocations.
type NodeAllocatableOverheadResources struct {
	// Name is the name of the resource (e.g., cpu, memory).
	// +required
	Name ResourceName
	// PerPod is the flat overhead quantity allocated per pod.
	// Adding to each container limit allows individual containers to utilize the overhead, while the parent pod-level cgroup limit caps the total usage at the pod boundary where the overhead is accounted for exactly once.
	// At least one of PerPod or PerContainer must be specified. Specifying neither is an invalid configuration.
	// +optional
	PerPod *resource.Quantity
	// PerContainer is the variable overhead quantity applied for each container referencing the claim.
	// The container references are recorded in `nodeAllocatableResourceClaimStatuses.containers`.
	// The total overhead quantity allocated for the claim is computed as:
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet accounts for this overhead in cgroups:
	// - Pod-level cgroup (requests and limits): Kubelet adds PerPod + (PerContainer * NumReferences).
	// - Container-level cgroup (limits only): Kubelet adds PerPod + PerContainer for each referencing container.
	// This allows any single container to access the pod-level overhead, while the parent cgroup caps the total usage to account for PerPod exactly once.
	// At least one of PerPod or PerContainer must be specified. Specifying neither is an invalid configuration.
	// +optional
	PerContainer *resource.Quantity
}
