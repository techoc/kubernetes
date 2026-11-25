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

// Volume 表示 Pod 中的一个具名卷，该 Pod 内的任何容器都可以访问它。
type Volume struct {
	// Required: 这必须是一个 DNS_LABEL。Pod 中的每个卷都必须拥有
	// 唯一的名称。
	Name string
	// VolumeSource 表示要挂载的卷的位置和类型。
	// 目前为可选项。若未指定，则该 Volume 被隐式视为 EmptyDir。
	// 此隐含行为已弃用，将在未来版本中移除。
	// +optional
	VolumeSource
}

// VolumeSource 表示要挂载的卷的来源位置。
// 只能指定其中一个成员。
type VolumeSource struct {
	// hostPath 表示宿主机上直接暴露给容器的文件或目录。通常用于系统
	// agent 或其他被允许查看宿主机器的特权事物。
	// 大多数容器都不需要它。
	// ---
	// TODO(jonesdl) 我们需要限制谁可以使用宿主目录挂载，以及谁可以/不可以
	// 以读写方式挂载宿主目录。
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
	// awsElasticBlockStore 表示挂载到 kubelet 宿主机、再暴露给 Pod 的 AWS EBS 磁盘。
	// Deprecated: AWSElasticBlockStore 已弃用。树内 awsElasticBlockStore 类型的所有操作
	// 都会重定向到 ebs.csi.aws.com CSI 驱动。
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
	// persistentVolumeClaim 表示对同一命名空间内某个 PersistentVolumeClaim 的引用
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

	// cephFS 表示主机上与 Pod 生命周期相同的 Ceph FS 挂载。
	// Deprecated: CephFS 已弃用，不再支持树内 cephfs 类型。
	// +optional
	CephFS *CephFSVolumeSource

	// flocker 表示挂载到 kubelet 主机的 Flocker 卷。依赖 Flocker 控制服务正在运行。
	// Deprecated: Flocker 已弃用，不再支持树内 flocker 类型。
	// +optional
	Flocker *FlockerVolumeSource

	// downwardAPI 表示应当用于填充该卷的 Pod 元数据
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
	// ConfigMap 表示应当用于填充该卷的 ConfigMap
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
	// storageOS 表示附加到 kubelet 宿主机上并挂载进 Pod 的 StorageOS 卷。
	// Deprecated: StorageOS 已弃用，不再支持树内 storageos 类型。
	// +optional
	StorageOS *StorageOSVolumeSource
	// csi（容器存储接口）表示由某些外部 CSI 驱动处理的临时存储。
	// +optional
	CSI *CSIVolumeSource
	// ephemeral 表示由集群存储驱动处理的卷。
	// 该卷的生命周期与定义它的 Pod 绑定——它会在 Pod 启动前创建，
	// 并在 Pod 被移除时删除。
	//
	// 满足以下条件时使用它：
	// a) 仅在 Pod 运行期间需要该卷，
	// b) 需要普通卷的特性，例如从快照恢复或容量
	//    跟踪，
	// c) 存储驱动通过 storage class 指定，并且
	// d) 存储驱动支持通过
	//    PersistentVolumeClaim 动态供给卷（关于此卷类型
	//    与 PersistentVolumeClaim 之间关系的更多
	//    信息，参见 EphemeralVolumeSource）。
	//
	// 对于生命周期长于单个 Pod 的持久卷，
	// 请使用 PersistentVolumeClaim 或某个厂商专用的
	// API。
	//
	// 如果 CSI 驱动适合以该方式使用，则用 CSI 处理轻量级本地临时卷——
	// 更多信息参见该驱动的文档。
	//
	// 一个 Pod 可以同时使用两类临时卷和
	// 持久卷。
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

// PersistentVolumeSource 与 VolumeSource 类似，但面向创建 PV 的管理者。
// 其成员中必须恰好设置一个。
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
	// hostPath 表示主机上的目录。
	// 由开发者或测试者供给。
	// 仅对单节点开发与测试有用！
	// 主机上存储不受任何形式支持，在多节点集群中绝对无法工作。
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

// PersistentVolumeClaimVolumeSource 表示对同一命名空间内某个 PersistentVolumeClaim 的引用
type PersistentVolumeClaimVolumeSource struct {
	// ClaimName 是与使用此卷的 Pod 处于同一命名空间的 PersistentVolumeClaim 的名称
	ClaimName string
	// Optional: 默认为 false（读写）。此处的 ReadOnly
	// 会强制 VolumeMounts 采用 ReadOnly 设置
	// +optional
	ReadOnly bool
}

const (
	// BetaStorageClassAnnotation 表示 beta/旧版 StorageClass 注解。
	// 已弃用，将在未来版本中移除。（#51440）
	BetaStorageClassAnnotation = "volume.beta.kubernetes.io/storage-class"

	// MountOptionAnnotation 定义 PV 中使用的 mountOptions 注解
	MountOptionAnnotation = "volume.beta.kubernetes.io/mount-options"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolume 结构体捕获 PV 存储实现的相关信息
type PersistentVolume struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义集群拥有的持久卷
	// +optional
	Spec PersistentVolumeSpec

	// Status 表示持久卷的当前信息。
	// +optional
	Status PersistentVolumeStatus
}

// PersistentVolumeSpec 包含定义持久卷所需的大部分信息
type PersistentVolumeSpec struct {
	// Resources 表示该卷的实际资源
	Capacity ResourceList
	// Source 表示要挂载的卷的位置和类型。
	PersistentVolumeSource
	// AccessModes 包含该卷所有可被挂载的方式
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// ClaimRef 是 PersistentVolume 与 PersistentVolumeClaim 之间双向绑定的一部分。
	// 绑定时 ClaimRef 预期为非 nil。
	// claim.VolumeName 才是 PV 与 PVC 绑定的权威依据。
	// 当设为非 nil 值时，被引用 PVC 的 PVC.Spec.Selector 会被忽略，
	// 即此 PV 的标签无需与 PVC 的 selector 匹配。
	// +optional
	ClaimRef *ObjectReference
	// Optional: 持久卷从其 claim 释放后会怎样处理。
	// +optional
	PersistentVolumeReclaimPolicy PersistentVolumeReclaimPolicy
	// 该持久卷所属 StorageClass 的名称。空值
	// 表示该卷不属于任何 StorageClass。
	// +optional
	StorageClassName string
	// 挂载选项列表，例如 ["ro", "soft"]。不做校验——只要有一项无效，
	// 挂载就会直接失败。
	// +optional
	MountOptions []string
	// volumeMode 定义卷是应使用格式化文件系统，还是保持原始块状态。
	// spec 中未包含时隐含为 Filesystem。
	// +optional
	VolumeMode *PersistentVolumeMode
	// NodeAffinity 定义限制该卷可以从哪些节点访问的约束。
	// 此字段会影响使用该卷的 Pod 的调度。
	// 若启用 MutablePVNodeAffinity 特性门控，此字段可变。
	// +optional
	NodeAffinity *VolumeNodeAffinity
	// 该持久卷所属 VolumeAttributesClass 的名称。不允许
	// 为空值。未设置此字段时表示该卷不属于任何
	// VolumeAttributesClass。此字段可变，在卷被成功更新到新 class 之后，
	// CSI 驱动可以修改它。
	// 对于未绑定的 PersistentVolume，volumeAttributesClassName 会在绑定过程中
	// 与未绑定的 PersistentVolumeClaim 进行匹配。
	// +featureGate=VolumeAttributesClass
	// +optional
	VolumeAttributesClassName *string
}

// VolumeNodeAffinity 定义限制可从哪些节点访问该卷的约束。
type VolumeNodeAffinity struct {
	// required 指定必须满足的硬性节点约束。
	Required *NodeSelector
}

// PersistentVolumeReclaimPolicy 描述持久卷生命周期结束后的维护策略
type PersistentVolumeReclaimPolicy string

const (
	// PersistentVolumeReclaimRecycle 表示卷从其声明释放后会被回收到未绑定持久卷池中。
	// 卷插件必须支持回收（Recycling）。
	// DEPRECATED: PersistentVolumeReclaimRecycle（Recycle）正在被弃用，公告见: https://groups.google.com/forum/#!topic/kubernetes-dev/uexugCza84I
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

// PersistentVolumeStatus 表示 PV 存储的状态
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
	// LastPhaseTransitionTime 是阶段从一个迁移到另一个的时间，
	// 每次卷阶段发生迁移时都会自动重置为当前时间。
	// +optional
	LastPhaseTransitionTime *metav1.Time
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolumeList 表示 PV 的列表
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

	// Spec 定义 Pod 作者请求的卷
	// +optional
	Spec PersistentVolumeClaimSpec

	// Status 表示关于该 claim 的当前信息
	// +optional
	Status PersistentVolumeClaimStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PersistentVolumeClaimList 表示 PV claim 的列表
type PersistentVolumeClaimList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta
	Items []PersistentVolumeClaim
}

// PersistentVolumeClaimSpec 描述存储设备的通用属性，
// 并允许通过 Source 承载供应商专用属性
type PersistentVolumeClaimSpec struct {
	// 包含所要求的访问模式类型
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// 用于筛选候选绑定卷的标签查询。当 VolumeName 已设置时，
	// 此 selector 会被忽略
	// +optional
	Selector *metav1.LabelSelector
	// Resources 表示所要求的最小资源
	// 若启用 RecoverVolumeExpansionFailure 特性，用户可以指定低于先前值的资源要求，
	// 但仍然必须高于 claim 的
	// status 字段中记录的容量。
	// +optional
	Resources VolumeResourceRequirements
	// VolumeName 是对支撑该 claim 的 PersistentVolume 的绑定引用。
	// 设为非空值时不会评估 Selector
	// +optional
	VolumeName string
	// 该 claim 所要求的 StorageClass 名称。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes/#class-1
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
	// 指定用哪个对象为卷填充数据（当需要一个非空卷时）。可以是来自非空 API 组的
	// 任意对象（非核心对象），或一个
	// PersistentVolumeClaim 对象。
	// 指定此字段时，只有当所指定对象的类型与某个已安装的卷填充器或动态
	// 供给器匹配时，卷绑定才会成功。
	// 此字段将取代 dataSource 字段的功能，因此
	// 若两个字段均非空，它们必须具有相同的值。为向后
	// 兼容，当 dataSourceRef 中未指定 namespace 时，
	// 若两字段（dataSource 与 dataSourceRef）之一为空而另一个非空，
	// 两者会自动被设置为相同的
	// 值。
	// 当 dataSourceRef 中指定了 namespace 时，
	// dataSource 不会设为相同值，且必须为空。
	// dataSource 与 dataSourceRef 之间有三个重要区别：
	// * dataSource 只允许两种特定类型的对象，而 dataSourceRef
	//   允许任意非核心对象以及 PersistentVolumeClaim 对象。
	// * dataSource 会忽略不允许的值（将其丢弃），而 dataSourceRef
	//   保留所有值，并在指定了
	//   不允许的值时生成错误。
	// * dataSource 只允许本地对象，而 dataSourceRef 允许
	//   任意命名空间中的对象。
	// +optional
	DataSourceRef *TypedObjectReference
	// volumeAttributesClassName 可用于设置此声明使用的 VolumeAttributesClass。
	// 若指定，CSI 驱动会使用相应 VolumeAttributesClass 中定义的属性创建或更新卷。
	// 它的用途与 storageClassName 不同，可在声明创建后修改。空字符串或 nil 表示不会对该声明应用任何 VolumeAttributesClass。
	// 如果声明进入 Infeasible 错误状态，可将此字段重置为之前的值（包括 nil）以取消修改。
	// 如果 volumeAttributesClass 引用的资源不存在，此 PersistentVolumeClaim 会被置为 Pending 状态，
	// 如 modifyVolumeStatus 字段所示，直至该资源存在为止。
	// 更多信息参见: https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/
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
// 有效值为：
//   - "Resizing", "FileSystemResizePending"
//
// 如果启用 RecoverVolumeExpansionFailure 特性门控，则还可能出现以下额外值：
//   - "ControllerResizeError", "NodeResizeError"
//
// 若启用 VolumeAttributesClass 特性门控，则可能还有以下额外值：
//   - "ModifyVolumeError", "ModifyingVolume"
type PersistentVolumeClaimConditionType string

// 以下是 PVC 的有效条件
const (
	// 用户触发的 PVC 扩容已开始
	PersistentVolumeClaimResizing PersistentVolumeClaimConditionType = "Resizing"
	// PersistentVolumeClaimFileSystemResizePending 表示控制器扩容已完成，节点上仍待进行文件系统扩容
	PersistentVolumeClaimFileSystemResizePending PersistentVolumeClaimConditionType = "FileSystemResizePending"

	// PersistentVolumeClaimControllerResizeError 表示控制器扩容卷容量时发生错误
	PersistentVolumeClaimControllerResizeError PersistentVolumeClaimConditionType = "ControllerResizeError"
	// PersistentVolumeClaimNodeResizeError 表示节点扩容卷容量时发生错误。
	PersistentVolumeClaimNodeResizeError PersistentVolumeClaimConditionType = "NodeResizeError"

	// 应用目标 VolumeAttributesClass 时遇到错误
	PersistentVolumeClaimVolumeModifyVolumeError PersistentVolumeClaimConditionType = "ModifyVolumeError"
	// 卷正在被修改
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
// 当控制器收到某个持久卷声明的更新，其中某资源的 ClaimResourceStatus
// 是其无法识别的，则控制器应当忽略该更新，
// 让其他控制器处理。
type ClaimResourceStatus string

const (
	// 扩容控制器开始在控制面扩容卷时设置的状态
	PersistentVolumeClaimControllerResizeInProgress ClaimResourceStatus = "ControllerResizeInProgress"

	// 当调整容量控制器因不可恢复的终结性错误而扩容失败时设置的状态。
	// 超时等瞬时错误不应设置此状态，也不应修改 allocatedResourceStatus，
	// 以便调整容量控制器可以继续卷扩容。
	PersistentVolumeClaimControllerResizeInfeasible ClaimResourceStatus = "ControllerResizeInfeasible"

	// 扩容控制器已完成卷扩容，但节点上仍需要
	// 进一步扩容卷时设置的状态。
	PersistentVolumeClaimNodeResizePending ClaimResourceStatus = "NodeResizePending"
	// 当 kubelet 开始扩容卷时设置的状态
	PersistentVolumeClaimNodeResizeInProgress ClaimResourceStatus = "NodeResizeInProgress"
	// 当 kubelet 扩容因不可恢复的终结性错误而失败时设置的状态。瞬时错误不应设置此状态
	PersistentVolumeClaimNodeResizeInfeasible ClaimResourceStatus = "NodeResizeInfeasible"
)

// +enum
// 未来可以新增状态。消费者应当检查未知状态并以合适的方式失败
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

// PersistentVolumeClaimCondition 表示 PV claim 的当前条件
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

// VolumeHealthCondition 表示上报的某个卷的不良健康状况。
type VolumeHealthCondition struct {
	// status 是可机器解析的健康类别。
	// 可能取值：
	// - "Inaccessible"：无法访问该卷。
	// - "DataLoss"：检测到该卷发生数据丢失。
	// - "Degraded"：卷仍可工作，但能力下降。
	Status VolumeHealthStatusType
	// reason 是简短的、机器可解析的 CamelCase 原因。
	// 与 status 一起构成某个条件条目的唯一标识。
	Reason string
	// message 是人类可读的描述。
	// +optional
	Message string
}

// VolumeHealthStatus 包含 CSI 控制器插件上报的
// 某个卷的健康信息。
type VolumeHealthStatus struct {
	// conditions 是 CSI 控制器插件上报的
	// 不良条件集合。空列表表示没有不良条件。
	// 最多可上报 16 个条件。
	// +optional
	// +listType=map
	// +listMapKey=status
	// +listMapKey=reason
	HealthConditions []VolumeHealthCondition
	// lastTransitionTime 是当前这组条件首次出现的时间。
	// +optional
	LastTransitionTime metav1.Time
}

// PodVolumeHealth 包含 Pod 使用的某个卷的健康信息，
// 由 CSI 节点插件通过 kubelet 上报。
type PodVolumeHealth struct {
	// name 与 pod.spec.volumes 中的某一项匹配。
	Name string
	// conditions 是 CSI 节点插件为本节点上该卷上报的
	// 不良条件集合。
	// 最多可上报 16 个条件。
	// +optional
	// +listType=map
	// +listMapKey=status
	// +listMapKey=reason
	HealthConditions []VolumeHealthCondition
	// lastTransitionTime 是当前这组条件首次出现的时间。
	// +optional
	LastTransitionTime metav1.Time
}

// PersistentVolumeClaimStatus 表示 PV claim 的状态
type PersistentVolumeClaimStatus struct {
	// Phase 表示 PersistentVolumeClaim 的当前阶段
	// +optional
	Phase PersistentVolumeClaimPhase
	// AccessModes 包含支撑 PVC 的卷所有可被挂载的方式
	// +optional
	AccessModes []PersistentVolumeAccessMode
	// 表示底层卷的实际资源
	// +optional
	Capacity ResourceList
	// +optional
	Conditions []PersistentVolumeClaimCondition
	// AllocatedResources 跟踪分配给 PVC 的资源，包括其容量。
	// 键名遵循标准 Kubernetes 标签语法。有效取值为以下之一：
	// 	* 无前缀键：
	//		- storage - 卷的容量。
	//	* 自定义资源必须使用实现定义的前缀名称，例如 "example.com/my-custom-resource"
	// 除上述取值外——无前缀或带 kubernetes.io 前缀的键被视为
	// 保留键，因此不得使用。
	//
	// 请求卷扩容操作时，此处上报的容量
	// 可能大于实际容量。
	// 存储配额取 allocatedResources 与 PVC.spec.resources 中的较大值。
	// 若未设置 allocatedResources，配额计算仅使用 PVC.spec.resources。
	// 若卷扩容容量请求被调低，只有在没有进行中的扩容操作、
	// 且实际卷容量等于或低于所请求容量时，
	// allocatedResources 才会被调低。
	//
	// 收到带有先前未知 resourceName 的 PVC 更新的控制器，
	// 应当忽略超出其设计用途的该更新。例如——仅负责
	// 扩容卷容量的控制器，应当忽略更改了与 PVC 关联的其他有效
	// 资源的 PVC 更新。
	// +optional
	AllocatedResources ResourceList
	// AllocatedResourceStatuses 存储给定 PVC 正在扩容的资源的状态。
	// 键名遵循标准 Kubernetes 标签语法。有效取值为以下之一：
	// 	* 无前缀键：
	//		- storage - 卷的容量。
	//	* 自定义资源必须使用实现定义的前缀名称，例如 "example.com/my-custom-resource"
	// 除上述取值外——无前缀或带 kubernetes.io 前缀的键被视为
	// 保留键，因此不得使用。
	//
	// ClaimResourceStatus 可处于以下任一状态：
	//	- ControllerResizeInProgress:
	//		扩容控制器开始在控制面扩容卷时设置的状态。
	// 	- ControllerResizeFailed:
	//		扩容在 resize 控制器中因终结性错误失败时设置的状态。
	//	- NodeResizePending:
	//		扩容控制器已完成卷扩容，但节点上仍需要
	//		进一步扩容卷时设置的状态。
	//	- NodeResizeInProgress:
	//		kubelet 开始扩容卷时设置的状态。
	//	- NodeResizeFailed:
	//		扩容在 kubelet 中因终结性错误失败时设置的状态。瞬时错误不会设置
	//		NodeResizeFailed。
	// 例如：若为更多容量扩容 PVC——此字段可为以下状态之一：
	// 	- pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeFailed"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizePending"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeFailed"
	// 未设置此字段时，表示给定 PVC 没有进行中的扩容操作。
	//
	// 收到带有先前未知 resourceName 或 ClaimResourceStatus 的 PVC 更新的控制器，
	// 应当忽略超出其设计用途的该更新。例如——仅负责
	// 扩容卷容量的控制器，应当忽略更改了与 PVC 关联的其他有效
	// 资源的 PVC 更新。
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
	// healthStatus 包含控制器最近一次为该 claim 绑定的卷
	// 上报的健康信息。
	// +featureGate=CSIVolumeHealth
	// +optional
	HealthStatus *VolumeHealthStatus
}

// PersistentVolumeAccessMode 定义 PV 的各种访问模式。
type PersistentVolumeAccessMode string

// 以下是 PersistentVolumeAccessMode 的有效取值
const (
	// 可以以读写模式挂载到恰好 1 台主机
	ReadWriteOnce PersistentVolumeAccessMode = "ReadWriteOnce"
	// 可以以只读模式挂载到多台主机
	ReadOnlyMany PersistentVolumeAccessMode = "ReadOnlyMany"
	// 可以以读写模式挂载到多台主机
	ReadWriteMany PersistentVolumeAccessMode = "ReadWriteMany"
	// 可以以读写模式挂载到恰好 1 个 Pod
	// 不能与其他访问模式组合使用
	ReadWriteOncePod PersistentVolumeAccessMode = "ReadWriteOncePod"
)

// PersistentVolumePhase 定义 PV 所处的阶段
type PersistentVolumePhase string

// 以下是 PersistentVolumePhase 的有效取值
const (
	// 用于不可用的 PersistentVolume
	VolumePending PersistentVolumePhase = "Pending"
	// 用于尚未绑定的 PersistentVolume
	// 可用卷由 binder 持有，并与 PersistentVolumeClaim 匹配
	VolumeAvailable PersistentVolumePhase = "Available"
	// 用于已绑定的 PersistentVolume
	VolumeBound PersistentVolumePhase = "Bound"
	// 用于所绑定的 PersistentVolumeClaim 已被删除的 PersistentVolume
	// 已释放的卷必须经过回收才能重新变为可用
	// 持久卷声明 binder 使用此阶段来通知另一进程回收该资源
	VolumeReleased PersistentVolumePhase = "Released"
	// 用于从声明释放后未能正确回收或删除的持久卷
	VolumeFailed PersistentVolumePhase = "Failed"
)

// PersistentVolumeClaimPhase 定义 PV claim 的阶段
type PersistentVolumeClaimPhase string

// 以下是 PersistentVolumeClaimPhase 的有效取值
const (
	// 用于尚未绑定的持久卷声明
	ClaimPending PersistentVolumeClaimPhase = "Pending"
	// 用于已绑定的 PersistentVolumeClaim
	ClaimBound PersistentVolumeClaimPhase = "Bound"
	// 用于失去其底层 PersistentVolume 的 PersistentVolumeClaim。
	// 该 claim 曾绑定过一个 PersistentVolume，而此卷已不复存在、其上所有数据均已丢失。
	ClaimLost PersistentVolumeClaimPhase = "Lost"
)

// HostPathType 定义 PV 的宿主路径类型
type HostPathType string

// 以下是 HostPathType 的有效取值
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

// HostPathVolumeSource 表示映射进 Pod 的主机路径。
// 主机路径卷不支持所有权管理或 SELinux 重标记。
type HostPathVolumeSource struct {
	// 如果路径是符号链接，将跟随链接到真实路径。
	Path string
	// 默认为 ""
	Type *HostPathType
}

// EmptyDirVolumeSource 表示 Pod 的空目录。
// 空目录卷支持所有权管理和 SELinux 重标记。
type EmptyDirVolumeSource struct {
	// TODO: 从长期来看，我们希望像调度问题那样来表达底层介质的选择——
	// 用户说明他们需要的特性，我们提供满足这些特性的存储后端。
	// 目前这样能满足最常见的需求。
	// 可选：应使用什么类型的存储介质作为此目录的后端。
	// 默认为 ""，表示使用节点的默认介质。
	// +optional
	Medium StorageMedium
	// 此 EmptyDir 卷所需的本地存储总量。
	// 该大小限制同样适用于 memory 介质。
	// memory 介质 EmptyDir 的最大用量为此处指定的 SizeLimit
	// 与 Pod 中所有容器内存限制之和两者中的最小值。
	// 默认为 nil，表示限制未定义。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#emptydir
	// +optional
	SizeLimit *resource.Quantity
	// mode 以数字表示法指定 emptyDir 目录的权限位
	// （例如 0755、01777）。必须是 0000 到 01777 之间的值。
	// 未指定时默认为 0777。
	// 这可能与影响文件模式的其他选项（如 fsGroup）冲突。
	// 若指定了 fsGroup，fsGroup 的权限将覆盖此处指定的 mode。
	// 此字段在 Windows 上无效。
	// +optional
	Mode *int32
}

// StorageMedium 定义卷的存储分配方式。
type StorageMedium string

// 以下是 StorageMedium 的有效取值
const (
	StorageMediumDefault         StorageMedium = ""           // 使用节点的默认介质
	StorageMediumMemory          StorageMedium = "Memory"     // 使用内存（tmpfs）
	StorageMediumHugePages       StorageMedium = "HugePages"  // 使用大页
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

// GCEPersistentDiskVolumeSource 表示 Google Compute Engine 中的持久磁盘（Persistent Disk）资源。
//
// GCE PD 必须在挂载到容器之前已存在。该磁盘还必须与 kubelet
// 位于同一 GCE 项目和可用区。GCE PD 只能以读写方式
// 挂载一次，或以只读方式挂载多次。GCE PD 支持所有权管理和
// SELinux 重标记。
type GCEPersistentDiskVolumeSource struct {
	// PD 资源的唯一名称。用于在 GCE 中标识该磁盘
	PDName string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：要挂载的磁盘分区。
	// 若省略，kubelet 将尝试挂载设备名。
	// 例如 /dev/sda1 对应此字段为 "1"，/dev/sda 对应此字段为 0 或空。
	// +optional
	Partition int32
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// ISCSIVolumeSource 表示一个 ISCSI 磁盘。
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIVolumeSource struct {
	// Required: iSCSI 目标门户（target portal）
	// portal 是一个 IP；若端口非默认（通常为 TCP 端口 860 和 3260），则为 ip_addr:port
	// +optional
	TargetPortal string
	// Required: 目标 iSCSI Qualified Name
	// +optional
	IQN string
	// Required: iSCSI target 的 lun 编号
	// +optional
	Lun int32
	// 可选：默认为 'default'（tcp）。使用 iSCSI 传输的 iSCSI 接口名称。
	// +optional
	ISCSIInterface string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：用于高可用的 iSCSI target portal IP 列表。
	// portal 是一个 IP；若端口非默认（通常为 TCP 端口 860 和 3260），则为 ip_addr:port
	// +optional
	Portals []string
	// 可选：是否支持 iSCSI Discovery CHAP 认证
	// +optional
	DiscoveryCHAPAuth bool
	// 可选：是否支持 iSCSI Session CHAP 认证
	// +optional
	SessionCHAPAuth bool
	// 可选：用于 iSCSI target 与 initiator 认证的 CHAP secret。
	// 当 DiscoveryCHAPAuth 或 SessionCHAPAuth 任一为 true 时使用该 secret
	// +optional
	SecretRef *LocalObjectReference
	// 可选：每个卷自定义的 initiator 名称。
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name>。
	// +optional
	InitiatorName *string
}

// ISCSIPersistentVolumeSource 表示 ISCSI 磁盘。
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIPersistentVolumeSource struct {
	// Required: iSCSI 目标门户（target portal）
	// portal 是一个 IP；若端口非默认（通常为 TCP 端口 860 和 3260），则为 ip_addr:port
	// +optional
	TargetPortal string
	// Required: 目标 iSCSI Qualified Name
	// +optional
	IQN string
	// Required: iSCSI target 的 lun 编号
	// +optional
	Lun int32
	// 可选：默认为 'default'（tcp）。使用 iSCSI 传输的 iSCSI 接口名称。
	// +optional
	ISCSIInterface string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：用于高可用的 iSCSI target portal IP 列表。
	// portal 是一个 IP；若端口非默认（通常为 TCP 端口 860 和 3260），则为 ip_addr:port
	// +optional
	Portals []string
	// 可选：是否支持 iSCSI Discovery CHAP 认证
	// +optional
	DiscoveryCHAPAuth bool
	// 可选：是否支持 iSCSI Session CHAP 认证
	// +optional
	SessionCHAPAuth bool
	// 可选：用于 iSCSI target 与 initiator 认证的 CHAP secret。
	// 当 DiscoveryCHAPAuth 或 SessionCHAPAuth 任一为 true 时使用该 secret
	// +optional
	SecretRef *SecretReference
	// 可选：每个卷自定义的 initiator 名称。
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name>。
	// +optional
	InitiatorName *string
}

// FCVolumeSource 表示一个光纤通道（Fibre Channel）卷。
// Fibre Channel 卷只能以读写方式挂载一次。
// Fibre Channel 卷支持所有权管理和 SELinux 重新标记。
type FCVolumeSource struct {
	// 可选：FC target 的 worldwide names (WWNs)
	// +optional
	TargetWWNs []string
	// 可选：FC target 的 lun 编号
	// +optional
	Lun *int32
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：FC 卷的 World Wide Identifiers (WWIDs)
	// 必须设置 WWIDs，或 TargetWWNs 加 Lun，二者不可同时设置。
	// +optional
	WWIDs []string
}

// FlexPersistentVolumeSource 表示通过基于 exec 的插件供给/挂载的通用持久卷资源。
type FlexPersistentVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。默认文件系统取决于 FlexVolume 脚本。
	// +optional
	FSType string
	// 可选：SecretRef 是指向包含敏感信息的 secret 对象的引用，
	// 该信息会传递给插件脚本。若未指定 secret 对象，
	// 此项可为空。若 secret 对象
	// 包含多个 secret，所有 secret 都会传递给插件
	// 脚本。
	// +optional
	SecretRef *SecretReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：如有，额外的驱动选项。
	// +optional
	Options map[string]string
}

// FlexVolumeSource 表示通过基于 exec 的插件供给/挂载的通用卷资源。
type FlexVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。默认文件系统取决于 FlexVolume 脚本。
	// +optional
	FSType string
	// 可选：SecretRef 是指向包含敏感信息的 secret 对象的引用，
	// 该信息会传递给插件脚本。若未指定 secret 对象，
	// 此项可为空。若 secret 对象
	// 包含多个 secret，所有 secret 都会传递给插件
	// 脚本。
	// +optional
	SecretRef *LocalObjectReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：如有，额外的驱动选项。
	// +optional
	Options map[string]string
}

// AWSElasticBlockStoreVolumeSource 表示 AWS 中的持久磁盘资源。
//
// AWS EBS 磁盘必须在挂载到容器之前已存在。该磁盘还必须
// 与 kubelet 位于同一 AWS 可用区。AWS EBS 磁盘只能以读写方式挂载一次。
// AWS EBS 卷支持所有权管理和 SELinux 重标记。
type AWSElasticBlockStoreVolumeSource struct {
	// 持久磁盘资源的唯一 ID。用于在 AWS 中标识该磁盘
	VolumeID string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：要挂载的磁盘分区。
	// 若省略，kubelet 将尝试挂载设备名。
	// 例如 /dev/sda1 对应此字段为 "1"，/dev/sda 对应此字段为 0 或空。
	// +optional
	Partition int32
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// GitRepoVolumeSource 表示一个使用 Git 仓库内容填充的卷。
// Git 仓库卷不支持所有权管理。
// Git 仓库卷支持 SELinux 重新标记。
//
// DEPRECATED: GitRepo 已弃用。要为容器提供 Git 仓库，请将一个
// EmptyDir 挂载到 InitContainer 中，由后者使用 git 克隆仓库，然后将该 EmptyDir 挂载到
// Pod 的容器中。
type GitRepoVolumeSource struct {
	// 仓库 URL
	Repository string
	// 提交哈希，此项可选
	// +optional
	Revision string
	// directory 是目标目录名称。
	// 不得包含或以 '..' 开头。若提供 '.'，卷目录即为 Git 仓库本身。
	// 否则，若指定了该字段，卷将在具有给定名称的子目录中包含 Git 仓库。
	// +optional
	Directory string
	// TODO: 此处需考虑凭据。
}

// SecretVolumeSource 将一个 Secret 适配为卷。
//
// 目标 Secret 的 Data 字段内容将以文件形式呈现在卷中，
// 文件名即 Data 字段中的键。
// Secret 卷支持所有权管理和 SELinux 重标记。
type SecretVolumeSource struct {
	// 要使用的 secret 的名称，位于 Pod 的命名空间中。
	// +optional
	SecretName string
	// items 若未指定，所引用 Secret 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 Secret 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// 默认应用于所创建文件的权限位。必须是介于 0 和 0777 之间的值。
	// 路径内的目录不受此设置影响。
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	DefaultMode *int32
	// 指定 Secret 或其键是否必须存在
	// +optional
	Optional *bool
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64
}

// SecretProjection 将一个 secret 适配为 projected 卷。
//
// 目标 Secret 的 Data 字段内容将以文件形式呈现在
// projected 卷中，文件名即 Data 字段中的键。
// 注意：除 default mode 外，这与 secret 卷源完全相同。
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

// NFSVolumeSource 表示一个持续整个 Pod 生命周期的 NFS 挂载。
// NFS 卷不支持所有权管理或 SELinux 重新标记。
type NFSVolumeSource struct {
	// server 是 NFS 服务器的主机名或 IP 地址。
	Server string

	// Path 是导出的 NFS 共享路径
	Path string

	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// NFS 导出以只读权限挂载
	// +optional
	ReadOnly bool
}

// QuobyteVolumeSource 表示一个持续整个 Pod 生命周期的 Quobyte 挂载。
// Quobyte 卷不支持所有权管理或 SELinux 重新标记。
type QuobyteVolumeSource struct {
	// registry 表示一个或多个 Quobyte Registry 服务，
	// 以 host:port 形式的字符串指定（多个条目以逗号分隔），
	// 作为卷的中央注册中心。
	Registry string

	// volume 是按名称引用已创建 Quobyte 卷的字符串。
	Volume string

	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// Quobyte 以只读权限挂载
	// +optional
	ReadOnly bool

	// 卷访问映射到的用户
	// 默认为 root 用户
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

// GlusterfsVolumeSource 表示一个持续整个 Pod 生命周期的 Glusterfs 挂载。
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsVolumeSource struct {
	// Required: EndpointsName 是描述 Glusterfs 拓扑的 endpoint 名称
	EndpointsName string

	// Required: Path 是 Glusterfs 卷路径
	Path string

	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// Glusterfs 以只读权限挂载
	// +optional
	ReadOnly bool
}

// GlusterfsPersistentVolumeSource 表示一个持续整个 Pod 生命周期的 Glusterfs 挂载。
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsPersistentVolumeSource struct {
	// EndpointsName 是描述 Glusterfs 拓扑的 endpoint 名称。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	EndpointsName string

	// path 是 Glusterfs 卷路径。
	// 更多信息参见: https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	Path string

	// 此处的 ReadOnly 会强制 Glusterfs 卷以只读权限挂载。
	// 默认为 false。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	ReadOnly bool

	// EndpointsNamespace 是包含 Glusterfs endpoint 的命名空间。
	// 若此字段为空，EndpointNamespace 默认与所绑定 PVC 相同的命名空间。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	EndpointsNamespace *string
}

// RBDVolumeSource 表示一个持续整个 Pod 生命周期的 Rados 块设备挂载。
// RBD 卷支持所有权管理和 SELinux 重标记。
type RBDVolumeSource struct {
	// Required: CephMonitors 是 Ceph monitor 的集合
	CephMonitors []string
	// Required: RBDImage 是 rados 镜像名
	RBDImage string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：RadosPool 是 rados 池名，默认为 rbd
	// +optional
	RBDPool string
	// 可选：RBDUser 是 rados 用户名，默认为 admin
	// +optional
	RadosUser string
	// 可选：Keyring 是 RBDUser 的 key ring 路径，默认为 /etc/ceph/keyring
	// +optional
	Keyring string
	// 可选：SecretRef 是 RBDUser 认证 secret 的名称，默认为 nil。
	// +optional
	SecretRef *LocalObjectReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// RBDPersistentVolumeSource 表示一个持续整个 Pod 生命周期的 Rados 块设备挂载。
// RBD 卷支持所有权管理和 SELinux 重标记。
type RBDPersistentVolumeSource struct {
	// Required: CephMonitors 是 Ceph monitor 的集合
	CephMonitors []string
	// Required: RBDImage 是 rados 镜像名
	RBDImage string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string
	// 可选：RadosPool 是 rados 池名，默认为 rbd
	// +optional
	RBDPool string
	// 可选：RBDUser 是 rados 用户名，默认为 admin
	// +optional
	RadosUser string
	// 可选：Keyring 是 RBDUser 的 key ring 路径，默认为 /etc/ceph/keyring
	// +optional
	Keyring string
	// 可选：SecretRef 是指向 User 认证 secret 的引用，默认为空。
	// +optional
	SecretRef *SecretReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// CinderVolumeSource 表示 OpenStack 中的 cinder 卷资源。Cinder 卷
// 必须在挂载到容器之前已存在。该卷还必须与 kubelet
// 位于同一区域。Cinder 卷支持所有权管理
// 和 SELinux 重标记。
type CinderVolumeSource struct {
	// 用于标识 cinder 卷的唯一卷 ID。
	VolumeID string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：指向包含用于连接
	// OpenStack 的参数的 secret 对象。
	// +optional
	SecretRef *LocalObjectReference
}

// CinderPersistentVolumeSource 表示 OpenStack 中的 cinder 卷资源。Cinder 卷
// 必须在挂载到容器之前已存在。该卷还必须与 kubelet
// 位于同一区域。Cinder 卷支持所有权管理
// 和 SELinux 重标记。
type CinderPersistentVolumeSource struct {
	// 用于标识 cinder 卷的唯一卷 ID。
	VolumeID string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 可选：指向包含用于连接
	// OpenStack 的参数的 secret 对象。
	// +optional
	SecretRef *SecretReference
}

// CephFSVolumeSource 表示一个持续整个 Pod 生命周期的 Ceph 文件系统挂载。
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
	// 可选：SecretFile 是 User 的 key ring 路径，默认为 /etc/ceph/user.secret
	// +optional
	SecretFile string
	// 可选：SecretRef 是指向 User 认证 secret 的引用，默认为空。
	// +optional
	SecretRef *LocalObjectReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// SecretReference 表示一个 Secret 引用，包含在任意命名空间中检索 secret
// 所需的信息
type SecretReference struct {
	// name 在命名空间内唯一，用于引用 Secret 资源。
	// +optional
	Name string
	// namespace 定义了 Secret 名称必须唯一的作用域。
	// +optional
	Namespace string
}

// CephFSPersistentVolumeSource 表示一个持续整个 Pod 生命周期的 Ceph 文件系统挂载。
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
	// 可选：SecretFile 是 User 的 key ring 路径，默认为 /etc/ceph/user.secret
	// +optional
	SecretFile string
	// 可选：SecretRef 是指向 User 认证 secret 的引用，默认为空。
	// +optional
	SecretRef *SecretReference
	// 可选：默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// FlockerVolumeSource 表示由 Flocker agent 挂载的 Flocker 卷。
// datasetName 与 datasetUUID 必须且只能设置其中一个。
// Flocker 卷不支持所有权管理或 SELinux 重新标记。
type FlockerVolumeSource struct {
	// 以 metadata -> name 形式存储在 Flocker 数据集上的数据集名称，
	// 应视为已弃用
	// +optional
	DatasetName string
	// 数据集的 UUID。这是 Flocker 数据集的唯一标识符
	// +optional
	DatasetUUID string
}

// DownwardAPIVolumeSource 表示包含 downward API 信息的卷。
// downward API 卷支持所有权管理和 SELinux 重新标记。
type DownwardAPIVolumeSource struct {
	// Items 是 DownwardAPIVolume 文件的列表
	// +optional
	Items []DownwardAPIVolumeFile
	// 默认应用于所创建文件的权限位。必须是介于 0 和 0777 之间的值。
	// 路径内的目录不受此设置影响。
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

// DownwardAPIVolumeFile 表示包含 downward API 信息的单个文件
type DownwardAPIVolumeFile struct {
	// Required: Path 是要创建的文件的相对路径名。不得为绝对路径或包含 '..' 路径。必须采用 utf-8 编码。相对路径的第一项不得以 '..' 开头
	Path string
	// 必填：选择 Pod 的一个字段：目前仅支持 annotations、labels、name、namespace 和 uid。
	// +optional
	FieldRef *ObjectFieldSelector
	// 选择容器的一种资源：目前仅支持资源限制和请求
	// （limits.cpu、limits.memory、requests.cpu 和 requests.memory）。
	// +optional
	ResourceFieldRef *ResourceFieldSelector
	// 可选：此文件使用的权限位，取值应为 0 到 0777 之间的值。
	// 若未指定，将使用卷的 defaultMode。
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

// DownwardAPIProjection 表示要投影到 projected 卷中的 downward API 信息。
// 注意：除 default mode 外，这与 downwardAPI 卷源完全
// 相同。
type DownwardAPIProjection struct {
	// Items 是 DownwardAPIVolume 文件的列表。
	// +optional
	Items []DownwardAPIVolumeFile
}

// AzureFileVolumeSource azureFile 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
type AzureFileVolumeSource struct {
	// 包含 Azure 存储账户名与密钥的 secret 名称
	SecretName string
	// 共享名称
	ShareName string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// AzureFilePersistentVolumeSource 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
type AzureFilePersistentVolumeSource struct {
	// 包含 Azure 存储账户名与密钥的 secret 名称
	SecretName string
	// 共享名称
	ShareName string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// 包含 Azure 存储账户名与密钥的 secret 的命名空间，
	// 默认与 Pod 相同
	// +optional
	SecretNamespace *string
}

// VsphereVirtualDiskVolumeSource 表示 vSphere 卷资源。
type VsphereVirtualDiskVolumeSource struct {
	// 标识 vSphere 卷 vmdk 的路径
	VolumePath string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 存储策略管理（SPBM）配置文件名称。
	// +optional
	StoragePolicyName string
	// 与 StoragePolicyName 关联的存储策略管理（SPBM）配置文件 ID。
	// +optional
	StoragePolicyID string
}

// PhotonPersistentDiskVolumeSource 表示 Photon Controller 持久盘资源。
type PhotonPersistentDiskVolumeSource struct {
	// 标识 Photon Controller 持久盘的 ID
	PdID string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	FSType string
}

// PortworxVolumeSource 表示 Portworx 卷资源。
type PortworxVolumeSource struct {
	// VolumeID 唯一标识一个 Portworx 卷
	VolumeID string
	// FSType 表示要挂载的文件系统类型
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// AzureDataDiskCachingMode 定义 Azure 数据盘的缓存模式
type AzureDataDiskCachingMode string

// AzureDataDiskKind 定义 Azure 数据盘的种类
type AzureDataDiskKind string

// 定义 Azure 数据盘的缓存模式与种类
const (
	AzureDataDiskCachingNone      AzureDataDiskCachingMode = "None"
	AzureDataDiskCachingReadOnly  AzureDataDiskCachingMode = "ReadOnly"
	AzureDataDiskCachingReadWrite AzureDataDiskCachingMode = "ReadWrite"

	AzureSharedBlobDisk    AzureDataDiskKind = "Shared"
	AzureDedicatedBlobDisk AzureDataDiskKind = "Dedicated"
	AzureManagedDisk       AzureDataDiskKind = "Managed"
)

// AzureDiskVolumeSource 表示主机上的 Azure 数据盘挂载，并绑定挂载到 Pod。
type AzureDiskVolumeSource struct {
	// blob 存储中数据盘的名称
	DiskName string
	// blob 存储中数据盘的 URI
	DataDiskURI string
	// 主机缓存模式：None、Read Only、Read Write。
	// +optional
	CachingMode *AzureDataDiskCachingMode
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType *string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly *bool
	// kind 的预期值为 Shared（每个存储账户多个 Blob 磁盘）、Dedicated（每个存储账户单个 Blob 磁盘）、
	// Managed（Azure 托管数据磁盘，仅适用于托管可用性集）。默认值为 Shared。
	Kind *AzureDataDiskKind
}

// ScaleIOVolumeSource 表示一个持久 ScaleIO 卷
type ScaleIOVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string
	// system 是 ScaleIO 中配置的存储系统名称。
	System string
	// SecretRef 引用包含 ScaleIO 用户及其他敏感信息的
	// secret。若未提供，登录操作将失败。
	SecretRef *LocalObjectReference
	// 启用/禁用与 Gateway 的 SSL 通信的标志，默认为 false
	// +optional
	SSLEnabled bool
	// 所配置存储对应的 ScaleIO Protection Domain 名称。
	// +optional
	ProtectionDomain string
	// 与该 protection domain 关联的 ScaleIO Storage Pool。
	// +optional
	StoragePool string
	// 指示卷的存储应为 ThickProvisioned 还是 ThinProvisioned。
	// 默认为 ThinProvisioned。
	// +optional
	StorageMode string
	// ScaleIO 系统中已创建的、与此卷源关联
	// 的卷名称。
	VolumeName string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// 默认为 "xfs"。
	// +optional
	FSType string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// ScaleIOPersistentVolumeSource 表示一个持久 ScaleIO 卷，可由管理员
// 通过 storage class 等方式定义。
type ScaleIOPersistentVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string
	// system 是 ScaleIO 中配置的存储系统名称。
	System string
	// SecretRef 引用包含 ScaleIO 用户及其他敏感信息的
	// secret。若未提供，登录操作将失败。
	SecretRef *SecretReference
	// 启用/禁用与 Gateway 的 SSL 通信的标志，默认为 false
	// +optional
	SSLEnabled bool
	// 所配置存储对应的 ScaleIO Protection Domain 名称。
	// +optional
	ProtectionDomain string
	// 与该 protection domain 关联的 ScaleIO Storage Pool。
	// +optional
	StoragePool string
	// 指示卷的存储应为 ThickProvisioned 还是 ThinProvisioned。
	// 默认为 ThinProvisioned。
	// +optional
	StorageMode string
	// ScaleIO 系统中创建的、与此卷源关联
	// 的卷名称。
	VolumeName string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// 默认为 "xfs"。
	// +optional
	FSType string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
}

// StorageOSVolumeSource 表示 StorageOS 持久卷资源。
type StorageOSVolumeSource struct {
	// VolumeName 是 StorageOS 卷的人类可读名称。卷名称
	// 仅在单个命名空间内唯一。
	VolumeName string
	// VolumeNamespace 指定卷在 StorageOS 内的作用域。若未指定
	// 命名空间，则使用 Pod 的命名空间。这使得
	// Kubernetes 的名称作用域可镜像到 StorageOS 内，实现更紧密的集成。
	// 将 VolumeName 设为任意名称可覆盖默认行为。
	// 若在 StorageOS 中不使用命名空间，则设为 "default"。
	// StorageOS 中不存在的命名空间将被自动创建。
	// +optional
	VolumeNamespace string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// SecretRef 指定用于获取 StorageOS API
	// 凭据的 secret。若未指定，将尝试使用默认值。
	// +optional
	SecretRef *LocalObjectReference
}

// StorageOSPersistentVolumeSource 表示 StorageOS 持久卷资源。
type StorageOSPersistentVolumeSource struct {
	// VolumeName 是 StorageOS 卷的人类可读名称。卷名称
	// 仅在单个命名空间内唯一。
	VolumeName string
	// VolumeNamespace 指定卷在 StorageOS 内的作用域。若未指定
	// 命名空间，则使用 Pod 的命名空间。这使得
	// Kubernetes 的名称作用域可镜像到 StorageOS 内，实现更紧密的集成。
	// 将 VolumeName 设为任意名称可覆盖默认行为。
	// 若在 StorageOS 中不使用命名空间，则设为 "default"。
	// StorageOS 中不存在的命名空间将被自动创建。
	// +optional
	VolumeNamespace string
	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string
	// 默认为 false（读写）。此处的 ReadOnly 会强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool
	// SecretRef 指定用于获取 StorageOS API
	// 凭据的 secret。若未指定，将尝试使用默认值。
	// +optional
	SecretRef *ObjectReference
}

// ConfigMapVolumeSource 将一个 ConfigMap 适配为卷。
//
// 目标 ConfigMap 的 Data 字段内容将以文件形式呈现在卷中，
// 文件名即 Data 字段中的键，除非 items 元素
// 配置了键到路径的特定映射。
// ConfigMap 卷支持所有权管理和 SELinux 重标记。
type ConfigMapVolumeSource struct {
	LocalObjectReference
	// items 若未指定，所引用 ConfigMap 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 ConfigMap 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	Items []KeyToPath
	// 默认应用于所创建文件的权限位。必须是介于 0 和 0777 之间的值。
	// 路径内的目录不受此设置影响。
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

// ConfigMapProjection 将一个 ConfigMap 适配为 projected 卷。
//
// 目标 ConfigMap 的 Data 字段内容将以文件形式呈现在 projected 卷中，
// 文件名即 Data 字段中的键，除非 items 元素
// 配置了键到路径的特定映射。
// 注意：除 default mode 外，这与 configmap 卷源完全相同。
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

// ServiceAccountTokenProjection 表示 projected service account token 卷。
// 此投影可用于将 service account token 插入 Pod 的
// 运行时文件系统，供访问 API（Kubernetes API Server
// 或其他）时使用。
type ServiceAccountTokenProjection struct {
	// audience 是令牌的目标受众。令牌接收方必须使用令牌受众中指定的标识符表明自身身份，
	// 否则应拒绝该令牌。受众默认为 apiserver 的标识符。
	Audience string
	// ExpirationSeconds 是请求的 service account token 有效期时长。
	// 随着 token 临近过期，kubelet 卷插件会主动轮换
	// service account token。当 token 的存活时间超过其 TTL 的
	// 80%，或 token 已超过 24 小时时，kubelet 就会开始尝试轮换。
	// 默认为 1 小时，且必须至少为 10 分钟。
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

// ClusterTrustBundleProjection 允许 Pod 通过一个自动更新的文件
// 访问 ClusterTrustBundle 对象的
// `.spec.trustBundle` 字段。
type ClusterTrustBundleProjection struct {
	// 按对象名称选择单个 ClusterTrustBundle。与 SignerName
	// 和 LabelSelector 互斥。
	Name *string

	// 选择该签名者下所有匹配 LabelSelector 的 ClusterTrustBundles。
	// 与 Name 互斥。
	SignerName *string

	// 选择所有匹配此 LabelSelecotr 的 ClusterTrustBundles。
	// 与 Name 互斥。
	LabelSelector *metav1.LabelSelector

	// 若所选 ClusterTrustBundle 不可用，是否阻塞 Pod 启动？
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

// PodCertificateProjection 在一个合并文件中提供私钥和 X.509 证书。
type PodCertificateProjection struct {
	// Kubelet 生成的 CSR 将提交给此签名者。
	SignerName string

	// Kubelet 为 Pod 生成的密钥对的类型。
	//
	// 有效值为 "RSA3072"、"RSA4096"、"ECDSAP256"、"ECDSAP384"、
	// "ECDSAP521" 和 "ED25519"。
	KeyType string

	// maxExpirationSeconds 是证书允许的最长生命周期。
	//
	// Kubelet 将该值逐字复制到它为此投影
	// 生成的 PodCertificateRequest 中。
	//
	// 若省略，kube-apiserver 会将其设为 86400（24 小时）。kube-apiserver
	// 会拒绝短于 3600（1 小时）的值。最大允许
	// 值为 7862400（91 天）。
	//
	// 此后签名者实现可以签发生命周期*短于* MaxExpirationSeconds
	// 的任意证书，但不得短于 3600
	// 秒（1 小时）。此约束由 kube-apiserver 强制执行。
	// `kubernetes.io` 签名者绝不会签发生命周期
	// 超过 24 小时的证书。
	MaxExpirationSeconds *int32

	// 将凭据包写入投影卷中的此路径。
	// 凭据包是单个文件，包含多个 PEM 块。第一个 PEM 块是 PRIVATE KEY 块，
	// 其中包含 PKCS#8 私钥。
	// 其余块为 CERTIFICATE 块，包含签名者签发的证书链（叶子证书及所有中间证书）。
	// 使用 credentialBundlePath 可让 Pod 中的应用代码通过一次原子读取获得一致的
	// 私钥和证书链。若将它们投影到不同文件，应用代码还需额外检查叶子证书是否签发给该私钥。
	CredentialBundlePath string

	// 在投影卷中的此路径写入密钥。
	//
	// 使用 keyPath 和 certificateChainPath 时，你的应用需要检查
	// 密钥与叶子证书是否一致，因为读取文件时
	// 可能正处于轮换中途。
	KeyPath string

	// 在投影卷中的此路径写入证书链。
	CertificateChainPath string

	// userAnnotations 允许 Pod 作者向签名者实现传递额外信息。
	// Kubernetes 不会以任何方式限制或校验这些
	// 元数据。
	//
	// 这些值会被逐字复制到 Kubelet 创建的
	// PodCertificateRequest 对象的 `spec.unverifiedUserAnnotations` 字段中。
	//
	// 条目受与对象元数据注解相同的校验约束，
	// 额外要求是所有键都必须带域名前缀。对值没有
	// 限制，仅对整个字段有总体大小限制。
	//
	// 签名者应当记录其支持的键和值。签名者应当
	// 拒绝包含其无法识别的键的请求。
	UserAnnotations map[string]string

	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64
}

// ProjectedVolumeSource 表示一个 projected 卷源
type ProjectedVolumeSource struct {
	// 卷投影列表
	Sources []VolumeProjection
	// 默认应用于所创建文件的权限位。必须是介于 0 和 0777 之间的值。
	// 路径内的目录不受此设置影响。
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

// VolumeProjection 可与其他受支持的卷类型一起投影到同一卷
type VolumeProjection struct {
	// 以下所有类型都是支持投影到同一卷的类型

	// secret 是要投影的 Secret 数据的相关信息。
	Secret *SecretProjection
	// downwardAPI 是要投影的 downwardAPI 数据的相关信息。
	DownwardAPI *DownwardAPIProjection
	// 要投影的 configMap 数据的相关信息
	ConfigMap *ConfigMapProjection
	// 要投影的 serviceAccountToken 数据的相关信息
	ServiceAccountToken *ServiceAccountTokenProjection
	// 要投影的 ClusterTrustBundle 数据的相关信息
	ClusterTrustBundle *ClusterTrustBundleProjection
	// 要投影的 Pod 证书的相关信息。
	PodCertificate *PodCertificateProjection
}

// KeyToPath 将字符串键映射到卷内的一个路径。
type KeyToPath struct {
	// 要投影的键。
	Key string

	// path 是键所映射到的文件的相对路径。
	// 不能是绝对路径。
	// 不能包含路径元素 '..'。
	// 不能以字符串 '..' 开头。
	Path string
	// 可选：此文件使用的权限位，取值应为 0 到 0777 之间的值。
	// 若未指定，将使用卷的 defaultMode。
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

// LocalVolumeSource 表示带有节点亲和性的直接挂载存储
type LocalVolumeSource struct {
	// path 是节点上卷的完整路径。
	// 它可以是目录或块设备（磁盘、分区等）。
	Path string

	// 要挂载的文件系统类型。
	// 仅当 Path 是块设备时适用。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认自动选择文件系统。
	// +optional
	FSType *string
}

// CSIPersistentVolumeSource 表示由外部 CSI 卷驱动管理的存储。
type CSIPersistentVolumeSource struct {
	// driver 是要用于此卷的驱动名称。
	// 必填。
	Driver string

	// VolumeHandle 是 CSI 卷插件的 CreateVolume 返回的唯一卷名称，
	// 用于在后续所有调用中引用该卷。
	// Required（必填）。
	VolumeHandle string

	// 可选：传给 ControllerPublishVolumeRequest 的值。
	// 默认为 false（读写）。
	// +optional
	ReadOnly bool

	// 要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// +optional
	FSType string

	// 要发布的卷的属性。
	// +optional
	VolumeAttributes map[string]string

	// ControllerPublishSecretRef 是指向 secret 对象的引用，其中包含
	// 要传给 CSI 驱动以完成 CSI
	// ControllerPublishVolume 与 ControllerUnpublishVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可为空。若
	// secret 对象包含多个 secret，则全部传递。
	// +optional
	ControllerPublishSecretRef *SecretReference

	// NodeStageSecretRef 是指向 secret 对象的引用，其中包含要传给
	// CSI 驱动以完成 CSI NodeStageVolume
	// 与 NodeUnstageVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可为空。若
	// secret 对象包含多个 secret，则全部传递。
	// +optional
	NodeStageSecretRef *SecretReference

	// NodePublishSecretRef 是指向 secret 对象的引用，其中包含
	// 要传给 CSI 驱动以完成 CSI
	// NodePublishVolume 与 NodeUnpublishVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可为空。若
	// secret 对象包含多个 secret，则全部传递。
	// +optional
	NodePublishSecretRef *SecretReference

	// ControllerExpandSecretRef 是指向 secret 对象的引用，其中包含
	// 要传给 CSI 驱动以完成 CSI
	// ControllerExpandVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可为空。若
	// secret 对象包含多个 secret，则全部传递。
	// +optional
	ControllerExpandSecretRef *SecretReference

	// NodeExpandSecretRef 是指向 secret 对象的引用，其中包含
	// 要传给 CSI 驱动以完成 CSI
	// NodeExpandVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可省略。若
	// secret 对象包含多个 secret，则全部传递。
	// +optional
	NodeExpandSecretRef *SecretReference
}

// CSIVolumeSource 表示由外部 CSI 驱动管理的待挂载卷的来源位置
type CSIVolumeSource struct {
	// driver 是处理此卷的 CSI 驱动名称。
	// 请向管理员咨询集群中注册的正确名称。
	Driver string

	// 指定卷的只读配置。
	// 默认为 false（读写）。
	// +optional
	ReadOnly *bool

	// 要挂载的文件系统类型。例如 "ext4"、"xfs"、"ntfs"。
	// 若未提供，则将空值传给关联的 CSI 驱动，
	// 由其决定要应用的默认文件系统。
	// +optional
	FSType *string

	// VolumeAttributes 存储传给 CSI 驱动的驱动专用属性。
	// 受支持的取值请查阅所用驱动的文档。
	// +optional
	VolumeAttributes map[string]string

	// NodePublishSecretRef 是指向 secret 对象的引用，其中包含
	// 要传给 CSI 驱动以完成 CSI
	// NodePublishVolume 与 NodeUnpublishVolume 调用的敏感信息。
	// 此字段可选，若不需要 secret 可为空。若
	// secret 对象包含多个 secret，则传递所有 secret 引用。
	// +optional
	NodePublishSecretRef *LocalObjectReference
}

// EphemeralVolumeSource 表示由普通存储驱动处理的临时卷。
type EphemeralVolumeSource struct {
	// VolumeClaimTemplate 将用于创建一个独立的 PVC 来供给该卷。
	// 内嵌此 EphemeralVolumeSource 的 Pod 将成为
	// 该 PVC 的 owner，即 PVC 会随 Pod 一起
	// 删除。PVC 的名称将是 `<pod name>-<volume name>`，其中
	// `<volume name>` 来自 `PodSpec.Volumes` 数组
	// 条目的名称。若拼接后的名称对 PVC 而言无效（例如过长），
	// Pod 校验会拒绝该 Pod。
	//
	// 若已存在同名但不被该 Pod 所有的 PVC，
	// 该 PVC 将*不会*被 Pod 使用，以免误用
	// 无关的卷。此后 Pod 的启动将被阻塞，直到
	// 无关的 PVC 被移除。若这样预创建的 PVC
	// 确实要被 Pod 使用，则一旦 Pod 存在，必须
	// 用指向该 Pod 的 owner reference 更新 PVC。通常
	// 这并无必要，但在手工重建
	// 损坏的集群时可能有用。
	//
	// 此字段为只读，PVC 创建后 Kubernetes 不会对其做任何修改。
	//
	// Required，必须非 nil。
	VolumeClaimTemplate *PersistentVolumeClaimTemplate
}

// PersistentVolumeClaimTemplate 用于生成 PersistentVolumeClaim 对象。
type PersistentVolumeClaimTemplate struct {
	// ObjectMeta 可以包含标签和注解，创建 PVC 时会将它们复制到
	// PVC 中。不允许其他字段，
	// 校验时会被拒绝。
	// +optional
	metav1.ObjectMeta

	// PersistentVolumeClaim 的规格。全部内容会原样复制到
	// 由此模板创建的 PVC 中。PersistentVolumeClaim 中的字段
	// 在此处同样有效。
	Spec PersistentVolumeClaimSpec
}

// ContainerPort 表示单个容器中的一个网络端口
type ContainerPort struct {
	// 可选：若指定，必须是 IANA_SVC_NAME。Pod 中的每个具名端口
	// 都必须有唯一的名称。
	// +optional
	Name string
	// HostPort 可选：如果指定，必须是有效端口号，0 < x < 65536。
	// 如果指定了 HostNetwork，此值必须与 ContainerPort 一致。
	// +optional
	HostPort int32
	// Required: 这必须是有效端口号，0 < x < 65536。
	ContainerPort int32
	// Required: 支持 "TCP"、"UDP" 和 "SCTP"
	// +optional
	Protocol Protocol
	// 可选：外部端口要绑定到的主机 IP。
	// +optional
	HostIP string
}

// VolumeMount 描述将卷挂载到容器中的一个挂载点。
type VolumeMount struct {
	// Required: 必须与某个 Volume 的 Name [见上] 匹配。
	Name string
	// 可选：默认为 false（读写）。
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
	// Required。如果路径不是绝对路径（例如 some/path），
	// 则会根据操作系统为其添加相应的根前缀。
	// 在 Linux 上为 '/'，在 Windows 上为 'C:\'。
	MountPath string
	// 容器卷应挂载到的卷内路径。
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

// MountPropagationMode 描述挂载传播。
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

// VolumeDevice 描述容器内原始块设备的映射。
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
	// 可选：以下字段最多只能指定一个。
	// 可选：默认为 ""；变量引用 $(VAR_NAME) 会使用
	// 容器中先前定义的环境变量以及任何 service 环境变量进行展开。
	// 若某个变量无法解析，
	// 输入字符串中的引用将保持不变。双写 $$
	// 会缩减为单个 $，从而可对 $(VAR_NAME)
	// 语法进行转义：即 "$$(VAR_NAME)" 将产生字符串字面量
	// "$(VAR_NAME)"。已转义的引用无论变量是否存在
	// 都不会被展开。
	// +optional
	Value string
	// 可选：指定该变量值的来源。
	// +optional
	ValueFrom *EnvVarSource
}

// EnvVarSource 表示 EnvVar 值的来源。
// 只能设置其中一个字段。
type EnvVarSource struct {
	// 选择 Pod 的一个字段：支持 metadata.name、metadata.namespace、`metadata.labels['<KEY>']`、`metadata.annotations['<KEY>']`、
	// metadata.uid、spec.nodeName、spec.serviceAccountName、status.hostIP、status.podIP、status.podIPs。
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
	// 从卷内选择文件的路径。
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
	// Required: FieldPath 所依据的 schema 版本。
	// 若未指定值，将默认为外层对象的
	// APIVersion。
	APIVersion string
	// Required: 在指定 API 版本中要选择的字段路径
	FieldPath string
}

// ResourceFieldSelector 表示容器资源（cpu、memory）及其输出格式。
type ResourceFieldSelector struct {
	// 容器名称：卷场景必填，环境变量场景可选
	// +optional
	ContainerName string
	// Required: 要选择的资源
	Resource string
	// 指定暴露资源的输出格式，默认为 "1"
	// +optional
	Divisor resource.Quantity
}

// ConfigMapKeySelector 从 ConfigMap 中选择一个键。
type ConfigMapKeySelector struct {
	// 要选择的 ConfigMap。
	LocalObjectReference
	// 要选择的键。
	Key string
	// 指定 ConfigMap 或其键是否必须存在
	// +optional
	Optional *bool
}

// SecretKeySelector 选择 Secret 的一个键。
type SecretKeySelector struct {
	// 要选择 Pod 命名空间中 Secret 的名称。
	LocalObjectReference
	// 要选择的 Secret 的键。必须是有效的 Secret 键。
	Key string
	// 指定 Secret 或其键是否必须存在
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

// ConfigMapEnvSource 选择一个 ConfigMap 用于填充环境
// 变量。
//
// 目标 ConfigMap 的 Data 字段内容将作为
// 键值对表示为环境变量。
type ConfigMapEnvSource struct {
	// 要选择的 ConfigMap。
	LocalObjectReference
	// 指定 ConfigMap 是否必须存在
	// +optional
	Optional *bool
}

// SecretEnvSource 选择一个 Secret 用于填充环境变量。
//
// 目标 Secret 的 Data 字段内容将作为
// 键值对表示为环境变量。
type SecretEnvSource struct {
	// 要选择的 Secret。
	LocalObjectReference
	// 指定 Secret 是否必须存在
	// +optional
	Optional *bool
}

// HTTPHeader 描述要在 HTTP 探针中使用的自定义请求头。
type HTTPHeader struct {
	// Name 是请求头字段名。
	// 输出时会被规范化，因此大小写不同的名称会被视为同一个请求头。
	Name string
	// 请求头字段的值
	Value string
}

// HTTPProtocol 独立于 URI scheme，为 HTTP 探针选择
// 线上传输协议。
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
	// 可选：在 HTTP 服务器上访问的路径。
	// +optional
	Path string
	// Required: 要访问的容器端口的名称或编号。
	// +optional
	Port intstr.IntOrString
	// 要连接到的主机名，默认为 Pod IP。通常你可能想在 httpHeaders 中设置 "Host"。
	// +optional
	Host string
	// 可选：连接主机使用的 scheme，默认为 HTTP。
	// +optional
	Scheme URIScheme
	// 可选：在请求中设置的自定义请求头。HTTP 允许重复的请求头。
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
	// URISchemeHTTP 表示使用的 scheme 为 http://
	URISchemeHTTP URIScheme = "HTTP"
	// URISchemeHTTPS 表示使用的 scheme 为 https://
	URISchemeHTTPS URIScheme = "HTTPS"
)

// TCPSocketAction 描述基于打开 socket 的动作。
type TCPSocketAction struct {
	// Required: 要连接的端口。
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

// Probe 描述对容器执行的健康检查，用于判断容器
// 是否存活或是否就绪接收流量。
type Probe struct {
	// 用于判断容器健康状况的动作
	ProbeHandler
	// 健康检查开始前的等待时长。单位为秒。
	// +optional
	InitialDelaySeconds int32
	// 健康检查超时前的时长。单位为秒。
	// +optional
	TimeoutSeconds int32
	// 执行探针的频率（单位为秒）。
	// +optional
	PeriodSeconds int32
	// 探针在失败之后被视为成功所需的最少连续成功次数。
	// liveness 和 startup 探针必须为 1。
	// +optional
	SuccessThreshold int32
	// 探针在成功之后被视为失败所需的最少连续失败次数。
	// +optional
	FailureThreshold int32
	// 可选：探针失败后 Pod 优雅终止所需的秒数。
	// 宽限期指从向 Pod 中运行的进程发送终止信号，到进程被 kill 信号强制停止之间的秒数。
	// 请将此值设置为大于你的进程的预计清理时间。
	// 若此值为 nil，将使用 Pod 的 terminationGracePeriodSeconds；否则此
	// 值会覆盖 Pod spec 提供的值。
	// 值必须为非负整数。零值表示通过 kill 信号立即停止
	// （没有关闭机会）。
	// 这是 beta 字段，需要启用 ProbeTerminationGracePeriod 特性门控。
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
	// 'NotRequired' 表示 Kubernetes 会尽可能在不重启容器的情况下
	// 调整容器。但若无法在不重启的情况下
	// 实施调整，Kubernetes 也可能选择重启
	// 容器。例如运行时不支持免重启调整。
	NotRequired ResourceResizeRestartPolicy = "NotRequired"
	// 'RestartContainer' 表示 Kubernetes 会在应用新资源时通过停止再启动
	// 容器来就地调整容器大小。
	// 遗留应用需要此策略。例如使用
	// -xmxN 参数的 java 应用，不重启便无法使用调整后的内存。
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

// Capability 表示 POSIX capabilities 类型
type Capability string

// Capabilities 表示可以添加或移除去到运行中容器的 POSIX capabilities。
type Capabilities struct {
	// 添加的 capabilities
	// +optional
	Add []Capability
	// 移除的 capabilities
	// +optional
	Drop []Capability
}

// ResourceRequirements 描述计算资源需求。
type ResourceRequirements struct {
	// Limits 描述允许使用的计算资源上限。
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

// Container 表示预期将在主机上运行的单个容器。
type Container struct {
	// Required: 这必须是一个 DNS_LABEL。Pod 中的每个容器都必须
	// 拥有唯一的名称。
	Name string
	// Required（必填）。
	Image string
	// 可选：未提供时使用容器镜像的 entrypoint；不可更新。
	// 变量引用 $(VAR_NAME) 使用容器的环境变量展开。若某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会
	// 缩减为单个 $，从而可对 $(VAR_NAME) 语法进行转义：即 "$$(VAR_NAME)" 将
	// 产生字符串字面量 "$(VAR_NAME)"。无论变量是否存在，
	// 已转义的引用都不会被展开。
	// +optional
	Command []string
	// 可选：未提供时使用容器镜像的 cmd；不可更新。
	// 变量引用 $(VAR_NAME) 使用容器的环境变量展开。若某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会
	// 缩减为单个 $，从而可对 $(VAR_NAME) 语法进行转义：即 "$$(VAR_NAME)" 将
	// 产生字符串字面量 "$(VAR_NAME)"。无论变量是否存在，
	// 已转义的引用都不会被展开。
	// +optional
	Args []string
	// 可选：默认为容器运行时的默认工作目录。
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
	// 计算资源需求。
	// +optional
	Resources ResourceRequirements
	// 容器的资源调整策略。
	// 临时容器不能设置此字段。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	ResizePolicy []ContainerResizePolicy
	// RestartPolicy 定义 Pod 中单个容器的重启行为。
	// 它会覆盖 Pod 级别的 restart policy。未指定此字段时，
	// 重启行为由 Pod 的 restart policy 和容器类型决定。
	// 若指定了 restartPolicyRules，则容器级 restartPolicy 也是
	// 必需的，作为无规则匹配时的 "default" 策略。restartPolicyRules 的优先级
	// 高于容器级重启策略。这是 alpha 级特性，
	// 由特性门控 "ContainerRestartRules" 控制。若未启用该特性，
	// 唯一允许的值为 "Always"。
	// 此外，将 init 容器的 RestartPolicy 设为 "Always" 会产生
	// 以下效果：
	// 该 init 容器在退出后会被持续重启，
	// 直到所有普通容器终止。一旦所有普通
	// 容器完成，所有 restartPolicy 为 "Always" 的 init 容器
	// 都会被关闭。这种生命周期不同于普通 init 容器，
	// 常被称为 "sidecar" 容器。虽然该 init
	// 容器仍在 init 容器序列中启动，但它不会等待
	// 该容器完成后才继续下一个 init
	// 容器。取而代之的是，下一个 init 容器会在该
	// init 容器启动后立即启动，或在任何 startupProbe 成功
	// 完成后启动。
	// +optional
	RestartPolicy *ContainerRestartPolicy
	// 表示一组用于判断容器退出时是否应重启的
	// 规则。规则按顺序求值。一旦某条规则匹配了容器退出条件，
	// 其余规则将被忽略。若没有规则匹配容器退出条件，
	// 由容器级 restart policy 决定容器
	// 是否重启。这是 alpha 级特性，由
	// 特性门控 "ContainerRestartRules" 控制。若未启用该特性，
	// 不允许设置此字段。规则约束：
	// - 最多允许 20 条规则。
	// - 规则可以有相同的 action。
	// - 校验时不禁止完全相同的规则。
	// 指定规则时，容器必须显式设置 RestartPolicy，
	// 即使它与 Pod 的 RestartPolicy 相同。
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
	// Required（必填）。
	// +optional
	TerminationMessagePath string
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy
	// Required: 该容器拉取镜像的策略
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
	// Exec 指定要采取的动作。
	// +optional
	Exec *ExecAction
	// HTTPGet 指定要执行的 http 请求。
	// +optional
	HTTPGet *HTTPGetAction
	// TCPSocket 指定涉及 TCP 端口的动作。
	// +optional
	TCPSocket *TCPSocketAction

	// GRPC 指定涉及 GRPC 端口的动作。
	// +optional
	GRPC *GRPCAction
}

// LifecycleHandler 定义生命周期钩子中要采取的具体动作。
// 除 TCPSocket 外，必须指定且只能指定其中一个字段。
type LifecycleHandler struct {
	// Exec 指定要采取的动作。
	// +optional
	Exec *ExecAction
	// HTTPGet 指定要执行的 http 请求。
	// +optional
	HTTPGet *HTTPGetAction
	// Deprecated。TCPSocket 并不作为 LifecycleHandler 支持，保留它只是为了
	// 向后兼容。此字段不做校验，并且指定 tcp handler 时
	// 生命周期钩子在运行时会失败。
	// +optional
	TCPSocket *TCPSocketAction
	// Sleep 表示容器被终止前应休眠的时长。
	// +optional
	Sleep *SleepAction
}

type GRPCAction struct {
	// gRPC 服务的端口号。数字必须在 1 到 65535 范围内。
	Port int32

	// Service 是放入 gRPC HealthCheckRequest 中的服务名称
	// （参见 https://github.com/grpc/grpc/blob/master/doc/health-checking.md）。
	//
	// 若未指定，默认行为是探测服务器的整体健康状态。
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

// Signal 定义容器的停止信号
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
// 对于 PostStart 和 PreStop 生命周期处理器，对容器的管理会阻塞直到动作完成，
// 除非容器进程失败（此时处理器会被中止）。
type Lifecycle struct {
	// PostStart 在容器创建后立即调用。如果 handler 失败，该容器
	// 会被终止并重启。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PostStart *LifecycleHandler
	// PreStop 在容器因 API 请求或管理事件（如存活/启动探针失败、
	// 抢占、资源争用等）而即将被终止前立即调用。如果容器崩溃或退出，则不会调用此钩子。
	// Pod 的终止宽限期倒计时在 PreStop 钩子执行之前就开始。
	// 无论钩子的结果如何，容器最终都会在 Pod 的终止宽限期内终止
	// （除非被 finalizer 延迟）。对容器的其他管理会阻塞直到钩子完成
	// 或达到终止宽限期。
	// 更多信息参见: https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PreStop *LifecycleHandler
	// StopSignal 定义容器被停止时要发送的信号。
	// 如果未指定，默认值由所使用的容器运行时定义。
	// StopSignal 只能为 .spec.os.name 非空的 Pod 设置。
	// +optional
	StopSignal *Signal
}

// 以下类型由 kube_client 和 api_server 使用。

// ConditionStatus 定义资源的条件状态
type ConditionStatus string

// 这些是有效的条件状态。"ConditionTrue" 表示资源处于该条件；
// "ConditionFalse" 表示资源不处于该条件；"ConditionUnknown" 表示 Kubernetes
// 无法确定资源是否处于该条件。将来，我们可能会添加其他
// 中间状态，例如 ConditionDegraded。
const (
	ConditionTrue    ConditionStatus = "True"
	ConditionFalse   ConditionStatus = "False"
	ConditionUnknown ConditionStatus = "Unknown"
)

// ContainerStateWaiting 表示容器的等待状态
type ContainerStateWaiting struct {
	// 简短的 CamelCase 字符串，说明容器处于等待状态的原因。
	// +optional
	Reason string
	// 人类可读的消息，详细说明容器处于等待状态的原因。
	// +optional
	Message string
}

// ContainerStateRunning 表示容器的运行状态
type ContainerStateRunning struct {
	// +optional
	StartedAt metav1.Time
}

// ContainerStateTerminated 表示容器的终止状态
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
	// 更多信息参见: https://kubernetes.io/docs/concepts/containers/images.
	Image string
	// ImageID 是容器镜像的镜像 ID。该镜像 ID 可能与
	// PodSpec 中使用的镜像的 ID 不同，因为它可能已被运行时解析。
	ImageID string
	// ContainerID 是格式为 '<type>://<container_id>' 的容器 ID。
	// 其中 type 是容器运行时标识符，由 CRI API 的 Version 调用返回
	// （例如 "containerd"）。
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
	// 卷挂载的状态。
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
	// StopSignal 是停止此容器时发送给它的信号。可能是用户指定的
	// 停止信号、容器镜像中指定的信号，或本节点容器运行时的
	// 默认停止信号。
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

	// 允许将来用整体健康字段或 Device Plugin 版本之类的信息扩展此结构体
}

// ResourceID 根据此资源健康信息的来源计算得出。
// 对于 DevicePlugin：
//
//	DeviceID：device plugin 标识设备的方式。同样的 DeviceID 可在 PodResources API 中找到。
//
// DevicePlugin ID 通常在节点的生命周期内保持不变，通常可用于唯一标识节点上的设备。
//
// 对于 DRA：
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
// 这是 KEP https://kep.k8s.io/4680 的一部分。
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
	// Message 为 Health 提供人类可读的上下文（例如 "ECC error count exceeded threshold"）。
	// 当启用 ResourceHealthStatusMessage 时，此字段由 kubelet 填充。
	Message *string
}

// ContainerUser 表示用户身份信息
type ContainerUser struct {
	// Linux 保存最初附加到 Linux 容器中容器第一个进程的用户身份信息。
	// 请注意，如果进程有足够权限，实际运行时的身份可以更改。
	// +optional
	Linux *LinuxContainerUser

	// Windows 保存 Windows Pod 中容器第一个进程的用户身份信息。
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
	// PodUnknown 表示由于某些原因无法获取 Pod 的状态，通常是因为
	// 与 Pod 所在主机通信出错。
	// Deprecated in v1.21: 自 2015 年起就未被设置 (74da3b14b0c0f658b3bb8d2def5094686d0e9095)
	PodUnknown PodPhase = "Unknown"
)

// PodConditionType 定义 Pod 的条件类型
type PodConditionType string

// 以下是 Pod 的有效条件。
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

// PodCondition 表示 Pod 的条件
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

// RestartPolicy 描述容器应如何被重启。
// 以下重启策略只能指定一个。
// 若均未指定，默认策略
// 为 RestartPolicyAlways。
type RestartPolicy string

// 以下是有效的重启策略
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

	// 表示容器退出时要检查的退出码。
	// +optional
	ExitCodes *ContainerRestartRuleOnExitCodes
}

// ContainerRestartRuleAction 描述容器退出时要采取的动作。
type ContainerRestartRuleAction string

// 以下是有效的重启规则动作。
const (
	// 规则匹配时容器将被重启。仅对普通 init 容器和
	// 普通容器有效。对 sidecar 容器和临时容器无效。
	ContainerRestartRuleActionRestart ContainerRestartRuleAction = "Restart"
	// Pod 内的所有容器（临时容器除外）将被终止并重启。
	// 对普通 init 容器、sidecar 容器和普通容器有效。对
	// 临时容器无效。
	ContainerRestartRuleActionRestartAllContainers ContainerRestartRuleAction = "RestartAllContainers"
)

// ContainerRestartRuleOnExitCodes 描述基于容器退出码
// 处理已退出容器的条件。
type ContainerRestartRuleOnExitCodes struct {
	// 表示容器退出码与指定值之间的关系。可能的值为：
	// - In：如果容器退出码在指定值集合中，则满足条件。
	// - NotIn：如果容器退出码不在指定值集合中，则满足条件。
	// +required
	Operator ContainerRestartRuleOnExitCodesOperator

	// 指定要检查的容器退出码取值集合。
	// 最多允许 255 个元素。
	// +optional
	// +listType=set
	Values []int32
}

// ContainerRestartRuleOnExitCodesOperator 描述针对退出码
// 使用的操作符。
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

// NodeSelector 表示对一个或多个节点集合执行标签查询所得结果的并集；
// 也就是说，它表示 node selector terms 所代表的
// 选择器的 OR。
type NodeSelector struct {
	// Required。node selector terms 列表。各项之间是 OR 关系。
	NodeSelectorTerms []NodeSelectorTerm
}

// NodeSelectorTerm 表示选择节点所需的表达式和字段。
// 为 null 或空的 node selector term 不匹配任何对象。其各项要求
// 之间是 AND 关系。
// TopologySelectorTerm 类型实现了 NodeSelectorTerm 的一个子集。
type NodeSelectorTerm struct {
	// 基于节点标签的 node selector requirements 列表。
	MatchExpressions []NodeSelectorRequirement
	// 基于节点字段的 node selector requirements 列表。
	MatchFields []NodeSelectorRequirement
}

// NodeSelectorRequirement 是一个包含 values、key 以及一个
// 关联 key 与 values 的操作符的选择器。
type NodeSelectorRequirement struct {
	// 该选择器所应用的标签键。
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

// NodeSelectorOperator 是 node selector requirement 中
// 可以使用的操作符集合。
type NodeSelectorOperator string

// 以下是 NodeSelectorOperator 的有效取值
const (
	NodeSelectorOpIn           NodeSelectorOperator = "In"
	NodeSelectorOpNotIn        NodeSelectorOperator = "NotIn"
	NodeSelectorOpExists       NodeSelectorOperator = "Exists"
	NodeSelectorOpDoesNotExist NodeSelectorOperator = "DoesNotExist"
	NodeSelectorOpGt           NodeSelectorOperator = "Gt"
	NodeSelectorOpLt           NodeSelectorOperator = "Lt"
)

// TopologySelectorTerm 表示标签查询的结果。
// 为 null 或空的拓扑选择器项不匹配任何对象。
// 其中的要求为 AND 关系。
// 它提供 NodeSelectorTerm 功能的子集。
// 这是一个 alpha 特性，将来可能会改变。
type TopologySelectorTerm struct {
	// 基于标签的拓扑选择器要求列表。
	// +optional
	MatchLabelExpressions []TopologySelectorLabelRequirement
}

// TopologySelectorLabelRequirement 是一个匹配给定标签的选择器。
// 这是一个 alpha 特性，将来可能会改变。
type TopologySelectorLabelRequirement struct {
	// 该选择器所应用的标签键。
	Key string
	// 字符串值数组。必须有一个值匹配所选标签。
	// Values 中的每个条目是 OR 关系。
	Values []string
}

// Affinity 是一组亲和性调度规则。
type Affinity struct {
	// 描述 Pod 的节点亲和性调度规则。
	// +optional
	NodeAffinity *NodeAffinity
	// 描述 Pod 亲和性调度规则（例如将此 Pod 与其他某些 Pod 共置在同一节点、可用区等）。
	// +optional
	PodAffinity *PodAffinity
	// 描述 Pod 反亲和性调度规则（例如避免将此 Pod 与其他某些 Pod 放在同一节点、可用区等）。
	// +optional
	PodAntiAffinity *PodAntiAffinity
}

// PodAffinity 是一组 Pod 间亲和性调度规则。
type PodAffinity struct {
	// NOT YET IMPLEMENTED. TODO: 实现后取消该字段的注释。
	// 若调度时未满足此字段指定的亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的亲和性要求在 Pod 运行期间的
	// 某个时刻不再满足（例如由于 Pod 标签更新），
	// 系统将尝试最终把 Pod 从其节点上驱逐。
	// 存在多个元素时，每个 podAffinityTerm 对应的节点列表
	// 取交集，即所有项都必须满足。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm

	// 若调度时未满足此字段指定的亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的亲和性要求在 Pod 运行期间的某个时刻
	// 不再满足（例如由于 Pod 标签更新），系统可能尝试、也可能不尝试
	// 最终把 Pod 从其节点上驱逐。
	// 存在多个元素时，每个 podAffinityTerm 对应的节点列表
	// 取交集，即所有项都必须满足。
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm
	// 调度器会优先将 Pod 调度到满足此字段指定
	// 亲和性表达式的节点，但它也可能选择
	// 违反一个或多个表达式的节点。最优选的节点
	// 是权重总和最大的节点，即对于满足所有调度要求
	// （资源请求、requiredDuringScheduling 亲和性表达式等）的每个节点，
	// 遍历此字段的元素求和，若节点上存在匹配相应
	// podAffinityTerm 的 Pod，则把 "weight" 加入总和；总和最高的
	// 节点为最优选。
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm
}

// PodAntiAffinity 是一组 Pod 间反亲和性调度规则。
type PodAntiAffinity struct {
	// NOT YET IMPLEMENTED. TODO: 实现后取消该字段的注释。
	// 若调度时未满足此字段指定的反亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的反亲和性要求在 Pod 运行期间的
	// 某个时刻不再满足（例如由于 Pod 标签更新），
	// 系统将尝试最终把 Pod 从其节点上驱逐。
	// 存在多个元素时，每个 podAffinityTerm 对应的节点列表
	// 取交集，即所有项都必须满足。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm

	// 若调度时未满足此字段指定的反亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的反亲和性要求在 Pod 运行期间的某个时刻
	// 不再满足（例如由于 Pod 标签更新），系统可能尝试、也可能不尝试
	// 最终把 Pod 从其节点上驱逐。
	// 存在多个元素时，每个 podAffinityTerm 对应的节点列表
	// 取交集，即所有项都必须满足。
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm
	// 调度器会优先将 Pod 调度到满足此字段指定
	// 反亲和性表达式的节点，但它也可能选择
	// 违反一个或多个表达式的节点。最优选的节点
	// 是权重总和最大的节点，即对于满足所有调度要求
	// （资源请求、requiredDuringScheduling 反亲和性表达式等）的每个节点，
	// 遍历此字段的元素求和，若节点上存在匹配相应
	// podAffinityTerm 的 Pod，则从总和中减去 "weight"；总和最高的
	// 节点为最优选。
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm
}

// WeightedPodAffinityTerm 表示：为找到最优节点，每个节点上所有匹配的
// WeightedPodAffinityTerm 字段的权重会被累加
type WeightedPodAffinityTerm struct {
	// 与匹配相应 podAffinityTerm 关联的权重，范围为 1-100。
	Weight int32
	// 必需。与相应权重关联的 Pod 亲和性项。
	PodAffinityTerm PodAffinityTerm
}

// PodAffinityTerm 定义一组 Pod（即在给定命名空间范围内匹配 labelSelector
// 的那些 Pod），本 Pod 应与它们共置（亲和性）
// 或不共置（反亲和性）；
// 共置定义为：运行在某个节点上，该节点键为 <topologyKey>
// 的标签值，与该 Pod 集合中任一 Pod 所在节点的
// 对应标签值匹配。
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

// NodeAffinity 是一组节点亲和性调度规则。
type NodeAffinity struct {
	// NOT YET IMPLEMENTED. TODO: 实现后取消该字段的注释。
	// 若调度时未满足此字段指定的亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的亲和性要求在 Pod 运行期间的某个时刻不再
	// 满足（例如由于某次更新），系统将
	// 尝试最终把 Pod 从其节点上驱逐。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution *NodeSelector

	// 若调度时未满足此字段指定的亲和性要求，
	// Pod 将不会被调度到该节点。
	// 若此字段指定的亲和性要求在 Pod 运行期间的某个时刻不再
	// 满足（例如由于某次更新），系统可能尝试、也可能不尝试
	// 最终把 Pod 从其节点上驱逐。
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution *NodeSelector
	// 调度器会优先将 Pod 调度到满足此字段指定
	// 亲和性表达式的节点，但它也可能选择
	// 违反一个或多个表达式的节点。最优选的节点
	// 是权重总和最大的节点，即对于满足所有调度要求
	// （资源请求、requiredDuringScheduling 亲和性表达式等）的每个节点，
	// 遍历此字段的元素求和，若节点匹配相应的 matchExpressions，
	// 则把 "weight" 加入总和；总和最高的
	// 节点为最优选。
	// +optional
	PreferredDuringSchedulingIgnoredDuringExecution []PreferredSchedulingTerm
}

// PreferredSchedulingTerm 表示：空的优先调度项以隐式权重 0 匹配所有对象，
// 为 null 时不匹配任何对象（同样是无操作）。
type PreferredSchedulingTerm struct {
	// 与匹配相应 nodeSelectorTerm 关联的权重，范围为 1-100。
	Weight int32
	// 与相应权重关联的 node selector term。
	Preference NodeSelectorTerm
}

// Taint 表示可应用于节点的污点。
// 附加此 Taint 的节点会对不容忍该 Taint 的
// 任何 Pod 产生该 "effect"。
type Taint struct {
	// Required。要应用于节点的 taint 键。
	Key string
	// Required。与 taint 键对应的 taint 值。
	// +optional
	Value string
	// 必需。此污点对不容忍它的 Pod 的影响。
	// 有效的影响有 NoSchedule、PreferNoSchedule 和 NoExecute。
	Effect TaintEffect
	// TimeAdded 表示添加此污点的时间。
	// +optional
	TimeAdded *metav1.Time
}

// TaintEffect 定义 Taint 的影响
type TaintEffect string

// 以下是 TaintEffect 的有效取值
const (
	// 不允许新的 Pod 调度到该节点上，除非它们容忍此污点，
	// 但允许所有未经过调度器而直接提交给 Kubelet 的 Pod 启动，
	// 并允许所有已在运行的 Pod 继续运行。
	// 由调度器强制执行。
	TaintEffectNoSchedule TaintEffect = "NoSchedule"
	// 与 TaintEffectNoSchedule 类似，但调度器会尽量不将新 Pod 调度到该节点上，
	// 而不是完全禁止新 Pod 调度到该节点。由调度器强制执行。
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule"
	// NOT YET IMPLEMENTED. TODO: 实现后取消该字段的注释。
	// 与 TaintEffectNoSchedule 类似，但不允许未经调度器直接提交给
	// Kubelet 的 Pod 启动。
	// 由 Kubelet 和调度器强制执行。
	// TaintEffectNoScheduleNoAdmit TaintEffect = "NoScheduleNoAdmit"

	// 驱逐所有不容忍此污点的、已在运行的 Pod。
	// 当前由 NodeController 强制执行。
	TaintEffectNoExecute TaintEffect = "NoExecute"
)

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

// TolerationOperator 是 toleration 中可以使用的操作符集合。
type TolerationOperator string

// 以下是 TolerationOperator 的有效取值
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

// PodSpec 是 Pod 的描述
type PodSpec struct {
	Volumes []Volume
	// 属于该 Pod 的初始化容器列表。
	InitContainers []Container
	// 属于该 Pod 的容器列表。
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
	// 相对于 StartTime，Pod 可在节点上处于活动状态、直到系统
	// 主动尝试终止 Pod 的可选秒数；值必须为正整数
	// +optional
	ActiveDeadlineSeconds *int64
	// 设置 Pod 的 DNS 策略。
	// 默认为 "ClusterFirst"。
	// 有效值为 'ClusterFirstWithHostNet'、'ClusterFirst'、'Default' 或 'None'。
	// DNSConfig 中给定的 DNS 参数将与 DNSPolicy 选择的策略合并。
	// 要与 hostNetwork 一起设置 DNS 选项，必须显式将 DNS 策略指定为 'ClusterFirstWithHostNet'。
	// +optional
	DNSPolicy DNSPolicy
	// NodeSelector 是一个选择器，Pod 必须满足它才能适配某个节点
	// +optional
	NodeSelector map[string]string

	// ServiceAccountName 是运行此 Pod 所使用的 ServiceAccount 的名称。
	// 该 Pod 将被允许使用 ServiceAccount 引用的 secret
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
	// 使用宿主机的网络命名空间。若设置此选项，必须指定
	// 将要使用的端口。
	// 可选：默认为 false
	// +optional
	HostNetwork bool
	// 使用宿主机的 PID 命名空间。
	// 可选：默认为 false。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	HostPID bool
	// 使用宿主机的 IPC 命名空间。
	// 可选：默认为 false。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	HostIPC bool
	// 在 Pod 的所有容器之间共享单个进程命名空间。
	// 设置后，容器将能够查看同一 Pod 中其他容器的进程并向其发送信号，
	// 且每个容器的第一个进程不会被分配 PID 1。
	// HostPID 与 ShareProcessNamespace 不能同时设置。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// 可选：默认为 false。
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
	// 若指定，则为 Pod 的调度约束
	// +optional
	Affinity *Affinity
	// 若指定，Pod 将由指定的调度器分派。
	// 如果未指定，Pod 将由默认调度器分派。
	// +optional
	SchedulerName string
	// 若指定，则为 Pod 的容忍（tolerations）。
	// +optional
	Tolerations []Toleration
	// HostAliases 是可选的主机名与 IP 列表，指定后会被注入到 Pod 的 hosts
	// 文件中。
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
	// 若指定，将针对 Pod 就绪状态评估所有 readiness gates。
	// 当 Pod 的所有容器都就绪，并且 readiness gates 中指定的
	// 所有条件的状态都等于 "True" 时，Pod 才算就绪。
	// 更多信息：https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates
	// +optional
	ReadinessGates []PodReadinessGate
	// RuntimeClassName 引用 node.k8s.io 组中的 RuntimeClass 对象，应用于运行此 Pod。
	// 如果没有与指定名称匹配的 RuntimeClass 资源，则不会运行该 Pod。
	// 如果未设置或为空，将使用 "legacy" RuntimeClass，这是一个隐式类，
	// 其定义为空，使用默认的运行时处理程序。
	// 更多信息参见: https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class
	// +optional
	RuntimeClassName *string
	// EnableServiceLinks 表示是否应将有关服务的信息注入到 Pod 的环境变量中，
	// 其语法与 Docker links 匹配。
	// 如果未指定，默认为 true。
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
	// 更多信息参见: https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead
	// +optional
	Overhead ResourceList
	// TopologySpreadConstraints 描述一组 Pod 应如何在拓扑域之间
	// 分布。调度器将以遵守这些约束的方式调度 Pod。
	// 所有 topologySpreadConstraints 之间是 AND 关系。
	// +optional
	TopologySpreadConstraints []TopologySpreadConstraint
	// 如果为 true，Pod 的主机名将配置为 Pod 的 FQDN，而不是叶子名称（默认）。
	// 在 Linux 容器中，这意味着在内核的 hostname 字段（struct utsname 的 nodename 字段）中设置 FQDN。
	// 在 Windows 容器中，这意味着将注册表键 HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters 下的 hostname 注册表值设置为 FQDN。
	// 如果 Pod 没有 FQDN，则此设置无效。
	// 默认为 false。
	// +optional
	SetHostnameAsFQDN *bool
	// 指定 Pod 中容器的操作系统。
	// 设置此字段后，部分 Pod 和容器字段会受限。
	//
	// 若 OS 字段设为 linux，以下字段必须不设置：
	// - securityContext.windowsOptions
	//
	// 若 OS 字段设为 windows，以下字段必须不设置：
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

	// 使用宿主机的 user namespace。
	// 可选：默认为 true。
	// 若设为 true 或未提供，Pod 将在宿主机 user namespace 中运行，适用于
	// Pod 需要仅宿主机 user namespace 才具备的功能的场景，例如
	// 使用 CAP_SYS_MODULE 加载内核模块。
	// 设为 false 时会为 Pod 创建新的 user namespace。设为 false 有助于
	// 缓解容器逃逸漏洞，即使用户以 root 运行
	// 容器，实际上也没有宿主机的 root 权限。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	HostUsers *bool

	// SchedulingGates 是一个不透明的列表，如果指定将阻止 Pod 被调度。
	// 如果 schedulingGates 非空，Pod 将保持在 SchedulingGated 状态，
	// 调度器不会尝试调度该 Pod。
	// SchedulingGates 只能在 Pod 创建时设置，之后只能删除。
	// +optional
	SchedulingGates []PodSchedulingGate
	// ResourceClaims 定义在 Pod 获准启动之前必须完成分配
	// 和预留的 ResourceClaims。这些资源
	// 将按名称提供给使用它们的容器。
	//
	// 这是 stable 字段，但需要启用
	// DynamicResourceAllocation 特性门控。
	//
	// 此字段不可变。
	//
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaims []PodResourceClaim
	// Resources 是 Pod 内所有容器所需 CPU 和内存资源的总量。
	// 它仅支持为 "cpu"、"memory" 和 "hugepages-" 资源名称指定 Requests 和 Limits。
	// 不支持 ResourceClaims。
	//
	// 此字段可对整个 Pod 的资源分配进行细粒度控制，允许 Pod 内容器之间共享资源。
	// TODO: 进入 beta 阶段时，请扩展此注释以提供更详细的说明。
	//
	// 此为 alpha 字段，需要启用 PodLevelResources 特性门控。
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
	// SchedulingGroup 提供对本 Pod 所属的直接调度运行时
	// 分组对象的引用。
	// 调度器使用此字段识别分组并应用正确的
	// 分组调度策略。与分组的关联还会影响 Pod 的其他生命周期方面，
	// 这些方面在更广泛的调度上下文中同样相关，
	// 如抢占、资源附着等。若未指定，
	// Pod 在所有这些方面都被当作单独单元处理。
	// 此字段引用的分组对象在 Pod 创建时
	// 可能尚不存在。
	// 此字段不可变，但同名分组对象可能以不同策略
	// 被重建。在 Pod 调度期间这样做
	// 可能导致放置结果不符合预期策略。
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

// PodResourceClaim 通过 ClaimSource 恰好引用一个 ResourceClaim。
// 它会添加一个名称，在 Pod 内唯一标识该 ResourceClaim。
// 需要访问该 ResourceClaim 的容器使用此名称引用它。
//
// 当 DRAWorkloadResourceClaims 特性门控启用且此 Pod 属于某个 PodGroup 时，
// 若所有字段（Name、ResourceClaimName 和 ResourceClaimTemplateName）都相等，
// 则 PodResourceClaim 会与相应的 PodGroupResourceClaim 匹配。匹配到的声明引用
// 一个在 PodGroup 所有 Pod 之间共享的 ResourceClaim，该 ResourceClaim 在
// ResourceClaimStatus.ReservedFor 中为 PodGroup 预留，而非为单个 Pod 预留。
type PodResourceClaim struct {
	// Name 在该 Pod 内唯一标识此资源声明。
	// 必须是 DNS_LABEL。
	Name string

	// ResourceClaimName 是与该 Pod 处于同一命名空间中的 ResourceClaim 对象的名称。
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其中一个。
	ResourceClaimName *string

	// ResourceClaimTemplateName 是与本 Pod 同一命名空间中
	// ResourceClaimTemplate 对象的名称。
	//
	// 该模板将用于创建新的 ResourceClaim，并绑定到
	// 本 Pod。当本 Pod 被删除时，该 ResourceClaim
	// 也会被删除。Pod 名称与资源名称连同
	// 生成部件将组成 ResourceClaim 的唯一名称，
	// 并记录在 pod.status.resourceClaimStatuses 中。
	//
	// 当启用 DRAWorkloadResourceClaims 特性门控，且该 Pod 所属的
	// PodGroup 定义了具有相同 Name 和 ResourceClaimTemplateName 的
	// PodGroupResourceClaim 时，此 PodResourceClaim 解析为
	// 为该 PodGroup 生成的 ResourceClaim。组内所有定义了与之等价、
	// 匹配 PodGroupResourceClaim 的 Name 和
	// ResourceClaimTemplateName 的 PodResourceClaim 的 Pod，
	// 共享同一个生成的 ResourceClaim。为 PodGroup 生成的
	// ResourceClaims 归 PodGroup 所有，其生命周期与 PodGroup
	// 绑定，而非与任何单个 Pod 绑定。
	//
	// 此字段不可变，且在创建 ResourceClaim 之后，
	// 控制平面不会对相应的 ResourceClaim
	// 做任何更改。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须
	// 且只能设置其中一个。
	ResourceClaimTemplateName *string
}

// PodResourceClaimStatus 为 PodStatus 中每个引用 ResourceClaimTemplate
// 的 PodResourceClaim 而存储，记录对应
// ResourceClaim 的生成名称。
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

// PodExtendedResourceClaimStatus 存储于 PodStatus 中，对应由 DRA
// 支撑的扩展资源请求，记录调度器创建的
// 相应特殊 ResourceClaim 的生成名称。
type PodExtendedResourceClaimStatus struct {
	// RequestMapping 标识 <容器、由 DRA 支撑的扩展资源> 到生成的
	// ResourceClaim 中设备请求的映射。
	RequestMappings []ContainerExtendedResourceRequest

	// ResourceClaimName 是在 Pod 所在命名空间中为该 Pod 生成的 ResourceClaim 的名称。
	ResourceClaimName string
}

type ContainerExtendedResourceRequest struct {
	// 请求资源的容器名称。
	ContainerName string
	// 该容器中由 DRA 支撑的扩展资源名称。
	ResourceName string
	// 特殊 ResourceClaim 中与该扩展资源对应的请求名称。
	RequestName string
}

// OSName 是可在 OS 中使用的操作系统类型集合。
type OSName string

// 以下是 OSName 的有效取值
const (
	Linux   OSName = "linux"
	Windows OSName = "windows"
)

// PodOS 定义 Pod 的操作系统参数。
type PodOS struct {
	// Name 是操作系统名称。当前支持的值为 linux 和 windows。
	// 未来可能定义其他值，示例参见：
	// https://github.com/opencontainers/runtime-spec/blob/master/config.md#platform-specific-configuration
	// 客户端应能处理其他取值，并将此字段中无法识别的值视为 os: null。
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

// HostAlias 保存 IP 与主机名之间的映射，将作为条目注入
// Pod 的 hosts 文件。
type HostAlias struct {
	IP        string
	Hostnames []string
}

// Sysctl 定义要设置的内核参数。
type Sysctl struct {
	// 要设置的属性的名称
	Name string
	// 要设置的属性的值
	Value string
}

// PodFSGroupChangePolicy 保存卷挂载时对卷应用 fsGroup 所采用的策略。
type PodFSGroupChangePolicy string

const (
	// FSGroupChangeOnRootMismatch 表示仅当卷根目录的权限和所有权与卷上预期的权限不匹配时，
	// 才更改卷的所有权和权限。这有助于缩短更改卷所有权和权限所需的时间。
	FSGroupChangeOnRootMismatch PodFSGroupChangePolicy = "OnRootMismatch"
	// FSGroupChangeAlways 表示每当卷在 Pod 内挂载时，都应更改其所有权和权限。
	// 这是默认行为。
	FSGroupChangeAlways PodFSGroupChangePolicy = "Always"
)

// SupplementalGroupsPolicy 定义首个容器进程的附加组
// （supplemental groups）如何计算。
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
	// 由容器运行时对 Pod 的所有卷进行递归重新打标签。
	// 对大容量卷这可能较慢，但允许在同一节点上共享同一卷的特权与非特权 Pod 混用。
	SELinuxChangePolicyRecursive PodSELinuxChangePolicy = "Recursive"
	// MountOption 使用 `-o context` 挂载选项挂载所有符合条件的 Pod 卷。
	// 这要求共享同一卷的所有 Pod 使用相同的 SELinux 标签。
	// 特权 Pod 与非特权 Pod 之间无法共享同一卷。
	// 符合条件的卷包括树内 FibreChannel 和 iSCSI 卷，以及所有在其 CSIDriver 实例中
	// 通过设置 spec.seLinuxMount: true 声明支持 SELinux 的 CSI 卷。
	// 其他卷始终递归重新打标签。
	SELinuxChangePolicyMountOption PodSELinuxChangePolicy = "MountOption"
)

// PodSecurityContext 保存 Pod 级别的安全属性和通用容器设置。
// 部分字段也存在于 container.securityContext 中。
// container.securityContext 的字段值优先于 PodSecurityContext 的字段值。
type PodSecurityContext struct {
	// 要应用于所有容器的 SELinux 上下文。
	// 未指定时，容器运行时将为每个容器分配随机的
	// SELinux 上下文。也可在 SecurityContext 中设置。若在
	// SecurityContext 和 PodSecurityContext 中都设置，则该容器以 SecurityContext
	// 中指定的值为准。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SELinuxOptions *SELinuxOptions
	// 应用于所有容器的 Windows 专用设置。
	// 未指定时，将使用容器自身 SecurityContext 中的选项。
	// 若在 SecurityContext 和 PodSecurityContext 中都设置，则以 SecurityContext 中指定的值为准。
	// 注意：spec.os.name 为 linux 时不能设置此字段。
	// +optional
	WindowsOptions *WindowsSecurityContextOptions
	// 运行容器进程入口点的 UID。
	// 未指定时默认为镜像元数据中指定的用户。
	// 也可在 SecurityContext 中设置。若在 SecurityContext 和
	// PodSecurityContext 中都设置，则该容器以 SecurityContext
	// 中指定的值为准。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsUser *int64
	// 运行容器进程入口点的 GID。
	// 未设置时使用运行时默认值。
	// 也可在 SecurityContext 中设置。若在 SecurityContext 和
	// PodSecurityContext 中都设置，则该容器以 SecurityContext
	// 中指定的值为准。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsGroup *int64
	// 表示容器必须以非 root 用户运行。
	// 若为 true，Kubelet 会在运行时校验镜像，确保其不以
	// UID 0（root）运行，若以 root 运行则容器启动失败。
	// 未设置或为 false 时不执行此类校验。
	// 也可在 SecurityContext 中设置。若在 SecurityContext 和
	// PodSecurityContext 中都设置，则该容器以 SecurityContext
	// 中指定的值为准。
	// +optional
	RunAsNonRoot *bool
	// 应用于每个容器中首个运行进程的组列表，在此之外
	// 还包括容器的主 GID 和 fsGroup（若指定）。若启用了
	// SupplementalGroupsPolicy 特性，则由
	// supplementalGroupsPolicy 字段决定这些组是叠加在
	// 容器镜像定义的组成员之上，还是取而代之。
	// 未指定时不添加任何附加组，但根据
	// supplementalGroupsPolicy 字段，仍可能使用容器镜像中定义的组成员。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SupplementalGroups []int64
	// 定义首个容器进程的附加组（supplemental groups）如何计算。
	// 有效值为 "Merge" 和 "Strict"。若未指定，使用 "Merge"。
	// （Alpha）使用此字段需要启用 SupplementalGroupsPolicy 特性门控，
	// 且容器运行时必须实现对该特性的支持。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// TODO: 在 v1.34 中，当 spec.os.name 不是 windows 时，将默认值更新为 "Merge"。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	SupplementalGroupsPolicy *SupplementalGroupsPolicy
	// 应用于 Pod 内所有容器的特殊附加组。
	// 某些卷类型允许 Kubelet 将该卷的所有者
	// 更改为 Pod 所属：
	//
	// 1. 所有者 GID 将是 FSGroup
	// 2. 设置 setgid 位（卷内新创建的文件将归 FSGroup 所有）
	// 3. 权限位与 rw-rw---- 进行 OR 运算
	//
	// 未设置时，Kubelet 不会修改任何卷的所有权和权限。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	FSGroup *int64
	// Sysctls 保存该 Pod 使用的命名空间级 sysctl 列表。
	// 如果 Pod 使用了容器运行时不支持的 sysctl，可能无法启动。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Sysctls []Sysctl
	// fsGroupChangePolicy 定义卷在 Pod 内暴露之前更改其所有权和权限的行为。
	// 此字段仅适用于支持基于 fsGroup 的所有权（及权限）
	// 的卷类型。
	// 对 secret、configmaps 和 emptydir 等临时卷类型
	// 无效。
	// 有效值为 "OnRootMismatch" 和 "Always"。未指定时使用 "Always"。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	FSGroupChangePolicy *PodFSGroupChangePolicy
	// 本 Pod 中容器使用的 seccomp 选项。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SeccompProfile *SeccompProfile
	// appArmorProfile 是本 Pod 中容器使用的 AppArmor 选项。
	// 注意：spec.os.name 为 windows 时不能设置此字段。
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
	// 加载在节点静态文件中定义的 Profile。
	// 该配置必须预先在节点上配置好才能生效。
	// LocalhostProfile 不能是绝对路径或下行（descending）路径。
	// +optional
	LocalhostProfile *string
}

// SeccompProfileType 定义支持的 seccomp 配置类型。
type SeccompProfileType string

const (
	// SeccompProfileTypeUnconfined 表示不应用任何 seccomp 配置（即 unconfined）。
	SeccompProfileTypeUnconfined SeccompProfileType = "Unconfined"
	// SeccompProfileTypeRuntimeDefault 表示容器运行时的默认 seccomp 配置。
	SeccompProfileTypeRuntimeDefault SeccompProfileType = "RuntimeDefault"
	// SeccompProfileTypeLocalhost 表示存放在节点磁盘上的自定义配置。
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

	// localhostProfile 指示应使用节点上已加载的配置。
	// 该配置必须预先在节点上配置好才能生效。
	// 必须与已加载配置的名称匹配。
	// 当且仅当 type 为 "Localhost" 时必须设置。
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

// 以下是 PodQOSClass 的有效取值
const (
	// PodQOSGuaranteed 是 Guaranteed QoS 类别。
	PodQOSGuaranteed PodQOSClass = "Guaranteed"
	// PodQOSBurstable 是 Burstable QoS 类别。
	PodQOSBurstable PodQOSClass = "Burstable"
	// PodQOSBestEffort 是 BestEffort QoS 类别。
	PodQOSBestEffort PodQOSClass = "BestEffort"
)

// PodDNSConfig 定义在 DNSPolicy 生成的参数之外，
// Pod 额外的 DNS 参数。
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
	// Required（必填）。
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
	// IP 是分配给主机的 IP 地址
	IP string
}

// EphemeralContainerCommon 是 Container 中所有字段的副本，用于内联到 EphemeralContainer 中。
// 这个独立类型便于将 EphemeralContainer 转换为 Container，
// 也便于单独为 EphemeralContainer 的字段编写文档。
// 向 Container 添加新字段时，也必须在此处添加。
type EphemeralContainerCommon struct {
	// Required: 这必须是一个 DNS_LABEL。Pod 中的每个容器都必须
	// 拥有唯一的名称。
	Name string
	// Required（必填）。
	Image string
	// 可选：未提供时使用容器镜像的 entrypoint；不可更新。
	// 变量引用 $(VAR_NAME) 使用容器的环境变量展开。若某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会
	// 缩减为单个 $，从而可对 $(VAR_NAME) 语法进行转义：即 "$$(VAR_NAME)" 将
	// 产生字符串字面量 "$(VAR_NAME)"。无论变量是否存在，
	// 已转义的引用都不会被展开。
	// +optional
	Command []string
	// 可选：未提供时使用容器镜像的 cmd；不可更新。
	// 变量引用 $(VAR_NAME) 使用容器的环境变量展开。若某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会
	// 缩减为单个 $，从而可对 $(VAR_NAME) 语法进行转义：即 "$$(VAR_NAME)" 将
	// 产生字符串字面量 "$(VAR_NAME)"。无论变量是否存在，
	// 已转义的引用都不会被展开。
	// +optional
	Args []string
	// 可选：默认为容器运行时的默认工作目录。
	// +optional
	WorkingDir string
	// Ports 不允许为临时容器设置端口。
	// +optional
	Ports []ContainerPort
	// 用于填充容器环境变量的来源列表。
	// 来源中定义的键可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// 所有无效的键
	// 会在容器启动时以事件形式上报。当某个键存在于多个
	// 来源中时，最后一个来源关联的值优先。
	// 由带有重复键的 Env 定义的值优先。
	// 不可更新。
	// +optional
	EnvFrom []EnvFromSource
	// +optional
	Env []EnvVar
	// Resources 不允许为临时容器设置资源。临时容器使用已分配给 Pod 的空闲资源。
	// +optional
	Resources ResourceRequirements
	// 容器的资源调整策略。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	ResizePolicy []ContainerResizePolicy
	// 容器的重启策略，用于管理 Pod 内每个容器的重启行为。
	// 使用 restartPolicyRules 时必须指定。
	// 临时容器不能设置此字段。
	// +optional
	RestartPolicy *ContainerRestartPolicy
	// 表示一组用于判断容器退出时是否应重启的
	// 规则。规则按顺序求值。一旦某条规则匹配了容器退出条件，
	// 其余规则将被忽略。若没有规则匹配容器退出条件，
	// 由 Pod 级 restart policy 决定容器
	// 是否重启。规则约束：
	// - 最多允许 20 条规则。
	// - 规则可以有相同的 action。
	// - 校验时不禁止完全相同的规则。
	// +featureGate=ContainerRestartRules
	// +optional
	// +listType=atomic
	RestartPolicyRules []ContainerRestartRule
	// 要挂载到容器文件系统的 Pod 卷。临时容器不允许使用子路径挂载。
	// +optional
	VolumeMounts []VolumeMount
	// volumeDevices 是容器要使用的块设备列表。
	// +optional
	VolumeDevices []VolumeDevice
	// 临时容器不允许使用探针。
	// +optional
	LivenessProbe *Probe
	// 临时容器不允许使用探针。
	// +optional
	ReadinessProbe *Probe
	// 临时容器不允许使用探针。
	// +optional
	StartupProbe *Probe
	// Lifecycle 不允许为临时容器设置生命周期钩子。
	// +optional
	Lifecycle *Lifecycle
	// Required（必填）。
	// +optional
	TerminationMessagePath string
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy
	// Required: 该容器拉取镜像的策略
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

// EphemeralContainerCommon 可转换为 Container。两种类型之间的所有字段必须保持同步。
var _ = Container(EphemeralContainerCommon{})

// EphemeralContainer 是一种临时容器，可添加到现有 Pod 中用于用户发起的活动
// （如调试）。临时容器没有资源或调度保证，在其退出或 Pod 被移除或重启时
// 也不会被重启。如果临时容器导致 Pod 超出其资源分配，kubelet 可能驱逐该 Pod。
//
// 要添加临时容器，请使用现有 Pod 的 ephemeralcontainers 子资源。
// 临时容器不能被移除或重启。
type EphemeralContainer struct {
	// 临时容器拥有 Container 的所有字段，外加临时容器特有的
	// 附加字段。与 Container 共有的字段放在
	// 以下内联结构体中，以便 EphemeralContainer 可以便捷地
	// 转换为 Container。
	EphemeralContainerCommon

	// 若设置，则为该临时容器所瞄准的、来自 PodSpec 的容器名称。
	// 临时容器将在该容器的命名空间（IPC、PID 等）中运行。
	// 若未设置，则临时容器使用 Pod spec 中配置的命名空间。
	//
	// 容器运行时必须实现对此特性的支持。若运行时不
	// 支持命名空间瞄准，则设置此字段的结果是未定义的。
	// +optional
	TargetContainerName string
}

// EvictionResponder 允许你指定对 Eviction 作出响应的响应器。
// 响应器应通过 Eviction Resource API 进行观察和通信，协助目标
// 的优雅驱逐（例如终止某个 Pod）。
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

// PodStatus 表示 Pod 状态的相关信息。Status 可能滞后于系统的
// 实际状态。
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
	// 简短的 CamelCase 消息，详细说明 Pod 处于当前状态的原因。例如 'Evicted'
	// +optional
	Reason string
	// nominatedNodeName 在此 Pod 抢占节点上的其他 Pod、但无法立即被调度时设置，
	// 因为被抢占的 Pod 需要经历各自的优雅终止期。
	// 此字段并不保证 Pod 一定会被调度到该节点。如果其他节点更早可用，
	// 调度器可能决定将 Pod 放到别处。调度器也可能决定在抢占之后，
	// 将该节点上的资源让给后来创建的更高优先级 Pod。
	// +optional
	NominatedNodeName string

	// HostIP 保存 Pod 被分配到的主机的 IP 地址。Pod 尚未启动时为空。
	// Pod 可能被分配到 kubelet 有问题的节点，这意味着即使 Pod 已分配到节点，
	// HostIP 也不会更新。
	// +optional
	HostIP string

	// HostIPs 保存分配给主机的 IP 地址。如果设置了此字段，第一项必须
	// 与 hostIP 字段匹配。如果 Pod 尚未启动，此列表为空。
	// Pod 可能被分配到 kubelet 有问题的节点，这意味着即使有节点分配给了
	// 该 Pod，HostIPs 也不会更新。
	// 必须与 hostIP 字段匹配。如果尚未分配任何 IP，此列表为空。
	// +optional
	HostIPs []HostIP

	// PodIP 是分配给 Pod 的 IP 地址，由 Pod 注册策略保持与 PodIPs[0] 同步。
	// 至少在集群内可路由。如果尚未分配则为空。
	// +optional
	PodIP string
	// PodIPs 保存分配给 Pod 的所有已知 IP 地址。每个 Pod 对 IPv4 和 IPv6
	// 各自最多只能分配一个值。
	// +optional
	PodIPs []PodIP

	// Kubelet 确认接收该对象的日期和时间。
	// 该时间在 Kubelet 拉取 Pod 的容器镜像之前。
	// +optional
	StartTime *metav1.Time

	// 此 Pod 中初始化容器的状态。最近成功完成且不可重启的初始化容器
	// ready = true，最近启动的容器会设置 startTime。
	// Pod 中的每个初始化容器在此列表中应最多有一个状态，
	// 且所有状态都应对应 Pod 内的容器。
	// 但这并不强制执行。
	// 如果列表中存在不存在容器的状态，或列表中有重复名称，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
	InitContainerStatuses []ContainerStatus

	// 此 Pod 中容器的状态。
	// Pod 中的每个容器在此列表中应最多有一个状态，
	// 且所有状态都应对应 Pod 内的容器。
	// 但这并不强制执行。
	// 如果列表中存在不存在容器的状态，或列表中有重复名称，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
	// +optional
	ContainerStatuses []ContainerStatus
	// +optional
	QOSClass PodQOSClass

	// 在此 Pod 中运行过的任何临时容器的状态。
	// Pod 中的每个临时容器在此列表中应最多有一个状态，
	// 且所有状态都应对应 Pod 内的容器。
	// 但这并不强制执行。
	// 如果列表中存在不存在容器的状态，或列表中有重复名称，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
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

	// 资源声明（resource claim）的状态。
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaimStatuses []PodResourceClaimStatus

	// 由 DRA 支撑的扩展资源声明的状态。
	// +featureGate=DRAExtendedResource
	// +optional
	ExtendedResourceClaimStatus *PodExtendedResourceClaimStatus

	// AllocatedResources 是节点为此 Pod 分配的总请求量。
	// 如果未设置 Pod 级别请求，则为 Pod 内各容器请求量的总和。
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	AllocatedResources ResourceList

	// Resources 表示已应用
	// 到 Pod 级别的计算资源请求和限制
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	Resources *ResourceRequirements

	// NodeAllocatableResourceClaimStatuses 包含通过 DRA 声明为此 Pod 分配的、
	// 节点可分配资源的状态。这包括当前在 v1.Node `status.allocatable` 中报告、
	// 且不属于扩展资源的资源
	// （参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources）。
	// 例如 "cpu"、"memory"、"ephemeral-storage" 和 HugePage。
	// +featureGate=DRANodeAllocatableResources
	// +optional
	// +listType=map
	// +listMapKey=resourceClaimName
	NodeAllocatableResourceClaimStatuses []NodeAllocatableResourceClaimStatus

	// volumeHealth 包含节点上报的、Pod 正在使用的每个卷的健康状况。
	// 由 Pod 所在节点上的 kubelet 填充。
	// +featureGate=CSIVolumeHealth
	// +optional
	// +listType=map
	// +listMapKey=name
	VolumeHealth []PodVolumeHealth
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Pod 是容器的集合，既可作为输入（create、update），也可作为输出（list、get）。
type Pod struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Pod 的行为。
	// +optional
	Spec PodSpec

	// Status 表示 Pod 的当前信息。这些数据可能不是最新的。
	// +optional
	Status PodStatus
}

// PodTemplateSpec 描述从模板创建 Pod 时该 Pod 应具备的数据。
type PodTemplateSpec struct {
	// 从此模板创建的 Pod 的元数据。
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Pod 的行为。
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
// 作为副本控制器的内部表示，它可能设置 TemplateRef 或 Template 之一。
type ReplicationControllerSpec struct {
	// Replicas 是期望的副本数。
	Replicas *int32

	// 新创建的 Pod 在被视为可用之前，需要保持就绪且容器不崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	MinReadySeconds int32

	// Selector 是对应与 Replicas 数量匹配的 Pod 的标签查询。
	Selector map[string]string

	// TemplateRef 引用一个描述对象，该对象定义在检测到副本数不足时要创建的 Pod。
	// 如果设置了 Template，此引用会被忽略。
	// 必须在转换为版本化 API 对象之前设置
	// +optional
	// TemplateRef *ObjectReference

	// Template 是描述当检测到副本不足时要创建的 Pod 的对象。其优先级高于 TemplateRef。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// +optional
	Template *PodTemplateSpec
}

// ReplicationControllerStatus 表示副本控制器的当前状态。
type ReplicationControllerStatus struct {
	// Replicas 是实际副本数。
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

	// ObservedGeneration 是控制器观测到的最新 generation。
	// +optional
	ObservedGeneration int64

	// 表示对副本控制器当前状态的最新可用观测。
	// +optional
	Conditions []ReplicationControllerCondition
}

// ReplicationControllerConditionType 定义副本控制器的条件。
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
	// 副本控制器条件的类型。
	Type ReplicationControllerConditionType
	// 条件的状态，取值为 True、False、Unknown 之一。
	Status ConditionStatus
	// 条件从一种状态过渡到另一种状态的最后时间。
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

	// Spec 定义此副本控制器的期望行为。
	// +optional
	Spec ReplicationControllerSpec

	// Status 是此副本控制器的当前状态。这些数据可能在一段时间内处于过期状态。
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

// ServiceAffinity 是 Type string（类型字符串）。
type ServiceAffinity string

const (
	// ServiceAffinityClientIP 表示基于客户端 IP 的会话亲和。
	ServiceAffinityClientIP ServiceAffinity = "ClientIP"

	// ServiceAffinityNone 表示无会话亲和。
	ServiceAffinityNone ServiceAffinity = "None"
)

const (
	// DefaultClientIPServiceAffinitySeconds 是基于客户端 IP 的会话亲和
	// 的默认超时秒数 —— 3 小时。
	DefaultClientIPServiceAffinitySeconds int32 = 10800
	// MaxClientIPServiceAffinitySeconds 是基于客户端 IP 的会话亲和
	// 的最大超时秒数 —— 1 天。
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

// ServiceType 字符串描述 Service 的入口方式。
type ServiceType string

const (
	// ServiceTypeClusterIP 表示该 Service 只能在集群内部
	// 通过 ClusterIP 访问。
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

// ServiceInternalTrafficPolicy 描述发往 ClusterIP 的流量的
// 端点选择策略。
type ServiceInternalTrafficPolicy string

const (
	// ServiceInternalTrafficPolicyCluster 将流量路由到所有端点。
	ServiceInternalTrafficPolicyCluster ServiceInternalTrafficPolicy = "Cluster"

	// ServiceInternalTrafficPolicyLocal 仅将流量路由到与客户端 Pod 位于同一节点的端点
	// （如果没有本地端点则丢弃流量）。
	ServiceInternalTrafficPolicyLocal ServiceInternalTrafficPolicy = "Local"
)

// ServiceExternalTrafficPolicy 描述发往 Service 外部入口
// （NodePort、ExternalIP 和
// 负载均衡器 IP）的流量的端点选择策略。
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

// ServiceStatus 表示 Service 的当前状态。
type ServiceStatus struct {
	// LoadBalancer 包含负载均衡器的当前状态（如果存在）。
	// +optional
	LoadBalancer LoadBalancerStatus

	// Service 的当前条件。
	// +optional
	Conditions []metav1.Condition
}

// LoadBalancerStatus 表示负载均衡器的状态。
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

	// IPMode 指定负载均衡器 IP 的行为方式，只有在设置了 ip 字段时才可指定。
	// 设置为 "VIP" 表示流量被投递到节点时，
	// 目标地址为负载均衡器的 IP 和端口。
	// 设置为 "Proxy" 表示流量被投递到节点或 Pod 时，
	// 目标地址为节点的 IP 和节点端口，或 Pod 的 IP 和端口。
	// Service 实现可以利用此信息来调整流量路由。
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
	// IPFamilyPolicySingleStack 表示此 Service 必须只具有单个 IPFamily。
	// 所分配的 IPFamily 基于集群使用的默认 IPFamily，
	// 或由 service.spec.ipFamilies 字段指定
	IPFamilyPolicySingleStack IPFamilyPolicy = "SingleStack"
	// IPFamilyPolicyPreferDualStack 表示当集群配置为双栈时，
	// 此 Service 优先使用双栈。如果集群未配置为双栈，
	// 则该 Service 只会被分配单个 IPFamily。如果 service.spec.ipFamilies
	// 未设置 IPFamily，则该 Service 会被分配集群上配置的默认 IPFamily
	IPFamilyPolicyPreferDualStack IPFamilyPolicy = "PreferDualStack"
	// IPFamilyPolicyRequireDualStack 表示此 Service 必须使用双栈。在单栈集群上使用
	// IPFamilyPolicyRequireDualStack 会导致校验错误。分配给此 Service 的
	// IPFamilies（及其顺序）基于 service.spec.ipFamilies。如果未提供
	// service.spec.ipFamilies，则按照集群上的配置进行分配。
	// 如果 service.spec.ipFamilies 只有一项，则由 apiserver 添加
	// 另一个 IPFamily
	IPFamilyPolicyRequireDualStack IPFamilyPolicy = "RequireDualStack"
)

// ServiceSpec 描述用户在 Service 上创建的属性。
type ServiceSpec struct {

	// 此 Service 暴露的端口列表。
	Ports []ServicePort

	// 将 Service 流量路由到标签键和值与此 selector 匹配的 Pod。
	// 如果为空或不存在，则认为该 Service 由外部进程管理其端点，Kubernetes 不会修改。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。
	// 当类型为 ExternalName 时忽略。
	// 更多信息参见: https://kubernetes.io/docs/concepts/services-networking/service/
	Selector map[string]string

	// ClusterIP 是 Service 的 IP 地址，通常由 master 随机分配。
	// 如果手动指定了一个未被他人使用的地址，则将其分配给该 Service；
	// 否则 Service 的创建将失败。此字段不能通过更新更改。
	// 有效值为 "None"、空字符串 ("") 或有效的 IP 地址。
	// 当不需要代理时，无头（headless）Service 可以指定 "None"。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。类型为 ExternalName 时忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +optional
	ClusterIP string

	// ClusterIPs 标识分配给此 Service 的所有 ClusterIP。
	// ClusterIP 的分配或保留基于 service.spec.ipFamilies 的值。
	// ClusterIPs 中最多允许两项（双栈 IP）。
	// 每个 ClusterIP 的 IPFamily 必须与 service.spec.ipFamilies 中提供的值匹配。
	// 使用 ClusterIPs 的客户端必须保持其与 ClusterIP（如提供）同步，
	// 即使 ClusterIP 与 ClusterIPs 的第一个元素一致。
	// +optional
	ClusterIPs []string
	// Type 决定 Service 的暴露方式。默认为 ClusterIP。
	// 有效选项为 ExternalName、ClusterIP、NodePort 和 LoadBalancer。
	// "ExternalName" 映射到指定的 externalName。
	// "ClusterIP" 分配一个集群内部 IP 地址，用于向端点做负载均衡。
	// 端点由 selector 决定；如果未指定 selector，则由手动构造的 Endpoints 对象决定。
	// 如果 clusterIP 为 "None"，则不分配虚拟 IP，端点以一组端点的形式发布，
	// 而不是一个稳定的 IP。
	// "NodePort" 在 ClusterIP 的基础上，在每个节点上分配一个端口，
	// 路由到 clusterIP。
	// "LoadBalancer" 在 NodePort 的基础上创建外部负载均衡器
	// （如果当前云环境支持），路由到 clusterIP。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/
	// +optional
	Type ServiceType

	// ExternalIPs 由外部负载均衡器使用，也可以由用户设置，
	// 用于处理到达节点的流量。
	//
	// Deprecated: ExternalIPs 已弃用，可能在将来的版本中移除。
	// +optional
	ExternalIPs []string

	// 可选：支持 "ClientIP" 和 "None"。用于维持会话亲和。
	// +optional
	SessionAffinity ServiceAffinity

	// 仅适用于 Service 类型：LoadBalancer
	// LoadBalancer 将使用此字段指定的 IP 创建。
	// 此特性取决于底层云厂商在创建负载均衡器时是否支持指定 loadBalancerIP。
	// 如果云厂商不支持此特性，该字段将被忽略。
	// Deprecated: 此字段定义不充分，其含义在不同实现中各不相同。
	// 使用它不具备可移植性，且可能不支持双栈。
	// 建议用户在可用时使用特定实现的注解。
	// +optional
	LoadBalancerIP string

	// 可选：如果指定且平台支持，通过云厂商负载均衡器的流量
	// 将被限制为指定的客户端 IP。如果云厂商不支持此特性，该字段将被忽略。"
	// +optional
	LoadBalancerSourceRanges []string

	// ExternalName 是 kubedns 或等价组件为该 Service 以 CNAME 记录形式返回的外部引用。
	// 不涉及任何代理。
	// 必须是有效的 RFC-1123 主机名（https://tools.ietf.org/html/rfc1123），
	// 且要求 Type 为 ExternalName。
	ExternalName string

	// externalTrafficPolicy 描述节点如何分发其在 Service 的"面向外部"地址
	// （NodePort、ExternalIP 和负载均衡器 IP）上接收到的 Service 流量。
	// 若设为 "Local"，代理将以如下方式配置该 Service：假定外部负载均衡器
	// 会负责在节点之间平衡 Service 流量，因此每个节点只把流量投递给
	// 本节点上的 Service 端点，且不会伪装客户端源 IP。
	// （误发到没有端点的节点的流量将被丢弃。）
	// 默认值 "Cluster" 使用标准行为：均匀路由到所有端点
	// （可能受拓扑及其他特性影响）。注意：从集群内部发往 External IP
	// 或负载均衡器 IP 的流量始终采用 "Cluster" 语义，但从集群内部发往
	// NodePort 的客户端在选择节点时可能需要考虑流量策略。
	// +optional
	ExternalTrafficPolicy ServiceExternalTrafficPolicy

	// healthCheckNodePort 指定 Service 的健康检查 nodePort。
	// 如果未指定，HealthCheckNodePort 将由 Service API 后端使用已分配的
	// nodePort 创建。如果客户端指定了 nodePort，将使用用户指定的值。
	// 仅当 Type 设为 LoadBalancer 且 ExternalTrafficPolicy 设为 Local 时生效。
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

	// IPFamilies 标识为此 Service 分配的所有 IPFamily。如果未提供 IPFamilies 的值，
	// 将基于集群配置和 service.spec.ipFamilyPolicy 的值设置默认值。
	// IPFamilies 中最多允许两个值（双栈 IPFamily）。IPFamilies 字段是条件可变的：
	// 允许添加或移除次要 IPFamily，但不允许更改 Service 的主 IPFamily。
	// +optional
	IPFamilies []IPFamily

	// IPFamilyPolicy 表示此 Service 请求或要求的双栈特性。
	// 如果未提供值，则此 Service 被视为 SingleStack（单个 IPFamily）。
	// Service 可以是 SingleStack（单个 IPFamily）、
	// PreferDualStack（在双栈集群上为两个双栈 IPFamily，
	// 在单栈集群上为单个 IPFamily），或 RequireDualStack
	// （在配置为双栈的集群上为两个双栈 IPFamily，否则失败）。
	// 分配给此 Service 的 IPFamilies 和 ClusterIPs 可分别通过
	// service.spec.ipFamilies 和 service.spec.clusterIPs 控制。
	// +optional
	IPFamilyPolicy *IPFamilyPolicy

	// allocateLoadBalancerNodePorts 定义是否为类型为 LoadBalancer 的 Service
	// 自动分配 NodePort。默认为 "true"。如果集群负载均衡器不依赖 NodePort，
	// 可以设为 "false"。无论此字段如何设置，如果调用方请求了特定的 NodePort
	// （通过指定值），这些请求都会被满足。
	// 此字段只能为类型为 LoadBalancer 的 Service 设置，
	// 如果类型改为其他任何类型，该字段将被清空。
	// +optional
	AllocateLoadBalancerNodePorts *bool

	// loadBalancerClass 是此 Service 所属的负载均衡器实现的类别。
	// 如果指定，此字段的值必须是带可选前缀的标签风格标识符，
	// 例如 "internal-vip" 或 "example.com/internal-vip"。无前缀的名称保留给最终用户使用。
	// 此字段只能在 Service 类型为 'LoadBalancer' 时设置。如果未设置，则使用默认负载均衡器实现，
	// 目前这通常通过云厂商集成完成，但也应适用于任何默认实现。
	// 如果设置了该字段，则假定有负载均衡器实现正在监听匹配该类别的 Service。
	// 任何默认负载均衡器实现（如云厂商）都应忽略设置了此字段的 Service。
	// 此字段只能在创建 Service 或将 Service 更新为 'LoadBalancer' 类型时设置。
	// 一旦设置，就不能更改。当 Service 被更新为非 'LoadBalancer' 类型时，此字段将被清除。
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

// ServicePort 表示 Service 暴露的端口。
type ServicePort struct {
	// 如果此 Service 只定义了一个 ServicePort，则可选：此端口在 Service 内的名称。
	// 必须是 DNS_LABEL。ServiceSpec 内的所有端口必须具有唯一名称。
	// 该名称映射到 EndpointPort 对象的 'Name' 字段。
	Name string

	// 此端口的 IP 协议。支持 "TCP"、"UDP" 和 "SCTP"。
	Protocol Protocol

	// 此端口的应用层协议。
	// 用作提示，让实现为其所理解的协议提供更丰富的行为。
	// 此字段遵循标准 Kubernetes 标签语法。
	// 有效值为以下之一：
	//
	// * 无前缀的协议名称 —— 保留给 IANA 标准服务名称（依据
	// RFC-6335 和 https://www.iana.org/assignments/service-names）。
	//
	// * Kubernetes 定义的带前缀名称：
	//   * 'kubernetes.io/h2c' - 明文上的 HTTP/2 预先知晓协议，见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - 明文上的 WebSocket，见 https://www.rfc-editor.org/rfc/rfc6455
	//   * 'kubernetes.io/wss' - TLS 上的 WebSocket，见 https://www.rfc-editor.org/rfc/rfc6455
	//
	// * 其他协议应使用实现定义的带前缀名称，例如
	// mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string

	// 将在 Service 上暴露的端口。
	Port int32

	// 可选：此 Service 所选 Pod 上的目标端口。如果该值是字符串，
	// 则将其作为目标 Pod 容器端口中的命名端口进行查找。
	// 如果未指定，则使用 'port' 字段的值（恒等映射）。
	// 对于 clusterIP=None 的 Service，此字段被忽略，
	// 应当省略或设置为与 'port' 字段相等的值。
	TargetPort intstr.IntOrString

	// 此 Service 在每个节点上暴露的端口。
	// 默认行为：如果此 Service 的 ServiceType 需要节点端口，则自动分配一个。
	NodePort int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Service 是对软件服务（例如 mysql）的命名抽象，由代理监听的本地端口
// （例如 3306）和决定哪些 Pod 响应经代理发送的请求的 selector 组成。
type Service struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义 Service 的行为。
	// +optional
	Spec ServiceSpec

	// Status 表示 Service 的当前状态。
	// +optional
	Status ServiceStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceAccount 将以下内容绑定在一起：
// * 一个可供用户（可能还有外围系统）理解的名称，用于标识身份
// * 一个可被认证和授权的主体
// * 一组 Secret
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

	// AutomountServiceAccountToken 指示以该 ServiceAccount 身份运行的 Pod 是否应自动挂载 API 令牌。
	// 可以在 Pod 级别覆盖。
	// +optional
	AutomountServiceAccountToken *bool
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceAccountList 是 ServiceAccount 对象的列表。
type ServiceAccountList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []ServiceAccount
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Endpoints 是实现实际服务的端点的集合。示例：
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

	// 所有端点的集合是所有子集（subset）的并集。
	Subsets []EndpointSubset
}

// EndpointSubset 是一组具有相同端口集合的地址。
// 展开后的端点集合是 Addresses × Ports 的笛卡尔积。
// 例如，给定：
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

// EndpointAddress 是描述单个 IP 地址的元组。
type EndpointAddress struct {
	// 此端点的 IP。
	// 不得为回环地址（127.0.0.0/8 或 ::1）、链路本地地址（169.254.0.0/16 或 fe80::/10），
	// 或链路本地组播地址（224.0.0.0/24 或 ff02::/16）。
	IP string
	// 可选：此端点的主机名。
	// 供 DNS 服务器等使用。
	// +optional
	Hostname string
	// 可选：承载此端点的节点。可用于确定某个节点上的本地端点。
	// +optional
	NodeName *string
	// 可选：与该入口点相关的 Kubernetes 对象。
	TargetRef *ObjectReference
}

// EndpointPort 是描述单个端口的元组。
type EndpointPort struct {
	// 此端口的名称（与 ServicePort.Name 对应）。如果只定义了一个端口，则可选。
	// 必须是 DNS_LABEL。
	Name string

	// 端口号。
	Port int32

	// 此端口的 IP 协议。
	Protocol Protocol

	// 此端口的应用层协议。
	// 用作提示，让实现为其所理解的协议提供更丰富的行为。
	// 此字段遵循标准 Kubernetes 标签语法。
	// 有效值为以下之一：
	//
	// * 无前缀的协议名称 —— 保留给 IANA 标准服务名称（依据
	// RFC-6335 和 https://www.iana.org/assignments/service-names）。
	//
	// * Kubernetes 定义的带前缀名称：
	//   * 'kubernetes.io/h2c' - 明文上的 HTTP/2 预先知晓协议，见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - 明文上的 WebSocket，见 https://www.rfc-editor.org/rfc/rfc6455
	//   * 'kubernetes.io/wss' - TLS 上的 WebSocket，见 https://www.rfc-editor.org/rfc/rfc6455
	//
	// * 其他协议应使用实现定义的带前缀名称，例如
	// mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// EndpointsList 是 Endpoints 的列表。
type EndpointsList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Endpoints
}

// NodeSpec 描述创建节点时所使用的属性。
type NodeSpec struct {
	// PodCIDRs 表示分配给该节点、供该节点上 Pod 使用的 IP 范围。每个节点对 IPv4 和 IPv6
	// 各自最多只能包含一个值。
	// 注意：当我们支持可迁移 IP 时，为节点分配 IP 范围的方式可能需要重新审视。
	// +optional
	PodCIDRs []string

	// 云厂商为该节点分配的 ID。
	// 注意：格式为 "<ProviderName>://<ProviderSpecificNodeID>"
	// +optional
	ProviderID string

	// Unschedulable 控制新 Pod 是否可调度到该节点。默认情况下节点是可调度的。
	// +optional
	Unschedulable bool

	// 如已指定，表示该节点的污点（taints）。
	// +optional
	Taints []Taint

	// Deprecated: 曾用于指定 DynamicKubeletConfig 特性中节点配置的来源。该特性已被移除。
	// +optional
	ConfigSource *NodeConfigSource

	// 已弃用。并非所有 kubelet 都会设置此字段。1.13 之后将移除该字段。
	// 参见: https://issues.k8s.io/61966
	// +optional
	DoNotUseExternalID string

	// PodPreemptionPolicy 控制此节点上 Pod 的节点级抢占行为。
	// 这是一个 alpha 字段，需要启用 InPlacePodVerticalScalingSchedulerPreemption 特性门控。
	// +featureGate=InPlacePodVerticalScalingSchedulerPreemption
	// +optional
	PodPreemptionPolicy *NodePodPreemptionPolicy
}

// NodePodPreemptionPolicy 定义管理此节点上 Pod 抢占行为的节点级策略。
type NodePodPreemptionPolicy struct {
	// DisableResizePreemption 列出已请求在此节点上禁用调度器和 Kubelet 针对原地 Pod 资源调整进行抢占的属主（owner）。
	// 这是一个 alpha 字段，需要启用 InPlacePodVerticalScalingSchedulerPreemption 特性门控。
	// +optional
	DisableResizePreemption []string
}

// Deprecated: NodeConfigSource 指定节点配置的来源。必须恰好有一个子字段非 nil。
type NodeConfigSource struct {
	ConfigMap *ConfigMapNodeConfigSource
}

// Deprecated: ConfigMapNodeConfigSource 表示节点的 ConfigMap。
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
		早期版本中 port 标签没有正确地放在引号内，因此为了向后兼容
		（因为它会回退到变量名 'Port'），此处必须使用大写。
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
	// Name 是运行时处理器名称。
	// 默认运行时处理器时为空。
	// +optional
	Name string
	// 支持的特性。
	// +optional
	Features *NodeRuntimeHandlerFeatures
}

// NodeFeatures 描述 CRI 实现所具备的特性集合。
// 其中包含的特性应仅取决于 CRI 实现，与运行时处理器无关。
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
	// 节点上报的 Boot ID。
	BootID string
	// 节点上报的内核版本。
	KernelVersion string
	// 节点上报的操作系统镜像。
	OSImage string
	// 节点上报的容器运行时版本。
	ContainerRuntimeVersion string
	// 节点上报的 Kubelet 版本。
	KubeletVersion string
	// Deprecated: 节点上报的 KubeProxy 版本。
	KubeProxyVersion string
	// 节点上报的操作系统。
	OperatingSystem string
	// 节点上报的体系结构。
	Architecture string
	// 节点上报的 Swap 信息。
	Swap *NodeSwapStatus
	// 节点是否运行在用户命名空间中。
	// +featureGate=KubeletInUserNamespace
	// +optional
	RunningInUserNamespace *bool
}

// NodeSwapStatus 表示交换内存信息。
type NodeSwapStatus struct {
	// 交换内存总量（字节）。
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
	// Error 描述协调 Spec.ConfigSource 与 Active 配置时出现的任何问题。
	// 错误可能发生在多种操作中，例如：尝试将 Spec.ConfigSource 检查点写入本地 Assigned
	// 记录、尝试将与 Spec.ConfigSource 关联的配置载荷做检查点、尝试加载或校验
	// Assigned 配置等。
	// 同步配置过程中的不同阶段都可能发生错误。较早的错误（如下载或检查点错误）
	// 不会导致回滚到 LastKnownGood，并可能在 Kubelet 重试过程中自行解决。
	// 较晚的错误（如加载或校验已做检查点的配置）会导致回滚到 LastKnownGood。
	// 对于后一种情况，通常可以通过修复 Spec.ConfigSource 中分配的配置来解决错误。
	// 可以在 Kubelet 日志中搜索错误消息以获取用于调试的额外信息。
	// Error 是对错误状态的人类可读描述；程序可以检查 Error 是否为空，
	// 但不应依赖 Error 文本在不同 Kubelet 版本之间的稳定性。
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
	// NodePhase 是节点的当前生命周期阶段。
	// +optional
	Phase NodePhase
	// Conditions 是当前观察到的节点状况列表。
	// +optional
	Conditions []NodeCondition
	// 如可用，则从云厂商查询获得。
	// +optional
	Addresses []NodeAddress
	// 节点上运行的守护进程的端点。
	// +optional
	DaemonEndpoints NodeDaemonEndpoints
	// 用于唯一标识节点的一组 id/uuid。
	// +optional
	NodeInfo NodeSystemInfo
	// 此节点上的容器镜像列表。
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
	// 可用的运行时处理器（runtime handler）。
	// +optional
	RuntimeHandlers []NodeRuntimeHandler
	// Features 描述 CRI 实现所具备的特性集合。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	Features *NodeFeatures
	// DeclaredFeatures 表示节点声明的特性。
	// +featureGate=NodeDeclaredFeatures
	// +optional
	DeclaredFeatures []string
}

// UniqueVolumeName 定义已附加卷的名称。
type UniqueVolumeName string

// AttachedVolume 描述附加到节点的卷
type AttachedVolume struct {
	// 已附加卷的名称。
	Name UniqueVolumeName

	// DevicePath 表示该卷应当可用的设备路径。
	DevicePath string
}

// AvoidPods 描述应当避开该节点的 Pod。它是 Node 上键为
// scheduler.alpha.kubernetes.io/preferAvoidPods 的注解的取值，
// 最终会成为 NodeStatus 的一个字段。
type AvoidPods struct {
	// 应避开此节点的 Pod 的签名列表，数量有上限，按时间戳从旧到新排序。
	// 切片大小未作规定。
	// +optional
	PreferAvoidPods []PreferAvoidPodsEntry
}

// PreferAvoidPodsEntry 描述一类应当避开该节点的 Pod。
type PreferAvoidPodsEntry struct {
	// Pod 的类别。
	PodSignature PodSignature
	// 此条目被添加到列表的时间。
	// +optional
	EvictionTime metav1.Time
	// （简要）此条目被添加到列表的原因。
	// +optional
	Reason string
	// 说明此条目为何被添加到列表的人类可读消息。
	// +optional
	Message string
}

// PodSignature 描述应当避开此节点的 Pod 类别。
// 应当只设置其中一个字段。
type PodSignature struct {
	// 引用其 Pod 应当避开该节点的控制器。
	// +optional
	PodController *metav1.OwnerReference
}

// ContainerImage 描述一个容器镜像。
type ContainerImage struct {
	// 该镜像已知的名称。
	// +optional
	Names []string
	// 镜像大小（字节）。
	// +optional
	SizeBytes int64
}

// NodePhase 定义节点所处的阶段。
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

// NodeConditionType 定义节点的状况类型。
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

// NodeCondition 表示节点的状况。
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

// NodeAddressType 定义节点的地址类型。
type NodeAddressType string

// 以下是节点地址类型的有效值。
const (
	// NodeHostName 标识节点的名称。虽然可以认为每个节点都有此类型的 NodeAddress，
	// 但其确切的语法和语义并未定义，在不同集群之间也不一致。
	NodeHostName NodeAddressType = "Hostname"

	// NodeInternalIP 标识分配给节点某个网络接口的 IP 地址。
	// 每个节点应当至少有一个此类型的地址。
	//
	// 内部 IP 通常预期可被集群中其他所有节点访问，但可能对集群外的主机不可见。
	// 默认假定 kube-apiserver 可以访问节点内部 IP，
	// 尽管也可以配置为不支持这一点。
	//
	// NodeInternalIP 是节点 IP 的默认类型，并不一定意味着该 IP 仅内部可达。
	// 如果节点有多个内部 IP，额外的 IP 不被赋予特定语义。
	NodeInternalIP NodeAddressType = "InternalIP"

	// NodeExternalIP 标识在某种程度上被认为比内部 IP 更适合从集群外部使用的
	// IP 地址，但未定义具体语义。它可能是全局可路由的 IP，但并不作要求。
	//
	// 外部 IP 可以像 NodeInternalIP 一样直接分配给节点上的接口；
	// 或者，发往外部 IP 的数据包可能被 NAT 转换到节点内部 IP，而非直接投递
	// （这使得该 IP 在节点间流量上的效率低于 NodeInternalIP）。
	NodeExternalIP NodeAddressType = "ExternalIP"

	// NodeInternalDNS 标识解析为具有 NodeInternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeInternalIP 地址。
	NodeInternalDNS NodeAddressType = "InternalDNS"

	// NodeExternalDNS 标识解析为具有 NodeExternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeExternalIP 地址。
	NodeExternalDNS NodeAddressType = "ExternalDNS"
)

// NodeAddress 表示节点的地址。
type NodeAddress struct {
	Type    NodeAddressType
	Address string
}

// ResourceName 是 ResourceList 中标识各种资源的名称。
type ResourceName string

// 资源名称不得超过 63 个字符，由大小写字母数字字符组成，
// -、_ 和 . 字符允许出现在除首尾之外的任意位置。
// 默认约定（与注解相同）是使用小写名称，复合词之间用短横线而不是驼峰式分隔。
// 完全限定的资源类型名由 DNS 风格的子域名构成，后跟斜杠 `/` 和名称。
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
	// ResourceDefaultNamespacePrefix 是默认的命名空间前缀。
	ResourceDefaultNamespacePrefix = "kubernetes.io/"
	// ResourceHugePagesPrefix 是 HugePage 资源的名称前缀（alpha）。
	ResourceHugePagesPrefix = "hugepages-"
	// ResourceAttachableVolumesPrefix 是存储资源限制的名称前缀。
	ResourceAttachableVolumesPrefix = "attachable-volumes-"
)

// ResourceList 是一组（资源名称，数量）对。
type ResourceList map[ResourceName]resource.Quantity

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Node 是 Kubernetes 中的工作节点。
// etcd 中记录的节点名称位于 ObjectMeta.Name。
type Node struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义节点的行为。
	// +optional
	Spec NodeSpec

	// Status 描述节点的当前状态。
	// +optional
	Status NodeStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeList 是节点的列表。
type NodeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Node
}

// NamespaceSpec 描述 Namespace 的属性。
type NamespaceSpec struct {
	// Finalizers 是一个不透明的值列表，必须为空才能将对象从存储中永久删除。
	Finalizers []FinalizerName
}

// FinalizerName 是在命名空间生命周期中标识终结器的名称。
type FinalizerName string

// 这些是 Kubernetes 的内部终结器值，除非在此处或 metav1 中定义，
// 否则必须是限定名称。
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

// NamespacePhase 定义命名空间所处的阶段。
type NamespacePhase string

// 以下是命名空间的有效阶段。
const (
	// NamespaceActive 表示命名空间可在系统中使用
	NamespaceActive NamespacePhase = "Active"
	// NamespaceTerminating 表示命名空间正在优雅终止
	NamespaceTerminating NamespacePhase = "Terminating"
)

// NamespaceConditionType 定义用于报告命名空间生命周期及删除进度状态的常量。
type NamespaceConditionType string

// 以下是命名空间的有效状况。
const (
	NamespaceDeletionDiscoveryFailure NamespaceConditionType = "NamespaceDeletionDiscoveryFailure"
	NamespaceDeletionContentFailure   NamespaceConditionType = "NamespaceDeletionContentFailure"
	NamespaceDeletionGVParsingFailure NamespaceConditionType = "NamespaceDeletionGroupVersionParsingFailure"
)

// NamespaceCondition 包含命名空间状态的详细信息。
type NamespaceCondition struct {
	// 命名空间控制器条件的类型。
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
// 使用多个命名空间是可选项。
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

// Binding 将一个对象绑定到另一个对象；例如，调度器将 Pod 绑定到节点。
type Binding struct {
	metav1.TypeMeta
	// ObjectMeta 描述被绑定的对象。
	// +optional
	metav1.ObjectMeta

	// Target 是要绑定到的目标对象。
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

// PodLogOptions 是对 Pod 日志 REST 调用的查询选项。
type PodLogOptions struct {
	metav1.TypeMeta

	// 要返回哪个容器的日志。
	Container string
	// 若为 true，则持续跟踪 Pod 的日志。
	Follow bool
	// 若为 true，则返回上一个已终止容器的日志。
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
	// 若为 true，则在每行日志输出的开头添加带 9 位小数秒的 RFC 3339 时间戳。
	Timestamps bool
	// 若设置，表示从日志末尾开始显示的行数。如果未指定，
	// 则从容器创建起，或从 sinceSeconds / sinceTime 指定的时间点起显示日志。
	// 注意，指定 "TailLines" 时，"Stream" 只能设为 nil 或 "All"。
	TailLines *int64
	// 若设置，表示在终止日志输出之前从服务器读取的字节数。
	// 这可能导致最后一行日志显示不完整，并且返回的内容可能略多或略少于指定的限制。
	LimitBytes *int64

	// insecureSkipTLSVerifyBackend 表示 apiserver 不应确认其所连接后端的服务器证书的有效性。
	// 这会使 apiserver 与后端之间的 HTTPS 连接变得不安全。
	// 这意味着 apiserver 无法验证其接收的日志数据来自真正的 kubelet。
	// 如果 kubelet 配置为校验 apiserver 的 TLS 凭据，并不意味着与真正 kubelet 的连接
	// 容易受到中间人攻击（例如攻击者无法拦截来自真正 kubelet 的实际日志数据）。
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

// PodAttachOptions 是对 Pod 远程 attach 调用的查询选项。
// TODO: 与下方的 PodExecOptions 合并，统一处理 stdin、stdout 等
type PodAttachOptions struct {
	metav1.TypeMeta

	// 若 Stdin 为 true，表示 attach 调用需要重定向 stdin。
	// +optional
	Stdin bool

	// 若 Stdout 为 true，表示 attach 调用需要重定向 stdout。
	// +optional
	Stdout bool

	// 若 Stderr 为 true，表示 attach 调用需要重定向 stderr。
	// +optional
	Stderr bool

	// 若 TTY 为 true，表示 attach 调用将分配一个 tty。
	// +optional
	TTY bool

	// 要 attach 到的容器。
	// +optional
	Container string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodExecOptions 是对 Pod 远程 exec 调用的查询选项。
type PodExecOptions struct {
	metav1.TypeMeta

	// 若 Stdin 为 true，表示 exec 调用需要重定向 stdin。
	Stdin bool

	// 若 Stdout 为 true，表示 exec 调用需要重定向 stdout。
	Stdout bool

	// 若 Stderr 为 true，表示 exec 调用需要重定向 stderr。
	Stderr bool

	// 若 TTY 为 true，表示 exec 调用将分配一个 tty。
	TTY bool

	// 在哪个容器中执行命令。
	Container string

	// Command 是要执行的远程命令；argv 数组；不在 shell 中执行。
	Command []string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodPortForwardOptions 是对 Pod 端口转发调用的查询选项。
type PodPortForwardOptions struct {
	metav1.TypeMeta

	// 要转发的端口列表。
	// +optional
	Ports []int32
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// PodProxyOptions 是对 Pod 代理调用的查询选项。
type PodProxyOptions struct {
	metav1.TypeMeta

	// Path 是本次到 Pod 的代理请求所使用的 URL 路径。
	Path string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// NodeProxyOptions 是对 Node 代理调用的查询选项。
type NodeProxyOptions struct {
	metav1.TypeMeta

	// Path 是本次到 Node 的代理请求所使用的 URL 路径。
	Path string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ServiceProxyOptions 是 Service 代理调用的查询选项。
type ServiceProxyOptions struct {
	metav1.TypeMeta

	// Path 是 URL 中包含服务端点、后缀以及当前对 Service
	// 的代理请求所使用的参数的部分。
	// 例如，完整的请求 URL 是
	// http://localhost/api/v1/namespaces/kube-system/services/elasticsearch-logging/_search?q=user:kimchy.
	// Path 为 _search?q=user:kimchy。
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

	// 可选。如果引用的是对象的一部分而非整个对象，此字符串应包含
	// 标识子对象的信息。例如，如果对象引用指向 Pod 内的某个容器，
	// 取值形如："spec.containers{name}"（其中 "name" 指触发该事件的容器名称），
	// 若未指定容器名称则为 "spec.containers[2]"（此 Pod 中索引为 2 的容器）。
	// 选择该语法只是为了有一种明确定义的、引用对象某部分的方式。
	// TODO: 此设计尚未定型，该字段将来可能变更。
	// +optional
	FieldPath string
}

// LocalObjectReference 包含足够的信息，让你能在同一命名空间内定位被引用的对象。
type LocalObjectReference struct {
	// TODO: 添加其他有用的字段。apiVersion、kind、uid？
	Name string
}

// TypedLocalObjectReference 包含足够的信息，让你能在同一命名空间内定位带类型的被引用对象。
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

// SerializedReference 表示序列化后的对象引用。
type SerializedReference struct {
	metav1.TypeMeta
	// +optional
	Reference ObjectReference
}

// EventSource 表示事件生成的来源。
type EventSource struct {
	// 生成该事件的组件。
	// +optional
	Component string
	// 生成该事件的节点名称。
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

// Event 是集群中某处发生事件的报告。事件的保留时间有限，
// 触发条件和消息可能随时间演化。事件消费者不应依赖
// 具有给定 Reason 的事件的时机来反映一致的底层触发条件，
// 也不应依赖该 Reason 的事件持续存在。事件应被视为
// 参考性、尽力而为的补充数据。
// TODO: 决定是单独存储事件，还是与其所应用的对象一起存储。
type Event struct {
	metav1.TypeMeta

	metav1.ObjectMeta

	// 该事件所涉及的对象。映射到 events.Event.regarding。
	InvolvedObject ObjectReference

	// Reason 可选；这应当是一个简短、机器可理解的字符串，说明生成此事件的原因。
	// 例如，若事件报告某容器无法启动，Reason 可能是 "ImageNotFound"。
	// TODO: 提供确切的格式规范。
	// +optional
	Reason string

	// 可选。对该操作状态的人类可读描述。
	// TODO: 确定最大长度。映射到 events.Event.note。
	// +optional
	Message string

	// 可选。上报此事件的组件。应当是一个简短的、机器可理解的字符串。
	// +optional
	Source EventSource

	// 事件首次被记录的时间。（服务端接收时间见 TypeMeta。）
	// +optional
	FirstTimestamp metav1.Time

	// 记录该事件最近一次发生的时间。
	// +optional
	LastTimestamp metav1.Time

	// 此事件发生的次数。
	// +optional
	Count int32

	// 此事件的类型（Normal、Warning），将来可能新增类型。
	// +optional
	Type string

	// 首次观测到此 Event 的时间。
	// +optional
	EventTime metav1.MicroTime

	// 此事件所属事件序列的数据；若为单次事件则为 nil。
	// +optional
	Series *EventSeries

	// 针对 Regarding 对象已执行/失败的动作。
	// +optional
	Action string

	// 可选的次要对象，用于更复杂的动作。
	// +optional
	Related *ObjectReference

	// 发出此 Event 的控制器名称，例如 `kubernetes.io/kubelet`。
	// +optional
	ReportingController string

	// 控制器实例的 ID，例如 `kubelet-xyzf`。
	// +optional
	ReportingInstance string
}

// EventSeries 表示一系列事件。
type EventSeries struct {
	// 该序列截至最后心跳时间的发生次数。
	Count int32
	// 最后一次观测到发生的时间。
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

// LimitType 定义被限制对象的类型。
type LimitType string

const (
	// LimitTypePod 定义适用于命名空间中所有 Pod 的限制。
	LimitTypePod LimitType = "Pod"
	// LimitTypeContainer 定义适用于命名空间中所有容器的限制。
	LimitTypeContainer LimitType = "Container"
	// LimitTypePersistentVolumeClaim 定义适用于命名空间中所有持久卷声明的限制。
	LimitTypePersistentVolumeClaim LimitType = "PersistentVolumeClaim"
)

// LimitRangeItem 为按类型匹配的任意资源定义最小/最大使用限制。
type LimitRangeItem struct {
	// 此限制所适用的资源类型。
	// +optional
	Type LimitType
	// 该类型按资源名称划分的最大使用约束。
	// +optional
	Max ResourceList
	// 该类型按资源名称划分的最小使用约束。
	// +optional
	Min ResourceList
	// 按资源名称划分的默认资源需求限制值。
	// +optional
	Default ResourceList
	// DefaultRequest 是按资源名称划分的默认资源请求值。
	// +optional
	DefaultRequest ResourceList
	// MaxLimitRequestRatio 表示命名资源的最大突发值。
	// +optional
	MaxLimitRequestRatio ResourceList
}

// LimitRangeSpec 为按类型匹配的资源定义最小/最大使用限制。
type LimitRangeSpec struct {
	// Limits 是需要强制执行的 LimitRangeItem 对象列表。
	Limits []LimitRangeItem
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LimitRange 为命名空间中每类资源设置资源使用限制。
type LimitRange struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Spec 定义要强制执行的限制。
	// +optional
	Spec LimitRangeSpec
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// LimitRangeList 是 LimitRange 对象的列表。
type LimitRangeList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 LimitRange 对象的列表。
	Items []LimitRange
}

// 以下常量标识 Kubernetes 对象类型的资源。
const (
	// Pod 数量。
	ResourcePods ResourceName = "pods"
	// Service 数量。
	ResourceServices ResourceName = "services"
	// ReplicationControllers，数量。
	ResourceReplicationControllers ResourceName = "replicationcontrollers"
	// ResourceQuotas，数量。
	ResourceQuotas ResourceName = "resourcequotas"
	// ResourceSecrets，数量。
	ResourceSecrets ResourceName = "secrets"
	// ResourceConfigMaps，数量。
	ResourceConfigMaps ResourceName = "configmaps"
	// ResourcePersistentVolumeClaims，数量。
	ResourcePersistentVolumeClaims ResourceName = "persistentvolumeclaims"
	// ResourceServicesNodePorts，数量。
	ResourceServicesNodePorts ResourceName = "services.nodeports"
	// ResourceServicesLoadBalancers，数量。
	ResourceServicesLoadBalancers ResourceName = "services.loadbalancers"
	// CPU 请求，单位为核数（500m = 0.5 核）。
	ResourceRequestsCPU ResourceName = "requests.cpu"
	// 内存请求，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	ResourceRequestsMemory ResourceName = "requests.memory"
	// 存储请求，单位为字节。
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
	// 默认资源请求前缀。
	DefaultResourceRequestsPrefix = "requests."
)

// ResourceQuotaScope 定义每个被配额跟踪的对象必须匹配的过滤器。
type ResourceQuotaScope string

// 以下是资源配额规格的有效值。
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

	// 匹配所有指定了卷属性类（VolumeAttributesClass）的 PVC 对象。
	ResourceQuotaScopeVolumeAttributesClass ResourceQuotaScope = "VolumeAttributesClass"
)

// ResourceQuotaSpec 定义配额要强制执行的目标硬性限制。
type ResourceQuotaSpec struct {
	// hard 是各命名资源所期望的硬性限制集合。
	// +optional
	Hard ResourceList
	// Scopes 是一组过滤器，配额跟踪的每个对象都必须匹配。
	// 若未指定，则配额匹配所有对象。
	// +optional
	Scopes []ResourceQuotaScope
	// ScopeSelector 是与 Scopes 类似的一组过滤器，配额跟踪的每个对象都必须匹配，
	// 但它通过 ScopeSelectorOperator 与可能的取值组合来表达。
	// +optional
	ScopeSelector *ScopeSelector
}

// ScopeSelector 表示各作用域资源选择器条件所选选择器的 AND（与）。
type ScopeSelector struct {
	// 按资源作用域划分的作用域选择器要求列表。
	// +optional
	MatchExpressions []ScopedResourceSelectorRequirement
}

// ScopedResourceSelectorRequirement 是一个包含取值、作用域名称的选择器，
// 以及用于关联作用域名称与这些值的操作符。
type ScopedResourceSelectorRequirement struct {
	// 该选择器所适用作用域的名称。
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

// ScopeSelectorOperator 是作用域选择器要求中可使用的操作符集合。
type ScopeSelectorOperator string

// 以下是 ScopeSelectorOperator 的有效值。
const (
	ScopeSelectorOpIn           ScopeSelectorOperator = "In"
	ScopeSelectorOpNotIn        ScopeSelectorOperator = "NotIn"
	ScopeSelectorOpExists       ScopeSelectorOperator = "Exists"
	ScopeSelectorOpDoesNotExist ScopeSelectorOperator = "DoesNotExist"
)

// ResourceQuotaStatus 定义已强制执行的硬性限制和观测到的用量。
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

	// Spec 定义期望的配额。
	// +optional
	Spec ResourceQuotaSpec

	// Status 定义实际施加的配额及其当前使用量。
	// +optional
	Status ResourceQuotaStatus
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ResourceQuotaList 是 ResourceQuota 条目的列表。
type ResourceQuotaList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 ResourceQuota 对象的列表。
	Items []ResourceQuota
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// Secret 保存某一类型的机密数据。Data 字段中所有值的总字节数
// 必须小于 MaxSecretSize 字节。
type Secret struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Immutable 字段若被设置，确保存储在 Secret 中的数据无法被更新
	// （只能修改对象元数据）。
	// +optional
	Immutable *bool

	// Data 包含私密数据。每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 私密数据的序列化形式为 base64 编码字符串，表示此处任意（可能是非字符串）的数据值。
	// +optional
	Data map[string][]byte `datapolicy:"password,security-key,token"`

	// 用于便于以编程方式处理机密数据。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/secret/#secret-types
	// +optional
	Type SecretType
}

// MaxSecretSize 表示 Secret 的最大大小。
const MaxSecretSize = 1 * 1024 * 1024

// SecretType 定义 Secret 的类型。
type SecretType string

// 以下是 SecretType 的有效值。
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

	// SecretTypeDockerConfigJSON 包含一个 dockercfg 文件，其格式规则与 ~/.docker/config.json 相同
	//
	// 必填字段：
	// - Secret.Data[".dockerconfigjson"] - 序列化后的 ~/.docker/config.json 文件
	SecretTypeDockerConfigJSON SecretType = "kubernetes.io/dockerconfigjson"

	// DockerConfigJSONKey 是 SecretTypeDockerConfigJson 类型 Secret 必填数据的键。
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
	// SecretTypeBootstrapToken 用于自动化引导过程（最初由 kubeadm 实现）。
	// 它保存用于签名众所周知的 ConfigMaps 的令牌。
	// 这些令牌用于认证（authn）。
	SecretTypeBootstrapToken SecretType = "bootstrap.kubernetes.io/token"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// SecretList 表示 Secret 的列表。
type SecretList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	Items []Secret
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConfigMap 保存供组件或应用程序消费的配置数据。
type ConfigMap struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// Immutable 字段若被设置，确保存储在 ConfigMap 中的数据无法被更新
	// （只能修改对象元数据）。
	// +optional
	Immutable *bool

	// Data 包含配置数据。
	// 每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 含非 UTF-8 字节序列的值必须使用 BinaryData 字段。
	// Data 中的键不得与 BinaryData 字段中的键重叠，校验过程会强制检查这一点。
	// +optional
	Data map[string]string

	// BinaryData 包含二进制数据。
	// 每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// BinaryData 可以包含不在 UTF-8 范围内的字节序列。
	// BinaryData 中存储的键不得与 Data 字段中的键重叠，
	// 这在校验过程中会被强制执行。
	// 使用此字段需要 1.10+ 的 apiserver 和 kubelet。
	// +optional
	BinaryData map[string][]byte
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ConfigMapList 是包含 ConfigMap 对象列表的资源。
type ConfigMapList struct {
	metav1.TypeMeta
	// +optional
	metav1.ListMeta

	// Items 是 ConfigMap 的列表。
	Items []ConfigMap
}

// 这些常量用于远程命令执行和端口转发，
// 由客户端和服务端组件共同使用。
//
// 这里可能不是存放它们的理想位置，但仅为了放一个只包含常量的文件
// 而创建 pkg/exec 和 pkg/portforward 似乎并不值得。
// 非常欢迎提出更合适的替代方案建议！
const (
	// 为远程命令执行启用 stdin。
	ExecStdinParam = "input"
	// 为远程命令执行启用 stdout。
	ExecStdoutParam = "output"
	// 为远程命令执行启用 stderr。
	ExecStderrParam = "error"
	// 为远程命令执行启用 TTY。
	ExecTTYParam = "tty"
	// 远程命令执行时要运行的命令。
	ExecCommandParam = "command"

	// 指定流类型的请求头名称。
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
	// 指定请求 ID 的请求头名称，该请求 ID 用于将单个转发连接的
	// 错误流和数据流关联起来。
	PortForwardRequestIDHeader = "requestID"
)

// ComponentConditionType 定义组件健康校验的类型和常量。
type ComponentConditionType string

// 以下是组件的有效状态。
const (
	ComponentHealthy ComponentConditionType = "Healthy"
)

// ComponentCondition 表示组件的状况。
type ComponentCondition struct {
	Type   ComponentConditionType
	Status ConditionStatus
	// +optional
	Message string
	// +optional
	Error string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComponentStatus（以及 ComponentStatusList）保存集群校验信息。
// Deprecated: 此 API 在 v1.19+ 中已弃用。
type ComponentStatus struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta

	// +optional
	Conditions []ComponentCondition
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ComponentStatusList 表示组件状态的列表。
// Deprecated: 此 API 在 v1.19+ 中已弃用。
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
	// 只读根文件系统可以限制应用可写入文件的位置，
	// 确保持久数据只能写入挂载点。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	ReadOnlyRootFilesystem *bool
	// AllowPrivilegeEscalation 控制进程能否获得比其父进程更多的权限。
	// 这个布尔值直接控制在容器进程上是否设置 no_new_privs 标志。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AllowPrivilegeEscalation *bool
	// ProcMount 表示容器使用的 proc 挂载类型。
	// 默认值为 Default，即对只读路径和屏蔽路径使用容器运行时的默认设置。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
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

// ProcMountType 定义 proc 挂载的类型。
type ProcMountType string

const (
	// DefaultProcMount 对 /proc 的只读路径和屏蔽路径使用容器运行时的默认设置。
	// 大多数容器运行时会屏蔽 /proc 中的某些路径，以避免特殊设备或信息被意外暴露。
	DefaultProcMount ProcMountType = "Default"

	// UnmaskedProcMount 绕过容器运行时的默认屏蔽行为，确保容器中新建的 /proc
	// 保持原样，不做任何修改。
	UnmaskedProcMount ProcMountType = "Unmasked"
)

// SELinuxOptions 是要应用于容器的标签。
type SELinuxOptions struct {
	// SELinux 用户标签。
	// +optional
	User string
	// SELinux 角色标签。
	// +optional
	Role string
	// SELinux 类型标签。
	// +optional
	Type string
	// SELinux 级别标签。
	// +optional
	Level string
}

// WindowsSecurityContextOptions 包含 Windows 特有的选项和凭据。
type WindowsSecurityContextOptions struct {
	// GMSACredentialSpecName 是要使用的 GMSA 凭据规范的名称。
	// +optional
	GMSACredentialSpecName *string

	// GMSACredentialSpec 是 GMSA 准入 webhook
	// (https://github.com/kubernetes-sigs/windows-gmsa) 内联的凭据规范内容，
	// 该规范由 GMSACredentialSpecName 字段指定。
	// +optional
	GMSACredentialSpec *string

	// Windows 中用于运行容器进程入口点的 UserName。
	// 若未指定，则默认使用镜像元数据中指定的用户。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
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

// RangeAllocation 是一个不透明的 API 对象（不暴露给最终用户），可被持久化以记录
// 集群的全局分配状态。Range 和 Data 的模式是通用的：Range 应当是范围输入的字符串
// 表示（例如，对于 IP 分配可能是 CIDR），而 Data 是分配器能够理解的不透明块，
// 通常是二进制范围。消费者应使用注解记录额外信息（模式版本、数据编码提示）。
// 范围分配应当*始终*可以在任意时刻通过观测集群重新生成，
// 因此该对象的类型约束比大多数对象更弱。
type RangeAllocation struct {
	metav1.TypeMeta
	// +optional
	metav1.ObjectMeta
	// 表示资源范围唯一标签的字符串，例如 CIDR "10.0.0.0/8" 或端口范围
	// "10000-30000"。此处 Range 没有强模式约束。除非存在隐式的结束值，
	// 否则 Range 预期会定义起始和结束。
	Range string
	// 表示范围分配序列化状态的字节数组。关于数据类型或格式的额外说明应通过注解表示。
	// 对于 IP 分配，它被表示为从 Range 中 CIDR 的基地址开始的位数组，
	// 每一位表示一个已分配的地址（CIDR 10.0.0.0/8 上的第五位是 10.0.0.4）。
	Data []byte
}

const (
	// DefaultHardPodAffinitySymmetricWeight 是隐式 PreferredDuringScheduling 亲和规则的权重。
	//
	// RequiredDuringScheduling 亲和不是对称的，但每条 RequiredDuringScheduling 亲和规则
	// 都对应一条隐式的 PreferredDuringScheduling 亲和规则。
	// 当未指定调度器的 --hard-pod-affinity-weight 标志时，
	// DefaultHardPodAffinityWeight 定义隐式 PreferredDuringScheduling 亲和规则的权重。
	DefaultHardPodAffinitySymmetricWeight int32 = 1
)

// UnsatisfiableConstraintAction 定义约束无法满足时可采取的动作。
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
	// TopologyKey 是节点标签的键。具有此键且值相同的标签的节点
	// 被视为处于同一拓扑中。
	// 我们将每个 <key, value> 视为一个"桶"，并尝试将数量均衡的 Pod
	// 放入每个桶中。
	// 我们将 domain（域）定义为某个拓扑的一个实例。
	// 同时，我们将满足 nodeAffinityPolicy 和 nodeTaintsPolicy 要求的节点所在的域
	// 定义为合格域（eligible domain）。
	// 例如，如果 TopologyKey 是 "kubernetes.io/hostname"，每个 Node 都是该拓扑的一个域。
	// 如果 TopologyKey 是 "topology.kubernetes.io/zone"，每个可用区都是该拓扑的一个域。
	// 这是必填字段。
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
	// MatchLabelKeys 是一组 Pod 标签键，用于选择要计算分布的 Pod。
	// 这些键用于从新来的 Pod 标签中查找值，这些键值标签与 labelSelector 做 AND，
	// 以选择要为该新来 Pod 计算分布的现有 Pod 组。
	// 同一个键禁止同时出现在 MatchLabelKeys 和 LabelSelector 中。
	// 未设置 LabelSelector 时不能设置 MatchLabelKeys。
	// 新来 Pod 标签中不存在的键将被忽略。
	// 为 null 或空列表表示仅与 labelSelector 匹配。
	//
	// 这是一个 beta 字段，需要启用 MatchLabelKeysInPodTopologySpread 特性门控（默认启用）。
	// +listType=atomic
	// +optional
	MatchLabelKeys []string
}

// 以下是 PortStatus 的内置错误。
const (
	// MixedProtocolNotSupported 在 PortStatus 中表示云端提供商无法在负载均衡器上发布该端口，
	// 因为同一 LoadBalancer 类型 Service 上混合使用多种协议不受云端提供商支持。
	MixedProtocolNotSupported = "MixedProtocolNotSupported"
)

// PortStatus 表示 Service 端口的错误状态。
type PortStatus struct {
	// Port 是记录此处状态的 Service 端口的端口号。
	Port int32
	// Protocol 是此处记录其状态的 Service 端口的协议。
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
	// 必填：要使用的镜像或制品引用。
	// 行为与 pod.spec.containers[*].image 相同。
	// 拉取密钥的组装方式与容器镜像相同：依次查找节点凭据、ServiceAccount 的镜像拉取密钥，
	// 以及 Pod spec 中的镜像拉取密钥。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images
	// 此字段为可选，以便更高层的配置管理可以在 Deployments、StatefulSets 等
	// 工作负载控制器中设置容器镜像的默认值或覆盖其值。
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

// NodeAllocatableResourceClaimStatus 描述通过 DRA 分配的节点可分配资源的状态。
type NodeAllocatableResourceClaimStatus struct {
	// ResourceClaimName 是该 Pod 引用的、导致此次节点可分配资源分配的资源声明。
	// +required
	ResourceClaimName string
	// Containers 列出此 Pod 中引用该声明的所有容器名称。
	// +optional
	// +listType=set
	Containers []string
	// Mapping 包含通过 `device.nodeAllocatableResources.mapping` 中映射的设备进行的分配。
	// kubelet 使用它在 Pod 级别和容器级别强制执行 cgroup 限制。
	// +optional
	// +listType=map
	// +listMapKey=name
	Mapping []NodeAllocatableMappedResources
	// Overhead 包含通过 `device.nodeAllocatableResources.overhead` 中映射的设备进行的分配。
	// kubelet 使用它在 Pod 级别和容器级别强制执行 cgroup 限制。
	// +optional
	// +listType=map
	// +listMapKey=name
	Overhead []NodeAllocatableOverheadResources
}

// NodeAllocatableMappedResources 描述已映射的节点可分配资源分配。
type NodeAllocatableMappedResources struct {
	// Name 是资源名称（例如 cpu、memory）。
	// +required
	Name ResourceName
	// Quantity 是为该声明分配的节点可分配资源总容量。
	// 该声明分配到的设备由所有引用该声明的容器共享。
	// kubelet 将此值同时加到 Pod 级 cgroup 的 requests 和 limits 上，
	// 并加到每个引用该声明的容器的容器级 cgroup 的 limits 上。
	// +required
	Quantity *resource.Quantity
}

// NodeAllocatableOverheadResources 描述辅助开销资源的分配。
type NodeAllocatableOverheadResources struct {
	// Name 是资源名称（例如 cpu、memory）。
	// +required
	Name ResourceName
	// PerPod 是按 Pod 分配的固定开销数量。
	// 将其加入每个容器的 limits，可以让单个容器使用该开销，
	// 而父级 Pod 级 cgroup 的 limits 会在 Pod 边界处封顶总用量，
	// 在该边界上该开销只被计算一次。
	// PerPod 和 PerContainer 至少要指定一个。两者都不指定是无效配置。
	// +optional
	PerPod *resource.Quantity
	// PerContainer 是对每个引用该声明的容器应用的可变开销数量。
	// 容器引用记录在 `nodeAllocatableResourceClaimStatuses.containers` 中。
	// 为该声明分配的总开销数量计算方式为：
	// Quantity = PerPod + (PerContainer * NumReferences)
	// kubelet 在 cgroup 中计入此开销：
	// - Pod 级 cgroup（requests 和 limits）：kubelet 加上 PerPod + (PerContainer * NumReferences)。
	// - 容器级 cgroup（仅 limits）：对每个引用容器，kubelet 加上 PerPod + PerContainer。
	// 这使得任何单个容器都可以使用 Pod 级开销，
	// 而父级 cgroup 将总用量封顶，使 PerPod 恰好只被计算一次。
	// PerPod 和 PerContainer 至少要指定一个。两者都不指定是无效配置。
	// +optional
	PerContainer *resource.Quantity
}
