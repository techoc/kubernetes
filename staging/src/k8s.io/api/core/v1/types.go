/*
Copyright 2015 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
)

const (
	// NamespaceDefault 表示对象位于默认命名空间，客户端未指定时即采用该命名空间
	NamespaceDefault string = "default"
	// NamespaceAll 是在上下文中指定的默认参数，用于跨所有命名空间列出或过滤资源
	NamespaceAll string = ""
	// NamespaceNodeLease 是放置节点租约对象（用于节点心跳）的命名空间
	NamespaceNodeLease string = "kube-node-lease"
)

// Volume 表示 Pod 中的一个具名卷，Pod 内的任意容器均可访问它。
type Volume struct {
	// name 是卷的名称。
	// 必须是 DNS_LABEL，且在 Pod 内唯一。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// volumeSource 表示所挂载卷的位置和类型。
	// 若未指定，则隐含该 Volume 为 EmptyDir。
	// 此隐含行为已弃用，将在未来版本中移除。
	VolumeSource `json:"" protobuf:"bytes,2,opt,name=volumeSource"`
}

// VolumeSource 表示要挂载的卷的来源。
// 只能指定其中一个成员。
type VolumeSource struct {
	// hostPath 表示主机上已存在的文件或目录，直接暴露给容器。
	// 通常用于系统代理或其他被允许查看主机的特权场景。
	// 大多数容器不需要此字段。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#hostpath
	// ---
	// TODO(jonesdl) 需要限制谁可以使用主机目录挂载，以及谁可以/不可以
	// 以读写方式挂载主机目录。
	// +optional
	HostPath *HostPathVolumeSource `json:"hostPath,omitempty" protobuf:"bytes,1,opt,name=hostPath"`
	// emptyDir 表示与 Pod 生命周期相同的临时目录。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#emptydir
	// +optional
	EmptyDir *EmptyDirVolumeSource `json:"emptyDir,omitempty" protobuf:"bytes,2,opt,name=emptyDir"`
	// gcePersistentDisk 表示挂载到 kubelet 主机、再暴露给 Pod 的 GCE 磁盘资源。
	// 已弃用：GCEPersistentDisk 已弃用。树内 gcePersistentDisk 类型的所有操作
	// 都会重定向到 pd.csi.storage.gke.io CSI 驱动。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	// +optional
	GCEPersistentDisk *GCEPersistentDiskVolumeSource `json:"gcePersistentDisk,omitempty" protobuf:"bytes,3,opt,name=gcePersistentDisk"`
	// awsElasticBlockStore 表示挂载到 kubelet 主机、再暴露给 Pod 的 AWS 磁盘资源。
	// 已弃用：AWSElasticBlockStore 已弃用。树内 awsElasticBlockStore 类型的所有操作
	// 都会重定向到 ebs.csi.aws.com CSI 驱动。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
	// +optional
	AWSElasticBlockStore *AWSElasticBlockStoreVolumeSource `json:"awsElasticBlockStore,omitempty" protobuf:"bytes,4,opt,name=awsElasticBlockStore"`
	// gitRepo 表示处于特定版本的 git 仓库。
	// 已弃用：GitRepo 已弃用。若要为容器提供 git 仓库，可将一个 EmptyDir 挂载到
	// InitContainer，用 git 克隆仓库，然后再将该 EmptyDir 挂载到 Pod 的容器中。
	// +optional
	GitRepo *GitRepoVolumeSource `json:"gitRepo,omitempty" protobuf:"bytes,5,opt,name=gitRepo"`
	// secret 表示用于填充该卷的 secret。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#secret
	// +optional
	Secret *SecretVolumeSource `json:"secret,omitempty" protobuf:"bytes,6,opt,name=secret"`
	// nfs 表示主机上的 NFS 挂载，其生命周期与 Pod 相同。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#nfs
	// +optional
	NFS *NFSVolumeSource `json:"nfs,omitempty" protobuf:"bytes,7,opt,name=nfs"`
	// iscsi 表示挂载到 kubelet 主机、再暴露给 Pod 的 ISCSI 磁盘资源。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes/#iscsi
	// +optional
	ISCSI *ISCSIVolumeSource `json:"iscsi,omitempty" protobuf:"bytes,8,opt,name=iscsi"`
	// glusterfs 表示主机上与 Pod 生命周期相同的 Glusterfs 挂载。
	// 已弃用：Glusterfs 已弃用，不再支持树内 glusterfs 类型。
	// +optional
	Glusterfs *GlusterfsVolumeSource `json:"glusterfs,omitempty" protobuf:"bytes,9,opt,name=glusterfs"`
	// persistentVolumeClaimVolumeSource 表示对同一命名空间中 PersistentVolumeClaim 的引用。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	PersistentVolumeClaim *PersistentVolumeClaimVolumeSource `json:"persistentVolumeClaim,omitempty" protobuf:"bytes,10,opt,name=persistentVolumeClaim"`
	// rbd 表示主机上与 Pod 生命周期相同的 Rados Block Device 挂载。
	// 已弃用：RBD 已弃用，不再支持树内 rbd 类型。
	// +optional
	RBD *RBDVolumeSource `json:"rbd,omitempty" protobuf:"bytes,11,opt,name=rbd"`
	// flexVolume 表示通过基于 exec 的插件供给/挂载的通用卷资源。
	// 已弃用：FlexVolume 已弃用。建议改用 CSIDriver。
	// +optional
	FlexVolume *FlexVolumeSource `json:"flexVolume,omitempty" protobuf:"bytes,12,opt,name=flexVolume"`
	// cinder 表示挂载到 kubelet 主机的 cinder 卷。
	// 已弃用：Cinder 已弃用。树内 cinder 类型的所有操作
	// 都会重定向到 cinder.csi.openstack.org CSI 驱动。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	Cinder *CinderVolumeSource `json:"cinder,omitempty" protobuf:"bytes,13,opt,name=cinder"`
	// cephFS 表示主机上与 Pod 生命周期相同的 Ceph FS 挂载。
	// 已弃用：CephFS 已弃用，不再支持树内 cephfs 类型。
	// +optional
	CephFS *CephFSVolumeSource `json:"cephfs,omitempty" protobuf:"bytes,14,opt,name=cephfs"`
	// flocker 表示挂载到 kubelet 主机的 Flocker 卷。依赖 Flocker 控制服务正在运行。
	// 已弃用：Flocker 已弃用，不再支持树内 flocker 类型。
	// +optional
	Flocker *FlockerVolumeSource `json:"flocker,omitempty" protobuf:"bytes,15,opt,name=flocker"`
	// downwardAPI 表示用于填充该卷的、与 Pod 相关的 downward API 数据。
	// +optional
	DownwardAPI *DownwardAPIVolumeSource `json:"downwardAPI,omitempty" protobuf:"bytes,16,opt,name=downwardAPI"`
	// fc 表示挂载到 kubelet 主机、再暴露给 Pod 的 Fibre Channel 资源。
	// +optional
	FC *FCVolumeSource `json:"fc,omitempty" protobuf:"bytes,17,opt,name=fc"`
	// azureFile 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
	// 已弃用：AzureFile 已弃用。树内 azureFile 类型的所有操作
	// 都会重定向到 file.csi.azure.com CSI 驱动。
	// +optional
	AzureFile *AzureFileVolumeSource `json:"azureFile,omitempty" protobuf:"bytes,18,opt,name=azureFile"`
	// configMap 表示用于填充该卷的 configMap。
	// +optional
	ConfigMap *ConfigMapVolumeSource `json:"configMap,omitempty" protobuf:"bytes,19,opt,name=configMap"`
	// vsphereVolume 表示挂载到 kubelet 主机的 vSphere 卷。
	// 已弃用：VsphereVolume 已弃用。树内 vsphereVolume 类型的所有操作
	// 都会重定向到 csi.vsphere.vmware.com CSI 驱动。
	// +optional
	VsphereVolume *VsphereVirtualDiskVolumeSource `json:"vsphereVolume,omitempty" protobuf:"bytes,20,opt,name=vsphereVolume"`
	// quobyte 表示主机上与 Pod 生命周期相同的 Quobyte 挂载。
	// 已弃用：Quobyte 已弃用，不再支持树内 quobyte 类型。
	// +optional
	Quobyte *QuobyteVolumeSource `json:"quobyte,omitempty" protobuf:"bytes,21,opt,name=quobyte"`
	// azureDisk 表示主机上的 Azure 数据盘挂载，并绑定挂载到 Pod。
	// 已弃用：AzureDisk 已弃用。树内 azureDisk 类型的所有操作
	// 都会重定向到 disk.csi.azure.com CSI 驱动。
	// +optional
	AzureDisk *AzureDiskVolumeSource `json:"azureDisk,omitempty" protobuf:"bytes,22,opt,name=azureDisk"`
	// photonPersistentDisk 表示挂载到 kubelet 主机的 PhotonController 持久盘。
	// 已弃用：PhotonPersistentDisk 已弃用，不再支持树内 photonPersistentDisk 类型。
	PhotonPersistentDisk *PhotonPersistentDiskVolumeSource `json:"photonPersistentDisk,omitempty" protobuf:"bytes,23,opt,name=photonPersistentDisk"`
	// projected 将 secret、configmap 和 downward API 全部汇聚到一处。
	Projected *ProjectedVolumeSource `json:"projected,omitempty" protobuf:"bytes,26,opt,name=projected"`
	// portworxVolume 表示挂载到 kubelet 主机的 portworx 卷。
	// 已弃用：PortworxVolume 已弃用。树内 portworxVolume 类型的所有操作
	// 都会重定向到 pxd.portworx.com CSI 驱动。
	// +optional
	PortworxVolume *PortworxVolumeSource `json:"portworxVolume,omitempty" protobuf:"bytes,24,opt,name=portworxVolume"`
	// scaleIO 表示挂载到 Kubernetes 节点的 ScaleIO 持久卷。
	// 已弃用：ScaleIO 已弃用，不再支持树内 scaleIO 类型。
	// +optional
	ScaleIO *ScaleIOVolumeSource `json:"scaleIO,omitempty" protobuf:"bytes,25,opt,name=scaleIO"`
	// storageOS 表示挂载到 Kubernetes 节点的 StorageOS 卷。
	// 已弃用：StorageOS 已弃用，不再支持树内 storageos 类型。
	// +optional
	StorageOS *StorageOSVolumeSource `json:"storageos,omitempty" protobuf:"bytes,27,opt,name=storageos"`
	// csi（容器存储接口）表示由某些外部 CSI 驱动处理的临时存储。
	// +optional
	CSI *CSIVolumeSource `json:"csi,omitempty" protobuf:"bytes,28,opt,name=csi"`
	// ephemeral 表示由集群存储驱动处理的卷。
	// 卷的生命周期与定义它的 Pod 绑定：在 Pod 启动前创建，Pod 删除时一并删除。
	//
	// 适用情形：
	// a) 仅在 Pod 运行期间需要该卷，
	// b) 需要普通卷的特性，如从快照恢复或容量
	//    跟踪，
	// c) 通过 StorageClass 指定存储驱动，且
	// d) 该存储驱动支持通过 PersistentVolumeClaim 动态供给卷
	//    （关于此卷类型与 PersistentVolumeClaim 的关系，
	//    详见 EphemeralVolumeSource 的说明）。
	//
	// 对于生命周期长于单个 Pod 的卷，请使用 PersistentVolumeClaim 或各厂商特定的 API。
	//
	// 如果 CSI 驱动本就用于轻量本地临时卷，请使用 CSI —— 详见驱动文档
	// 了解更多信息。
	//
	// 一个 Pod 可以同时使用临时卷和持久卷这两种类型。
	//
	// +optional
	Ephemeral *EphemeralVolumeSource `json:"ephemeral,omitempty" protobuf:"bytes,29,opt,name=ephemeral"`
	// image 表示在 kubelet 主机上拉取并挂载的 OCI 对象（容器镜像或制品）。
	// 卷在 Pod 启动时根据所提供的 PullPolicy 值进行解析：
	//
	// - Always：kubelet 总是尝试拉取引用。拉取失败则容器创建失败。
	// - Never：kubelet 从不拉取引用，只使用本地镜像或制品。引用不存在则容器创建失败。
	// - IfNotPresent：若磁盘上尚无引用，kubelet 才会拉取。引用不存在且拉取失败则容器创建失败。
	//
	// 若 Pod 被删除并重建，卷会重新解析，因此重建 Pod 后可使用新的远端内容。
	// Pod 启动期间解析或拉取镜像失败会阻塞容器启动，并可能带来显著延迟。
	// 失败会按常规卷退避策略重试，并反映在 Pod 的 reason 和 message 中。
	// 该卷可挂载的对象类型由主机上的容器运行时实现定义，至少须包含 container image 字段支持的所有有效类型。
	// OCI 对象以与容器镜像相同的方式合并清单层，挂载到单个目录（spec.containers[*].volumeMounts.mountPath）中。
	// 卷以只读（ro）方式挂载。
	// 1.33 之前不支持容器的子路径挂载（spec.containers[*].volumeMounts.subpath）。
	// 字段 spec.securityContext.fsGroupChangePolicy 对此卷类型无效。
	// +optional
	Image *ImageVolumeSource `json:"image,omitempty" protobuf:"bytes,30,opt,name=image"`
}

// PersistentVolumeClaimVolumeSource 引用同一命名空间中用户的 PVC。
// 该卷会找到绑定的 PV 并为 Pod 挂载该卷。
// PersistentVolumeClaimVolumeSource 本质上是对另一种由他人（系统）拥有的卷类型的包装。
type PersistentVolumeClaimVolumeSource struct {
	// claimName 是与此卷所在 Pod 同一命名空间中的 PersistentVolumeClaim 的名称。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	ClaimName string `json:"claimName" protobuf:"bytes,1,opt,name=claimName"`
	// readOnly 将强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认为 false。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`
}

// PersistentVolumeSource 与 VolumeSource 类似，但面向创建 PV 的管理员。
// 必须且只能设置其中一个成员。
type PersistentVolumeSource struct {
	// gcePersistentDisk 表示挂载到 kubelet 主机、再暴露给 Pod 的 GCE 磁盘资源，由管理员供给。
	// 已弃用：GCEPersistentDisk 已弃用。树内 gcePersistentDisk 类型的所有操作
	// 都会重定向到 pd.csi.storage.gke.io CSI 驱动。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	// +optional
	GCEPersistentDisk *GCEPersistentDiskVolumeSource `json:"gcePersistentDisk,omitempty" protobuf:"bytes,1,opt,name=gcePersistentDisk"`
	// awsElasticBlockStore 表示挂载到 kubelet 主机、再暴露给 Pod 的 AWS 磁盘资源。
	// 已弃用：AWSElasticBlockStore 已弃用。树内 awsElasticBlockStore 类型的所有操作
	// 都会重定向到 ebs.csi.aws.com CSI 驱动。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
	// +optional
	AWSElasticBlockStore *AWSElasticBlockStoreVolumeSource `json:"awsElasticBlockStore,omitempty" protobuf:"bytes,2,opt,name=awsElasticBlockStore"`
	// hostPath 表示主机上的一个目录。
	// 由开发者或测试人员供给。
	// 仅适用于单节点开发和测试！
	// 完全不支持主机上存储，在多节点集群中无法工作。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#hostpath
	// +optional
	HostPath *HostPathVolumeSource `json:"hostPath,omitempty" protobuf:"bytes,3,opt,name=hostPath"`
	// glusterfs 表示挂载到主机、再暴露给 Pod 的 Glusterfs 卷，由管理员供给。
	// 已弃用：Glusterfs 已弃用，不再支持树内 glusterfs 类型。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md
	// +optional
	Glusterfs *GlusterfsPersistentVolumeSource `json:"glusterfs,omitempty" protobuf:"bytes,4,opt,name=glusterfs"`
	// nfs 表示主机上的 NFS 挂载，由管理员供给。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#nfs
	// +optional
	NFS *NFSVolumeSource `json:"nfs,omitempty" protobuf:"bytes,5,opt,name=nfs"`
	// rbd 表示主机上与 Pod 生命周期相同的 Rados Block Device 挂载。
	// 已弃用：RBD 已弃用，不再支持树内 rbd 类型。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md
	// +optional
	RBD *RBDPersistentVolumeSource `json:"rbd,omitempty" protobuf:"bytes,6,opt,name=rbd"`
	// iscsi 表示挂载到 kubelet 主机、再暴露给 Pod 的 ISCSI 磁盘资源，由管理员供给。
	// +optional
	ISCSI *ISCSIPersistentVolumeSource `json:"iscsi,omitempty" protobuf:"bytes,7,opt,name=iscsi"`
	// cinder 表示挂载到 kubelet 主机的 cinder 卷。
	// 已弃用：Cinder 已弃用。树内 cinder 类型的所有操作
	// 都会重定向到 cinder.csi.openstack.org CSI 驱动。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	Cinder *CinderPersistentVolumeSource `json:"cinder,omitempty" protobuf:"bytes,8,opt,name=cinder"`
	// cephFS 表示主机上与 Pod 生命周期相同的 Ceph FS 挂载。
	// 已弃用：CephFS 已弃用，不再支持树内 cephfs 类型。
	// +optional
	CephFS *CephFSPersistentVolumeSource `json:"cephfs,omitempty" protobuf:"bytes,9,opt,name=cephfs"`
	// fc 表示挂载到 kubelet 主机、再暴露给 Pod 的 Fibre Channel 资源。
	// +optional
	FC *FCVolumeSource `json:"fc,omitempty" protobuf:"bytes,10,opt,name=fc"`
	// flocker 表示挂载到 kubelet 主机、供 Pod 使用的 Flocker 卷。依赖 Flocker 控制服务正在运行。
	// 已弃用：Flocker 已弃用，不再支持树内 flocker 类型。
	// +optional
	Flocker *FlockerVolumeSource `json:"flocker,omitempty" protobuf:"bytes,11,opt,name=flocker"`
	// flexVolume 表示通过基于 exec 的插件供给/挂载的通用卷资源。
	// 已弃用：FlexVolume 已弃用。建议改用 CSIDriver。
	// +optional
	FlexVolume *FlexPersistentVolumeSource `json:"flexVolume,omitempty" protobuf:"bytes,12,opt,name=flexVolume"`
	// azureFile 表示主机上的 Azure File Service 挂载，并绑定挂载到 Pod。
	// 已弃用：AzureFile 已弃用。树内 azureFile 类型的所有操作
	// 都会重定向到 file.csi.azure.com CSI 驱动。
	// +optional
	AzureFile *AzureFilePersistentVolumeSource `json:"azureFile,omitempty" protobuf:"bytes,13,opt,name=azureFile"`
	// vsphereVolume 表示挂载到 kubelet 主机的 vSphere 卷。
	// 已弃用：VsphereVolume 已弃用。树内 vsphereVolume 类型的所有操作
	// 都会重定向到 csi.vsphere.vmware.com CSI 驱动。
	// +optional
	VsphereVolume *VsphereVirtualDiskVolumeSource `json:"vsphereVolume,omitempty" protobuf:"bytes,14,opt,name=vsphereVolume"`
	// quobyte 表示主机上与 Pod 生命周期相同的 Quobyte 挂载。
	// 已弃用：Quobyte 已弃用，不再支持树内 quobyte 类型。
	// +optional
	Quobyte *QuobyteVolumeSource `json:"quobyte,omitempty" protobuf:"bytes,15,opt,name=quobyte"`
	// azureDisk 表示主机上的 Azure 数据盘挂载，并绑定挂载到 Pod。
	// 已弃用：AzureDisk 已弃用。树内 azureDisk 类型的所有操作
	// 都会重定向到 disk.csi.azure.com CSI 驱动。
	// +optional
	AzureDisk *AzureDiskVolumeSource `json:"azureDisk,omitempty" protobuf:"bytes,16,opt,name=azureDisk"`
	// photonPersistentDisk 表示挂载到 kubelet 主机的 PhotonController 持久盘。
	// 已弃用：PhotonPersistentDisk 已弃用，不再支持树内 photonPersistentDisk 类型。
	PhotonPersistentDisk *PhotonPersistentDiskVolumeSource `json:"photonPersistentDisk,omitempty" protobuf:"bytes,17,opt,name=photonPersistentDisk"`
	// portworxVolume 表示挂载到 kubelet 主机的 portworx 卷。
	// 已弃用：PortworxVolume 已弃用。树内 portworxVolume 类型的所有操作
	// 都会重定向到 pxd.portworx.com CSI 驱动。
	// +optional
	PortworxVolume *PortworxVolumeSource `json:"portworxVolume,omitempty" protobuf:"bytes,18,opt,name=portworxVolume"`
	// scaleIO 表示挂载到 Kubernetes 节点的 ScaleIO 持久卷。
	// 已弃用：ScaleIO 已弃用，不再支持树内 scaleIO 类型。
	// +optional
	ScaleIO *ScaleIOPersistentVolumeSource `json:"scaleIO,omitempty" protobuf:"bytes,19,opt,name=scaleIO"`
	// local 表示带有节点亲和性的直接挂载存储。
	// +optional
	Local *LocalVolumeSource `json:"local,omitempty" protobuf:"bytes,20,opt,name=local"`
	// storageOS 表示挂载到 kubelet 主机并挂载进 Pod 的 StorageOS 卷。
	// 已弃用：StorageOS 已弃用，不再支持树内 storageos 类型。
	// 更多信息：https://examples.k8s.io/volumes/storageos/README.md
	// +optional
	StorageOS *StorageOSPersistentVolumeSource `json:"storageos,omitempty" protobuf:"bytes,21,opt,name=storageos"`
	// csi 表示由外部 CSI 驱动处理的存储。
	// +optional
	CSI *CSIPersistentVolumeSource `json:"csi,omitempty" protobuf:"bytes,22,opt,name=csi"`
}

const (
	// BetaStorageClassAnnotation 表示 beta/旧版 StorageClass 注解。
	// 目前仍在使用，并将保留以兼容旧版本。
	BetaStorageClassAnnotation = "volume.beta.kubernetes.io/storage-class"

	// MountOptionAnnotation 定义 PV 中使用的挂载选项注解。
	MountOptionAnnotation = "volume.beta.kubernetes.io/mount-options"
)

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PersistentVolume（PV）是由管理员供给的存储资源。
// 它类似于节点。
// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes
// +k8s:supportsSubresource="/status"
type PersistentVolume struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义集群所拥有的持久卷的规格。
	// 由管理员供给。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistent-volumes
	// +optional
	Spec PersistentVolumeSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// status 表示持久卷的当前信息/状态。
	// 由系统填充。
	// 只读。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistent-volumes
	// +optional
	Status PersistentVolumeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// PersistentVolumeSpec 是持久卷的规格。
type PersistentVolumeSpec struct {
	// capacity 描述持久卷的资源和容量。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#capacity
	// +optional
	Capacity ResourceList `json:"capacity,omitempty" protobuf:"bytes,1,rep,name=capacity,casttype=ResourceList,castkey=ResourceName"`
	// persistentVolumeSource 是支撑该持久卷的实际卷。
	PersistentVolumeSource `json:"" protobuf:"bytes,2,opt,name=persistentVolumeSource"`
	// accessModes 包含该卷可挂载的所有方式。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes
	// +optional
	// +listType=atomic
	AccessModes []PersistentVolumeAccessMode `json:"accessModes,omitempty" protobuf:"bytes,3,rep,name=accessModes,casttype=PersistentVolumeAccessMode"`
	// claimRef 是 PersistentVolume 与 PersistentVolumeClaim 之间双向绑定关系的一部分。
	// 绑定后应为非 nil。
	// claim.VolumeName 才是 PV 与 PVC 绑定的权威依据。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#binding
	// +optional
	// +structType=granular
	ClaimRef *ObjectReference `json:"claimRef,omitempty" protobuf:"bytes,4,opt,name=claimRef"`
	// persistentVolumeReclaimPolicy 定义持久卷从其声明释放后如何处理。
	// 有效选项为 Retain（手动创建的 PersistentVolume 的默认值）、Delete
	// （动态供给的 PersistentVolume 的默认值）和 Recycle（已弃用）。
	// Recycle 必须由此 PersistentVolume 底层的卷插件支持。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#reclaiming
	// +optional
	PersistentVolumeReclaimPolicy PersistentVolumeReclaimPolicy `json:"persistentVolumeReclaimPolicy,omitempty" protobuf:"bytes,5,opt,name=persistentVolumeReclaimPolicy,casttype=PersistentVolumeReclaimPolicy"`
	// storageClassName 是该持久卷所属 StorageClass 的名称。空值
	// 表示该卷不属于任何 StorageClass。
	// +optional
	StorageClassName string `json:"storageClassName,omitempty" protobuf:"bytes,6,opt,name=storageClassName"`
	// mountOptions 是挂载选项列表，例如 ["ro", "soft"]。不做校验——若某选项无效，
	// 挂载会直接失败。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes/#mount-options
	// +optional
	// +listType=atomic
	MountOptions []string `json:"mountOptions,omitempty" protobuf:"bytes,7,opt,name=mountOptions"`
	// volumeMode 定义卷是应使用格式化文件系统，还是保持原始块状态。
	// spec 中未包含时隐含为 Filesystem。
	// +optional
	VolumeMode *PersistentVolumeMode `json:"volumeMode,omitempty" protobuf:"bytes,8,opt,name=volumeMode,casttype=PersistentVolumeMode"`
	// nodeAffinity 定义限制可从哪些节点访问该卷的约束。
	// 该字段会影响使用此卷的 Pod 的调度。
	// 若启用 MutablePVNodeAffinity 特性门控，则该字段可变。
	// +optional
	NodeAffinity *VolumeNodeAffinity `json:"nodeAffinity,omitempty" protobuf:"bytes,9,opt,name=nodeAffinity"`
	// 该持久卷所属 VolumeAttributesClass 的名称，不允许为空值。
	// 未设置该字段表示此卷不属于任何 VolumeAttributesClass。
	// 该字段可变，CSI 驱动可在卷成功更新到新类别后对其进行修改。
	// 对于未绑定的 PersistentVolume，绑定时会将其 volumeAttributesClassName
	// 与未绑定的 PersistentVolumeClaim 进行匹配。
	// +featureGate=VolumeAttributesClass
	// +optional
	VolumeAttributesClassName *string `json:"volumeAttributesClassName,omitempty" protobuf:"bytes,10,opt,name=volumeAttributesClassName"`
}

// VolumeNodeAffinity 定义限制可从哪些节点访问该卷的约束。
type VolumeNodeAffinity struct {
	// required 指定必须满足的硬性节点约束。
	Required *NodeSelector `json:"required,omitempty" protobuf:"bytes,1,opt,name=required"`
}

// PersistentVolumeReclaimPolicy 描述持久卷生命周期结束时的回收策略。
// +enum
type PersistentVolumeReclaimPolicy string

const (
	// PersistentVolumeReclaimRecycle 表示卷从其声明释放后会被回收到未绑定持久卷池中。
	// 卷插件必须支持回收（Recycling）。
	PersistentVolumeReclaimRecycle PersistentVolumeReclaimPolicy = "Recycle"
	// PersistentVolumeReclaimDelete 表示卷从其声明释放后会从 Kubernetes 中删除。
	// 卷插件必须支持删除（Deletion）。
	PersistentVolumeReclaimDelete PersistentVolumeReclaimPolicy = "Delete"
	// PersistentVolumeReclaimRetain 表示卷保持当前阶段（Released），由管理员手动回收。
	// 默认策略为 Retain。
	PersistentVolumeReclaimRetain PersistentVolumeReclaimPolicy = "Retain"
)

// PersistentVolumeMode 描述卷的使用方式：Block 或 Filesystem。
// +enum
type PersistentVolumeMode string

const (
	// PersistentVolumeBlock 表示卷不会用文件系统格式化，而是保持为原始块设备。
	PersistentVolumeBlock PersistentVolumeMode = "Block"
	// PersistentVolumeFilesystem 表示卷将或已使用文件系统格式化。
	PersistentVolumeFilesystem PersistentVolumeMode = "Filesystem"
)

// PersistentVolumeStatus 是持久卷的当前状态。
type PersistentVolumeStatus struct {
	// phase 表示卷是否可用、已绑定到声明或已被声明释放。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#phase
	// +optional
	Phase PersistentVolumePhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=PersistentVolumePhase"`
	// message 是人类可读的消息，说明卷处于该状态的详细原因。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
	// reason 是简短的 CamelCase 字符串，描述任何故障，
	// 便于机器解析并在 CLI 中整洁展示。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// lastPhaseTransitionTime 是阶段发生转换的时间，
	// 每次卷阶段转换时都会自动重置为当前时间。
	// +optional
	LastPhaseTransitionTime *metav1.Time `json:"lastPhaseTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastPhaseTransitionTime"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PersistentVolumeList 是 PersistentVolume 条目的列表。
type PersistentVolumeList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是持久卷的列表。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes
	Items []PersistentVolume `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PersistentVolumeClaim 是用户对持久卷的请求和声明
// +k8s:supportsSubresource="/status"
type PersistentVolumeClaim struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// spec 定义 Pod 作者所请求卷的期望特性。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	Spec PersistentVolumeClaimSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// status 表示持久卷声明的当前信息/状态。
	// 只读。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	// +optional
	Status PersistentVolumeClaimStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PersistentVolumeClaimList 是 PersistentVolumeClaim 条目的列表。
type PersistentVolumeClaimList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// More info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`
	// items 是持久卷声明的列表。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#persistentvolumeclaims
	Items []PersistentVolumeClaim `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PersistentVolumeClaimSpec 描述存储设备的通用属性，并允许提供特定供给方的属性来源
type PersistentVolumeClaimSpec struct {
	// accessModes 包含卷应具有的期望访问模式。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes-1
	// +optional
	// +listType=atomic
	AccessModes []PersistentVolumeAccessMode `json:"accessModes,omitempty" protobuf:"bytes,1,rep,name=accessModes,casttype=PersistentVolumeAccessMode"`
	// selector 是对要考虑用于绑定的卷的标签查询。
	// +optional
	Selector *metav1.LabelSelector `json:"selector,omitempty" protobuf:"bytes,4,opt,name=selector"`
	// resources 表示卷应具有的最小资源。
	// 允许用户指定低于之前值的资源需求，但仍必须高于声明 status 字段中记录的容量。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#resources
	// +optional
	Resources VolumeResourceRequirements `json:"resources,omitempty" protobuf:"bytes,2,opt,name=resources"`
	// volumeName 是对支撑此声明的 PersistentVolume 的绑定引用。
	// +optional
	VolumeName string `json:"volumeName,omitempty" protobuf:"bytes,3,opt,name=volumeName"`
	// storageClassName 是声明所需的 StorageClass 名称。
	// More info: https://kubernetes.io/docs/concepts/storage/persistent-volumes#class-1
	// +optional
	StorageClassName *string `json:"storageClassName,omitempty" protobuf:"bytes,5,opt,name=storageClassName"`
	// volumeMode 定义声明所需的卷类型。
	// 声明 spec 中未包含时隐含为 Filesystem。
	// +optional
	VolumeMode *PersistentVolumeMode `json:"volumeMode,omitempty" protobuf:"bytes,6,opt,name=volumeMode,casttype=PersistentVolumeMode"`
	// dataSource 字段可用于指定以下之一：
	// * 现有的 VolumeSnapshot 对象（snapshot.storage.k8s.io/VolumeSnapshot）
	// * 现有的 PVC（PersistentVolumeClaim）
	// 如果供给器或外部控制器支持指定的数据源，它将基于该数据源的内容创建新卷。
	// dataSource 的内容会复制到 dataSourceRef；当 dataSourceRef.namespace 未指定时，
	// dataSourceRef 的内容也会复制到 dataSource。
	// 若指定了命名空间，则 dataSourceRef 不会复制到 dataSource。
	// +optional
	DataSource *TypedLocalObjectReference `json:"dataSource,omitempty" protobuf:"bytes,7,opt,name=dataSource"`
	// dataSourceRef 指定在需要非空卷时，用于向卷中填充数据的对象。它可以是任何非空 API 组中的对象（非核心对象），也可以是 PersistentVolumeClaim 对象。
	// 指定该字段时，只有当指定对象的类型与某个已安装的卷填充器（volume populator）或动态供给器匹配时，卷绑定才会成功。
	// 该字段将取代 dataSource 字段的功能，因此若两个字段都非空，其值必须相同。
	// 为向后兼容，当 dataSourceRef 中未指定命名空间时，如果其中一个字段为空而另一个非空，
	// 两个字段（dataSource 和 dataSourceRef）都会自动设为相同的值。
	// 当 dataSourceRef 中指定了命名空间时，dataSource 不会设为相同的值，且必须为空。
	// dataSource 与 dataSourceRef 有三个重要区别：
	// * dataSource 只允许两种特定类型的对象，而 dataSourceRef
	//   允许任何非核心对象以及 PersistentVolumeClaim 对象。
	// * dataSource 会忽略并丢弃不允许的值，而 dataSourceRef
	//   保留所有值，并在指定了不允许的值时产生错误。
	// * dataSource 只允许本地对象，dataSourceRef 允许任意命名空间中的对象。
	// (Alpha) 使用 dataSourceRef 的 namespace 字段需要启用 CrossNamespaceVolumeDataSource 特性门控。
	// +optional
	DataSourceRef *TypedObjectReference `json:"dataSourceRef,omitempty" protobuf:"bytes,8,opt,name=dataSourceRef"`
	// volumeAttributesClassName 可用于设置此声明使用的 VolumeAttributesClass。
	// 若指定，CSI 驱动会使用相应 VolumeAttributesClass 中定义的属性创建或更新卷。
	// 它的用途与 storageClassName 不同，可在声明创建后修改。空字符串或 nil 表示不会对该声明应用任何 VolumeAttributesClass。
	// 如果声明进入 Infeasible 错误状态，可将此字段重置为之前的值（包括 nil）以取消修改。
	// 如果 volumeAttributesClass 引用的资源不存在，此 PersistentVolumeClaim 会被置为 Pending 状态，
	// 如 modifyVolumeStatus 字段所示，直至该资源存在为止。
	// More info: https://kubernetes.io/docs/concepts/storage/volume-attributes-classes/
	// +featureGate=VolumeAttributesClass
	// +optional
	VolumeAttributesClassName *string `json:"volumeAttributesClassName,omitempty" protobuf:"bytes,9,opt,name=volumeAttributesClassName"`
}

// TypedObjectReference 包含足够的信息，可用于定位带类型信息的被引用对象
type TypedObjectReference struct {
	// APIGroup 是被引用资源所属的组。
	// 若未指定 APIGroup，则指定的 Kind 必须属于核心 API 组。
	// 对于其他任何第三方类型，APIGroup 为必填项。
	// +optional
	APIGroup *string `json:"apiGroup" protobuf:"bytes,1,opt,name=apiGroup"`
	// Kind 是被引用资源的类型
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// Name 是被引用资源的名称
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
	// Namespace 是被引用资源所在的命名空间
	// 注意，指定命名空间时，被引用命名空间中必须存在一个 gateway.networking.k8s.io/ReferenceGrant 对象，
	// 以允许该命名空间的所有者接受此引用。详情参见 ReferenceGrant 文档。
	// (Alpha) 此字段需要启用 CrossNamespaceVolumeDataSource 特性门控。
	// +featureGate=CrossNamespaceVolumeDataSource
	// +optional
	Namespace *string `json:"namespace,omitempty" protobuf:"bytes,4,opt,name=namespace"`
}

// PersistentVolumeClaimConditionType 定义 PVC 的条件类型。
// 有效值为：
//   - "Resizing"、"FileSystemResizePending"
//
// 还可能有以下额外值：
//   - "ControllerResizeError"、"NodeResizeError"
//
// 若启用 VolumeAttributesClass 特性门控，则可能还有以下额外值：
//   - "ModifyVolumeError"、"ModifyingVolume"
type PersistentVolumeClaimConditionType string

// 以下是 PVC 的有效条件
const (
	// PersistentVolumeClaimResizing 表示用户触发的 PVC 扩容已开始
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
	//
	// 由于处理延迟或集群中启用该功能的时间点不同，此条件所指示的使用时长和未使用时长
	// 可能短于或略长于实际的使用/未使用时长。
	//
	// 需要 PersistentVolumeClaimUnusedSinceTime beta 特性门控
	PersistentVolumeClaimUnused PersistentVolumeClaimConditionType = "Unused"
)

// ClaimResourceStatus 描述 PVC 中某个资源的状态。
// +enum
// 当控制器收到带有它无法识别的资源对应的 ClaimResourceStatus 的 PVC 更新时，
// 应忽略该更新，交由其他控制器处理。
type ClaimResourceStatus string

const (
	// 当调整容量控制器开始在控制平面扩容卷时设置的状态
	PersistentVolumeClaimControllerResizeInProgress ClaimResourceStatus = "ControllerResizeInProgress"

	// 当调整容量控制器因不可恢复的终结性错误而扩容失败时设置的状态。
	// 超时等瞬时错误不应设置此状态，也不应修改 allocatedResourceStatus，
	// 以便调整容量控制器可以继续卷扩容。
	PersistentVolumeClaimControllerResizeInfeasible ClaimResourceStatus = "ControllerResizeInfeasible"

	// 当调整容量控制器已完成卷扩容，但节点上仍需进一步扩容时设置的状态。
	PersistentVolumeClaimNodeResizePending ClaimResourceStatus = "NodeResizePending"
	// 当 kubelet 开始扩容卷时设置的状态
	PersistentVolumeClaimNodeResizeInProgress ClaimResourceStatus = "NodeResizeInProgress"
	// 当 kubelet 扩容因不可恢复的终结性错误而失败时设置的状态。瞬时错误不应设置此状态
	PersistentVolumeClaimNodeResizeInfeasible ClaimResourceStatus = "NodeResizeInfeasible"
)

// PersistentVolumeClaimModifyVolumeStatus 描述 PVC 修改卷操作的状态。
// +enum
// 未来可能新增状态。消费方应检查未知状态并作适当处理
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
	TargetVolumeAttributesClassName string `json:"targetVolumeAttributesClassName,omitempty" protobuf:"bytes,1,opt,name=targetVolumeAttributesClassName"`
	// status 是 ControllerModifyVolume 操作的状态，可以是以下任一状态：
	//  - Pending
	//    Pending 表示因未满足条件（例如指定的 VolumeAttributesClass 不存在），PersistentVolumeClaim 无法被修改。
	//  - InProgress
	//    InProgress 表示卷正在被修改。
	//  - Infeasible
	//   Infeasible 表示请求被 CSI 驱动判定为无效而拒绝。要解决该错误，需指定有效的 VolumeAttributesClass。
	// 注意：未来可能新增状态。消费方应检查未知状态并作适当处理。
	Status PersistentVolumeClaimModifyVolumeStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=PersistentVolumeClaimModifyVolumeStatus"`
}

// PersistentVolumeClaimCondition 包含有关 PVC 状态的详细信息
type PersistentVolumeClaimCondition struct {
	// Type 是条件的类型。
	// 更多信息：https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#:~:text=set%20to%20%27ResizeStarted%27.-,PersistentVolumeClaimCondition,-contains%20details%20about
	Type PersistentVolumeClaimConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=PersistentVolumeClaimConditionType"`
	// Status 是条件的状态。
	// 可以是 True、False、Unknown。
	// 更多信息：https://kubernetes.io/docs/reference/kubernetes-api/config-and-storage-resources/persistent-volume-claim-v1/#:~:text=state%20of%20pvc-,conditions.status,-(string)%2C%20required
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// lastProbeTime 是我们探测该条件的时间。
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// lastTransitionTime 是条件从一种状态转换为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// reason 是唯一的、简短且机器可理解的标志，说明条件上次状态转换的原因。
	// 若为 "Resizing"，表示底层持久卷正在扩容。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// message 是人类可读的消息，描述上次状态转换的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// VolumeHealthStatusType 描述卷的健康状态类别。
// +enum
// +k8s:enum
type VolumeHealthStatusType string

const (
	// VolumeHealthInaccessible 表示无法访问该卷。
	VolumeHealthInaccessible VolumeHealthStatusType = "Inaccessible"
	// VolumeHealthDataLoss 表示检测到该卷发生数据丢失。
	VolumeHealthDataLoss VolumeHealthStatusType = "DataLoss"
	// VolumeHealthDegraded 表示卷仍可工作，但能力有所下降。
	VolumeHealthDegraded VolumeHealthStatusType = "Degraded"
)

// VolumeHealthCondition 表示针对某个卷报告的不利健康状况。
type VolumeHealthCondition struct {
	// status 是机器可解析的健康类别。
	// 可能的值：
	// - "Inaccessible"：无法访问该卷。
	// - "DataLoss"：检测到该卷发生数据丢失。
	// - "Degraded"：卷仍可工作，但能力有所下降。
	// +required
	// +k8s:required
	Status VolumeHealthStatusType `json:"status" protobuf:"bytes,1,opt,name=status,casttype=VolumeHealthStatusType"`
	// reason 是简短的、机器可解析的 CamelCase 原因。
	// 与 status 一起构成某个条件条目的唯一标识。
	// reason 的最大允许长度为 256 字节。
	// +required
	// +k8s:required
	// +k8s:maxBytes=256
	Reason string `json:"reason" protobuf:"bytes,2,opt,name=reason"`
	// message 是人类可读的描述。
	// message 的最大允许长度为 1024 字节。
	// +optional
	// +k8s:optional
	// +k8s:maxBytes=1024
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
}

// VolumeHealthStatus 包含 CSI 控制器插件报告的卷健康信息。
type VolumeHealthStatus struct {
	// conditions 是 CSI 控制器插件报告的不利条件集合。空列表表示没有不利条件。
	// 最多可报告 16 个条件。
	// +optional
	// +listType=map
	// +listMapKey=status
	// +patchMergeKey=status
	// +patchStrategy=merge
	// +listMapKey=reason
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=status
	// +k8s:listMapKey=reason
	// +k8s:maxItems=16
	HealthConditions []VolumeHealthCondition `json:"healthConditions,omitempty" patchStrategy:"merge" patchMergeKey:"status" protobuf:"bytes,1,rep,name=healthConditions"`
	// lastTransitionTime 是当前条件集合首次出现的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,2,opt,name=lastTransitionTime"`
}

// PodVolumeHealth 包含 CSI 节点插件通过 kubelet 报告的、Pod 所用卷的健康信息。
type PodVolumeHealth struct {
	// name 与 pod.spec.volumes 中的条目匹配。
	// +required
	// +k8s:required
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// conditions 是 CSI 节点插件针对此节点上该卷报告的不利条件集合。
	// 最多可报告 16 个条件。
	// +optional
	// +listType=map
	// +listMapKey=status
	// +patchMergeKey=status
	// +patchStrategy=merge
	// +listMapKey=reason
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=status
	// +k8s:listMapKey=reason
	// +k8s:maxItems=16
	HealthConditions []VolumeHealthCondition `json:"healthConditions,omitempty" patchStrategy:"merge" patchMergeKey:"status" protobuf:"bytes,2,rep,name=healthConditions"`
	// lastTransitionTime 是当前条件集合首次出现的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
}

// PersistentVolumeClaimStatus 是持久卷声明的当前状态。
type PersistentVolumeClaimStatus struct {
	// phase 表示 PersistentVolumeClaim 的当前阶段。
	// +optional
	Phase PersistentVolumeClaimPhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=PersistentVolumeClaimPhase"`
	// accessModes 包含支撑 PVC 的卷的实际访问模式。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/persistent-volumes#access-modes-1
	// +optional
	// +listType=atomic
	AccessModes []PersistentVolumeAccessMode `json:"accessModes,omitempty" protobuf:"bytes,2,rep,name=accessModes,casttype=PersistentVolumeAccessMode"`
	// capacity 表示底层卷的实际资源。
	// +optional
	Capacity ResourceList `json:"capacity,omitempty" protobuf:"bytes,3,rep,name=capacity,casttype=ResourceList,castkey=ResourceName"`
	// conditions 是持久卷声明的当前条件。如果底层持久卷正在扩容，则条件会设为 'Resizing'。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []PersistentVolumeClaimCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,4,rep,name=conditions"`
	// allocatedResources 记录分配给 PVC 的资源（包括容量）。
	// 键名遵循标准 Kubernetes 标签语法。有效值为：
	// 	* 无前缀的键：
	//		- storage - 卷的容量。
	//	* 自定义资源必须使用实现自定义的带前缀名称，例如 "example.com/my-custom-resource"
	// 除上述值外，无前缀或带 kubernetes.io 前缀的键均视为保留键，不得使用。
	//
	// 请求扩容时，此处报告的容量可能大于实际容量。
	// 对于存储配额，使用 allocatedResources 与 PVC.spec.resources 中较大的值。
	// 若未设置 allocatedResources，则配额计算仅使用 PVC.spec.resources。
	// 若下调扩容容量请求，只有在没有进行中的扩容操作且实际卷容量等于或低于请求容量时，
	// allocatedResources 才会下调。
	//
	// 收到带有此前未知 resourceName 的 PVC 更新的控制器，应出于自身设计目的忽略该更新。例如 -
	// 只负责卷容量扩容的控制器，应忽略更改 PVC 关联的其他有效资源的 PVC 更新。
	// +optional
	AllocatedResources ResourceList `json:"allocatedResources,omitempty" protobuf:"bytes,5,rep,name=allocatedResources,casttype=ResourceList,castkey=ResourceName"`

	// resizestatus 已废弃，因为该字段已被 allocatedResourceStatus 取代
	// ResizeStatus *PersistentVolumeClaimResizeStatus `json:"resizeStatus,omitempty" protobuf:"bytes,6,opt,name=resizeStatus,casttype=PersistentVolumeClaimResizeStatus"`

	// allocatedResourceStatuses 存储给定 PVC 上正在扩容的资源的调整容量状态。
	// 键名遵循标准 Kubernetes 标签语法。有效值为：
	// 	* 无前缀的键：
	//		- storage - 卷的容量。
	//	* 自定义资源必须使用实现自定义的带前缀名称，例如 "example.com/my-custom-resource"
	// 除上述值外，无前缀或带 kubernetes.io 前缀的键均视为保留键，不得使用。
	//
	// ClaimResourceStatus 可以是以下任一状态：
	//	- ControllerResizeInProgress:
	//		当调整容量控制器开始在控制平面扩容卷时设置的状态。
	// 	- ControllerResizeFailed:
	//		当调整容量控制器因终结性错误而扩容失败时设置的状态。
	//	- NodeResizePending:
	//		当调整容量控制器已完成卷扩容，但节点上仍需进一步扩容时设置的状态。
	//	- NodeResizeInProgress:
	//		当 kubelet 开始扩容卷时设置的状态。
	//	- NodeResizeFailed:
	//		当 kubelet 扩容因终结性错误而失败时设置的状态。瞬时错误不会设置 NodeResizeFailed。
	// 例如：扩容 PVC 以获得更多容量时 - 此字段可以是以下任一状态：
	// 	- pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "ControllerResizeFailed"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizePending"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeInProgress"
	//      - pvc.status.allocatedResourceStatus['storage'] = "NodeResizeFailed"
	// 未设置此字段时，表示给定 PVC 没有正在进行的扩容操作。
	//
	// 收到带有此前未知 resourceName 或 ClaimResourceStatus 的 PVC 更新的控制器，应出于自身设计目的忽略该更新。例如 -
	// 只负责卷容量扩容的控制器，应忽略更改 PVC 关联的其他有效资源的 PVC 更新。
	// +mapType=granular
	// +optional
	AllocatedResourceStatuses map[ResourceName]ClaimResourceStatus `json:"allocatedResourceStatuses,omitempty" protobuf:"bytes,7,rep,name=allocatedResourceStatuses"`
	// currentVolumeAttributesClassName 是 PVC 当前使用的 VolumeAttributesClass 名称。
	// 未设置时，表示此 PersistentVolumeClaim 未应用任何 VolumeAttributeClass
	// +featureGate=VolumeAttributesClass
	// +optional
	CurrentVolumeAttributesClassName *string `json:"currentVolumeAttributesClassName,omitempty" protobuf:"bytes,8,opt,name=currentVolumeAttributesClassName"`
	// ModifyVolumeStatus 表示 ControllerModifyVolume 操作的状态对象。
	// 未设置时，表示没有正在尝试的 ModifyVolume 操作。
	// +featureGate=VolumeAttributesClass
	// +optional
	ModifyVolumeStatus *ModifyVolumeStatus `json:"modifyVolumeStatus,omitempty" protobuf:"bytes,9,opt,name=modifyVolumeStatus"`
	// healthStatus 包含控制器报告的最新健康信息，针对绑定到此声明的卷。
	// +featureGate=CSIVolumeHealth
	// +optional
	// +k8s:optional
	HealthStatus *VolumeHealthStatus `json:"healthStatus,omitempty" protobuf:"bytes,10,opt,name=healthStatus"`
}

// PersistentVolumeAccessMode 描述持久卷的访问模式。
// +enum
type PersistentVolumeAccessMode string

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

// PersistentVolumePhase 描述持久卷的阶段。
// +enum
type PersistentVolumePhase string

const (
	// 用于不可用的持久卷
	VolumePending PersistentVolumePhase = "Pending"
	// 用于尚未绑定的持久卷
	// 可用卷由绑定器持有，并与 PersistentVolumeClaim 匹配
	VolumeAvailable PersistentVolumePhase = "Available"
	// 用于已绑定的持久卷
	VolumeBound PersistentVolumePhase = "Bound"
	// 用于所绑定的 PersistentVolumeClaim 已被删除的持久卷
	// 释放的卷必须先被回收，才能再次变为可用
	// 持久卷声明绑定器使用此阶段通知另一个进程回收该资源
	VolumeReleased PersistentVolumePhase = "Released"
	// 用于从声明释放后未能正确回收或删除的持久卷
	VolumeFailed PersistentVolumePhase = "Failed"
)

// PersistentVolumeClaimPhase 描述持久卷声明的阶段。
// +enum
type PersistentVolumeClaimPhase string

const (
	// 用于尚未绑定的持久卷声明
	ClaimPending PersistentVolumeClaimPhase = "Pending"
	// 用于已绑定的持久卷声明
	ClaimBound PersistentVolumeClaimPhase = "Bound"
	// 用于丢失底层 PersistentVolume 的持久卷声明。
	// 该声明曾绑定到一个 PersistentVolume，
	// 但此卷现已不存在，且其上所有数据均已丢失。
	ClaimLost PersistentVolumeClaimPhase = "Lost"
)

// HostPathType 描述 HostPath 卷路径所期望的条目类型。
// +enum
type HostPathType string

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

// 表示映射到 Pod 中的主机路径。
// 主机路径卷不支持所有权管理或 SELinux 重标记。
type HostPathVolumeSource struct {
	// path 是主机上目录的路径。
	// 若路径是符号链接，将跟随链接指向真实路径。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#hostpath
	Path string `json:"path" protobuf:"bytes,1,opt,name=path"`
	// type 是 HostPath 卷的类型
	// 默认为 ""
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#hostpath
	// +optional
	Type *HostPathType `json:"type,omitempty" protobuf:"bytes,2,opt,name=type"`
}

// 表示 Pod 的空目录。
// 空目录卷支持所有权管理和 SELinux 重标记。
type EmptyDirVolumeSource struct {
	// medium 表示支撑此目录的存储介质类型。
	// 默认为 ""，表示使用节点的默认介质。
	// 必须是空字符串（默认）或 Memory。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#emptydir
	// +optional
	Medium StorageMedium `json:"medium,omitempty" protobuf:"bytes,1,opt,name=medium,casttype=StorageMedium"`
	// sizeLimit 是此 EmptyDir 卷所需本地存储的总量。
	// 大小限制同样适用于内存介质。
	// 内存介质 EmptyDir 的最大使用量为此处指定的 SizeLimit
	// 与 Pod 中所有容器内存限制之和两者中的较小值。
	// 默认为 nil，表示未定义限制。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#emptydir
	// +optional
	SizeLimit *resource.Quantity `json:"sizeLimit,omitempty" protobuf:"bytes,2,opt,name=sizeLimit"`
	// mode 以数字表示法指定 emptyDir 目录的权限位（例如 0755、01777）。取值必须在 0000 到 01777 之间。
	// 未指定时默认为 0777。
	// 这可能与其他影响文件模式的选项（如 fsGroup）冲突。若指定了 fsGroup，其权限会覆盖此处指定的 mode。
	// 此字段在 Windows 上无效。
	// 此字段为 alpha 特性，需要启用 EmptyDirVolumeMode 特性门控。
	// +featureGate=EmptyDirVolumeMode
	// +optional
	Mode *int32 `json:"mode,omitempty" protobuf:"varint,3,opt,name=mode"`
}

// 表示与 Pod 生命周期相同的 Glusterfs 挂载。
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsVolumeSource struct {
	// endpoints 是描述 Glusterfs 拓扑的 endpoint 名称。
	EndpointsName string `json:"endpoints" protobuf:"bytes,1,opt,name=endpoints"`

	// path 是 Glusterfs 卷路径。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	Path string `json:"path" protobuf:"bytes,2,opt,name=path"`

	// readOnly 会强制以只读权限挂载 Glusterfs 卷。
	// 默认为 false。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
}

// 表示与 Pod 生命周期相同的 Glusterfs 挂载。
// Glusterfs 卷不支持所有权管理或 SELinux 重标记。
type GlusterfsPersistentVolumeSource struct {
	// endpoints 是描述 Glusterfs 拓扑的 endpoint 名称。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	EndpointsName string `json:"endpoints" protobuf:"bytes,1,opt,name=endpoints"`

	// path 是 Glusterfs 卷路径。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	Path string `json:"path" protobuf:"bytes,2,opt,name=path"`

	// readOnly 会强制以只读权限挂载 Glusterfs 卷。
	// 默认为 false。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`

	// endpointsNamespace 是包含 Glusterfs endpoint 的命名空间。
	// 若此字段为空，EndpointNamespace 默认为与绑定的 PVC 相同的命名空间。
	// 更多信息：https://examples.k8s.io/volumes/glusterfs/README.md#create-a-pod
	// +optional
	EndpointsNamespace *string `json:"endpointsNamespace,omitempty" protobuf:"bytes,4,opt,name=endpointsNamespace"`
}

// 表示与 Pod 生命周期相同的 Rados Block Device 挂载。
// RBD 卷支持所有权管理和 SELinux 重标记。
type RBDVolumeSource struct {
	// monitors 是 Ceph monitor 的集合。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +listType=atomic
	CephMonitors []string `json:"monitors" protobuf:"bytes,1,rep,name=monitors"`
	// image 是 rados 镜像名称。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	RBDImage string `json:"image" protobuf:"bytes,2,opt,name=image"`
	// fsType 是要挂载卷的文件系统类型。
	// 提示：请确保主机操作系统支持该文件系统类型。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时隐含为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#rbd
	// TODO: 如何防止文件系统中的错误危害机器
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`
	// pool 是 rados 池名称。
	// 默认为 rbd。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="rbd"
	RBDPool string `json:"pool,omitempty" protobuf:"bytes,4,opt,name=pool"`
	// user 是 rados 用户名。
	// 默认为 admin。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="admin"
	RadosUser string `json:"user,omitempty" protobuf:"bytes,5,opt,name=user"`
	// keyring 是 RBDUser 的密钥环路径。
	// 默认为 /etc/ceph/keyring。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="/etc/ceph/keyring"
	Keyring string `json:"keyring,omitempty" protobuf:"bytes,6,opt,name=keyring"`
	// secretRef 是 RBDUser 的认证 secret 名称。若提供，将覆盖 keyring。
	// 默认为 nil。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,7,opt,name=secretRef"`
	// readOnly 会强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认为 false。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,8,opt,name=readOnly"`
}

// 表示与 Pod 生命周期相同的 Rados Block Device 挂载。
// RBD 卷支持所有权管理和 SELinux 重新标记。
type RBDPersistentVolumeSource struct {
	// monitors 是 Ceph monitor 的集合。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +listType=atomic
	CephMonitors []string `json:"monitors" protobuf:"bytes,1,rep,name=monitors"`
	// image 是 rados 镜像名称。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	RBDImage string `json:"image" protobuf:"bytes,2,opt,name=image"`
	// fsType 是要挂载的卷的文件系统类型。
	// 提示：请确保该文件系统类型受宿主机操作系统支持。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#rbd
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`
	// pool 是 rados 存储池名称。
	// 默认值为 rbd。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="rbd"
	RBDPool string `json:"pool,omitempty" protobuf:"bytes,4,opt,name=pool"`
	// user 是 rados 用户名。
	// 默认值为 admin。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="admin"
	RadosUser string `json:"user,omitempty" protobuf:"bytes,5,opt,name=user"`
	// keyring 是 RBDUser 的密钥环路径。
	// 默认值为 /etc/ceph/keyring。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	// +default="/etc/ceph/keyring"
	Keyring string `json:"keyring,omitempty" protobuf:"bytes,6,opt,name=keyring"`
	// secretRef 是 RBDUser 的认证 Secret 名称。若提供，
	// 将覆盖 keyring。
	// 默认值为 nil。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty" protobuf:"bytes,7,opt,name=secretRef"`
	// 此处的 readOnly 将强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认值为 false。
	// 更多信息：https://examples.k8s.io/volumes/rbd/README.md#how-to-use-it
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,8,opt,name=readOnly"`
}

// 表示 Openstack 中的 Cinder 卷资源。
// Cinder 卷必须先存在，才能挂载到容器。
// 该卷还必须与 kubelet 位于同一区域。
// Cinder 卷支持所有权管理和 SELinux 重新标记。
type CinderVolumeSource struct {
	// volumeID 用于在 Cinder 中标识该卷。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	VolumeID string `json:"volumeID" protobuf:"bytes,1,opt,name=volumeID"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
	// secretRef 为可选项：指向包含连接 OpenStack 所用参数的 Secret 对象。
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,4,opt,name=secretRef"`
}

// 表示 Openstack 中的 Cinder 卷资源。
// Cinder 卷必须先存在，才能挂载到容器。
// 该卷还必须与 kubelet 位于同一区域。
// Cinder 卷支持所有权管理和 SELinux 重新标记。
type CinderPersistentVolumeSource struct {
	// volumeID 用于在 Cinder 中标识该卷。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	VolumeID string `json:"volumeID" protobuf:"bytes,1,opt,name=volumeID"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// 更多信息：https://examples.k8s.io/mysql-cinder-pd/README.md
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
	// secretRef 为可选项：指向包含连接 OpenStack 所用参数的 Secret 对象。
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty" protobuf:"bytes,4,opt,name=secretRef"`
}

// 表示持续整个 Pod 生命周期的 Ceph 文件系统挂载。
// CephFS 卷不支持所有权管理或 SELinux 重新标记。
type CephFSVolumeSource struct {
	// monitors 为必填项：Ceph monitor 的集合。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +listType=atomic
	Monitors []string `json:"monitors" protobuf:"bytes,1,rep,name=monitors"`
	// path 为可选项：用作挂载根目录，而非整个 Ceph 目录树，默认值为 /。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,2,opt,name=path"`
	// user 为可选项：rados 用户名，默认值为 admin。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,3,opt,name=user"`
	// secretFile 为可选项：User 的密钥环路径，默认值为 /etc/ceph/user.secret。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	SecretFile string `json:"secretFile,omitempty" protobuf:"bytes,4,opt,name=secretFile"`
	// secretRef 为可选项：指向 User 的认证 Secret 的引用，默认为空。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,5,opt,name=secretRef"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,6,opt,name=readOnly"`
}

// SecretReference 表示一个 Secret 引用。它包含足够的信息，可在任意命名空间中检索 Secret。
// +structType=atomic
type SecretReference struct {
	// name 在命名空间内唯一，用于引用 Secret 资源。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	// namespace 定义了 Secret 名称必须唯一的作用域。
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,opt,name=namespace"`
}

// 表示持续整个 Pod 生命周期的 Ceph 文件系统挂载。
// CephFS 卷不支持所有权管理或 SELinux 重新标记。
type CephFSPersistentVolumeSource struct {
	// monitors 为必填项：Ceph monitor 的集合。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +listType=atomic
	Monitors []string `json:"monitors" protobuf:"bytes,1,rep,name=monitors"`
	// path 为可选项：用作挂载根目录，而非整个 Ceph 目录树，默认值为 /。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,2,opt,name=path"`
	// user 为可选项：rados 用户名，默认值为 admin。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,3,opt,name=user"`
	// secretFile 为可选项：User 的密钥环路径，默认值为 /etc/ceph/user.secret。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	SecretFile string `json:"secretFile,omitempty" protobuf:"bytes,4,opt,name=secretFile"`
	// secretRef 为可选项：指向 User 的认证 Secret 的引用，默认为空。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty" protobuf:"bytes,5,opt,name=secretRef"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// 更多信息：https://examples.k8s.io/volumes/cephfs/README.md#how-to-use-it
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,6,opt,name=readOnly"`
}

// 表示由 Flocker agent 挂载的 Flocker 卷。
// datasetName 与 datasetUUID 必须且只能设置其中一个。
// Flocker 卷不支持所有权管理或 SELinux 重新标记。
type FlockerVolumeSource struct {
	// datasetName 是数据集名称，在 Flocker 中存储为数据集的 metadata -> name，
	// 应视为已弃用。
	// +optional
	DatasetName string `json:"datasetName,omitempty" protobuf:"bytes,1,opt,name=datasetName"`
	// datasetUUID 是数据集的 UUID，即 Flocker 数据集的唯一标识符。
	// +optional
	DatasetUUID string `json:"datasetUUID,omitempty" protobuf:"bytes,2,opt,name=datasetUUID"`
}

// StorageMedium 定义卷的存储分配方式。
type StorageMedium string

const (
	StorageMediumDefault         StorageMedium = ""           // 使用节点的默认介质；未显式处理的情况均按此处理
	StorageMediumMemory          StorageMedium = "Memory"     // 使用内存（例如 Linux 上的 tmpfs）
	StorageMediumHugePages       StorageMedium = "HugePages"  // 使用大页内存
	StorageMediumHugePagesPrefix StorageMedium = "HugePages-" // 完整介质表示法 HugePages-<size> 的前缀
)

// Protocol 定义容器端口等所使用的网络协议。
// +enum
type Protocol string

const (
	// ProtocolTCP 表示 TCP 协议。
	ProtocolTCP Protocol = "TCP"
	// ProtocolUDP 表示 UDP 协议。
	ProtocolUDP Protocol = "UDP"
	// ProtocolSCTP 表示 SCTP 协议。
	ProtocolSCTP Protocol = "SCTP"
)

// 表示 Google Compute Engine 中的永久磁盘（Persistent Disk）资源。
//
// GCE PD 必须先存在，才能挂载到容器。该磁盘还必须与 kubelet 位于同一 GCE 项目和可用区。
// GCE PD 只能以读写方式挂载一次，或以只读方式挂载多次。
// GCE PD 支持所有权管理和 SELinux 重新标记。
type GCEPersistentDiskVolumeSource struct {
	// pdName 是 GCE 中 PD 资源的唯一名称，用于标识 GCE 中的磁盘。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	PDName string `json:"pdName" protobuf:"bytes,1,opt,name=pdName"`
	// fsType 是要挂载的卷的文件系统类型。
	// 提示：请确保该文件系统类型受宿主机操作系统支持。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// partition 是要挂载的卷中的分区。
	// 若省略，默认按卷名挂载。
	// 示例：对于卷 /dev/sda1，分区指定为 "1"。
	// 类似地，/dev/sda 的卷分区为 "0"（也可以留空该字段）。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	// +optional
	Partition int32 `json:"partition,omitempty" protobuf:"varint,3,opt,name=partition"`
	// 此处的 readOnly 将强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认值为 false。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#gcepersistentdisk
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
}

// 表示持续整个 Pod 生命周期的 Quobyte 挂载。
// Quobyte 卷不支持所有权管理或 SELinux 重新标记。
type QuobyteVolumeSource struct {
	// registry 表示一个或多个 Quobyte Registry 服务，
	// 以 host:port 形式的字符串指定（多个条目以逗号分隔），
	// 作为卷的中央注册中心。
	Registry string `json:"registry" protobuf:"bytes,1,opt,name=registry"`

	// volume 是按名称引用已创建 Quobyte 卷的字符串。
	Volume string `json:"volume" protobuf:"bytes,2,opt,name=volume"`

	// 此处的 readOnly 将强制以只读权限挂载 Quobyte 卷。
	// 默认值为 false。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`

	// user 指定卷访问映射到的用户。
	// 默认值为 serviceaccount 用户。
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,4,opt,name=user"`

	// group 指定卷访问映射到的组。
	// 默认不指定组。
	// +optional
	Group string `json:"group,omitempty" protobuf:"bytes,5,opt,name=group"`

	// tenant 是后端中拥有该 Quobyte 卷的租户。
	// 用于动态配置的 Quobyte 卷，其值由插件设置。
	// +optional
	Tenant string `json:"tenant,omitempty" protobuf:"bytes,6,opt,name=tenant"`
}

// FlexPersistentVolumeSource 表示通过基于 exec 的插件配置/挂载的通用持久卷资源。
type FlexPersistentVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string `json:"driver" protobuf:"bytes,1,opt,name=driver"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。默认文件系统取决于 FlexVolume 脚本。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// secretRef 为可选项：指向包含敏感信息的 Secret 对象的引用，
	// 这些信息将传递给插件脚本。若未指定 Secret 对象，该字段可为空。
	// 如果 Secret 对象包含多个 secret，则所有 secret 都会传递给插件脚本。
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty" protobuf:"bytes,3,opt,name=secretRef"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
	// options 为可选项：用于保存额外的命令选项（如有）。
	// +optional
	Options map[string]string `json:"options,omitempty" protobuf:"bytes,5,rep,name=options"`
}

// FlexVolumeSource 表示通过基于 exec 的插件配置/挂载的通用卷资源。
type FlexVolumeSource struct {
	// driver 是该卷要使用的驱动名称。
	Driver string `json:"driver" protobuf:"bytes,1,opt,name=driver"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。默认文件系统取决于 FlexVolume 脚本。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// secretRef 为可选项：指向包含敏感信息的 Secret 对象的引用，
	// 这些信息将传递给插件脚本。若未指定 Secret 对象，该字段可为空。
	// 如果 Secret 对象包含多个 secret，则所有 secret 都会传递给插件脚本。
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,3,opt,name=secretRef"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
	// options 为可选项：用于保存额外的命令选项（如有）。
	// +optional
	Options map[string]string `json:"options,omitempty" protobuf:"bytes,5,rep,name=options"`
}

// 表示 AWS 中的永久磁盘资源。
//
// AWS EBS 磁盘必须先存在，才能挂载到容器。该磁盘还必须与 kubelet 位于同一 AWS 可用区。
// AWS EBS 磁盘只能以读写方式挂载一次。
// AWS EBS 卷支持所有权管理和 SELinux 重新标记。
type AWSElasticBlockStoreVolumeSource struct {
	// volumeID 是 AWS 中永久磁盘资源（Amazon EBS 卷）的唯一 ID。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
	VolumeID string `json:"volumeID" protobuf:"bytes,1,opt,name=volumeID"`
	// fsType 是要挂载的卷的文件系统类型。
	// 提示：请确保该文件系统类型受宿主机操作系统支持。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// partition 是要挂载的卷中的分区。
	// 若省略，默认按卷名挂载。
	// 示例：对于卷 /dev/sda1，分区指定为 "1"。
	// 类似地，/dev/sda 的卷分区为 "0"（也可以留空该字段）。
	// +optional
	Partition int32 `json:"partition,omitempty" protobuf:"varint,3,opt,name=partition"`
	// readOnly 设为 true 时将强制 VolumeMounts 中的 readOnly 设置。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#awselasticblockstore
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
}

// 表示用 Git 仓库内容填充的卷。
// Git 仓库卷不支持所有权管理。
// Git 仓库卷支持 SELinux 重新标记。
//
// DEPRECATED: GitRepo 已弃用。若要为容器提供 Git 仓库，请将 EmptyDir 挂载到
// InitContainer 中，由后者使用 git 克隆仓库，然后将该 EmptyDir 挂载到
// Pod 的容器中。
type GitRepoVolumeSource struct {
	// repository 是仓库 URL。
	Repository string `json:"repository" protobuf:"bytes,1,opt,name=repository"`
	// revision 是指定版本的提交哈希。
	// +optional
	Revision string `json:"revision,omitempty" protobuf:"bytes,2,opt,name=revision"`
	// directory 是目标目录名称。
	// 不得包含或以 '..' 开头。若提供 '.'，卷目录即为 Git 仓库本身。
	// 否则，若指定了该字段，卷将在具有给定名称的子目录中包含 Git 仓库。
	// +optional
	Directory string `json:"directory,omitempty" protobuf:"bytes,3,opt,name=directory"`
}

// 将 Secret 适配为卷。
//
// 目标 Secret 的 Data 字段内容将以文件形式呈现在卷中，文件名即 Data 字段的键。
// Secret 卷支持所有权管理和 SELinux 重新标记。
type SecretVolumeSource struct {
	// secretName 是 Pod 所在命名空间中要使用的 Secret 名称。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#secret
	// +optional
	SecretName string `json:"secretName,omitempty" protobuf:"bytes,1,opt,name=secretName"`
	// items 若未指定，所引用 Secret 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 Secret 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	// +listType=atomic
	Items []KeyToPath `json:"items,omitempty" protobuf:"bytes,2,rep,name=items"`
	// defaultMode 为可选项：默认用于设置所创建文件权限的模式位。
	// 必须是 0000 到 0777 之间的八进制值，或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求模式位使用十进制值。默认值为 0644。
	// 路径中的目录不受此设置影响。
	// 这可能与 fsGroup 等其他影响文件模式的选项冲突，最终设置的模式位可能不同。
	// +optional
	DefaultMode *int32 `json:"defaultMode,omitempty" protobuf:"bytes,3,opt,name=defaultMode"`
	// optional 字段指定是否必须定义 Secret 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,4,opt,name=optional"`
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64 `json:"defaultUser,omitempty" protobuf:"varint,5,opt,name=defaultUser"`
}

const (
	// SecretVolumeSourceDefaultMode 是 Secret 卷所创建文件的默认权限模式。
	SecretVolumeSourceDefaultMode int32 = 0644
)

// 将 Secret 适配为投影卷。
//
// 目标 Secret 的 Data 字段内容将以文件形式呈现在投影卷中，文件名即 Data 字段的键。
// 注意，这与不带默认模式的 Secret 卷源相同。
type SecretProjection struct {
	// LocalObjectReference 是对要投影的 Secret 的引用。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// items 若未指定，所引用 Secret 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 Secret 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	// +listType=atomic
	Items []KeyToPath `json:"items,omitempty" protobuf:"bytes,2,rep,name=items"`
	// optional 字段指定是否必须定义 Secret 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,4,opt,name=optional"`
}

// 表示持续整个 Pod 生命周期的 NFS 挂载。
// NFS 卷不支持所有权管理或 SELinux 重新标记。
type NFSVolumeSource struct {
	// server 是 NFS 服务器的主机名或 IP 地址。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#nfs
	Server string `json:"server" protobuf:"bytes,1,opt,name=server"`

	// path 是 NFS 服务器导出的路径。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#nfs
	Path string `json:"path" protobuf:"bytes,2,opt,name=path"`

	// 此处的 readOnly 将强制以只读权限挂载 NFS 导出目录。
	// 默认值为 false。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#nfs
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
}

// 表示 ISCSI 磁盘。
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIVolumeSource struct {
	// targetPortal 是 iSCSI Target Portal。Portal 为 IP 地址，若端口非默认值，
	// 则为 ip_addr:port（通常为 TCP 端口 860 和 3260）。
	TargetPortal string `json:"targetPortal" protobuf:"bytes,1,opt,name=targetPortal"`
	// iqn 是目标 iSCSI 限定名称。
	IQN string `json:"iqn" protobuf:"bytes,2,opt,name=iqn"`
	// lun 表示 iSCSI Target 的 LUN 号。
	Lun int32 `json:"lun" protobuf:"varint,3,opt,name=lun"`
	// iscsiInterface 是使用 iSCSI 传输的接口名称。
	// 默认值为 'default'（tcp）。
	// +optional
	// +default="default"
	ISCSIInterface string `json:"iscsiInterface,omitempty" protobuf:"bytes,4,opt,name=iscsiInterface"`
	// fsType 是要挂载的卷的文件系统类型。
	// 提示：请确保该文件系统类型受宿主机操作系统支持。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#iscsi
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,5,opt,name=fsType"`
	// 此处的 readOnly 将强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认值为 false。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,6,opt,name=readOnly"`
	// portals 是 iSCSI Target Portal 列表。Portal 为 IP 地址，若端口非默认值，
	// 则为 ip_addr:port（通常为 TCP 端口 860 和 3260）。
	// +optional
	// +listType=atomic
	Portals []string `json:"portals,omitempty" protobuf:"bytes,7,opt,name=portals"`
	// chapAuthDiscovery 定义是否支持 iSCSI Discovery CHAP 认证。
	// +optional
	DiscoveryCHAPAuth bool `json:"chapAuthDiscovery,omitempty" protobuf:"varint,8,opt,name=chapAuthDiscovery"`
	// chapAuthSession 定义是否支持 iSCSI Session CHAP 认证。
	// +optional
	SessionCHAPAuth bool `json:"chapAuthSession,omitempty" protobuf:"varint,11,opt,name=chapAuthSession"`
	// secretRef 是用于 iSCSI target 和 initiator 认证的 CHAP Secret。
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,10,opt,name=secretRef"`
	// initiatorName 是自定义的 iSCSI Initiator 名称。
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name>。
	// +optional
	InitiatorName *string `json:"initiatorName,omitempty" protobuf:"bytes,12,opt,name=initiatorName"`
}

// ISCSIPersistentVolumeSource 表示 ISCSI 磁盘。
// ISCSI 卷只能以读写方式挂载一次。
// ISCSI 卷支持所有权管理和 SELinux 重新标记。
type ISCSIPersistentVolumeSource struct {
	// targetPortal 是 iSCSI Target Portal。Portal 为 IP 地址，若端口非默认值，
	// 则为 ip_addr:port（通常为 TCP 端口 860 和 3260）。
	TargetPortal string `json:"targetPortal" protobuf:"bytes,1,opt,name=targetPortal"`
	// iqn 是目标 iSCSI 限定名称。
	IQN string `json:"iqn" protobuf:"bytes,2,opt,name=iqn"`
	// lun 是 iSCSI Target 的 LUN 号。
	Lun int32 `json:"lun" protobuf:"varint,3,opt,name=lun"`
	// iscsiInterface 是使用 iSCSI 传输的接口名称。
	// 默认值为 'default'（tcp）。
	// +optional
	// +default="default"
	ISCSIInterface string `json:"iscsiInterface,omitempty" protobuf:"bytes,4,opt,name=iscsiInterface"`
	// fsType 是要挂载的卷的文件系统类型。
	// 提示：请确保该文件系统类型受宿主机操作系统支持。
	// 示例："ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes#iscsi
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,5,opt,name=fsType"`
	// 此处的 readOnly 将强制 VolumeMounts 中的 ReadOnly 设置。
	// 默认值为 false。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,6,opt,name=readOnly"`
	// portals 是 iSCSI Target Portal 列表。Portal 为 IP 地址，若端口非默认值，
	// 则为 ip_addr:port（通常为 TCP 端口 860 和 3260）。
	// +optional
	// +listType=atomic
	Portals []string `json:"portals,omitempty" protobuf:"bytes,7,opt,name=portals"`
	// chapAuthDiscovery 定义是否支持 iSCSI Discovery CHAP 认证。
	// +optional
	DiscoveryCHAPAuth bool `json:"chapAuthDiscovery,omitempty" protobuf:"varint,8,opt,name=chapAuthDiscovery"`
	// chapAuthSession 定义是否支持 iSCSI Session CHAP 认证。
	// +optional
	SessionCHAPAuth bool `json:"chapAuthSession,omitempty" protobuf:"varint,11,opt,name=chapAuthSession"`
	// secretRef 是用于 iSCSI target 和 initiator 认证的 CHAP Secret。
	// +optional
	SecretRef *SecretReference `json:"secretRef,omitempty" protobuf:"bytes,10,opt,name=secretRef"`
	// initiatorName 是自定义的 iSCSI Initiator 名称。
	// 如果同时指定 initiatorName 和 iscsiInterface，将为该连接创建新的 iSCSI 接口
	// <target portal>:<volume name>。
	// +optional
	InitiatorName *string `json:"initiatorName,omitempty" protobuf:"bytes,12,opt,name=initiatorName"`
}

// 表示 Fibre Channel 卷。
// Fibre Channel 卷只能以读写方式挂载一次。
// Fibre Channel 卷支持所有权管理和 SELinux 重新标记。
type FCVolumeSource struct {
	// targetWWNs 为可选项：FC target 的全球名称（WWN）。
	// +optional
	// +listType=atomic
	TargetWWNs []string `json:"targetWWNs,omitempty" protobuf:"bytes,1,rep,name=targetWWNs"`
	// lun 为可选项：FC target 的 LUN 号。
	// +optional
	Lun *int32 `json:"lun,omitempty" protobuf:"varint,2,opt,name=lun"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// TODO: 如何防止文件系统中的错误危及整机
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`
	// readOnly 为可选项：默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
	// wwids 为可选项：FC 卷的全球唯一标识符（wwid）。
	// 必须设置 wwids 或 targetWWNs 与 lun 的组合，但不能同时设置两者。
	// +optional
	// +listType=atomic
	WWIDs []string `json:"wwids,omitempty" protobuf:"bytes,5,rep,name=wwids"`
}

// AzureFile 表示宿主机上的 Azure 文件服务挂载，并以绑定挂载方式挂载到 Pod。
type AzureFileVolumeSource struct {
	// secretName 是包含 Azure 存储账户名称和密钥的 Secret 名称。
	SecretName string `json:"secretName" protobuf:"bytes,1,opt,name=secretName"`
	// shareName 是 Azure 共享名称。
	ShareName string `json:"shareName" protobuf:"bytes,2,opt,name=shareName"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
}

// AzureFilePersistentVolumeSource 表示宿主机上的 Azure 文件服务挂载，并以绑定挂载方式挂载到 Pod。
type AzureFilePersistentVolumeSource struct {
	// secretName 是包含 Azure 存储账户名称和密钥的 Secret 名称。
	SecretName string `json:"secretName" protobuf:"bytes,1,opt,name=secretName"`
	// shareName 是 Azure 共享名称。
	ShareName string `json:"shareName" protobuf:"bytes,2,opt,name=shareName"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
	// secretNamespace 是包含 Azure 存储账户名称和密钥的 Secret 所在的命名空间。
	// 默认与 Pod 所在命名空间相同。
	// +optional
	SecretNamespace *string `json:"secretNamespace" protobuf:"bytes,4,opt,name=secretNamespace"`
}

// 表示 vSphere 卷资源。
type VsphereVirtualDiskVolumeSource struct {
	// volumePath 是标识 vSphere 卷 vmdk 的路径。
	VolumePath string `json:"volumePath" protobuf:"bytes,1,opt,name=volumePath"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// storagePolicyName 是基于存储策略的管理（SPBM）配置文件名称。
	// +optional
	StoragePolicyName string `json:"storagePolicyName,omitempty" protobuf:"bytes,3,opt,name=storagePolicyName"`
	// storagePolicyID 是与 StoragePolicyName 关联的存储策略管理（SPBM）配置文件 ID。
	// +optional
	StoragePolicyID string `json:"storagePolicyID,omitempty" protobuf:"bytes,4,opt,name=storagePolicyID"`
}

// 表示 Photon Controller 持久磁盘资源。
type PhotonPersistentDiskVolumeSource struct {
	// pdID 是标识 Photon Controller 持久磁盘的 ID。
	PdID string `json:"pdID" protobuf:"bytes,1,opt,name=pdID"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
}

// AzureDataDiskCachingMode 表示 Azure 数据磁盘的缓存模式。
// +enum
type AzureDataDiskCachingMode string

// AzureDataDiskKind 表示 Azure 数据磁盘的类型。
// +enum
type AzureDataDiskKind string

const (
	// AzureDataDiskCachingNone 表示不缓存。
	AzureDataDiskCachingNone AzureDataDiskCachingMode = "None"
	// AzureDataDiskCachingReadOnly 表示只读缓存。
	AzureDataDiskCachingReadOnly AzureDataDiskCachingMode = "ReadOnly"
	// AzureDataDiskCachingReadWrite 表示读写缓存。
	AzureDataDiskCachingReadWrite AzureDataDiskCachingMode = "ReadWrite"

	// AzureSharedBlobDisk 表示每个存储账户多个 Blob 磁盘。
	AzureSharedBlobDisk AzureDataDiskKind = "Shared"
	// AzureDedicatedBlobDisk 表示每个存储账户单个 Blob 磁盘。
	AzureDedicatedBlobDisk AzureDataDiskKind = "Dedicated"
	// AzureManagedDisk 表示 Azure 托管数据磁盘（仅适用于托管可用性集）。
	AzureManagedDisk AzureDataDiskKind = "Managed"
)

// AzureDisk 表示宿主机上的 Azure 数据磁盘挂载，并以绑定挂载方式挂载到 Pod。
type AzureDiskVolumeSource struct {
	// diskName 是 Blob 存储中数据磁盘的名称。
	DiskName string `json:"diskName" protobuf:"bytes,1,opt,name=diskName"`
	// diskURI 是 Blob 存储中数据磁盘的 URI。
	DataDiskURI string `json:"diskURI" protobuf:"bytes,2,opt,name=diskURI"`
	// cachingMode 是主机缓存模式：None、ReadOnly、ReadWrite。
	// +optional
	// +default=ref(AzureDataDiskCachingReadWrite)
	CachingMode *AzureDataDiskCachingMode `json:"cachingMode,omitempty" protobuf:"bytes,3,opt,name=cachingMode,casttype=AzureDataDiskCachingMode"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	// +default="ext4"
	FSType *string `json:"fsType,omitempty" protobuf:"bytes,4,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	// +default=false
	ReadOnly *bool `json:"readOnly,omitempty" protobuf:"varint,5,opt,name=readOnly"`
	// kind 的预期值为 Shared（每个存储账户多个 Blob 磁盘）、Dedicated（每个存储账户单个 Blob 磁盘）、
	// Managed（Azure 托管数据磁盘，仅适用于托管可用性集）。默认值为 Shared。
	// +default=ref(AzureSharedBlobDisk)
	Kind *AzureDataDiskKind `json:"kind,omitempty" protobuf:"bytes,6,opt,name=kind,casttype=AzureDataDiskKind"`
}

// PortworxVolumeSource 表示 Portworx 卷资源。
type PortworxVolumeSource struct {
	// volumeID 唯一标识一个 Portworx 卷。
	VolumeID string `json:"volumeID" protobuf:"bytes,1,opt,name=volumeID"`
	// fSType 表示要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"。未指定时默认为 "ext4"。
	FSType string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
}

// ScaleIOVolumeSource 表示一个持久的 ScaleIO 卷。
type ScaleIOVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string `json:"gateway" protobuf:"bytes,1,opt,name=gateway"`
	// system 是 ScaleIO 中配置的存储系统名称。
	System string `json:"system" protobuf:"bytes,2,opt,name=system"`
	// secretRef 引用 ScaleIO 用户及其他敏感信息的 Secret。
	// 若未提供，登录操作将失败。
	SecretRef *LocalObjectReference `json:"secretRef" protobuf:"bytes,3,opt,name=secretRef"`
	// sslEnabled 用于启用/禁用与 Gateway 的 SSL 通信，默认值为 false。
	// +optional
	SSLEnabled bool `json:"sslEnabled,omitempty" protobuf:"varint,4,opt,name=sslEnabled"`
	// protectionDomain 是所配置存储的 ScaleIO 保护域名称。
	// +optional
	ProtectionDomain string `json:"protectionDomain,omitempty" protobuf:"bytes,5,opt,name=protectionDomain"`
	// storagePool 是与保护域关联的 ScaleIO 存储池。
	// +optional
	StoragePool string `json:"storagePool,omitempty" protobuf:"bytes,6,opt,name=storagePool"`
	// storageMode 指示卷的存储应采用厚配置（ThickProvisioned）还是精简配置（ThinProvisioned）。
	// 默认值为 ThinProvisioned。
	// +optional
	// +default="ThinProvisioned"
	StorageMode string `json:"storageMode,omitempty" protobuf:"bytes,7,opt,name=storageMode"`
	// volumeName 是 ScaleIO 系统中已创建、并与此卷源关联的卷的名称。
	VolumeName string `json:"volumeName,omitempty" protobuf:"bytes,8,opt,name=volumeName"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// 默认值为 "xfs"。
	// +optional
	// +default="xfs"
	FSType string `json:"fsType,omitempty" protobuf:"bytes,9,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,10,opt,name=readOnly"`
}

// ScaleIOPersistentVolumeSource 表示一个持久的 ScaleIO 卷。
type ScaleIOPersistentVolumeSource struct {
	// gateway 是 ScaleIO API Gateway 的主机地址。
	Gateway string `json:"gateway" protobuf:"bytes,1,opt,name=gateway"`
	// system 是 ScaleIO 中配置的存储系统名称。
	System string `json:"system" protobuf:"bytes,2,opt,name=system"`
	// secretRef 引用 ScaleIO 用户及其他敏感信息的 Secret。
	// 若未提供，登录操作将失败。
	SecretRef *SecretReference `json:"secretRef" protobuf:"bytes,3,opt,name=secretRef"`
	// sslEnabled 是用于启用/禁用与 Gateway 的 SSL 通信的标志，默认值为 false。
	// +optional
	SSLEnabled bool `json:"sslEnabled,omitempty" protobuf:"varint,4,opt,name=sslEnabled"`
	// protectionDomain 是所配置存储的 ScaleIO 保护域名称。
	// +optional
	ProtectionDomain string `json:"protectionDomain,omitempty" protobuf:"bytes,5,opt,name=protectionDomain"`
	// storagePool 是与保护域关联的 ScaleIO 存储池。
	// +optional
	StoragePool string `json:"storagePool,omitempty" protobuf:"bytes,6,opt,name=storagePool"`
	// storageMode 指示卷的存储应采用厚配置（ThickProvisioned）还是精简配置（ThinProvisioned）。
	// 默认值为 ThinProvisioned。
	// +optional
	// +default="ThinProvisioned"
	StorageMode string `json:"storageMode,omitempty" protobuf:"bytes,7,opt,name=storageMode"`
	// volumeName 是 ScaleIO 系统中已创建、并与此卷源关联的卷的名称。
	VolumeName string `json:"volumeName,omitempty" protobuf:"bytes,8,opt,name=volumeName"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// 默认值为 "xfs"。
	// +optional
	// +default="xfs"
	FSType string `json:"fsType,omitempty" protobuf:"bytes,9,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,10,opt,name=readOnly"`
}

// 表示 StorageOS 持久卷资源。
type StorageOSVolumeSource struct {
	// volumeName 是 StorageOS 卷的可读名称。卷名仅在命名空间内唯一。
	VolumeName string `json:"volumeName,omitempty" protobuf:"bytes,1,opt,name=volumeName"`
	// volumeNamespace 指定卷在 StorageOS 中的作用域。若未指定命名空间，
	// 则使用 Pod 的命名空间。这样可在 StorageOS 中镜像 Kubernetes 的名称作用域，实现更紧密的集成。
	// 将 VolumeName 设置为任意名称可覆盖默认行为。
	// 若未在 StorageOS 中使用命名空间，请设置为 "default"。
	// StorageOS 中不存在的命名空间将被自动创建。
	// +optional
	VolumeNamespace string `json:"volumeNamespace,omitempty" protobuf:"bytes,2,opt,name=volumeNamespace"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
	// secretRef 指定用于获取 StorageOS API 凭据的 Secret。
	// 若未指定，将尝试使用默认值。
	// +optional
	SecretRef *LocalObjectReference `json:"secretRef,omitempty" protobuf:"bytes,5,opt,name=secretRef"`
}

// 表示 StorageOS 持久卷资源。
type StorageOSPersistentVolumeSource struct {
	// volumeName 是 StorageOS 卷的可读名称。卷名仅在命名空间内唯一。
	VolumeName string `json:"volumeName,omitempty" protobuf:"bytes,1,opt,name=volumeName"`
	// volumeNamespace 指定卷在 StorageOS 中的作用域。若未指定命名空间，
	// 则使用 Pod 的命名空间。这样可在 StorageOS 中镜像 Kubernetes 的名称作用域，实现更紧密的集成。
	// 将 VolumeName 设置为任意名称可覆盖默认行为。
	// 若未在 StorageOS 中使用命名空间，请设置为 "default"。
	// StorageOS 中不存在的命名空间将被自动创建。
	// +optional
	VolumeNamespace string `json:"volumeNamespace,omitempty" protobuf:"bytes,2,opt,name=volumeNamespace"`
	// fsType 是要挂载的文件系统类型。
	// 必须是宿主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。未指定时默认为 "ext4"。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`
	// readOnly 默认为 false（读写）。此处的 readOnly 将强制
	// VolumeMounts 中的 ReadOnly 设置。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,4,opt,name=readOnly"`
	// secretRef 指定用于获取 StorageOS API 凭据的 Secret。
	// 若未指定，将尝试使用默认值。
	// +optional
	SecretRef *ObjectReference `json:"secretRef,omitempty" protobuf:"bytes,5,opt,name=secretRef"`
}

// 将 ConfigMap 适配为卷。
//
// 目标 ConfigMap 的 Data 字段内容将以文件形式呈现在卷中，文件名即 Data 字段的键，
// 除非 items 元素指定了键到路径的特定映射。
// ConfigMap 卷支持所有权管理和 SELinux 重新标记。
type ConfigMapVolumeSource struct {
	// LocalObjectReference 是对要投影的 ConfigMap 的引用。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// items 若未指定，所引用 ConfigMap 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 ConfigMap 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	// +listType=atomic
	Items []KeyToPath `json:"items,omitempty" protobuf:"bytes,2,rep,name=items"`
	// defaultMode 为可选项：默认用于设置所创建文件权限的模式位。
	// 必须是 0000 到 0777 之间的八进制值，或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求模式位使用十进制值。默认值为 0644。
	// 路径中的目录不受此设置影响。
	// 这可能与 fsGroup 等其他影响文件模式的选项冲突，最终设置的模式位可能不同。
	// +optional
	DefaultMode *int32 `json:"defaultMode,omitempty" protobuf:"varint,3,opt,name=defaultMode"`
	// optional 指定是否必须定义 ConfigMap 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,4,opt,name=optional"`
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64 `json:"defaultUser,omitempty" protobuf:"varint,5,opt,name=defaultUser"`
}

const (
	// ConfigMapVolumeSourceDefaultMode 是 ConfigMap 卷所创建文件的默认权限模式。
	ConfigMapVolumeSourceDefaultMode int32 = 0644
)

// 将 ConfigMap 适配为投影卷。
//
// 目标 ConfigMap 的 Data 字段内容将以文件形式呈现在投影卷中，文件名即 Data 字段的键，
// 除非 items 元素指定了键到路径的特定映射。
// 注意，这与不带默认模式的 ConfigMap 卷源相同。
type ConfigMapProjection struct {
	// LocalObjectReference 是对要投影的 ConfigMap 的引用。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// items 若未指定，所引用 ConfigMap 的 Data 字段中的每个键值对都将投影到卷中，
	// 文件名即键，内容即值。若指定，则列出的键将投影到指定路径，未列出的键不会出现。
	// 如果指定了 ConfigMap 中不存在的键，除非标记为 optional，否则卷设置将报错。
	// 路径必须为相对路径，不得包含 '..' 路径或以 '..' 开头。
	// +optional
	// +listType=atomic
	Items []KeyToPath `json:"items,omitempty" protobuf:"bytes,2,rep,name=items"`
	// optional 指定是否必须定义 ConfigMap 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,4,opt,name=optional"`
}

// ServiceAccountTokenProjection 表示投影的 ServiceAccount 令牌卷。
// 此投影可用于将 ServiceAccount 令牌插入 Pod 的运行时文件系统，
// 以用于访问 API（Kubernetes API Server 或其他 API）。
type ServiceAccountTokenProjection struct {
	// audience 是令牌的目标受众。令牌接收方必须使用令牌受众中指定的标识符表明自身身份，
	// 否则应拒绝该令牌。受众默认为 apiserver 的标识符。
	// +optional
	Audience string `json:"audience,omitempty" protobuf:"bytes,1,rep,name=audience"`
	// expirationSeconds 是所请求的 ServiceAccount 令牌有效时长。
	// 当令牌临近过期时，kubelet 卷插件将主动轮换该令牌。
	// 若令牌已超过其生存时间的 80%，或令牌已存在超过 24 小时，kubelet 将开始尝试轮换。
	// 默认值为 1 小时，且不得小于 10 分钟。
	// +optional
	ExpirationSeconds *int64 `json:"expirationSeconds,omitempty" protobuf:"varint,2,opt,name=expirationSeconds"`
	// path 是令牌投影文件的路径，相对于挂载点。
	Path string `json:"path" protobuf:"bytes,3,opt,name=path"`
	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64 `json:"user,omitempty" protobuf:"varint,4,opt,name=user"`
}

// ClusterTrustBundleProjection 描述如何选择一组 ClusterTrustBundle 对象，
// 并将其内容投影到 Pod 文件系统中。
type ClusterTrustBundleProjection struct {
	// 按对象名称选择单个 ClusterTrustBundle。
	// 与 signerName 和 labelSelector 互斥。
	// +optional
	Name *string `json:"name,omitempty" protobuf:"bytes,1,rep,name=name"`

	// 选择所有与此签名者名称匹配的 ClusterTrustBundle。
	// 与 name 互斥。所有选中的 ClusterTrustBundle 的内容将被合并并去重。
	// +optional
	SignerName *string `json:"signerName,omitempty" protobuf:"bytes,2,rep,name=signerName"`

	// 选择所有与此标签选择器匹配的 ClusterTrustBundle。仅在设置了 signerName 时生效。
	// 与 name 互斥。若未设置，解释为“不匹配任何对象”；若设置但为空，解释为“匹配所有对象”。
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty" protobuf:"bytes,3,rep,name=labelSelector"`

	// 若为 true，则当引用的 ClusterTrustBundle 不可用时，不阻塞 Pod 启动。
	// 使用 name 时，允许指定名称的 ClusterTrustBundle 不存在。
	// 使用 signerName 时，允许 signerName 与 labelSelector 的组合匹配不到任何
	// ClusterTrustBundle。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,5,opt,name=optional"`

	// 写入 bundle 的路径，相对于卷根目录。
	Path string `json:"path" protobuf:"bytes,4,rep,name=path"`

	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64 `json:"user,omitempty" protobuf:"varint,6,opt,name=user"`
}

// PodCertificateProjection 在 Pod 文件系统中提供私钥和 X.509 证书。
type PodCertificateProjection struct {
	// Kubelet 生成的 CSR 将发送给此签名者。
	//
	// +required
	SignerName string `json:"signerName,omitempty" protobuf:"bytes,1,rep,name=signerName"`

	// Kubelet 将为 Pod 生成的密钥对类型。
	//
	// 有效值为 "RSA3072"、"RSA4096"、"ECDSAP256"、"ECDSAP384"、
	// "ECDSAP521" 和 "ED25519"。
	//
	// +required
	KeyType string `json:"keyType,omitempty" protobuf:"bytes,2,rep,name=keyType"`

	// maxExpirationSeconds 是证书允许的最大有效期。
	//
	// Kubelet 会将此值原样复制到为此投影生成的 PodCertificateRequest 中。
	//
	// 若省略，kube-apiserver 将其设为 86400（24 小时）。kube-apiserver 将拒绝小于
	// 3600（1 小时）的值。允许的最大值为 7862400（91 天）。
	//
	// 签名者实现可签发有效期短于 MaxExpirationSeconds 的证书，但不得短于 3600
	// 秒（1 小时）。该约束由 kube-apiserver 强制执行。
	// `kubernetes.io` 签名者永远不会签发有效期超过 24 小时的证书。
	//
	// +optional
	MaxExpirationSeconds *int32 `json:"maxExpirationSeconds,omitempty" protobuf:"varint,3,opt,name=maxExpirationSeconds"`

	// 将凭据包写入投影卷中的此路径。
	//
	// 凭据包是单个文件，包含多个 PEM 块。第一个 PEM 块是 PRIVATE KEY 块，
	// 其中包含 PKCS#8 私钥。
	//
	// 其余块为 CERTIFICATE 块，包含签名者签发的证书链（叶子证书及所有中间证书）。
	//
	// 使用 credentialBundlePath 可让 Pod 中的应用代码通过一次原子读取获得一致的
	// 私钥和证书链。若将它们投影到不同文件，应用代码还需额外检查叶子证书是否签发给该私钥。
	//
	// +optional
	CredentialBundlePath string `json:"credentialBundlePath,omitempty" protobuf:"bytes,4,rep,name=credentialBundlePath"`

	// 将私钥写入投影卷中的此路径。
	//
	// 大多数应用应使用 credentialBundlePath。使用 keyPath 和 certificateChainPath 时，
	// 应用需要检查私钥与叶子证书是否一致，因为可能在轮换过程中读取到文件。
	//
	// +optional
	KeyPath string `json:"keyPath,omitempty" protobuf:"bytes,5,rep,name=keyPath"`

	// 将证书链写入投影卷中的此路径。
	//
	// 大多数应用应使用 credentialBundlePath。使用 keyPath 和 certificateChainPath 时，
	// 应用需要检查私钥与叶子证书是否一致，因为可能在轮换过程中读取到文件。
	//
	// +optional
	CertificateChainPath string `json:"certificateChainPath,omitempty" protobuf:"bytes,6,rep,name=certificateChainPath"`

	// userAnnotations 允许 Pod 编写者向签名者实现传递附加信息。
	// Kubernetes 不对此元数据进行任何限制或校验。
	//
	// 这些值将被原样复制到 Kubelet 创建的 PodCertificateRequest 对象的
	// `spec.unverifiedUserAnnotations` 字段中。
	//
	// 这些条目遵循与对象元数据注解相同的校验规则，此外所有键必须带域名前缀。
	// 对值没有限制，但整个字段有总大小限制。
	//
	// 签名者应记录其支持的键和值，并拒绝包含无法识别的键的请求。
	UserAnnotations map[string]string `json:"userAnnotations,omitempty" protobuf:"bytes,7,rep,name=userAnnotations"`

	// user 为可选项：所创建文件的所有者 UID。
	// 若指定，item 级别的 user 字段优先于 defaultUser。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64 `json:"user,omitempty" protobuf:"varint,8,opt,name=user"`
}

// 表示投影卷源。
type ProjectedVolumeSource struct {
	// sources 是卷投影的列表。列表中的每个条目处理一个卷源。
	// +optional
	// +listType=atomic
	Sources []VolumeProjection `json:"sources" protobuf:"bytes,1,rep,name=sources"`
	// defaultMode 是默认用于设置所创建文件权限的模式位。
	// 必须是 0000 到 0777 之间的八进制值，或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求模式位使用十进制值。
	// 路径中的目录不受此设置影响。
	// 这可能与 fsGroup 等其他影响文件模式的选项冲突，最终设置的模式位可能不同。
	// +optional
	DefaultMode *int32 `json:"defaultMode,omitempty" protobuf:"varint,2,opt,name=defaultMode"`
	// defaultUser 为可选项：所创建文件的默认所有者 UID。
	// defaultUser 字段仅在 item 级别的 user 字段未设置时作为回退值。
	// (Alpha) 此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64 `json:"defaultUser,omitempty" protobuf:"varint,3,opt,name=defaultUser"`
}

// 可与其他受支持的卷类型一起投影的投影源。
// 这些字段必须且只能设置其中一个。
type VolumeProjection struct {
	// secret 是要投影的 Secret 数据的相关信息。
	// +optional
	Secret *SecretProjection `json:"secret,omitempty" protobuf:"bytes,1,opt,name=secret"`
	// downwardAPI 是要投影的 downwardAPI 数据的相关信息。
	// +optional
	DownwardAPI *DownwardAPIProjection `json:"downwardAPI,omitempty" protobuf:"bytes,2,opt,name=downwardAPI"`
	// configMap 是要投影的 ConfigMap 数据的相关信息。
	// +optional
	ConfigMap *ConfigMapProjection `json:"configMap,omitempty" protobuf:"bytes,3,opt,name=configMap"`
	// serviceAccountToken 是要投影的 ServiceAccount 令牌数据的相关信息。
	// +optional
	ServiceAccountToken *ServiceAccountTokenProjection `json:"serviceAccountToken,omitempty" protobuf:"bytes,4,opt,name=serviceAccountToken"`

	// ClusterTrustBundle 允许 Pod 通过自动更新的文件访问 ClusterTrustBundle 对象的
	// `.spec.trustBundle` 字段。
	//
	// Alpha 阶段，由 ClusterTrustBundleProjection 特性门控控制。
	//
	// ClusterTrustBundle 对象可按名称选择，也可按签名者名称与标签选择器的组合选择。
	//
	// Kubelet 会对写入 Pod 文件系统的 PEM 内容进行深度规范化处理，去除块间注释和块头等
	// 生僻的 PEM 特性，并对证书去重。文件中证书的顺序是任意的，Kubelet 可能随时间改变顺序。
	//
	// +featureGate=ClusterTrustBundleProjection
	// +optional
	ClusterTrustBundle *ClusterTrustBundleProjection `json:"clusterTrustBundle,omitempty" protobuf:"bytes,5,opt,name=clusterTrustBundle"`

	// 投影一个自动轮换的凭据包（私钥和证书链），Pod 可将其用作 TLS 客户端或服务端。
	//
	// Kubelet 生成私钥，并用它向指定签名者发送 PodCertificateRequest。签名者批准请求并
	// 签发证书链后，Kubelet 将私钥和证书链写入 Pod 文件系统。在 Pod 规格中每个
	// podCertificate 投影卷源都获得证书之前，Pod 不会启动。
	//
	// Kubelet 会在签名者通过 PodCertificateRequest.Status.BeginRefreshAt 时间戳
	// 指定的时间开始尝试轮换证书。
	//
	// Kubelet 可以写入单个文件（由 credentialBundlePath 字段指定），也可以写入多个文件
	// （由 keyPath 和 certificateChainPath 字段指定）。
	//
	// 凭据包是 PEM 格式的单个文件。第一个 PEM 条目是私钥（PKCS#8 格式），其余 PEM
	// 条目是签名者签发的证书链（通常签名者按叶子证书到根证书的顺序返回证书链）。
	//
	// 建议优先使用凭据包格式，因为应用代码可以原子地读取它。若使用 keyPath 和
	// certificateChainPath，应用必须进行两次独立的文件读取。如果这两次读取恰逢证书轮换，
	// 读到的私钥和叶子证书可能不匹配。应用需要检查这种情况，并重新读取直到两者一致。
	//
	// 指定签名者决定其签发证书的格式；请参阅签名者实现的文档以了解如何使用其签发的证书。
	//
	// +featureGate=PodCertificateProjection
	// +optional
	PodCertificate *PodCertificateProjection `json:"podCertificate,omitempty" protobuf:"bytes,6,opt,name=podCertificate"`
}

const (
	// ProjectedVolumeSourceDefaultMode 是投影卷所创建文件的默认权限模式。
	ProjectedVolumeSourceDefaultMode int32 = 0644
)

// KeyToPath 将字符串键映射到卷内的路径。
type KeyToPath struct {
	// key 是要投射的键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`

	// path 是键所映射到的文件的相对路径。
	// 不能是绝对路径。
	// 不能包含路径元素 '..'。
	// 不能以字符串 '..' 开头。
	Path string `json:"path" protobuf:"bytes,2,opt,name=path"`
	// mode 是可选的：用于设置该文件权限的权限位。
	// 必须是 0000 到 0777 之间的八进制值，或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求权限位使用十进制值。
	// 如果未指定，将使用卷的 defaultMode。
	// 这可能与其他影响文件权限的选项（如 fsGroup）冲突，
	// 最终结果可能是其他权限位被设置。
	// +optional
	Mode *int32 `json:"mode,omitempty" protobuf:"varint,3,opt,name=mode"`
	// user 是可选的：所创建文件的所有者 UID。
	// 如果指定，条目级别的 user 字段优先于 defaultUser。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64 `json:"user,omitempty" protobuf:"varint,4,opt,name=user"`
}

// LocalVolumeSource 表示具有节点亲和性的直接附加存储。
type LocalVolumeSource struct {
	// path 是节点上卷的完整路径。
	// 它可以是目录或块设备（磁盘、分区等）。
	Path string `json:"path" protobuf:"bytes,1,opt,name=path"`

	// fsType 是要挂载的文件系统类型。
	// 仅当 Path 是块设备时适用。
	// 必须是主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。如果未指定，默认会自动选择文件系统。
	// +optional
	FSType *string `json:"fsType,omitempty" protobuf:"bytes,2,opt,name=fsType"`
}

// CSIPersistentVolumeSource 表示由外部 CSI 卷驱动管理的存储。
type CSIPersistentVolumeSource struct {
	// driver 是要用于此卷的驱动名称。
	// 必填。
	Driver string `json:"driver" protobuf:"bytes,1,opt,name=driver"`

	// volumeHandle 是 CSI 卷插件的 CreateVolume 返回的唯一卷名称，
	// 用于后续所有调用中引用该卷。
	// 必填。
	VolumeHandle string `json:"volumeHandle" protobuf:"bytes,2,opt,name=volumeHandle"`

	// readOnly 是要传给 ControllerPublishVolumeRequest 的值。
	// 默认为 false（读写）。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`

	// fsType 是要挂载的文件系统。必须是主机操作系统支持的文件系统类型。
	// 例如 "ext4"、"xfs"、"ntfs"。
	// +optional
	FSType string `json:"fsType,omitempty" protobuf:"bytes,4,opt,name=fsType"`

	// volumeAttributes 是要发布的卷的属性。
	// +optional
	VolumeAttributes map[string]string `json:"volumeAttributes,omitempty" protobuf:"bytes,5,rep,name=volumeAttributes"`

	// controllerPublishSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 ControllerPublishVolume 和 ControllerUnpublishVolume 调用。
	// 此字段可选，如果不需要 Secret 可以为空。如果 Secret 对象包含多个 secret，则全部传递。
	// +optional
	ControllerPublishSecretRef *SecretReference `json:"controllerPublishSecretRef,omitempty" protobuf:"bytes,6,opt,name=controllerPublishSecretRef"`

	// nodeStageSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 NodeStageVolume、NodeStageVolume 和 NodeUnstageVolume 调用。
	// 此字段可选，如果不需要 Secret 可以为空。如果 Secret 对象包含多个 secret，则全部传递。
	// +optional
	NodeStageSecretRef *SecretReference `json:"nodeStageSecretRef,omitempty" protobuf:"bytes,7,opt,name=nodeStageSecretRef"`

	// nodePublishSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 NodePublishVolume 和 NodeUnpublishVolume 调用。
	// 此字段可选，如果不需要 Secret 可以为空。如果 Secret 对象包含多个 secret，则全部传递。
	// +optional
	NodePublishSecretRef *SecretReference `json:"nodePublishSecretRef,omitempty" protobuf:"bytes,8,opt,name=nodePublishSecretRef"`

	// controllerExpandSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 ControllerExpandVolume 调用。
	// 此字段可选，如果不需要 Secret 可以为空。如果 Secret 对象包含多个 secret，则全部传递。
	// +optional
	ControllerExpandSecretRef *SecretReference `json:"controllerExpandSecretRef,omitempty" protobuf:"bytes,9,opt,name=controllerExpandSecretRef"`

	// nodeExpandSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 NodeExpandVolume 调用。
	// 此字段可选，如果不需要 Secret 可以省略。如果 Secret 对象包含多个 secret，则全部传递。
	// +optional
	NodeExpandSecretRef *SecretReference `json:"nodeExpandSecretRef,omitempty" protobuf:"bytes,10,opt,name=nodeExpandSecretRef"`
}

// CSIVolumeSource 表示由外部 CSI 驱动管理的卷的挂载来源位置。
type CSIVolumeSource struct {
	// driver 是处理此卷的 CSI 驱动名称。
	// 请向管理员咨询集群中注册的正确名称。
	Driver string `json:"driver" protobuf:"bytes,1,opt,name=driver"`

	// readOnly 指定卷的只读配置。
	// 默认为 false（读写）。
	// +optional
	ReadOnly *bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`

	// fsType 是要挂载的文件系统。例如 "ext4"、"xfs"、"ntfs"。
	// 如果未提供，会将空值传给相应的 CSI 驱动，
	// 由驱动决定要使用的默认文件系统。
	// +optional
	FSType *string `json:"fsType,omitempty" protobuf:"bytes,3,opt,name=fsType"`

	// volumeAttributes 存储要传给 CSI 驱动的驱动特定属性。
	// 支持的值请查阅驱动文档。
	// +optional
	VolumeAttributes map[string]string `json:"volumeAttributes,omitempty" protobuf:"bytes,4,rep,name=volumeAttributes"`

	// nodePublishSecretRef 是对某个 Secret 对象的引用，其中包含要传给 CSI 驱动的敏感信息，
	// 用于完成 CSI 的 NodePublishVolume 和 NodeUnpublishVolume 调用。
	// 此字段可选，如果不需要 Secret 可以为空。如果 Secret 对象包含多个 secret，则传递全部引用。
	// +optional
	NodePublishSecretRef *LocalObjectReference `json:"nodePublishSecretRef,omitempty" protobuf:"bytes,5,opt,name=nodePublishSecretRef"`
}

// EphemeralVolumeSource 表示由普通存储驱动处理的临时卷。
type EphemeralVolumeSource struct {
	// 将用于创建一个独立的 PVC 来配置该卷。
	// 嵌入此 EphemeralVolumeSource 的 Pod 将成为该 PVC 的所有者，
	// 即 PVC 会随 Pod 一起被删除。PVC 的名称为 `<pod name>-<volume name>`，
	// 其中 `<volume name>` 是 `PodSpec.Volumes` 数组条目中的名称。
	// 如果拼接后的名称对 PVC 无效（例如过长），Pod 校验将拒绝该 Pod。
	//
	// 已存在的同名 PVC 如果不属于该 Pod，将*不会*被该 Pod 使用，
	// 以避免误用无关的卷。此时 Pod 的启动会被阻塞，
	// 直到该无关的 PVC 被删除。如果某个预先创建的 PVC 确实要供该 Pod 使用，
	// 则在 Pod 创建后，必须为 PVC 更新一个指向该 Pod 的所有者引用。
	// 通常不需要这样做，但在手动重建故障集群时可能有用。
	//
	// 此字段为只读，PVC 创建后 Kubernetes 不会对其进行任何更改。
	//
	// 必填，不能为 nil。
	VolumeClaimTemplate *PersistentVolumeClaimTemplate `json:"volumeClaimTemplate,omitempty" protobuf:"bytes,1,opt,name=volumeClaimTemplate"`

	// ReadOnly 已作墓碑处理，用于说明为何 2 是保留的 protobuf 标签。
	// ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`
}

// PersistentVolumeClaimTemplate 用于在 EphemeralVolumeSource 中生成
// PersistentVolumeClaim 对象。
type PersistentVolumeClaimTemplate struct {
	// 可以包含创建时复制到 PVC 中的标签和注解。
	// 不允许其他字段，校验时会被拒绝。
	//
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// PersistentVolumeClaim 的规格。全部内容会原样复制到
	// 由此模板创建的 PVC 中。PersistentVolumeClaim 中的字段
	// 在此处同样有效。
	Spec PersistentVolumeClaimSpec `json:"spec" protobuf:"bytes,2,name=spec"`
}

// ContainerPort 表示单个容器中的一个网络端口。
type ContainerPort struct {
	// 如果指定，必须是 IANA_SVC_NAME，且在 Pod 内唯一。
	// Pod 中每个命名端口都必须有唯一的名称。可供 Service 引用的端口名称。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	// 要在主机上暴露的端口号。
	// 如果指定，必须是有效的端口号，0 < x < 65536。
	// 如果指定了 HostNetwork，此值必须与 ContainerPort 一致。
	// 大多数容器不需要此字段。
	// +optional
	HostPort int32 `json:"hostPort,omitempty" protobuf:"varint,2,opt,name=hostPort"`
	// 要在 Pod IP 地址上暴露的端口号。
	// 必须是有效的端口号，0 < x < 65536。
	ContainerPort int32 `json:"containerPort" protobuf:"varint,3,opt,name=containerPort"`
	// 端口协议。必须是 UDP、TCP 或 SCTP。
	// 默认为 "TCP"。
	// +optional
	// +default="TCP"
	Protocol Protocol `json:"protocol,omitempty" protobuf:"bytes,4,opt,name=protocol,casttype=Protocol"`
	// 外部端口要绑定到的主机 IP。
	// +optional
	HostIP string `json:"hostIP,omitempty" protobuf:"bytes,5,opt,name=hostIP"`
}

// VolumeMount 描述将卷挂载到容器中的一个挂载点。
type VolumeMount struct {
	// 必须与某个 Volume 的 Name 匹配。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 为 true 时以只读方式挂载，否则（false 或未指定）以读写方式挂载。
	// 默认为 false。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,2,opt,name=readOnly"`
	// RecursiveReadOnly 指定是否以递归方式处理只读挂载。
	//
	// 如果 ReadOnly 为 false，此字段无意义且必须不指定。
	//
	// 如果 ReadOnly 为 true 且此字段设为 Disabled，则挂载不会递归只读。
	// 如果设为 IfPossible，且容器运行时支持，则挂载会递归只读。
	// 如果设为 Enabled，且容器运行时支持，则挂载会递归只读；
	// 否则 Pod 不会启动，并会生成指明原因的错误。
	//
	// 如果此字段设为 IfPossible 或 Enabled，MountPropagation 必须设为
	// None（或不指定，默认为 None）。
	//
	// 如果未指定此字段，则视为等同于 Disabled。
	// +optional
	RecursiveReadOnly *RecursiveReadOnlyMode `json:"recursiveReadOnly,omitempty" protobuf:"bytes,7,opt,name=recursiveReadOnly,casttype=RecursiveReadOnlyMode"`
	// 容器内挂载卷所在的路径。
	MountPath string `json:"mountPath" protobuf:"bytes,3,opt,name=mountPath"`
	// 卷内要挂载到容器的路径。
	// 默认为 ""（卷的根目录）。
	// +optional
	SubPath string `json:"subPath,omitempty" protobuf:"bytes,4,opt,name=subPath"`
	// mountPropagation 决定挂载如何从主机传播到容器，以及反向传播。
	// 未设置时使用 MountPropagationNone。
	// 此字段在 1.10 中为 beta。
	// 当 RecursiveReadOnly 设为 IfPossible 或 Enabled 时，MountPropagation 必须为 None 或不指定
	// （默认为 None）。
	// +optional
	MountPropagation *MountPropagationMode `json:"mountPropagation,omitempty" protobuf:"bytes,5,opt,name=mountPropagation,casttype=MountPropagationMode"`
	// 卷内要挂载到容器的扩展路径。
	// 行为类似 SubPath，但环境变量引用 $(VAR_NAME) 会使用容器的环境变量展开。
	// 默认为 ""（卷的根目录）。
	// SubPathExpr 与 SubPath 互斥。
	// +optional
	SubPathExpr string `json:"subPathExpr,omitempty" protobuf:"bytes,6,opt,name=subPathExpr"`
	// bindMountOptions 是挂载此卷到容器时要应用的额外绑定挂载选项列表。
	// 允许的值为 noexec、nodev 和 nosuid。这些是 Linux 挂载选项，
	// 在 Windows 节点上无效。
	// 此字段不支持镜像卷。
	// 这是 alpha 字段，需要启用 VolumeBindMountOptions 特性门控。
	// +featureGate=VolumeBindMountOptions
	// +optional
	// +listType=set
	BindMountOptions []string `json:"bindMountOptions,omitempty" protobuf:"bytes,8,rep,name=bindMountOptions"`
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
// +enum
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

// volumeDevice 描述容器内原始块设备的映射。
type VolumeDevice struct {
	// name 必须与 Pod 中某个 persistentVolumeClaim 的名称匹配。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// devicePath 是设备将映射到的容器内路径。
	DevicePath string `json:"devicePath" protobuf:"bytes,2,opt,name=devicePath"`
}

// EnvVar 表示容器中存在的一个环境变量。
type EnvVar struct {
	// 环境变量的名称。
	// 可以包含除 '=' 以外的任意可打印 ASCII 字符。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// 可选：以下内容最多只能指定一项。

	// 变量引用 $(VAR_NAME) 会使用
	// 容器中先前定义的环境变量以及任何 Service 环境变量进行展开。
	// 如果某个变量无法解析，输入字符串中的引用将保持不变。
	// 双写 $$ 会缩减为单个 $，从而可以转义 $(VAR_NAME) 语法：
	// 即 "$$(VAR_NAME)" 会产生字符串字面量 "$(VAR_NAME)"。
	// 转义后的引用永远不会被展开，无论变量是否存在。
	// 默认为 ""。
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`
	// 环境变量值的来源。如果 value 非空，则不能使用此字段。
	// +optional
	ValueFrom *EnvVarSource `json:"valueFrom,omitempty" protobuf:"bytes,3,opt,name=valueFrom"`
}

// EnvVarSource 表示 EnvVar 值的来源。
type EnvVarSource struct {
	// 选择 Pod 的一个字段：支持 metadata.name、metadata.namespace、
	// `metadata.labels['<KEY>']`、`metadata.annotations['<KEY>']`、
	// spec.nodeName、spec.serviceAccountName、status.hostIP、status.podIP、status.podIPs。
	// +optional
	FieldRef *ObjectFieldSelector `json:"fieldRef,omitempty" protobuf:"bytes,1,opt,name=fieldRef"`
	// 选择容器的一个资源：目前仅支持资源限制和请求
	// （limits.cpu、limits.memory、limits.ephemeral-storage、requests.cpu、requests.memory 和 requests.ephemeral-storage）。
	// +optional
	ResourceFieldRef *ResourceFieldSelector `json:"resourceFieldRef,omitempty" protobuf:"bytes,2,opt,name=resourceFieldRef"`
	// 选择 ConfigMap 的一个键。
	// +optional
	ConfigMapKeyRef *ConfigMapKeySelector `json:"configMapKeyRef,omitempty" protobuf:"bytes,3,opt,name=configMapKeyRef"`
	// 选择 Pod 命名空间中某个 Secret 的一个键。
	// +optional
	SecretKeyRef *SecretKeySelector `json:"secretKeyRef,omitempty" protobuf:"bytes,4,opt,name=secretKeyRef"`
	// FileKeyRef 选择 env 文件中的一个键。
	// 需要启用 EnvFiles 特性门控。
	//
	// +featureGate=EnvFiles
	// +optional
	FileKeyRef *FileKeySelector `json:"fileKeyRef,omitempty" protobuf:"bytes,5,opt,name=fileKeyRef"`
}

// FileKeySelector 选择 env 文件中的一个键。
// +structType=atomic
type FileKeySelector struct {
	// 包含该 env 文件的卷挂载的名称。
	// +required
	VolumeName string `json:"volumeName" protobuf:"bytes,1,opt,name=volumeName"`
	// 卷内要选择文件的路径。
	// 必须是相对路径，且不能包含 '..' 路径或以 '..' 开头。
	// +required
	Path string `json:"path" protobuf:"bytes,2,opt,name=path"`
	// env 文件中的键。无效的键会阻止 Pod 启动。
	// 来源中定义的键可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// 在 EnvFiles 特性门控的 Alpha 阶段，键的长度限制为 128 个字符。
	// +required
	Key string `json:"key" protobuf:"bytes,3,opt,name=key"`
	// 指定是否必须定义文件或其键。如果文件或键不存在，
	// 则不会发布该环境变量。
	// 如果 optional 设为 true 且指定的键不存在，
	// 则不会在 Pod 的容器中设置该环境变量。
	//
	// 如果 optional 设为 false 且指定的键不存在，
	// 则在创建 Pod 时会返回错误。
	// +optional
	// +default=false
	Optional *bool `json:"optional,omitempty" protobuf:"varint,4,opt,name=optional"`
}

// ObjectFieldSelector 选择对象的一个带 API 版本的字段。
// +structType=atomic
type ObjectFieldSelector struct {
	// FieldPath 所依据的 schema 版本，默认为 "v1"。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,1,opt,name=apiVersion"`
	// 在指定 API 版本中选择字段的路径。
	FieldPath string `json:"fieldPath" protobuf:"bytes,2,opt,name=fieldPath"`
}

// ResourceFieldSelector 表示容器资源（cpu、memory）及其输出格式。
// +structType=atomic
type ResourceFieldSelector struct {
	// 容器名称：卷中必填，环境变量中可选。
	// +optional
	ContainerName string `json:"containerName,omitempty" protobuf:"bytes,1,opt,name=containerName"`
	// 必填：要选择的资源。
	Resource string `json:"resource" protobuf:"bytes,2,opt,name=resource"`
	// 指定所暴露资源的输出格式，默认为 "1"。
	// +optional
	Divisor resource.Quantity `json:"divisor,omitempty" protobuf:"bytes,3,opt,name=divisor"`
}

// ConfigMapKeySelector 从 ConfigMap 中选择一个键。
// +structType=atomic
type ConfigMapKeySelector struct {
	// 要选择的 ConfigMap。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// 要从 ConfigMap 的 Data 字段中选择的键。
	// BinaryData 字段中的键目前不会传播到容器环境变量。
	Key string `json:"key" protobuf:"bytes,2,opt,name=key"`
	// 指定是否必须定义 ConfigMap 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,3,opt,name=optional"`
}

// SecretKeySelector 选择 Secret 的一个键。
// +structType=atomic
type SecretKeySelector struct {
	// 要选择 Pod 命名空间中 Secret 的名称。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// 要选择的 Secret 的键。必须是有效的 Secret 键。
	Key string `json:"key" protobuf:"bytes,2,opt,name=key"`
	// 指定是否必须定义 Secret 或其键。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,3,opt,name=optional"`
}

// EnvFromSource 表示一组 ConfigMap 或 Secret 的来源。
type EnvFromSource struct {
	// 可选：在每个环境变量名称前添加的前缀文本。
	// 可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// +optional
	Prefix string `json:"prefix,omitempty" protobuf:"bytes,1,opt,name=prefix"`
	// 要选择的 ConfigMap。
	// +optional
	ConfigMapRef *ConfigMapEnvSource `json:"configMapRef,omitempty" protobuf:"bytes,2,opt,name=configMapRef"`
	// 要选择的 Secret。
	// +optional
	SecretRef *SecretEnvSource `json:"secretRef,omitempty" protobuf:"bytes,3,opt,name=secretRef"`
}

// ConfigMapEnvSource 选择一个 ConfigMap 来填充环境变量。
//
// 目标 ConfigMap 的 Data 字段内容将以键值对的形式作为环境变量。
// BinaryData 字段中的键目前不会传播到容器环境变量。
type ConfigMapEnvSource struct {
	// 要选择的 ConfigMap。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// 指定是否必须定义 ConfigMap。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,2,opt,name=optional"`
}

// SecretEnvSource 选择一个 Secret 来填充环境变量。
//
// 目标 Secret 的 Data 字段内容将以键值对的形式作为环境变量。
type SecretEnvSource struct {
	// 要选择的 Secret。
	LocalObjectReference `json:"" protobuf:"bytes,1,opt,name=localObjectReference"`
	// 指定是否必须定义 Secret。
	// +optional
	Optional *bool `json:"optional,omitempty" protobuf:"varint,2,opt,name=optional"`
}

// HTTPHeader 描述要在 HTTP 探针中使用的自定义请求头。
type HTTPHeader struct {
	// 请求头字段名。
	// 输出时会被规范化，因此大小写不同的名称会被视为同一个请求头。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 请求头字段值。
	Value string `json:"value" protobuf:"bytes,2,opt,name=value"`
}

// HTTPProtocol 选择 HTTP 探针的线协议，与 URI scheme 无关。
// +enum
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
	// HTTP 服务器上要访问的路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
	// 容器上要访问的端口号或名称。
	// 数字必须在 1 到 65535 范围内。
	// 名称必须是 IANA_SVC_NAME。
	Port intstr.IntOrString `json:"port" protobuf:"bytes,2,opt,name=port"`
	// 要连接到的主机名，默认为 Pod IP。通常你可能想在 httpHeaders 中设置 "Host"。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,3,opt,name=host"`
	// 连接主机使用的 scheme。
	// 默认为 HTTP。
	// +optional
	Scheme URIScheme `json:"scheme,omitempty" protobuf:"bytes,4,opt,name=scheme,casttype=URIScheme"`
	// 请求中要设置的自定义请求头。HTTP 允许重复的请求头。
	// +optional
	// +listType=atomic
	HTTPHeaders []HTTPHeader `json:"httpHeaders,omitempty" protobuf:"bytes,5,rep,name=httpHeaders"`
	// Protocol 选择探针连接的线协议。
	// 为 nil 时默认为 HTTP/1.1。
	// +optional
	// +featureGate=H2CContainerProbe
	Protocol *HTTPProtocol `json:"protocol,omitempty" protobuf:"bytes,6,opt,name=protocol,casttype=HTTPProtocol"`
}

// URIScheme 标识 Get 动作连接主机时使用的 scheme。
// +enum
type URIScheme string

const (
	// URISchemeHTTP 表示使用的 scheme 为 http://
	URISchemeHTTP URIScheme = "HTTP"
	// URISchemeHTTPS 表示使用的 scheme 为 https://
	URISchemeHTTPS URIScheme = "HTTPS"
)

// TCPSocketAction 描述基于打开 socket 的动作。
type TCPSocketAction struct {
	// 容器上要访问的端口的编号或名称。
	// 数字必须在 1 到 65535 范围内。
	// 名称必须是 IANA_SVC_NAME。
	Port intstr.IntOrString `json:"port" protobuf:"bytes,1,opt,name=port"`
	// 可选：要连接到的主机名，默认为 Pod IP。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,2,opt,name=host"`
}

// GRPCAction 指定涉及 GRPC 服务的动作。
type GRPCAction struct {
	// gRPC 服务的端口号。数字必须在 1 到 65535 范围内。
	Port int32 `json:"port" protobuf:"bytes,1,opt,name=port"`

	// Service 是要放入 gRPC HealthCheckRequest 的服务名称
	// （参见 https://github.com/grpc/grpc/blob/master/doc/health-checking.md）。
	//
	// 如果未指定，默认行为由 gRPC 定义。
	// +optional
	// +default=""
	Service *string `json:"service" protobuf:"bytes,2,opt,name=service"`

	// mode 指定 gRPC 健康检查探针的连接模式。
	// 设为 "TLS" 时使用 TLS 但不校验证书。
	// 设为 "Plaintext" 时显式使用明文（不安全）连接。
	// 如果未指定，探针使用明文（不安全）连接。
	// +featureGate=GRPCContainerProbeTLS
	// +optional
	Mode *GRPCProbeMode `json:"mode,omitempty" protobuf:"bytes,3,opt,name=mode,casttype=GRPCProbeMode"`
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

// ExecAction 描述"在容器内运行"的动作。
type ExecAction struct {
	// Command 是要在容器内执行的命令行，命令的工作目录
	// 是容器文件系统的根目录（'/'）。命令只是被直接 exec，
	// 并不在 shell 中运行，因此传统的 shell 指令（'|' 等）不起作用。
	// 若要使用 shell，需要显式调用该 shell。
	// 退出状态 0 视为存活/健康，非零视为不健康。
	// +optional
	// +listType=atomic
	Command []string `json:"command,omitempty" protobuf:"bytes,1,rep,name=command"`
}

// SleepAction 描述"休眠"动作。
type SleepAction struct {
	// Seconds 是要休眠的秒数。
	Seconds int64 `json:"seconds" protobuf:"bytes,1,opt,name=seconds"`
}

// Probe 描述对容器执行的健康检查，以确定其是否存活或已就绪可接收流量。
type Probe struct {
	// 用于判断容器健康状况的动作
	ProbeHandler `json:"" protobuf:"bytes,1,opt,name=handler"`
	// 容器启动后，在开始执行存活探针之前等待的秒数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	InitialDelaySeconds int32 `json:"initialDelaySeconds,omitempty" protobuf:"varint,2,opt,name=initialDelaySeconds"`
	// 探针超时的秒数。
	// 默认为 1 秒。最小值为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,3,opt,name=timeoutSeconds"`
	// 执行探针的频率（以秒为单位）。
	// 默认为 10 秒。最小值为 1。
	// +optional
	PeriodSeconds int32 `json:"periodSeconds,omitempty" protobuf:"varint,4,opt,name=periodSeconds"`
	// 探针在失败后被认定为成功所需的最小连续成功次数。
	// 默认为 1。存活探针和启动探针必须为 1。最小值为 1。
	// +optional
	SuccessThreshold int32 `json:"successThreshold,omitempty" protobuf:"varint,5,opt,name=successThreshold"`
	// 探针在成功后被认定为失败所需的最小连续失败次数。
	// 默认为 3。最小值为 1。
	// +optional
	FailureThreshold int32 `json:"failureThreshold,omitempty" protobuf:"varint,6,opt,name=failureThreshold"`
	// 探针失败时 Pod 需要优雅终止的可选时长（秒）。
	// 优雅期限是指从向 Pod 中运行的进程发送终止信号，
	// 到进程被 kill 信号强制停止之间的秒数。
	// 请将此值设置为长于进程的预期清理时间。
	// 如果此值为 nil，将使用 Pod 的 terminationGracePeriodSeconds；
	// 否则此值会覆盖 Pod spec 中提供的值。
	// 值必须是非负整数。值为 0 表示立即通过 kill 信号停止
	// （没有关停机会）。
	// 这是 beta 字段，需要启用 ProbeTerminationGracePeriod 特性门控。
	// 最小值为 1。未设置时使用 spec.terminationGracePeriodSeconds。
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,7,opt,name=terminationGracePeriodSeconds"`
}

// PullPolicy 描述是否/何时拉取容器镜像的策略。
// +enum
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
	// 'NotRequired' 表示 Kubernetes 会在可能的情况下尝试
	// 在不重启容器的情况下调整其大小。但如果无法在不重启的情况下
	// 完成调整，Kubernetes 可能会选择重启容器，
	// 例如运行时不支持免重启调整。
	NotRequired ResourceResizeRestartPolicy = "NotRequired"
	// 'RestartContainer' 表示 Kubernetes 会在应用新资源时
	// 通过停止并启动容器来原地调整容器大小。
	// 旧版应用需要此方式，例如使用 -xmxN 标志的 Java 应用，
	// 它们无法在不重启的情况下使用调整后的内存。
	RestartContainer ResourceResizeRestartPolicy = "RestartContainer"
)

// ContainerResizePolicy 表示容器的资源调整策略。
type ContainerResizePolicy struct {
	// 此资源调整策略适用的资源名称。
	// 支持的值：cpu、memory。
	ResourceName ResourceName `json:"resourceName" protobuf:"bytes,1,opt,name=resourceName,casttype=ResourceName"`
	// 调整指定资源时要应用的重启策略。
	// 如果未指定，默认为 NotRequired。
	RestartPolicy ResourceResizeRestartPolicy `json:"restartPolicy" protobuf:"bytes,2,opt,name=restartPolicy,casttype=ResourceResizeRestartPolicy"`
}

// PreemptionPolicy 描述是否/何时抢占 Pod 的策略。
// +enum
type PreemptionPolicy string

const (
	// PreemptLowerPriority 表示 Pod 可以抢占优先级更低的 Pod。
	PreemptLowerPriority PreemptionPolicy = "PreemptLowerPriority"
	// PreemptNever 表示 Pod 从不抢占其他优先级更低的 Pod。
	PreemptNever PreemptionPolicy = "Never"
)

// TerminationMessagePolicy 描述如何从容器获取终止消息。
// +enum
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

// Capability 表示 POSIX capabilities 类型。
type Capability string

// Capabilities 用于在运行中的容器上添加和移除 POSIX capabilities。
type Capabilities struct {
	// 添加的 capabilities
	// +optional
	// +listType=atomic
	Add []Capability `json:"add,omitempty" protobuf:"bytes,1,rep,name=add,casttype=Capability"`
	// 移除的 capabilities
	// +optional
	// +listType=atomic
	Drop []Capability `json:"drop,omitempty" protobuf:"bytes,2,rep,name=drop,casttype=Capability"`
}

// ResourceRequirements 描述计算资源需求。
type ResourceRequirements struct {
	// Limits 描述允许使用的计算资源上限。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Limits ResourceList `json:"limits,omitempty" protobuf:"bytes,1,rep,name=limits,casttype=ResourceList,castkey=ResourceName"`
	// Requests 描述所需计算资源的最小值。
	// 如果容器省略 Requests，则默认为显式指定的 Limits，
	// 否则为具体实现定义的值。Requests 不能超过 Limits。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Requests ResourceList `json:"requests,omitempty" protobuf:"bytes,2,rep,name=requests,casttype=ResourceList,castkey=ResourceName"`
	// Claims 列出了 spec.resourceClaims 中定义的、
	// 由此容器使用的资源名称。
	//
	// 此字段依赖 DynamicResourceAllocation 特性门控。
	//
	// 此字段不可变，且只能为容器设置。
	//
	// +listType=map
	// +listMapKey=name
	// +featureGate=DynamicResourceAllocation
	// +optional
	Claims []ResourceClaim `json:"claims,omitempty" protobuf:"bytes,3,opt,name=claims"`
}

// VolumeResourceRequirements 描述卷的存储资源需求。
type VolumeResourceRequirements struct {
	// Limits 描述允许使用的计算资源上限。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Limits ResourceList `json:"limits,omitempty" protobuf:"bytes,1,rep,name=limits,casttype=ResourceList,castkey=ResourceName"`
	// Requests 描述所需计算资源的最小值。
	// 如果容器省略 Requests，则默认为显式指定的 Limits，
	// 否则为具体实现定义的值。Requests 不能超过 Limits。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Requests ResourceList `json:"requests,omitempty" protobuf:"bytes,2,rep,name=requests,casttype=ResourceList,castkey=ResourceName"`

	// Claims 是在卷与容器共享 ResourceRequirements 结构体时误加进来的。
	// 移除该字段的改动在 1.27 中加入，并回溯到了 1.26。
	// 从 Kubernetes 1.28 开始，此字段不再属于卷 API。
	//
	// 未来的扩展不得使用 "claims" 或字段编号 3。
	// Claims []ResourceClaim `json:"claims,omitempty" protobuf:"bytes,3,opt,name=claims"`
}

// ResourceClaim 引用 PodSpec.ResourceClaims 中的一个条目。
type ResourceClaim struct {
	// Name 必须与使用该字段的 Pod 的 pod.spec.resourceClaims 中某个条目的名称匹配。
	// 它使该资源在容器内可用。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// Request 是所引用 claim 中某个请求的名称。
	// 如果为空，则 claim 中的全部内容均可用；否则只有该请求的结果可用。
	//
	// +optional
	Request string `json:"request,omitempty" protobuf:"bytes,2,opt,name=request"`
}

const (
	// TerminationMessagePathDefault 表示捕获容器中运行的应用程序终止消息的默认路径。
	TerminationMessagePathDefault string = "/dev/termination-log"
)

// Container 是希望在 Pod 中运行的一个应用程序容器。
type Container struct {
	// 容器名称，指定为 DNS_LABEL。
	// Pod 中每个容器都必须有唯一的名称（DNS_LABEL）。
	// 不可更新。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 容器镜像名称。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images
	// 此字段可选，以便更高层的配置管理能够在 Deployment 和 StatefulSet 等工作负载控制器中
	// 为容器镜像设置默认值或进行覆盖。
	// +optional
	Image string `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	// 入口点数组。不在 shell 中执行。
	// 如果未提供，则使用容器镜像的 ENTRYPOINT。
	// 变量引用 $(VAR_NAME) 会使用容器的环境变量展开。如果某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会缩减为
	// 单个 $，从而可以转义 $(VAR_NAME) 语法：即 "$$(VAR_NAME)" 会
	// 产生字符串字面量 "$(VAR_NAME)"。转义后的引用永远不会被展开，
	// 无论变量是否存在。不可更新。
	// 更多信息：https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
	// +optional
	// +listType=atomic
	Command []string `json:"command,omitempty" protobuf:"bytes,3,rep,name=command"`
	// 入口点的参数。
	// 如果未提供，则使用容器镜像的 CMD。
	// 变量引用 $(VAR_NAME) 会使用容器的环境变量展开。如果某个变量
	// 无法解析，输入字符串中的引用将保持不变。双写 $$ 会缩减为
	// 单个 $，从而可以转义 $(VAR_NAME) 语法：即 "$$(VAR_NAME)" 会
	// 产生字符串字面量 "$(VAR_NAME)"。转义后的引用永远不会被展开，
	// 无论变量是否存在。不可更新。
	// 更多信息：https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
	// +optional
	// +listType=atomic
	Args []string `json:"args,omitempty" protobuf:"bytes,4,rep,name=args"`
	// 容器工作目录。
	// 如果未指定，将使用容器运行时的默认值，
	// 该值可能在容器镜像中配置。
	// 不可更新。
	// +optional
	WorkingDir string `json:"workingDir,omitempty" protobuf:"bytes,5,opt,name=workingDir"`
	// 要从容器暴露的端口列表。此处不指定端口
	// 并*不*会阻止该端口被暴露。容器内监听默认 "0.0.0.0" 地址的
	// 任何端口都可以从网络访问。
	// 使用 strategic merge patch 修改此数组可能会损坏数据。
	// 更多信息参见 https://github.com/kubernetes/kubernetes/issues/108255.
	// 不可更新。
	// +optional
	// +patchMergeKey=containerPort
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=containerPort
	// +listMapKey=protocol
	Ports []ContainerPort `json:"ports,omitempty" patchStrategy:"merge" patchMergeKey:"containerPort" protobuf:"bytes,6,rep,name=ports"`
	// 用于填充容器中环境变量的来源列表。
	// 来源中定义的键可以包含除 '=' 以外的任意可打印 ASCII 字符。
	// 当某个键存在于多个来源时，
	// 最后一个来源关联的值优先。
	// 由带有重复键的 Env 定义的值优先。
	// 不可更新。
	// +optional
	// +listType=atomic
	EnvFrom []EnvFromSource `json:"envFrom,omitempty" protobuf:"bytes,19,rep,name=envFrom"`
	// 要在容器中设置的环境变量列表。
	// 不可更新。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Env []EnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=env"`
	// 此容器所需的计算资源。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	// +optional
	Resources ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`
	// 容器的资源调整策略。
	// 此字段不能在临时容器上设置。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	// +listType=atomic
	ResizePolicy []ContainerResizePolicy `json:"resizePolicy,omitempty" protobuf:"bytes,23,rep,name=resizePolicy"`
	// RestartPolicy 定义 Pod 中单个容器的重启行为。
	// 它会覆盖 Pod 级别的重启策略。未指定此字段时，
	// 重启行为由 Pod 的重启策略和容器类型决定。
	// 此外，将 init 容器的 RestartPolicy 设为 "Always" 会产生以下效果：
	// 该 init 容器会在退出后被持续重启，
	// 直到所有普通容器都终止。一旦所有普通容器都完成，
	// 所有 restartPolicy 为 "Always" 的 init 容器都会被关闭。
	// 此生命周期与普通 init 容器不同，通常称为 "sidecar" 容器。
	// 尽管此 init 容器仍在 init 容器序列中启动，但它不会等待
	// 该容器完成后再进入下一个 init 容器。
	// 相反，下一个 init 容器会在此 init 容器启动后立即启动，
	// 或在任何 startupProbe 成功完成后启动。
	// +optional
	RestartPolicy *ContainerRestartPolicy `json:"restartPolicy,omitempty" protobuf:"bytes,24,opt,name=restartPolicy,casttype=ContainerRestartPolicy"`
	// 表示要检查的规则列表，用于判断容器退出时是否应重启。
	// 规则按顺序评估。一旦某条规则匹配了容器退出条件，其余规则将被忽略。
	// 如果没有规则匹配容器退出条件，则由容器级别的重启策略决定
	// 是否重启容器。对规则的限制：
	// - 最多允许 20 条规则。
	// - 规则可以有相同的 action。
	// - 校验中并不禁止完全相同的规则。
	// 指定规则时，容器必须显式设置 RestartPolicy，
	// 即使它与 Pod 的 RestartPolicy 一致。
	// +featureGate=ContainerRestartRules
	// +optional
	// +listType=atomic
	RestartPolicyRules []ContainerRestartRule `json:"restartPolicyRules,omitempty" protobuf:"bytes,25,rep,name=restartPolicyRules"`
	// 要挂载到容器文件系统中的 Pod 卷。
	// 不可更新。
	// +optional
	// +patchMergeKey=mountPath
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=mountPath
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,9,rep,name=volumeMounts"`
	// volumeDevices 是容器要使用的块设备列表。
	// +patchMergeKey=devicePath
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=devicePath
	// +optional
	VolumeDevices []VolumeDevice `json:"volumeDevices,omitempty" patchStrategy:"merge" patchMergeKey:"devicePath" protobuf:"bytes,21,rep,name=volumeDevices"`
	// 容器存活性的周期性探针。
	// 探针失败时容器将被重启。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	LivenessProbe *Probe `json:"livenessProbe,omitempty" protobuf:"bytes,10,opt,name=livenessProbe"`
	// 容器服务就绪性的周期性探针。
	// 探针失败时容器将从 Service 端点中移除。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	ReadinessProbe *Probe `json:"readinessProbe,omitempty" protobuf:"bytes,11,opt,name=readinessProbe"`
	// StartupProbe 表示 Pod 已成功初始化。
	// 如果指定，在此探针成功完成之前不会执行其他探针。
	// 如果此探针失败，Pod 将被重启，就像 livenessProbe 失败一样。
	// 这可用于在 Pod 生命周期开始时提供与稳态运行不同的探针参数，
	// 例如加载数据或预热缓存可能需要较长时间。
	// 此项不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#container-probes
	// +optional
	StartupProbe *Probe `json:"startupProbe,omitempty" protobuf:"bytes,22,opt,name=startupProbe"`
	// 管理系统应针对容器生命周期事件采取的动作。
	// 不可更新。
	// +optional
	Lifecycle *Lifecycle `json:"lifecycle,omitempty" protobuf:"bytes,12,opt,name=lifecycle"`
	// 可选：容器终止消息要写入的文件挂载到容器文件系统中的路径。
	// 写入的消息应为简短的最终状态，例如断言失败消息。
	// 超过 4096 字节时会被节点截断。所有容器的消息总长度限制为 12kb。
	// 默认为 /dev/termination-log。
	// 不可更新。
	// +optional
	TerminationMessagePath string `json:"terminationMessagePath,omitempty" protobuf:"bytes,13,opt,name=terminationMessagePath"`
	// 指明应如何填充终止消息。File 会在成功和失败时都使用
	// terminationMessagePath 的内容来填充容器状态消息。
	// FallbackToLogsOnError 会在终止消息文件为空且容器以错误退出时，
	// 使用容器日志输出的最后一块内容。
	// 日志输出限制为 2048 字节或 80 行，取较小者。
	// 默认为 File。
	// 不可更新。
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy `json:"terminationMessagePolicy,omitempty" protobuf:"bytes,20,opt,name=terminationMessagePolicy,casttype=TerminationMessagePolicy"`
	// 镜像拉取策略。
	// 取值为 Always、Never、IfNotPresent 之一。
	// 如果指定了 :latest 标签，默认为 Always，否则为 IfNotPresent。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images#updating-images
	// +optional
	ImagePullPolicy PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	// SecurityContext 定义容器运行时应使用的安全选项。
	// 如果设置，SecurityContext 中的字段会覆盖 PodSecurityContext 中的对应字段。
	// 更多信息：https://kubernetes.io/docs/tasks/configure-pod-container/security-context/
	// +optional
	SecurityContext *SecurityContext `json:"securityContext,omitempty" protobuf:"bytes,15,opt,name=securityContext"`

	// 交互式容器的变量，这些有非常专门的用例（例如调试），
	// 不应用于通用容器。

	// 此容器是否应在容器运行时中为标准输入分配缓冲区。如果未设置，
	// 从容器中 stdin 的读取将总是得到 EOF。
	// 默认为 false。
	// +optional
	Stdin bool `json:"stdin,omitempty" protobuf:"varint,16,opt,name=stdin"`
	// 在单次 attach 打开 stdin 后，容器运行时是否应关闭 stdin 通道。
	// 当 stdin 为 true 时，stdin 流会在多次 attach 会话之间保持打开。
	// 如果 stdinOnce 设为 true，stdin 在容器启动时打开，在第一个客户端 attach 到 stdin 之前为空，
	// 之后保持打开并接收数据，直到客户端断开连接，
	// 此时 stdin 被关闭并保持关闭状态，直到容器重启。如果此
	// 标志为 false，从 stdin 读取的容器进程永远不会收到 EOF。
	// 默认为 false
	// +optional
	StdinOnce bool `json:"stdinOnce,omitempty" protobuf:"varint,17,opt,name=stdinOnce"`
	// 此容器是否应为自身分配一个 TTY，还要求 'stdin' 为 true。
	// 默认为 false。
	// +optional
	TTY bool `json:"tty,omitempty" protobuf:"varint,18,opt,name=tty"`
}

// ProbeHandler 定义探针中要采取的具体动作。
// 必须指定且只能指定其中一个字段。
type ProbeHandler struct {
	// Exec 指定要在容器中执行的命令。
	// +optional
	Exec *ExecAction `json:"exec,omitempty" protobuf:"bytes,1,opt,name=exec"`
	// HTTPGet 指定要执行的 HTTP GET 请求。
	// +optional
	HTTPGet *HTTPGetAction `json:"httpGet,omitempty" protobuf:"bytes,2,opt,name=httpGet"`
	// TCPSocket 指定到 TCP 端口的连接。
	// +optional
	TCPSocket *TCPSocketAction `json:"tcpSocket,omitempty" protobuf:"bytes,3,opt,name=tcpSocket"`
	// GRPC 指定一个 GRPC HealthCheckRequest。
	// +optional
	GRPC *GRPCAction `json:"grpc,omitempty" protobuf:"bytes,4,opt,name=grpc"`
}

// LifecycleHandler 定义生命周期钩子中要采取的具体动作。
// 除 TCPSocket 外，必须指定且只能指定其中一个字段。
type LifecycleHandler struct {
	// Exec 指定要在容器中执行的命令。
	// +optional
	Exec *ExecAction `json:"exec,omitempty" protobuf:"bytes,1,opt,name=exec"`
	// HTTPGet 指定要执行的 HTTP GET 请求。
	// +optional
	HTTPGet *HTTPGetAction `json:"httpGet,omitempty" protobuf:"bytes,2,opt,name=httpGet"`
	// 已弃用。TCPSocket 不作为 LifecycleHandler 支持，仅为
	// 向后兼容而保留。此字段不做校验，指定时生命周期钩子会在运行时失败。
	// +optional
	TCPSocket *TCPSocketAction `json:"tcpSocket,omitempty" protobuf:"bytes,3,opt,name=tcpSocket"`
	// Sleep 表示容器应休眠的时长。
	// +optional
	Sleep *SleepAction `json:"sleep,omitempty" protobuf:"bytes,4,opt,name=sleep"`
}

// Signal 定义容器的停止信号。
// +enum
type Signal string

const (
	// SIGABRT 表示异常终止（abort）信号。
	SIGABRT Signal = "SIGABRT"
	// SIGALRM 表示闹钟（alarm）信号。
	SIGALRM Signal = "SIGALRM"
	// SIGBUS 表示总线错误（bus error）信号。
	SIGBUS Signal = "SIGBUS"
	// SIGCHLD 表示子进程状态改变信号。
	SIGCHLD Signal = "SIGCHLD"
	// SIGCLD 表示子进程状态改变信号（SIGCHLD 的别名）。
	SIGCLD Signal = "SIGCLD"
	// SIGCONT 表示若进程已停止则继续运行。
	SIGCONT Signal = "SIGCONT"
	// SIGFPE 表示浮点异常信号。
	SIGFPE Signal = "SIGFPE"
	// SIGHUP 表示控制终端挂断（hangup）信号。
	SIGHUP Signal = "SIGHUP"
	// SIGILL 表示非法指令信号。
	SIGILL Signal = "SIGILL"
	// SIGINT 表示来自键盘的中断信号。
	SIGINT Signal = "SIGINT"
	// SIGIO 表示文件描述符上可进行 I/O。
	SIGIO Signal = "SIGIO"
	// SIGIOT 表示 I/O 陷阱信号（在 Linux 上同 SIGABRT）。
	SIGIOT Signal = "SIGIOT"
	// SIGKILL 表示强制终止信号，不可被捕获或忽略。
	SIGKILL Signal = "SIGKILL"
	// SIGPIPE 表示向无读端的管道写入数据。
	SIGPIPE Signal = "SIGPIPE"
	// SIGPOLL 表示可轮询事件信号。
	SIGPOLL Signal = "SIGPOLL"
	// SIGPROF 表示性能分析定时器到期信号。
	SIGPROF Signal = "SIGPROF"
	// SIGPWR 表示电源故障信号。
	SIGPWR Signal = "SIGPWR"
	// SIGQUIT 表示来自键盘的退出信号。
	SIGQUIT Signal = "SIGQUIT"
	// SIGSEGV 表示无效内存引用（段错误）信号。
	SIGSEGV Signal = "SIGSEGV"
	// SIGSTKFLT 表示栈错误信号。
	SIGSTKFLT Signal = "SIGSTKFLT"
	// SIGSTOP 表示停止进程，不可被捕获或忽略。
	SIGSTOP Signal = "SIGSTOP"
	// SIGSYS 表示系统调用参数错误信号。
	SIGSYS Signal = "SIGSYS"
	// SIGTERM 表示终止信号。
	SIGTERM Signal = "SIGTERM"
	// SIGTRAP 表示跟踪/断点陷阱信号。
	SIGTRAP Signal = "SIGTRAP"
	// SIGTSTP 表示来自终端的停止信号。
	SIGTSTP Signal = "SIGTSTP"
	// SIGTTIN 表示后台进程从终端读取。
	SIGTTIN Signal = "SIGTTIN"
	// SIGTTOU 表示后台进程向终端写入。
	SIGTTOU Signal = "SIGTTOU"
	// SIGURG 表示紧急 I/O 条件信号。
	SIGURG Signal = "SIGURG"
	// SIGUSR1 表示用户自定义信号 1。
	SIGUSR1 Signal = "SIGUSR1"
	// SIGUSR2 表示用户自定义信号 2。
	SIGUSR2 Signal = "SIGUSR2"
	// SIGVTALRM 表示虚拟定时器到期信号。
	SIGVTALRM Signal = "SIGVTALRM"
	// SIGWINCH 表示终端窗口大小改变信号。
	SIGWINCH Signal = "SIGWINCH"
	// SIGXCPU 表示超过 CPU 时间限制信号。
	SIGXCPU Signal = "SIGXCPU"
	// SIGXFSZ 表示超过文件大小限制信号。
	SIGXFSZ Signal = "SIGXFSZ"
	// SIGRTMIN 表示最小实时信号。
	SIGRTMIN Signal = "SIGRTMIN"
	// SIGRTMINPLUS1 表示实时信号 SIGRTMIN+1。
	SIGRTMINPLUS1 Signal = "SIGRTMIN+1"
	// SIGRTMINPLUS2 表示实时信号 SIGRTMIN+2。
	SIGRTMINPLUS2 Signal = "SIGRTMIN+2"
	// SIGRTMINPLUS3 表示实时信号 SIGRTMIN+3。
	SIGRTMINPLUS3 Signal = "SIGRTMIN+3"
	// SIGRTMINPLUS4 表示实时信号 SIGRTMIN+4。
	SIGRTMINPLUS4 Signal = "SIGRTMIN+4"
	// SIGRTMINPLUS5 表示实时信号 SIGRTMIN+5。
	SIGRTMINPLUS5 Signal = "SIGRTMIN+5"
	// SIGRTMINPLUS6 表示实时信号 SIGRTMIN+6。
	SIGRTMINPLUS6 Signal = "SIGRTMIN+6"
	// SIGRTMINPLUS7 表示实时信号 SIGRTMIN+7。
	SIGRTMINPLUS7 Signal = "SIGRTMIN+7"
	// SIGRTMINPLUS8 表示实时信号 SIGRTMIN+8。
	SIGRTMINPLUS8 Signal = "SIGRTMIN+8"
	// SIGRTMINPLUS9 表示实时信号 SIGRTMIN+9。
	SIGRTMINPLUS9 Signal = "SIGRTMIN+9"
	// SIGRTMINPLUS10 表示实时信号 SIGRTMIN+10。
	SIGRTMINPLUS10 Signal = "SIGRTMIN+10"
	// SIGRTMINPLUS11 表示实时信号 SIGRTMIN+11。
	SIGRTMINPLUS11 Signal = "SIGRTMIN+11"
	// SIGRTMINPLUS12 表示实时信号 SIGRTMIN+12。
	SIGRTMINPLUS12 Signal = "SIGRTMIN+12"
	// SIGRTMINPLUS13 表示实时信号 SIGRTMIN+13。
	SIGRTMINPLUS13 Signal = "SIGRTMIN+13"
	// SIGRTMINPLUS14 表示实时信号 SIGRTMIN+14。
	SIGRTMINPLUS14 Signal = "SIGRTMIN+14"
	// SIGRTMINPLUS15 表示实时信号 SIGRTMIN+15。
	SIGRTMINPLUS15 Signal = "SIGRTMIN+15"
	// SIGRTMAXMINUS14 表示实时信号 SIGRTMAX-14。
	SIGRTMAXMINUS14 Signal = "SIGRTMAX-14"
	// SIGRTMAXMINUS13 表示实时信号 SIGRTMAX-13。
	SIGRTMAXMINUS13 Signal = "SIGRTMAX-13"
	// SIGRTMAXMINUS12 表示实时信号 SIGRTMAX-12。
	SIGRTMAXMINUS12 Signal = "SIGRTMAX-12"
	// SIGRTMAXMINUS11 表示实时信号 SIGRTMAX-11。
	SIGRTMAXMINUS11 Signal = "SIGRTMAX-11"
	// SIGRTMAXMINUS10 表示实时信号 SIGRTMAX-10。
	SIGRTMAXMINUS10 Signal = "SIGRTMAX-10"
	// SIGRTMAXMINUS9 表示实时信号 SIGRTMAX-9。
	SIGRTMAXMINUS9 Signal = "SIGRTMAX-9"
	// SIGRTMAXMINUS8 表示实时信号 SIGRTMAX-8。
	SIGRTMAXMINUS8 Signal = "SIGRTMAX-8"
	// SIGRTMAXMINUS7 表示实时信号 SIGRTMAX-7。
	SIGRTMAXMINUS7 Signal = "SIGRTMAX-7"
	// SIGRTMAXMINUS6 表示实时信号 SIGRTMAX-6。
	SIGRTMAXMINUS6 Signal = "SIGRTMAX-6"
	// SIGRTMAXMINUS5 表示实时信号 SIGRTMAX-5。
	SIGRTMAXMINUS5 Signal = "SIGRTMAX-5"
	// SIGRTMAXMINUS4 表示实时信号 SIGRTMAX-4。
	SIGRTMAXMINUS4 Signal = "SIGRTMAX-4"
	// SIGRTMAXMINUS3 表示实时信号 SIGRTMAX-3。
	SIGRTMAXMINUS3 Signal = "SIGRTMAX-3"
	// SIGRTMAXMINUS2 表示实时信号 SIGRTMAX-2。
	SIGRTMAXMINUS2 Signal = "SIGRTMAX-2"
	// SIGRTMAXMINUS1 表示实时信号 SIGRTMAX-1。
	SIGRTMAXMINUS1 Signal = "SIGRTMAX-1"
	// SIGRTMAX 表示最大实时信号。
	SIGRTMAX Signal = "SIGRTMAX"
)

// Lifecycle 描述管理系统应针对容器生命周期事件采取的动作。
// 对于 PostStart 和 PreStop 生命周期钩子，除非容器进程失败（此时钩子被中止），
// 否则对容器的管理会阻塞直到动作完成。
type Lifecycle struct {
	// PostStart 在容器创建后立即调用。如果钩子失败，
	// 容器将根据其重启策略被终止并重启。
	// 对容器的其他管理会阻塞直到钩子完成。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PostStart *LifecycleHandler `json:"postStart,omitempty" protobuf:"bytes,1,opt,name=postStart"`
	// PreStop 在容器因 API 请求或管理事件（如存活/启动探针失败、
	// 抢占、资源争用等）而即将被终止前立即调用。如果容器崩溃或退出，则不会调用此钩子。
	// Pod 的终止宽限期倒计时在 PreStop 钩子执行之前就开始。
	// 无论钩子的结果如何，容器最终都会在 Pod 的终止宽限期内终止
	// （除非被 finalizer 延迟）。对容器的其他管理会阻塞直到钩子完成
	// 或达到终止宽限期。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks
	// +optional
	PreStop *LifecycleHandler `json:"preStop,omitempty" protobuf:"bytes,2,opt,name=preStop"`
	// StopSignal 定义容器被停止时要发送的信号。
	// 如果未指定，默认值由所使用的容器运行时定义。
	// StopSignal 只能为 .spec.os.name 非空的 Pod 设置。
	// +optional
	StopSignal *Signal `json:"stopSignal,omitempty" protobuf:"bytes,3,opt,name=stopSignal"`
}

// ConditionStatus 表示资源条件的状态。
type ConditionStatus string

// 以下是有效的条件状态。"ConditionTrue" 表示资源处于该条件。
// "ConditionFalse" 表示资源不处于该条件。"ConditionUnknown" 表示 Kubernetes
// 无法确定资源是否处于该条件。将来，我们可能会添加其他
// 中间状态，例如 ConditionDegraded。
const (
	// ConditionTrue 表示资源处于该条件。
	ConditionTrue ConditionStatus = "True"
	// ConditionFalse 表示资源不处于该条件。
	ConditionFalse ConditionStatus = "False"
	// ConditionUnknown 表示无法确定资源是否处于该条件。
	ConditionUnknown ConditionStatus = "Unknown"
)

// ContainerStateWaiting 表示容器的等待状态。
type ContainerStateWaiting struct {
	// （简要）说明容器尚未运行的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,1,opt,name=reason"`
	// 说明容器为何尚未运行的消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,2,opt,name=message"`
}

// ContainerStateRunning 表示容器的运行状态。
type ContainerStateRunning struct {
	// 容器上次（重新）启动的时间。
	// +optional
	StartedAt metav1.Time `json:"startedAt,omitempty" protobuf:"bytes,1,opt,name=startedAt"`
}

// ContainerStateTerminated 表示容器的终止状态。
type ContainerStateTerminated struct {
	// 容器上次终止时的退出码。
	ExitCode int32 `json:"exitCode" protobuf:"varint,1,opt,name=exitCode"`
	// 容器上次终止时收到的信号。
	// +optional
	Signal int32 `json:"signal,omitempty" protobuf:"varint,2,opt,name=signal"`
	// （简要）说明容器上次终止的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// 容器上次终止的相关消息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`
	// 容器上一次执行开始的时间。
	// +optional
	StartedAt metav1.Time `json:"startedAt,omitempty" protobuf:"bytes,5,opt,name=startedAt"`
	// 容器上次终止的时间。
	// +optional
	FinishedAt metav1.Time `json:"finishedAt,omitempty" protobuf:"bytes,6,opt,name=finishedAt"`
	// 容器 ID，格式为 '<type>://<container_id>'。
	// +optional
	ContainerID string `json:"containerID,omitempty" protobuf:"bytes,7,opt,name=containerID"`
}

// ContainerState 表示容器可能处于的状态。
// 只能指定其中一个成员。
// 如果都未指定，则默认为 ContainerStateWaiting。
type ContainerState struct {
	// 等待中容器的详细信息。
	// +optional
	Waiting *ContainerStateWaiting `json:"waiting,omitempty" protobuf:"bytes,1,opt,name=waiting"`
	// 运行中容器的详细信息。
	// +optional
	Running *ContainerStateRunning `json:"running,omitempty" protobuf:"bytes,2,opt,name=running"`
	// 已终止容器的详细信息。
	// +optional
	Terminated *ContainerStateTerminated `json:"terminated,omitempty" protobuf:"bytes,3,opt,name=terminated"`
}

// ContainerStatus 包含此容器当前状态的详细信息。
type ContainerStatus struct {
	// Name 是表示容器唯一名称的 DNS_LABEL。
	// Pod 中的每个容器在所有容器类型中都必须具有唯一的名称。
	// 不可更新。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// State 保存容器当前状况的详细信息。
	// +optional
	State ContainerState `json:"state,omitempty" protobuf:"bytes,2,opt,name=state"`
	// LastTerminationState 保存容器的上一次终止状态，
	// 用于帮助调试容器崩溃和重启。如果容器仍在运行且 RestartCount 为 0，
	// 则不会填充该字段。
	// +optional
	LastTerminationState ContainerState `json:"lastState,omitempty" protobuf:"bytes,3,opt,name=lastState"`
	// Ready 指定容器当前是否通过就绪性检查。
	// 该值会随着就绪性探针持续执行而变化。如果未指定就绪性探针，
	// 则在容器完全启动后该字段默认为 true（参见 Started 字段）。
	//
	// 该值通常用于判断容器是否已准备好接收流量。
	Ready bool `json:"ready" protobuf:"varint,4,opt,name=ready"`
	// RestartCount 保存容器被重启的次数。
	// Kubelet 会努力始终递增该值，但在某些情况下状态可能因节点重启而丢失，
	// 此时该值可能被重置为 0。该值永不为负数。
	RestartCount int32 `json:"restartCount" protobuf:"varint,5,opt,name=restartCount"`
	// Image 是容器正在运行的容器镜像的名称。
	// 该镜像可能与 PodSpec 中指定的镜像不同，
	// 因为它可能已被运行时解析。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images.
	Image string `json:"image" protobuf:"bytes,6,opt,name=image"`
	// ImageID 是容器镜像的镜像 ID。该镜像 ID 可能与
	// PodSpec 中使用的镜像的 ID 不同，因为它可能已被运行时解析。
	ImageID string `json:"imageID" protobuf:"bytes,7,opt,name=imageID"`
	// ContainerID 是容器的 ID，格式为 '<type>://<container_id>'。
	// 其中 type 是容器运行时标识符，由 CRI API 的 Version 调用返回
	//（例如 "containerd"）。
	// +optional
	ContainerID string `json:"containerID,omitempty" protobuf:"bytes,8,opt,name=containerID"`
	// Started 表示容器是否已完成 postStart 生命周期钩子
	// 并通过了启动探针。
	// 初始值为 false，在 startupProbe 被认为成功后变为 true。
	// 当容器重启或 kubelet 临时丢失状态时重置为 false；这两种情况下启动探针都会重新运行。
	// 如果未定义 startupProbe 且容器正在运行并已通过 postStart 生命周期钩子，则该值始终为 true。
	// null 值必须按 false 处理。
	// +optional
	Started *bool `json:"started,omitempty" protobuf:"varint,9,opt,name=started"`
	// AllocatedResources 表示节点为此容器分配的计算资源。
	// Kubelet 在 Pod 准入成功以及期望的 Pod 扩缩容准入成功后，
	// 将此值设置为 Container.Resources.Requests。
	// +optional
	AllocatedResources ResourceList `json:"allocatedResources,omitempty" protobuf:"bytes,10,rep,name=allocatedResources,casttype=ResourceList,castkey=ResourceName"`
	// Resources 表示在容器启动后或成功扩缩容后，
	// 已在此运行容器上成功生效的计算资源请求和限制。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,11,opt,name=resources"`
	// 卷挂载的状态。
	// +optional
	// +patchMergeKey=mountPath
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=mountPath
	VolumeMounts []VolumeMountStatus `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,12,rep,name=volumeMounts"`
	// User 表示最初附加到容器第一个进程的用户身份信息。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	User *ContainerUser `json:"user,omitempty" protobuf:"bytes,13,opt,name=user,casttype=ContainerUser"`
	// AllocatedResourcesStatus 表示为此 Pod 分配的各种资源的状态。
	// +featureGate=ResourceHealthStatus
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	AllocatedResourcesStatus []ResourceStatus `json:"allocatedResourcesStatus,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,14,rep,name=allocatedResourcesStatus"`
	// StopSignal 报告此容器的有效停止信号。
	// +featureGate=ContainerStopSignals
	// +optional
	StopSignal *Signal `json:"stopSignal,omitempty" protobuf:"bytes,15,opt,name=stopSignal"`
}

// ResourceStatus 表示分配给 Pod 的单个资源的状态。
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
	Name ResourceName `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 资源健康状态的唯一列表。列表中的每个元素包含一个唯一的资源 ID 及其健康状况。
	// 至少在 Pod 的生命周期内，资源 ID 必须唯一标识节点上分配给该 Pod 的资源。
	// 如果同一节点上的其他 Pod 报告了具有相同资源 ID 的状态，则它们必须是共享的同一资源。
	// 有关其在各种使用场景下的具体格式，请参见 ResourceID 类型定义。
	// +listType=map
	// +listMapKey=resourceID
	Resources []ResourceHealth `json:"resources,omitempty" protobuf:"bytes,2,rep,name=resources"`
}

// ResourceHealthStatus 表示资源的健康状况。
type ResourceHealthStatus string

const (
	// ResourceHealthStatusHealthy 表示资源健康。
	ResourceHealthStatusHealthy ResourceHealthStatus = "Healthy"
	// ResourceHealthStatusUnhealthy 表示资源不健康。
	ResourceHealthStatusUnhealthy ResourceHealthStatus = "Unhealthy"
	// ResourceHealthStatusUnknown 表示资源健康状况未知。
	ResourceHealthStatusUnknown ResourceHealthStatus = "Unknown"
)

// ResourceHealthMessageMaxLength 是 ResourceHealth.Message 字段的最大长度。
// 超过此长度的消息将被截断并追加 "..."。
const ResourceHealthMessageMaxLength = 1024

// ResourceID 根据此资源健康信息的来源计算得出。
// 对于 DevicePlugin：
//
//	DeviceID，其中 DeviceID 来自 DevicePlugin 的 ListAndWatchResponse 类型中的 Device 结构：https://github.com/kubernetes/kubernetes/blob/eda1c780543a27c078450e2f17d674471e00f494/staging/src/k8s.io/kubelet/pkg/apis/deviceplugin/v1alpha/api.proto#L61-L73
//
// DevicePlugin ID 通常在节点的生命周期内保持不变，通常可用于唯一标识节点上的设备。
// 对于 DRA：
//
//	<driver name>/<pool name>/<device name>：可以在该 DRA 驱动程序发布的信息中查找此类设备以了解更多信息。它被设计为在集群内全局唯一。
type ResourceID string

// ResourceHealth 表示资源的健康状况，包含最新的设备健康信息。
// 这是 KEP https://kep.k8s.io/4680 的一部分。
type ResourceHealth struct {
	// ResourceID 是资源的唯一标识符。有关更多信息，请参见 ResourceID 类型。
	ResourceID ResourceID `json:"resourceID" protobuf:"bytes,1,opt,name=resourceID"`
	// 资源的健康状况。
	// 可以是以下之一：
	//  - Healthy：正常运行
	//  - Unhealthy：报告为不健康。我们认为这是暂时的健康问题，
	//              因为我们目前没有机制区分暂时性和永久性问题。
	//  - Unknown：无法确定状态。
	//            例如，Device Plugin 已注销且此后未重新注册。
	//
	// 将来我们可能会引入 PermanentlyUnhealthy 状态。
	Health ResourceHealthStatus `json:"health,omitempty" protobuf:"bytes,2,name=health"`
	// Message 为 Health 提供人类可读的上下文信息（例如 "ECC error count exceeded threshold"）。
	// 当 ResourceHealthStatusMessage 启用且 DRA 插件返回消息时，此字段由 kubelet 填充，否则为 null。
	// +featureGate=ResourceHealthStatusMessage
	// +optional
	Message *string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// ContainerUser 表示用户身份信息。
type ContainerUser struct {
	// Linux 保存最初附加到 Linux 容器中容器第一个进程的用户身份信息。
	// 请注意，如果进程有足够权限，实际运行时的身份可以更改。
	// +optional
	Linux *LinuxContainerUser `json:"linux,omitempty" protobuf:"bytes,1,opt,name=linux,casttype=LinuxContainerUser"`

	// Windows 保存最初附加到 Windows 容器中容器第一个进程的用户身份信息。
	// 这只是为将来使用而保留。
	// Windows *WindowsContainerUser
}

// LinuxContainerUser 表示 Linux 容器中的用户身份信息。
type LinuxContainerUser struct {
	// UID 是最初附加到容器中第一个进程的主 uid。
	UID int64 `json:"uid" protobuf:"varint,1,name=uid"`
	// GID 是最初附加到容器中第一个进程的主 gid。
	GID int64 `json:"gid" protobuf:"varint,2,name=gid"`
	// SupplementalGroups 是最初附加到容器中第一个进程的附加组。
	// +optional
	// +listType=atomic
	SupplementalGroups []int64 `json:"supplementalGroups,omitempty" protobuf:"varint,3,rep,name=supplementalGroups"`
}

// PodPhase 是当前时刻 Pod 所处状况的标签。
// +enum
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
	// PodUnknown 表示由于某种原因无法获取 Pod 的状态，通常是由于
	// 与 Pod 所在主机通信时出错。
	// Deprecated: 自 2015 年起就不再设置（74da3b14b0c0f658b3bb8d2def5094686d0e9095）
	PodUnknown PodPhase = "Unknown"
)

// PodConditionType 是 PodCondition.Type 的有效值。
type PodConditionType string

// 以下是 Pod 的内置状况。应用程序可以使用此处未列出的自定义状况。
const (
	// ContainersReady 表示 Pod 中的所有容器是否都已就绪。
	ContainersReady PodConditionType = "ContainersReady"
	// PodInitialized 表示 Pod 中的所有 init 容器都已成功启动。
	PodInitialized PodConditionType = "Initialized"
	// PodReady 表示 Pod 能够处理请求，应将其加入
	// 所有匹配服务的负载均衡池。
	PodReady PodConditionType = "Ready"
	// PodScheduled 表示此 Pod 的调度过程的状态。
	PodScheduled PodConditionType = "PodScheduled"
	// DisruptionTarget 表示该 Pod 由于中断（如抢占、驱逐 API 或垃圾回收）
	// 即将被终止。
	DisruptionTarget PodConditionType = "DisruptionTarget"
	// PodReadyToStartContainers 表示 Pod 沙箱已成功配置，
	// 且 Pod 已准备好启动容器。
	PodReadyToStartContainers PodConditionType = "PodReadyToStartContainers"
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

// 以下是 Pod 状况发生转变的原因。
const (
	// PodReasonUnschedulable 是 PodScheduled PodCondition 中的原因，表示调度器
	// 当前无法调度该 Pod，例如由于集群资源不足。
	PodReasonUnschedulable = "Unschedulable"

	// PodReasonSchedulingGated 是 PodScheduled PodCondition 中的原因，表示调度器
	// 跳过该 Pod 的调度，因为仍存在一个或多个调度门禁。
	PodReasonSchedulingGated = "SchedulingGated"

	// PodReasonSchedulerError 是 PodScheduled PodCondition 中的原因，表示调度过程中
	// 发生了某些内部错误，例如 nodeAffinity 解析错误。
	PodReasonSchedulerError = "SchedulerError"

	// PodReasonTerminationByKubelet 是 DisruptionTarget Pod 状况中的原因，表示
	// 终止由 kubelet 发起。
	PodReasonTerminationByKubelet = "TerminationByKubelet"

	// PodReasonPreemptionByScheduler 是 DisruptionTarget Pod 状况中的原因，表示
	// 中断由调度器的抢占发起。
	PodReasonPreemptionByScheduler = "PreemptionByScheduler"

	// PodReasonDeferred 是 PodResizePending Pod 状况中的原因，表示提议的调整
	// 在理论上可行（适合此节点），但当前无法实现。
	PodReasonDeferred = "Deferred"

	// PodReasonInfeasible 是 PodResizePending Pod 状况中的原因，表示提议的调整
	// 不可行且已被拒绝；可能不会重新评估。
	PodReasonInfeasible = "Infeasible"

	// PodReasonError 是 PodResizeInProgress Pod 状况中的原因，表示
	// 执行调整时发生错误。
	PodReasonError = "Error"
)

// PodCondition 包含此 Pod 当前状况的详细信息。
type PodCondition struct {
	// Type 是状况的类型。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
	Type PodConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=PodConditionType"`
	// 如果设置，表示设置该 Pod 状况时所依据的 .metadata.generation。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,7,opt,name=observedGeneration"`
	// Status 是状况的状态。
	// 可以是 True、False、Unknown。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 上次探测该状况的时间。
	// +optional
	LastProbeTime metav1.Time `json:"lastProbeTime,omitempty" protobuf:"bytes,3,opt,name=lastProbeTime"`
	// 上次状况从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 状况上次转变的唯一、单词、驼峰式原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 人类可读的消息，指示上次转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
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
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// MountPath 对应原始 VolumeMount。
	MountPath string `json:"mountPath" protobuf:"bytes,2,opt,name=mountPath"`
	// ReadOnly 对应原始 VolumeMount。
	// +optional
	ReadOnly bool `json:"readOnly,omitempty" protobuf:"varint,3,opt,name=readOnly"`
	// RecursiveReadOnly 必须设置为 Disabled、Enabled 或未指定（对于非只读挂载）。
	// 原始 VolumeMount 中的 IfPossible 值必须根据挂载结果转换为 Disabled 或 Enabled。
	// +optional
	RecursiveReadOnly *RecursiveReadOnlyMode `json:"recursiveReadOnly,omitempty" protobuf:"bytes,4,opt,name=recursiveReadOnly,casttype=RecursiveReadOnlyMode"`
	// volumeStatus 表示已挂载卷的特定于卷类型的状态。
	// +optional
	VolumeStatus *VolumeStatus `json:"volumeStatus,omitempty" protobuf:"bytes,5,opt,name=volumeStatus"`
}

// VolumeStatus 表示已挂载卷的状态。
// 最多只能指定其中一个成员。
type VolumeStatus struct {
	// image 表示在 kubelet 主机上拉取并挂载的 OCI 对象（容器镜像或制品）。
	// +featureGate=ImageVolumeWithDigest
	// +optional
	Image *ImageVolumeStatus `json:"image,omitempty" protobuf:"bytes,1,opt,name=image"`
}

// ImageVolumeStatus 表示基于镜像的卷状态。
type ImageVolumeStatus struct {
	// ImageRef 是此卷所用镜像的摘要。
	// 其值应类似于 Pod 的 status.containerStatuses[i].imageID。
	// ImageRef 长度不应超过 256 个字符。
	// +kubebuilder:validation:MaxLength=256
	// +required
	ImageRef string `json:"imageRef,omitempty" protobuf:"bytes,1,opt,name=imageRef"`
}

// RestartPolicy 描述容器应如何重启。
// 只能指定以下重启策略之一。
// 如果未指定以下任何策略，则默认为 RestartPolicyAlways。
// +enum
type RestartPolicy string

const (
	// RestartPolicyAlways 表示始终重启容器。
	RestartPolicyAlways RestartPolicy = "Always"
	// RestartPolicyOnFailure 表示仅在容器失败时重启。
	RestartPolicyOnFailure RestartPolicy = "OnFailure"
	// RestartPolicyNever 表示从不重启容器。
	RestartPolicyNever RestartPolicy = "Never"
)

// ContainerRestartPolicy 是单个容器的重启策略。
// 唯一允许的值为 "Always"、"Never" 和 "OnFailure"。
type ContainerRestartPolicy string

const (
	// ContainerRestartPolicyAlways 表示始终重启容器。
	ContainerRestartPolicyAlways ContainerRestartPolicy = "Always"
	// ContainerRestartPolicyNever 表示从不重启容器。
	ContainerRestartPolicyNever ContainerRestartPolicy = "Never"
	// ContainerRestartPolicyOnFailure 表示仅在容器失败时重启。
	ContainerRestartPolicyOnFailure ContainerRestartPolicy = "OnFailure"
)

// ContainerRestartRule 描述如何处理容器退出。
type ContainerRestartRule struct {
	// 指定在满足条件时对容器退出采取的操作。唯一可能的值为 "Restart"，
	// 表示重启容器。
	// +required
	Action ContainerRestartRuleAction `json:"action,omitempty" proto:"bytes,1,opt,name=action" protobuf:"bytes,1,opt,name=action,casttype=ContainerRestartRuleAction"`

	// 表示要在容器退出时检查的退出码。
	// +optional
	// +oneOf=when
	ExitCodes *ContainerRestartRuleOnExitCodes `json:"exitCodes,omitempty" proto:"bytes,2,opt,name=exitCodes" protobuf:"bytes,2,opt,name=exitCodes"`
}

// ContainerRestartRuleAction 描述容器退出时要采取的操作。
type ContainerRestartRuleAction string

// 唯一有效的操作是 Restart。
const (
	// ContainerRestartRuleActionRestart 表示重启该容器。
	ContainerRestartRuleActionRestart ContainerRestartRuleAction = "Restart"
	// ContainerRestartRuleActionRestartAllContainers 表示重启所有容器。
	ContainerRestartRuleActionRestartAllContainers ContainerRestartRuleAction = "RestartAllContainers"
)

// ContainerRestartRuleOnExitCodes 描述根据退出码处理已退出容器的条件。
type ContainerRestartRuleOnExitCodes struct {
	// 表示容器退出码与指定值之间的关系。可能的值为：
	// - In：如果容器退出码在指定值集合中，则满足条件。
	// - NotIn：如果容器退出码不在指定值集合中，则满足条件。
	// +required
	Operator ContainerRestartRuleOnExitCodesOperator `json:"operator,omitempty" proto:"bytes,1,opt,name=operator" protobuf:"bytes,1,opt,name=operator,casttype=ContainerRestartRuleOnExitCodesOperator"`

	// 指定要检查的容器退出码值集合。
	// 最多允许 255 个元素。
	// +optional
	// +listType=set
	Values []int32 `json:"values,omitempty" proto:"varint,2,rep,name=values" protobuf:"varint,2,rep,name=values"`
}

// ContainerRestartRuleOnExitCodesOperator 描述对退出码使用的操作符。
type ContainerRestartRuleOnExitCodesOperator string

const (
	// ContainerRestartRuleOnExitCodesOpIn 表示退出码必须在指定值集合中。
	ContainerRestartRuleOnExitCodesOpIn ContainerRestartRuleOnExitCodesOperator = "In"
	// ContainerRestartRuleOnExitCodesOpNotIn 表示退出码不得在指定值集合中。
	ContainerRestartRuleOnExitCodesOpNotIn ContainerRestartRuleOnExitCodesOperator = "NotIn"
)

// DNSPolicy 定义 Pod 的 DNS 配置方式。
// +enum
type DNSPolicy string

const (
	// DNSClusterFirstWithHostNet 表示 Pod 应优先使用集群 DNS（如果可用），
	// 然后回退到默认（由 kubelet 确定）DNS 设置。
	DNSClusterFirstWithHostNet DNSPolicy = "ClusterFirstWithHostNet"

	// DNSClusterFirst 表示除非 hostNetwork 为 true，否则 Pod 应优先使用集群 DNS
	//（如果可用），然后回退到默认（由 kubelet 确定）DNS 设置。
	DNSClusterFirst DNSPolicy = "ClusterFirst"

	// DNSDefault 表示 Pod 应使用默认（由 kubelet 确定）DNS 设置。
	DNSDefault DNSPolicy = "Default"

	// DNSNone 表示 Pod 应使用空的 DNS 设置。nameservers 和 search paths 等 DNS
	// 参数应通过 DNSConfig 定义。
	DNSNone DNSPolicy = "None"
)

const (
	// DefaultTerminationGracePeriodSeconds 表示 Pod 优雅终止所需的默认秒数。
	DefaultTerminationGracePeriodSeconds = 30
)

// NodeSelector 表示对一组节点进行一个或多个标签查询的结果的并集；
// 即，它表示各 node selector term 所代表的选择器的 OR 关系。
// +structType=atomic
type NodeSelector struct {
	// 必需。节点选择器项列表。各项之间为 OR 关系。
	// +listType=atomic
	NodeSelectorTerms []NodeSelectorTerm `json:"nodeSelectorTerms" protobuf:"bytes,1,rep,name=nodeSelectorTerms"`
}

// NodeSelectorTerm 为 null 或空时不匹配任何对象。其中的要求为 AND 关系。
// TopologySelectorTerm 类型实现了 NodeSelectorTerm 的一个子集。
// +structType=atomic
type NodeSelectorTerm struct {
	// 按节点标签匹配的节点选择器要求列表。
	// +optional
	// +listType=atomic
	MatchExpressions []NodeSelectorRequirement `json:"matchExpressions,omitempty" protobuf:"bytes,1,rep,name=matchExpressions"`
	// 按节点字段匹配的节点选择器要求列表。
	// +optional
	// +listType=atomic
	MatchFields []NodeSelectorRequirement `json:"matchFields,omitempty" protobuf:"bytes,2,rep,name=matchFields"`
}

// NodeSelectorRequirement 是包含值、键以及关联键和值的操作符的选择器。
type NodeSelectorRequirement struct {
	// 选择器作用的标签键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
	// 表示键与一组值之间的关系。
	// 有效的操作符有 In、NotIn、Exists、DoesNotExist、Gt 和 Lt。
	Operator NodeSelectorOperator `json:"operator" protobuf:"bytes,2,opt,name=operator,casttype=NodeSelectorOperator"`
	// 字符串值数组。如果操作符是 In 或 NotIn，则 values 数组必须非空。
	// 如果操作符是 Exists 或 DoesNotExist，则 values 数组必须为空。
	// 如果操作符是 Gt 或 Lt，则 values 数组必须只有一个元素，该元素将被解释为整数。
	// 在 strategic merge patch 期间会替换此数组。
	// +optional
	// +listType=atomic
	Values []string `json:"values,omitempty" protobuf:"bytes,3,rep,name=values"`
}

// NodeSelectorOperator 是节点选择器要求中可以使用的操作符集合。
// +enum
type NodeSelectorOperator string

const (
	// NodeSelectorOpIn 表示键的值必须在给定集合中。
	NodeSelectorOpIn NodeSelectorOperator = "In"
	// NodeSelectorOpNotIn 表示键的值不得在给定集合中。
	NodeSelectorOpNotIn NodeSelectorOperator = "NotIn"
	// NodeSelectorOpExists 表示键必须存在。
	NodeSelectorOpExists NodeSelectorOperator = "Exists"
	// NodeSelectorOpDoesNotExist 表示键必须不存在。
	NodeSelectorOpDoesNotExist NodeSelectorOperator = "DoesNotExist"
	// NodeSelectorOpGt 表示键的值必须大于给定值。
	NodeSelectorOpGt NodeSelectorOperator = "Gt"
	// NodeSelectorOpLt 表示键的值必须小于给定值。
	NodeSelectorOpLt NodeSelectorOperator = "Lt"
)

// TopologySelectorTerm 表示标签查询的结果。
// 为 null 或空的拓扑选择器项不匹配任何对象。
// 其中的要求为 AND 关系。
// 它提供 NodeSelectorTerm 功能的子集。
// 这是一个 alpha 特性，将来可能会改变。
// +structType=atomic
type TopologySelectorTerm struct {
	// 用法：[]TopologySelectorTerm 类型的字段必须使用 listType=atomic。

	// 按标签匹配的拓扑选择器要求列表。
	// +optional
	// +listType=atomic
	MatchLabelExpressions []TopologySelectorLabelRequirement `json:"matchLabelExpressions,omitempty" protobuf:"bytes,1,rep,name=matchLabelExpressions"`
}

// TopologySelectorLabelRequirement 是匹配给定标签的选择器。
// 这是一个 alpha 特性，将来可能会改变。
type TopologySelectorLabelRequirement struct {
	// 选择器作用的标签键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
	// 字符串值数组。必须有一个值与要选择的标签匹配。
	// Values 中的每个条目之间为 OR 关系。
	// +listType=atomic
	Values []string `json:"values" protobuf:"bytes,2,rep,name=values"`
}

// Affinity 是一组亲和性调度规则。
type Affinity struct {
	// 描述 Pod 的节点亲和性调度规则。
	// +optional
	NodeAffinity *NodeAffinity `json:"nodeAffinity,omitempty" protobuf:"bytes,1,opt,name=nodeAffinity"`
	// 描述 Pod 的亲和性调度规则（例如，将此 Pod 与其他 Pod 放在同一节点、可用区等）。
	// +optional
	PodAffinity *PodAffinity `json:"podAffinity,omitempty" protobuf:"bytes,2,opt,name=podAffinity"`
	// 描述 Pod 的反亲和性调度规则（例如，避免将此 Pod 与其他 Pod 放在同一节点、可用区等）。
	// +optional
	PodAntiAffinity *PodAntiAffinity `json:"podAntiAffinity,omitempty" protobuf:"bytes,3,opt,name=podAntiAffinity"`
}

// PodAffinity 是一组 Pod 间的亲和性调度规则。
type PodAffinity struct {
	// 尚未实现。TODO: 实现后取消注释该字段。
	// 如果调度时不满足此字段指定的亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的亲和性要求（例如由于 Pod 标签更新），
	// 系统将尝试最终将该 Pod 从其节点上驱逐。
	// 当有多个元素时，每个 podAffinityTerm 对应的节点列表取交集，即必须满足所有条件。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm  `json:"requiredDuringSchedulingRequiredDuringExecution,omitempty"`

	// 如果调度时不满足此字段指定的亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的亲和性要求（例如由于 Pod 标签更新），
	// 系统可能会也可能不会尝试最终将该 Pod 从其节点上驱逐。
	// 当有多个元素时，每个 podAffinityTerm 对应的节点列表取交集，即必须满足所有条件。
	// +optional
	// +listType=atomic
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,1,rep,name=requiredDuringSchedulingIgnoredDuringExecution"`
	// 调度器会优先将 Pod 调度到满足此字段指定的亲和性表达式的节点上，但它也可能选择
	// 违反其中一个或多个表达式的节点。最优选定的节点是权重总和最大的节点，即
	// 对于每个满足所有调度要求（资源请求、requiredDuringScheduling 亲和性表达式等）的节点，
	// 遍历此字段的元素并在该节点具有匹配相应 podAffinityTerm 的 Pod 时将 "weight" 加到总和上；
	// 总和最高的节点即为最优选定的节点。
	// +optional
	// +listType=atomic
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,2,rep,name=preferredDuringSchedulingIgnoredDuringExecution"`
}

// PodAntiAffinity 是一组 Pod 间的反亲和性调度规则。
type PodAntiAffinity struct {
	// 尚未实现。TODO: 实现后取消注释该字段。
	// 如果调度时不满足此字段指定的反亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的反亲和性要求（例如由于 Pod 标签更新），
	// 系统将尝试最终将该 Pod 从其节点上驱逐。
	// 当有多个元素时，每个 podAffinityTerm 对应的节点列表取交集，即必须满足所有条件。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution []PodAffinityTerm  `json:"requiredDuringSchedulingRequiredDuringExecution,omitempty"`

	// 如果调度时不满足此字段指定的反亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的反亲和性要求（例如由于 Pod 标签更新），
	// 系统可能会也可能不会尝试最终将该 Pod 从其节点上驱逐。
	// 当有多个元素时，每个 podAffinityTerm 对应的节点列表取交集，即必须满足所有条件。
	// +optional
	// +listType=atomic
	RequiredDuringSchedulingIgnoredDuringExecution []PodAffinityTerm `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,1,rep,name=requiredDuringSchedulingIgnoredDuringExecution"`
	// 调度器会优先将 Pod 调度到满足此字段指定的反亲和性表达式的节点上，但它也可能选择
	// 违反其中一个或多个表达式的节点。最优选定的节点是权重总和最大的节点，即
	// 对于每个满足所有调度要求（资源请求、requiredDuringScheduling 反亲和性表达式等）的节点，
	// 遍历此字段的元素并在该节点具有匹配相应 podAffinityTerm 的 Pod 时从总和中减去 "weight"；
	// 总和最高的节点即为最优选定的节点。
	// +optional
	// +listType=atomic
	PreferredDuringSchedulingIgnoredDuringExecution []WeightedPodAffinityTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,2,rep,name=preferredDuringSchedulingIgnoredDuringExecution"`
}

// WeightedPodAffinityTerm 为每个节点累加所有匹配的 WeightedPodAffinityTerm 字段的权重，以找出最优选定的节点。
type WeightedPodAffinityTerm struct {
	// 与匹配相应 podAffinityTerm 关联的权重，范围为 1-100。
	Weight int32 `json:"weight" protobuf:"varint,1,opt,name=weight"`
	// 必需。与相应权重关联的 Pod 亲和性项。
	PodAffinityTerm PodAffinityTerm `json:"podAffinityTerm" protobuf:"bytes,2,opt,name=podAffinityTerm"`
}

// PodAffinityTerm 定义一组 Pod（即那些与给定命名空间相关的 labelSelector 匹配的 Pod），
// 此 Pod 应与它们共置（亲和性）或不共置（反亲和性），
// 其中共置定义为运行在一个节点上，该节点上键为 <topologyKey> 的标签值
// 与该 Pod 集合中任一 Pod 所在节点的对应标签值匹配。
type PodAffinityTerm struct {
	// 对一组资源（此处为 Pod）的标签查询。
	// 如果为 null，则此 PodAffinityTerm 不匹配任何 Pod。
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty" protobuf:"bytes,1,opt,name=labelSelector"`
	// namespaces 指定此条件适用的一组命名空间的静态列表。
	// 该条件应用于此字段列出的命名空间与 namespaceSelector 选择的命名空间的并集。
	// namespaces 列表为 null 或为空且 namespaceSelector 为 null 时表示“此 Pod 所在的命名空间”。
	// +optional
	// +listType=atomic
	Namespaces []string `json:"namespaces,omitempty" protobuf:"bytes,2,rep,name=namespaces"`
	// 此 Pod 应与指定命名空间中匹配 labelSelector 的 Pod 共置（亲和性）或不共置（反亲和性），
	// 其中共置定义为运行在一个节点上，该节点上键为 topologyKey 的标签值
	// 与任一被选中 Pod 所在节点的对应标签值匹配。
	// topologyKey 不得为空。
	TopologyKey string `json:"topologyKey" protobuf:"bytes,3,opt,name=topologyKey"`
	// 对此条件适用的一组命名空间的标签查询。
	// 该条件应用于此字段选择的命名空间与 namespaces 字段中列出的命名空间的并集。
	// null 选择器且 namespaces 列表为 null 或为空时表示“此 Pod 所在的命名空间”。
	// 空选择器（{}）匹配所有命名空间。
	// +optional
	NamespaceSelector *metav1.LabelSelector `json:"namespaceSelector,omitempty" protobuf:"bytes,4,opt,name=namespaceSelector"`
	// MatchLabelKeys 是一组 Pod 标签键，用于选择要考虑的 Pod。
	// 这些键用于从新传入 Pod 的标签中查找值，这些键值标签与 `labelSelector` 合并为 `key in (value)`，
	// 以选出在新传入 Pod 的 Pod（反）亲和性中需要考虑的现有 Pod 组。
	// 新传入 Pod 标签中不存在的键将被忽略。默认值为空。
	// 同一键不得同时存在于 matchLabelKeys 和 labelSelector 中。
	// 此外，如果未设置 labelSelector，则不能设置 matchLabelKeys。
	//
	// +listType=atomic
	// +optional
	MatchLabelKeys []string `json:"matchLabelKeys,omitempty" protobuf:"bytes,5,opt,name=matchLabelKeys"`
	// MismatchLabelKeys 是一组 Pod 标签键，用于选择要考虑的 Pod。
	// 这些键用于从新传入 Pod 的标签中查找值，这些键值标签与 `labelSelector` 合并为 `key notin (value)`，
	// 以选出在新传入 Pod 的 Pod（反）亲和性中需要考虑的现有 Pod 组。
	// 新传入 Pod 标签中不存在的键将被忽略。默认值为空。
	// 同一键不得同时存在于 mismatchLabelKeys 和 labelSelector 中。
	// 此外，如果未设置 labelSelector，则不能设置 mismatchLabelKeys。
	//
	// +listType=atomic
	// +optional
	MismatchLabelKeys []string `json:"mismatchLabelKeys,omitempty" protobuf:"bytes,6,opt,name=mismatchLabelKeys"`
}

// NodeAffinity 是一组节点亲和性调度规则。
type NodeAffinity struct {
	// 尚未实现。TODO: 实现后取消注释该字段。
	// 如果调度时不满足此字段指定的亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的亲和性要求（例如由于更新），
	// 系统将尝试最终将该 Pod 从其节点上驱逐。
	// +optional
	// RequiredDuringSchedulingRequiredDuringExecution *NodeSelector `json:"requiredDuringSchedulingRequiredDuringExecution,omitempty"`

	// 如果调度时不满足此字段指定的亲和性要求，则 Pod 不会被调度到该节点上。
	// 如果在 Pod 执行期间的某个时刻不再满足此字段指定的亲和性要求（例如由于更新），
	// 系统可能会也可能不会尝试最终将该 Pod 从其节点上驱逐。
	// +optional
	RequiredDuringSchedulingIgnoredDuringExecution *NodeSelector `json:"requiredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,1,opt,name=requiredDuringSchedulingIgnoredDuringExecution"`
	// 调度器会优先将 Pod 调度到满足此字段指定的亲和性表达式的节点上，但它也可能选择
	// 违反其中一个或多个表达式的节点。最优选定的节点是权重总和最大的节点，即
	// 对于每个满足所有调度要求（资源请求、requiredDuringScheduling 亲和性表达式等）的节点，
	// 遍历此字段的元素并在该节点匹配相应 matchExpressions 时将 "weight" 加到总和上；
	// 总和最高的节点即为最优选定的节点。
	// +optional
	// +listType=atomic
	PreferredDuringSchedulingIgnoredDuringExecution []PreferredSchedulingTerm `json:"preferredDuringSchedulingIgnoredDuringExecution,omitempty" protobuf:"bytes,2,rep,name=preferredDuringSchedulingIgnoredDuringExecution"`
}

// PreferredSchedulingTerm 为空时匹配所有对象且隐含权重为 0（即无操作）。
// 为 null 时不匹配任何对象（同样是无操作）。
type PreferredSchedulingTerm struct {
	// 与匹配相应 nodeSelectorTerm 关联的权重，范围为 1-100。
	Weight int32 `json:"weight" protobuf:"varint,1,opt,name=weight"`
	// 与相应权重关联的节点选择器项。
	Preference NodeSelectorTerm `json:"preference" protobuf:"bytes,2,opt,name=preference"`
}

// Taint 所附加到的节点会对任何不容忍该 Taint 的 Pod 产生 effect 中指定的影响。
type Taint struct {
	// 必需。要应用到节点上的污点键。
	Key string `json:"key" protobuf:"bytes,1,opt,name=key"`
	// 与污点键对应的污点值。
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`
	// 必需。此污点对不容忍它的 Pod 的影响。
	// 有效的影响有 NoSchedule、PreferNoSchedule 和 NoExecute。
	Effect TaintEffect `json:"effect" protobuf:"bytes,3,opt,name=effect,casttype=TaintEffect"`
	// TimeAdded 表示添加此污点的时间。
	// +optional
	TimeAdded *metav1.Time `json:"timeAdded,omitempty" protobuf:"bytes,4,opt,name=timeAdded"`
}

// TaintEffect 表示污点对不容忍它的 Pod 的影响。
// +enum
type TaintEffect string

const (
	// 不允许新的 Pod 调度到该节点上，除非它们容忍此污点，
	// 但允许所有未经过调度器而直接提交给 Kubelet 的 Pod 启动，
	// 并允许所有已在运行的 Pod 继续运行。
	// 由调度器强制执行。
	TaintEffectNoSchedule TaintEffect = "NoSchedule"
	// 与 TaintEffectNoSchedule 类似，但调度器会尽量不将新 Pod 调度到该节点上，
	// 而不是完全禁止新 Pod 调度到该节点。由调度器强制执行。
	TaintEffectPreferNoSchedule TaintEffect = "PreferNoSchedule"
	// 尚未实现。TODO: 实现后取消注释该字段。
	// 与 TaintEffectNoSchedule 类似，但另外不允许未经过调度器直接提交给 Kubelet 的 Pod 启动。
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
	// +k8s:alpha(since: "1.37")=+k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:format=k8s-label-key
	Key string `json:"key,omitempty" protobuf:"bytes,1,opt,name=key"`
	// Operator 表示键与值之间的关系。
	// 有效的操作符有 Exists、Equal、Lt 和 Gt。默认为 Equal。
	// Exists 等价于值的通配符，因此 Pod 可以容忍某一类别的所有污点。
	// Lt 和 Gt 执行数值比较（需要启用特性门控 TaintTolerationComparisonOperators）。
	// +optional
	Operator TolerationOperator `json:"operator,omitempty" protobuf:"bytes,2,opt,name=operator,casttype=TolerationOperator"`
	// Value 是此容忍匹配的污点值。
	// 如果操作符为 Exists，则值应为空，否则为普通字符串。
	// +optional
	Value string `json:"value,omitempty" protobuf:"bytes,3,opt,name=value"`
	// Effect 指示要匹配的污点影响。空表示匹配所有污点影响。
	// 指定时，允许的值为 NoSchedule、PreferNoSchedule 和 NoExecute。
	// +optional
	Effect TaintEffect `json:"effect,omitempty" protobuf:"bytes,4,opt,name=effect,casttype=TaintEffect"`
	// TolerationSeconds 表示容忍（其影响必须为 NoExecute，否则忽略此字段）
	// 容忍该污点的时长。默认情况下未设置，表示永久容忍该污点（不驱逐）。
	// 零值和负值将被系统视为 0（立即驱逐）。
	// +optional
	TolerationSeconds *int64 `json:"tolerationSeconds,omitempty" protobuf:"varint,5,opt,name=tolerationSeconds"`
}

// TolerationOperator 是容忍中可以使用的操作符集合。
// +enum
type TolerationOperator string

const (
	// TolerationOpExists 表示容忍匹配具有该键的所有污点。
	TolerationOpExists TolerationOperator = "Exists"
	// TolerationOpEqual 表示容忍仅匹配键和值都相等的污点。
	TolerationOpEqual TolerationOperator = "Equal"
	// TolerationOpLt 表示容忍匹配键值小于指定值的污点。
	TolerationOpLt TolerationOperator = "Lt"
	// TolerationOpGt 表示容忍匹配键值大于指定值的污点。
	TolerationOpGt TolerationOperator = "Gt"
)

// PodReadinessGate 包含对 Pod 状况的引用。
type PodReadinessGate struct {
	// ConditionType 引用 Pod 状况列表中类型匹配的状况。
	ConditionType PodConditionType `json:"conditionType" protobuf:"bytes,1,opt,name=conditionType,casttype=PodConditionType"`
}

// PodSpec 是 Pod 的描述。
type PodSpec struct {
	// 可被此 Pod 中的容器挂载的卷列表。
	// 更多信息：https://kubernetes.io/docs/concepts/storage/volumes
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	Volumes []Volume `json:"volumes,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,1,rep,name=volumes"`
	// 属于此 Pod 的初始化容器列表。
	// init 容器会在容器启动之前按顺序执行。如果任何 init 容器失败，
	// 则 Pod 被视为失败，并根据其 restartPolicy 进行处理。init 容器或普通容器的名称
	// 在所有容器中必须唯一。
	// init 容器不能具有 Lifecycle 动作、Readiness 探针、Liveness 探针或 Startup 探针。
	// 调度时会考虑 init 容器的 resourceRequirements：对每种资源类型取最高的请求/限制，
	// 然后使用该值与普通容器之和中的较大者。限制以类似方式应用于 init 容器。
	// 目前不能添加或删除 init 容器。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/init-containers/
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	InitContainers []Container `json:"initContainers,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,20,rep,name=initContainers"`
	// 属于此 Pod 的容器列表。
	// 目前不能添加或删除容器。
	// Pod 中必须至少有一个容器。
	// 不可更新。
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Containers []Container `json:"containers" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=containers"`
	// 在此 Pod 中运行的临时容器列表。临时容器可以在现有 Pod 中运行，
	// 以执行用户发起的操作（例如调试）。创建 Pod 时不能指定此列表，
	// 也不能通过更新 Pod spec 来修改它。要向现有 Pod 添加临时容器，
	// 请使用该 Pod 的 ephemeralcontainers 子资源。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	EphemeralContainers []EphemeralContainer `json:"ephemeralContainers,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,34,rep,name=ephemeralContainers"`
	// Pod 中所有容器的重启策略。
	// 取 Always、OnFailure、Never 之一。在某些上下文中可能只允许这些值的子集。
	// 默认为 Always。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#restart-policy
	// +optional
	RestartPolicy RestartPolicy `json:"restartPolicy,omitempty" protobuf:"bytes,3,opt,name=restartPolicy,casttype=RestartPolicy"`
	// Pod 优雅终止所需的可选秒数，可在删除请求中减小。
	// 值必须为非负整数。值为 0 表示通过 kill 信号立即停止（没有关停机会）。
	// 如果此值为 nil，则使用默认的优雅终止时间。
	// 优雅终止时间是指从向 Pod 中运行的进程发送终止信号，
	// 到进程被 kill 信号强制停止之间的秒数。
	// 请将此值设置为长于进程的预期清理时间。
	// 默认为 30 秒。
	// +optional
	TerminationGracePeriodSeconds *int64 `json:"terminationGracePeriodSeconds,omitempty" protobuf:"varint,4,opt,name=terminationGracePeriodSeconds"`
	// Pod 在节点上可活动的可选秒数（相对于 StartTime），超过后系统将主动尝试
	// 将其标记为失败并终止关联的容器。
	// 值必须为正整数。
	// +optional
	ActiveDeadlineSeconds *int64 `json:"activeDeadlineSeconds,omitempty" protobuf:"varint,5,opt,name=activeDeadlineSeconds"`
	// 设置 Pod 的 DNS 策略。
	// 默认为 "ClusterFirst"。
	// 有效值为 'ClusterFirstWithHostNet'、'ClusterFirst'、'Default' 或 'None'。
	// DNSConfig 中给定的 DNS 参数将与 DNSPolicy 选择的策略合并。
	// 要与 hostNetwork 一起设置 DNS 选项，必须显式将 DNS 策略指定为 'ClusterFirstWithHostNet'。
	// +optional
	DNSPolicy DNSPolicy `json:"dnsPolicy,omitempty" protobuf:"bytes,6,opt,name=dnsPolicy,casttype=DNSPolicy"`
	// NodeSelector 是一个选择器，Pod 必须满足它才能适配到某个节点。
	// Pod 要被调度到某节点上，选择器必须匹配该节点的标签。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/assign-pod-node/
	// +optional
	// +mapType=atomic
	NodeSelector map[string]string `json:"nodeSelector,omitempty" protobuf:"bytes,7,rep,name=nodeSelector"`

	// ServiceAccountName 是运行此 Pod 时使用的 ServiceAccount 的名称。
	// 更多信息：https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
	// +optional
	ServiceAccountName string `json:"serviceAccountName,omitempty" protobuf:"bytes,8,opt,name=serviceAccountName"`
	// DeprecatedServiceAccount 是 ServiceAccountName 的已弃用别名。
	// Deprecated: 请改用 serviceAccountName。
	// +optional
	DeprecatedServiceAccount string `json:"serviceAccount,omitempty" protobuf:"bytes,9,opt,name=serviceAccount"`
	// AutomountServiceAccountToken 表示是否应自动挂载 service account token。
	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty" protobuf:"varint,21,opt,name=automountServiceAccountToken"`

	// NodeName 表示此 Pod 被调度到哪个节点上。
	// 如果为空，则此 Pod 是由 schedulerName 指定的调度器的候选调度对象。
	// 一旦设置此字段，该节点的 kubelet 就负责此 Pod 的生命周期。
	// 此字段不应用于表达希望将 Pod 调度到特定节点的意愿。
	// https://kubernetes.io/docs/concepts/scheduling-eviction/assign-pod-node/#nodename
	// +optional
	NodeName string `json:"nodeName,omitempty" protobuf:"bytes,10,opt,name=nodeName"`
	// 为此 Pod 请求主机网络。使用主机的网络命名空间。
	// 使用 HostNetwork 时应指定端口，以便调度器知晓。
	// 当 `hostNetwork` 为 true 时，端口定义中指定的 `hostPort` 字段必须与 `containerPort` 匹配，
	// 端口定义中未指定的 `hostPort` 字段将默认设置为与 `containerPort` 匹配。
	// 默认为 false。
	// +optional
	HostNetwork bool `json:"hostNetwork,omitempty" protobuf:"varint,11,opt,name=hostNetwork"`
	// 使用主机的 pid 命名空间。
	// 可选：默认为 false。
	// +optional
	HostPID bool `json:"hostPID,omitempty" protobuf:"varint,12,opt,name=hostPID"`
	// 使用主机的 ipc 命名空间。
	// 可选：默认为 false。
	// +optional
	HostIPC bool `json:"hostIPC,omitempty" protobuf:"varint,13,opt,name=hostIPC"`
	// 在 Pod 中的所有容器之间共享单个进程命名空间。
	// 设置此项后，容器将能够查看同一 Pod 中其他容器的进程并向其发送信号，
	// 且每个容器中的第一个进程不会被分配 PID 1。
	// HostPID 和 ShareProcessNamespace 不能同时设置。
	// 可选：默认为 false。
	// +optional
	ShareProcessNamespace *bool `json:"shareProcessNamespace,omitempty" protobuf:"varint,27,opt,name=shareProcessNamespace"`
	// SecurityContext 保存 Pod 级别的安全属性和通用容器设置。
	// 可选：默认为空。各字段的默认值请参见类型说明。
	// +optional
	SecurityContext *PodSecurityContext `json:"securityContext,omitempty" protobuf:"bytes,14,opt,name=securityContext"`
	// ImagePullSecrets 是同一命名空间中 secret 引用的可选列表，用于拉取此 PodSpec 中使用的任何镜像。
	// 如果指定，这些 secret 将传递给各个拉取器实现供其使用。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	ImagePullSecrets []LocalObjectReference `json:"imagePullSecrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,15,rep,name=imagePullSecrets"`
	// 指定 Pod 的主机名。
	// 如果未指定，Pod 的主机名将设置为系统定义的值。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,16,opt,name=hostname"`
	// 如果指定，Pod 的完整限定主机名为 "<hostname>.<subdomain>.<pod namespace>.svc.<cluster domain>"。
	// 如果未指定，Pod 将完全没有域名。
	// +optional
	Subdomain string `json:"subdomain,omitempty" protobuf:"bytes,17,opt,name=subdomain"`
	// 如果指定，表示此 Pod 的调度约束。
	// +optional
	Affinity *Affinity `json:"affinity,omitempty" protobuf:"bytes,18,opt,name=affinity"`
	// 如果指定，Pod 将由指定的调度器分派。
	// 如果未指定，Pod 将由默认调度器分派。
	// +optional
	SchedulerName string `json:"schedulerName,omitempty" protobuf:"bytes,19,opt,name=schedulerName"`
	// 如果指定，表示此 Pod 的容忍。
	// +optional
	// +listType=atomic
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Tolerations []Toleration `json:"tolerations,omitempty" protobuf:"bytes,22,opt,name=tolerations"`
	// HostAliases 是可选的主机和 IP 列表，如果指定，将注入 Pod 的 hosts 文件。
	// +optional
	// +patchMergeKey=ip
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=ip
	HostAliases []HostAlias `json:"hostAliases,omitempty" patchStrategy:"merge" patchMergeKey:"ip" protobuf:"bytes,23,rep,name=hostAliases"`
	// 如果指定，表示 Pod 的优先级。"system-node-critical" 和
	// "system-cluster-critical" 是两个特殊关键字，表示最高优先级，前者优先级最高。
	// 任何其他名称必须通过创建具有该名称的 PriorityClass 对象来定义。
	// 如果未指定，Pod 优先级将为默认值；如果没有默认值则为 0。
	// +optional
	PriorityClassName string `json:"priorityClassName,omitempty" protobuf:"bytes,24,opt,name=priorityClassName"`
	// 优先级值。各种系统组件使用此字段查找 Pod 的优先级。
	// 启用 Priority Admission Controller 时，会阻止用户设置此字段。
	// 该准入控制器会根据 PriorityClassName 填充此字段。
	// 值越高，优先级越高。
	// +optional
	Priority *int32 `json:"priority,omitempty" protobuf:"bytes,25,opt,name=priority"`
	// 指定 Pod 的 DNS 参数。
	// 此处指定的参数将与基于 DNSPolicy 生成的 DNS 配置合并。
	// +optional
	DNSConfig *PodDNSConfig `json:"dnsConfig,omitempty" protobuf:"bytes,26,opt,name=dnsConfig"`
	// 如果指定，将评估所有就绪门控以确定 Pod 是否就绪。
	// 当所有容器都已就绪且就绪门控中指定的所有状况状态均为 "True" 时，Pod 才算就绪。
	// 更多信息：https://git.k8s.io/enhancements/keps/sig-network/580-pod-readiness-gates
	// +optional
	// +listType=atomic
	ReadinessGates []PodReadinessGate `json:"readinessGates,omitempty" protobuf:"bytes,28,opt,name=readinessGates"`
	// RuntimeClassName 引用 node.k8s.io 组中的 RuntimeClass 对象，应用于运行此 Pod。
	// 如果没有与指定名称匹配的 RuntimeClass 资源，则不会运行该 Pod。
	// 如果未设置或为空，将使用 "legacy" RuntimeClass，这是一个隐式类，
	// 其定义为空，使用默认的运行时处理程序。
	// 更多信息：https://git.k8s.io/enhancements/keps/sig-node/585-runtime-class
	// +optional
	RuntimeClassName *string `json:"runtimeClassName,omitempty" protobuf:"bytes,29,opt,name=runtimeClassName"`
	// EnableServiceLinks 表示是否应将服务信息注入 Pod 的环境变量，
	// 其语法与 Docker links 匹配。
	// 可选：默认为 true。
	// +optional
	EnableServiceLinks *bool `json:"enableServiceLinks,omitempty" protobuf:"varint,30,opt,name=enableServiceLinks"`
	// PreemptionPolicy 是抢占较低优先级 Pod 的策略。
	// 取 Never、PreemptLowerPriority 之一。
	// 启用 Priority Admission Controller 时，会阻止用户设置此字段。
	// 该准入控制器会根据 PriorityClassName 填充此字段。
	// 如果未设置，默认为 PreemptLowerPriority。
	// +optional
	PreemptionPolicy *PreemptionPolicy `json:"preemptionPolicy,omitempty" protobuf:"bytes,31,opt,name=preemptionPolicy"`
	// Overhead 表示在给定 RuntimeClass 下运行 Pod 相关的资源开销。
	// 此字段将在准入时由 RuntimeClass 准入控制器自动填充。如果启用了
	// RuntimeClass 准入控制器，则 Pod 创建请求中不得设置 overhead。
	// RuntimeClass 准入控制器将拒绝已设置 overhead 的 Pod 创建请求。
	// 如果在 PodSpec 中配置并选择了 RuntimeClass，Overhead 将设置为
	// 相应 RuntimeClass 中定义的值，否则保持未设置并视为 0。
	// 更多信息：https://git.k8s.io/enhancements/keps/sig-node/688-pod-overhead/README.md
	// +optional
	Overhead ResourceList `json:"overhead,omitempty" protobuf:"bytes,32,opt,name=overhead"`
	// TopologySpreadConstraints 描述一组 Pod 应如何在拓扑域中分布。
	// 调度器将按照约束调度 Pod。
	// 所有 topologySpreadConstraints 之间为 AND 关系。
	// +optional
	// +patchMergeKey=topologyKey
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=topologyKey
	// +listMapKey=whenUnsatisfiable
	TopologySpreadConstraints []TopologySpreadConstraint `json:"topologySpreadConstraints,omitempty" patchStrategy:"merge" patchMergeKey:"topologyKey" protobuf:"bytes,33,opt,name=topologySpreadConstraints"`
	// 如果为 true，Pod 的主机名将配置为 Pod 的 FQDN，而不是叶子名称（默认）。
	// 在 Linux 容器中，这意味着在内核的 hostname 字段（struct utsname 的 nodename 字段）中设置 FQDN。
	// 在 Windows 容器中，这意味着将注册表键 HKEY_LOCAL_MACHINE\\SYSTEM\\CurrentControlSet\\Services\\Tcpip\\Parameters 下的 hostname 注册表值设置为 FQDN。
	// 如果 Pod 没有 FQDN，则此设置无效。
	// 默认为 false。
	// +optional
	SetHostnameAsFQDN *bool `json:"setHostnameAsFQDN,omitempty" protobuf:"varint,35,opt,name=setHostnameAsFQDN"`
	// 指定 Pod 中容器的操作系统。
	// 如果设置此项，某些 Pod 和容器字段会受到限制。
	//
	// 如果 OS 字段设置为 linux，则以下字段必须未设置：
	// -securityContext.windowsOptions
	//
	// 如果 OS 字段设置为 windows，则以下字段必须未设置：
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
	OS *PodOS `json:"os,omitempty" protobuf:"bytes,36,opt,name=os"`

	// 使用主机的用户命名空间。
	// 可选：默认为 true。
	// 如果设置为 true 或未指定，Pod 将在主机用户命名空间中运行，
	// 适用于 Pod 需要仅主机用户命名空间可用的特性时，例如使用 CAP_SYS_MODULE 加载内核模块。
	// 设置为 false 时，会为 Pod 创建新的用户命名空间。设置为 false 有助于
	// 缓解容器逃逸漏洞，甚至允许用户在主机上实际不具备 root 权限的情况下以 root 运行容器。
	// +optional
	HostUsers *bool `json:"hostUsers,omitempty" protobuf:"bytes,37,opt,name=hostUsers"`

	// SchedulingGates 是一个不透明的列表，如果指定将阻止 Pod 被调度。
	// 如果 schedulingGates 非空，Pod 将保持在 SchedulingGated 状态，
	// 调度器不会尝试调度该 Pod。
	//
	// SchedulingGates 只能在 Pod 创建时设置，之后只能删除。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +optional
	SchedulingGates []PodSchedulingGate `json:"schedulingGates,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,38,opt,name=schedulingGates"`
	// ResourceClaims 定义允许 Pod 启动前必须分配和预留的 ResourceClaim。
	// 这些资源将按名称提供给消费它们的容器。
	//
	// 这是一个稳定字段，但要求启用 DynamicResourceAllocation 特性门控。
	//
	// 此字段不可变。
	//
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaims []PodResourceClaim `json:"resourceClaims,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,39,rep,name=resourceClaims"`
	// Resources 是 Pod 中所有容器所需的 CPU 和内存资源总量。
	// 它仅支持为 "cpu"、"memory" 和 "hugepages-" 资源名称指定 Requests 和 Limits。
	// 不支持 ResourceClaims。
	//
	// 此字段可对整个 Pod 的资源分配进行细粒度控制，允许 Pod 内容器之间共享资源。
	// TODO: 进入 beta 阶段时，请扩展此注释以提供更详细的说明。
	//
	// 这是一个 alpha 字段，需要启用 PodLevelResources 特性门控。
	//
	// +featureGate=PodLevelResources
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,40,opt,name=resources"`
	// HostnameOverride 为 Pod 自身感知到的主机名指定显式覆盖值。
	// 此字段仅指定 Pod 的主机名，不影响其 DNS 记录。
	// 当此字段设置为非空字符串时：
	// - 它优先于 `hostname` 和 `subdomain` 中设置的值。
	// - Pod 的主机名将设置为此值。
	// - `setHostnameAsFQDN` 必须为 nil 或设置为 false。
	// - `hostNetwork` 必须设置为 false。
	//
	// 此字段必须是 RFC 1123 定义的有效 DNS 子域，且最多包含 64 个字符。
	//
	// +featureGate=HostnameOverride
	// +optional
	HostnameOverride *string `json:"hostnameOverride,omitempty" protobuf:"bytes,41,opt,name=hostnameOverride"`

	// WorkloadRef 被设为墓碑，以说明为何 42 是保留的 protobuf 标签。
	// 该字段在 Kubernetes 1.36 中已被 SchedulingGroup 字段取代。
	// WorkloadRef *WorkloadReference `json:"workloadRef,omitempty" protobuf:"bytes,42,opt,name=workloadRef"`

	// SchedulingGroup 提供对此 Pod 所属的直接调度运行时分组对象的引用。
	// 调度器使用此字段识别分组并应用正确的分组调度策略。与分组的关联还会
	// 影响 Pod 在其他与调度相关的更广泛生命周期方面（如抢占、资源附加等）。
	// 如果未指定，Pod 在这些方面都被视为单个单元。
	// 此字段引用的分组对象在 Pod 创建时可能不存在。
	// 此字段不可变，但同名分组对象可能会以不同策略重新创建。
	// 在 Pod 调度期间这样做可能导致放置结果不符合预期策略。
	//
	// +featureGate=GenericWorkload
	// +optional
	SchedulingGroup *PodSchedulingGroup `json:"schedulingGroup,omitempty" protobuf:"bytes,43,opt,name=schedulingGroup"`

	// evictionResponders 引用基于 EvictionRequests 对驱逐做出响应的响应器。
	// 响应器应通过观察并通过 Eviction Resource API 通信，协助 Pod 的优雅终止。
	// 响应器按其指定优先级依次选择。
	//
	// 响应器应通过更新 Eviction 对象的 .status.responders[].heartbeatTime 字段
	// 定期报告驱逐进度。如果未在 Eviction API 定义的心跳截止时间（目前为 20 分钟）内
	// 更新此字段，驱逐将移交给下一个优先级较低的响应器。如果没有其他响应器，
	// 最后的默认 imperative-eviction.k8s.io/evictor 响应器（优先级为 100）将使用
	// 命令式 Eviction API（pods/<name>/eviction 子资源）驱逐该 Pod。
	//
	// 响应器列表的最大长度为 10。
	// 当 Pod 属于 PodGroup（设置了 .spec.schedulingGroup）时，不支持响应器。
	// 此字段只能在创建时设置，之后不可变。
	// +featureGate=EvictionRequestAPI
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	// +k8s:maxItems=10
	// +k8s:alpha(since: "1.37")=+k8s:dependentForbidden("schedulingGroup")
	EvictionResponders []EvictionResponder `json:"evictionResponders,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,44,rep,name=evictionResponders"`
}

// PodResourceClaim 恰好引用一个 ResourceClaim，可以直接引用，
// 也可以通过命名一个 ResourceClaimTemplate 来引用，该模板随后会转换为 Pod 的 ResourceClaim。
//
// 它会添加一个名称，在 Pod 内唯一标识该 ResourceClaim。
// 需要访问该 ResourceClaim 的容器使用此名称引用它。
//
// 当启用 DRAWorkloadResourceClaims 特性门控且此 Pod 属于某个 PodGroup 时，
// 如果所有字段（Name、ResourceClaimName 和 ResourceClaimTemplateName）都相等，
// PodResourceClaim 就与 PodGroupResourceClaim 匹配。匹配的声明引用
// 一个在 PodGroup 中所有 Pod 之间共享的单个 ResourceClaim，在 ResourceClaimStatus.ReservedFor 中
// 为 PodGroup 预留，而不是为单个
// Pod 相关。
type PodResourceClaim struct {
	// Name 在该 Pod 内唯一标识此资源声明。
	// 必须是 DNS_LABEL。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// Source 自 Kubernetes 1.31 起已废弃，由下面的内联字段取代。
	//
	// Source ClaimSource `json:"source,omitempty" protobuf:"bytes,2,name=source"`

	// ResourceClaimName 是与该 Pod 处于同一命名空间中的 ResourceClaim 对象的名称。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其中一个。
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,3,opt,name=resourceClaimName"`

	// ResourceClaimTemplateName 是与该 Pod 处于同一命名空间中的 ResourceClaimTemplate 对象的名称。
	//
	// 该模板将用于创建一个新的 ResourceClaim，并绑定到这个 Pod。当该 Pod 被删除时，
	// 对应的 ResourceClaim 也会被删除。Pod 名称、资源名称以及一个生成的组成部分会组合成
	// ResourceClaim 的唯一名称，该名称记录在 pod.status.resourceClaimStatuses 中。
	//
	// 当 DRAWorkloadResourceClaims 特性门控启用，且该 Pod 所属的 PodGroup 定义了具有相同
	// Name 和 ResourceClaimTemplateName 的 PodGroupResourceClaim 时，此 PodResourceClaim
	// 将解析为该 PodGroup 生成的 ResourceClaim。组内所有定义了与 PodGroupResourceClaim 的
	// Name 和 ResourceClaimTemplateName 相匹配的等价 PodResourceClaim 的 Pod，共享同一个
	// 生成的 ResourceClaim。为 PodGroup 生成的 ResourceClaim 归 PodGroup 所有，
	// 其生命周期与 PodGroup 绑定，而非与任何单个 Pod 绑定。
	//
	// 此字段不可变；控制面在创建 ResourceClaim 后不会对其进行任何更改。
	//
	// ResourceClaimName 与 ResourceClaimTemplateName 必须且只能设置其中一个。
	ResourceClaimTemplateName *string `json:"resourceClaimTemplateName,omitempty" protobuf:"bytes,4,opt,name=resourceClaimTemplateName"`
}

// PodResourceClaimStatus 存储在 PodStatus 中，对应每个引用 ResourceClaimTemplate 的
// PodResourceClaim。它记录对应 ResourceClaim 生成的名称。
type PodResourceClaimStatus struct {
	// Name 在该 Pod 内唯一标识此资源声明。
	// 必须与 pod.spec.resourceClaims 中某个条目的名称一致，
	// 这意味着该字符串必须是 DNS_LABEL。
	Name string `json:"name" protobuf:"bytes,1,name=name"`

	// ResourceClaimName 是在 Pod 所在命名空间中为该 Pod 生成的 ResourceClaim 的名称。
	//
	// 当 DRAWorkloadResourceClaims 特性启用，且对应的 PodResourceClaim 与该 Pod 所属
	// PodGroup 创建的 PodGroupResourceClaim 匹配时，此值为为 PodGroup 生成并预留的
	// ResourceClaim 的名称。
	//
	// 如果未设置此值，说明无需生成 ResourceClaim。此时可以忽略 pod.spec.resourceClaims
	// 中的相应条目。
	//
	// +optional
	ResourceClaimName *string `json:"resourceClaimName,omitempty" protobuf:"bytes,2,opt,name=resourceClaimName"`
}

// PodExtendedResourceClaimStatus 存储在 PodStatus 中，用于记录由 DRA 支持的扩展资源请求。
// 它记录调度器创建的对应特殊 ResourceClaim 的名称。
type PodExtendedResourceClaimStatus struct {
	// RequestMappings 标识 <容器、由 DRA 支持的扩展资源> 到生成的 ResourceClaim 中
	// 设备请求的映射关系。
	// +listType=atomic
	RequestMappings []ContainerExtendedResourceRequest `json:"requestMappings" protobuf:"bytes,1,rep,name=requestMappings"`

	// ResourceClaimName 是在 Pod 所在命名空间中为该 Pod 生成的 ResourceClaim 的名称。
	ResourceClaimName string `json:"resourceClaimName" protobuf:"bytes,2,name=resourceClaimName"`
}

// ContainerExtendedResourceRequest 保存容器名称、扩展资源名称到设备请求名称的映射关系。
type ContainerExtendedResourceRequest struct {
	// ContainerName 是请求资源的容器名称。
	ContainerName string `json:"containerName" protobuf:"bytes,1,name=containerName"`
	// ResourceName 是该容器中由 DRA 支持的扩展资源名称。
	ResourceName string `json:"resourceName" protobuf:"bytes,2,name=resourceName"`
	// RequestName 是特殊 ResourceClaim 中对应此扩展资源的请求名称。
	RequestName string `json:"requestName" protobuf:"bytes,3,name=requestName"`
}

// OSName 是可在 OS 中使用的操作系统类型集合。
type OSName string

// OSName 的有效取值如下。
const (
	// Linux 表示 Linux 操作系统。
	Linux OSName = "linux"
	// Windows 表示 Windows 操作系统。
	Windows OSName = "windows"
)

// PodOS 定义 Pod 的操作系统参数。
type PodOS struct {
	// Name 是操作系统名称。当前支持的值为 linux 和 windows。
	// 未来可能定义其他值，示例参见：
	// https://github.com/opencontainers/runtime-spec/blob/master/config.md#platform-specific-configuration
	// 客户端应能处理其他取值，并将此字段中无法识别的值视为 os: null。
	Name OSName `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PodSchedulingGate 与 Pod 关联，用于阻止其调度。
type PodSchedulingGate struct {
	// Name 是调度门控的名称。
	// 每个调度门控的名称字段必须唯一。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
}

// PodSchedulingGroup 标识 Pod 所属的运行时调度组实例。
// 调度器使用此信息来应用考虑工作负载的调度语义。
// 必须且只能指定一个字段。
// +union
type PodSchedulingGroup struct {
	// PodGroupName 指定独立 PodGroup 对象的名称，
	// 该对象代表此调度组的运行时实例。
	// 必须是 DNS 子域名。
	//
	// +optional
	// +oneOf=GroupSelection
	PodGroupName *string `json:"podGroupName,omitempty" protobuf:"bytes,1,opt,name=podGroupName"`
}

// UnsatisfiableConstraintAction 定义当约束无法满足时要采取的操作。
// +enum
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
	// +-------+-------+-------+
	// | zone1 | zone2 | zone3 |
	// +-------+-------+-------+
	// |  P P  |  P P  |   P   |
	// +-------+-------+-------+
	// - 若 MaxSkew 为 1，新来的 Pod 只能调度到 zone3，使其变为 2/2/2；
	//   若调度到 zone1（zone2），则 zone1（zone2）上的 ActualSkew（3-1）会违反 MaxSkew（1）。
	// - 若 MaxSkew 为 2，新来的 Pod 可以调度到任意可用区。
	// 当 `whenUnsatisfiable=ScheduleAnyway` 时，该值用于对满足约束的拓扑赋予更高优先级。
	// 这是必填字段。默认值为 1，不允许为 0。
	MaxSkew int32 `json:"maxSkew" protobuf:"varint,1,opt,name=maxSkew"`
	// TopologyKey 是节点标签的键。具有该键且值相同的节点被视为处于同一拓扑中。
	// 我们将每个 <key, value> 视为一个"桶"，并尝试将数量均衡的 Pod 放入每个桶中。
	// 我们将域（domain）定义为拓扑的某个具体实例。
	// 同时，将合格域定义为其节点满足 nodeAffinityPolicy 和 nodeTaintsPolicy 要求的域。
	// 例如，若 TopologyKey 为 "kubernetes.io/hostname"，则每个 Node 都是该拓扑的一个域。
	// 若 TopologyKey 为 "topology.kubernetes.io/zone"，则每个可用区是该拓扑的一个域。
	// 这是必填字段。
	TopologyKey string `json:"topologyKey" protobuf:"bytes,2,opt,name=topologyKey"`
	// WhenUnsatisfiable 指示当 Pod 不满足分布约束时如何处理。
	// - DoNotSchedule（默认）告诉调度器不要调度该 Pod。
	// - ScheduleAnyway 告诉调度器可以将 Pod 调度到任意位置，
	//   但优先选择有助于降低倾斜度的拓扑。
	// 当且仅当对某个新来的 Pod，所有可能的节点分配都会在某一拓扑上违反 "MaxSkew" 时，
	// 才认为该约束对此 Pod "无法满足"。
	// 例如，在一个有 3 个可用区的集群中，MaxSkew 设为 1，具有相同 labelSelector 的 Pod
	// 分布为 3/1/1：
	// +-------+-------+-------+
	// | zone1 | zone2 | zone3 |
	// +-------+-------+-------+
	// | P P P |   P   |   P   |
	// +-------+-------+-------+
	// 若 WhenUnsatisfiable 设为 DoNotSchedule，新来的 Pod 只能调度到 zone2（zone3），
	// 使其变为 3/2/1（3/1/2），因为 zone2（zone3）上的 ActualSkew（2-1）满足 MaxSkew（1）。
	// 换句话说，集群仍可能是不均衡的，但调度器不会让它*更*不均衡。
	// 这是必填字段。
	WhenUnsatisfiable UnsatisfiableConstraintAction `json:"whenUnsatisfiable" protobuf:"bytes,3,opt,name=whenUnsatisfiable,casttype=UnsatisfiableConstraintAction"`
	// LabelSelector 用于查找匹配的 Pod。
	// 匹配该标签选择器的 Pod 会被计数，以确定其所在拓扑域中的 Pod 数量。
	// +optional
	LabelSelector *metav1.LabelSelector `json:"labelSelector,omitempty" protobuf:"bytes,4,opt,name=labelSelector"`
	// MinDomains 表示合格域的最小数量。
	// 当具有匹配拓扑键的合格域数量少于 minDomains 时，Pod 拓扑分布会将"全局最小值"视为 0，
	// 然后进行倾斜度的计算。
	// 当具有匹配拓扑键的合格域数量大于或等于 minDomains 时，此值对调度没有影响。
	// 因此，当合格域数量少于 minDomains 时，调度器向这些域调度的 Pod 不会超过 maxSkew 个。
	// 若值为 nil，则该约束的行为等同于 MinDomains 等于 1。
	// 有效值为大于 0 的整数。
	// 当值不为 nil 时，WhenUnsatisfiable 必须为 DoNotSchedule。
	//
	// 例如，在一个有 3 个可用区的集群中，MaxSkew 设为 2，MinDomains 设为 5，
	// 具有相同 labelSelector 的 Pod 分布为 2/2/2：
	// +-------+-------+-------+
	// | zone1 | zone2 | zone3 |
	// +-------+-------+-------+
	// |  P P  |  P P  |  P P  |
	// +-------+-------+-------+
	// 域数量少于 5（MinDomains），因此"全局最小值"被视为 0。
	// 此时具有相同 labelSelector 的新 Pod 无法被调度，
	// 因为若将其调度到三个可用区中的任何一个，计算出的倾斜度将为 3（3 - 0），
	// 从而违反 MaxSkew。
	// +optional
	MinDomains *int32 `json:"minDomains,omitempty" protobuf:"varint,5,opt,name=minDomains"`
	// NodeAffinityPolicy 指示在计算 Pod 拓扑分布倾斜度时如何处理 Pod 的 nodeAffinity/nodeSelector。
	// 可选项为：
	// - Honor：仅将匹配 nodeAffinity/nodeSelector 的节点纳入计算。
	// - Ignore：忽略 nodeAffinity/nodeSelector，所有节点都纳入计算。
	//
	// 若此值为 nil，则行为等同于 Honor 策略。
	// +optional
	NodeAffinityPolicy *NodeInclusionPolicy `json:"nodeAffinityPolicy,omitempty" protobuf:"bytes,6,opt,name=nodeAffinityPolicy"`
	// NodeTaintsPolicy 指示在计算 Pod 拓扑分布倾斜度时如何处理节点污点。
	// 可选项为：
	// - Honor：包含无污点的节点，以及新来的 Pod 能够容忍其污点的节点。
	// - Ignore：忽略节点污点，包含所有节点。
	//
	// 若此值为 nil，则行为等同于 Ignore 策略。
	// +optional
	NodeTaintsPolicy *NodeInclusionPolicy `json:"nodeTaintsPolicy,omitempty" protobuf:"bytes,7,opt,name=nodeTaintsPolicy"`
	// MatchLabelKeys 是一组 Pod 标签键，用于选择计算分布时所依据的 Pod。
	// 这些键用于从新来的 Pod 标签中查找值，得到的键值对与 labelSelector 取交集，
	// 以选出为新来的 Pod 计算分布时所依据的现有 Pod 组。
	// MatchLabelKeys 和 LabelSelector 中不允许存在相同的键。
	// 未设置 LabelSelector 时不能设置 MatchLabelKeys。
	// 新来的 Pod 标签中不存在的键将被忽略。
	// 空列表或 null 表示仅按 labelSelector 匹配。
	//
	// 这是 beta 字段，需要启用 MatchLabelKeysInPodTopologySpread 特性门控（默认启用）。
	// +listType=atomic
	// +optional
	MatchLabelKeys []string `json:"matchLabelKeys,omitempty" protobuf:"bytes,8,opt,name=matchLabelKeys"`
}

const (
	// DefaultEnableServiceLinks 是 enableServiceLinks 属性的默认值。
	DefaultEnableServiceLinks = true
)

// HostAlias 保存 IP 与主机名的映射关系，将作为一条记录注入 Pod 的 hosts 文件。
type HostAlias struct {
	// IP 是 hosts 文件记录中的 IP 地址。
	// +required
	IP string `json:"ip" protobuf:"bytes,1,opt,name=ip"`
	// Hostnames 是上述 IP 地址对应的主机名列表。
	// +listType=atomic
	Hostnames []string `json:"hostnames,omitempty" protobuf:"bytes,2,rep,name=hostnames"`
}

// PodFSGroupChangePolicy 定义在挂载卷时应用 fsGroup 所使用的策略。
// +enum
type PodFSGroupChangePolicy string

const (
	// FSGroupChangeOnRootMismatch 表示仅当卷根目录的权限和所有权与卷上预期的权限不匹配时，
	// 才更改卷的所有权和权限。这有助于缩短更改卷所有权和权限所需的时间。
	FSGroupChangeOnRootMismatch PodFSGroupChangePolicy = "OnRootMismatch"
	// FSGroupChangeAlways 表示每当卷在 Pod 内挂载时，都应更改其所有权和权限。
	// 这是默认行为。
	FSGroupChangeAlways PodFSGroupChangePolicy = "Always"
)

// SupplementalGroupsPolicy 定义如何计算容器首个进程的附加组（supplemental groups）。
// +enum
type SupplementalGroupsPolicy string

const (
	// SupplementalGroupsPolicyMerge 表示容器提供的 SupplementalGroups 和 FsGroup
	//（在 SecurityContext 中指定）将与容器镜像中定义的主用户组（/etc/group）合并。
	SupplementalGroupsPolicyMerge SupplementalGroupsPolicy = "Merge"
	// SupplementalGroupsPolicyStrict 表示使用容器提供的 SupplementalGroups 和 FsGroup
	//（在 SecurityContext 中指定），而不使用容器镜像中定义的任何组。
	SupplementalGroupsPolicyStrict SupplementalGroupsPolicy = "Strict"
)

// PodSELinuxChangePolicy 定义容器的 SELinux 标签如何应用到 Pod 使用的所有卷上。
type PodSELinuxChangePolicy string

const (
	// SELinuxChangePolicyRecursive 表示由容器运行时对所有 Pod 卷递归重新打标签。
	// 对于大容量卷这可能较慢，但允许在同一节点上共享同一卷的特权 Pod 与非特权 Pod 混用。
	SELinuxChangePolicyRecursive PodSELinuxChangePolicy = "Recursive"
	// SELinuxChangePolicyMountOption 表示使用 `-o context` 挂载选项挂载所有符合条件的 Pod 卷。
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
	// SELinuxOptions 是要应用到所有容器的 SELinux 上下文。
	// 如果未指定，容器运行时会为每个容器分配一个随机的 SELinux 上下文。
	// 该字段也可在 SecurityContext 中设置。若两处都设置，
	// SecurityContext 中指定的值对该容器优先。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SELinuxOptions *SELinuxOptions `json:"seLinuxOptions,omitempty" protobuf:"bytes,1,opt,name=seLinuxOptions"`
	// WindowsOptions 是应用到所有容器的 Windows 特有设置。
	// 如果未指定，则使用容器 SecurityContext 中的选项。
	// 若两处都设置，SecurityContext 中指定的值优先。
	// 注意，当 spec.os.name 为 linux 时不能设置此字段。
	// +optional
	WindowsOptions *WindowsSecurityContextOptions `json:"windowsOptions,omitempty" protobuf:"bytes,8,opt,name=windowsOptions"`
	// RunAsUser 是运行容器进程入口点的 UID。
	// 如果未指定，默认使用镜像元数据中指定的用户。
	// 该字段也可在 SecurityContext 中设置。若两处都设置，
	// SecurityContext 中指定的值对该容器优先。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty" protobuf:"varint,2,opt,name=runAsUser"`
	// RunAsGroup 是运行容器进程入口点的 GID。
	// 如果未设置，则使用运行时默认值。
	// 该字段也可在 SecurityContext 中设置。若两处都设置，
	// SecurityContext 中指定的值对该容器优先。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty" protobuf:"varint,6,opt,name=runAsGroup"`
	// RunAsNonRoot 表示容器必须以非 root 用户运行。
	// 若为 true，Kubelet 会在运行时校验镜像，确保其不以 UID 0（root）运行，否则容器启动失败。
	// 如果未设置或为 false，则不进行此类校验。
	// 该字段也可在 SecurityContext 中设置。若两处都设置，SecurityContext 中指定的值优先。
	// +optional
	RunAsNonRoot *bool `json:"runAsNonRoot,omitempty" protobuf:"varint,3,opt,name=runAsNonRoot"`
	// SupplementalGroups 是应用到每个容器中首个进程的组列表，
	// 在容器的主 GID 和 fsGroup（如指定）之外附加。
	// 如果启用了 SupplementalGroupsPolicy 特性，
	// supplementalGroupsPolicy 字段决定这些组是附加到还是替代容器镜像中定义的组关系。
	// 如果未指定，则不添加额外组，但根据 supplementalGroupsPolicy 字段，
	// 容器镜像中定义的组关系仍可能被使用。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	// +listType=atomic
	SupplementalGroups []int64 `json:"supplementalGroups,omitempty" protobuf:"varint,4,rep,name=supplementalGroups"`
	// SupplementalGroupsPolicy 定义如何计算容器首个进程的附加组。
	// 有效值为 "Merge" 和 "Strict"。如果未指定，则使用 "Merge"。
	//（Alpha）使用此字段需要启用 SupplementalGroupsPolicy 特性门控，
	// 且容器运行时必须实现对特性的支持。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// TODO: 当 spec.os.name 不是 windows 时，在 v1.34 中将默认值更新为 "Merge"
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	SupplementalGroupsPolicy *SupplementalGroupsPolicy `json:"supplementalGroupsPolicy,omitempty" protobuf:"bytes,12,opt,name=supplementalGroupsPolicy"`
	// FSGroup 是应用到 Pod 中所有容器的特殊附加组。
	// 某些卷类型允许 Kubelet 更改卷的所有权，使其归该 Pod 所有：
	//
	// 1. 所属 GID 将为 FSGroup
	// 2. 设置 setgid 位（卷中创建的新文件将归 FSGroup 所有）
	// 3. 权限位与 rw-rw---- 按位或
	//
	// 如果未设置，Kubelet 不会修改任何卷的所有权和权限。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	FSGroup *int64 `json:"fsGroup,omitempty" protobuf:"varint,5,opt,name=fsGroup"`
	// Sysctls 保存该 Pod 使用的命名空间级 sysctl 列表。
	// 如果 Pod 使用了容器运行时不支持的 sysctl，可能无法启动。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	// +listType=atomic
	Sysctls []Sysctl `json:"sysctls,omitempty" protobuf:"bytes,7,rep,name=sysctls"`
	// FSGroupChangePolicy 定义在卷暴露到 Pod 内部之前更改其所有权和权限的行为。
	// 此字段仅适用于支持基于 fsGroup 的所有权（和权限）的卷类型。
	// 对 secret、configmaps 和 emptydir 等临时卷类型无效。
	// 有效值为 "OnRootMismatch" 和 "Always"。如果未指定，则使用 "Always"。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	FSGroupChangePolicy *PodFSGroupChangePolicy `json:"fsGroupChangePolicy,omitempty" protobuf:"bytes,9,opt,name=fsGroupChangePolicy"`
	// SeccompProfile 是该 Pod 中容器要使用的 seccomp 选项。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SeccompProfile *SeccompProfile `json:"seccompProfile,omitempty" protobuf:"bytes,10,opt,name=seccompProfile"`
	// AppArmorProfile 是该 Pod 中容器要使用的 AppArmor 选项。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AppArmorProfile *AppArmorProfile `json:"appArmorProfile,omitempty" protobuf:"bytes,11,opt,name=appArmorProfile"`
	// SELinuxChangePolicy 定义容器的 SELinux 标签如何应用到 Pod 使用的所有卷上。
	// 对不支持 SELinux 的节点或不支持 SELinux 的卷无效。
	// 有效值为 "MountOption" 和 "Recursive"。
	//
	// "Recursive" 表示由容器运行时对所有 Pod 卷上的所有文件重新打标签。
	// 对于大容量卷这可能较慢，但允许在同一节点上共享同一卷的特权 Pod 与非特权 Pod 混用。
	//
	// "MountOption" 使用 `-o context` 挂载选项挂载所有符合条件的 Pod 卷。
	// 这要求共享同一卷的所有 Pod 使用相同的 SELinux 标签。
	// 特权 Pod 与非特权 Pod 之间无法共享同一卷。
	// 符合条件的卷包括树内 FibreChannel 和 iSCSI 卷，以及所有在其 CSIDriver 实例中
	// 通过设置 spec.seLinuxMount: true 声明支持 SELinux 的 CSI 卷。
	// 其他卷始终递归重新打标签。
	//
	// 如果未指定，则使用 "MountOption"。
	//
	// 此字段仅影响设置了 SELinux 标签的 Pod，无论是在 PodSecurityContext 中，
	// 还是在所有容器的 SecurityContext 中设置的标签。
	//
	// 使用同一卷的所有 Pod 应使用相同的 seLinuxChangePolicy，
	// 否则某些 Pod 可能卡在 ContainerCreating 状态。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +featureGate=SELinuxChangePolicy
	// +optional
	SELinuxChangePolicy *PodSELinuxChangePolicy `json:"seLinuxChangePolicy,omitempty" protobuf:"bytes,13,opt,name=seLinuxChangePolicy"`
}

// SeccompProfile 定义 Pod/容器的 seccomp 配置设置。
// 只能设置一种配置来源。
// +union
type SeccompProfile struct {
	// Type 指示将应用哪种 seccomp 配置。
	// 有效选项为：
	//
	// Localhost - 使用节点上文件中定义的配置。
	// RuntimeDefault - 使用容器运行时的默认配置。
	// Unconfined - 不应用任何配置。
	// +unionDiscriminator
	Type SeccompProfileType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=SeccompProfileType"`
	// LocalhostProfile 表示应使用节点上文件中定义的配置。
	// 该配置必须预先在节点上配置好才能生效。
	// 必须是相对于 kubelet 配置的 seccomp 配置位置的下级路径。
	// 当 type 为 "Localhost" 时必须设置；其他类型下不得设置。
	// +optional
	LocalhostProfile *string `json:"localhostProfile,omitempty" protobuf:"bytes,2,opt,name=localhostProfile"`
}

// SeccompProfileType 定义支持的 seccomp 配置类型。
// +enum
type SeccompProfileType string

const (
	// SeccompProfileTypeUnconfined 表示不应用任何 seccomp 配置（即 unconfined）。
	SeccompProfileTypeUnconfined SeccompProfileType = "Unconfined"
	// SeccompProfileTypeRuntimeDefault 表示容器运行时的默认 seccomp 配置。
	SeccompProfileTypeRuntimeDefault SeccompProfileType = "RuntimeDefault"
	// SeccompProfileTypeLocalhost 表示应使用节点上文件中定义的配置。
	// 文件路径相对于 <kubelet-root-dir>/seccomp。
	SeccompProfileTypeLocalhost SeccompProfileType = "Localhost"
)

// AppArmorProfile 定义 Pod 或容器的 AppArmor 设置。
// +union
type AppArmorProfile struct {
	// Type 指示将应用哪种 AppArmor 配置。
	// 有效选项为：
	//   Localhost - 节点上预加载的配置。
	//   RuntimeDefault - 容器运行时的默认配置。
	//   Unconfined - 不实施 AppArmor 限制。
	// +unionDiscriminator
	Type AppArmorProfileType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=AppArmorProfileType"`

	// LocalhostProfile 表示应使用节点上已加载的配置。
	// 该配置必须预先在节点上配置好才能生效。
	// 必须与配置加载后的名称一致。
	// 当且仅当 type 为 "Localhost" 时必须设置。
	// +optional
	LocalhostProfile *string `json:"localhostProfile,omitempty" protobuf:"bytes,2,opt,name=localhostProfile"`
}

// AppArmorProfileType 定义支持的 AppArmor 配置类型。
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
// +enum
type PodQOSClass string

const (
	// PodQOSGuaranteed 是 Guaranteed QoS 类别。
	PodQOSGuaranteed PodQOSClass = "Guaranteed"
	// PodQOSBurstable 是 Burstable QoS 类别。
	PodQOSBurstable PodQOSClass = "Burstable"
	// PodQOSBestEffort 是 BestEffort QoS 类别。
	PodQOSBestEffort PodQOSClass = "BestEffort"
)

// PodDNSConfig 定义在 DNSPolicy 生成的参数之外，Pod 的额外 DNS 参数。
type PodDNSConfig struct {
	// Nameservers 是 DNS 名称服务器 IP 地址列表。
	// 会追加到由 DNSPolicy 生成的基础名称服务器列表之后。
	// 重复的名称服务器会被去除。
	// +optional
	// +listType=atomic
	Nameservers []string `json:"nameservers,omitempty" protobuf:"bytes,1,rep,name=nameservers"`
	// Searches 是用于主机名查找的 DNS 搜索域列表。
	// 会追加到由 DNSPolicy 生成的基础搜索路径之后。
	// 重复的搜索路径会被去除。
	// +optional
	// +listType=atomic
	Searches []string `json:"searches,omitempty" protobuf:"bytes,2,rep,name=searches"`
	// Options 是 DNS 解析器选项列表。
	// 会与由 DNSPolicy 生成的基础选项合并。
	// 重复的条目会被去除。Options 中给出的解析选项
	// 会覆盖基础 DNSPolicy 中出现的选项。
	// +optional
	// +listType=atomic
	Options []PodDNSConfigOption `json:"options,omitempty" protobuf:"bytes,3,rep,name=options"`
}

// PodDNSConfigOption 定义 Pod 的 DNS 解析器选项。
type PodDNSConfigOption struct {
	// Name 是此 DNS 解析器选项的名称。
	// 必填。
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
	// Value 是此 DNS 解析器选项的值。
	// +optional
	Value *string `json:"value,omitempty" protobuf:"bytes,2,opt,name=value"`
}

// PodIP 表示分配给 Pod 的单个 IP 地址。
type PodIP struct {
	// IP 是分配给 Pod 的 IP 地址。
	// +required
	IP string `json:"ip" protobuf:"bytes,1,opt,name=ip"`
}

// HostIP 表示分配给主机的单个 IP 地址。
type HostIP struct {
	// IP 是分配给主机的 IP 地址。
	// +required
	IP string `json:"ip" protobuf:"bytes,1,opt,name=ip"`
}

// EphemeralContainerCommon 是 Container 中所有字段的副本，用于内联到 EphemeralContainer 中。
// 这个独立类型便于将 EphemeralContainer 转换为 Container，
// 也便于单独为 EphemeralContainer 的字段编写文档。
// 向 Container 添加新字段时，也必须在此处添加。
type EphemeralContainerCommon struct {
	// Name 是临时容器的名称，以 DNS_LABEL 形式指定。
	// 此名称在所有容器、init 容器和临时容器中必须唯一。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// Image 是容器镜像名称。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images
	Image string `json:"image,omitempty" protobuf:"bytes,2,opt,name=image"`
	// Command 是入口点数组，不在 shell 中执行。
	// 如果未提供，则使用镜像的 ENTRYPOINT。
	// 变量引用 $(VAR_NAME) 会使用容器环境变量展开。如果变量无法解析，
	// 输入字符串中的引用将保持不变。连续的 $$ 会缩减为单个 $，
	// 这样可以对 $(VAR_NAME) 语法进行转义：例如 "$$(VAR_NAME)" 会生成字符串字面量
	// "$(VAR_NAME)"。无论变量是否存在，转义后的引用都不会被展开。不可更新。
	// 更多信息：https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
	// +optional
	// +listType=atomic
	Command []string `json:"command,omitempty" protobuf:"bytes,3,rep,name=command"`
	// Args 是入口点的参数。
	// 如果未提供，则使用镜像的 CMD。
	// 变量引用 $(VAR_NAME) 会使用容器环境变量展开。如果变量无法解析，
	// 输入字符串中的引用将保持不变。连续的 $$ 会缩减为单个 $，
	// 这样可以对 $(VAR_NAME) 语法进行转义：例如 "$$(VAR_NAME)" 会生成字符串字面量
	// "$(VAR_NAME)"。无论变量是否存在，转义后的引用都不会被展开。不可更新。
	// 更多信息：https://kubernetes.io/docs/tasks/inject-data-application/define-command-argument-container/#running-a-command-in-a-shell
	// +optional
	// +listType=atomic
	Args []string `json:"args,omitempty" protobuf:"bytes,4,rep,name=args"`
	// WorkingDir 是容器的工作目录。
	// 如果未指定，则使用容器运行时的默认值，该默认值可能在容器镜像中配置。
	// 不可更新。
	// +optional
	WorkingDir string `json:"workingDir,omitempty" protobuf:"bytes,5,opt,name=workingDir"`
	// Ports 不允许为临时容器设置端口。
	// +optional
	// +patchMergeKey=containerPort
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=containerPort
	// +listMapKey=protocol
	Ports []ContainerPort `json:"ports,omitempty" patchStrategy:"merge" patchMergeKey:"containerPort" protobuf:"bytes,6,rep,name=ports"`
	// EnvFrom 是用于填充容器环境变量的来源列表。
	// 来源中定义的键可以包含除 '=' 以外的任何可打印 ASCII 字符。
	// 当同一个键出现在多个来源中时，
	// 最后一个来源中的值优先。
	// 由 Env 中重复键定义的值优先。
	// 不可更新。
	// +optional
	// +listType=atomic
	EnvFrom []EnvFromSource `json:"envFrom,omitempty" protobuf:"bytes,19,rep,name=envFrom"`
	// Env 是要在容器中设置的环境变量列表。
	// 不可更新。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Env []EnvVar `json:"env,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,7,rep,name=env"`
	// Resources 不允许为临时容器设置资源。临时容器使用已分配给 Pod 的空闲资源。
	// +optional
	Resources ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,8,opt,name=resources"`
	// ResizePolicy 是容器的资源调整策略。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	// +listType=atomic
	ResizePolicy []ContainerResizePolicy `json:"resizePolicy,omitempty" protobuf:"bytes,23,rep,name=resizePolicy"`
	// RestartPolicy 是容器的重启策略，用于管理 Pod 内每个容器的重启行为。
	// 临时容器不能设置此字段。
	// +optional
	RestartPolicy *ContainerRestartPolicy `json:"restartPolicy,omitempty" protobuf:"bytes,24,opt,name=restartPolicy,casttype=ContainerRestartPolicy"`
	// RestartPolicyRules 表示一组规则，用于判断容器退出时是否应重启。
	// 临时容器不能设置此字段。
	// +featureGate=ContainerRestartRules
	// +optional
	// +listType=atomic
	RestartPolicyRules []ContainerRestartRule `json:"restartPolicyRules,omitempty" protobuf:"bytes,25,rep,name=restartPolicyRules"`
	// VolumeMounts 是要挂载到容器文件系统中的 Pod 卷。
	// 临时容器不允许使用子路径挂载。
	// 不可更新。
	// +optional
	// +patchMergeKey=mountPath
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=mountPath
	VolumeMounts []VolumeMount `json:"volumeMounts,omitempty" patchStrategy:"merge" patchMergeKey:"mountPath" protobuf:"bytes,9,rep,name=volumeMounts"`
	// VolumeDevices 是容器要使用的块设备列表。
	// +patchMergeKey=devicePath
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=devicePath
	// +optional
	VolumeDevices []VolumeDevice `json:"volumeDevices,omitempty" patchStrategy:"merge" patchMergeKey:"devicePath" protobuf:"bytes,21,rep,name=volumeDevices"`
	// LivenessProbe 不允许为临时容器设置探针。
	// +optional
	LivenessProbe *Probe `json:"livenessProbe,omitempty" protobuf:"bytes,10,opt,name=livenessProbe"`
	// ReadinessProbe 不允许为临时容器设置探针。
	// +optional
	ReadinessProbe *Probe `json:"readinessProbe,omitempty" protobuf:"bytes,11,opt,name=readinessProbe"`
	// StartupProbe 不允许为临时容器设置探针。
	// +optional
	StartupProbe *Probe `json:"startupProbe,omitempty" protobuf:"bytes,22,opt,name=startupProbe"`
	// Lifecycle 不允许为临时容器设置生命周期钩子。
	// +optional
	Lifecycle *Lifecycle `json:"lifecycle,omitempty" protobuf:"bytes,12,opt,name=lifecycle"`
	// TerminationMessagePath 是容器终止消息所在文件挂载到容器文件系统中的路径（可选）。
	// 写入的消息应是简短的最终状态，例如断言失败消息。
	// 超过 4096 字节时节点会将其截断。所有容器的消息总长度上限为 12kb。
	// 默认为 /dev/termination-log。
	// 不可更新。
	// +optional
	TerminationMessagePath string `json:"terminationMessagePath,omitempty" protobuf:"bytes,13,opt,name=terminationMessagePath"`
	// TerminationMessagePolicy 指示如何填充终止消息。
	// File 会在成功和失败时都使用 terminationMessagePath 的内容填充容器状态消息。
	// FallbackToLogsOnError 会在终止消息文件为空且容器以错误退出时，
	// 使用容器日志输出的最后一段。
	// 日志输出上限为 2048 字节或 80 行，取较小者。
	// 默认为 File。
	// 不可更新。
	// +optional
	TerminationMessagePolicy TerminationMessagePolicy `json:"terminationMessagePolicy,omitempty" protobuf:"bytes,20,opt,name=terminationMessagePolicy,casttype=TerminationMessagePolicy"`
	// ImagePullPolicy 是镜像拉取策略。
	// 取值为 Always、Never、IfNotPresent 之一。
	// 若指定 :latest 标签，默认为 Always，否则为 IfNotPresent。
	// 不可更新。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images#updating-images
	// +optional
	ImagePullPolicy PullPolicy `json:"imagePullPolicy,omitempty" protobuf:"bytes,14,opt,name=imagePullPolicy,casttype=PullPolicy"`
	// SecurityContext 定义临时容器运行时的安全选项（可选）。
	// 如果设置，SecurityContext 中的字段会覆盖 PodSecurityContext 中的对应字段。
	// +optional
	SecurityContext *SecurityContext `json:"securityContext,omitempty" protobuf:"bytes,15,opt,name=securityContext"`

	// 以下变量用于交互式容器，它们有非常特定的使用场景（例如调试），
	// 不应在通用容器中使用。

	// Stdin 表示此容器是否应在容器运行时为 stdin 分配缓冲区。
	// 如果未设置，从容器 stdin 读取将始终返回 EOF。
	// 默认为 false。
	// +optional
	Stdin bool `json:"stdin,omitempty" protobuf:"varint,16,opt,name=stdin"`
	// StdinOnce 表示容器运行时应在单次 attach 打开 stdin 通道后将其关闭。
	// 当 stdin 为 true 时，stdin 流会在多次 attach 会话之间保持打开。
	// 如果 stdinOnce 设为 true，stdin 在容器启动时打开，在第一个客户端 attach 到 stdin 之前为空，
	// 之后保持打开并接收数据，直到客户端断开连接；此时 stdin 关闭，并保持关闭状态直到容器重启。
	// 如果此标志为 false，从 stdin 读取的容器进程永远不会收到 EOF。
	// 默认为 false。
	// +optional
	StdinOnce bool `json:"stdinOnce,omitempty" protobuf:"varint,17,opt,name=stdinOnce"`
	// TTY 表示此容器是否应为自己分配一个 TTY，同时要求 'stdin' 为 true。
	// 默认为 false。
	// +optional
	TTY bool `json:"tty,omitempty" protobuf:"varint,18,opt,name=tty"`
}

// EphemeralContainerCommon 可转换为 Container。这两个类型的所有字段必须保持同步。
var _ = Container(EphemeralContainerCommon{})

// EphemeralContainer 是可以添加到现有 Pod 中的临时容器，用于用户发起的活动（例如调试）。
// 临时容器没有资源或调度保证，退出时或 Pod 被移除/重启时都不会重启。
// 如果临时容器导致 Pod 超出其资源配额，kubelet 可能会驱逐该 Pod。
//
// 要添加临时容器，请使用现有 Pod 的 ephemeralcontainers 子资源。
// 临时容器不能被移除或重启。
type EphemeralContainer struct {
	// EphemeralContainerCommon 包含 Container 的所有字段，以及临时容器特有的附加字段。
	// 与 Container 共有的字段放在下面的内联结构体中，
	// 这样 EphemeralContainer 可以轻松转换为 Container。
	EphemeralContainerCommon `json:"" protobuf:"bytes,1,req"`

	// TargetContainerName 如果设置，表示此临时容器要指向 PodSpec 中某个容器的名称。
	// 临时容器将在该容器的命名空间（IPC、PID 等）中运行。
	// 如果未设置，则临时容器使用 Pod spec 中配置的命名空间。
	//
	// 容器运行时必须实现对特性的支持。如果运行时不支持命名空间指向，
	// 则设置此字段的结果是未定义的。
	// +optional
	TargetContainerName string `json:"targetContainerName,omitempty" protobuf:"bytes,2,opt,name=targetContainerName"`
}

// EvictionResponder 用于指定响应 Eviction 的响应者。
// 响应者应通过 Eviction Resource API 进行观察和通信，以协助目标（例如 Pod 终止）的优雅驱逐。
// +structType=atomic
type EvictionResponder struct {
	// Name 用于标识响应 Eviction 的响应者。
	//
	// 必须是有效的带域名前缀的键（例如 "acme.io/foo"）。
	// 域名 *.k8s.io 和 *.kubernetes.io 保留。
	// 每个响应者的此字段必须唯一。
	// 此字段必填。
	// +required
	// +k8s:required
	// +k8s:format=k8s-prefixed-label-key
	// +k8s:customValidation
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`

	// Priority 是此响应者的优先级。evictionrequest-controller 会优先选择优先级更高的响应者。
	// 如果存在优先级相同的响应者，则选择域名按字母序靠前的响应者。
	// 这意味着先按字母序比较顶级域名标签，再比较下级域名标签，最后比较键。
	//
	// 作为 Pod 管理控制器的响应者应将此字段值设为 10000，
	// 以便其他响应者进行抢占或回退注册。
	//
	// 最小值为 0，最大值为 100000。
	// 0-999 区间保留给后缀为 *.k8s.io 的响应者。
	// 此字段必填。
	// +required
	// +k8s:required
	// +k8s:minimum=0
	// +k8s:maximum=100000
	// +k8s:customValidation
	Priority *int32 `json:"priority" protobuf:"varint,2,opt,name=priority"`
}

// PodStatus 表示 Pod 的状态信息。状态可能滞后于系统的实际状态，
// 尤其是在托管 Pod 的节点无法与控制面通信时。
type PodStatus struct {
	// ObservedGeneration 如果设置，表示 Pod 状态是基于哪个 .metadata.generation 设置的。
	// 使用此字段必须启用 PodObservedGenerationTracking 特性门控。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,17,opt,name=observedGeneration"`
	// Phase 是 Pod 生命周期阶段的简单高层概览。
	// conditions 数组、reason 和 message 字段以及各容器状态数组包含有关 Pod 状态的更多细节。
	// 阶段有五种可能取值：
	//
	// Pending：Pod 已被 Kubernetes 系统接受，但尚未创建一个或多个容器镜像。
	// 这包括调度前的时间以及通过网络下载镜像所花费的时间，后者可能较慢。
	// Running：Pod 已绑定到某个节点，所有容器均已创建。
	// 至少有一个容器仍在运行，或正在启动或重启过程中。
	// Succeeded：Pod 中所有容器均已成功终止，且不会重启。
	// Failed：Pod 中所有容器均已终止，且至少有一个容器以失败告终。
	// 该容器要么以非零状态退出，要么被系统终止。
	// Unknown：由于某种原因无法获取 Pod 的状态，通常是与 Pod 所在主机通信时出错。
	//
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-phase
	// +optional
	Phase PodPhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=PodPhase"`
	// Conditions 是 Pod 当前的服务状态。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-conditions
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []PodCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,rep,name=conditions"`
	// Message 是人类可读的消息，说明 Pod 处于当前状态的详细原因。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// Reason 是简短的驼峰命名消息，说明 Pod 处于此状态的详细原因。
	// 例如 'Evicted'。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// NominatedNodeName 仅在 Pod 抢占了节点上的其他 Pod 时设置，
	// 但由于被抢占的 Pod 需要优雅终止期，该 Pod 无法立即调度。
	// 此字段不保证 Pod 一定会被调度到该节点。如果其他节点更早可用，
	// 调度器可能决定将 Pod 放到别处。调度器也可能将本节点上的资源
	// 分配给抢占后创建的更高优先级 Pod。
	// 因此，Pod 被调度后，此字段可能与 PodSpec.nodeName 不同。
	// +optional
	NominatedNodeName string `json:"nominatedNodeName,omitempty" protobuf:"bytes,11,opt,name=nominatedNodeName"`

	// HostIP 保存 Pod 被分配到的主机的 IP 地址。Pod 尚未启动时为空。
	// Pod 可能被分配到 kubelet 有问题的节点，这意味着即使 Pod 已分配到节点，
	// HostIP 也不会更新。
	// +optional
	HostIP string `json:"hostIP,omitempty" protobuf:"bytes,5,opt,name=hostIP"`

	// HostIPs 保存分配给主机的 IP 地址。如果指定了此字段，第一个条目必须与 hostIP 字段一致。
	// Pod 尚未启动时此列表为空。
	// Pod 可能被分配到 kubelet 有问题的节点，这意味着即使 Pod 已分配到节点，
	// HostIPs 也不会更新。
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=ip
	// +listType=atomic
	HostIPs []HostIP `json:"hostIPs,omitempty" protobuf:"bytes,16,rep,name=hostIPs" patchStrategy:"merge" patchMergeKey:"ip"`

	// PodIP 是分配给 Pod 的 IP 地址，至少在集群内可路由。
	// 尚未分配时为空。
	// +optional
	PodIP string `json:"podIP,omitempty" protobuf:"bytes,6,opt,name=podIP"`

	// PodIPs 保存分配给 Pod 的 IP 地址。如果指定了此字段，第 0 个条目必须与 podIP 字段一致。
	// IPv4 和 IPv6 各自最多分配 1 个地址。
	// 尚未分配任何 IP 时此列表为空。
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=ip
	// +listType=map
	// +listMapKey=ip
	PodIPs []PodIP `json:"podIPs,omitempty" protobuf:"bytes,12,rep,name=podIPs" patchStrategy:"merge" patchMergeKey:"ip"`

	// StartTime 是 Kubelet 确认该对象的时间，采用 RFC 3339 格式。
	// 该时间早于 Kubelet 为 Pod 拉取容器镜像的时间。
	// +optional
	StartTime *metav1.Time `json:"startTime,omitempty" protobuf:"bytes,7,opt,name=startTime"`

	// InitContainerStatuses 是此 Pod 中 init 容器的状态。
	// 最近一次成功且不可重启的 init 容器将具有 ready = true，
	// 最近启动的容器将设置 startTime。
	// Pod 中每个 init 容器在此列表中最多应有一个状态，
	// 且所有状态都应针对 Pod 中的容器。
	// 但这一点并未被强制校验。
	// 如果列表中存在不存在容器的状态，或名称重复，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
	// +listType=atomic
	InitContainerStatuses []ContainerStatus `json:"initContainerStatuses,omitempty" protobuf:"bytes,10,rep,name=initContainerStatuses"`

	// ContainerStatuses 是此 Pod 中容器的状态。
	// Pod 中每个容器在此列表中最多应有一个状态，
	// 且所有状态都应针对 Pod 中的容器。
	// 但这一点并未被强制校验。
	// 如果列表中存在不存在容器的状态，或名称重复，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-and-container-status
	// +optional
	// +listType=atomic
	ContainerStatuses []ContainerStatus `json:"containerStatuses,omitempty" protobuf:"bytes,8,rep,name=containerStatuses"`

	// QOSClass 是根据资源需求为 Pod 分配的服务质量（QOS）类别。
	// 可用的 QOS 类别参见 PodQOSClass 类型。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-qos/#quality-of-service-classes
	// +optional
	QOSClass PodQOSClass `json:"qosClass,omitempty" protobuf:"bytes,9,rep,name=qosClass"`

	// EphemeralContainerStatuses 是在此 Pod 中运行过的临时容器的状态。
	// Pod 中每个临时容器在此列表中最多应有一个状态，
	// 且所有状态都应针对 Pod 中的容器。
	// 但这一点并未被强制校验。
	// 如果列表中存在不存在容器的状态，或名称重复，
	// 各 Kubernetes 组件的行为未定义，这些状态可能会被忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle#pod-and-container-status
	// +optional
	// +listType=atomic
	EphemeralContainerStatuses []ContainerStatus `json:"ephemeralContainerStatuses,omitempty" protobuf:"bytes,13,rep,name=ephemeralContainerStatuses"`

	// Resize 是 Pod 容器期望的资源调整状态。
	// 如果没有待处理的资源调整，则为空。
	// 对容器资源的任何更改都会自动将其设置为 "Proposed"。
	// Deprecated: 调整状态已迁移到两个 Pod 条件：PodResizePending 和 PodResizeInProgress。
	// PodResizePending 用于跟踪 spec 已调整但 Kubelet 尚未分配资源的状态。
	// PodResizeInProgress 用于跟踪进行中的调整，当已分配资源 != 已确认资源时应存在该条件。
	// +featureGate=InPlacePodVerticalScaling
	// +optional
	Resize PodResizeStatus `json:"resize,omitempty" protobuf:"bytes,14,opt,name=resize,casttype=PodResizeStatus"`

	// ResourceClaimStatuses 是资源声明的状态。
	// +patchMergeKey=name
	// +patchStrategy=merge,retainKeys
	// +listType=map
	// +listMapKey=name
	// +featureGate=DynamicResourceAllocation
	// +optional
	ResourceClaimStatuses []PodResourceClaimStatus `json:"resourceClaimStatuses,omitempty" patchStrategy:"merge,retainKeys" patchMergeKey:"name" protobuf:"bytes,15,rep,name=resourceClaimStatuses"`
	// ExtendedResourceClaimStatus 是由 DRA 支持的扩展资源声明的状态。
	// +featureGate=DRAExtendedResource
	// +optional
	ExtendedResourceClaimStatus *PodExtendedResourceClaimStatus `json:"extendedResourceClaimStatus,omitempty" protobuf:"bytes,18,opt,name=extendedResourceClaimStatus"`

	// AllocatedResources 是节点为此 Pod 分配的总请求量。
	// 如果未设置 Pod 级别请求，则为 Pod 内各容器请求量的总和。
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	AllocatedResources ResourceList `json:"allocatedResources,omitempty" protobuf:"bytes,19,rep,name=allocatedResources,casttype=ResourceList,castkey=ResourceName"`

	// Resources 表示在 PodSpec.Resources 中设置了 Pod 级别请求或限制时，
	// 在 Pod 级别应用的计算资源请求和限制。
	// +featureGate=InPlacePodLevelResourcesVerticalScaling
	// +optional
	Resources *ResourceRequirements `json:"resources,omitempty" protobuf:"bytes,20,opt,name=resources"`

	// NodeAllocatableResourceClaimStatuses 包含通过 DRA 声明为此 Pod 分配的节点可分配资源的状态。
	// 这包括当前在 v1.Node `status.allocatable` 中报告的、非扩展资源的资源
	//（参见 https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/#extended-resources）。
	// 例如 "cpu"、"memory"、"ephemeral-storage" 和 hugepages。
	// +featureGate=DRANodeAllocatableResources
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=resourceClaimName
	// +listType=map
	// +listMapKey=resourceClaimName
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=resourceClaimName
	NodeAllocatableResourceClaimStatuses []NodeAllocatableResourceClaimStatus `json:"nodeAllocatableResourceClaimStatuses,omitempty" patchStrategy:"merge" patchMergeKey:"resourceClaimName" protobuf:"bytes,21,rep,name=nodeAllocatableResourceClaimStatuses"`

	// VolumeHealth 包含节点报告的 Pod 所用各卷的健康状况。
	// 由 Pod 所在节点上的 kubelet 填充。
	// +featureGate=CSIVolumeHealth
	// +optional
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	VolumeHealth []PodVolumeHealth `json:"volumeHealth,omitempty" protobuf:"bytes,22,rep,name=volumeHealth"`
}

// +genclient
// +genclient:method=UpdateEphemeralContainers,verb=update,subresource=ephemeralcontainers
// +genclient:method=UpdateResize,verb=update,subresource=resize
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Pod 是可以在主机上运行的一组容器。该资源由客户端创建，并被调度到主机上。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/ephemeralcontainers"
// +k8s:supportsSubresource="/resize"
// +k8s:supportsSubresource="/eviction"
type Pod struct {
	metav1.TypeMeta `json:""`
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec PodSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Pod 最近一次观测到的状态。
	// 该数据可能不是最新的。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status PodStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodList 是 Pod 的列表。
type PodList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 列表。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md
	Items []Pod `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// PodTemplateSpec 描述从模板创建 Pod 时该 Pod 应具备的数据。
type PodTemplateSpec struct {
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec PodSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodTemplate 描述用于创建预定义 Pod 副本的模板。
type PodTemplate struct {
	metav1.TypeMeta `json:""`
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Template 定义将从此 Pod 模板创建的 Pod。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Template PodTemplateSpec `json:"template,omitempty" protobuf:"bytes,2,opt,name=template"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodTemplateList 是 PodTemplate 的列表。
type PodTemplateList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Pod 模板列表。
	Items []PodTemplate `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ReplicationControllerSpec 是副本控制器的规格说明。
type ReplicationControllerSpec struct {
	// Replicas 是期望的副本数。
	// 使用指针以区分显式设置为零和未指定。
	// 默认为 1。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#what-is-a-replicationcontroller
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=1
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	Replicas *int32 `json:"replicas,omitempty" protobuf:"varint,1,opt,name=replicas"`

	// 新创建的 Pod 在被视为可用之前，需要保持就绪且容器不崩溃的最短秒数。
	// 默认为 0（Pod 一旦就绪即被视为可用）。
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:optional
	// +default=0
	// +k8s:beta(since: "1.37")=+k8s:minimum=0
	MinReadySeconds int32 `json:"minReadySeconds,omitempty" protobuf:"varint,4,opt,name=minReadySeconds"`

	// Selector 是对 Pod 的标签查询，匹配的 Pod 计入 Replicas 数量。
	// 如果 Selector 为空，则默认使用 Pod 模板上的标签。
	// 副本控制器要管控的 Pod 必须匹配的标签键和值；为空时默认使用 Pod 模板上的标签。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/labels/#label-selectors
	// +optional
	// +mapType=atomic
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`

	// TemplateRef 是对某个对象的引用，该对象描述了当检测到副本不足时要创建的 Pod。
	// 对某个对象的引用，该对象描述了当检测到副本不足时要创建的 Pod。
	// +optional
	// TemplateRef *ObjectReference `json:"templateRef,omitempty"`

	// Template 是描述当检测到副本不足时要创建的 Pod 的对象。其优先级高于 TemplateRef。
	// template.spec.restartPolicy 唯一允许的值是 "Always"。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#pod-template
	// +optional
	// +k8s:alpha(since: "1.37")=+k8s:optional
	Template *PodTemplateSpec `json:"template,omitempty" protobuf:"bytes,3,opt,name=template"`
}

// ReplicationControllerStatus 表示副本控制器的当前状态。
type ReplicationControllerStatus struct {
	// Replicas 是最近一次观测到的副本数。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller#what-is-a-replicationcontroller
	Replicas int32 `json:"replicas" protobuf:"varint,1,opt,name=replicas"`

	// 标签与副本控制器 Pod 模板的标签相匹配的 Pod 数量。
	// +optional
	FullyLabeledReplicas int32 `json:"fullyLabeledReplicas,omitempty" protobuf:"varint,2,opt,name=fullyLabeledReplicas"`

	// 此副本控制器中就绪的副本数。
	// +optional
	ReadyReplicas int32 `json:"readyReplicas,omitempty" protobuf:"varint,4,opt,name=readyReplicas"`

	// 此副本控制器中可用的副本数（至少就绪 minReadySeconds 秒）。
	// +optional
	AvailableReplicas int32 `json:"availableReplicas,omitempty" protobuf:"varint,5,opt,name=availableReplicas"`

	// ObservedGeneration 反映最近一次观测到的副本控制器的代（generation）。
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty" protobuf:"varint,3,opt,name=observedGeneration"`

	// 表示对副本控制器当前状态的最新可用观测。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []ReplicationControllerCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,6,rep,name=conditions"`
}

// ReplicationControllerConditionType 表示副本控制器的条件类型。
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
	Type ReplicationControllerConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=ReplicationControllerConditionType"`
	// 条件的状态，取值为 True、False、Unknown 之一。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 条件上一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,3,opt,name=lastTransitionTime"`
	// 条件上一次状态转变的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,4,opt,name=reason"`
	// 人类可读的消息，说明此次转变的详细信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,5,opt,name=message"`
}

// +genclient
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/api/autoscaling/v1.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/api/autoscaling/v1.Scale,result=k8s.io/api/autoscaling/v1.Scale
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/status"

// ReplicationController 表示一个副本控制器的配置。
type ReplicationController struct {
	metav1.TypeMeta `json:""`

	// 如果 ReplicationController 的 Labels 为空，则默认与其管理的 Pod 的标签相同。
	// 标准的对象元数据。更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:optional
	// +k8s:beta(since: "1.37")=+k8s:subfield(name)=+k8s:format=k8s-long-name
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义副本控制器期望行为的规格说明。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ReplicationControllerSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 是副本控制器最近一次观测到的状态。
	// 该数据可能有一段时间的延迟。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ReplicationControllerStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ReplicationControllerList 是副本控制器的集合。
type ReplicationControllerList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 副本控制器列表。
	// 更多信息：https://kubernetes.io/docs/concepts/workloads/controllers/replicationcontroller
	Items []ReplicationController `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ServiceAffinity 表示会话亲和类型（字符串）。
// +enum
type ServiceAffinity string

const (
	// ServiceAffinityClientIP 表示基于客户端 IP 的会话亲和。
	ServiceAffinityClientIP ServiceAffinity = "ClientIP"

	// ServiceAffinityNone 表示无会话亲和。
	ServiceAffinityNone ServiceAffinity = "None"
)

// DefaultClientIPServiceAffinitySeconds 是客户端 IP 会话亲和的默认超时秒数。
const DefaultClientIPServiceAffinitySeconds int32 = 10800

// SessionAffinityConfig 表示会话亲和的配置。
type SessionAffinityConfig struct {
	// clientIP 包含基于客户端 IP 的会话亲和配置。
	// +optional
	ClientIP *ClientIPConfig `json:"clientIP,omitempty" protobuf:"bytes,1,opt,name=clientIP"`
}

// ClientIPConfig 表示基于客户端 IP 的会话亲和配置。
type ClientIPConfig struct {
	// timeoutSeconds 指定 ClientIP 类型会话保持的秒数。
	// 当 ServiceAffinity == "ClientIP" 时，该值必须 >0 && <=86400（1 天）。
	// 默认值为 10800（3 小时）。
	// +optional
	TimeoutSeconds *int32 `json:"timeoutSeconds,omitempty" protobuf:"varint,1,opt,name=timeoutSeconds"`
}

// ServiceType 是描述 Service 暴露方式的字符串类型。
// +enum
type ServiceType string

const (
	// ServiceTypeClusterIP 表示 Service 只能通过集群内 IP 在集群内部访问。
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

// ServiceInternalTrafficPolicy 描述节点如何分发其在 ClusterIP 上接收到的 Service 流量。
// +enum
type ServiceInternalTrafficPolicy string

const (
	// ServiceInternalTrafficPolicyCluster 将流量路由到所有端点。
	ServiceInternalTrafficPolicyCluster ServiceInternalTrafficPolicy = "Cluster"

	// ServiceInternalTrafficPolicyLocal 仅将流量路由到与客户端 Pod 位于同一节点的端点
	// （如果没有本地端点则丢弃流量）。
	ServiceInternalTrafficPolicyLocal ServiceInternalTrafficPolicy = "Local"
)

// ServiceInternalTrafficPolicy 描述节点如何分发其在 ClusterIP 上接收到的 Service 流量。
// +enum
//
// 已弃用：请改用 ServiceInternalTrafficPolicy。
type ServiceInternalTrafficPolicyType = ServiceInternalTrafficPolicy

// ServiceExternalTrafficPolicy 描述节点如何分发其在 Service 的"对外"地址
// （NodePort、ExternalIP 和 LoadBalancer IP）上接收到的流量。
// +enum
type ServiceExternalTrafficPolicy string

const (
	// ServiceExternalTrafficPolicyCluster 将流量路由到所有端点。
	ServiceExternalTrafficPolicyCluster ServiceExternalTrafficPolicy = "Cluster"

	// ServiceExternalTrafficPolicyLocal 仅将流量路由到接收该流量的节点上的端点，
	// 从而保留流量的源 IP（如果没有本地端点则丢弃流量）。
	ServiceExternalTrafficPolicyLocal ServiceExternalTrafficPolicy = "Local"
)

// ServiceExternalTrafficPolicy 描述节点如何分发其在 Service 的"对外"地址
// （NodePort、ExternalIP 和 LoadBalancer IP）上接收到的流量。
// +enum
//
// 已弃用：请改用 ServiceExternalTrafficPolicy。
type ServiceExternalTrafficPolicyType = ServiceExternalTrafficPolicy

const (
	// ServiceExternalTrafficPolicyTypeLocal 是 ServiceExternalTrafficPolicyLocal 的旧称。
	ServiceExternalTrafficPolicyTypeLocal = ServiceExternalTrafficPolicyLocal
	// ServiceExternalTrafficPolicyTypeCluster 是 ServiceExternalTrafficPolicyCluster 的旧称。
	ServiceExternalTrafficPolicyTypeCluster = ServiceExternalTrafficPolicyCluster
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
	// 已弃用：请改用 "PreferSameZone"。
	ServiceTrafficDistributionPreferClose = "PreferClose"
)

// 以下是 Service 的有效条件。
const (
	// LoadBalancerPortsError 表示云负载均衡器实例上所请求端口的条件。
	LoadBalancerPortsError = "LoadBalancerPortsError"
	// LoadBalancerPortsErrorReason 是 ServiceStatus 中 LoadBalancerPortsError 条件的原因，
	// 表示负载均衡器未能正确配置。
	LoadBalancerPortsErrorReason = "LoadBalancerMixedProtocolNotSupported"
)

// ServiceStatus 表示 Service 的当前状态。
type ServiceStatus struct {
	// LoadBalancer 包含负载均衡器的当前状态（如果存在）。
	// +optional
	LoadBalancer LoadBalancerStatus `json:"loadBalancer,omitempty" protobuf:"bytes,1,opt,name=loadBalancer"`
	// Service 的当前状态
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	// +k8s:alpha(since: "1.37")=+k8s:eachVal=+k8s:opaqueType
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,rep,name=conditions"`
}

// LoadBalancerStatus 表示负载均衡器的状态。
type LoadBalancerStatus struct {
	// Ingress 是包含负载均衡器入口点的列表。
	// 发往该 Service 的流量应发送到这些入口点。
	// +optional
	// +listType=atomic
	Ingress []LoadBalancerIngress `json:"ingress,omitempty" protobuf:"bytes,1,rep,name=ingress"`
}

// LoadBalancerIngress 表示负载均衡器入口点的状态：
// 发往该 Service 的流量应发送到某个入口点。
type LoadBalancerIngress struct {
	// IP 用于基于 IP 的负载均衡器入口点（通常是 GCE 或 OpenStack 负载均衡器）。
	// +optional
	IP string `json:"ip,omitempty" protobuf:"bytes,1,opt,name=ip"`

	// Hostname 用于基于 DNS 的负载均衡器入口点（通常是 AWS 负载均衡器）。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,2,opt,name=hostname"`

	// IPMode 指定负载均衡器 IP 的行为方式，只能在指定了 ip 字段时指定。
	// 设为 "VIP" 表示流量被送到节点，目标地址为负载均衡器的 IP 和端口。
	// 设为 "Proxy" 表示流量被送到节点或 Pod，目标地址为节点的 IP 和 nodePort，或 Pod 的 IP 和端口。
	// Service 的实现可利用此信息调整流量路由。
	// +optional
	IPMode *LoadBalancerIPMode `json:"ipMode,omitempty" protobuf:"bytes,3,opt,name=ipMode"`

	// Ports 是 Service 端口记录的列表。
	// 如果使用该字段，Service 中定义的每个端口都应有一条对应记录。
	// +listType=atomic
	// +optional
	Ports []PortStatus `json:"ports,omitempty" protobuf:"bytes,4,rep,name=ports"`
}

// IPFamily 表示 IP 协议族（IPv4 或 IPv6）。该类型用于表达某个类型所描述的 IP 的协议族
// （例如 service.spec.ipFamilies）。
// +enum
type IPFamily string

const (
	// IPv4Protocol 表示此 IP 为 IPv4 协议。
	IPv4Protocol IPFamily = "IPv4"
	// IPv6Protocol 表示此 IP 为 IPv6 协议。
	IPv6Protocol IPFamily = "IPv6"
	// IPFamilyUnknown 表示此 IP 为未知协议。
	IPFamilyUnknown IPFamily = ""
)

// IPFamilyPolicy 表示 Service 请求或要求的双栈特性。
// +enum
type IPFamilyPolicy string

const (
	// IPFamilyPolicySingleStack 表示此 Service 必须使用单一 IP 协议族。
	// 分配的 IP 协议族基于集群默认使用的 IP 协议族，或由 service.spec.ipFamilies 字段指定。
	IPFamilyPolicySingleStack IPFamilyPolicy = "SingleStack"
	// IPFamilyPolicyPreferDualStack 表示当集群配置了双栈时，此 Service 优先使用双栈。
	// 如果集群未配置双栈，则为该 Service 分配单一 IP 协议族。
	// 如果 service.spec.ipFamilies 未设置 IP 协议族，则分配集群配置的默认 IP 协议族。
	IPFamilyPolicyPreferDualStack IPFamilyPolicy = "PreferDualStack"
	// IPFamilyPolicyRequireDualStack 表示此 Service 必须使用双栈。
	// 在单栈集群上使用 IPFamilyPolicyRequireDualStack 会导致校验错误。
	// 分配给该 Service 的 IPFamilies（及其顺序）基于 service.spec.ipFamilies；
	// 如果未提供 service.spec.ipFamilies，则按集群的配置进行分配。
	// 如果 service.spec.ipFamilies 只有一个条目，apiserver 会补上另一种 IP 协议族。
	IPFamilyPolicyRequireDualStack IPFamilyPolicy = "RequireDualStack"
)

// IPFamilyPolicy 表示 Service 请求或要求的双栈特性。
// +enum
//
// 已弃用：请改用 IPFamilyPolicy。
type IPFamilyPolicyType = IPFamilyPolicy

// ServiceSpec 描述用户在 Service 上创建的属性。
type ServiceSpec struct {
	// 此 Service 暴露的端口列表。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +patchMergeKey=port
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=port
	// +listMapKey=protocol
	Ports []ServicePort `json:"ports,omitempty" patchStrategy:"merge" patchMergeKey:"port" protobuf:"bytes,1,rep,name=ports"`

	// 将 Service 流量路由到标签键和值与此 selector 匹配的 Pod。
	// 如果为空或不存在，则认为该 Service 由外部进程管理其端点，Kubernetes 不会修改。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。
	// 当类型为 ExternalName 时忽略。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/
	// +optional
	// +mapType=atomic
	Selector map[string]string `json:"selector,omitempty" protobuf:"bytes,2,rep,name=selector"`

	// clusterIP 是 Service 的 IP 地址，通常随机分配。
	// 如果手动指定了地址、该地址在范围内（按系统配置）且未被占用，则会分配给该 Service；
	// 否则 Service 创建会失败。除非同时将 type 字段改为 ExternalName（此时该字段必须为空），
	// 或将 type 从 ExternalName 改回（此时可按上述说明选择性地指定该字段），
	// 否则不可通过更新修改此字段。有效值为 "None"、空字符串（""）或合法的 IP 地址。
	// 将其设为 "None" 会创建"无头 Service"（无虚拟 IP），适用于希望直接连接端点而无需代理的场景。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。
	// 创建 ExternalName 类型的 Service 时如果指定此字段，创建会失败。
	// 将 Service 更新为 ExternalName 类型时，此字段会被清除。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +optional
	ClusterIP string `json:"clusterIP,omitempty" protobuf:"bytes,3,opt,name=clusterIP"`

	// ClusterIPs 是分配给此 Service 的 IP 地址列表，通常随机分配。
	// 如果手动指定了地址、该地址在范围内（按系统配置）且未被占用，则会分配给该 Service；
	// 否则 Service 创建会失败。除非同时将 type 字段改为 ExternalName（此时该字段必须为空），
	// 或将 type 从 ExternalName 改回（此时可按上述说明选择性地指定该字段），
	// 否则不可通过更新修改此字段。有效值为 "None"、空字符串（""）或合法的 IP 地址。
	// 将其设为 "None" 会创建"无头 Service"（无虚拟 IP），适用于希望直接连接端点而无需代理的场景。
	// 仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型。
	// 创建 ExternalName 类型的 Service 时如果指定此字段，创建会失败。
	// 将 Service 更新为 ExternalName 类型时，此字段会被清除。
	// 如果未指定此字段，将从 clusterIP 字段初始化。
	// 如果指定了此字段，客户端必须确保 clusterIPs[0] 与 clusterIP 的值相同。
	//
	// 此字段最多可包含两个条目（双栈 IP，顺序任意）。
	// 这些 IP 必须与 ipFamilies 字段的值对应。clusterIPs 和 ipFamilies 均由 ipFamilyPolicy 字段约束。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +listType=atomic
	// +optional
	ClusterIPs []string `json:"clusterIPs,omitempty" protobuf:"bytes,18,opt,name=clusterIPs"`

	// type 决定 Service 的暴露方式。默认为 ClusterIP。
	// 有效选项为 ExternalName、ClusterIP、NodePort 和 LoadBalancer。
	// "ClusterIP" 分配一个集群内部 IP 地址，用于对端点进行负载均衡。
	// 端点由 selector 决定；如果未指定 selector，则由手工创建的 Endpoints 对象或 EndpointSlice 对象决定。
	// 如果 clusterIP 为 "None"，则不分配虚拟 IP，端点以一组端点的形式发布，而非虚拟 IP。
	// "NodePort" 在 ClusterIP 基础上，在每个节点上分配一个端口，路由到与 clusterIP 相同的端点。
	// "LoadBalancer" 在 NodePort 基础上，创建一个外部负载均衡器（如果当前云环境支持），
	// 路由到与 clusterIP 相同的端点。
	// "ExternalName" 将此 Service 别名为指定的 externalName。
	// 若干其他字段不适用于 ExternalName 类型的 Service。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#publishing-services-service-types
	// +optional
	Type ServiceType `json:"type,omitempty" protobuf:"bytes,4,opt,name=type,casttype=ServiceType"`

	// externalIPs 是一组 IP 地址，集群中的节点也会接受发往这些地址的 Service 流量。
	// 这些 IP 不由 Kubernetes 管理。用户需负责确保流量到达具有该 IP 的节点。
	// 常见例子是不属于 Kubernetes 系统的外部负载均衡器。
	// +optional
	// +listType=atomic
	ExternalIPs []string `json:"externalIPs,omitempty" protobuf:"bytes,5,rep,name=externalIPs"`

	// 支持 "ClientIP" 和 "None"。用于维持会话亲和。
	// 启用基于客户端 IP 的会话亲和。
	// 必须为 ClientIP 或 None。
	// 默认为 None。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#virtual-ips-and-service-proxies
	// +optional
	SessionAffinity ServiceAffinity `json:"sessionAffinity,omitempty" protobuf:"bytes,7,opt,name=sessionAffinity,casttype=ServiceAffinity"`

	// 仅适用于 LoadBalancer 类型的 Service。
	// 此功能取决于底层云厂商是否支持在创建负载均衡器时指定 loadBalancerIP。
	// 如果云厂商不支持此功能，该字段将被忽略。
	// 已弃用：此字段定义不明确，各实现的含义不一。使用它不具备可移植性，且可能不支持双栈。
	// 鼓励用户在可用时使用特定于实现的注解。
	// +optional
	LoadBalancerIP string `json:"loadBalancerIP,omitempty" protobuf:"bytes,8,opt,name=loadBalancerIP"`

	// 如果指定且平台支持，通过云厂商负载均衡器的流量将被限制在指定的客户端 IP 范围内。
	// 如果云厂商不支持此功能，该字段将被忽略。
	// 更多信息：https://kubernetes.io/docs/tasks/access-application-cluster/create-external-load-balancer/
	// +optional
	// +listType=atomic
	LoadBalancerSourceRanges []string `json:"loadBalancerSourceRanges,omitempty" protobuf:"bytes,9,opt,name=loadBalancerSourceRanges"`

	// externalName 是发现机制作为此 Service 别名返回的外部引用（例如 DNS CNAME 记录）。
	// 不涉及任何代理。必须是小写 RFC-1123 主机名
	// (https://tools.ietf.org/html/rfc1123)，且要求 `type` 为 "ExternalName"。
	// +optional
	ExternalName string `json:"externalName,omitempty" protobuf:"bytes,10,opt,name=externalName"`

	// externalTrafficPolicy 描述节点如何分发其在 Service 的"对外"地址
	// （NodePort、ExternalIP 和 LoadBalancer IP）上接收到的 Service 流量。
	// 若设为 "Local"，代理会按如下方式配置 Service：假设外部负载均衡器负责在节点间均衡流量，
	// 因此每个节点只将流量送到本节点上的 Service 端点，且不伪装客户端源 IP。
	// （误发到没有端点的节点的流量将被丢弃。）
	// 默认值 "Cluster" 使用标准行为：均匀路由到所有端点（可能受拓扑及其他特性影响）。
	// 注意，从集群内部发往 External IP 或 LoadBalancer IP 的流量始终采用 "Cluster" 语义；
	// 但从集群内部发往 NodePort 的客户端在选择节点时可能需要考虑流量策略。
	// +optional
	ExternalTrafficPolicy ServiceExternalTrafficPolicy `json:"externalTrafficPolicy,omitempty" protobuf:"bytes,11,opt,name=externalTrafficPolicy"`

	// healthCheckNodePort 指定 Service 的健康检查 nodePort。
	// 仅在 type 为 LoadBalancer 且 externalTrafficPolicy 为 Local 时适用。
	// 如果指定的值在范围内且未被占用，则使用该值；如果未指定，则自动分配。
	// 外部系统（例如负载均衡器）可用此端口判断某个节点是否承载该 Service 的端点。
	// 创建不需要此字段的 Service 时若指定它，创建会失败。
	// 将 Service 更新为不再需要此字段（例如改变 type）时，该字段会被清除。
	// 该字段一旦设置便不可更新。
	// +optional
	HealthCheckNodePort int32 `json:"healthCheckNodePort,omitempty" protobuf:"bytes,12,opt,name=healthCheckNodePort"`

	// publishNotReadyAddresses 表示处理此 Service 端点的任何代理都应忽略就绪/未就绪的标记。
	// 设置此字段的主要场景是 StatefulSet 的无头 Service 传播其 Pod 的 SRV DNS 记录，用于对等发现。
	// 为 Service 生成 Endpoints 和 EndpointSlice 资源的 Kubernetes 控制器会将其理解为：
	// 即使 Pod 本身未就绪，所有端点也视为"就绪"。
	// 仅通过 Endpoints 或 EndpointSlice 资源消费 Kubernetes 生成端点的代理，可以安全地依赖此行为。
	// +optional
	PublishNotReadyAddresses bool `json:"publishNotReadyAddresses,omitempty" protobuf:"varint,13,opt,name=publishNotReadyAddresses"`

	// sessionAffinityConfig 包含会话亲和的配置。
	// +optional
	SessionAffinityConfig *SessionAffinityConfig `json:"sessionAffinityConfig,omitempty" protobuf:"bytes,14,opt,name=sessionAffinityConfig"`

	// TopologyKeys 已被墓碑化，用于说明为何 16 是保留的 protobuf 标签。
	// TopologyKeys []string `json:"topologyKeys,omitempty" protobuf:"bytes,16,opt,name=topologyKeys"`

	// IPFamily 已被墓碑化，用于说明为何 15 是保留的 protobuf 标签。
	// IPFamily *IPFamily `json:"ipFamily,omitempty" protobuf:"bytes,15,opt,name=ipFamily,Configcasttype=IPFamily"`

	// IPFamilies 是分配给此 Service 的 IP 协议族列表（例如 IPv4、IPv6）。
	// 此字段通常根据集群配置和 ipFamilyPolicy 字段自动分配。
	// 如果手动指定此字段，且所请求的协议族在集群中可用且 ipFamilyPolicy 允许，则会使用它；
	// 否则 Service 创建会失败。此字段可有条件地修改：允许添加或删除次要 IP 协议族，
	// 但不允许更改 Service 的主要 IP 协议族。有效值为 "IPv4" 和 "IPv6"。
	// 此字段仅适用于 ClusterIP、NodePort 和 LoadBalancer 类型的 Service，也适用于"无头" Service。
	// 将 Service 更新为 ExternalName 类型时，此字段会被清除。
	//
	// 此字段最多可包含两个条目（双栈协议族，顺序任意）。
	// 如果指定了 clusterIPs 字段，这些协议族必须与其值对应。
	// clusterIPs 和 ipFamilies 均由 ipFamilyPolicy 字段约束。
	// +listType=atomic
	// +optional
	IPFamilies []IPFamily `json:"ipFamilies,omitempty" protobuf:"bytes,19,opt,name=ipFamilies,casttype=IPFamily"`

	// IPFamilyPolicy 表示此 Service 请求或要求的双栈特性。如果未提供值，则该字段设为 SingleStack。
	// Service 可以是 "SingleStack"（单一 IP 协议族）、
	// "PreferDualStack"（在配置了双栈的集群中为两个 IP 协议族，在单栈集群中为单一 IP 协议族），
	// 或 "RequireDualStack"（在配置了双栈的集群中为两个 IP 协议族，否则失败）。
	// ipFamilies 和 clusterIPs 字段取决于此字段的值。
	// 将 Service 更新为 ExternalName 类型时，此字段会被清除。
	// +optional
	IPFamilyPolicy *IPFamilyPolicy `json:"ipFamilyPolicy,omitempty" protobuf:"bytes,17,opt,name=ipFamilyPolicy,casttype=IPFamilyPolicy"`

	// allocateLoadBalancerNodePorts 定义是否为 LoadBalancer 类型的 Service 自动分配 NodePort。
	// 默认为 "true"。如果集群负载均衡器不依赖 NodePort，可设为 "false"。
	// 如果调用方请求了特定的 NodePort（通过指定值），无论此字段如何，这些请求都会被尊重。
	// 此字段只能为 LoadBalancer 类型的 Service 设置，若 type 改为其他类型则会被清除。
	// +optional
	AllocateLoadBalancerNodePorts *bool `json:"allocateLoadBalancerNodePorts,omitempty" protobuf:"bytes,20,opt,name=allocateLoadBalancerNodePorts"`

	// loadBalancerClass 是此 Service 所属的负载均衡器实现的类别。
	// 如果指定，该字段的值必须是带可选前缀的标签风格标识符，
	// 例如 "internal-vip" 或 "example.com/internal-vip"。无前缀的名称保留给最终用户。
	// 此字段只能在 Service 类型为 'LoadBalancer' 时设置。如果未设置，则使用默认的负载均衡器实现；
	// 目前通常通过云厂商集成实现，但应适用于任何默认实现。如果设置了此字段，
	// 则认为存在某个负载均衡器实现正在监听具有匹配类别的 Service。
	// 任何默认的负载均衡器实现（例如云厂商）都应忽略设置了此字段的 Service。
	// 此字段只能在创建 Service 或将其更新为 'LoadBalancer' 类型时设置。
	// 一旦设置便不可更改。将 Service 更新为非 'LoadBalancer' 类型时，此字段会被清除。
	// +optional
	LoadBalancerClass *string `json:"loadBalancerClass,omitempty" protobuf:"bytes,21,opt,name=loadBalancerClass"`

	// InternalTrafficPolicy 描述节点如何分发其在 ClusterIP 上接收到的 Service 流量。
	// 若设为 "Local"，代理会假定 Pod 只希望与同一节点上的 Service 端点通信，
	// 如果没有本地端点则丢弃流量。默认值 "Cluster" 使用标准行为：
	// 均匀路由到所有端点（可能受拓扑及其他特性影响）。
	// +optional
	InternalTrafficPolicy *ServiceInternalTrafficPolicy `json:"internalTrafficPolicy,omitempty" protobuf:"bytes,22,opt,name=internalTrafficPolicy"`

	// TrafficDistribution 提供一种表达流量分发到 Service 端点的偏好的方式。
	// 实现可以将此字段作为提示，但无需保证严格遵循。
	// 如果未设置此字段，实现将采用其默认路由策略。
	// 如果设为 "PreferClose"，实现应优先选择同一可用区内的端点。
	// +optional
	TrafficDistribution *string `json:"trafficDistribution,omitempty" protobuf:"bytes,23,opt,name=trafficDistribution"`
}

// ServicePort 包含 Service 端口的信息。
type ServicePort struct {
	// 此端口在 Service 内的名称，必须是 DNS_LABEL。
	// 一个 ServiceSpec 中的所有端口名称必须唯一。
	// 在考虑 Service 的端点时，此名称必须与 EndpointPort 的 'name' 字段匹配。
	// 如果此 Service 只定义了一个 ServicePort，则该字段可选。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// 此端口的 IP 协议。支持 "TCP"、"UDP" 和 "SCTP"。
	// 默认为 TCP。
	// +default="TCP"
	// +optional
	Protocol Protocol `json:"protocol,omitempty" protobuf:"bytes,2,opt,name=protocol,casttype=Protocol"`

	// 此端口的应用层协议。
	// 实现可将其作为提示，为所理解的协议提供更丰富的行为。
	// 此字段遵循标准的 Kubernetes 标签语法。
	// 有效值为以下之一：
	//
	// * 无前缀的协议名 - 保留给 IANA 标准服务名（参见 RFC-6335 和 https://www.iana.org/assignments/service-names）。
	//
	// * Kubernetes 定义的前缀名称：
	//   * 'kubernetes.io/h2c' - 明文上的 HTTP/2 先验知识，参见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - 明文上的 WebSocket，参见 https://www.rfc-editor.org/rfc6455
	//   * 'kubernetes.io/wss' - TLS 上的 WebSocket，参见 https://www.rfc-editor.org/rfc6455
	//
	// * 其他协议应使用实现自定义的前缀名称，例如 mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string `json:"appProtocol,omitempty" protobuf:"bytes,6,opt,name=appProtocol"`

	// 此 Service 暴露的端口。
	Port int32 `json:"port" protobuf:"varint,3,opt,name=port"`

	// 此 Service 目标 Pod 上要访问的端口号或端口名。
	// 数字必须在 1 到 65535 范围内。名称必须是 IANA_SVC_NAME。
	// 如果为字符串，将在目标 Pod 的容器端口中按名称查找。如果未指定，则使用 'port' 字段的值
	// （即恒等映射）。
	// 对于 clusterIP=None 的 Service，此字段被忽略，应省略或设为与 'port' 字段相同。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#defining-a-service
	// +optional
	TargetPort intstr.IntOrString `json:"targetPort,omitempty" protobuf:"bytes,4,opt,name=targetPort"`

	// 当 type 为 NodePort 或 LoadBalancer 时，此 Service 在每个节点上暴露的端口。
	// 通常由系统分配。如果指定的值在范围内且未被占用，则使用该值，否则操作会失败。
	// 如果未指定，而此 Service 需要端口，则会分配一个。
	// 创建不需要此字段的 Service 时若指定它，创建会失败。
	// 将 Service 更新为不再需要此字段（例如从 NodePort 改为 ClusterIP）时，该字段会被清除。
	// 更多信息：https://kubernetes.io/docs/concepts/services-networking/service/#type-nodeport
	// +optional
	NodePort int32 `json:"nodePort,omitempty" protobuf:"varint,5,opt,name=nodePort"`
}

// +genclient
// +genclient:skipVerbs=deleteCollection
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Service 是对软件服务（例如 mysql）的命名抽象，由代理监听的本地端口
// （例如 3306）和决定哪些 Pod 响应经代理发送请求的 selector 组成。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/proxy"
type Service struct {
	metav1.TypeMeta `json:""`
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 Service 的行为。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ServiceSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Service 最近一次观测到的状态。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ServiceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

const (
	// ClusterIPNone - 不分配集群 IP。
	// 无需代理，也不应为 Pod 创建环境变量。
	ClusterIPNone = "None"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ServiceList 保存 Service 的列表。
type ServiceList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Service 列表。
	Items []Service `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +genclient:method=CreateToken,verb=create,subresource=token,input=k8s.io/api/authentication/v1.TokenRequest,result=k8s.io/api/authentication/v1.TokenRequest
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ServiceAccount 将以下内容绑定在一起：
// * 一个可供用户（可能还有外围系统）理解的名称，用于标识身份
// * 一个可被认证和授权的主体
// * 一组 Secret
type ServiceAccount struct {
	metav1.TypeMeta `json:""`
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Secrets 是同一命名空间中的 Secret 列表，使用此 ServiceAccount 运行的 Pod 可以使用这些 Secret。
	// 只有当此 ServiceAccount 设置了 "kubernetes.io/enforce-mountable-secrets" 注解为 "true" 时，
	// Pod 才受此列表限制。
	// "kubernetes.io/enforce-mountable-secrets" 注解自 v1.32 起已弃用。
	// 建议使用独立的命名空间来隔离对已挂载 Secret 的访问。
	// 不应使用此字段查找自动生成的 ServiceAccount 令牌 Secret 以供 Pod 之外使用。
	// 应改用 TokenRequest API 直接请求令牌，或手动创建 ServiceAccount 令牌 Secret。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/secret
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=name
	Secrets []ObjectReference `json:"secrets,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,2,rep,name=secrets"`

	// ImagePullSecrets 是对同一命名空间中 Secret 的引用列表，用于拉取引用此 ServiceAccount 的 Pod 中的任何镜像。
	// ImagePullSecrets 与 Secrets 不同：Secret 可以挂载到 Pod 中，而 ImagePullSecrets 只有 kubelet 访问。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images/#specifying-imagepullsecrets-on-a-pod
	// +optional
	// +listType=atomic
	ImagePullSecrets []LocalObjectReference `json:"imagePullSecrets,omitempty" protobuf:"bytes,3,rep,name=imagePullSecrets"`

	// AutomountServiceAccountToken 表示以此 ServiceAccount 运行的 Pod 是否应自动挂载 API 令牌。
	// 可在 Pod 级别覆盖。
	// +optional
	AutomountServiceAccountToken *bool `json:"automountServiceAccountToken,omitempty" protobuf:"varint,4,opt,name=automountServiceAccountToken"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ServiceAccountList 是 ServiceAccount 对象的列表。
type ServiceAccountList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ServiceAccount 列表。
	// 更多信息：https://kubernetes.io/docs/tasks/configure-pod-container/configure-service-account/
	Items []ServiceAccount `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Endpoints 是实现实际 Service 的端点集合。示例：
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
//
// Endpoints 是旧版 API，不包含所有 Service 特性的信息。
// 如需 Service 端点的完整信息，请使用 discoveryv1.EndpointSlice。
//
// 已弃用：此 API 自 v1.33+ 起已弃用。请改用 discoveryv1.EndpointSlice。
type Endpoints struct {
	metav1.TypeMeta `json:""`
	// 标准的对象元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 所有端点的集合是各个子集的并集。地址按共享的 IP 归入不同子集。
	// 同一个地址若具有多个端口，其中部分就绪、部分未就绪（因为它们来自不同容器），
	// 该地址会在不同子集里针对不同端口分别出现。
	// 在同一子集中，某个地址不会同时出现在 Addresses 和 NotReadyAddresses 中。
	// 构成 Service 的地址和端口集合。
	// +optional
	// +listType=atomic
	Subsets []EndpointSubset `json:"subsets,omitempty" protobuf:"bytes,2,rep,name=subsets"`
}

// EndpointSubset 是具有共同端口集合的一组地址。
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
//
// 已弃用：此 API 自 v1.33+ 起已弃用。
type EndpointSubset struct {
	// 提供相关端口且被标记为就绪的 IP 地址。这些端点可视为对负载均衡器和客户端是安全的。
	// +optional
	// +listType=atomic
	Addresses []EndpointAddress `json:"addresses,omitempty" protobuf:"bytes,1,rep,name=addresses"`
	// 提供相关端口但当前未标记为就绪的 IP 地址，原因可能是尚未完成启动、
	// 最近未通过就绪检查，或最近未通过存活检查。
	// +optional
	// +listType=atomic
	NotReadyAddresses []EndpointAddress `json:"notReadyAddresses,omitempty" protobuf:"bytes,2,rep,name=notReadyAddresses"`
	// 相关 IP 地址上可用的端口号。
	// +optional
	// +listType=atomic
	Ports []EndpointPort `json:"ports,omitempty" protobuf:"bytes,3,rep,name=ports"`
}

// EndpointAddress 是描述单个 IP 地址的元组。
// 已弃用：此 API 自 v1.33+ 起已弃用。
// +structType=atomic
type EndpointAddress struct {
	// 此端点的 IP。
	// 不得为回环地址（127.0.0.0/8 或 ::1）、链路本地地址（169.254.0.0/16 或 fe80::/10），
	// 或链路本地组播地址（224.0.0.0/24 或 ff02::/16）。
	IP string `json:"ip" protobuf:"bytes,1,opt,name=ip"`
	// 此端点的 Hostname。
	// +optional
	Hostname string `json:"hostname,omitempty" protobuf:"bytes,3,opt,name=hostname"`
	// 可选：承载此端点的节点。可用于确定某个节点上的本地端点。
	// +optional
	NodeName *string `json:"nodeName,omitempty" protobuf:"bytes,4,opt,name=nodeName"`
	// 对提供该端点的对象的引用。
	// +optional
	TargetRef *ObjectReference `json:"targetRef,omitempty" protobuf:"bytes,2,opt,name=targetRef"`
}

// EndpointPort 是描述单个端口的元组。
// 已弃用：此 API 自 v1.33+ 起已弃用。
// +structType=atomic
type EndpointPort struct {
	// 此端口的名称。必须与对应的 ServicePort 中的 'name' 字段一致。
	// 必须是 DNS_LABEL。
	// 仅在只定义一个端口时可选。
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`

	// 端点的端口号。
	Port int32 `json:"port" protobuf:"varint,2,opt,name=port"`

	// 此端口的 IP 协议。
	// 必须是 UDP、TCP 或 SCTP。
	// 默认为 TCP。
	// +optional
	Protocol Protocol `json:"protocol,omitempty" protobuf:"bytes,3,opt,name=protocol,casttype=Protocol"`

	// 此端口的应用层协议。
	// 实现可将其作为提示，为所理解的协议提供更丰富的行为。
	// 此字段遵循标准的 Kubernetes 标签语法。
	// 有效值为以下之一：
	//
	// * 无前缀的协议名 - 保留给 IANA 标准服务名（参见 RFC-6335 和 https://www.iana.org/assignments/service-names）。
	//
	// * Kubernetes 定义的前缀名称：
	//   * 'kubernetes.io/h2c' - 明文上的 HTTP/2 先验知识，参见 https://www.rfc-editor.org/rfc/rfc9113.html#name-starting-http-2-with-prior-
	//   * 'kubernetes.io/ws'  - 明文上的 WebSocket，参见 https://www.rfc-editor.org/rfc6455
	//   * 'kubernetes.io/wss' - TLS 上的 WebSocket，参见 https://www.rfc-editor.org/rfc6455
	//
	// * 其他协议应使用实现自定义的前缀名称，例如 mycompany.com/my-custom-protocol。
	// +optional
	AppProtocol *string `json:"appProtocol,omitempty" protobuf:"bytes,4,opt,name=appProtocol"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// EndpointsList 是端点的列表。
// 已弃用：此 API 自 v1.33+ 起已弃用。
type EndpointsList struct {
	metav1.TypeMeta `json:""`
	// 标准的列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 端点列表。
	Items []Endpoints `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// NodeSpec 描述创建节点时所使用的属性。
type NodeSpec struct {
	// PodCIDR 表示分配给节点的 Pod IP 地址范围。
	// +optional
	PodCIDR string `json:"podCIDR,omitempty" protobuf:"bytes,1,opt,name=podCIDR"`

	// podCIDRs 表示分配给节点供其上 Pod 使用的 IP 地址范围。
	// 如果指定了此字段，第 0 个条目必须与 podCIDR 字段一致。
	// IPv4 和 IPv6 各自最多可包含 1 个值。
	// +optional
	// +patchStrategy=merge
	// +listType=set
	PodCIDRs []string `json:"podCIDRs,omitempty" protobuf:"bytes,7,opt,name=podCIDRs" patchStrategy:"merge"`

	// 云厂商分配的节点 ID，格式为：<ProviderName>://<ProviderSpecificNodeID>
	// +optional
	// +k8s:alpha(since: "1.36")=+k8s:optional
	// +k8s:alpha(since: "1.36")=+k8s:update=NoModify
	// +k8s:alpha(since: "1.36")=+k8s:update=NoUnset
	ProviderID string `json:"providerID,omitempty" protobuf:"bytes,3,opt,name=providerID"`
	// Unschedulable 控制新 Pod 是否可调度到该节点。默认情况下节点是可调度的。
	// 更多信息：https://kubernetes.io/docs/concepts/nodes/node/#manual-node-administration
	// +optional
	Unschedulable bool `json:"unschedulable,omitempty" protobuf:"varint,4,opt,name=unschedulable"`
	// 如果指定，表示节点的污点。
	// +optional
	// +listType=atomic
	Taints []Taint `json:"taints,omitempty" protobuf:"bytes,5,opt,name=taints"`

	// 已弃用：曾用于指定 DynamicKubeletConfig 特性中节点配置的来源。该特性已被移除。
	// +optional
	ConfigSource *NodeConfigSource `json:"configSource,omitempty" protobuf:"bytes,6,opt,name=configSource"`

	// 已弃用。并非所有 kubelet 都会设置此字段。1.13 之后将移除该字段。
	// 参见：https://issues.k8s.io/61966
	// +optional
	DoNotUseExternalID string `json:"externalID,omitempty" protobuf:"bytes,2,opt,name=externalID"`

	// PodPreemptionPolicy 控制此节点上 Pod 的节点级抢占行为。
	// 这是 alpha 字段，需要启用 InPlacePodVerticalScalingSchedulerPreemption 特性门控。
	// +featureGate=InPlacePodVerticalScalingSchedulerPreemption
	// +optional
	// +k8s:optional
	// +k8s:ifDisabled(InPlacePodVerticalScalingSchedulerPreemption)=+k8s:forbidden
	PodPreemptionPolicy *NodePodPreemptionPolicy `json:"podPreemptionPolicy,omitempty" protobuf:"bytes,8,opt,name=podPreemptionPolicy"`
}

// NodePodPreemptionPolicy 定义管理此节点上 Pod 抢占行为的节点级策略。
type NodePodPreemptionPolicy struct {
	// DisableResizePreemption 列出已请求在此节点上禁用调度器和 Kubelet 对 Pod 原位扩缩容抢占的所有者
	// （例如自动扩缩容器、运维工具、管理员）。
	// 如果此列表非空，则在此节点上禁用由扩缩容引发的抢占。
	// 这是 alpha 字段，需要启用 InPlacePodVerticalScalingSchedulerPreemption 特性门控。
	// +listType=set
	// +k8s:listType=set
	// +optional
	// +k8s:maxItems=20
	// +k8s:optional
	// +k8s:eachVal=+k8s:format=k8s-label-key
	DisableResizePreemption []string `json:"disableResizePreemption,omitempty" protobuf:"bytes,1,rep,name=disableResizePreemption"`
}

// NodeConfigSource 指定节点配置的来源。除 metadata 外，必须且只能有一个子字段非空。
// 此 API 自 1.22 起已弃用。
type NodeConfigSource struct {
	// 为便于理解历史背景，关于下方 kind、apiVersion 和 configMapRef 的弃用标记：
	// 1. kubelet 曾用 kind/apiVersion 将此结构体持久化到磁盘（它们没有 protobuf 标签）。
	// 2. API 曾用 configMapRef 和 proto 标签 1 引用 ConfigMap，
	//    但使用的是通用的 ObjectReference 类型，并不包含我们真正需要的字段。
	// 1.11 之前对 NodeConfigSource 结构体的所有使用/持久化都由 alpha 特性开关控制，
	// 因此这些字段没有需要迁移/处理的持久化数据。

	// +k8s:deprecated=kind
	// +k8s:deprecated=apiVersion
	// +k8s:deprecated=configMapRef,protobuf=1

	// ConfigMap 是对节点 ConfigMap 的引用。
	ConfigMap *ConfigMapNodeConfigSource `json:"configMap,omitempty" protobuf:"bytes,2,opt,name=configMap"`
}

// ConfigMapNodeConfigSource 包含将 ConfigMap 作为节点配置来源进行引用所需的信息。
// 此 API 自 1.22 起已弃用：https://git.k8s.io/enhancements/keps/sig-node/281-dynamic-kubelet-configuration
type ConfigMapNodeConfigSource struct {
	// Namespace 是被引用 ConfigMap 的 metadata.namespace。
	// 此字段在任何情况下都是必填的。
	Namespace string `json:"namespace" protobuf:"bytes,1,opt,name=namespace"`

	// Name 是被引用 ConfigMap 的 metadata.name。
	// 此字段在任何情况下都是必填的。
	Name string `json:"name" protobuf:"bytes,2,opt,name=name"`

	// UID 是被引用 ConfigMap 的 metadata.UID。
	// 此字段在 Node.Spec 中禁止使用，在 Node.Status 中为必填。
	// +optional
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,3,opt,name=uid"`

	// ResourceVersion 是被引用 ConfigMap 的 metadata.ResourceVersion。
	// 此字段在 Node.Spec 中禁止使用，在 Node.Status 中为必填。
	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,4,opt,name=resourceVersion"`

	// KubeletConfigKey 声明被引用 ConfigMap 中哪个键对应 KubeletConfiguration 结构。
	// 此字段在任何情况下都是必填的。
	KubeletConfigKey string `json:"kubeletConfigKey" protobuf:"bytes,5,opt,name=kubeletConfigKey"`
}

// DaemonEndpoint 包含单个守护进程端点的信息。
type DaemonEndpoint struct {
	/*
		在早期版本中，port 标签没有正确地加引号，因此为了向后兼容必须将其大写
		（因为它会回退到变量名 'Port'）。
	*/

	// 给定端点的端口号。
	Port int32 `json:"Port" protobuf:"varint,1,opt,name=Port"`
}

// NodeDaemonEndpoints 列出节点上运行的守护进程所开放的端口。
type NodeDaemonEndpoints struct {
	// Kubelet 监听的端点。
	// +optional
	KubeletEndpoint DaemonEndpoint `json:"kubeletEndpoint,omitempty" protobuf:"bytes,1,opt,name=kubeletEndpoint"`
}

// NodeRuntimeHandlerFeatures 是运行时处理器实现的一组特性。
type NodeRuntimeHandlerFeatures struct {
	// 如果运行时处理器支持 RecursiveReadOnlyMounts，则 RecursiveReadOnlyMounts 设为 true。
	// +optional
	RecursiveReadOnlyMounts *bool `json:"recursiveReadOnlyMounts,omitempty" protobuf:"varint,1,opt,name=recursiveReadOnlyMounts"`
	// 如果运行时处理器支持 UserNamespaces（包括卷），则 UserNamespaces 设为 true。
	// +optional
	UserNamespaces *bool `json:"userNamespaces,omitempty" protobuf:"varint,2,opt,name=userNamespaces"`
}

// NodeRuntimeHandler 是一组运行时处理器信息。
type NodeRuntimeHandler struct {
	// 运行时处理器名称。
	// 默认运行时处理器时为空。
	// +optional
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 支持的特性。
	// +optional
	Features *NodeRuntimeHandlerFeatures `json:"features,omitempty" protobuf:"bytes,2,opt,name=features"`
}

// NodeFeatures 描述 CRI 实现所具备的特性集合。
// 其中包含的特性应仅取决于 CRI 实现，
// 而与运行时处理器无关。
type NodeFeatures struct {
	// 如果运行时支持 SupplementalGroupsPolicy 和 ContainerUser，则 SupplementalGroupsPolicy 设为 true。
	// +optional
	SupplementalGroupsPolicy *bool `json:"supplementalGroupsPolicy,omitempty" protobuf:"varint,1,opt,name=supplementalGroupsPolicy"`
}

// NodeSystemInfo 是一组用于唯一标识节点的 id/uuid。
type NodeSystemInfo struct {
	// 节点上报的 MachineID。在集群中唯一标识机器时
	// 首选此字段。更多信息参见 man(5)
	// machine-id: http://man7.org/linux/man-pages/man5/machine-id.5.html
	MachineID string `json:"machineID" protobuf:"bytes,1,opt,name=machineID"`
	// 节点上报的 SystemUUID。如需唯一标识机器，
	// 首选 MachineID。此字段仅适用于 Red Hat 主机
	// https://access.redhat.com/documentation/en-us/red_hat_subscription_management/1/html/rhsm/uuid
	SystemUUID string `json:"systemUUID" protobuf:"bytes,2,opt,name=systemUUID"`
	// 节点上报的启动 ID。
	BootID string `json:"bootID" protobuf:"bytes,3,opt,name=bootID"`
	// 节点通过 'uname -r' 上报的内核版本（例如 3.16.0-0.bpo.4-amd64）。
	KernelVersion string `json:"kernelVersion" protobuf:"bytes,4,opt,name=kernelVersion"`
	// 节点从 /etc/os-release 上报的操作系统镜像（例如 Debian GNU/Linux 7 (wheezy)）。
	OSImage string `json:"osImage" protobuf:"bytes,5,opt,name=osImage"`
	// 节点通过运行时远程 API 上报的容器运行时版本（例如 containerd://1.4.2）。
	ContainerRuntimeVersion string `json:"containerRuntimeVersion" protobuf:"bytes,6,opt,name=containerRuntimeVersion"`
	// 节点上报的 Kubelet 版本。
	KubeletVersion string `json:"kubeletVersion" protobuf:"bytes,7,opt,name=kubeletVersion"`
	// 已弃用：节点上报的 KubeProxy 版本。
	KubeProxyVersion string `json:"kubeProxyVersion" protobuf:"bytes,8,opt,name=kubeProxyVersion"`
	// 节点上报的操作系统。
	OperatingSystem string `json:"operatingSystem" protobuf:"bytes,9,opt,name=operatingSystem"`
	// 节点上报的架构。
	Architecture string `json:"architecture" protobuf:"bytes,10,opt,name=architecture"`
	// 节点上报的交换内存信息。
	Swap *NodeSwapStatus `json:"swap,omitempty" protobuf:"bytes,11,opt,name=swap"`
	// 节点是否运行在用户命名空间中。
	// +featureGate=KubeletInUserNamespace
	// +optional
	RunningInUserNamespace *bool `json:"runningInUserNamespace,omitempty" protobuf:"varint,12,opt,name=runningInUserNamespace"`
}

// NodeSwapStatus 表示交换内存信息。
type NodeSwapStatus struct {
	// 交换内存总量，单位为字节。
	// +optional
	Capacity *int64 `json:"capacity,omitempty" protobuf:"varint,1,opt,name=capacity"`
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
	Assigned *NodeConfigSource `json:"assigned,omitempty" protobuf:"bytes,1,opt,name=assigned"`
	// Active 记录节点当前实际使用的已存档配置。
	// Active 可能表示 Assigned 配置的当前版本，也可能表示 LastKnownGood 配置的当前版本，
	// 具体取决于尝试使用 Assigned 配置时是否出错。
	// +optional
	Active *NodeConfigSource `json:"active,omitempty" protobuf:"bytes,2,opt,name=active"`
	// LastKnownGood 记录节点在尝试使用 Assigned 配置出错时将回退到的已存档配置。
	// 当节点判定 Assigned 配置稳定且正确时，该配置就会成为 LastKnownGood 配置。
	// 目前的实现方式是：从本地 Assigned 配置记录更新开始，进行 10 分钟的观察期。
	// 若观察期结束时 Assigned 配置仍处于 Active 状态，它便成为 LastKnownGood。
	// 注意，如果 Spec.ConfigSource 被重置为 nil（使用本地默认配置），LastKnownGood 也会立即重置为 nil，
	// 因为本地默认配置始终被认为是正确的。
	// 不要对节点判断配置稳定性和正确性的方式做任何假设，
	// 因为将来这种方式可能会改变或变得可配置。
	// +optional
	LastKnownGood *NodeConfigSource `json:"lastKnownGood,omitempty" protobuf:"bytes,3,opt,name=lastKnownGood"`
	// Error 描述在将 Spec.ConfigSource 同步为 Active 配置过程中出现的任何问题。
	// 例如，将 Spec.ConfigSource 存档为本地 Assigned 记录、存档与 Spec.ConfigSource 关联的配置载荷、
	// 加载或校验 Assigned 配置等步骤都可能出错。
	// 同步配置的不同阶段都可能出现错误。早期错误（例如下载或存档错误）不会导致回滚到 LastKnownGood，
	// 并且可能在 Kubelet 重试时自行恢复。后期错误（例如加载或校验已存档配置）则会回滚到 LastKnownGood。
	// 在后一种情况下，通常可以通过修正 Spec.ConfigSource 中分配的配置来解决问题。
	// 在 Kubelet 日志中搜索错误信息可以找到更多调试线索。
	// Error 是对错误状态的可读描述；机器可以检查 Error 是否为空，
	// 但不应依赖 Error 文本在不同 Kubelet 版本间的稳定性。
	// +optional
	Error string `json:"error,omitempty" protobuf:"bytes,4,opt,name=error"`
}

// NodeStatus 描述节点的当前状态信息。
type NodeStatus struct {
	// Capacity 表示节点的总资源量。
	// 更多信息：https://kubernetes.io/docs/reference/node/node-status/#capacity
	// +optional
	Capacity ResourceList `json:"capacity,omitempty" protobuf:"bytes,1,rep,name=capacity,casttype=ResourceList,castkey=ResourceName"`
	// Allocatable 表示节点上可供调度使用的资源。
	// 默认为 Capacity。
	// +optional
	Allocatable ResourceList `json:"allocatable,omitempty" protobuf:"bytes,2,rep,name=allocatable,casttype=ResourceList,castkey=ResourceName"`
	// NodePhase 是最近观察到的节点生命周期阶段。
	// 更多信息：https://kubernetes.io/docs/concepts/nodes/node/#phase
	// 该字段从未被赋值，现已弃用。
	// +optional
	Phase NodePhase `json:"phase,omitempty" protobuf:"bytes,3,opt,name=phase,casttype=NodePhase"`
	// Conditions 是当前观察到的节点状况列表。
	// 更多信息：https://kubernetes.io/docs/reference/node/node-status/#condition
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []NodeCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,4,rep,name=conditions"`
	// 节点可达的地址列表。
	// 如果可用，则从云服务商查询。
	// 更多信息：https://kubernetes.io/docs/reference/node/node-status/#addresses
	// 注意：该字段声明为可合并，但合并键的唯一性不足，合并时可能导致数据损坏。
	// 调用方应改用全量替换的 patch。示例见 https://pr.k8s.io/79391 （原文示例）。
	// 使用方应假定地址在 Node 的生命周期内可能发生变化。
	// 但也有例外情况无法变化，例如 Pod 在其自身 status 中继承节点的地址，
	// 或 downward API 的使用方（status.hostIP）。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Addresses []NodeAddress `json:"addresses,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,5,rep,name=addresses"`
	// 节点上运行的守护进程的端点。
	// +optional
	DaemonEndpoints NodeDaemonEndpoints `json:"daemonEndpoints,omitempty" protobuf:"bytes,6,opt,name=daemonEndpoints"`
	// 用于唯一标识节点的一组 id/uuid。
	// 更多信息：https://kubernetes.io/docs/reference/node/node-status/#info
	// +optional
	NodeInfo NodeSystemInfo `json:"nodeInfo,omitempty" protobuf:"bytes,7,opt,name=nodeInfo"`
	// 此节点上的容器镜像列表
	// +optional
	// +listType=atomic
	Images []ContainerImage `json:"images,omitempty" protobuf:"bytes,8,rep,name=images"`
	// 节点正在使用（挂载）的可挂载卷列表。
	// +optional
	// +listType=atomic
	VolumesInUse []UniqueVolumeName `json:"volumesInUse,omitempty" protobuf:"bytes,9,rep,name=volumesInUse"`
	// 已附加到节点的卷列表。
	// +optional
	// +listType=atomic
	VolumesAttached []AttachedVolume `json:"volumesAttached,omitempty" protobuf:"bytes,10,rep,name=volumesAttached"`
	// 通过动态 Kubelet 配置功能分配给节点的配置的状态。
	// +optional
	Config *NodeConfigStatus `json:"config,omitempty" protobuf:"bytes,11,opt,name=config"`
	// 可用的运行时处理器。
	// +optional
	// +listType=atomic
	RuntimeHandlers []NodeRuntimeHandler `json:"runtimeHandlers,omitempty" protobuf:"bytes,12,rep,name=runtimeHandlers"`
	// Features 描述 CRI 实现所具备的特性集合。
	// +featureGate=SupplementalGroupsPolicy
	// +optional
	Features *NodeFeatures `json:"features,omitempty" protobuf:"bytes,13,rep,name=features"`
	// DeclaredFeatures 表示节点声明的、与特性开关相关的特性。
	// +featureGate=NodeDeclaredFeatures
	// +optional
	// +listType=atomic
	DeclaredFeatures []string `json:"declaredFeatures,omitempty" protobuf:"bytes,14,rep,name=declaredFeatures"`
}

// UniqueVolumeName 是卷的唯一名称。
type UniqueVolumeName string

// AttachedVolume 描述附加到节点的卷
type AttachedVolume struct {
	// 附加卷的名称
	Name UniqueVolumeName `json:"name" protobuf:"bytes,1,rep,name=name"`

	// DevicePath 表示该卷应可用的设备路径
	DevicePath string `json:"devicePath" protobuf:"bytes,2,rep,name=devicePath"`
}

// AvoidPods 描述应避开此节点的 Pod。它是键为 scheduler.alpha.kubernetes.io/preferAvoidPods 的
// Node 注解的值，最终将成为 NodeStatus 的一个字段。
type AvoidPods struct {
	// 应避开此节点的 Pod 的签名列表，数量有上限，按时间戳从旧到新排序。
	// 切片大小未作规定。
	// +optional
	// +listType=atomic
	PreferAvoidPods []PreferAvoidPodsEntry `json:"preferAvoidPods,omitempty" protobuf:"bytes,1,rep,name=preferAvoidPods"`
}

// 描述应避开此节点的一类 Pod。
type PreferAvoidPodsEntry struct {
	// Pod 的类别。
	PodSignature PodSignature `json:"podSignature" protobuf:"bytes,1,opt,name=podSignature"`
	// 此条目加入列表的时间。
	// +optional
	EvictionTime metav1.Time `json:"evictionTime,omitempty" protobuf:"bytes,2,opt,name=evictionTime"`
	// 此条目加入列表的（简要）原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`
	// 可读的消息，说明此条目加入列表的原因。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`
}

// 描述应避开此节点的 Pod 类别。
// 应当只设置其中一个字段。
type PodSignature struct {
	// 指向控制器的引用，该控制器的 Pod 应避开此节点。
	// +optional
	PodController *metav1.OwnerReference `json:"podController,omitempty" protobuf:"bytes,1,opt,name=podController"`
}

// 描述一个容器镜像
type ContainerImage struct {
	// 此镜像的已知名称。
	// 例如 ["kubernetes.example/hyperkube:v1.0.7", "cloud-vendor.registry.example/cloud-vendor/hyperkube:v1.0.7"]
	// +optional
	// +listType=atomic
	Names []string `json:"names" protobuf:"bytes,1,rep,name=names"`
	// 镜像大小，单位为字节。
	// +optional
	SizeBytes int64 `json:"sizeBytes,omitempty" protobuf:"varint,2,opt,name=sizeBytes"`
}

// NodePhase 是节点的生命周期阶段。
// +enum
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

// NodeConditionType 是节点状况的类型。
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
	// NodePIDPressure 表示 kubelet 因可用 PID 不足而处于压力状态。
	NodePIDPressure NodeConditionType = "PIDPressure"
	// NodeNetworkUnavailable 表示节点的网络未正确配置。
	NodeNetworkUnavailable NodeConditionType = "NetworkUnavailable"
	// NodeGracefulNodeShutdownInProgress 报告此节点是否正在执行优雅关机。
	//
	// 管理员负责设置和清除此状况。
	NodeGracefulNodeShutdownInProgress NodeConditionType = "GracefulNodeShutdownInProgress"
	// NodeDrainInProgress 报告此节点正在被排空，
	// 具体依据管理员对排空的定义。
	// 通常这意味着移除一些 Pod、卷和网络。
	//
	// 管理员负责设置和清除此状况。
	NodeDrainInProgress NodeConditionType = "DrainInProgress"
	// NodeDrained 报告此节点已达到管理员选择的排空标准。
	//
	// 管理员负责设置和清除此状况。
	NodeDrained NodeConditionType = "Drained"
	// NodeMaintenancePlanned 报告此节点预计在未来将发生变更。
	// 如果此变更会影响节点使用者，管理员应先排空该节点。
	//
	// 管理员可将节点维护状况用于硬件或软件升级、
	// 修复、下线或调试等场景。
	//
	// 管理员负责设置和清除此状况。
	NodeMaintenancePlanned NodeConditionType = "MaintenancePlanned"
	// NodeMaintenanceInProgress 报告此节点正在执行维护。
	//
	// 管理员决定该变更是否会影响节点使用者并需要排空节点。
	// 例如，建议在 Kubernetes 升级前排空节点。
	// 相反，内核热补丁可能不需要排空节点，但通告维护正在进行
	// 仍然是有用的。
	//
	// 管理员负责设置和清除此状况。
	NodeMaintenanceInProgress NodeConditionType = "MaintenanceInProgress"
)

// NodeCondition 包含节点的状况信息。
type NodeCondition struct {
	// 节点状况的类型。
	Type NodeConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=NodeConditionType"`
	// 状况的状态，取值为 True、False 或 Unknown。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 最近一次收到该状况更新的时间。
	// +optional
	LastHeartbeatTime metav1.Time `json:"lastHeartbeatTime,omitempty" protobuf:"bytes,3,opt,name=lastHeartbeatTime"`
	// 状况最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 状况最近一次转变的（简要）原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 可读的消息，说明最近一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// NodeAddressType 是节点地址的类型。
type NodeAddressType string

// 以下是节点的内置地址类型。云服务商可以设置此处未列出的类型。
const (
	// NodeHostName 标识节点的名称。虽然可以认为每个节点都有此类型的 NodeAddress，
	// 但其确切的语法和语义并未定义，在不同集群之间也不一致。
	NodeHostName NodeAddressType = "Hostname"

	// NodeInternalIP 标识分配到节点某个网络接口上的 IP 地址。
	// 每个节点应至少有一个此类型的地址。
	//
	// 内部 IP 通常应能从其他所有节点访问，但可能对集群外部的主机不可见。
	// 默认假定 kube-apiserver 可以访问节点内部 IP，不过也可以配置成
	// kube-apiserver 无法访问这些 IP 的集群。
	//
	// NodeInternalIP 是节点 IP 的默认类型，并不表示该 IP 仅可在内部访问。
	// 如果节点有多个内部 IP，额外的 IP 不被赋予特定语义。
	NodeInternalIP NodeAddressType = "InternalIP"

	// NodeExternalIP 标识在某种程度上比内部 IP 更适合从集群外部使用的 IP 地址，
	// 但并未定义具体语义。它可能是全局可路由的 IP，但并非必须如此。
	//
	// 外部 IP 可以像 NodeInternalIP 一样直接分配到节点的某个接口上；
	// 也可以将发往外部 IP 的数据包 NAT 到节点的内部 IP，而不是直接投递
	// （这会使该 IP 在节点间流量上的效率低于 NodeInternalIP）。
	NodeExternalIP NodeAddressType = "ExternalIP"

	// NodeInternalDNS 标识解析为具有 NodeInternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeInternalIP 地址。
	NodeInternalDNS NodeAddressType = "InternalDNS"

	// NodeExternalDNS 标识解析为具有 NodeExternalIP 特性的 IP 地址的 DNS 名称。
	// 其解析出的 IP 可能是、也可能不是列出的 NodeExternalIP 地址。
	NodeExternalDNS NodeAddressType = "ExternalDNS"
)

// NodeAddress 包含节点的地址信息。
type NodeAddress struct {
	// 节点地址类型，取值为 Hostname、ExternalIP 或 InternalIP。
	Type NodeAddressType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=NodeAddressType"`
	// 节点地址。
	Address string `json:"address" protobuf:"bytes,2,opt,name=address"`
}

// ResourceName 是 ResourceList 中标识各种资源的名称。
type ResourceName string

// 资源名称不得超过 63 个字符，由大小写字母数字组成，
// 其中 -、_ 和 . 可以出现在任意位置，但不能是首字符或末字符。
// 默认约定与注解一致：使用小写名称，并以连字符而非驼峰形式分隔复合词。
// 完全限定的资源类型名由 DNS 风格的子域、斜杠 `/` 和名称构成。
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
	// 默认命名空间前缀。
	ResourceDefaultNamespacePrefix = "kubernetes.io/"
	// 大页资源的名称前缀（alpha）。
	ResourceHugePagesPrefix = "hugepages-"
	// 存储资源限制的名称前缀
	ResourceAttachableVolumesPrefix = "attachable-volumes-"
)

// ResourceList 是一组（资源名称，数量）对。
type ResourceList map[ResourceName]resource.Quantity

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Node 是 Kubernetes 中的工作节点。
// 每个节点在缓存（即 etcd）中都有唯一的标识符。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/proxy"
type Node struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义节点的行为。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec NodeSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// 最近观察到的节点状态。
	// 由系统填充。
	// 只读。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status NodeStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// NodeList 是已向 master 注册的所有节点的完整列表。
type NodeList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 节点列表
	Items []Node `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// FinalizerName 是在命名空间生命周期中标识终结器的名称。
type FinalizerName string

// 以下是 Kubernetes 内部的终结器值，除非在此处或 metav1 中定义，否则必须使用限定名称。
const (
	// FinalizerKubernetes 是 Kubernetes 内部使用的终结器。
	FinalizerKubernetes FinalizerName = "kubernetes"
)

// NamespaceSpec 描述 Namespace 的属性。
type NamespaceSpec struct {
	// Finalizers 是一个不透明的值列表，必须为空才能将对象从存储中永久删除。
	// 更多信息：https://kubernetes.io/docs/tasks/administer-cluster/namespaces/
	// +optional
	// +listType=atomic
	Finalizers []FinalizerName `json:"finalizers,omitempty" protobuf:"bytes,1,rep,name=finalizers,casttype=FinalizerName"`
}

// NamespaceStatus 描述 Namespace 的当前状态信息。
type NamespaceStatus struct {
	// Phase 是命名空间当前的生命周期阶段。
	// 更多信息：https://kubernetes.io/docs/tasks/administer-cluster/namespaces/
	// +optional
	Phase NamespacePhase `json:"phase,omitempty" protobuf:"bytes,1,opt,name=phase,casttype=NamespacePhase"`

	// 表示对命名空间当前状态的最新可用观察结果。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []NamespaceCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,rep,name=conditions"`
}

// NamespacePhase 是命名空间的生命周期阶段。
// +enum
type NamespacePhase string

// 以下是命名空间的有效阶段。
const (
	// NamespaceActive 表示命名空间可在系统中使用
	NamespaceActive NamespacePhase = "Active"
	// NamespaceTerminating 表示命名空间正在优雅终止
	NamespaceTerminating NamespacePhase = "Terminating"
)

const (
	// 当因命名空间正在终止而禁止变更时，NamespaceTerminatingCause 会作为 defaults.cause 项返回。
	NamespaceTerminatingCause metav1.CauseType = "NamespaceTerminating"
)

// NamespaceConditionType 是命名空间状况的类型。
type NamespaceConditionType string

// 以下是命名空间的内置状况。
const (
	// NamespaceDeletionDiscoveryFailure 包含命名空间删除器在资源发现阶段的错误信息。
	NamespaceDeletionDiscoveryFailure NamespaceConditionType = "NamespaceDeletionDiscoveryFailure"
	// NamespaceDeletionContentFailure 包含命名空间删除器在删除资源阶段的错误信息。
	NamespaceDeletionContentFailure NamespaceConditionType = "NamespaceDeletionContentFailure"
	// NamespaceDeletionGVParsingFailure 包含命名空间删除器解析旧类型 GV 时的错误信息。
	NamespaceDeletionGVParsingFailure NamespaceConditionType = "NamespaceDeletionGroupVersionParsingFailure"
	// NamespaceContentRemaining 包含命名空间中剩余资源的信息。
	NamespaceContentRemaining NamespaceConditionType = "NamespaceContentRemaining"
	// NamespaceFinalizersRemaining 包含命名空间中剩余资源上终结器的信息。
	NamespaceFinalizersRemaining NamespaceConditionType = "NamespaceFinalizersRemaining"
)

// NamespaceCondition 包含命名空间状态的详细信息。
type NamespaceCondition struct {
	// 命名空间控制器状况的类型。
	Type NamespaceConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=NamespaceConditionType"`
	// 状况的状态，取值为 True、False 或 Unknown。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// 状况最近一次从一种状态转变为另一种状态的时间。
	// +optional
	LastTransitionTime metav1.Time `json:"lastTransitionTime,omitempty" protobuf:"bytes,4,opt,name=lastTransitionTime"`
	// 状况最近一次转变的唯一、单词、驼峰格式的原因。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,5,opt,name=reason"`
	// 可读的消息，说明最近一次转变的详情。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,6,opt,name=message"`
}

// +genclient
// +genclient:nonNamespaced
// +genclient:skipVerbs=deleteCollection
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Namespace 为名称提供了一个作用域。
// 使用多个命名空间是可选的。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/finalize"
type Namespace struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义 Namespace 的行为。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec NamespaceSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 描述 Namespace 的当前状态。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status NamespaceStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// NamespaceList 是 Namespace 的列表。
type NamespaceList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是列表中的 Namespace 对象列表。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	Items []Namespace `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Binding 将一个对象绑定到另一个对象；例如，Pod 由调度器绑定到节点。
type Binding struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 要绑定到标准对象的目标对象。
	Target ObjectReference `json:"target" protobuf:"bytes,2,opt,name=target"`
}

// 在执行操作（更新、删除等）之前必须满足的前置条件。
// +k8s:openapi-gen=false
type Preconditions struct {
	// 指定目标 UID。
	// +optional
	UID *types.UID `json:"uid,omitempty" protobuf:"bytes,1,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
}

const (
	// LogStreamStdout 表示 stdout 的流类型。
	LogStreamStdout = "Stdout"
	// LogStreamStderr 表示 stderr 的流类型。
	LogStreamStderr = "Stderr"
	// LogStreamAll 表示 stdout 和 stderr 的合并流。
	LogStreamAll = "All"
)

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodLogOptions 是 Pod 日志 REST 调用的查询选项。
type PodLogOptions struct {
	metav1.TypeMeta `json:""`

	// 要流式获取日志的容器。如果 Pod 中只有一个容器，则默认使用该容器。
	// +optional
	Container string `json:"container,omitempty" protobuf:"bytes,1,opt,name=container"`
	// 跟随 Pod 的日志流。默认为 false。
	// +optional
	Follow bool `json:"follow,omitempty" protobuf:"varint,2,opt,name=follow"`
	// 返回上一个已终止容器的日志。默认为 false。
	// +optional
	Previous bool `json:"previous,omitempty" protobuf:"varint,3,opt,name=previous"`
	// 相对于当前时间往前推的秒数，从该时间点开始显示日志。如果此值
	// 早于 Pod 启动时间，则只返回 Pod 启动以来的日志。
	// 如果此值在未来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中之一。
	// +optional
	SinceSeconds *int64 `json:"sinceSeconds,omitempty" protobuf:"varint,4,opt,name=sinceSeconds"`
	// 从该 RFC3339 时间戳开始显示日志。如果此值
	// 早于 Pod 启动时间，则只返回 Pod 启动以来的日志。
	// 如果此值在未来，则不返回任何日志。
	// sinceSeconds 和 sinceTime 只能指定其中之一。
	// +optional
	SinceTime *metav1.Time `json:"sinceTime,omitempty" protobuf:"bytes,5,opt,name=sinceTime"`
	// 若为 true，则在每行日志输出的开头添加 RFC3339 或 RFC3339Nano 时间戳。
	// 默认为 false。
	// +optional
	Timestamps bool `json:"timestamps,omitempty" protobuf:"varint,6,opt,name=timestamps"`
	// 若设置，表示从日志末尾开始显示的行数。如果未指定，
	// 则从容器创建起，或从 sinceSeconds / sinceTime 指定的时间点起显示日志。
	// 注意，指定 "TailLines" 时，"Stream" 只能设为 nil 或 "All"。
	// +optional
	TailLines *int64 `json:"tailLines,omitempty" protobuf:"varint,7,opt,name=tailLines"`
	// 若设置，表示在终止日志输出前从服务端读取的字节数。
	// 最后一行日志可能显示不完整，返回的数据也可能略多或略少于指定的限制。
	// +optional
	LimitBytes *int64 `json:"limitBytes,omitempty" protobuf:"varint,8,opt,name=limitBytes"`

	// insecureSkipTLSVerifyBackend 表示 apiserver 不应校验其连接的后端服务的证书有效性。
	// 这会使 apiserver 与后端之间的 HTTPS 连接变为不安全连接。
	// 这意味着 apiserver 无法确认收到的日志数据确实来自真正的 kubelet。
	// 如果 kubelet 配置了校验 apiserver 的 TLS 凭据，这并不意味着
	// 到真正 kubelet 的连接容易受到中间人攻击（例如攻击者无法截获
	// 来自真正 kubelet 的实际日志数据）。
	// +optional
	InsecureSkipTLSVerifyBackend bool `json:"insecureSkipTLSVerifyBackend,omitempty" protobuf:"varint,9,opt,name=insecureSkipTLSVerifyBackend"`

	// 指定向客户端返回哪个容器日志流。
	// 可接受的值为 "All"、"Stdout" 和 "Stderr"。若未指定，则使用 "All"，
	// 并将 stdout 和 stderr 交错返回。
	// 注意，指定 "TailLines" 时，"Stream" 只能设为 nil 或 "All"。
	// +featureGate=PodLogsQuerySplitStreams
	// +optional
	Stream *string `json:"stream,omitempty" protobuf:"varint,10,opt,name=stream"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.1

// PodAttachOptions 是 Pod 远程 attach 调用的查询选项。
// ---
// TODO: 与下方的 PodExecOptions 合并，统一处理 stdin、stdout 等
// 另外在发布 V2 时，应导出一个类似 "StreamOptions" 的类型，包含 Stdin、Stdout、Stderr 和 TTY
type PodAttachOptions struct {
	metav1.TypeMeta `json:""`

	// Stdin 为 true 时，为此调用重定向 Pod 的标准输入流。
	// 默认为 false。
	// +optional
	Stdin bool `json:"stdin,omitempty" protobuf:"varint,1,opt,name=stdin"`

	// Stdout 为 true 表示 attach 调用需要重定向 stdout。
	// 默认为 true。
	// +optional
	Stdout bool `json:"stdout,omitempty" protobuf:"varint,2,opt,name=stdout"`

	// Stderr 为 true 表示 attach 调用需要重定向 stderr。
	// 默认为 true。
	// +optional
	Stderr bool `json:"stderr,omitempty" protobuf:"varint,3,opt,name=stderr"`

	// TTY 为 true 表示为 attach 调用分配一个 tty。
	// 此设置会传递给容器运行时，
	// 由容器运行时在工作节点上分配 tty。
	// 默认为 false。
	// +optional
	TTY bool `json:"tty,omitempty" protobuf:"varint,4,opt,name=tty"`

	// 执行命令所在的容器。
	// 如果 Pod 中只有一个容器，则默认使用该容器。
	// +optional
	Container string `json:"container,omitempty" protobuf:"bytes,5,opt,name=container"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodExecOptions 是 Pod 远程 exec 调用的查询选项。
// ---
// TODO: 此类型与上方的 PodAttachOptions 大体相同，请确保两者保持同步，并考虑合并
// 另外在发布 V2 时，应导出一个类似 "StreamOptions" 的类型，包含 Stdin、Stdout、Stderr 和 TTY
type PodExecOptions struct {
	metav1.TypeMeta `json:""`

	// 为此调用重定向 Pod 的标准输入流。
	// 默认为 false。
	// +optional
	Stdin bool `json:"stdin,omitempty" protobuf:"varint,1,opt,name=stdin"`

	// 为此调用重定向 Pod 的标准输出流。
	// +optional
	Stdout bool `json:"stdout,omitempty" protobuf:"varint,2,opt,name=stdout"`

	// 为此调用重定向 Pod 的标准错误流。
	// +optional
	Stderr bool `json:"stderr,omitempty" protobuf:"varint,3,opt,name=stderr"`

	// TTY 为 true 表示为 exec 调用分配一个 tty。
	// 默认为 false。
	// +optional
	TTY bool `json:"tty,omitempty" protobuf:"varint,4,opt,name=tty"`

	// 执行命令所在的容器。
	// 如果 Pod 中只有一个容器，则默认使用该容器。
	// +optional
	Container string `json:"container,omitempty" protobuf:"bytes,5,opt,name=container"`

	// Command 是要远程执行的命令，为 argv 数组，不在 shell 中执行。
	// +listType=atomic
	Command []string `json:"command" protobuf:"bytes,6,rep,name=command"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.6

// PodPortForwardOptions 是使用 WebSocket 时 Pod 端口转发调用的查询选项。
// `port` 查询参数必须指定要转发的端口，
// 多个端口用逗号分隔。
// 通过 SPDY 进行的端口转发不使用这些选项，而是要求将端口
// 作为请求的一部分放在 `port` 头中传递。
type PodPortForwardOptions struct {
	metav1.TypeMeta `json:""`

	// 要转发的端口列表
	// 使用 WebSocket 时必填
	// +optional
	// +listType=atomic
	Ports []int32 `json:"ports,omitempty" protobuf:"varint,1,rep,name=ports"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// PodProxyOptions 是 Pod 代理调用的查询选项。
type PodProxyOptions struct {
	metav1.TypeMeta `json:""`

	// Path 是本次到 Pod 的代理请求所使用的 URL 路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// NodeProxyOptions 是 Node 代理调用的查询选项。
type NodeProxyOptions struct {
	metav1.TypeMeta `json:""`

	// Path 是本次到 Node 的代理请求所使用的 URL 路径。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
}

// +k8s:conversion-gen:explicit-from=net/url.Values
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// ServiceProxyOptions 是 Service 代理调用的查询选项。
type ServiceProxyOptions struct {
	metav1.TypeMeta `json:""`

	// Path 是 URL 中包含服务端点、后缀
	// 以及本次到服务的代理请求所用参数的部分。
	// 例如，完整的请求 URL 是
	// http://localhost/api/v1/namespaces/kube-system/services/elasticsearch-logging/_search?q=user:kimchy.
	// 此时 Path 为 _search?q=user:kimchy。
	// +optional
	Path string `json:"path,omitempty" protobuf:"bytes,1,opt,name=path"`
}

// ObjectReference 包含足够的信息，可用于检查或修改被引用的对象。
// ---
// 不建议在新代码中使用此类型，因为将其嵌入 API 时难以描述其用法。
//  1. 字段会被忽略。它包含许多通常不被处理的字段。例如，ResourceVersion 和 FieldPath 在实际使用中很少有效。
//  2. 用法提示无效。无法针对具体用法给出专门帮助。在大多数嵌入场景中，都存在特定
//     限制，例如"只能引用类型 A 和 B"、"不处理 UID"或"名称必须受限"。
//     这些限制在嵌入时无法得到充分说明。
//  3. 校验不一致。由于用法各异，校验规则也因用法而异，用户难以预测结果。
//  4. 字段既不精确又过于精确。Kind 并非到 URL 的精确映射，这可能在解释时产生歧义，
//     并需要借助 REST 映射。多数情况下，依赖的是 group,resource 二元组，
//     而实际结构体的版本无关紧要。
//  5. 难以修改。由于此类型被嵌入多处，对它的修改会影响大量 schema。
//     不要让新的 API 嵌入自己无法控制的、定义不明确的 API 类型。
//
// 请勿使用此类型，而应创建一个本地定义并使用的类型，使其专注于你的引用需求。
// 例如，准入注册中的 ServiceReferences：https://github.com/kubernetes/api/blob/release-1.17/admissionregistration/v1/types.go#L533 。
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +structType=atomic
type ObjectReference struct {
	// 被引用对象的 Kind。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	Kind string `json:"kind,omitempty" protobuf:"bytes,1,opt,name=kind"`
	// 被引用对象的命名空间。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/namespaces/
	// +optional
	Namespace string `json:"namespace,omitempty" protobuf:"bytes,2,opt,name=namespace"`
	// 被引用对象的名称。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	// +optional
	Name string `json:"name,omitempty" protobuf:"bytes,3,opt,name=name"`
	// 被引用对象的 UID。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#uids
	// +optional
	UID types.UID `json:"uid,omitempty" protobuf:"bytes,4,opt,name=uid,casttype=k8s.io/apimachinery/pkg/types.UID"`
	// 被引用对象的 API 版本。
	// +optional
	APIVersion string `json:"apiVersion,omitempty" protobuf:"bytes,5,opt,name=apiVersion"`
	// 此引用指向的具体 resourceVersion（如果有）。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#concurrency-control-and-consistency
	// +optional
	ResourceVersion string `json:"resourceVersion,omitempty" protobuf:"bytes,6,opt,name=resourceVersion"`

	// 如果引用的是对象的一部分而非整个对象，此字符串应包含一个有效的 JSON/Go 字段访问语句，
	// 例如 desiredState.manifest.containers[2]。
	// 例如，如果对象引用指向 Pod 中的某个容器，则取值形如：
	// "spec.containers{name}"（其中 "name" 指触发事件的容器名称），
	// 如果未指定容器名称，则为 "spec.containers[2]"（此 Pod 中索引为 2 的容器）。
	// 选择这种语法只是为了提供一种明确的方式来引用对象的一部分。
	// TODO: 此设计尚未最终确定，将来可能会改动。
	// +optional
	FieldPath string `json:"fieldPath,omitempty" protobuf:"bytes,7,opt,name=fieldPath"`
}

// LocalObjectReference 包含足够的信息，可用于在同一命名空间内定位被引用的对象。
// ---
// 不建议在新代码中使用此类型，因为将其嵌入 API 时难以描述其用法。
//  1. 用法提示无效。无法针对具体用法给出专门帮助。在大多数嵌入场景中，都存在特定
//     限制，例如"只能引用类型 A 和 B"、"不处理 UID"或"名称必须受限"。
//     这些限制在嵌入时无法得到充分说明。
//  2. 校验不一致。由于用法各异，校验规则也因用法而异，用户难以预测结果。
//  3. 难以修改。由于此类型被嵌入多处，对它的修改会影响大量 schema。
//     不要让新的 API 嵌入自己无法控制的、定义不明确的 API 类型。
//
// 请勿使用此类型，而应创建一个本地定义并使用的类型，使其专注于你的引用需求。
// 例如，准入注册中的 ServiceReferences：https://github.com/kubernetes/api/blob/release-1.17/admissionregistration/v1/types.go#L533 。
// +structType=atomic
type LocalObjectReference struct {
	// 被引用对象的名称。
	// 此字段实际上是必填的，但出于向后兼容考虑允许为空。
	// 此类型中该字段为空的实例几乎肯定是错误的。
	// 更多信息：https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#names
	// +optional
	// +default=""
	// +kubebuilder:default=""
	// TODO: 当 controller-gen 不再需要时，移除 `kubebuilder:default` https://github.com/kubernetes-sigs/kubebuilder/issues/3896.
	Name string `json:"name,omitempty" protobuf:"bytes,1,opt,name=name"`
}

// TypedLocalObjectReference 包含足够的信息，可用于在同一命名空间内定位带类型的被引用对象。
// ---
// 不建议在新代码中使用此类型，因为将其嵌入 API 时难以描述其用法。
//  1. 用法提示无效。无法针对具体用法给出专门帮助。在大多数嵌入场景中，都存在特定
//     限制，例如"只能引用类型 A 和 B"、"不处理 UID"或"名称必须受限"。
//     这些限制在嵌入时无法得到充分说明。
//  2. 校验不一致。由于用法各异，校验规则也因用法而异，用户难以预测结果。
//  3. 字段既不精确又过于精确。Kind 并非到 URL 的精确映射，这可能在解释时产生歧义，
//     并需要借助 REST 映射。多数情况下，依赖的是 group,resource 二元组，
//     而实际结构体的版本无关紧要。
//  4. 难以修改。由于此类型被嵌入多处，对它的修改会影响大量 schema。
//     不要让新的 API 嵌入自己无法控制的、定义不明确的 API 类型。
//
// 请勿使用此类型，而应创建一个本地定义并使用的类型，使其专注于你的引用需求。
// 例如，准入注册中的 ServiceReferences：https://github.com/kubernetes/api/blob/release-1.17/admissionregistration/v1/types.go#L533 。
// +structType=atomic
type TypedLocalObjectReference struct {
	// APIGroup 是被引用资源所属的组。
	// 如果未指定 APIGroup，则指定的 Kind 必须属于核心 API 组。
	// 对于其他第三方类型，APIGroup 是必填的。
	// +optional
	APIGroup *string `json:"apiGroup" protobuf:"bytes,1,opt,name=apiGroup"`
	// Kind 是被引用资源的类型
	Kind string `json:"kind" protobuf:"bytes,2,opt,name=kind"`
	// Name 是被引用资源的名称
	Name string `json:"name" protobuf:"bytes,3,opt,name=name"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// SerializedReference 是对序列化对象的引用。
type SerializedReference struct {
	metav1.TypeMeta `json:""`
	// 对系统中某个对象的引用。
	// +optional
	Reference ObjectReference `json:"reference,omitempty" protobuf:"bytes,1,opt,name=reference"`
}

// EventSource 包含事件的相关信息。
type EventSource struct {
	// 产生事件的组件。
	// +optional
	Component string `json:"component,omitempty" protobuf:"bytes,1,opt,name=component"`
	// 产生事件的节点名称。
	// +optional
	Host string `json:"host,omitempty" protobuf:"bytes,2,opt,name=host"`
}

// 事件类型的有效值（将来可能新增类型）
const (
	// 仅提供信息，不会导致任何问题
	EventTypeNormal string = "Normal"
	// 此类事件用于警告可能出问题
	EventTypeWarning string = "Warning"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Event 是对集群中某处发生的事件的报告。事件
// 保留时间有限，且触发原因和消息可能随时间演变。
// 事件的使用方不应依赖具有某个 Reason 的事件时序来推断一致的底层触发原因，
// 也不应依赖具有该 Reason 的事件会持续存在。
// 事件应被视为仅供参考、尽力而为的补充数据。
type Event struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	metav1.ObjectMeta `json:"metadata" protobuf:"bytes,1,opt,name=metadata"`

	// 此事件所涉及的对象。
	InvolvedObject ObjectReference `json:"involvedObject" protobuf:"bytes,2,opt,name=involvedObject"`

	// 应为简短、机器可理解的字符串，说明对象
	// 转变为当前状态的原因。
	// TODO: 提供确切的格式规范。
	// +optional
	Reason string `json:"reason,omitempty" protobuf:"bytes,3,opt,name=reason"`

	// 对此操作状态的可读描述。
	// TODO: 确定最大长度。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,4,opt,name=message"`

	// 上报此事件的组件。应为简短、机器可理解的字符串。
	// +optional
	Source EventSource `json:"source,omitempty" protobuf:"bytes,5,opt,name=source"`

	// 事件首次被记录的时间。（服务端接收时间见 TypeMeta。）
	// +optional
	FirstTimestamp metav1.Time `json:"firstTimestamp,omitempty" protobuf:"bytes,6,opt,name=firstTimestamp"`

	// 此事件最近一次发生被记录的时间。
	// +optional
	LastTimestamp metav1.Time `json:"lastTimestamp,omitempty" protobuf:"bytes,7,opt,name=lastTimestamp"`

	// 此事件发生的次数。
	// +optional
	Count int32 `json:"count,omitempty" protobuf:"varint,8,opt,name=count"`

	// 事件类型（Normal、Warning），将来可能新增类型
	// +optional
	Type string `json:"type,omitempty" protobuf:"bytes,9,opt,name=type"`

	// 首次观察到该事件的时间。
	// +optional
	EventTime metav1.MicroTime `json:"eventTime,omitempty" protobuf:"bytes,10,opt,name=eventTime"`

	// 此事件所属事件序列的数据；若为单次事件则为 nil。
	// +optional
	Series *EventSeries `json:"series,omitempty" protobuf:"bytes,11,opt,name=series"`

	// 针对 Regarding 对象已执行/失败的动作。
	// +optional
	Action string `json:"action,omitempty" protobuf:"bytes,12,opt,name=action"`

	// 可选的次要对象，用于更复杂的动作。
	// +optional
	Related *ObjectReference `json:"related,omitempty" protobuf:"bytes,13,opt,name=related"`

	// 发出此事件的控制器名称，例如 `kubernetes.io/kubelet`。
	// +optional
	ReportingController string `json:"reportingComponent" protobuf:"bytes,14,opt,name=reportingComponent"`

	// 控制器实例的 ID，例如 `kubelet-xyzf`。
	// +optional
	ReportingInstance string `json:"reportingInstance" protobuf:"bytes,15,opt,name=reportingInstance"`
}

// EventSeries 包含事件序列的信息，即某件事在一段时间内持续发生。
type EventSeries struct {
	// 截至上次心跳时间，此序列中的发生次数
	Count int32 `json:"count,omitempty" protobuf:"varint,1,name=count"`
	// 观察到的最近一次发生的时间
	LastObservedTime metav1.MicroTime `json:"lastObservedTime,omitempty" protobuf:"bytes,2,name=lastObservedTime"`

	// +k8s:deprecated=state,protobuf=3
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// EventList 是事件列表。
type EventList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 事件列表
	Items []Event `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// List 保存一组对象，服务端可能并不知晓这些对象的类型。
type List metav1.List

// LimitType 是受限制的对象类型。可以是 Pod、Container、PersistentVolumeClaim，
// 或完全限定的资源名称。
type LimitType string

const (
	// 应用于命名空间中所有 Pod 的限制
	LimitTypePod LimitType = "Pod"
	// 应用于命名空间中所有容器的限制
	LimitTypeContainer LimitType = "Container"
	// 应用于命名空间中所有持久卷声明的限制
	LimitTypePersistentVolumeClaim LimitType = "PersistentVolumeClaim"
)

// LimitRangeItem 为匹配该 kind 的任意资源定义最小/最大使用限制。
type LimitRangeItem struct {
	// 此限制适用的资源类型。
	Type LimitType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=LimitType"`
	// 按资源名称对此类资源设置的最大用量约束。
	// +optional
	Max ResourceList `json:"max,omitempty" protobuf:"bytes,2,rep,name=max,casttype=ResourceList,castkey=ResourceName"`
	// 按资源名称对此类资源设置的最小用量约束。
	// +optional
	Min ResourceList `json:"min,omitempty" protobuf:"bytes,3,rep,name=min,casttype=ResourceList,castkey=ResourceName"`
	// 如果省略资源限制，则按资源名称给出默认的资源需求上限值。
	// +optional
	Default ResourceList `json:"default,omitempty" protobuf:"bytes,4,rep,name=default,casttype=ResourceList,castkey=ResourceName"`
	// DefaultRequest 是在省略资源请求时，按资源名称给出的默认资源需求请求值。
	// +optional
	DefaultRequest ResourceList `json:"defaultRequest,omitempty" protobuf:"bytes,5,rep,name=defaultRequest,casttype=ResourceList,castkey=ResourceName"`
	// MaxLimitRequestRatio 若指定，则命名资源必须同时具有非零的 request 和 limit，且 limit 除以 request 的结果不超过枚举值；这表示该资源的最大突发量。
	// +optional
	MaxLimitRequestRatio ResourceList `json:"maxLimitRequestRatio,omitempty" protobuf:"bytes,6,rep,name=maxLimitRequestRatio,casttype=ResourceList,castkey=ResourceName"`
}

// LimitRangeSpec 为匹配 kind 的资源定义最小/最大使用限制。
type LimitRangeSpec struct {
	// Limits 是需要强制执行的 LimitRangeItem 对象列表。
	// +listType=atomic
	Limits []LimitRangeItem `json:"limits" protobuf:"bytes,1,rep,name=limits"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// LimitRange 为命名空间中的每种资源设置资源使用限制。
type LimitRange struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义所施加的限制。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec LimitRangeSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// LimitRangeList 是 LimitRange 对象的列表。
type LimitRangeList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 LimitRange 对象的列表。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/manage-resources-containers/
	Items []LimitRange `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// 以下常量标识 Kubernetes 对象类型的资源。
const (
	// Pod 数量。
	ResourcePods ResourceName = "pods"
	// Service 数量。
	ResourceServices ResourceName = "services"
	// ReplicationController 数量。
	ResourceReplicationControllers ResourceName = "replicationcontrollers"
	// ResourceQuota 数量。
	ResourceQuotas ResourceName = "resourcequotas"
	// Secret 数量。
	ResourceSecrets ResourceName = "secrets"
	// ConfigMap 数量。
	ResourceConfigMaps ResourceName = "configmaps"
	// PersistentVolumeClaim 数量。
	ResourcePersistentVolumeClaims ResourceName = "persistentvolumeclaims"
	// Service NodePort 数量。
	ResourceServicesNodePorts ResourceName = "services.nodeports"
	// Service LoadBalancer 数量。
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
	// 按指定 DeviceClass 通过隐式扩展资源名请求的 resource.k8s.io 设备数量。
	ResourceImplicitExtendedClaimsPerClass string = "requests.deviceclass.resource.kubernetes.io/"
)

// 以下常量标识 Kubernetes 对象类型的资源前缀。
const (
	// HugePages 请求，单位为字节（500Gi = 500GiB = 500 * 1024 * 1024 * 1024）。
	// HugePages 不支持突发的用量，因此只对其请求值配额，忽略限制值。
	ResourceRequestsHugePagesPrefix = "requests.hugepages-"
	// 默认资源请求前缀。
	DefaultResourceRequestsPrefix = "requests."
)

// ResourceQuotaScope 定义一个过滤器，配额跟踪的每个对象都必须匹配该过滤器。
// +enum
type ResourceQuotaScope string

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

	// 匹配所有指定了卷属性类的 PVC 对象。
	ResourceQuotaScopeVolumeAttributesClass ResourceQuotaScope = "VolumeAttributesClass"
)

// ResourceQuotaSpec 定义配额要施加的硬性限制。
type ResourceQuotaSpec struct {
	// hard 是各命名资源所期望的硬性限制集合。
	// 更多信息：https://kubernetes.io/docs/concepts/policy/resource-quotas/
	// +optional
	Hard ResourceList `json:"hard,omitempty" protobuf:"bytes,1,rep,name=hard,casttype=ResourceList,castkey=ResourceName"`
	// 配额跟踪的每个对象都必须匹配的一组过滤器。
	// 若未指定，则配额匹配所有对象。
	// +optional
	// +listType=atomic
	Scopes []ResourceQuotaScope `json:"scopes,omitempty" protobuf:"bytes,2,rep,name=scopes,casttype=ResourceQuotaScope"`
	// scopeSelector 与 scopes 类似，也是一组过滤器，配额跟踪的每个对象都必须匹配，
	// 但它通过 ScopeSelectorOperator 与可能的取值组合来表达。
	// 资源要匹配，必须同时匹配 scopes 和 scopeSelector（如果在 spec 中指定）。
	// +optional
	ScopeSelector *ScopeSelector `json:"scopeSelector,omitempty" protobuf:"bytes,3,opt,name=scopeSelector"`
}

// ScopeSelector 表示各作用域资源选择器要求之间的逻辑与（AND）。
// +structType=atomic
type ScopeSelector struct {
	// 按资源作用域划分的作用域选择器要求列表。
	// +optional
	// +listType=atomic
	MatchExpressions []ScopedResourceSelectorRequirement `json:"matchExpressions,omitempty" protobuf:"bytes,1,rep,name=matchExpressions"`
}

// 作用域资源选择器要求是一个选择器，包含若干值、一个作用域名称，
// 以及用于关联作用域名称与这些值的操作符。
type ScopedResourceSelectorRequirement struct {
	// 该选择器适用的作用域名称。
	ScopeName ResourceQuotaScope `json:"scopeName" protobuf:"bytes,1,opt,name=scopeName"`
	// 表示作用域与一组值之间的关系。
	// 有效操作符为 In、NotIn、Exists、DoesNotExist。
	Operator ScopeSelectorOperator `json:"operator" protobuf:"bytes,2,opt,name=operator,casttype=ScopedResourceSelectorOperator"`
	// 字符串值数组。当操作符为 In 或 NotIn 时，该数组必须非空；
	// 当操作符为 Exists 或 DoesNotExist 时，该数组必须为空。
	// 在 strategic merge patch 中会替换此数组。
	// +optional
	// +listType=atomic
	Values []string `json:"values,omitempty" protobuf:"bytes,3,rep,name=values"`
}

// ScopeSelectorOperator 是作用域选择器要求中可用的操作符集合。
// +enum
type ScopeSelectorOperator string

const (
	// ScopeSelectorOpIn 表示匹配给定值之一。
	ScopeSelectorOpIn ScopeSelectorOperator = "In"
	// ScopeSelectorOpNotIn 表示不匹配任何给定值。
	ScopeSelectorOpNotIn ScopeSelectorOperator = "NotIn"
	// ScopeSelectorOpExists 表示作用域存在。
	ScopeSelectorOpExists ScopeSelectorOperator = "Exists"
	// ScopeSelectorOpDoesNotExist 表示作用域不存在。
	ScopeSelectorOpDoesNotExist ScopeSelectorOperator = "DoesNotExist"
)

// ResourceQuotaStatus 定义已施加的硬性限制和观测到的使用量。
type ResourceQuotaStatus struct {
	// Hard 是各命名资源已施加的硬性限制集合。
	// 更多信息：https://kubernetes.io/docs/concepts/policy/resource-quotas/
	// +optional
	Hard ResourceList `json:"hard,omitempty" protobuf:"bytes,1,rep,name=hard,casttype=ResourceList,castkey=ResourceName"`
	// Used 是该命名空间中当前观测到的资源总使用量。
	// +optional
	Used ResourceList `json:"used,omitempty" protobuf:"bytes,2,rep,name=used,casttype=ResourceList,castkey=ResourceName"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ResourceQuota 设置每个命名空间所施加的聚合配额限制。
// +k8s:supportsSubresource="/status"
type ResourceQuota struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Spec 定义所期望的配额。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Spec ResourceQuotaSpec `json:"spec,omitempty" protobuf:"bytes,2,opt,name=spec"`

	// Status 定义实际施加的配额及其当前使用量。
	// https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#spec-and-status
	// +optional
	Status ResourceQuotaStatus `json:"status,omitempty" protobuf:"bytes,3,opt,name=status"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ResourceQuotaList 是 ResourceQuota 对象的列表。
type ResourceQuotaList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ResourceQuota 对象的列表。
	// 更多信息：https://kubernetes.io/docs/concepts/policy/resource-quotas/
	Items []ResourceQuota `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// Secret 保存指定类型的私密数据。Data 字段中所有值的总字节数必须小于 MaxSecretSize 字节。
type Secret struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Immutable 设为 true 时，确保 Secret 中存储的数据无法更新
	// （只能修改对象元数据）。
	// 若未设为 true，该字段可随时修改。
	// 默认为 nil。
	// +optional
	Immutable *bool `json:"immutable,omitempty" protobuf:"varint,5,opt,name=immutable"`

	// Data 包含私密数据。每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 私密数据的序列化形式为 base64 编码字符串，表示此处任意（可能是非字符串）的数据值。
	// 参见 https://tools.ietf.org/html/rfc4648#section-4
	// +optional
	Data map[string][]byte `json:"data,omitempty" protobuf:"bytes,2,rep,name=data"`

	// stringData 允许以字符串形式指定非二进制私密数据。
	// 为方便起见，它作为只写输入字段提供。
	// 写入时所有键和值都会合并到 data 字段，覆盖已有值。
	// 从 API 读取时永远不会输出 stringData 字段。
	// +k8s:conversion-gen=false
	// +optional
	StringData map[string]string `json:"stringData,omitempty" protobuf:"bytes,4,rep,name=stringData"`

	// 用于以编程方式处理私密数据。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/secret/#secret-types
	// +optional
	// +k8s:optional
	// +k8s:alpha(since: "1.37")=+k8s:immutable
	Type SecretType `json:"type,omitempty" protobuf:"bytes,3,opt,name=type,casttype=SecretType"`
}

// MaxSecretSize 是 Secret 数据的最大字节数。
const MaxSecretSize = 1 * 1024 * 1024

// SecretType 表示 Secret 的类型。
type SecretType string

const (
	// SecretTypeOpaque 是默认值，表示任意用户自定义数据。
	SecretTypeOpaque SecretType = "Opaque"

	// SecretTypeServiceAccountToken 包含一个用于向 API 标识服务账号的令牌。
	//
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
	//
	// 必填字段：
	// - Secret.Data[".dockercfg"] - 序列化后的 ~/.dockercfg 文件
	SecretTypeDockercfg SecretType = "kubernetes.io/dockercfg"

	// DockerConfigKey 是 SecretTypeDockercfg 类型 Secret 必填数据的键。
	DockerConfigKey = ".dockercfg"

	// SecretTypeDockerConfigJson 包含一个遵循 ~/.docker/config.json 相同格式规则的 dockercfg 文件。
	//
	// 必填字段：
	// - Secret.Data[".dockerconfigjson"] - 序列化后的 ~/.docker/config.json 文件
	SecretTypeDockerConfigJson SecretType = "kubernetes.io/dockerconfigjson"

	// DockerConfigJsonKey 是 SecretTypeDockerConfigJson 类型 Secret 必填数据的键。
	DockerConfigJsonKey = ".dockerconfigjson"

	// SecretTypeBasicAuth 包含基本认证所需的数据。
	//
	// 至少要求以下字段之一：
	// - Secret.Data["username"] - 用于认证的用户名
	// - Secret.Data["password"] - 认证所需的密码或令牌
	SecretTypeBasicAuth SecretType = "kubernetes.io/basic-auth"

	// BasicAuthUsernameKey 是 SecretTypeBasicAuth 类型 Secret 用户名的键。
	BasicAuthUsernameKey = "username"
	// BasicAuthPasswordKey 是 SecretTypeBasicAuth 类型 Secret 密码或令牌的键。
	BasicAuthPasswordKey = "password"

	// SecretTypeSSHAuth 包含 SSH 认证所需的数据。
	//
	// 必填字段：
	// - Secret.Data["ssh-privatekey"] - 认证所需的 SSH 私钥
	SecretTypeSSHAuth SecretType = "kubernetes.io/ssh-auth"

	// SSHAuthPrivateKey 是 SecretTypeSSHAuth 类型 Secret 必填 SSH 私钥的键。
	SSHAuthPrivateKey = "ssh-privatekey"
	// SecretTypeTLS 包含 TLS 客户端或服务端 Secret 的信息。它主要用于 Ingress 资源的 TLS 终止，
	// 也可用于其他类型。
	//
	// 必填字段：
	// - Secret.Data["tls.key"] - TLS 私钥。
	//   Secret.Data["tls.crt"] - TLS 证书。
	// TODO: 考虑支持不同格式，以及指定 CA/destinationCA。
	SecretTypeTLS SecretType = "kubernetes.io/tls"

	// TLSCertKey 是 TLS Secret 中 TLS 证书的键。
	TLSCertKey = "tls.crt"
	// TLSPrivateKeyKey 是 TLS Secret 中私钥字段的键。
	TLSPrivateKeyKey = "tls.key"
	// SecretTypeBootstrapToken 用于自动化引导过程（最初由 kubeadm 实现）。
	// 它存储用于对已知 ConfigMap 签名的令牌，这些令牌用于身份认证。
	SecretTypeBootstrapToken SecretType = "bootstrap.kubernetes.io/token"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// SecretList 是 Secret 的列表。
type SecretList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 Secret 对象的列表。
	// 更多信息：https://kubernetes.io/docs/concepts/configuration/secret
	Items []Secret `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// ConfigMap 保存供 Pod 使用的配置数据。
type ConfigMap struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Immutable 设为 true 时，确保 ConfigMap 中存储的数据无法更新
	// （只能修改对象元数据）。
	// 若未设为 true，该字段可随时修改。
	// 默认为 nil。
	// +optional
	Immutable *bool `json:"immutable,omitempty" protobuf:"varint,4,opt,name=immutable"`

	// Data 包含配置数据。
	// 每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// 含非 UTF-8 字节序列的值必须使用 BinaryData 字段。
	// Data 中的键不得与 BinaryData 字段中的键重叠，校验过程会强制检查这一点。
	// +optional
	Data map[string]string `json:"data,omitempty" protobuf:"bytes,2,rep,name=data"`

	// BinaryData 包含二进制数据。
	// 每个键必须由字母数字字符、'-'、'_' 或 '.' 组成。
	// BinaryData 可以包含不属于 UTF-8 范围的字节序列。
	// BinaryData 中的键不得与 Data 字段中的键重叠，校验过程会强制检查这一点。
	// 使用该字段需要 1.10+ 的 apiserver 和 kubelet。
	// 注意：BinaryData 的键目前不会通过 ConfigMapKeyRef 或 ConfigMapRef 环境变量源
	// 传播到容器环境变量；只有 Data 的键会被使用。
	// +optional
	BinaryData map[string][]byte `json:"binaryData,omitempty" protobuf:"bytes,3,rep,name=binaryData"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.2

// ConfigMapList 是包含 ConfigMap 对象列表的资源。
type ConfigMapList struct {
	metav1.TypeMeta `json:""`

	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Items 是 ConfigMap 的列表。
	Items []ConfigMap `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// ComponentConditionType 用于组件健康状态校验。
type ComponentConditionType string

// 以下是组件的有效状态。
const (
	// ComponentHealthy 表示组件健康。
	ComponentHealthy ComponentConditionType = "Healthy"
)

// ComponentCondition 保存组件状态的相关信息。
type ComponentCondition struct {
	// Type 是组件的状态类型。
	// 有效值："Healthy"
	Type ComponentConditionType `json:"type" protobuf:"bytes,1,opt,name=type,casttype=ComponentConditionType"`
	// Status 是组件状态的状态值。
	// "Healthy" 的有效值为："True"、"False" 或 "Unknown"。
	Status ConditionStatus `json:"status" protobuf:"bytes,2,opt,name=status,casttype=ConditionStatus"`
	// Message 是组件状态的相关消息。
	// 例如健康检查的信息。
	// +optional
	Message string `json:"message,omitempty" protobuf:"bytes,3,opt,name=message"`
	// Error 是组件状态错误码。
	// 例如健康检查的错误码。
	// +optional
	Error string `json:"error,omitempty" protobuf:"bytes,4,opt,name=error"`
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ComponentStatus（及 ComponentStatusList）保存集群校验信息。
// Deprecated: 该 API 自 v1.19 起已废弃。
type ComponentStatus struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// 观测到的组件状态列表。
	// +optional
	// +patchMergeKey=type
	// +patchStrategy=merge
	// +listType=map
	// +listMapKey=type
	Conditions []ComponentCondition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type" protobuf:"bytes,2,rep,name=conditions"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// ComponentStatusList 是组件所有状态组成的列表，以 ComponentStatus 对象列表表示。
// Deprecated: 该 API 自 v1.19 起已废弃。
type ComponentStatusList struct {
	metav1.TypeMeta `json:""`
	// 标准列表元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds
	// +optional
	metav1.ListMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// ComponentStatus 对象的列表。
	Items []ComponentStatus `json:"items" protobuf:"bytes,2,rep,name=items"`
}

// DownwardAPIVolumeSource 表示包含 downward API 信息的卷。
// downward API 卷支持所有权管理和 SELinux 重新标记。
type DownwardAPIVolumeSource struct {
	// Items 是 downward API 卷文件的列表。
	// +optional
	// +listType=atomic
	Items []DownwardAPIVolumeFile `json:"items,omitempty" protobuf:"bytes,1,rep,name=items"`
	// 可选：默认情况下所创建文件使用的权限位。
	// 可选：用于设置所创建文件默认权限的权限位。
	// 必须是 0000 到 0777 之间的八进制值，或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求权限位使用十进制值。
	// 默认为 0644。
	// 路径中的目录不受此设置影响。
	// 这可能与其他影响文件模式的选项（如 fsGroup）冲突，最终可能设置其他权限位。
	// +optional
	DefaultMode *int32 `json:"defaultMode,omitempty" protobuf:"varint,2,opt,name=defaultMode"`
	// defaultUser 可选：默认情况下所创建文件的所有者 UID。
	// 仅当条目级别的 user 字段未设置时，才使用 defaultUser 作为回退值。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	DefaultUser *int64 `json:"defaultUser,omitempty" protobuf:"varint,3,opt,name=defaultUser"`
}

const (
	// DownwardAPIVolumeSourceDefaultMode 是所创建文件的默认权限位。
	DownwardAPIVolumeSourceDefaultMode int32 = 0644
)

// DownwardAPIVolumeFile 表示创建包含 Pod 字段的文件所需的信息。
type DownwardAPIVolumeFile struct {
	// 必填：Path 是要创建文件的相对路径名。不得为绝对路径或包含 '..' 路径，且必须为 UTF-8 编码。相对路径的第一项不得以 '..' 开头。
	Path string `json:"path" protobuf:"bytes,1,opt,name=path"`
	// 必填：选择 Pod 的一个字段：目前仅支持 annotations、labels、name、namespace 和 uid。
	// +optional
	FieldRef *ObjectFieldSelector `json:"fieldRef,omitempty" protobuf:"bytes,2,opt,name=fieldRef"`
	// 选择容器的一种资源：目前仅支持资源限制和请求
	// （limits.cpu、limits.memory、requests.cpu 和 requests.memory）。
	// +optional
	ResourceFieldRef *ResourceFieldSelector `json:"resourceFieldRef,omitempty" protobuf:"bytes,3,opt,name=resourceFieldRef"`
	// 可选：用于设置此文件权限的权限位，必须是 0000 到 0777 之间的八进制值，
	// 或 0 到 511 之间的十进制值。
	// YAML 接受八进制和十进制值，JSON 要求权限位使用十进制值。
	// 若未指定，则使用卷的 defaultMode。
	// 这可能与其他影响文件模式的选项（如 fsGroup）冲突，最终可能设置其他权限位。
	// +optional
	Mode *int32 `json:"mode,omitempty" protobuf:"varint,4,opt,name=mode"`
	// user 可选：所创建文件的所有者 UID。
	// 若指定，条目级别的 user 字段优先于 defaultUser。
	// （Alpha）此字段需要启用 AtomicWriteVolumeUserFields 特性门控。
	// +featureGate=AtomicWriteVolumeUserFields
	// +optional
	User *int64 `json:"user,omitempty" protobuf:"varint,5,opt,name=user"`
}

// DownwardAPIProjection 表示要投射到 projected 卷中的 downward API 信息。
// 注意，它与不带默认模式的 downwardAPI 卷源完全相同。
type DownwardAPIProjection struct {
	// Items 是 DownwardAPIVolume 文件的列表。
	// +optional
	// +listType=atomic
	Items []DownwardAPIVolumeFile `json:"items,omitempty" protobuf:"bytes,1,rep,name=items"`
}

// SecurityContext 保存将应用于容器的安全配置。
// 有些字段同时存在于 SecurityContext 和 PodSecurityContext 中。当两者都设置时，
// SecurityContext 中的值优先。
type SecurityContext struct {
	// 运行容器时添加/删除的能力（capabilities）。
	// 默认为容器运行时授予的默认能力集。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Capabilities *Capabilities `json:"capabilities,omitempty" protobuf:"bytes,1,opt,name=capabilities"`
	// 以特权模式运行容器。
	// 特权容器中的进程基本上等同于宿主机上的 root。
	// 默认为 false。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	Privileged *bool `json:"privileged,omitempty" protobuf:"varint,2,opt,name=privileged"`
	// 要应用于容器的 SELinux 上下文。
	// 若未指定，容器运行时会为每个容器分配一个随机的 SELinux 上下文。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SELinuxOptions *SELinuxOptions `json:"seLinuxOptions,omitempty" protobuf:"bytes,3,opt,name=seLinuxOptions"`
	// 应用于所有容器的 Windows 特有设置。
	// 若未指定，则使用 PodSecurityContext 中的选项。
	// 若同时在 SecurityContext 和 PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 linux 时不能设置此字段。
	// +optional
	WindowsOptions *WindowsSecurityContextOptions `json:"windowsOptions,omitempty" protobuf:"bytes,10,opt,name=windowsOptions"`
	// 运行容器进程入口点的 UID。
	// 若未指定，默认使用镜像元数据中指定的用户。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsUser *int64 `json:"runAsUser,omitempty" protobuf:"varint,4,opt,name=runAsUser"`
	// 运行容器进程入口点的 GID。
	// 若未设置，则使用运行时默认值。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	RunAsGroup *int64 `json:"runAsGroup,omitempty" protobuf:"varint,8,opt,name=runAsGroup"`
	// 表示容器必须以非 root 用户身份运行。
	// 若为 true，Kubelet 会在运行时校验镜像，确保不以 UID 0（root）运行；否则容器启动失败。
	// 若未设置或为 false，则不执行此类校验。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// +optional
	RunAsNonRoot *bool `json:"runAsNonRoot,omitempty" protobuf:"varint,5,opt,name=runAsNonRoot"`
	// 此容器是否使用只读根文件系统。
	// 默认为 false。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	ReadOnlyRootFilesystem *bool `json:"readOnlyRootFilesystem,omitempty" protobuf:"varint,6,opt,name=readOnlyRootFilesystem"`
	// AllowPrivilegeEscalation 控制进程能否获得比其父进程更多的权限。
	// 此布尔值直接控制是否在容器进程上设置 no_new_privs 标志。
	// 在以下情况下，AllowPrivilegeEscalation 始终为 true：
	// 1) 以特权模式运行
	// 2) 具有 CAP_SYS_ADMIN 能力
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AllowPrivilegeEscalation *bool `json:"allowPrivilegeEscalation,omitempty" protobuf:"varint,7,opt,name=allowPrivilegeEscalation"`
	// procMount 表示容器使用的 proc 挂载类型。
	// 默认值为 Default，对只读路径和屏蔽路径使用容器运行时的默认设置。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	ProcMount *ProcMountType `json:"procMount,omitempty" protobuf:"bytes,9,opt,name=procMount"`
	// 此容器使用的 seccomp 选项。若同时在 Pod 和容器级别提供 seccomp 选项，
	// 容器级别的选项会覆盖 Pod 级别的选项。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	SeccompProfile *SeccompProfile `json:"seccompProfile,omitempty" protobuf:"bytes,11,opt,name=seccompProfile"`
	// appArmorProfile 是此容器使用的 AppArmor 选项。若设置，此配置会
	// 覆盖 Pod 的 appArmorProfile。
	// 注意，当 spec.os.name 为 windows 时不能设置此字段。
	// +optional
	AppArmorProfile *AppArmorProfile `json:"appArmorProfile,omitempty" protobuf:"bytes,12,opt,name=appArmorProfile"`
}

// ProcMountType 定义容器使用的 /proc 挂载类型。
// +enum
type ProcMountType string

const (
	// DefaultProcMount 对 /proc 的只读路径和屏蔽路径使用容器运行时的默认设置。
	// 大多数容器运行时会屏蔽 /proc 中的某些路径，以避免特殊设备或信息被意外暴露。
	DefaultProcMount ProcMountType = "Default"

	// UnmaskedProcMount 绕过容器运行时的默认屏蔽行为，确保容器中新建的 /proc
	// 保持原样，不做任何修改。
	UnmaskedProcMount ProcMountType = "Unmasked"
)

// SELinuxOptions 是要应用于容器的 SELinux 标签。
type SELinuxOptions struct {
	// User 是应用于容器的 SELinux 用户标签。
	// +optional
	User string `json:"user,omitempty" protobuf:"bytes,1,opt,name=user"`
	// Role 是应用于容器的 SELinux 角色标签。
	// +optional
	Role string `json:"role,omitempty" protobuf:"bytes,2,opt,name=role"`
	// Type 是应用于容器的 SELinux 类型标签。
	// +optional
	Type string `json:"type,omitempty" protobuf:"bytes,3,opt,name=type"`
	// Level 是应用于容器的 SELinux 级别标签。
	// +optional
	Level string `json:"level,omitempty" protobuf:"bytes,4,opt,name=level"`
}

// WindowsSecurityContextOptions 包含 Windows 特有的选项和凭据。
type WindowsSecurityContextOptions struct {
	// GMSACredentialSpecName 是要使用的 GMSA 凭据规范的名称。
	// +optional
	GMSACredentialSpecName *string `json:"gmsaCredentialSpecName,omitempty" protobuf:"bytes,1,opt,name=gmsaCredentialSpecName"`

	// GMSACredentialSpec 是 GMSA 准入 webhook
	// (https://github.com/kubernetes-sigs/windows-gmsa) 内联 GMSA 凭据规范内容的位置，
	// 该规范由 GMSACredentialSpecName 字段指定。
	// +optional
	GMSACredentialSpec *string `json:"gmsaCredentialSpec,omitempty" protobuf:"bytes,2,opt,name=gmsaCredentialSpec"`

	// RunAsUserName 是运行容器进程入口点的 Windows 用户名。
	// 若未指定，默认使用镜像元数据中指定的用户。
	// 也可在 PodSecurityContext 中设置。若同时在 SecurityContext 和
	// PodSecurityContext 中设置，则以 SecurityContext 中的值为准。
	// +optional
	RunAsUserName *string `json:"runAsUserName,omitempty" protobuf:"bytes,3,opt,name=runAsUserName"`

	// HostProcess 决定容器是否应作为“Host Process”容器运行。
	// Pod 中所有容器的 HostProcess 有效值必须相同
	// （不允许混合使用 HostProcess 容器和非 HostProcess 容器）。
	// 此外，若 HostProcess 为 true，则 HostNetwork 也必须设为 true。
	// +optional
	HostProcess *bool `json:"hostProcess,omitempty" protobuf:"bytes,4,opt,name=hostProcess"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +k8s:prerelease-lifecycle-gen:introduced=1.0

// RangeAllocation 不是公开类型。
type RangeAllocation struct {
	metav1.TypeMeta `json:""`
	// 标准对象的元数据。
	// 更多信息：https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#metadata
	// +optional
	// +k8s:opaqueType
	metav1.ObjectMeta `json:"metadata,omitempty" protobuf:"bytes,1,opt,name=metadata"`

	// Range 是标识 'data' 所表示范围的字符串。
	Range string `json:"range" protobuf:"bytes,2,opt,name=range"`
	// Data 是包含前一段中所有已分配地址的位数组。
	Data []byte `json:"data" protobuf:"bytes,3,opt,name=data"`
}

const (
	// DefaultSchedulerName 定义默认调度器的名称。
	DefaultSchedulerName = "default-scheduler"

	// RequiredDuringScheduling 亲和性不是对称的，但每条 RequiredDuringScheduling 亲和性规则
	// 都隐含着一条对应的 PreferredDuringScheduling 亲和性规则。
	// 当未指定 --hard-pod-affinity-weight 调度器标志时，
	// DefaultHardPodAffinitySymmetricWeight 定义该隐含 PreferredDuringScheduling 亲和性规则的权重。
	DefaultHardPodAffinitySymmetricWeight int32 = 1
)

// Sysctl 定义要设置的内核参数。
type Sysctl struct {
	// 要设置的属性名。
	Name string `json:"name" protobuf:"bytes,1,opt,name=name"`
	// 要设置的属性值。
	Value string `json:"value" protobuf:"bytes,2,opt,name=value"`
}

const (
	// 为远程命令执行启用 stdin。
	ExecStdinParam = "input"
	// 为远程命令执行启用 stdout。
	ExecStdoutParam = "output"
	// 为远程命令执行启用 stderr。
	ExecStderrParam = "error"
	// 为远程命令执行启用 TTY。
	ExecTTYParam = "tty"
	// 远程命令执行时运行的命令。
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
	// 请求 ID 的请求头名称，用于将单次转发连接的错误流和数据流关联起来。
	PortForwardRequestIDHeader = "requestID"
)

const (
	// MixedProtocolNotSupported 在 PortStatus 中表示云端提供商无法在负载均衡器上发布该端口，
	// 因为同一 LoadBalancer 类型 Service 上混合使用多种协议不受云端提供商支持。
	MixedProtocolNotSupported = "MixedProtocolNotSupported"
)

// PortStatus 表示 Service 端口的错误状态。
type PortStatus struct {
	// Port 是记录此处状态的 Service 端口的端口号。
	Port int32 `json:"port" protobuf:"varint,1,opt,name=port"`
	// Protocol 是记录此处状态的 Service 端口的协议。
	// 支持的值为："TCP"、"UDP"、"SCTP"。
	Protocol Protocol `json:"protocol" protobuf:"bytes,2,opt,name=protocol,casttype=Protocol"`
	// Error 记录 Service 端口的问题。
	// 错误格式必须遵循以下规则：
	// - 内置错误值必须在本文件中指定，且使用 CamelCase 命名
	// - 云端提供商特有的错误值必须符合 foo.example.com/CamelCase 格式。
	// ---
	// 其匹配的正则表达式为 (dns1123SubdomainFmt/)?(qualifiedNameFmt)
	// +optional
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:Pattern=`^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?(([A-Za-z0-9][-A-Za-z0-9_.]*)?[A-Za-z0-9])$`
	// +kubebuilder:validation:MaxLength=316
	Error *string `json:"error,omitempty" protobuf:"bytes,3,opt,name=error"`
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
	// 拉取密钥的组装方式与容器镜像相同，会查找节点凭据、ServiceAccount 镜像拉取密钥以及 Pod spec 中的镜像拉取密钥。
	// 更多信息：https://kubernetes.io/docs/concepts/containers/images
	// 此字段为可选，以便更高层的配置管理能够在 Deployment 和 StatefulSet 等工作负载控制器中
	// 为容器镜像设置默认值或进行覆盖。
	// +optional
	Reference string `json:"reference,omitempty" protobuf:"bytes,1,opt,name=reference"`

	// 拉取 OCI 对象的策略。可能的值为：
	// Always：kubelet 总是尝试拉取引用。若拉取失败，容器创建将失败。
	// Never：kubelet 从不拉取引用，只使用本地镜像或制品。若引用不存在，容器创建将失败。
	// IfNotPresent：若磁盘上不存在该引用，kubelet 才会拉取。若引用不存在且拉取失败，容器创建将失败。
	// 指定 :latest 标签时默认为 Always，否则默认为 IfNotPresent。
	// +optional
	PullPolicy PullPolicy `json:"pullPolicy,omitempty" protobuf:"bytes,2,opt,name=pullPolicy,casttype=PullPolicy"`
}

// NodeAllocatableResourceClaimStatus 描述通过 DRA 分配的节点可分配资源的状态。
type NodeAllocatableResourceClaimStatus struct {
	// ResourceClaimName 是导致此次节点可分配资源分配的 Pod 所引用的资源声明。
	// +required
	// +k8s:required
	ResourceClaimName string `json:"resourceClaimName" protobuf:"bytes,1,opt,name=resourceClaimName"`
	// Containers 列出此 Pod 中引用该声明的所有容器名称。
	// +optional
	// +listType=set
	// +k8s:optional
	// +k8s:listType=set
	Containers []string `json:"containers,omitempty" protobuf:"bytes,2,rep,name=containers"`

	// Resources 已废弃（tombstoned），已由更细粒度的 Mapping 和 Overhead 字段取代。
	// Resources map[ResourceName]resource.Quantity `json:"resources,omitempty" protobuf:"bytes,3,rep,name=resources"`

	// Mapping 包含通过设备 spec 的 `nodeAllocatableResources[...].mapping` 字段映射的设备分配。
	// kubelet 使用它执行 Pod 级别和容器级别的 cgroup 限制。
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	Mapping []NodeAllocatableMappedResources `json:"mapping,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,4,rep,name=mapping"`
	// Overhead 包含通过设备 spec 的 `nodeAllocatableResources[...].overhead` 字段映射的设备分配。
	// kubelet 使用它执行 Pod 级别和容器级别的 cgroup 限制。
	// +optional
	// +patchStrategy=merge
	// +patchMergeKey=name
	// +listType=map
	// +listMapKey=name
	// +k8s:optional
	// +k8s:listType=map
	// +k8s:listMapKey=name
	Overhead []NodeAllocatableOverheadResources `json:"overhead,omitempty" patchStrategy:"merge" patchMergeKey:"name" protobuf:"bytes,5,rep,name=overhead"`
}

// NodeAllocatableMappedResources 描述映射的节点可分配资源分配。
type NodeAllocatableMappedResources struct {
	// Name 是资源名称（例如 cpu、memory）。
	// +required
	// +k8s:required
	Name ResourceName `json:"name" protobuf:"bytes,1,opt,name=name,casttype=ResourceName"`
	// Quantity 是为该声明分配的节点可分配资源总容量。
	// 该声明分配的设备由所有引用该声明的容器共享。
	// Kubelet 会将此值加到 Pod 级别 cgroup 的请求值和限制值中，并加到每个引用该声明的容器的
	// 容器级别 cgroup 限制值中。
	// +required
	// +k8s:required
	Quantity *resource.Quantity `json:"quantity" protobuf:"bytes,2,opt,name=quantity"`
}

// NodeAllocatableOverheadResources 描述辅助性的开销资源分配。
type NodeAllocatableOverheadResources struct {
	// Name 是资源名称（例如 cpu、memory）。
	// +required
	// +k8s:required
	Name ResourceName `json:"name" protobuf:"bytes,1,opt,name=name,casttype=ResourceName"`
	// PerPod 是每个 Pod 分配的固定开销数量。
	// 将其加到每个容器限制中，各容器即可使用该开销；而父级 Pod 级别 cgroup 的限制会将总使用量
	// 限制在 Pod 边界内，从而确保该开销只被计入一次。
	// PerPod 或 PerContainer 至少指定一个。两者都未指定属于无效配置。
	// +optional
	// +k8s:optional
	PerPod *resource.Quantity `json:"perPod,omitempty" protobuf:"bytes,2,opt,name=perPod"`
	// PerContainer 是应用于每个引用该声明的容器的可变开销数量。
	// 容器引用记录在 `nodeAllocatableResourceClaimStatuses.containers` 中。
	// 为该声明分配的总开销数量计算如下：
	// Quantity = PerPod + (PerContainer * NumReferences)
	// Kubelet 在 cgroup 中计入此开销：
	// - Pod 级别 cgroup（请求值和限制值）：Kubelet 加上 PerPod + (PerContainer * NumReferences)。
	// - 容器级别 cgroup（仅限制值）：Kubelet 为每个引用该声明的容器加上 PerPod + PerContainer。
	// 这样任何单个容器都可以使用 Pod 级别的开销，而父级 cgroup 会将总使用量限制为 PerPod 只计入一次。
	// PerPod 或 PerContainer 至少指定一个。两者都未指定属于无效配置。
	// +optional
	// +k8s:optional
	PerContainer *resource.Quantity `json:"perContainer,omitempty" protobuf:"bytes,3,opt,name=perContainer"`
}
