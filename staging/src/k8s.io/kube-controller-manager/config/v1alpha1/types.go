/*
Copyright 2018 The Kubernetes Authors.

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cpconfigv1alpha1 "k8s.io/cloud-provider/config/v1alpha1"
	serviceconfigv1alpha1 "k8s.io/cloud-provider/controllers/service/config/v1alpha1"
	cmconfigv1alpha1 "k8s.io/controller-manager/config/v1alpha1"
)

// 这个文件是 kube-controller-manager 启动配置的 schema：程序启动时从文件中反序列化
// KubeControllerManagerConfiguration，再把各控制器子结构交给对应控制器使用。
// 结构体按“一个控制器一节”组织，而不是把所有开关平铺在一层：这样配置项与负责它的控制器
// 一一对应，既能按控制器拆分代码，也不会让其中一个控制器的参数散落在文件各处。
// 这里复用 controller-manager、cloud-provider 等其他模块的配置类型，是为了避免把公共配置
// 复制到本模块后再手工保持同步；跨模块的字段含义以被引用模块的类型定义为准。
// 时间字段统一用 metav1.Duration 而不是裸整数，是为了让配置文件里能写 "30s" 这类带单位的
// 文本，默认的 JSON 解码即可完成转换，不必在每个控制器里各写一套单位约定。
// 部分开关用 *bool 而不是 bool，是为了区分“显式配置为 false”和“未提供、走默认值”，
// 否则配置文件无法关闭一个默认为 true 的行为。

// PersistentVolumeRecyclerConfiguration contains elements describing persistent volume plugins.
//
// 回收（recycle）是早期 PV 回收策略的一种，靠启动临时 Pod 清理卷上的数据后再复用卷；
// 这组参数集中在同一结构里，是因为它们共同决定清理 Pod 的超时与模板，
// 分散到各插件会让同一套语义出现多份实现。HostPath 相关字段只用于单机开发与测试，
// 因为 HostPath 绑定的是具体节点，在多节点集群中无法保证 Pod 调度到正确节点。
type PersistentVolumeRecyclerConfiguration struct {
	// maximumRetry is number of retries the PV recycler will execute on failure to recycle
	// PV.
	MaximumRetry int32
	// minimumTimeoutNFS is the minimum ActiveDeadlineSeconds to use for an NFS Recycler
	// pod.
	MinimumTimeoutNFS int32
	// podTemplateFilePathNFS is the file path to a pod definition used as a template for
	// NFS persistent volume recycling
	PodTemplateFilePathNFS string
	// incrementTimeoutNFS is the increment of time added per Gi to ActiveDeadlineSeconds
	// for an NFS scrubber pod.
	IncrementTimeoutNFS int32
	// podTemplateFilePathHostPath is the file path to a pod definition used as a template for
	// HostPath persistent volume recycling. This is for development and testing only and
	// will not work in a multi-node cluster.
	PodTemplateFilePathHostPath string
	// minimumTimeoutHostPath is the minimum ActiveDeadlineSeconds to use for a HostPath
	// Recycler pod.  This is for development and testing only and will not work in a multi-node
	// cluster.
	MinimumTimeoutHostPath int32
	// incrementTimeoutHostPath is the increment of time added per Gi to ActiveDeadlineSeconds
	// for a HostPath scrubber pod.  This is for development and testing only and will not work
	// in a multi-node cluster.
	IncrementTimeoutHostPath int32
}

// VolumeConfiguration 包含用于配置所有 volume 插件的全部枚举开关。controller-manager
// 二进制会从这里为每个插件派生出只含该插件所需配置的 volume.VolumeConfig 实例并传给对应插件；
// 只有 ControllerManager 本身知道支持哪些插件、哪个开关对应哪个插件。
// 这里把各插件的开关汇总在一处，是为了让配置文件只对应一个卷配置入口；
// 具体插件只接收与自己相关的子配置，从而不知道其他插件的存在。
// 这种集中定义、分散消费的划分让插件不必感知控制器的启动参数，
// 也避免插件之间因为共用同一份配置结构而产生隐式耦合。
type VolumeConfiguration struct {
	// enableHostPathProvisioning 在无云厂商环境时启用 HostPath PV 供给，仅用于供给功能的
	// 测试与开发；HostPath 供给不受任何方式的支持，无法在多节点集群中工作，
	// 除测试或开发外不应用于其他用途。
	// 用指针布尔值是为了区分“显式关闭”与“未配置走默认值”：
	// 默认值是 false，显式写成 true 才打开这个仅供开发测试的路径。
	EnableHostPathProvisioning *bool
	// enableDynamicProvisioning 在支持动态供给的环境中启用卷供给，默认值为 true。
	// 用指针布尔值是为了支持显式关闭默认开启的动态供给。
	EnableDynamicProvisioning *bool
	// persistentVolumeRecyclerConfiguration holds configuration for persistent volume plugins.
	PersistentVolumeRecyclerConfiguration PersistentVolumeRecyclerConfiguration
	// volumePluginDir 是 flex 卷插件搜索额外第三方卷插件的目录的完整路径。
	// FlexVolume 是外部插件机制，插件以可执行文件形式放在该目录，
	// 控制器按约定扫描它来发现第三方卷插件。
	FlexVolumePluginDir string
}

// GroupResource describes an group resource.
//
// 用一个独立结构而不是拼接后的字符串表示“组/资源”，是为了在 GC 忽略列表里
// 按字段匹配，避免调用方约定分隔符或在字符串上做脆弱的解析。
type GroupResource struct {
	// group is the group portion of the GroupResource.
	Group string
	// resource is the resource portion of the GroupResource.
	Resource string
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// KubeControllerManagerConfiguration contains elements describing kube-controller manager.
//
// 这是配置文件的根对象，register.go 中也只把这一个类型注册进 scheme：
// 所有控制器配置都作为它的字段被一次反序列化出来，配置文件因此只有一个顶层入口。
// 内嵌 TypeMeta 是为了让该对象满足 runtime.Object 的约定，携带 Kind/APIVersion 元信息；
// 空 json tag 是按既有行为保留的写法，改动它会影响序列化结果，不要顺手“修正”。
// 字段按控制器划分，而不是按调用顺序或启动阶段划分：配置项的归属一眼可从字段名看出，
// 新增控制器时只需追加一个对应字段，不必调整其他控制器的参数。
// 其中 Generic、KubeCloudShared、ServiceController 直接复用其他模块的类型，
// 是因为这些配置本来就是多个组件共用的，复制一份会引入行为漂移。
// 指令 +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// 要求生成满足 runtime.Object 接口所需的 DeepCopyObject 方法，
// 因为该对象要通过 scheme 参与注册与拷贝；对应生成物不要手工编辑。
type KubeControllerManagerConfiguration struct {
	metav1.TypeMeta `json:""`

	// Generic holds configuration for a generic controller-manager
	Generic cmconfigv1alpha1.GenericControllerManagerConfiguration
	// KubeCloudSharedConfiguration holds configuration for shared related features
	// both in cloud controller manager and kube-controller manager.
	KubeCloudShared cpconfigv1alpha1.KubeCloudSharedConfiguration

	// AttachDetachControllerConfiguration holds configuration for
	// AttachDetachController related features.
	AttachDetachController AttachDetachControllerConfiguration
	// CSRSigningControllerConfiguration holds configuration for
	// CSRSigningController related features.
	CSRSigningController CSRSigningControllerConfiguration
	// DaemonSetControllerConfiguration holds configuration for DaemonSetController
	// related features.
	DaemonSetController DaemonSetControllerConfiguration
	// DeploymentControllerConfiguration holds configuration for
	// DeploymentController related features.
	DeploymentController DeploymentControllerConfiguration
	// DisruptionControllerConfiguration holds configuration for
	// DisruptionController related features.
	DisruptionController DisruptionControllerConfiguration
	// StatefulSetControllerConfiguration holds configuration for
	// StatefulSetController related features.
	StatefulSetController StatefulSetControllerConfiguration
	// DeprecatedControllerConfiguration holds configuration for some deprecated
	// features.
	DeprecatedController DeprecatedControllerConfiguration
	// EndpointControllerConfiguration holds configuration for EndpointController
	// related features.
	EndpointController EndpointControllerConfiguration
	// EndpointSliceControllerConfiguration holds configuration for
	// EndpointSliceController related features.
	EndpointSliceController EndpointSliceControllerConfiguration
	// EndpointSliceMirroringControllerConfiguration holds configuration for
	// EndpointSliceMirroringController related features.
	EndpointSliceMirroringController EndpointSliceMirroringControllerConfiguration
	// EphemeralVolumeControllerConfiguration holds configuration for EphemeralVolumeController
	// related features.
	EphemeralVolumeController EphemeralVolumeControllerConfiguration
	// GarbageCollectorControllerConfiguration holds configuration for
	// GarbageCollectorController related features.
	GarbageCollectorController GarbageCollectorControllerConfiguration
	// HPAControllerConfiguration holds configuration for HPAController related features.
	HPAController HPAControllerConfiguration
	// JobControllerConfiguration holds configuration for JobController related features.
	JobController JobControllerConfiguration
	// CronJobControllerConfiguration holds configuration for CronJobController related features.
	CronJobController CronJobControllerConfiguration
	// LegacySATokenCleanerConfiguration holds configuration for LegacySATokenCleaner related features.
	LegacySATokenCleaner LegacySATokenCleanerConfiguration
	// NamespaceControllerConfiguration holds configuration for NamespaceController
	// related features.
	NamespaceController NamespaceControllerConfiguration
	// NodeIPAMControllerConfiguration holds configuration for NodeIPAMController
	// related features.
	NodeIPAMController NodeIPAMControllerConfiguration
	// NodeLifecycleControllerConfiguration holds configuration for
	// NodeLifecycleController related features.
	NodeLifecycleController NodeLifecycleControllerConfiguration
	// PersistentVolumeBinderControllerConfiguration holds configuration for
	// PersistentVolumeBinderController related features.
	PersistentVolumeBinderController PersistentVolumeBinderControllerConfiguration
	// PodGCControllerConfiguration holds configuration for PodGCController
	// related features.
	PodGCController PodGCControllerConfiguration
	// ReplicaSetControllerConfiguration holds configuration for ReplicaSet related features.
	ReplicaSetController ReplicaSetControllerConfiguration
	// ReplicationControllerConfiguration holds configuration for
	// ReplicationController related features.
	ReplicationController ReplicationControllerConfiguration
	// ResourceQuotaControllerConfiguration holds configuration for
	// ResourceQuotaController related features.
	ResourceQuotaController ResourceQuotaControllerConfiguration
	// SAControllerConfiguration holds configuration for ServiceAccountController
	// related features.
	SAController SAControllerConfiguration
	// ServiceControllerConfiguration holds configuration for ServiceController
	// related features.
	ServiceController serviceconfigv1alpha1.ServiceControllerConfiguration
	// TTLAfterFinishedControllerConfiguration holds configuration for
	// TTLAfterFinishedController related features.
	TTLAfterFinishedController TTLAfterFinishedControllerConfiguration
	// ValidatingAdmissionPolicyStatusControllerConfiguration holds configuration for
	// ValidatingAdmissionPolicyStatusController related features.
	ValidatingAdmissionPolicyStatusController ValidatingAdmissionPolicyStatusControllerConfiguration
	// DeviceTaintEvictionControllerConfiguration contains elements configuring the device taint eviction controller.
	DeviceTaintEvictionController DeviceTaintEvictionControllerConfiguration
	// ResourceClaimControllerConfiguration contains elements configuring the resource claim controller.
	ResourceClaimController ResourceClaimControllerConfiguration
}

// AttachDetachControllerConfiguration 包含描述 AttachDetachController 的各个要素。
// Attach/Detach 控制器通过周期性对账把期望状态与集群实际状态对齐，
// 因为卷的挂载状态可能因为节点重启或外部操作而悄悄偏离记录值。
// Disable 开头的布尔值与命令行开关同名，保持配置项到 flag 的直接映射；
// 默认 false 即启用对账，避免只配置其他字段的用户意外失去这项保护。
// 超时后的强制卸载单独设开关，是因为强制操作可能中断正在使用卷的 Pod，
// 默认情况下允许，但给出显式关闭的余地。
type AttachDetachControllerConfiguration struct {
	// DisableAttachDetachReconcilerSync 启用或禁用对账：对账循环通过触发 attach/detach
	// 操作使期望状态与集群实际状态保持一致。字段名带 Disable 而默认值为 false，等于默认启用对账；
	// 保留这种反向命名是为了兼容已有命令行开关，避免改配置时语义错位。
	DisableAttachDetachReconcilerSync bool
	// ReconcilerSyncLoopPeriod is the amount of time the reconciler sync states loop
	// wait between successive executions. Is set to 60 sec by default.
	ReconcilerSyncLoopPeriod metav1.Duration
	// DisableForceDetachOnTimeout 在超过最大卸载时间后禁用强制卸载。默认为 false，
	// 即超时后仍启用强制卸载；反向命名同样是为了对齐命令行开关。
	DisableForceDetachOnTimeout bool `json:"disableForceDetachOnTimeout"`
}

// CSRSigningControllerConfiguration contains elements describing CSRSigningController.
//
// 集群签名证书与密钥作为全局缺省，另外按 signer 名称细分配置，
// 是因为 kubelet serving、kubelet client、apiserver client 等用途的信任域不同，
// 需要能在不影响其他用途的前提下单独轮换某一 signer 的签发材料。
// legacy-unknown 一项保留下来是为了继续处理旧客户端留下的 CSR，
// 即使新签发的证书不再走这条路径，也不能直接让历史请求失败。
// 证书有效期设为上限而非固定值，允许 CSR 通过 spec.expirationSeconds 申请更短的证书。
type CSRSigningControllerConfiguration struct {
	// clusterSigningCertFile is the filename containing a PEM-encoded
	// X509 CA certificate used to issue cluster-scoped certificates
	ClusterSigningCertFile string
	// clusterSigningCertFile is the filename containing a PEM-encoded
	// RSA or ECDSA private key used to issue cluster-scoped certificates
	ClusterSigningKeyFile string

	// kubeletServingSignerConfiguration holds the certificate and key used to issue certificates for the kubernetes.io/kubelet-serving signer
	KubeletServingSignerConfiguration CSRSigningConfiguration
	// kubeletClientSignerConfiguration holds the certificate and key used to issue certificates for the kubernetes.io/kube-apiserver-client-kubelet
	KubeletClientSignerConfiguration CSRSigningConfiguration
	// kubeAPIServerClientSignerConfiguration holds the certificate and key used to issue certificates for the kubernetes.io/kube-apiserver-client
	KubeAPIServerClientSignerConfiguration CSRSigningConfiguration
	// legacyUnknownSignerConfiguration holds the certificate and key used to issue certificates for the kubernetes.io/legacy-unknown
	LegacyUnknownSignerConfiguration CSRSigningConfiguration

	// clusterSigningDuration 是签发证书时长的上限，允许为不同签发方设置不同的最大值；
	// 单个 CSR 仍可通过设置 spec.expirationSeconds 申请更短的证书。
	ClusterSigningDuration metav1.Duration
}

// CSRSigningConfiguration holds information about a particular CSR signer
//
// 证书与密钥必须成对提供，签发行为缺一不可，所以放在同一个结构里。
// 这里存的是文件路径而不是证书内容，便于挂载密钥卷后原地轮换文件，
// 不必因为证书更新而重写配置文件。
type CSRSigningConfiguration struct {
	// certFile is the filename containing a PEM-encoded
	// X509 CA certificate used to issue certificates
	CertFile string
	// keyFile is the filename containing a PEM-encoded
	// RSA or ECDSA private key used to issue certificates
	KeyFile string
}

// DaemonSetControllerConfiguration contains elements describing DaemonSetController.
//
// 并发数针对的是不同的 DaemonSet 对象，不影响单个 DaemonSet 内 Pod 的调度顺序。
// 调大可以提高响应速度，代价是更多的 CPU 与 API Server 压力，属于典型的吞吐取舍。
type DaemonSetControllerConfiguration struct {
	// concurrentDaemonSetSyncs 是允许并发同步的 daemonset 对象数量：数量越大响应越快，
	// 但 CPU（及网络）负载也越高。提高并发会按比例增加 API Server 负载，调整前需评估集群规模。
	ConcurrentDaemonSetSyncs int32
}

// DeploymentControllerConfiguration contains elements describing DeploymentController.
//
// 并发数限制同时处理的 Deployment 对象数量，
// 让一个集群里的大量工作负载更新不会同时压向 API Server。
type DeploymentControllerConfiguration struct {
	// concurrentDeploymentSyncs 是允许并发同步的 deployment 对象数量：数量越大响应越快，
	// 但 CPU（及网络）负载也越高。提高并发会按比例增加 API Server 负载，调整前需评估集群规模。
	ConcurrentDeploymentSyncs int32
}

// DisruptionControllerConfiguration 包含描述 DisruptionController 的各个要素。
// PDB 的状态直接决定驱逐是否放行，因此同步并发数会影响驱逐判断的及时性；
// 这里的取舍和其他控制器一样，是在响应速度与 API 负载之间找平衡点。
type DisruptionControllerConfiguration struct {
	// concurrentDisruptionSyncs 是允许并发同步的 PodDisruptionBudget 对象数量：数量越大
	// PDB 更新越及时，但 CPU（及网络）负载也越高。提高并发会加快 PDB 状态更新，代价是更多的 API 调用。
	ConcurrentDisruptionSyncs int32
}

// StatefulSetControllerConfiguration contains elements describing StatefulSetController.
//
// 并发数针对多个 StatefulSet 对象，而不是同一个集合内的多个 Pod，
// 因此提高并发不会破坏有状态应用依赖的序号语义。
type StatefulSetControllerConfiguration struct {
	// concurrentStatefulSetSyncs 是允许并发同步的 statefulset 对象数量：数量越大响应越快，
	// 但 CPU（及网络）负载也越高。提高并发只影响对象级并行，不破坏单个 StatefulSet 内的顺序约束。
	ConcurrentStatefulSetSyncs int32
}

// DeprecatedControllerConfiguration contains elements be deprecated.
//
// 空结构体是历史遗留的占位：原先这里放已弃用控制器的开关，
// 移除字段后保留类型本身，可以让仍引用该类型的代码和旧配置文件继续工作。
type DeprecatedControllerConfiguration struct {
}

// EndpointControllerConfiguration contains elements describing EndpointController.
//
// 批量周期把短时间内的多次 Pod 变化合并成一次 Endpoint 更新，
// 用少量延迟换取更少的 API 写入；默认值对应“不刻意合并”的行为。
// 并发数同理，提高后更新更及时，但会增加 API Server 负载。
type EndpointControllerConfiguration struct {
	// concurrentEndpointSyncs is the number of endpoint syncing operations
	// that will be done concurrently. Larger number = faster endpoint updating,
	// but more CPU (and network) load.
	ConcurrentEndpointSyncs int32

	// EndpointUpdatesBatchPeriod describes the length of endpoint updates batching period.
	// Processing of pod changes will be delayed by this duration to join them with potential
	// upcoming updates and reduce the overall number of endpoints updates.
	EndpointUpdatesBatchPeriod metav1.Duration
}

// EndpointSliceControllerConfiguration contains elements describing
// EndpointSliceController.
//
// 每个 Slice 的端点上限在对象数量与单个对象大小之间取舍：
// 上限越大，Slice 越少但单个资源越大，更新时传输的数据也越多。
// 批量周期与 Endpoint 控制器保持同一语义，让两条路径上的变更合并策略一致。
type EndpointSliceControllerConfiguration struct {
	// concurrentServiceEndpointSyncs is the number of service endpoint syncing
	// operations that will be done concurrently. Larger number = faster
	// endpoint slice updating, but more CPU (and network) load.
	ConcurrentServiceEndpointSyncs int32

	// maxEndpointsPerSlice is the maximum number of endpoints that will be
	// added to an EndpointSlice. More endpoints per slice will result in fewer
	// and larger endpoint slices, but larger resources.
	MaxEndpointsPerSlice int32

	// EndpointUpdatesBatchPeriod describes the length of endpoint updates batching period.
	// Processing of pod changes will be delayed by this duration to join them with potential
	// upcoming updates and reduce the overall number of endpoints updates.
	EndpointUpdatesBatchPeriod metav1.Duration
}

// EndpointSliceMirroringControllerConfiguration contains elements describing
// EndpointSliceMirroringController.
//
// 镜像控制器负责把旧版 Endpoints 资源复制成 EndpointSlice，
// 让还在使用 Endpoints 的组件与只消费 EndpointSlice 的组件能共存。
// 它的并发、端点和批量参数独立配置，是因为镜像路径的变更来自 Endpoints，
// 与原生 EndpointSlice 控制器的更新来源和频率都不同，不宜共用同一组数值。
// 批量周期为 0 时不延迟，每次 Endpoints 变化立即写一次 EndpointSlice。
type EndpointSliceMirroringControllerConfiguration struct {
	// mirroringConcurrentServiceEndpointSyncs is the number of service endpoint
	// syncing operations that will be done concurrently. Larger number = faster
	// endpoint slice updating, but more CPU (and network) load.
	MirroringConcurrentServiceEndpointSyncs int32

	// mirroringMaxEndpointsPerSubset is the maximum number of endpoints that
	// will be mirrored to an EndpointSlice for an EndpointSubset.
	MirroringMaxEndpointsPerSubset int32

	// mirroringEndpointUpdatesBatchPeriod can be used to batch EndpointSlice
	// updates. All updates triggered by EndpointSlice changes will be delayed
	// by up to 'mirroringEndpointUpdatesBatchPeriod'. If other addresses in the
	// same Endpoints resource change in that period, they will be batched to a
	// single EndpointSlice update. Default 0 value means that each Endpoints
	// update triggers an EndpointSlice update.
	MirroringEndpointUpdatesBatchPeriod metav1.Duration
}

// EphemeralVolumeControllerConfiguration contains elements describing EphemeralVolumeController.
//
// 临时卷控制器处理 Pod 内联声明的 generic ephemeral volume，
// 其生命周期跟随 Pod，与被独立管理的 PVC 不同。
// 并发数限制同时同步的卷对象数量，避免大量 Pod 创建时集中产生 PVC。
type EphemeralVolumeControllerConfiguration struct {
	// ConcurrentEphemeralVolumeSyncs 是并发执行的临时卷同步操作数量：数量越大临时卷更新越快，
	// 但 CPU（及网络）负载也越高。提高并发可缓解大量短生命周期 Pod 同时创建 PVC 时的积压。
	ConcurrentEphemeralVolumeSyncs int32
}

// GarbageCollectorControllerConfiguration contains elements describing GarbageCollectorController.
//
// 该开关必须与 kube-apiserver 侧保持一致：只有一端开启时，
// 级联删除的依赖关系无法在两端同时被正确处理，容易留下无人回收的孤儿对象。
// 忽略列表按 GroupResource 指定，用于让特定资源免于级联删除，
// 避免控制器的通用清理规则误伤需要自行决定生命周期的资源。
// 并发数控制 GC worker，既要及时清理，也要防止扫描依赖图时压垮 API Server。
type GarbageCollectorControllerConfiguration struct {
	// EnableGarbageCollector 启用通用垃圾回收器（WARNING：这是 alpha 特性），
	// 必须与 kube-apiserver 的同名开关保持同步；用指针布尔值是为了显式表达“关闭”。
	EnableGarbageCollector *bool
	// concurrentGCSyncs is the number of garbage collector workers that are
	// allowed to sync concurrently.
	ConcurrentGCSyncs int32
	// gcIgnoredResources 是垃圾回收应忽略的 GroupResource 列表；
	// 忽略列表按 GroupResource 精确匹配，不会误伤同组下的其他资源。
	GCIgnoredResources []GroupResource
}

// HPAControllerConfiguration contains elements describing HPAController.
//
// 多个时间窗口参数共同抑制指标抖动带来的扩缩容震荡：
// 同步周期决定多久看一次指标，稳定窗口要求回缩建议在一定时间内保持成立，
// 容忍度过滤掉幅度不足以覆盖噪声的变化。
// CPU 初始化期与就绪延迟针对刚启动的 Pod：此时 CPU 样本尚未稳定，
// 直接用于计算容易把启动峰值误判为持续负载。
type HPAControllerConfiguration struct {
	// ConcurrentHorizontalPodAutoscalerSyncs 是允许并发同步的 HPA 对象数量：数量越大
	// HPA 处理越及时，但需要更多 CPU、网络与 API 调用开销。
	ConcurrentHorizontalPodAutoscalerSyncs int32
	// HorizontalPodAutoscalerSyncPeriod is the period for syncing the number of
	// pods in horizontal pod autoscaler.
	HorizontalPodAutoscalerSyncPeriod metav1.Duration
	// HorizontalPodAutoscalerDowncaleStabilizationWindow is a period for which autoscaler will look
	// backwards and not scale down below any recommendation it made during that period.
	HorizontalPodAutoscalerDownscaleStabilizationWindow metav1.Duration
	// HorizontalPodAutoscalerTolerance 是判断资源用量是否建议扩缩容的容忍度：
	// 为相对比例，只有指标偏离目标值超过该比例才触发扩缩，用于过滤噪声。
	HorizontalPodAutoscalerTolerance float64
	// HorizontalPodAutoscalerCPUInitializationPeriod is the period after pod start when CPU samples
	// might be skipped.
	HorizontalPodAutoscalerCPUInitializationPeriod metav1.Duration
	// HorizontalPodAutoscalerInitialReadinessDelay is period after pod start during which readiness
	// changes are treated as readiness being set for the first time. The only effect of this is that
	// HPA will disregard CPU samples from unready pods that had last readiness change during that
	// period.
	HorizontalPodAutoscalerInitialReadinessDelay metav1.Duration
}

// JobControllerConfiguration contains elements describing JobController.
//
// 并发数控制同时同步的 Job 对象数量，
// 数值越大任务状态更新越及时，但对应更多的 API 调用。
type JobControllerConfiguration struct {
	// concurrentJobSyncs 是允许并发同步的 job 对象数量：数量越大响应越快，
	// 但 CPU（及网络）负载也越高。提高并发可让更多 Job 及时更新状态，代价是更多 API 调用。
	ConcurrentJobSyncs int32
}

// CronJobControllerConfiguration 包含描述 CronJobController 的各个要素。
// CronJob 有独立的并发配置，因为它的同步逻辑由 CronJob 控制器负责，
// 与 Job 控制器处理的对象和触发节奏都不同，直接复用同一数值没有必要。
type CronJobControllerConfiguration struct {
	// concurrentCronJobSyncs 是允许并发同步的 job 对象数量：数量越大响应越快，
	// 但 CPU（及网络）负载也越高。与 Job 控制器分开限速，避免定时任务调度与 Job 执行相互拖慢。
	ConcurrentCronJobSyncs int32
}

// LegacySATokenCleanerConfiguration contains elements describing LegacySATokenCleaner
//
// 自动生成的 ServiceAccount token secret 不会随着 Pod 删除而消失，
// 需要按最后使用时间定期清理，否则会无限累积。
// 这里的周期同时承担“多久检查一次”和“多久算过期”的角色，
// 配置过短可能误删仍在使用的 token。
type LegacySATokenCleanerConfiguration struct {
	// CleanUpPeriod 是自动生成的 ServiceAccount token 自最后一次使用后、可被删除前需经过的时长；
	// 该周期同时决定检查频率和过期门槛，设置过短可能清理掉仍在使用的 token。
	CleanUpPeriod metav1.Duration
}

// NamespaceControllerConfiguration contains elements describing NamespaceController.
//
// 命名空间删除要先终结所有子资源，再真正移除对象，
// 因此需要周期性地重新扫描，而不是只靠一次删除请求驱动。
// 并发数控制同时处理的 Namespace 数量，避免批量删除时压垮 API Server。
type NamespaceControllerConfiguration struct {
	// namespaceSyncPeriod 是同步命名空间生命周期更新的周期。
	// 删除命名空间需要多轮终结器处理，靠周期重入推进，而不是一次请求完成。
	NamespaceSyncPeriod metav1.Duration
	// concurrentNamespaceSyncs 是允许并发同步的 namespace 对象数量。
	// 提高并发可同时终结更多命名空间，但在大批量删除时会增加 API 压力。
	ConcurrentNamespaceSyncs int32
}

// NodeIPAMControllerConfiguration contains elements describing NodeIpamController.
//
// 单栈与双栈配置并存：ServiceCIDR 和 NodeCIDRMaskSize 覆盖单栈集群，
// 双栈集群才需要 secondary CIDR 与按地址族区分的掩码大小。
// 两个 CIDR 必须属于不同地址族，否则双栈分配无法从中各取一段。
type NodeIPAMControllerConfiguration struct {
	// serviceCIDR is CIDR Range for Services in cluster.
	ServiceCIDR string
	// secondaryServiceCIDR 是集群中 Service 的 CIDR 范围，只在双栈集群中使用，
	// SecondaryServiceCIDR 必须与 ServiceCIDR 属于不同地址族。
	SecondaryServiceCIDR string
	// NodeCIDRMaskSize is the mask size for node cidr in cluster.
	NodeCIDRMaskSize int32
	// NodeCIDRMaskSizeIPv4 是双栈集群中节点 CIDR 的 IPv4 掩码大小；
	// 双栈场景按地址族分别设定掩码，以便为 IPv4 和 IPv6 分配不同大小的网段。
	NodeCIDRMaskSizeIPv4 int32
	// NodeCIDRMaskSizeIPv6 是双栈集群中节点 CIDR 的 IPv6 掩码大小；
	// 双栈场景按地址族分别设定掩码，以便为 IPv4 和 IPv6 分配不同大小的网段。
	NodeCIDRMaskSizeIPv6 int32
}

// NodeLifecycleControllerConfiguration contains elements describing NodeLifecycleController.
//
// 节点失联的判定依赖多个时间窗口：启动宽限期容忍新节点尚未就绪，
// 运行宽限期容忍短暂心跳丢失，驱逐超时决定 Pod 被删除前的等待时间。
// 运行宽限期必须大于 kubelet 上报间隔并考虑 HTTP/2 连接超时，
// 否则会把正常的网络抖动误判为节点宕机。
// 驱逐速率按可用区健康状态分档，并在小集群中压低次速率，
// 是为了避免一次区域性故障引发对大量节点的同步驱逐。
// 不健康可用区的判定要求至少三个节点处于 NotReady，
// 同样是为了防止少数节点异常被放大成可用区级别的判断。
type NodeLifecycleControllerConfiguration struct {
	// nodeEvictionRate is the number of nodes per second on which pods are deleted in case of node failure when a zone is healthy
	NodeEvictionRate float32
	// secondaryNodeEvictionRate is the number of nodes per second on which pods are deleted in case of node failure when a zone is unhealthy
	SecondaryNodeEvictionRate float32
	// nodeStartupGracePeriod is the amount of time which we allow starting a node to
	// be unresponsive before marking it unhealthy.
	NodeStartupGracePeriod metav1.Duration
	// nodeMontiorGracePeriod is the amount of time which we allow a running node to be
	// unresponsive before marking it unhealthy. Must be N times more than kubelet's
	// nodeStatusUpdateFrequency, where N means number of retries allowed for kubelet
	// to post node status. This value should also be greater than the sum of
	// HTTP2_PING_TIMEOUT_SECONDS and HTTP2_READ_IDLE_TIMEOUT_SECONDS.
	NodeMonitorGracePeriod metav1.Duration
	// podEvictionTimeout is the grace period for deleting pods on failed nodes.
	PodEvictionTimeout metav1.Duration
	// 对于规模小于或等于 largeClusterSizeThreshold 的集群，secondaryNodeEvictionRate
	// 会被隐式覆盖为 0；小集群中同时驱逐过多节点风险更大，超过该阈值才启用次驱逐速率。
	LargeClusterSizeThreshold int32
	// 当可用区中至少有 unhealthyZoneThreshold（不小于 3）个节点处于 NotReady 时，
	// nodeEvictionRate 与 secondaryNodeEvictionRate 会将该可用区视为不健康；
	// 下限设为至少 3 个节点，避免单节点故障被误判成整个可用区不健康。
	UnhealthyZoneThreshold float32
	// NodeMonitorPeriod 是 NodeLifecycleController 中同步 NodeStatus 的周期，
	// 与宽限期配合决定失联判定速度。
	NodeMonitorPeriod metav1.Duration
}

// PersistentVolumeBinderControllerConfiguration contains elements describing
// PersistentVolumeBinderController.
//
// 把同步周期和卷配置放在一起，是因为 PV/Claim 绑定控制器同时负责
// 匹配与卷插件相关的 Provision/Delete 操作，两者共用同一轮同步节奏。
type PersistentVolumeBinderControllerConfiguration struct {
	// pvClaimBinderSyncPeriod 是同步持久卷与持久卷声明的周期；
	// 绑定、供给和删除都在同一轮同步里推进，因此共用一个周期。
	PVClaimBinderSyncPeriod metav1.Duration
	// volumeConfiguration holds configuration for volume related features.
	VolumeConfiguration VolumeConfiguration
}

// PodGCControllerConfiguration contains elements describing PodGCController.
//
// 阈值按已终止 Pod 的数量触发清理，而不是按存活时间，
// 因为目标是限制遗留对象总量；小于等于 0 表示关闭该清理器。
type PodGCControllerConfiguration struct {
	// terminatedPodGCThreshold 是已终止 pod 垃圾回收器开始删除前可存在的已终止 Pod 数量；
	// 按数量而不是按时间触发，目标是限制遗留对象总量；小于等于 0 表示关闭。
	TerminatedPodGCThreshold int32
}

// ReplicaSetControllerConfiguration contains elements describing ReplicaSetController.
//
// 并发数限制同时处理的 ReplicaSet 数量，
// 提高后副本数调整更及时，代价是更高的 API 调用频率。
type ReplicaSetControllerConfiguration struct {
	// concurrentRSSyncs 是允许并发同步的 replica set 数量：数量越大副本管理越及时，
	// 但 CPU（及网络）负载也越高。提高并发可让更多 ReplicaSet 及时补齐副本，代价是更多 API 调用。
	ConcurrentRSSyncs int32
}

// ReplicationControllerConfiguration contains elements describing ReplicationController.
//
// 与 ReplicaSet 分开配置，是因为两者由不同的控制器实现，
// 复制控制器仍需为旧工作负载保留，不能假设它的参数会随 ReplicaSet 一起调整。
type ReplicationControllerConfiguration struct {
	// concurrentRCSyncs 是允许并发同步的 replication controller 数量：数量越大副本管理越及时，
	// 但 CPU（及网络）负载也越高。与 ReplicaSet 控制器各自限速，因为两者处理的是不同的工作负载对象。
	ConcurrentRCSyncs int32
}

// ResourceQuotaControllerConfiguration contains elements describing ResourceQuotaController.
//
// 配额用量是周期性重算的，而不是在每个请求上同步更新，
// 这样可以把大量 Pod 变更合并成较少的用量写回；代价是统计存在短暂延迟。
// 并发数控制同时处理的配额对象数量，避免大规模命名空间下集中重算。
type ResourceQuotaControllerConfiguration struct {
	// resourceQuotaSyncPeriod is the period for syncing quota usage status
	// in the system.
	ResourceQuotaSyncPeriod metav1.Duration
	// concurrentResourceQuotaSyncs 是允许并发同步的 resource quota 数量：数量越大配额管理越及时，
	// 但 CPU（及网络）负载也越高。提高并发可让更多命名空间的配额状态及时重算，代价是更多 API 调用。
	ConcurrentResourceQuotaSyncs int32
}

// SAControllerConfiguration contains elements describing ServiceAccountController.
//
// 签名密钥用于签发 ServiceAccount token，根 CA 证书则被放进 token secret
// 供 Pod 验证 API Server，两者用途不同，因此分别配置。
// 根 CA 必须是合法的 PEM bundle，否则注入的 secret 对客户端没有实际信任价值。
type SAControllerConfiguration struct {
	// serviceAccountKeyFile is the filename containing a PEM-encoded private RSA key
	// used to sign service account tokens.
	ServiceAccountKeyFile string
	// concurrentSATokenSyncs is the number of service account token syncing operations
	// that will be done concurrently.
	ConcurrentSATokenSyncs int32
	// rootCAFile 是会被包含进 ServiceAccount token secret 的根证书颁发机构，供 Pod 校验
	// API Server，必须是合法的 PEM 编码 CA bundle。
	RootCAFile string
}

// TTLAfterFinishedControllerConfiguration contains elements describing TTLAfterFinishedController.
//
// 该控制器按 TTL 清理已完成的 Job 等对象，避免历史执行记录无限堆积；
// 并发数决定同时执行清理的 worker 数量。
type TTLAfterFinishedControllerConfiguration struct {
	// concurrentTTLSyncs is the number of TTL-after-finished collector workers that are
	// allowed to sync concurrently.
	ConcurrentTTLSyncs int32
}

// ValidatingAdmissionPolicyStatusControllerConfiguration contains elements describing ValidatingAdmissionPolicyStatusController.
//
// 该控制器把策略的类型检查结果回写为状态，
// 让声明了策略的用户能看到编译是否通过，而不必等到实际请求被拒绝才知道。
// 并发数主要影响类型检查的吞吐，提高后状态更新更快但会占用更多 CPU。
type ValidatingAdmissionPolicyStatusControllerConfiguration struct {
	// ConcurrentPolicySyncs 是允许并发同步的 policy 对象数量，默认值为 5：数量越大类型检查越快，
	// 但 CPU（及网络）负载也越高，能让策略状态尽快反映声明是否可用。
	ConcurrentPolicySyncs int32
}

// DeviceTaintEvictionControllerConfiguration contains elements configuring the device taint eviction controller.
//
// 设备污点控制器在 DRA 设备不可用时驱逐受影响 Pod，
// 并更新 ResourceClaim 状态以反映设备分配的变化。
// 并发数限制这两类操作的并行量：默认值偏向保守，避免设备批量故障时
// 同时驱逐过多 Pod 造成二次冲击。
type DeviceTaintEvictionControllerConfiguration struct {
	// ConcurrentSyncs 是并发执行的操作数量（删除 pod、更新 ResourceClaim 状态等），默认为 10：
	// 数量越大处理越快，但 CPU（及网络）负载也越高。默认并发数不同，是因为设备污点控制器只在设备异常时工作，
	// 而 ResourceClaim 控制器负责所有 claim 的常规生命周期。
	ConcurrentSyncs int32
}

// ResourceClaimControllerConfiguration 包含配置 resource claim 控制器的各个要素。
// ResourceClaim 控制器负责 DRA 资源声明的完整生命周期，
// 工作范围覆盖集群中所有 claim，因此默认并发数高于只处理特定事件的设备污点控制器。
// 并发数限制删除 Pod、更新状态等操作的并行量，防止 DRA 组件集中操作时压垮 API Server。
type ResourceClaimControllerConfiguration struct {
	// ConcurrentSyncs 是并发执行的操作数量（删除 pod、更新 ResourceClaim 状态等），默认为 50：
	// 数量越大处理越快，但 CPU（及网络）负载也越高。默认并发数不同，是因为设备污点控制器只在设备异常时工作，
	// 而 ResourceClaim 控制器负责所有 claim 的常规生命周期。
	ConcurrentSyncs int32
}
