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

package features

import (
	apiextensionsfeatures "k8s.io/apiextensions-apiserver/pkg/features"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	genericfeatures "k8s.io/apiserver/pkg/features"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientfeatures "k8s.io/client-go/features"
	"k8s.io/component-base/featuregate"
	zpagesfeatures "k8s.io/component-base/zpages/features"
	kcmfeatures "k8s.io/controller-manager/pkg/features"
)

// 每个功能门控都应在此按如下模板添加条目：
//
// // owner: @username
// // kep: https://kep.k8s.io/NNN
// MyFeature featuregate.Feature = "MyFeature"
//
// 功能门控应按区分大小写的字母顺序排列
// （大写字母排在小写字母之前）。这样能降低代码冲突的风险，
// 因为改动更可能分散在
// 文件的不同位置。
const (
	// owner: @aojea
	//
	// 允许 kubelet 在没有任何 Node IP、仅有
	// DNS 名称的情况下申请证书。
	AllowDNSOnlyNodeCSR featuregate.Feature = "AllowDNSOnlyNodeCSR"

	// owner: @micahhausler
	//
	// 将 AllowInsecureKubeletCertificateSigningRequests 设为 true 会禁用节点准入环节对
	// kubelet 签名方（CN=system:node:$nodeName）的 CSR 校验。
	AllowInsecureKubeletCertificateSigningRequests featuregate.Feature = "AllowInsecureKubeletCertificateSigningRequests"

	// owner: @HirazawaUi
	//
	// 在软驱逐中允许用 MaxPodGracePeriodSeconds 覆盖 spec.terminationGracePeriodSeconds。
	AllowOverwriteTerminationGracePeriodSeconds featuregate.Feature = "AllowOverwriteTerminationGracePeriodSeconds"

	// owner: @adrianmoisey
	// kep: https://kep.k8s.io/5707
	//
	// 启用后，kube-proxy 会为 Service.spec.externalIPs 编写 iptables/nftables/ipvs 规则。
	// 禁用时，kube-proxy 不会为 externalIPs 编写规则，等同于关闭这一
	// 已废弃特性。
	AllowServiceExternalIPs featuregate.Feature = "AllowServiceExternalIPs"

	// owner: @gavinkflam
	// kep: https://kep.k8s.io/5936
	//
	// 新增用户字段，用于控制 atomic write 卷文件属主 UID
	AtomicWriteVolumeUserFields featuregate.Feature = "AtomicWriteVolumeUserFields"

	// owner: @seans3
	// kep: http://kep.k8s.io/4006
	//
	// 强制对 exec、attach、portforward 等 Pod 子资源执行 "create" 动词的授权。
	// 参见：https://github.com/kubernetes/kubernetes/issues/133515
	AuthorizePodWebsocketUpgradeCreatePermission featuregate.Feature = "AuthorizePodWebsocketUpgradeCreatePermission"

	// owner: @szuecs
	//
	// 允许节点修改 CPUCFSQuotaPeriod
	CPUCFSQuotaPeriod featuregate.Feature = "CustomCPUCFSQuotaPeriod"

	// owner: @fromanirh
	// beta：见下文。
	//
	// 允许微调 cpumanager 策略，即实验性的 alpha 质量选项
	// 依据 https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// 我们要避免功能门控泛滥。该门控：
	// - 将守护一组质量为 alpha 的 cpumanager 选项。
	// - 永远不会晋升到 beta 或 stable。
	// 参见 https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// 了解该门控移除的细节。
	CPUManagerPolicyAlphaOptions featuregate.Feature = "CPUManagerPolicyAlphaOptions"

	// owner: @fromanirh
	// beta：见下文。
	//
	// 允许微调 cpumanager 策略，即实验性的 beta 质量选项
	// 依据 https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// 我们要避免功能门控泛滥。该门控：
	// - 将守护一组质量为 beta 的 cpumanager 选项。
	// - 因此*引入*即为 beta
	// - 永远不会晋升到 stable。
	// 参见 https://groups.google.com/g/kubernetes-sig-architecture/c/Nxsc7pfe5rw/m/vF2djJh0BAAJ
	// 了解该门控移除的细节。
	CPUManagerPolicyBetaOptions featuregate.Feature = "CPUManagerPolicyBetaOptions"

	// owner: @ffromani
	//
	// 允许使用选项微调 cpumanager 策略。
	CPUManagerPolicyOptions featuregate.Feature = "CPUManagerPolicyOptions"

	// owner: @bitoku
	// kep: https://kep.k8s.io/5825
	//
	// 启用对 CRI list 操作使用流式 RPC。
	CRIListStreaming featuregate.Feature = "CRIListStreaming"

	// owner: @aramase
	// kep:  http://kep.k8s.io/5538
	//
	// 允许 CSI 驱动选择通过 NodePublishVolumeRequest 中
	// 专用的 secrets 字段（而非 volume_context 字段）接收 kubelet 提供的服务账号令牌。
	CSIServiceAccountTokenSecrets featuregate.Feature = "CSIServiceAccountTokenSecrets"

	// owner: @fengzixu
	//
	// 允许 kubelet 检测 CSI 卷状态，并把异常卷事件发送给正在使用该卷的对应 Pod。
	CSIVolumeHealth featuregate.Feature = "CSIVolumeHealth"

	// owner: @HirazawaUi
	//
	// 启用该门控会导致 Pod 状态因 kubelet 重启而发生变化。
	ChangeContainerStatusOnKubeletRestart featuregate.Feature = "ChangeContainerStatusOnKubeletRestart"

	// owner: @sanposhiho @wojtek-t
	// kep: https://kep.k8s.io/5278
	//
	// 在 Pod 绑定到节点时清除 pod.Status.NominatedNodeName。
	// 避免过期信息影响外部调度组件。
	ClearingNominatedNodeNameAfterBinding featuregate.Feature = "ClearingNominatedNodeNameAfterBinding"

	// owner: @ahmedtd
	//
	// 启用 ClusterTrustBundle 对象及与 Kubelet 的集成。
	ClusterTrustBundle featuregate.Feature = "ClusterTrustBundle"

	// owner: @ahmedtd
	//
	// 启用 ClusterTrustBundle 的 Kubelet 投射卷。依赖 ClusterTrustBundle。
	ClusterTrustBundleProjection featuregate.Feature = "ClusterTrustBundleProjection"

	// owner: @tosi3k
	// kep: https://kep.k8s.io/6012
	//
	// 启用对 CompositePodGroups 的支持。
	CompositePodGroup featuregate.Feature = "CompositePodGroup"

	// owner: @adrianreber
	// kep: https://kep.k8s.io/2008
	//
	// 在 kubelet 中启用容器 Checkpoint 支持
	ContainerCheckpoint featuregate.Feature = "ContainerCheckpoint"

	// onwer: @yuanwang04
	// kep: https://kep.k8s.io/5307
	//
	// 支持容器重启策略及容器重启策略规则，以覆盖 Pod 的重启策略。
	// 允许单个容器重启，即使 Pod 的重启策略为 "Never"。
	ContainerRestartRules featuregate.Feature = "ContainerRestartRules"

	// owner: @sreeram-venkitesh
	//
	// 允许通过容器生命周期配置自定义停止信号
	ContainerStopSignals featuregate.Feature = "ContainerStopSignals"

	// owner: @jefftree
	// kep: https://kep.k8s.io/4355
	//
	// 在 API server 中启用协调式 leader election
	CoordinatedLeaderElection featuregate.Feature = "CoordinatedLeaderElection"

	// owner: @ttakahashi21 @mkimuram
	// kep: https://kep.k8s.io/3294
	//
	// 允许从其他命名空间的快照创建 PVC
	CrossNamespaceVolumeDataSource featuregate.Feature = "CrossNamespaceVolumeDataSource"

	// owner: @ritazh
	// kep: http://kep.k8s.io/5018
	//
	// 启用在 ResourceClaim 中申请管理员访问权限的支持。
	// 即使设备已被占用，管理员访问仍会被授予，并且
	// 取决于 DRA 驱动，在容器使用所分配设备时
	// 可能启用额外的权限。
	DRAAdminAccess featuregate.Feature = "DRAAdminAccess"

	// owner: @sunya-ch
	// kep: https://kep.k8s.io/5075
	//
	// DRAConsumableCapacity：启用按可消耗容量进行资源分配的支持。
	DRAConsumableCapacity featuregate.Feature = "DRAConsumableCapacity"

	// owner: @gauravkghildiyal
	// kep: http://kep.k8s.io/6080
	//
	// 启用对动态资源分配（DRA）中派生属性的支持。
	DRADerivedAttributes featuregate.Feature = "DRADerivedAttributes"

	// owner: @KobayashiD27
	// kep: http://kep.k8s.io/5007
	// alpha：v1.34
	//
	// 启用对延迟绑定那些依赖带绑定条件设备的 Pod 的支持
	//
	// 还需要启用 DRAResourceClaimDeviceStatus
	// 。
	DRADeviceBindingConditions featuregate.Feature = "DRADeviceBindingConditions"

	// owner: @omeryahud
	// kep: https://kep.k8s.io/5963
	//
	// 允许驱动在 ResourceSlice 的每个
	// device.consumesCounters[] 条目上声明不透明的兼容性组。调度器随后
	// 仅在声明的组有交集时，才把来自同一计数器集合的设备共同分配，
	// 从而把不兼容共分配（例如同一物理设备上的 GPU MIG 与 vGPU）的检测
	// 从准备阶段失败
	// 前移到调度阶段拒绝。
	//
	// 还必须启用 DRAPartitionableDevices，因为该字段只存在于
	// 可分区设备才有的 consumesCounters[] 条目上。
	DRADeviceCompatibilityGroups featuregate.Feature = "DRADeviceCompatibilityGroups"

	// owner: @pohly
	// kep: http://kep.k8s.io/5055
	//
	// DeviceTaintRules 允许管理员给设备添加污点。
	DRADeviceTaintRules featuregate.Feature = "DRADeviceTaintRules"

	// owner: @pohly
	// kep: http://kep.k8s.io/5055
	//
	// 给设备打污点可阻止新 Pod 使用它们，和/或
	// 让正在使用它们的 Pod 停止。用户可以选择容忍污点。
	DRADeviceTaints featuregate.Feature = "DRADeviceTaints"

	// owner: @yliaog
	// kep: http://kep.k8s.io/5004
	//
	// 启用对由 DRA 支撑的扩展资源请求的支持。
	DRAExtendedResource featuregate.Feature = "DRAExtendedResource"

	// owner: @sunya-ch
	// kep: https://kep.k8s.io/5075
	//
	// 允许 CapacityRequestPolicyRange 的
	// min、max 和 step 字段使用小数（毫单位）值。
	DRAFractionalCapacityRange featuregate.Feature = "DRAFractionalCapacityRange"

	// owner: @everpeace
	// kep: http://kep.k8s.io/5491
	//
	// 在 ResourceSlice 中为 DRA 设备启用列表类型属性
	// 并扩展 ResourceClaim 的 matchAttribute/distinctAttribute
	// 语义，使其能够处理列表类型属性。
	DRAListTypeAttributes featuregate.Feature = "DRAListTypeAttributes"

	// owner: @pravk03
	// kep: https://kep.k8s.io/5517
	//
	// 启用对由 DRA 支撑的节点可分配资源的支持。
	DRANodeAllocatableResources featuregate.Feature = "DRANodeAllocatableResources"

	// owner: @troychiu
	// kep: http://kep.k8s.io/5945
	//
	// 启用声明节点本地操作（准备和
	// 清理）对设备可选的支持。
	DRAOptionalNodeOperations featuregate.Feature = "DRAOptionalNodeOperations"

	// owner: @mortent, @cici37
	// kep: http://kep.k8s.io/4815
	//
	// 启用根据调度过程中设备被分配的部分
	// 对设备进行动态分区的支持。
	//
	DRAPartitionableDevices featuregate.Feature = "DRAPartitionableDevices"

	// owner: @nmn3m
	// kep: http://kep.k8s.io/5677
	//
	// 启用 ResourceSlice.Spec.PartitionTypeAttribute，它让
	// 可分区资源池选择 ResourcePoolStatusRequest 的类型化
	// partitionSummary 视图。同时依赖 DRAPartitionableDevices
	// （该字段仅对带 SharedCounters 的池有意义）和
	// DRAResourcePoolStatus（消费该字段的特性）。
	DRAPartitionableDevicesType featuregate.Feature = "DRAPartitionableDevicesType"

	// owner: @mortent
	// kep: http://kep.k8s.io/4816
	//
	// 启用按优先级排列的资源请求列表支持。
	// 将选择第一个能被满足的
	// 条目。
	DRAPrioritizedList featuregate.Feature = "DRAPrioritizedList"

	// owner: @LionelJouin
	// kep: http://kep.k8s.io/4817
	//
	// 启用对 ResourceClaim.status.devices 字段的支持，并允许
	// 由 DRA 驱动设置该状态。
	DRAResourceClaimDeviceStatus featuregate.Feature = "DRAResourceClaimDeviceStatus"

	// owner: @aojea
	// kep: http://kep.k8s.io/4817
	//
	// 对 ResourceClaim 状态更新启用细粒度授权检查。
	// 更新 status.allocation 和 status.reservedFor 需要对
	// resourceclaims/binding 的单独权限；更新 status.devices 需要对
	// resourceclaims/driver 使用 associated-node / arbitrary-node 动词前缀的
	// 按驱动划分的权限。
	DRAResourceClaimGranularStatusAuthorization featuregate.Feature = "DRAResourceClaimGranularStatusAuthorization"

	// owner: @nmn3m
	// kep: http://kep.k8s.io/5677
	//
	// 启用 ResourcePoolStatusRequest API，用于查询 DRA 资源池的
	// 可用性状态。
	DRAResourcePoolStatus featuregate.Feature = "DRAResourcePoolStatus"

	// owner: @pohly
	// kep: http://kep.k8s.io/4381
	//
	// 启用在调度器每节点 Filter 操作超过
	// 一定时间后中止该操作（默认 10 秒，可在 DynamicResources
	// 调度插件配置中调整）。
	DRASchedulerFilterTimeout featuregate.Feature = "DRASchedulerFilterTimeout"

	// owner: @nojnhuh
	// kep: https://kep.k8s.io/5729
	//
	// 启用为整个 PodGroup 预留并复制模板化 ResourceClaim 的支持。
	DRAWorkloadResourceClaims featuregate.Feature = "DRAWorkloadResourceClaims"

	// owner: @VeraQin
	// kep: https://kep.k8s.io/5996
	//
	// 在 Kubelet 配置中启用对 DefaultPodSysctls 的支持。
	DefaultPodSysctls featuregate.Feature = "DefaultPodSysctls"

	// owner: @atiratree
	// kep: http://kep.k8s.io/3973
	//
	// Deployment 和 ReplicaSet 现在也可通过 .status.terminatingReplicas 跟踪正在终止的 Pod。
	DeploymentReplicaSetTerminatingReplicas featuregate.Feature = "DeploymentReplicaSetTerminatingReplicas"

	// owner: @aojea
	//
	// 启用 MultiCIDRServiceAllocator 的 apiserver 为支持从旧的 bitmap ClusterIP 分配器在线迁移到
	// MultiCIDRServiceAllocator 引入的新 IPAddress 分配器，会对两套分配器执行双写。
	// 该门控用于禁用新 Cluster IP 分配器上的双写。
	DisableAllocatorDualWrite featuregate.Feature = "DisableAllocatorDualWrite"

	// owner: @ffromani
	// beta：v1.33
	//
	// 对已分配独占 CPU 的容器禁用 CPU Quota。
	// 对至少包含一个独占 CPU 容器的 Pod 禁用 Pod 级 CPU Quota
	// 容器（init、application、sidecar）在以下条件下分配独占 CPU：
	// （1）cpumanager 策略为 static，
	// （2）Pod 的 QoS 为 Guaranteed，
	// （3）容器有整数 CPU request。
	// 预期行为是对分配了独占 CPU 的容器禁用 CPU Quota。
	// 由于该修复改变了长期存在（但错误）的行为，遇到
	// 回归的用户可用 DisableCPUQuotaWithExclusiveCPUs 门控（默认开启）
	// 恢复旧行为。若不得不使用该门控并遇到问题，请提交 issue。
	// 若无 bug 报告，该门控将在 2 个版本后（1.35）锁定为 true 并移除
	DisableCPUQuotaWithExclusiveCPUs featuregate.Feature = "DisableCPUQuotaWithExclusiveCPUs"

	// owner: @HirazawaUi
	// kep: http://kep.k8s.io/4004
	//
	// DisableNodeKubeProxyVersion 禁用 v1.Node 的 status.nodeInfo.kubeProxyVersion 字段
	DisableNodeKubeProxyVersion featuregate.Feature = "DisableNodeKubeProxyVersion"

	// owner: @pohly
	// kep: http://kep.k8s.io/4381
	//
	// 启用对具有自定义参数且生命周期
	// 独立于 Pod 的资源的支持。资源由调度器基于
	// "结构化参数"完成分配。
	DynamicResourceAllocation featuregate.Feature = "DynamicResourceAllocation"

	// owner: @nispriha
	// kep: https://kep.k8s.io/5502
	//
	// 允许为 emptyDir 卷目录设置自定义 Unix 权限位。
	EmptyDirVolumeMode featuregate.Feature = "EmptyDirVolumeMode"

	// owner: @HirazawaUi
	// kep: http://kep.k8s.io/3721
	//
	// 允许容器从文件中读取环境变量。
	// 环境变量文件必须由 initContainer 生成，且位于 emptyDir 卷内。
	// kubelet 会从 emptyDir 卷中指定的文件
	// 填充容器的环境变量，而无需挂载该文件。
	EnvFiles featuregate.Feature = "EnvFiles"

	// owner: @harche
	// kep: http://kep.k8s.io/3386
	//
	// 允许通过 kubelet 使用事件驱动的 PLEG（Pod 生命周期事件生成器），
	// 避免频繁重新列出容器，有助于优化性能。
	EventedPLEG featuregate.Feature = "EventedPLEG"

	// owner: @atiratree
	// kep: http://kep.k8s.io/4563
	//
	// 启用新的 .spec.evictionResponders Pod 字段以及新的 EvictionRequest 和 Eviction Resources。
	// 供一组请求方与响应方协调 Pod 的优雅驱逐。
	EvictionRequestAPI featuregate.Feature = "EvictionRequestAPI"

	// owner: @andrewsykim @SergeyKanzhelev
	//
	// 确保 kubelet 遵守 exec 探针超时。保留该门控是为了兼容可能
	// 依赖忽略 exec 探针超时这一旧行为的已有工作负载。
	// 已锁定默认值，将在 v1.38 移除。进展见 KEP #1972 更新
	ExecProbeTimeout featuregate.Feature = "ExecProbeTimeout"

	// owner: @seans3
	// kep: http://kep.k8s.io/4006
	//
	// 允许 API server 在 kubelet 声明支持时，为 exec/attach
	// 将 websocket 代理到 kubelet。
	ExtendWebSocketsToKubelet featuregate.Feature = "ExtendWebSocketsToKubelet"

	// owner: @HarshalNeelkamal
	//
	// 启用外部服务账号 JWT 签名与密钥管理。
	// 启用后可传入 --service-account-signing-endpoint 参数来配置外部签名方。
	ExternalServiceAccountTokenSigner featuregate.Feature = "ExternalServiceAccountTokenSigner"

	// owner: @amritansh1502
	// kep: https://kep.k8s.io/4939
	//
	// 启用对 gRPC 健康检查探针的 TLS 支持。
	GRPCContainerProbeTLS featuregate.Feature = "GRPCContainerProbeTLS"

	// owner: @erictune @wojtek-t
	//
	// 启用对通用 Workload API 的支持。
	GenericWorkload featuregate.Feature = "GenericWorkload"

	// owner: @vinayakankugoyal @thockin
	//
	// 控制是否支持 gitRepo 卷插件。
	// KEP #5040 从 v1.33 起默认禁用 gitRepo 卷插件，
	// 该门控为用户提供覆盖此行为的方式。
	GitRepoVolumeDriver featuregate.Feature = "GitRepoVolumeDriver"

	// owner: @bobbypage
	// 支持 kubelet 检测节点关停，并在节点关停前优雅终止 Pod。
	GracefulNodeShutdown featuregate.Feature = "GracefulNodeShutdown"

	// owner: @wzshiming
	// 让 kubelet 在优雅关停时使用基于 Pod 优先级值的配置。
	GracefulNodeShutdownBasedOnPodPriority featuregate.Feature = "GracefulNodeShutdownBasedOnPodPriority"

	// owner @amritansh1502
	// kep: https://kep.k8s.io/5999
	//
	// 启用 HTTPGetAction 的 Protocol 字段，以选择线协议（HTTP/1.1 或 HTTP/2）。
	H2CContainerProbe featuregate.Feature = "H2CContainerProbe"

	// owner: @jm-franc
	// kep: https://kep.k8s.io/4951
	//
	// 启用可配置的 HPA 扩容与缩容容差。
	HPAConfigurableTolerance featuregate.Feature = "HPAConfigurableTolerance"

	// owner: @adrianmoisey
	//
	// 为 HorizontalPodAutoscaler 启用 generation 跟踪
	HPAGeneration featuregate.Feature = "HPAGeneration"

	// owner: @hakuna-matatah
	//
	// 为 HPA 重叠检测启用优化的选择器存储。
	HPAOptimizedSelectorStore featuregate.Feature = "HPAOptimizedSelectorStore"

	// owner: @johanneswuerbach
	// kep: https://kep.k8s.io/2021
	//
	// 启用在配置对象或自定义指标时 HPA 缩容至零 Pod 的支持。
	HPAScaleToZero featuregate.Feature = "HPAScaleToZero"

	// owner: @HirazawaUi
	// kep: https://kep.k8s.io/4762
	//
	// 允许将任意 FQDN 设为 Pod 的 hostname
	HostnameOverride featuregate.Feature = "HostnameOverride"

	// owner: @jingczhang
	// beta：v1.37
	//
	// 从 memory.available 中扣除 hugepage 容量，使上报的
	// memory 反映真实的普通内存可用性。这修正了长期存在的
	// bug：hugepage 预留 RAM 抬高了 AvailableBytes，延迟驱逐。
	// 遇到回归的用户可禁用该门控以恢复旧行为。
	HugepageAwareEviction featuregate.Feature = "HugepageAwareEviction"

	// owner: @haircommander
	// kep: http://kep.k8s.io/4210
	// ImageMaximumGCAge 启用 Kubelet 的同名配置字段，允许管理员
	// 指定镜像在多大年龄后被垃圾回收。
	ImageMaximumGCAge featuregate.Feature = "ImageMaximumGCAge"

	// owner: @saschagrunert
	// kep: https://kep.k8s.io/4639
	//
	// 启用镜像卷数据源。
	ImageVolume featuregate.Feature = "ImageVolume"

	// owner: @iholder101
	// kep: https://kep.k8s.io/4639
	//
	// 启用将 ImageVolume 的 digest 写入 Pod 状态。
	ImageVolumeWithDigest featuregate.Feature = "ImageVolumeWithDigest"

	// owner: @ndixita
	// kep: https://kep.k8s.io/5419
	//
	// 启用在 Pod 级别指定资源。
	InPlacePodLevelResourcesVerticalScaling featuregate.Feature = "InPlacePodLevelResourcesVerticalScaling"

	// owner: @vinaykul,@tallclair
	// kep: http://kep.k8s.io/1287
	//
	// 启用 Pod 原地垂直扩缩容
	InPlacePodVerticalScaling featuregate.Feature = "InPlacePodVerticalScaling"

	// owner: @tallclair @esotsal
	//
	// 允许对使用整数 CPU request 的 Guaranteed Pod 中的容器调整资源（默认 false）。
	// 仅在启用了 InPlacePodVerticalScaling 与 CPU Manager 特性，且
	// 设置了 CPU Manager Static Policy 的节点上生效。
	InPlacePodVerticalScalingExclusiveCPUs featuregate.Feature = "InPlacePodVerticalScalingExclusiveCPUs"

	// owner: @tallclair @pkrishn
	//
	// 当 Memory Manager 策略为 Static 时，允许调整 Guaranteed Pod 中容器的内存（默认 false）。
	// 仅在启用了 InPlacePodVerticalScaling 与 Memory Manager 特性的节点上生效。
	InPlacePodVerticalScalingExclusiveMemory featuregate.Feature = "InPlacePodVerticalScalingExclusiveMemory"

	// owner: @natasha41575
	//
	// 允许对正在运行的非 sidecar init 容器进行原地 Pod 调整。
	InPlacePodVerticalScalingInitContainers featuregate.Feature = "InPlacePodVerticalScalingInitContainers"

	// owner: @natasha41575
	// kep: https://kep.k8s.io/6030
	//
	// 允许原地更新由内存支撑的卷大小限制。
	InPlacePodVerticalScalingMemoryBackedVolumes featuregate.Feature = "InPlacePodVerticalScalingMemoryBackedVolumes"

	// owner: @natasha41575
	// kep: https://kep.k8s.io/5836
	//
	// 为延迟原地垂直扩缩容的 Pod 启用调度器触发的抢占。
	InPlacePodVerticalScalingSchedulerPreemption featuregate.Feature = "InPlacePodVerticalScalingSchedulerPreemption"

	// owner: @tetianakh
	//
	// 当拓扑键为 kubernetes.io/hostname 时，启用 Pod 间亲和性计算的快速路径。
	InterPodAffinityHostnameFastPath featuregate.Feature = "InterPodAffinityHostnameFastPath"

	// owner: @mimowo
	// kep: https://kep.k8s.io/4368
	//
	// 允许把 Job 对象的协调委派给外部控制器。
	JobManagedBy featuregate.Feature = "JobManagedBy"

	// owner: @adrianmoisey @danwinship
	// kep: https://kep.k8s.io/5495
	//
	// 允许在 kube-proxy 中使用 IPVS 模式
	KubeProxyIPVS featuregate.Feature = "KubeProxyIPVS"

	// owner: @austinabro321
	// kep: https://kep.k8s.io/6032
	//
	// 在 nftables kube-proxy 后端启用用户态 TCP 代理，
	// 以在 IPv4 和 IPv6 上服务 localhost NodePort Service。
	KubeProxyNFTablesLocalhostNodePorts featuregate.Feature = "KubeProxyNFTablesLocalhostNodePorts"

	// owner: @tallclair
	//
	// 启用 kubelet 的 allocated pods 端点。
	KubeletAllocatedPodsEndpoint featuregate.Feature = "KubeletAllocatedPodsEndpoint"

	// owner: @marquiz
	// kep: http://kep.k8s.io/4033
	//
	// 启用从 CRI 检测 kubelet cgroup driver 配置选项。
	// CRI 运行时尚需支持该特性，此时
	// kubelet 会忽略 cgroupDriver（--cgroup-driver）
	// 配置项。若运行时不支持，kubelet 将
	// 回退使用其自身的 cgroupDriver 选项。
	KubeletCgroupDriverFromCRI featuregate.Feature = "KubeletCgroupDriverFromCRI"

	// owner: @lauralorenz @hankfreund
	// kep: https://kep.k8s.io/5593
	//
	// 启用可配置的每节点容器重启退避上限，
	// 即处于 CrashLoopBackOff 的容器
	KubeletCrashLoopBackOffMax featuregate.Feature = "KubeletCrashLoopBackOffMax"

	// owner: @stlaz
	// kep: https://kep.k8s.io/2535
	//
	// 启用跟踪镜像拉取凭据，以便为不同租户的镜像访问
	// 授权。
	KubeletEnsureSecretPulledImages featuregate.Feature = "KubeletEnsureSecretPulledImages"

	// owner: @vinayakankugoyal
	// kep: http://kep.k8s.io/2862
	//
	// 为基于 webhook 的授权启用 kubelet API 细粒度授权。
	//
	KubeletFineGrainedAuthz featuregate.Feature = "KubeletFineGrainedAuthz"

	// owner: @AkihiroSuda
	// kep: https://kep.k8s.io/2033
	//
	// 启用在用户命名空间中运行 kubelet 的支持。
	// 用户命名空间必须在运行 kubelet 之前创建。
	// CRI 等所有节点组件都需运行在同一个用户命名空间中。
	KubeletInUserNamespace featuregate.Feature = "KubeletInUserNamespace"

	// KubeletPSI 让 Kubelet 暴露 PSI 指标
	// owner: @roycaihw
	// kep: https://kep.k8s.io/4205
	KubeletPSI featuregate.Feature = "KubeletPSI"

	// owner: @moshe010
	//
	// 启用 POD resources API 返回由动态资源分配的资源
	KubeletPodResourcesDynamicResources featuregate.Feature = "KubeletPodResourcesDynamicResources"

	// owner: @moshe010
	//
	// 启用带 Get 方法的 POD resources API
	KubeletPodResourcesGet featuregate.Feature = "KubeletPodResourcesGet"

	// owner: @ffromani
	// 已废弃：v1.34
	//
	// issue：https://github.com/kubernetes/kubernetes/issues/119423
	// 禁用 podresources API list 端点的受限输出。
	// "受限"输出仅包含实际运行的 Pod，因为它们
	// 才占用资源。事实证明这原本就是预期行为，参见：
	// https://github.com/kubernetes/kubernetes/pull/79409#issuecomment-505975671
	// 该行为后来丢失，并且与 memory manager 的交互产生了
	// 无法修复的 bug：端点返回客户端无法过滤的虚假陈旧信息，
	// 因为 API 未提供足够上下文。参见：
	// https://github.com/kubernetes/kubernetes/issues/132020
	// 该端点长期返回额外信息，但这些信息
	// 对该 API 的用途同样无用。尽管如此，我们仍要改变一个长期存在
	// 但有 bug 的行为，遇到回归的用户可使用
	// KubeletPodResourcesListUseActivePods 门控（默认开启）恢复旧行为。
	// 若不得不使用该门控并遇到问题，请提交 issue。
	// 若无 bug 报告，该门控将在 4 个版本后（1.38）锁定为 true，并在 1.39 移除
	// 若无 bug 报告。
	KubeletPodResourcesListUseActivePods featuregate.Feature = "KubeletPodResourcesListUseActivePods"

	// owner: @hoskeri
	//
	// 恢复旧行为：节点创建返回 403 Forbidden 时 Kubelet 自注册失败。
	KubeletRegistrationGetOnExistsOnly featuregate.Feature = "KubeletRegistrationGetOnExistsOnly"

	// owner: @kannon92
	// kep: https://kep.k8s.io/4191
	//
	// 镜像文件系统拆分特性让 kubelet 能对部署在
	// 不同文件系统上的镜像（只读层）和/或容器（可写层）
	// 执行垃圾回收。
	KubeletSeparateDiskGC featuregate.Feature = "KubeletSeparateDiskGC"

	// owner: @aramase
	// kep: http://kep.k8s.io/4412
	//
	// 允许 kubelet 把绑定到正在拉取镜像的 Pod 的服务账号令牌
	// 发送给 credential provider 插件。
	KubeletServiceAccountTokenForCredentialProviders featuregate.Feature = "KubeletServiceAccountTokenForCredentialProviders"

	// owner: @sallyom
	// kep: https://kep.k8s.io/2832
	//
	// 在 kubelet 中添加对分布式追踪的支持
	KubeletTracing featuregate.Feature = "KubeletTracing"

	// owner: @RobertKrawitz
	//
	// 允许使用文件系统进行临时存储监控。
	// 仅在设置 LocalStorageCapacityIsolation 时生效。
	// 依赖 UserNamespacesSupport 特性，因此设置默认值时应与其保持一致。
	LocalStorageCapacityIsolationFSQuotaMonitoring featuregate.Feature = "LocalStorageCapacityIsolationFSQuotaMonitoring"

	// owner: @sanposhiho
	// kep: https://kep.k8s.io/3633
	//
	// 在 PodAffinity 和 PodAntiAffinity 中启用 MatchLabelKeys 与 MismatchLabelKeys。
	MatchLabelKeysInPodAffinity featuregate.Feature = "MatchLabelKeysInPodAffinity"

	// owner: @denkensk
	// kep: https://kep.k8s.io/3243
	//
	// 在 PodTopologySpread 中启用 MatchLabelKeys。
	MatchLabelKeysInPodTopologySpread featuregate.Feature = "MatchLabelKeysInPodTopologySpread"

	// owner: @mochizuki875
	// kep: https://kep.k8s.io/3243
	//
	// 启用把键值标签合并到 PodTopologySpread 中 MatchLabelKeys 对应的 LabelSelector。
	MatchLabelKeysInPodTopologySpreadSelectorMerge featuregate.Feature = "MatchLabelKeysInPodTopologySpreadSelectorMerge"

	// owner: @krmayankk
	// kep: https://kep.k8s.io/961
	//
	// 为 StatefulSet 启用 maxUnavailable
	MaxUnavailableStatefulSet featuregate.Feature = "MaxUnavailableStatefulSet"

	// owner: @xiaoxubeii
	// kep: https://kep.k8s.io/2570
	//
	// 启用 kubelet 通过 cgroups v2 支持 memory QoS。
	MemoryQoS featuregate.Feature = "MemoryQoS"

	// owner: @aojea
	// kep: https://kep.k8s.io/1880
	//
	// 启用 Service IP 范围的动态配置
	MultiCIDRServiceAllocator featuregate.Feature = "MultiCIDRServiceAllocator"

	// owner: torredil
	// kep: https://kep.k8s.io/4876
	//
	// 使 CSINode.Spec.Drivers[*].Allocatable.Count 可变，允许 CSI 驱动
	// 更新节点上可分配的卷数量
	MutableCSINodeAllocatableCount featuregate.Feature = "MutableCSINodeAllocatableCount"

	// owner: huww98
	// kep: https://kep.k8s.io/5381
	//
	// 使 PersistentVolume.Spec.NodeAffinity 可变，允许 CSI 驱动
	// 在数据迁移时更新拓扑信息
	MutablePVNodeAffinity featuregate.Feature = "MutablePVNodeAffinity"

	// owner: @kannon92
	// kep: https://kep.k8s.io/5440
	//
	// 为暂停的 Job 启用可变的 Pod 资源，无论其之前是否已启动。
	MutablePodResourcesForSuspendedJobs featuregate.Feature = "MutablePodResourcesForSuspendedJobs"

	// owner: @mimowo
	// kep: https://kep.k8s.io/5440
	//
	// 为暂停的 Job 启用可变的调度指令，无论其之前是否已启动。
	MutableSchedulingDirectivesForSuspendedJobs featuregate.Feature = "MutableSchedulingDirectivesForSuspendedJobs"

	// owner: @aojea
	//
	// 允许 kube-proxy 直接使用 netlink 执行 nftables 操作，
	// 而不执行 nft 命令行程序。
	NFTablesNetlink featuregate.Feature = "NFTablesNetlink"

	// owner: @danwinship
	// kep: https://kep.k8s.io/3866
	//
	// 允许以 `--mode nftables` 运行 kube-proxy。
	NFTablesProxyMode featuregate.Feature = "NFTablesProxyMode"

	// owner: @michaelasp
	//
	// Node Lifecycle Controller 的门控，确保标记节点不健康前 Lease 对象确实已过期。
	NodeControllerLeaseCircuitBreaker featuregate.Feature = "NodeControllerLeaseCircuitBreaker"

	// owner: @pravk03, @tallclair
	// kep: https://kep.k8s.io/5328
	//
	// 启用由 Kubelet 填充的 NodeStatus 中的 DeclaredFeatures API，并启用使用该 API 的调度器过滤器。
	NodeDeclaredFeatures featuregate.Feature = "NodeDeclaredFeatures"

	// owner: @kerthcet
	// kep: https://kep.k8s.io/3094
	//
	// 允许用户指定在计算 Pod 拓扑分布倾斜时
	// 是否考虑 nodeAffinity/nodeTaint。
	NodeInclusionPolicyInPodTopologySpread featuregate.Feature = "NodeInclusionPolicyInPodTopologySpread"

	// owner: @rthallisey
	// kep: https://kep.k8s.io/5683
	//
	// 为生命周期状态启用已知的 Node 生命周期 condition。
	NodeLifecycleConditions featuregate.Feature = "NodeLifecycleConditions"

	// owner: @aravindhp @LorbusChris
	// kep: http://kep.k8s.io/2271
	//
	// 启用通过 /logs 端点查询节点服务日志。启用该特性有安全影响。
	// 建议仅在调试需要时启用，其他情况下禁用。
	NodeLogQuery featuregate.Feature = "NodeLogQuery"

	// owner: @iholder101 @kannon92
	// kep: https://kep.k8s.io/2400
	//
	// 允许 kubelet 在启用 swap 的情况下运行。
	NodeSwap featuregate.Feature = "NodeSwap"

	// owner: @sanposhiho, @wojtek-t
	// kep: https://kep.k8s.io/5278
	//
	// 扩展 NominatedNodeName 字段以表达预期的 Pod 放置，使
	// 调度器和外部组件（如 Cluster Autoscaler、Karpenter、Kueue）
	// 能共享 Pod 放置意图。这有助于组件间更好协调，
	// 避免不合适的节点缩容，并帮助调度器
	// 在重启后恢复工作。
	NominatedNodeNameForExpectation featuregate.Feature = "NominatedNodeNameForExpectation"

	// owner: @bwsalmon
	// kep: https://kep.k8s.io/5598
	//
	// 在调度器中启用机会式批处理。
	OpportunisticBatching featuregate.Feature = "OpportunisticBatching"

	// owner: @tallclair
	//
	// 启用按需重新列出单个 Pod。
	PLEGOnDemandRelist featuregate.Feature = "PLEGOnDemandRelist"

	// owner: @ArvindParekh
	// kep: https://kep.k8s.io/5541
	//
	// 为 PersistentVolumeClaim 状态添加 Unused condition，标识 PVC 上次被 Pod 使用的时间。
	PersistentVolumeClaimUnusedSinceTime featuregate.Feature = "PersistentVolumeClaimUnusedSinceTime"

	// owner: @haircommander
	// kep: https://kep.k8s.io/2364
	//
	// 配置 Kubelet 使用 CRI 填充 Pod 和容器统计信息，而非用 cAdvisor 的数据补充。
	// 要求 CRI 实现支持提供所需统计信息。
	PodAndContainerStatsFromCRI featuregate.Feature = "PodAndContainerStatsFromCRI"

	// owner: @ahmedtd
	// kep: https://kep.k8s.io/4317
	//
	// 启用 PodCertificateRequest 对象及 podCertificate 投射卷数据源。
	PodCertificateRequest featuregate.Feature = "PodCertificateRequest"

	// owner: @ahg-g
	//
	// 启用在 ReplicaSet 缩容时控制 Pod 排序。
	PodDeletionCost featuregate.Feature = "PodDeletionCost"

	// owner: @wojtek-t @argh4k
	// kep: https://kep.k8s.io/5710
	//
	// 启用在 podgroup 级别指定 PreemptionPolicy。
	PodGroupPreemptionPolicy featuregate.Feature = "PodGroupPreemptionPolicy"

	// owner: @KevinTMtz
	// kep: https://kep.k8s.io/5526
	//
	// PodLevelResourceManagers 在 Topology、CPU 和 Memory manager 中启用 Pod 级资源管理。
	// 该特性依赖 PodLevelResources 特性。
	PodLevelResourceManagers featuregate.Feature = "PodLevelResourceManagers"

	// owner: @ndixita
	// key: https://kep.k8s.io/2837
	//
	// 启用在 Pod 级别指定资源。
	PodLevelResources featuregate.Feature = "PodLevelResources"

	// owner: @ndixita
	//
	// 启用 PodLevelResourcesFixDefaulting。
	PodLevelResourcesFixDefaulting featuregate.Feature = "PodLevelResourcesFixDefaulting"

	// owner: @KevinTMtz
	// key: https://kep.k8s.io/2837
	//
	// 修复对 QoS 类别中 nil/空 Pod 级资源值的处理。
	PodLevelResourcesFixKubeletQOSClass featuregate.Feature = "PodLevelResourcesFixKubeletQOSClass"

	// owner: @knight42
	// kep: https://kep.k8s.io/3288
	//
	// 启用仅获取容器的 stdout 或 stderr。
	PodLogsQuerySplitStreams featuregate.Feature = "PodLogsQuerySplitStreams"

	// owner: @natasha41575
	// kep: http://kep.k8s.io/5067
	//
	// 允许 Pod 上报 status.ObservedGeneration，以反映上次观察到的 podspec 的 generation。
	PodObservedGenerationTracking featuregate.Feature = "PodObservedGenerationTracking"

	// owner: @ddebroy, @kannon92, @Priyankasaggu11929
	// kep: https://kep.k8s.io/3085, https://kep.k8s.io/4138
	//
	// 启用在 Pod 状态中上报 PodReadyToStartContainersCondition condition，
	// 上报时机为 Pod sandbox 创建、网络配置、卷挂载以及
	// （如需要）动态资源分配完成之后。
	PodReadyToStartContainersCondition featuregate.Feature = "PodReadyToStartContainersCondition"

	// owner: @Huang-Wei
	// kep: https://kep.k8s.io/3521
	//
	// 允许用户指定 Pod 何时可被调度。
	PodSchedulingReadiness featuregate.Feature = "PodSchedulingReadiness"

	// owner: @munnerz
	// kep: https://kep.k8s.io/4742
	// alpha: v1.33
	// beta: v1.35
	//
	// 启用 PodTopologyLabelsAdmission 准入插件，它通过复制
	// 已分配 Node 对象（在待准入的 Binding 中）的
	// `topology.kubernetes.io/{zone,region}` 标签到 Binding 来修改 `pod/binding` 请求，
	// 从而在 Pod 被调度时把标签持久化到 Pod 对象上。
	// 这让 Pod 中运行的工作负载能了解所分配节点的拓扑信息。
	// 启用该特性还允许外部调度器在调度 Pod 时，通过设置所提交 Binding 的
	// `metadata.labels` 字段，以原子操作方式为 Pod 设置标签，
	// 与 `metadata.annotations` 的行为类似。
	PodTopologyLabelsAdmission featuregate.Feature = "PodTopologyLabelsAdmission"

	// owner: @briansonnenberg
	// kep: https://kep.k8s.io/4188
	//
	// 启用 PodsAPI 特性，通过 kubelet 上的 gRPC API 暴露 Pod 信息。
	PodsAPI featuregate.Feature = "PodsAPI"

	// owner: @seans3
	// kep: http://kep.k8s.io/4006
	//
	// 允许使用 websocket 客户端代理 PortForward
	PortForwardWebsockets featuregate.Feature = "PortForwardWebsockets"

	// owner: @danwinship
	// kep: https://kep.k8s.io/3015
	//
	// 为 trafficDistribution 启用 PreferSameZone 和 PreferSameNode 值
	PreferSameTrafficDistribution featuregate.Feature = "PreferSameTrafficDistribution"

	// owner: @jessfraz
	//
	// 启用对容器 ProcMountType 的控制。
	ProcMountType featuregate.Feature = "ProcMountType"

	// owner: @sjenning
	//
	// 允许在 QoS 级别预留资源，防止较低 QoS 级别的 Pod
	// 抢占较高 QoS 级别请求的资源（目前仅内存）
	QOSReserved featuregate.Feature = "QOSReserved"

	// owner: @gnufied
	// kep: https://kep.k8s.io/1790
	//
	// 允许用户从卷扩容失败中恢复
	RecoverVolumeExpansionFailure featuregate.Feature = "RecoverVolumeExpansionFailure"

	// owner: @AkihiroSuda
	// kep: https://kep.k8s.io/3857
	//
	// 允许递归只读挂载。
	RecursiveReadOnlyMounts featuregate.Feature = "RecursiveReadOnlyMounts"

	// owner: @lauralorenz
	// kep: https://kep.k8s.io/4603
	//
	// 启用更低的内部集群级重启退避上限，
	// 即处于 CrashLoopBackOff 的容器
	ReduceDefaultCrashLoopBackOffDecay featuregate.Feature = "ReduceDefaultCrashLoopBackOffDecay"

	// owner: @HirazawaUi
	// kep: https://kep.k8s.io/4369
	//
	// 允许环境变量使用几乎所有可打印 ASCII 字符
	RelaxedEnvironmentVariableValidation featuregate.Feature = "RelaxedEnvironmentVariableValidation"

	// owner: @adrianmoisey
	// kep: https://kep.k8s.io/5311
	//
	// 放宽 DNS 搜索字符串校验。
	RelaxedServiceNameValidation featuregate.Feature = "RelaxedServiceNameValidation"

	// owner: @harshaln
	//
	// 启用一种 kubelet 配置：从文件路径监控并加载 ClientCA
	ReloadKubeletClientCAFile = "ReloadKubeletClientCAFile"

	// owner: @zhangweikop
	//
	// 当指定证书文件变化时，允许 kubelet tls server 更新证书。
	// 在 kubelet 配置中指定 tlsCertFile 和 tlsPrivateKeyFile 时该特性很有用。
	// 对其他情况（如使用 serverTLSbootstrap）无影响。
	ReloadKubeletServerCertificateFile featuregate.Feature = "ReloadKubeletServerCertificateFile"

	// owner: @SergeyKanzhelev
	// kep: https://kep.k8s.io/4680
	//
	// 在容器状态中添加 AllocatedResourcesStatus。
	ResourceHealthStatus featuregate.Feature = "ResourceHealthStatus"

	// owner: @SergeyKanzhelev
	// kep: https://kep.k8s.io/4680
	//
	// 为 AllocatedResourcesStatus 条目添加消息。
	ResourceHealthStatusMessage featuregate.Feature = "ResourceHealthStatusMessage"

	// owner: @yuanwang04
	// kep: https://kep.k8s.io/5532
	//
	// 在同一节点上原地重启 Pod。
	RestartAllContainersOnContainerExits featuregate.Feature = "RestartAllContainersOnContainerExits"

	// owner: @mikedanese
	//
	// 从 Certificate Signing Request API 为 kubelet 获取服务端证书，
	// 而非生成自签名证书，并在证书即将过期时
	// 自动轮换。
	RotateKubeletServerCertificate featuregate.Feature = "RotateKubeletServerCertificate"

	// owner: @kiashok
	// kep: https://kep.k8s.io/4216
	//
	// 支持根据指定的 runtime class 拉取镜像。
	RuntimeClassInImageCriAPI featuregate.Feature = "RuntimeClassInImageCriApi"

	// owner: @jsafrane
	// kep: https://kep.k8s.io/1710
	//
	// 通过以正确的 SELinux 标签挂载卷来加速容器启动，
	// 而不是递归修改卷上每个文件。
	// 在 SELinuxMount 门控启用前，启用 PodSecurityContext 中的 SELinuxChangePolicy 字段。
	SELinuxChangePolicy featuregate.Feature = "SELinuxChangePolicy"

	// owner: @jsafrane
	// kep: https://kep.k8s.io/1710
	// 通过以正确的 SELinux 标签挂载卷来加速容器启动，
	// 而不是递归修改卷上每个文件。
	SELinuxMount featuregate.Feature = "SELinuxMount"

	// owner: @jsafrane
	// kep: https://kep.k8s.io/1710
	// 通过以正确的 SELinux 标签挂载卷来加速容器启动，
	// 而不是递归修改卷上每个文件。
	// 初始实现聚焦 ReadWriteOncePod 卷。
	SELinuxMountReadWriteOncePod featuregate.Feature = "SELinuxMountReadWriteOncePod"

	// owner: @macsko
	// kep: http://kep.k8s.io/5229
	//
	// 通过引入 kube-scheduler 全局的调用处理方式，使调度期间的所有 API 调用异步化。
	SchedulerAsyncAPICalls featuregate.Feature = "SchedulerAsyncAPICalls"

	// owner: @sanposhiho
	// kep: http://kep.k8s.io/4832
	//
	// 把调度器抢占中的一些昂贵操作异步执行，
	// 从而改善涉及抢占时的调度延迟。
	SchedulerAsyncPreemption featuregate.Feature = "SchedulerAsyncPreemption"

	// owner: @macsko
	// kep: http://kep.k8s.io/5142
	//
	// 改进调度队列行为：activeQ 为空时从 backoffQ 中弹出 Pod。
	// 这样可尽快处理可能可调度的 Pod，消除退避队列带来的惩罚效应。
	SchedulerPopFromBackoffQ featuregate.Feature = "SchedulerPopFromBackoffQ"

	// owner: @geetasg
	// kep: https://kep.k8s.io/6132
	//
	// 启用 PreQueueingHint 扩展点，以在事件到达时缩小 Pod 评估范围。
	SchedulerPreQueueingHints featuregate.Feature = "SchedulerPreQueueingHints"

	// owner: @atosatto @yuanchen8911
	// kep: http://kep.k8s.io/3902
	//
	// 将执行基于污点的 Pod 驱逐的 Taint Eviction Controller 与 Node Lifecycle Controller 解耦。
	SeparateTaintEvictionController featuregate.Feature = "SeparateTaintEvictionController"

	// owner: @aramase
	// kep: https://kep.k8s.io/4412
	//
	// ServiceAccountNodeAudienceRestriction 用于限制 kubelet
	// 可申请服务账号令牌的 audience。
	ServiceAccountNodeAudienceRestriction featuregate.Feature = "ServiceAccountNodeAudienceRestriction"

	// owner: @munnerz
	// kep: http://kep.k8s.io/4193
	//
	// 控制是否在生成的服务账号令牌中嵌入 JTI（UUID），以及是否把这些 JTI
	// 记录到审计日志，供这些令牌后续请求使用。
	ServiceAccountTokenJTI featuregate.Feature = "ServiceAccountTokenJTI"

	// owner: @munnerz
	// kep: http://kep.k8s.io/4193
	//
	// 控制 apiserver 是否支持把服务账号令牌绑定到 Node 对象。
	ServiceAccountTokenNodeBinding featuregate.Feature = "ServiceAccountTokenNodeBinding"

	// owner: @munnerz
	// kep: http://kep.k8s.io/4193
	//
	// 控制 apiserver 是否校验服务账号令牌中的 Node claim。
	ServiceAccountTokenNodeBindingValidation featuregate.Feature = "ServiceAccountTokenNodeBindingValidation"

	// owner: @munnerz
	// kep: http://kep.k8s.io/4193
	//
	// 控制 apiserver 在签发绑定到 Pod 对象的服务账号令牌时，是否嵌入关联节点的
	// 名称和 uid。
	ServiceAccountTokenPodNodeInfo featuregate.Feature = "ServiceAccountTokenPodNodeInfo"

	// owner: @jpbetz
	//
	// 启用后，apiserver 会忽略对 ServiceCIDR 根资源写入时 status 字段的变更。
	ServiceCIDRStatusFieldWiping featuregate.Feature = "ServiceCIDRStatusFieldWiping"

	// owner: @michaelasp
	// kep: http://kep.k8s.io/5647
	//
	// 让 DaemonSet 控制器在对同一对象执行协调前，
	// 能读取自己刚写入的数据。
	StaleControllerConsistencyDaemonSet featuregate.Feature = "StaleControllerConsistencyDaemonSet"

	// owner: @michaelasp
	// kep: http://kep.k8s.io/5647
	//
	// 让 HPA 控制器在对同一对象执行协调前，
	// 能读取自己刚写入的数据。
	StaleControllerConsistencyHPA featuregate.Feature = "StaleControllerConsistencyHPA"

	// owner: @michaelasp
	// kep: http://kep.k8s.io/5647
	//
	// 让 Job 控制器在对同一对象执行协调前，
	// 能读取自己刚写入的数据。
	StaleControllerConsistencyJob featuregate.Feature = "StaleControllerConsistencyJob"

	// owner: @michaelasp
	// kep: http://kep.k8s.io/5647
	//
	// 让 ReplicaSet 控制器在对同一对象执行协调前，
	// 能读取自己刚写入的数据。
	StaleControllerConsistencyReplicaSet featuregate.Feature = "StaleControllerConsistencyReplicaSet"

	// owner: @michaelasp
	// kep: http://kep.k8s.io/5647
	//
	// 让 StatefulSet 控制器在对同一对象执行协调前，
	// 能读取自己刚写入的数据。
	StaleControllerConsistencyStatefulSet featuregate.Feature = "StaleControllerConsistencyStatefulSet"

	// owner: @helayoty @galal-hussein
	// kep: http://kep.k8s.io/3541
	//
	// 引入重建 StatefulSet 的新更新策略，类似
	// Deployment 的 Recreate 策略。
	StatefulSetRecreateStrategy featuregate.Feature = "StatefulSetRecreateStrategy"

	// owner: @liggitt
	//
	// 缓解因 controller revision 比较不匹配而导致的 StatefulSet 虚假滚动更新，
	// 这些不匹配在语义上并不重要（如序列化差异或缺失的默认值字段）。
	StatefulSetSemanticRevisionComparison featuregate.Feature = "StatefulSetSemanticRevisionComparison"

	// owner: @cupnes
	// kep: https://kep.k8s.io/4049
	//
	// 启用通过 StorageCapacityScoring 门控
	// 按可用存储容量对节点打分。
	StorageCapacityScoring featuregate.Feature = "StorageCapacityScoring"

	// owner: @ahutsunshine
	//
	// 允许在 apiserver 缓存中为命名空间作用域资源建立索引，以加速 list 操作。
	// 已被 BtreeWatchCache 取代。
	StorageNamespaceIndex featuregate.Feature = "StorageNamespaceIndex"

	// owner: @enj, @michaelasp
	// kep: https://kep.k8s.io/4192
	//
	// 启用对 StorageVersionMigrator 控制器的支持。
	StorageVersionMigrator featuregate.Feature = "StorageVersionMigrator"

	// owner: @danwinship
	// kep: https://kep.k8s.io/4858
	//
	// 对 API 对象中的 IP 地址和 CIDR 值启用更严格的校验。
	StrictIPCIDRValidation featuregate.Feature = "StrictIPCIDRValidation"

	// owner: @everpeace
	// kep: https://kep.k8s.io/3619
	//
	// 在 PodSecurityContext 中启用 SupplementalGroupsPolicy 特性
	SupplementalGroupsPolicy featuregate.Feature = "SupplementalGroupsPolicy"

	// owner: @zhifei92
	//
	// 为 kubelet 启用 systemd watchdog。启用后 kubelet 会
	// 定期通知 systemd watchdog 表明自己仍在运行。
	// 这可避免在 kubelet 无响应时系统重启它，
	// 但该门控默认启用，仅应在系统支持并正确配置了
	// systemd watchdog 特性时使用。
	SystemdWatchdog = featuregate.Feature("SystemdWatchdog")

	// owner: @helayoty
	// kep: https://kep.k8s.io/5471
	//
	// 为 toleration 启用数值比较运算符（Lt、Gt），以匹配基于阈值的污点值。
	TaintTolerationComparisonOperators featuregate.Feature = "TaintTolerationComparisonOperators"

	// owner: @44past4
	// kep: https://kep.k8s.io/5732
	//
	// 在 kube-scheduler 及相关 PodGroup API 字段中启用拓扑感知工作负载调度特性。
	// 启用后，调度器会尝试 Pod 组的多种放置方式并选择最佳方案。
	TopologyAwareWorkloadScheduling featuregate.Feature = "TopologyAwareWorkloadScheduling"

	// owner: @PiotrProkop
	// kep: https://kep.k8s.io/3545
	//
	// 允许使用 alpha 选项微调 topology manager 策略。
	// 该门控：
	// - 将守护一组质量为 alpha 的 topology manager 选项。
	// - 永远不会晋升到 beta 或 stable。
	TopologyManagerPolicyAlphaOptions featuregate.Feature = "TopologyManagerPolicyAlphaOptions"

	// owner: @PiotrProkop
	// kep: https://kep.k8s.io/3545
	//
	// 允许使用 beta 选项微调 topology manager 策略。
	// 该门控：
	// - 将守护一组质量为 beta 的 topology manager 选项。
	// - 因此*引入*即为 beta
	// - 永远不会晋升到 stable。
	TopologyManagerPolicyBetaOptions featuregate.Feature = "TopologyManagerPolicyBetaOptions"

	// owner: @PiotrProkop
	// kep: https://kep.k8s.io/3545
	//
	// 允许使用选项微调 topology manager 策略。
	TopologyManagerPolicyOptions featuregate.Feature = "TopologyManagerPolicyOptions"

	// owner: @seans3
	// kep: http://kep.k8s.io/4006
	//
	// 启用 StreamTranslator 代理处理 RemoteCommand 子协议中
	// 支持 "close" 信号版本的 WebSockets 升级请求。
	TranslateStreamCloseWebsocketRequests featuregate.Feature = "TranslateStreamCloseWebsocketRequests"

	// owner: @HirazawaUi
	// kep: https://kep.k8s.io/5607
	//
	// 允许 hostNetwork Pod 使用用户命名空间
	UserNamespacesHostNetworkSupport featuregate.Feature = "UserNamespacesHostNetworkSupport"

	// owner: @rata, @giuseppe
	// kep: https://kep.k8s.io/127
	//
	// 为无状态 Pod 启用用户命名空间支持。
	UserNamespacesSupport featuregate.Feature = "UserNamespacesSupport"

	// owner: @mattcarry, @sunnylovestiramisu
	// kep: https://kep.k8s.io/3751
	//
	// 允许为持久卷指定用户自定义属性，如 iops 和 throughput。
	VolumeAttributesClass featuregate.Feature = "VolumeAttributesClass"

	// owner: @nispriha
	// kep: https://kep.k8s.io/5855
	//
	// 在 volumeMounts 上启用绑定挂载选项（noexec、nodev、nosuid）。
	VolumeBindMountOptions featuregate.Feature = "VolumeBindMountOptions"

	// owner: @gnufied
	// kep: https://kep.k8s.io/5030
	// alpha: v1.35
	// beta：v1.37
	//
	// 为 CSI 驱动启用卷数量限制扩展，使调度器能
	// 与 cluster-autoscaler 在存储限制上更好协调。
	VolumeLimitScaling featuregate.Feature = "VolumeLimitScaling"

	// owner: @ksubrmnn
	//
	// 允许 kube-proxy 为 Windows 创建 DSR 负载均衡器
	WinDSR featuregate.Feature = "WinDSR"

	// owner: @ksubrmnn
	//
	// 允许 kube-proxy 在 Windows 上以 Overlay 模式运行
	WinOverlay featuregate.Feature = "WinOverlay"

	// owner: @jsturtevant
	// kep: https://kep.k8s.io/4885
	//
	// 通过 CPUManager、MemoryManager 和 Topology manager 为 Windows 节点添加 CPU 与内存亲和性支持
	WindowsCPUAndMemoryAffinity featuregate.Feature = "WindowsCPUAndMemoryAffinity"

	// owner: @zylxjtu
	// kep: https://kep.k8s.io/4802
	//
	// 启用 Windows 节点优雅关停支持。
	WindowsGracefulNodeShutdown featuregate.Feature = "WindowsGracefulNodeShutdown"

	// owner: @marosset
	// kep: https://kep.k8s.io/3503
	//
	// 启用把 Windows 容器加入宿主网络命名空间的支持。
	WindowsHostNetwork featuregate.Feature = "WindowsHostNetwork"

	// owner: @helayoty @mm4tt @wojtek-t
	// kep: https://kep.k8s.io/5547
	//
	// 启用 Job 控制器为符合条件的工作负载自动创建 Workload 和 PodGroup
	// 对象，以支持 gang scheduling。
	WorkloadWithJob featuregate.Feature = "WorkloadWithJob"
)

// defaultVersionedKubernetesFeatureGates 包含所有已知的 Kubernetes 专有功能键及其 VersionedSpecs。
// 添加新特性时，在 pkg/features/kube_features.go 中定义键并在此登记。这些特性将在
// 所有 Kubernetes 二进制中可用。
// 对于仅通过 apiserver、cloud-controller-manager 等特定组件提供的特性，
// 需找到对应的 kube_features.go 文件
// （如 staging/src/apiserver/pkg/features/kube_features.go），在其中定义版本化
// 功能门控，并在本文件中引用。
// 为支持 n-3 版本兼容，特性在毕业 3 个版本后才可移除。
//
// 功能门控的注册入口见本文件末尾的 init 函数，它将版本化定义与依赖关系一次性加入默认门控。
//
// 条目按字母顺序排列。
var defaultVersionedKubernetesFeatureGates = map[featuregate.Feature]featuregate.VersionedSpecs{
	AllowDNSOnlyNodeCSR: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Deprecated},
	},

	AllowInsecureKubeletCertificateSigningRequests: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Deprecated},
	},

	AllowOverwriteTerminationGracePeriodSeconds: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Deprecated, LockToDefault: true}, // 在 1.38 移除
	},

	AllowServiceExternalIPs: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
	},

	AtomicWriteVolumeUserFields: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	AuthorizePodWebsocketUpgradeCreatePermission: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	CPUCFSQuotaPeriod: {
		{Version: version.MustParse("1.12"), Default: false, PreRelease: featuregate.Alpha},
		// 完整背景参见 https://github.com/kubernetes/kubernetes/pull/136339
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA}, // 在 1.37 锁定默认值，在 1.40 移除
	},

	CPUManagerPolicyAlphaOptions: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
	},

	CPUManagerPolicyBetaOptions: {
		{Version: version.MustParse("1.23"), Default: true, PreRelease: featuregate.Beta},
	},

	CPUManagerPolicyOptions: {
		{Version: version.MustParse("1.22"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.23"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 移除
	},

	CRIListStreaming: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	CSIServiceAccountTokenSecrets: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 毕业为 GA；在 1.39 移除
	},

	CSIVolumeHealth: {
		{Version: version.MustParse("1.21"), Default: false, PreRelease: featuregate.Alpha},
	},

	ChangeContainerStatusOnKubeletRestart: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Deprecated},
	},

	ClearingNominatedNodeNameAfterBinding: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	ClusterTrustBundle: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA}, // 在 1.38 锁定默认值
	},

	ClusterTrustBundleProjection: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA}, // 在 1.38 锁定默认值
	},

	CompositePodGroup: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	ContainerCheckpoint: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
	},

	ContainerRestartRules: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	ContainerStopSignals: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
	},

	CrossNamespaceVolumeDataSource: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRAAdminAccess: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 毕业为 GA；在 1.39 移除
	},

	DRAConsumableCapacity: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	DRADerivedAttributes: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRADeviceBindingConditions: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	DRADeviceCompatibilityGroups: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRADeviceTaintRules: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Beta},                    // 依赖一个默认关闭的 beta API。
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: false}, // 在 1.38 设置 LockToDefault: true；在 1.41 移除
	},

	DRADeviceTaints: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: false}, // 在 1.38 设置 LockToDefault: true；在 1.41 移除
	},

	DRAExtendedResource: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 毕业为 GA；在 1.40 移除
	},

	DRAFractionalCapacityRange: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta, MinCompatibilityVersion: version.MustParse("1.37")},
	},

	DRAListTypeAttributes: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRANodeAllocatableResources: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRAOptionalNodeOperations: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRAPartitionableDevices: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	DRAPartitionableDevicesType: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRAPrioritizedList: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
		// 在 1.40 中彻底移除。
	},

	DRAResourceClaimDeviceStatus: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.40 移除
	},

	DRAResourceClaimGranularStatusAuthorization: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	DRAResourcePoolStatus: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	DRASchedulerFilterTimeout: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	DRAWorkloadResourceClaims: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Beta},
	},

	DefaultPodSysctls: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	DeploymentReplicaSetTerminatingReplicas: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	DisableAllocatorDualWrite: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 MultiCIDRServiceAllocator 毕业为 GA 后移除
	},

	DisableCPUQuotaWithExclusiveCPUs: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Deprecated, LockToDefault: true}, // 在 1.38 移除
	},

	DisableNodeKubeProxyVersion: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Deprecated, LockToDefault: true}, // 在 1.36 锁定默认值，并在 v1.39 移除
	},

	DynamicResourceAllocation: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
		// TODO (https://github.com/kubernetes/kubernetes/issues/134459): remove completely in 1.38
	},

	EmptyDirVolumeMode: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	EnvFiles: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},
	EventedPLEG: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
	},

	EvictionRequestAPI: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	ExecProbeTimeout: {
		{Version: version.MustParse("1.20"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 v1.38 移除
	},

	ExtendWebSocketsToKubelet: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	ExternalServiceAccountTokenSigner: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	GRPCContainerProbeTLS: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	GenericWorkload: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Beta},
	},

	GitRepoVolumeDriver: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Deprecated, LockToDefault: true},
	},

	GracefulNodeShutdown: {
		{Version: version.MustParse("1.20"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.21"), Default: true, PreRelease: featuregate.Beta},
	},

	GracefulNodeShutdownBasedOnPodPriority: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.24"), Default: true, PreRelease: featuregate.Beta},
	},

	H2CContainerProbe: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	HPAConfigurableTolerance: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.40 移除
	},

	HPAGeneration: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	HPAOptimizedSelectorStore: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	HPAScaleToZero: {
		{Version: version.MustParse("1.16"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	HostnameOverride: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	HugepageAwareEviction: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	ImageMaximumGCAge: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.38 移除
	},

	ImageVolume: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 毕业为 GA 并锁定默认值，在 1.39 移除
	},

	ImageVolumeWithDigest: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},

	InPlacePodLevelResourcesVerticalScaling: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	InPlacePodVerticalScaling: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.38 移除
	},

	InPlacePodVerticalScalingExclusiveCPUs: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
	},

	InPlacePodVerticalScalingExclusiveMemory: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
	},

	InPlacePodVerticalScalingInitContainers: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.40 移除
	},

	InPlacePodVerticalScalingMemoryBackedVolumes: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	InPlacePodVerticalScalingSchedulerPreemption: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	InterPodAffinityHostnameFastPath: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	JobManagedBy: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.38 移除
	},

	KubeProxyIPVS: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Deprecated},
	},

	KubeProxyNFTablesLocalhostNodePorts: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	KubeletAllocatedPodsEndpoint: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	KubeletCgroupDriverFromCRI: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	KubeletCrashLoopBackOffMax: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	KubeletEnsureSecretPulledImages: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	KubeletFineGrainedAuthz: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	KubeletInUserNamespace: {
		{Version: version.MustParse("1.22"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	KubeletPSI: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	KubeletPodResourcesDynamicResources: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	KubeletPodResourcesGet: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	KubeletPodResourcesListUseActivePods: {
		{Version: version.MustParse("1.0"), Default: false, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Deprecated}, // 在 1.38 锁定默认值，在 1.39 移除
	},

	KubeletRegistrationGetOnExistsOnly: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Deprecated},
	},

	KubeletSeparateDiskGC: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
	},

	KubeletServiceAccountTokenForCredentialProviders: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	KubeletTracing: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.27"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	LocalStorageCapacityIsolationFSQuotaMonitoring: {
		{Version: version.MustParse("1.15"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
	},

	MatchLabelKeysInPodAffinity: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	MatchLabelKeysInPodTopologySpread: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.27"), Default: true, PreRelease: featuregate.Beta},
	},

	MatchLabelKeysInPodTopologySpreadSelectorMerge: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	MaxUnavailableStatefulSet: {
		{Version: version.MustParse("1.24"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	MemoryQoS: {
		{Version: version.MustParse("1.22"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	MultiCIDRServiceAllocator: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除（在 1.34 已锁定默认值）
	},

	MutableCSINodeAllocatableCount: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.38 移除
	},

	MutablePVNodeAffinity: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},

	MutablePodResourcesForSuspendedJobs: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	MutableSchedulingDirectivesForSuspendedJobs: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	NFTablesNetlink: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	NFTablesProxyMode: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	NodeControllerLeaseCircuitBreaker: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	NodeDeclaredFeatures: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	NodeInclusionPolicyInPodTopologySpread: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.26"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	NodeLifecycleConditions: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	NodeLogQuery: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.39 移除，在 1.36 已锁定默认值
	},

	NodeSwap: {
		{Version: version.MustParse("1.22"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	NominatedNodeNameForExpectation: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	OpportunisticBatching: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	PLEGOnDemandRelist: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 v1.40 移除
	},

	PersistentVolumeClaimUnusedSinceTime: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	PodAndContainerStatsFromCRI: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Beta},
	},

	PodCertificateRequest: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA}, // 在 1.38 锁定默认值
	},

	PodDeletionCost: {
		{Version: version.MustParse("1.21"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.22"), Default: true, PreRelease: featuregate.Beta},
	},

	PodGroupPreemptionPolicy: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	PodLevelResourceManagers: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Beta},
	},

	PodLevelResources: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	PodLevelResourcesFixDefaulting: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	PodLevelResourcesFixKubeletQOSClass: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	PodLogsQuerySplitStreams: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
	},

	PodObservedGenerationTracking: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.35 毕业为 GA，在 1.38 移除
	},

	PodReadyToStartContainersCondition: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.29"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 毕业为 GA，在 1.40 移除
	},

	PodSchedulingReadiness: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.27"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.30 毕业为 GA；在 1.32 移除
	},

	PodTopologyLabelsAdmission: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	PodsAPI: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	PortForwardWebsockets: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
	},

	PreferSameTrafficDistribution: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	ProcMountType: {
		{Version: version.MustParse("1.12"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.39 移除
	},

	QOSReserved: {
		{Version: version.MustParse("1.11"), Default: false, PreRelease: featuregate.Alpha},
	},

	RecoverVolumeExpansionFailure: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.34 毕业为 GA；在 1.37 移除
	},

	RecursiveReadOnlyMounts: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 移除
	},

	ReduceDefaultCrashLoopBackOffDecay: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
	},

	RelaxedEnvironmentVariableValidation: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	RelaxedServiceNameValidation: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.40 移除
	},

	ReloadKubeletClientCAFile: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	ReloadKubeletServerCertificateFile: {
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
	},

	ResourceHealthStatus: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	ResourceHealthStatusMessage: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	RestartAllContainersOnContainerExits: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	RotateKubeletServerCertificate: {
		{Version: version.MustParse("1.7"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.12"), Default: true, PreRelease: featuregate.Beta},
	},

	RuntimeClassInImageCriAPI: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
	},

	SELinuxChangePolicy: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.39 移除，在 1.36 已锁定默认值
	},

	SELinuxMount: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA}, // 它在 1.37 毕业为 GA 并默认启用。为稳妥起见在 1.38 锁定。
	},

	SELinuxMountReadWriteOncePod: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.39 移除，在 1.36 已锁定默认值
	},

	SchedulerAsyncAPICalls: {
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Beta},
	},

	SchedulerAsyncPreemption: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	SchedulerPopFromBackoffQ: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},
	SchedulerPreQueueingHints: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	SeparateTaintEvictionController: {
		{Version: version.MustParse("1.29"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除（在 1.34 已锁定默认值）
	},

	ServiceAccountNodeAudienceRestriction: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	ServiceAccountTokenJTI: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	ServiceAccountTokenNodeBinding: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	ServiceAccountTokenNodeBindingValidation: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	ServiceAccountTokenPodNodeInfo: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	ServiceCIDRStatusFieldWiping: {
		{Version: version.MustParse("1.0"), Default: false, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Deprecated},
	},

	StaleControllerConsistencyDaemonSet: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	StaleControllerConsistencyHPA: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	StaleControllerConsistencyJob: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	StaleControllerConsistencyReplicaSet: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	StaleControllerConsistencyStatefulSet: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	StatefulSetRecreateStrategy: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	StatefulSetSemanticRevisionComparison: {
		// 这是对 1.34 回归的缓解措施，其根因是无法通过门控控制的序列化差异，
		// 因此即使通过 --emulation-version 模拟 1.34 之前的版本，也不应自动禁用该缓解措施。
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.Beta},
	},

	StorageCapacityScoring: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	StorageNamespaceIndex: {
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Deprecated},
	},

	StorageVersionMigrator: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA},
	},

	StrictIPCIDRValidation: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	SupplementalGroupsPolicy: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.38 移除
	},

	SystemdWatchdog: {
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	TaintTolerationComparisonOperators: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},

	TopologyAwareWorkloadScheduling: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	TopologyManagerPolicyAlphaOptions: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
	},

	TopologyManagerPolicyBetaOptions: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Beta},
	},

	TopologyManagerPolicyOptions: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.37 移除
	},

	TranslateStreamCloseWebsocketRequests: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
	},

	UserNamespacesHostNetworkSupport: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},

	UserNamespacesSupport: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.39 移除。
	},

	VolumeAttributesClass: {
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	VolumeBindMountOptions: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	VolumeLimitScaling: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	WinDSR: {
		{Version: version.MustParse("1.14"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	WinOverlay: {
		{Version: version.MustParse("1.14"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.20"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	WindowsCPUAndMemoryAffinity: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
	},

	WindowsGracefulNodeShutdown: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	WindowsHostNetwork: {
		{Version: version.MustParse("1.26"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Deprecated},
	},

	WorkloadWithJob: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	apiextensionsfeatures.CRDObservedGenerationTracking: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	apiextensionsfeatures.CRDValidationRatcheting: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.30"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	apiextensionsfeatures.CustomResourceFieldSelectors: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.31"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.32"), Default: true, LockToDefault: true, PreRelease: featuregate.GA},
	},

	genericfeatures.APIResponseCompression: {
		{Version: version.MustParse("1.8"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.16"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.APIServerIdentity: {
		{Version: version.MustParse("1.20"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.26"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.APIServerWebhookAuthenticationToken: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.APIServingWithRoutine: {
		{Version: version.MustParse("1.30"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.AggregatedDiscoveryRemoveBetaType: {
		{Version: version.MustParse("1.0"), Default: false, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Deprecated, LockToDefault: true},
	},

	genericfeatures.AllowParsingUserUIDFromCertAuth: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.AllowUnsafeMalformedObjectDeletion: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.CBORServingAndStorage: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.ConcurrentWatchObjectDecode: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.ConsistentListFromCacheSkipTimeoutFallback: {
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.ConstrainedImpersonation: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.CoordinatedLeaderElection: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
	},

	genericfeatures.DeclarativeValidation: {
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA, LockToDefault: true}, // 在 1.36 毕业为 GA 并锁定默认值，在 1.39 移除
	},

	genericfeatures.DeclarativeValidationBeta: {
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.DeclarativeValidationTakeover: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.37"), Default: false, PreRelease: featuregate.Deprecated, LockToDefault: true},
	},

	genericfeatures.DetectCacheInconsistency: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.EtcdRangeStream: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.ExcludeAdmissionWebhookVirtualResources: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.KMSv1: {
		{Version: version.MustParse("1.0"), Default: true, PreRelease: featuregate.GA},
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.29"), Default: false, PreRelease: featuregate.Deprecated},
	},

	genericfeatures.ListFromCacheSnapshot: {
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.ManifestBasedAdmissionControlConfig: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.MutatingAdmissionPolicy: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.34"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.GA},
	},

	genericfeatures.OpenAPIEnums: {
		{Version: version.MustParse("1.23"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.24"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.RemoteRequestHeaderUID: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.33"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.SeparateCacheWatchRPC: {
		{Version: version.MustParse("1.28"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Deprecated},
		{Version: version.MustParse("1.36"), Default: false, LockToDefault: true, PreRelease: featuregate.Deprecated},
	},

	genericfeatures.ShardedListAndWatch: {
		{Version: version.MustParse("1.36"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.SizeBasedListCostEstimate: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.StorageVersionAPI: {
		{Version: version.MustParse("1.20"), Default: false, PreRelease: featuregate.Alpha},
	},

	genericfeatures.StorageVersionHash: {
		{Version: version.MustParse("1.14"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.15"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.StructuredAuthenticationConfigurationEgressSelector: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.StructuredAuthenticationConfigurationJWKSMetrics: {
		{Version: version.MustParse("1.35"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.TokenRequestServiceAccountUIDValidation: {
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.UnauthenticatedHTTP2DOSMitigation: {
		{Version: version.MustParse("1.25"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.29"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.UnknownVersionInteroperabilityProxy: {
		{Version: version.MustParse("1.28"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.WatchCacheInitializationPostStartHook: {
		{Version: version.MustParse("1.31"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.GA, LockToDefault: true},
	},

	genericfeatures.WatchList: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.32"), Default: true, PreRelease: featuregate.Beta},
		// 将其切回 false，因为 json 和 proto 流式编码器表现更好。
		{Version: version.MustParse("1.33"), Default: false, PreRelease: featuregate.Beta},
		{Version: version.MustParse("1.34"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.WatchListCompression: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	genericfeatures.WebhookRoundTripLoadBalancing: {
		{Version: version.MustParse("1.37"), Default: true, PreRelease: featuregate.Beta},
	},

	kcmfeatures.CloudControllerManagerWatchBasedRoutesReconciliation: {
		{Version: version.MustParse("1.35"), Default: false, PreRelease: featuregate.Alpha},
	},

	kcmfeatures.CloudControllerManagerWebhook: {
		{Version: version.MustParse("1.27"), Default: false, PreRelease: featuregate.Alpha},
	},

	zpagesfeatures.ComponentFlagz: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},

	zpagesfeatures.ComponentStatusz: {
		{Version: version.MustParse("1.32"), Default: false, PreRelease: featuregate.Alpha},
		{Version: version.MustParse("1.36"), Default: true, PreRelease: featuregate.Beta},
	},
}

// defaultKubernetesFeatureGateDependencies 枚举所有存在依赖的功能门控的
// 依赖关系。依赖确保被依赖门控只有在所有依赖都启用时才能开启，
// 并保证更高稳定性级别的特性不能
// 依赖较低稳定性级别的特性。
//
// 条目按字母顺序排列。
var defaultKubernetesFeatureGateDependencies = map[featuregate.Feature][]featuregate.Feature{
	AllowDNSOnlyNodeCSR: {},

	AllowInsecureKubeletCertificateSigningRequests: {},

	AllowOverwriteTerminationGracePeriodSeconds: {},

	AllowServiceExternalIPs: {},

	AtomicWriteVolumeUserFields: {},

	AuthorizePodWebsocketUpgradeCreatePermission: {},

	CPUCFSQuotaPeriod: {},

	CPUManagerPolicyAlphaOptions: {},

	CPUManagerPolicyBetaOptions: {},

	CPUManagerPolicyOptions: {},

	CRIListStreaming: {},

	CSIServiceAccountTokenSecrets: {},

	CSIVolumeHealth: {},

	ChangeContainerStatusOnKubeletRestart: {},

	ClearingNominatedNodeNameAfterBinding: {},

	ClusterTrustBundle: {},

	ClusterTrustBundleProjection: {ClusterTrustBundle},

	CompositePodGroup: {GenericWorkload, TopologyAwareWorkloadScheduling},

	ContainerCheckpoint: {},

	ContainerRestartRules: {},

	ContainerStopSignals: {},

	CoordinatedLeaderElection: {},

	CrossNamespaceVolumeDataSource: {},

	DRAAdminAccess: {DynamicResourceAllocation},

	DRAConsumableCapacity: {DynamicResourceAllocation},

	DRADerivedAttributes: {DynamicResourceAllocation},

	DRADeviceBindingConditions: {DynamicResourceAllocation, DRAResourceClaimDeviceStatus},

	DRADeviceCompatibilityGroups: {DynamicResourceAllocation, DRAPartitionableDevices},

	DRADeviceTaintRules: {DRADeviceTaints}, // DynamicResourceAllocation 是间接依赖。

	DRADeviceTaints: {DynamicResourceAllocation},

	DRAExtendedResource: {DynamicResourceAllocation},

	DRAFractionalCapacityRange: {DRAConsumableCapacity},

	DRAListTypeAttributes: {DynamicResourceAllocation},

	DRANodeAllocatableResources: {DynamicResourceAllocation},

	DRAOptionalNodeOperations: {DynamicResourceAllocation, NodeDeclaredFeatures},

	DRAPartitionableDevices: {DynamicResourceAllocation},

	DRAPartitionableDevicesType: {DynamicResourceAllocation, DRAPartitionableDevices, DRAResourcePoolStatus},

	DRAPrioritizedList: {DynamicResourceAllocation},

	DRAResourceClaimDeviceStatus: {}, // 由于默认开启/关闭的冲突，对 DynamicResourceAllocation 为软依赖。

	DRAResourceClaimGranularStatusAuthorization: {DynamicResourceAllocation, DRAResourceClaimDeviceStatus},

	DRAResourcePoolStatus: {DynamicResourceAllocation},

	DRASchedulerFilterTimeout: {DynamicResourceAllocation},

	DRAWorkloadResourceClaims: {DynamicResourceAllocation, GenericWorkload},

	DefaultPodSysctls: {},

	DeploymentReplicaSetTerminatingReplicas: {},

	DisableAllocatorDualWrite: {MultiCIDRServiceAllocator},

	DisableCPUQuotaWithExclusiveCPUs: {},

	DisableNodeKubeProxyVersion: {},

	DynamicResourceAllocation: {},

	EmptyDirVolumeMode: {},

	EnvFiles: {},

	EventedPLEG: {PLEGOnDemandRelist},

	EvictionRequestAPI: {},

	ExecProbeTimeout: {},

	ExtendWebSocketsToKubelet: {NodeDeclaredFeatures},

	ExternalServiceAccountTokenSigner: {},

	GRPCContainerProbeTLS: {},

	GenericWorkload: {},

	GitRepoVolumeDriver: {},

	GracefulNodeShutdown: {},

	GracefulNodeShutdownBasedOnPodPriority: {GracefulNodeShutdown},

	H2CContainerProbe: {NodeDeclaredFeatures},

	HPAConfigurableTolerance: {},

	HPAGeneration: {},

	HPAOptimizedSelectorStore: {},

	HPAScaleToZero: {},

	HostnameOverride: {},

	HugepageAwareEviction: {},

	ImageMaximumGCAge: {},

	ImageVolume: {},

	ImageVolumeWithDigest: {ImageVolume},

	InPlacePodLevelResourcesVerticalScaling: {InPlacePodVerticalScaling, PodLevelResources, NodeDeclaredFeatures},

	InPlacePodVerticalScaling: {},

	InPlacePodVerticalScalingExclusiveCPUs: {InPlacePodVerticalScaling},

	InPlacePodVerticalScalingExclusiveMemory: {InPlacePodVerticalScaling},

	InPlacePodVerticalScalingInitContainers: {InPlacePodVerticalScaling, NodeDeclaredFeatures},

	InPlacePodVerticalScalingMemoryBackedVolumes: {InPlacePodVerticalScaling, NodeDeclaredFeatures},

	InPlacePodVerticalScalingSchedulerPreemption: {InPlacePodVerticalScaling},

	InterPodAffinityHostnameFastPath: {},

	JobManagedBy: {},

	KubeProxyIPVS: {},

	KubeProxyNFTablesLocalhostNodePorts: {},

	KubeletAllocatedPodsEndpoint: {InPlacePodVerticalScaling},

	KubeletCgroupDriverFromCRI: {},

	KubeletCrashLoopBackOffMax: {},

	KubeletEnsureSecretPulledImages: {},

	KubeletFineGrainedAuthz: {},

	KubeletInUserNamespace: {},

	KubeletPSI: {},

	KubeletPodResourcesDynamicResources: {},

	KubeletPodResourcesGet: {},

	KubeletPodResourcesListUseActivePods: {},

	KubeletRegistrationGetOnExistsOnly: {},

	KubeletSeparateDiskGC: {},

	KubeletServiceAccountTokenForCredentialProviders: {},

	KubeletTracing: {},

	LocalStorageCapacityIsolationFSQuotaMonitoring: {},

	MatchLabelKeysInPodAffinity: {},

	MatchLabelKeysInPodTopologySpread: {},

	MatchLabelKeysInPodTopologySpreadSelectorMerge: {MatchLabelKeysInPodTopologySpread},

	MaxUnavailableStatefulSet: {},

	MemoryQoS: {},

	MultiCIDRServiceAllocator: {},

	MutableCSINodeAllocatableCount: {},

	MutablePVNodeAffinity: {},

	MutablePodResourcesForSuspendedJobs: {},

	MutableSchedulingDirectivesForSuspendedJobs: {},

	NFTablesNetlink: {},

	NFTablesProxyMode: {},

	NodeControllerLeaseCircuitBreaker: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	NodeDeclaredFeatures: {},

	NodeInclusionPolicyInPodTopologySpread: {},

	NodeLifecycleConditions: {},

	NodeLogQuery: {},

	NodeSwap: {},

	NominatedNodeNameForExpectation: {},

	OpportunisticBatching: {},

	PLEGOnDemandRelist: {},

	PersistentVolumeClaimUnusedSinceTime: {},

	PodAndContainerStatsFromCRI: {},

	PodCertificateRequest: {},

	PodDeletionCost: {},

	PodGroupPreemptionPolicy: {GenericWorkload},

	PodLevelResourceManagers: {PodLevelResources},

	PodLevelResources: {},

	PodLevelResourcesFixDefaulting: {PodLevelResources},

	PodLevelResourcesFixKubeletQOSClass: {PodLevelResources},

	PodLogsQuerySplitStreams: {},

	PodObservedGenerationTracking: {},

	PodReadyToStartContainersCondition: {},

	PodSchedulingReadiness: {},

	PodTopologyLabelsAdmission: {},

	PodsAPI: {},

	PortForwardWebsockets: {},

	PreferSameTrafficDistribution: {},

	ProcMountType: {UserNamespacesSupport},

	QOSReserved: {},

	RecoverVolumeExpansionFailure: {},

	RecursiveReadOnlyMounts: {},

	ReduceDefaultCrashLoopBackOffDecay: {},

	RelaxedEnvironmentVariableValidation: {},

	RelaxedServiceNameValidation: {},

	ReloadKubeletClientCAFile: {},

	ReloadKubeletServerCertificateFile: {},

	ResourceHealthStatus: {DynamicResourceAllocation},

	ResourceHealthStatusMessage: {ResourceHealthStatus},

	// RestartAllContainersOnContainerExits 引入一种新的容器重启规则动作。
	// 若未启用 ContainerRestartRules 特性，API 会丢弃所有重启规则。
	RestartAllContainersOnContainerExits: {ContainerRestartRules, NodeDeclaredFeatures},

	RotateKubeletServerCertificate: {},

	RuntimeClassInImageCriAPI: {},

	SELinuxChangePolicy: {},

	SELinuxMount: {},

	SELinuxMountReadWriteOncePod: {},

	SchedulerAsyncAPICalls: {},

	SchedulerAsyncPreemption: {},

	SchedulerPopFromBackoffQ:  {},
	SchedulerPreQueueingHints: {},

	SeparateTaintEvictionController: {},

	ServiceAccountNodeAudienceRestriction: {},

	ServiceAccountTokenJTI: {},

	ServiceAccountTokenNodeBinding: {ServiceAccountTokenNodeBindingValidation},

	ServiceAccountTokenNodeBindingValidation: {},

	ServiceAccountTokenPodNodeInfo: {},

	ServiceCIDRStatusFieldWiping: {},

	StaleControllerConsistencyDaemonSet: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	StaleControllerConsistencyHPA: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	StaleControllerConsistencyJob: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	StaleControllerConsistencyReplicaSet: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	StaleControllerConsistencyStatefulSet: {featuregate.Feature(clientfeatures.AtomicFIFO)},

	StatefulSetRecreateStrategy: {},

	StatefulSetSemanticRevisionComparison: {},

	StorageCapacityScoring: {},

	StorageNamespaceIndex: {},

	StorageVersionMigrator: {},

	StrictIPCIDRValidation: {},

	SupplementalGroupsPolicy: {},

	SystemdWatchdog: {},

	TaintTolerationComparisonOperators: {},

	TopologyAwareWorkloadScheduling: {GenericWorkload},

	TopologyManagerPolicyAlphaOptions: {},

	TopologyManagerPolicyBetaOptions: {},

	TopologyManagerPolicyOptions: {},

	TranslateStreamCloseWebsocketRequests: {},

	UserNamespacesHostNetworkSupport: {UserNamespacesSupport, NodeDeclaredFeatures},

	UserNamespacesSupport: {},

	VolumeAttributesClass: {},

	VolumeBindMountOptions: {},

	VolumeLimitScaling: {},

	WinDSR: {},

	WinOverlay: {},

	WindowsCPUAndMemoryAffinity: {},

	WindowsGracefulNodeShutdown: {GracefulNodeShutdown},

	WindowsHostNetwork: {},

	WorkloadWithJob: {GenericWorkload},

	apiextensionsfeatures.CRDObservedGenerationTracking: {},

	apiextensionsfeatures.CRDValidationRatcheting: {},

	apiextensionsfeatures.CustomResourceFieldSelectors: {},

	genericfeatures.APIResponseCompression: {},

	genericfeatures.APIServerIdentity: {},

	genericfeatures.APIServerWebhookAuthenticationToken: {},

	genericfeatures.APIServingWithRoutine: {},

	genericfeatures.AggregatedDiscoveryRemoveBetaType: {},

	genericfeatures.AllowParsingUserUIDFromCertAuth: {},

	genericfeatures.AllowUnsafeMalformedObjectDeletion: {},

	genericfeatures.CBORServingAndStorage: {},

	genericfeatures.ConcurrentWatchObjectDecode: {},

	genericfeatures.ConsistentListFromCacheSkipTimeoutFallback: {},

	genericfeatures.ConstrainedImpersonation: {},

	genericfeatures.DeclarativeValidation: {},

	genericfeatures.DeclarativeValidationBeta:     {genericfeatures.DeclarativeValidation},
	genericfeatures.DeclarativeValidationTakeover: {genericfeatures.DeclarativeValidation},

	genericfeatures.DetectCacheInconsistency: {},

	genericfeatures.EtcdRangeStream: {},

	genericfeatures.ExcludeAdmissionWebhookVirtualResources: {},

	genericfeatures.KMSv1: {},

	genericfeatures.ListFromCacheSnapshot: {},

	genericfeatures.ManifestBasedAdmissionControlConfig: {},

	genericfeatures.MutatingAdmissionPolicy: {},

	genericfeatures.OpenAPIEnums: {},

	genericfeatures.RemoteRequestHeaderUID: {},

	genericfeatures.SeparateCacheWatchRPC: {},

	genericfeatures.ShardedListAndWatch: {},

	genericfeatures.SizeBasedListCostEstimate: {},

	genericfeatures.StorageVersionAPI: {genericfeatures.APIServerIdentity},

	genericfeatures.StorageVersionHash: {},

	genericfeatures.StructuredAuthenticationConfigurationEgressSelector: {},

	genericfeatures.StructuredAuthenticationConfigurationJWKSMetrics: {},

	genericfeatures.TokenRequestServiceAccountUIDValidation: {},

	genericfeatures.UnauthenticatedHTTP2DOSMitigation: {},

	genericfeatures.UnknownVersionInteroperabilityProxy: {genericfeatures.APIServerIdentity},

	genericfeatures.WatchCacheInitializationPostStartHook: {},

	genericfeatures.WatchList: {},

	genericfeatures.WatchListCompression: {},

	genericfeatures.WebhookRoundTripLoadBalancing: {},

	kcmfeatures.CloudControllerManagerWatchBasedRoutesReconciliation: {},

	kcmfeatures.CloudControllerManagerWebhook: {},

	zpagesfeatures.ComponentFlagz: {},

	zpagesfeatures.ComponentStatusz: {},
}

func init() {
	// 把所有 client-go 特性注册到 kube 的功能门控实例，并让所有 client-go
	// 特性检查都使用 kube 的实例。效果是：对 kube 二进制而言，client-go
	// 特性与其他特性一样接入现有的 --feature-gates 参数，并自动支持现有的特性启用指标与测试覆盖机制。
	// 用 kube 的门控构造适配器，使 client-go 与 kube 共享同一份门控实例。
	ca := &clientAdapter{utilfeature.DefaultMutableFeatureGate}
	// 先注册 client-go 特性。runtime.Must 在注册失败时直接 panic，因为门控缺失会导致二进制行为不正确。
	runtime.Must(clientfeatures.AddVersionedFeaturesToExistingFeatureGates(ca))
	// 替换 client-go 全局门控，让后续 client-go 代码的门控检查都走 kube 实例。
	clientfeatures.ReplaceFeatureGates(ca)

	// 注册 Kubernetes 专有特性及依赖关系。顺序保证依赖门控在检查时已全部存在。
	runtime.Must(utilfeature.DefaultMutableFeatureGate.AddVersioned(defaultVersionedKubernetesFeatureGates))
	runtime.Must(utilfeature.DefaultMutableFeatureGate.AddDependencies(defaultKubernetesFeatureGateDependencies))
	// zpages 特性单独注册，因为它来自 component-base 而非本包的版本化定义。
	runtime.Must(zpagesfeatures.AddFeatureGates(utilfeature.DefaultMutableFeatureGate))
}
