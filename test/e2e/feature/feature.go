/*
Copyright 2023 The Kubernetes Authors.

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

// Package feature 包含供 test/e2e 和/或
// test/e2e_node.
package feature

import (
	"k8s.io/kubernetes/test/e2e/framework"
)

// 请保持列表按字母顺序、区分大小写
// （大写字符排在任何小写字符之前）排列。
var (
	// Owner: sig-api-machinery
	// TODO: 记录该特性（何时在测试中使用此特性）
	APIServerIdentity = framework.WithFeature(framework.ValidFeatures.Add("APIServerIdentity"))

	// Owner: sig-auth
	// TODO: 记录该特性（何时在测试中使用此特性）
	BoundServiceAccountTokenVolume = framework.WithFeature(framework.ValidFeatures.Add("BoundServiceAccountTokenVolume"))

	// Owner: sig-api-machinery
	// 标记使用 CBOR 数据格式进行服务或存储的测试。
	CBOR = framework.WithFeature(framework.ValidFeatures.Add("CBOR"))

	// owning-sig: sig-node
	// kep: https://kep.k8s.io/3570
	// test-infra jobs:
	// - https://testgrid.k8s.io/sig-node-kubelet#kubelet-serial-gce-e2e-cpu-manager
	//
	// 此标签用于需要以下条件的测试：
	// - 只在特定 job 上运行 CPU Manager 测试，即 ci-kubernetes-node-kubelet-serial-cpu-manager 和 pull-kubernetes-node-kubelet-serial-cpu-manager
	CPUManager = framework.WithFeature(framework.ValidFeatures.Add("CPUManager"))

	// Owner: sig-node
	// 标记验证容器 checkpointing 的测试
	CheckpointContainer = framework.WithFeature(framework.ValidFeatures.Add("CheckpointContainer"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	CloudProvider = framework.WithFeature(framework.ValidFeatures.Add("CloudProvider"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	ClusterDowngrade = framework.WithFeature(framework.ValidFeatures.Add("ClusterDowngrade"))

	// Owner: sig-autoscaling
	// TODO: 记录该特性（何时在测试中使用此特性）
	ClusterScaleUpBypassScheduler = framework.WithFeature(framework.ValidFeatures.Add("ClusterScaleUpBypassScheduler"))

	// Owner: sig-autoscaling
	ClusterSizeAutoscalingScaleDown = framework.WithFeature(framework.ValidFeatures.Add("ClusterSizeAutoscalingScaleDown"))

	// Owner: sig-autoscaling
	ClusterSizeAutoscalingScaleUp = framework.WithFeature(framework.ValidFeatures.Add("ClusterSizeAutoscalingScaleUp"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	ClusterUpgrade = framework.WithFeature(framework.ValidFeatures.Add("ClusterUpgrade"))

	// Owner: sig-api-machinery
	// TODO: 记录该特性（何时在测试中使用此特性）
	ComprehensiveNamespaceDraining = framework.WithFeature(framework.ValidFeatures.Add("ComprehensiveNamespaceDraining"))

	// Owner: sig-node
	// 启用配置每容器重启策略和重启策略规则。
	ContainerRestartRules = framework.WithFeature(framework.ValidFeatures.Add("ContainerRestartRules"))

	// Owner: sig-node
	// 启用从容器生命周期为容器配置自定义停止信号。
	ContainerStopSignals = framework.WithFeature(framework.ValidFeatures.Add("ContainerStopSignals"))

	// Owner: sig-api-machinery
	// 标记需要协调 leader election 的测试
	CoordinatedLeaderElection = framework.WithFeature(framework.ValidFeatures.Add("CoordinatedLeaderElection"))

	// Owner: sig-node
	// 带有此特性标记的测试 MUST 在配置了 CRI Proxy 的情况下运行，以便向 kubelet 的 CRI 调用注入错误。
	// 这有助于测试 kubelet 如何处理 CRI 交互中的各种错误情况。
	// test-infra jobs:
	// - pull-kubernetes-node-e2e-cri-proxy-serial（需手动触发）
	// - ci-kubernetes-node-e2e-cri-proxy-serial
	CriProxy = framework.WithFeature(framework.ValidFeatures.Add("CriProxy"))

	// OWNER: sig-node
	// 测试 critical pod admission
	CriticalPod = framework.WithFeature(framework.ValidFeatures.Add("CriticalPod"))

	// OWNER: sig-node
	// 测试 device managers
	DeviceManager = framework.WithFeature(framework.ValidFeatures.Add("DeviceManager"))

	// OWNER: sig-node
	// 测试 device plugins
	DevicePlugin = framework.WithFeature(framework.ValidFeatures.Add("DevicePlugin"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	Downgrade = framework.WithFeature(framework.ValidFeatures.Add("Downgrade"))

	// Owner: sig-node
	// 测试 downward API huge pages
	DownwardAPIHugePages = framework.WithFeature(framework.ValidFeatures.Add("DownwardAPIHugePages"))

	// owning-sig: sig-node
	// kep: https://kep.k8s.io/4381
	// test-infra jobs:
	// - https://testgrid.k8s.io/sig-node-dynamic-resource-allocation
	//
	// 此特性标签用于需要以下条件的测试：
	// - kubelet 支持插件在 /var/lib/kubelet 下的"常规"路径
	// - 容器运行时启用了 CDI（https://github.com/cncf-tags/container-device-interface）
	//   支持，从而可以通过 CRI 字段传递 CDI device ID
	//
	// 此外，测试必须用 framework.WithFeatureGate 标记，以说明
	// 它们对特定 feature gate 及相应 API 组的依赖。
	DynamicResourceAllocation = framework.WithFeature(framework.ValidFeatures.Add("DynamicResourceAllocation"))

	// Owner: sig-storage
	// EmptyDirVolumeMode 用于需要 EmptyDirVolumeMode feature gate 的测试。
	EmptyDirVolumeMode = framework.WithFeature(framework.ValidFeatures.Add("EmptyDirVolumeMode"))

	// Owner: sig-node
	// EnvFiles 用于测试 EnvFiles 特性（基于文件的环境变量）。
	EnvFiles = framework.WithFeature(framework.ValidFeatures.Add("EnvFiles"))

	// Owner: sig-storage
	// TODO: 记录该特性（何时在测试中使用此特性）
	EphemeralStorage = framework.WithFeature(framework.ValidFeatures.Add("EphemeralStorage"))

	// OWNER: sig-node
	// 测试 eviction manager
	Eviction = framework.WithFeature(framework.ValidFeatures.Add("Eviction"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	Example = framework.WithFeature(framework.ValidFeatures.Add("Example"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	ExperimentalResourceUsageTracking = framework.WithFeature(framework.ValidFeatures.Add("ExperimentalResourceUsageTracking"))

	// OWNER: sig-storage
	// 这些测试需要能执行 shell（bash）的 kube-controller-manager。多数 Kubernetes e2e
	// 测试运行时 kube-controller-manager 是没有这种 shell 的 distroless 容器。
	// 如需运行这些测试，请自行构建包含 bash 等所需软件包的镜像。
	// 详情参见 https://github.com/kubernetes/kubernetes/issues/78737
	Flexvolumes = framework.WithFeature(framework.ValidFeatures.Add("Flexvolumes"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	GKENodePool = framework.WithFeature(framework.ValidFeatures.Add("GKENodePool"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	GPUDevicePlugin = framework.WithFeature(framework.ValidFeatures.Add("GPUDevicePlugin"))

	// OWNER: sig-node
	// 测试优雅节点关闭
	GracefulNodeShutdown = framework.WithFeature(framework.ValidFeatures.Add("GracefulNodeShutdown"))

	// OWNER: sig-node
	// 基于 Pod 优先级的 GracefulNodeShutdown
	GracefulNodeShutdownBasedOnPodPriority = framework.WithFeature(framework.ValidFeatures.Add("GracefulNodeShutdownBasedOnPodPriority"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	HAMaster = framework.WithFeature(framework.ValidFeatures.Add("HAMaster"))

	// Owner: sig-autoscaling
	// TODO: 记录该特性（何时在测试中使用此特性）
	HPA = framework.WithFeature(framework.ValidFeatures.Add("HPA"))

	// Owner: sig-node
	HostAccess = framework.WithFeature(framework.ValidFeatures.Add("HostAccess"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	HugePages = framework.WithFeature(framework.ValidFeatures.Add("HugePages"))

	// Owner: sig-network
	// 标记需要 pod 和 service 网络均为双栈的集群的测试。
	IPv6DualStack = framework.WithFeature(framework.ValidFeatures.Add("IPv6DualStack"))

	// Owner: sig-node
	ImageID = framework.WithFeature(framework.ValidFeatures.Add("ImageID"))

	// Owner: sig-node
	// ImageVolume 用于测试 image volume source 特性（https://kep.k8s.io/4639）。
	ImageVolume = framework.WithFeature(framework.ValidFeatures.Add("ImageVolume"))

	// Owner: sig-node
	// InPlacePodVerticalScaling 用于测试原地 Pod 垂直扩缩容（https://kep.k8s.io/1287）。
	InPlacePodVerticalScaling = framework.WithFeature(framework.ValidFeatures.Add("InPlacePodVerticalScaling"))

	// Owner: sig-storage
	// 标记只能在 sigs.k8s.io/kind 集群上运行的测试
	Kind = framework.WithFeature(framework.ValidFeatures.Add("Kind"))

	// Owner: sig-network
	// 标记需要 kube-dns-autoscaler 的测试
	KubeDNSAutoscaler = framework.WithFeature(framework.ValidFeatures.Add("KubeDNSAutoscaler"))

	// Owner: sig-network
	// 标记需要内核支持 nfacct 子系统的测试。
	//（某些发行版的内核不包含该子系统。）
	KubeProxyNFAcct = framework.WithFeature(framework.ValidFeatures.Add("KubeProxyNFAcct"))

	// Owner: sig-node
	// 测试 kubelet drop in KEP
	KubeletConfigDropInDir = framework.WithFeature(framework.ValidFeatures.Add("KubeletConfigDropInDir"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	KubeletCredentialProviders = framework.WithFeature(framework.ValidFeatures.Add("KubeletCredentialProviders"))

	// Owner: sig-node
	// 标记需要启用了 KubeletInUserNamespace 的集群的测试 <https://kep.k8s.io/2033>
	KubeletInUserNamespace = framework.WithFeature(framework.ValidFeatures.Add("KubeletInUserNamespace"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	KubeletSecurity = framework.WithFeature(framework.ValidFeatures.Add("KubeletSecurity"))

	// KubeletSeparateDiskGC（SIG-node，用于测试独立的镜像文件系统 <https://kep.k8s.io/4191>）
	// 这些测试需要节点上配置独立磁盘，且 storage.conf 中配置独立文件系统
	KubeletSeparateDiskGC = framework.WithFeature(framework.ValidFeatures.Add("KubeletSeparateDiskGC"))

	// Owner: sig-storage
	LSCIQuotaMonitoring = framework.WithFeature(framework.ValidFeatures.Add("LSCIQuotaMonitoring"))

	// Owner: sig-network
	// 标记需要实现了 LoadBalancer Service 的云提供商的测试
	LoadBalancer = framework.WithFeature(framework.ValidFeatures.Add("LoadBalancer"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	LocalStorageCapacityIsolationQuota = framework.WithFeature(framework.ValidFeatures.Add("LocalStorageCapacityIsolationQuota"))

	// Owner: sig-network
	// 标记需要 service proxy 支持 localhost NodePort service 的测试
	LocalhostNodePorts = framework.WithFeature(framework.ValidFeatures.Add("LocalhostNodePorts"))

	// owning-sig: sig-node
	// 标记用于锁竞争的破坏性测试
	LockContention = framework.WithFeature(framework.ValidFeatures.Add("LockContention"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	MasterUpgrade = framework.WithFeature(framework.ValidFeatures.Add("MasterUpgrade"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	MemoryManager = framework.WithFeature(framework.ValidFeatures.Add("MemoryManager"))

	// Owner: sig-network
	// 标记需要存在符合规范的
	// NetworkPolicy.networking.k8s.io 实现的测试。
	NetworkPolicy = framework.WithFeature(framework.ValidFeatures.Add("NetworkPolicy"))

	// Owner: sig-network
	// 标记需要可用的外部 DNS 的测试。
	NetworkingDNS = framework.WithFeature(framework.ValidFeatures.Add("Networking-DNS"))

	// Owner: sig-network
	// 标记需要能通过 IPv4 连接互联网的测试
	NetworkingIPv4 = framework.WithFeature(framework.ValidFeatures.Add("Networking-IPv4"))

	// Owner: sig-network
	// 标记需要能通过 IPv6 连接互联网的测试
	NetworkingIPv6 = framework.WithFeature(framework.ValidFeatures.Add("Networking-IPv6"))

	// Owner: sig-network
	// 标记单个会产生潜在破坏性网络流量的测试，
	// 这些流量发生在节点之间。
	NetworkingPerformance = framework.WithFeature(framework.ValidFeatures.Add("Networking-Performance"))

	// Owner: sig-node
	// 测试 node allocatable 校验
	NodeAllocatable = framework.WithFeature(framework.ValidFeatures.Add("NodeAllocatable"))

	// Owner: sig-auth
	// TODO: 记录该特性（何时在测试中使用此特性）
	NodeAuthorizer = framework.WithFeature(framework.ValidFeatures.Add("NodeAuthorizer"))

	// Owner: sig-windows, sig-node
	// kep: https://kep.k8s.io/2271
	// 标记需要 kubelet 配置中 enableSystemLogQuery 设为 true 的测试
	// 该 handler 默认关闭。
	NodeLogQuery = framework.WithFeature(framework.ValidFeatures.Add("NodeLogQuery"))

	// Owner: sig-node
	// 树内的 Node Problem Detect e2e 测试。
	NodeProblemDetector = framework.WithFeature(framework.ValidFeatures.Add("NodeProblemDetector"))

	// Owner: sig-node
	// 旨在验证 oom_score 功能的测试
	OOMScoreAdj = framework.WithFeature(framework.ValidFeatures.Add("OOMScoreAdj"))

	// Owner: sig-network
	// 标记单个测试，该测试用大量 service 测试集群 DNS 性能。
	PerformanceDNS = framework.WithFeature(framework.ValidFeatures.Add("PerformanceDNS"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	PodGarbageCollector = framework.WithFeature(framework.ValidFeatures.Add("PodGarbageCollector"))

	// owner: sig-node
	// 标记 pod 级资源管理器特性的测试，该测试需要
	// 启用 PodLevelResourceManagers feature gate。
	PodLevelResourceManagers = framework.WithFeature(framework.ValidFeatures.Add("PodLevelResourceManagers"))

	// owner: sig-node
	// 标记 pod 级资源特性的测试，该测试需要
	// 启用 PodLevelResources feature gate。
	PodLevelResources = framework.WithFeature(framework.ValidFeatures.Add("PodLevelResources"))

	// Owner: sig-node
	// 标记需要带 PodLogsQuerySplitStreams 的集群的测试
	//（用于测试特定日志流 <https://kep.k8s.io/3288>）
	PodLogsQuerySplitStreams = framework.WithFeature(framework.ValidFeatures.Add("PodLogsQuerySplitStreams"))

	// Owner: sig-api-machinery
	// TODO: 记录该特性（何时在测试中使用此特性）
	PodPriority = framework.WithFeature(framework.ValidFeatures.Add("PodPriority"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	PodReadyToStartContainersCondition = framework.WithFeature(framework.ValidFeatures.Add("PodReadyToStartContainersCondition"))

	// Owner: sig-node
	// 标记使用或消耗 kubelet 本地 Pod Resources API 的测试
	// 参见：KEP 606、2043；参见：pkg/kubelet/apis/podresources/
	PodResourcesAPI = framework.WithFeature(framework.ValidFeatures.Add("PodResourcesAPI"))

	// Owner: sig-node
	// 验证 ProcMount 特性。
	// 与 user namespace 配合使用
	ProcMountType = framework.WithFeature(framework.ValidFeatures.Add("ProcMountType"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	Reboot = framework.WithFeature(framework.ValidFeatures.Add("Reboot"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	RegularResourceUsageTracking = framework.WithFeature(framework.ValidFeatures.Add("RegularResourceUsageTracking"))

	// Owner: sig-node
	// Device Management metrics
	ResourceMetrics = framework.WithFeature(framework.ValidFeatures.Add("ResourceMetrics"))

	// Owner: sig-node
	// Runtime Handler
	RuntimeHandler = framework.WithFeature(framework.ValidFeatures.Add("RuntimeHandler"))

	// Owner: sig-network
	// 标记需要 pod 网络实现支持 Pod 间 SCTP
	// 流量的测试。
	SCTPConnectivity = framework.WithFeature(framework.ValidFeatures.Add("SCTPConnectivity"))

	// Owner: sig-storage
	// 此特性标记需要集群中所有可调度 Linux 节点都启用 SELinux 的测试。
	SELinux = framework.WithFeature(framework.ValidFeatures.Add("SELinux"))

	// Owner: sig-storage
	// 此特性标记需要启用 SELinuxMountReadWriteOncePod feature gate 且**禁用** SELinuxMount 的测试。
	// 这是一个临时特性，用于在 SELinuxMount 禁用时测试 metrics。
	// TODO: 当 SELinuxMount feature gate 默认启用后移除。
	SELinuxMountReadWriteOncePodOnly = framework.WithFeature(framework.ValidFeatures.Add("SELinuxMountReadWriteOncePodOnly"))

	// Owner: sig-scheduling
	// 标记异步抢占（KEP-4832）的测试，需要 `SchedulerAsyncPreemption` feature gate。
	SchedulerAsyncPreemption = framework.WithFeature(framework.ValidFeatures.Add("SchedulerAsyncPreemption"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	SeccompDefault = framework.WithFeature(framework.ValidFeatures.Add("SeccompDefault"))

	// Owner: sig-storage
	// KEP: https://kep.k8s.io/3314
	// 带有此特性标记的测试需要：
	// - 支持 snapshot metadata service 的 CSI driver（例如带 --enable-snapshot-metadata 的 CSI hostpath driver）
	// - 与 CSI driver 一同部署的 external-snapshot-metadata sidecar
	// - 已安装 SnapshotMetadataService CRD（cbt.storage.k8s.io/v1beta1）
	// - 实现了 CapSnapshotMetadata 能力的存储驱动
	SnapshotMetadata = framework.WithFeature(framework.ValidFeatures.Add("snapshotmetadata"))

	// 带有此特性标记的测试需要 kubelet 以 standalone 模式运行（--standalone-mode=true），如下：
	// make test-e2e-node PARALLELISM=1 FOCUS="StandaloneMode" TEST_ARGS='--kubelet-flags="--fail-swap-on=false" --standalone-mode=true'
	// 验证 kubelet 在没有 API server 时行为的测试。
	StandaloneMode = framework.WithFeature(framework.ValidFeatures.Add("StandaloneMode"))

	// Owner: sig-storage
	// 带有此特性标记的测试验证基于 CSI 存储容量对节点评分的调度行为。
	StorageCapacityScoring = framework.WithFeature(framework.ValidFeatures.Add("StorageCapacityScoring"))

	// Owner: sig-storage
	// TODO: 记录该特性（何时在测试中使用此特性）
	StorageProvider = framework.WithFeature(framework.ValidFeatures.Add("StorageProvider"))

	// Owner: sig-api-machinery
	// TODO: 记录该特性（何时在测试中使用此特性）
	StorageVersionAPI = framework.WithFeature(framework.ValidFeatures.Add("StorageVersionAPI"))

	// Owner: sig-node
	// SupplementalGroupsPolicy 用于测试 SupplementalGroupsPolicy 特性。
	SupplementalGroupsPolicy = framework.WithFeature(framework.ValidFeatures.Add("SupplementalGroupsPolicy"))

	// Swap 特性测试必须在分配了 swap 内存且 kubelet 启用 swap 的节点上运行。KEP-2400（Node Swap Support）
	Swap = framework.WithFeature(framework.ValidFeatures.Add("NodeSwap"))

	// Owner: sig-node
	SystemNodeCriticalPod = framework.WithFeature(framework.ValidFeatures.Add("SystemNodeCriticalPod"))

	// Owner: sig-node
	// TODO: 记录该特性（何时在测试中使用此特性）
	TopologyManager = framework.WithFeature(framework.ValidFeatures.Add("TopologyManager"))

	// Owner: sig-cloud-provider
	// TODO: 记录该特性（何时在测试中使用此特性）
	Upgrade = framework.WithFeature(framework.ValidFeatures.Add("Upgrade"))

	// UserNamespacesSupport 位于运行时支持 user namespace 的 test-infra 节点上
	// 当 CI 移除对 containerd 1.7 的支持后，它将被删除。
	// Owner: sig-node
	UserNamespacesSupport = framework.WithFeature(framework.ValidFeatures.Add("UserNamespacesSupport"))

	// Owner: sig-storage
	// TODO: 当 csi driver 升为 GA 且 manifest 更新后移除。
	VolumeAttributesClass = framework.WithFeature(framework.ValidFeatures.Add("VolumeAttributesClass"))

	// Owner: sig-storage
	// volume group snapshot 测试
	VolumeGroupSnapshotDataSource = framework.WithFeature(framework.ValidFeatures.Add("volumegroupsnapshot"))

	// Owner: sig-storage
	// TODO: 记录该特性（何时在测试中使用此特性）
	VolumeSnapshotDataSource = framework.WithFeature(framework.ValidFeatures.Add("VolumeSnapshotDataSource"))

	// Owner: sig-storage
	// TODO: 记录该特性（何时在测试中使用此特性）
	VolumeSourceXFS = framework.WithFeature(framework.ValidFeatures.Add("VolumeSourceXFS"))

	// Owner: sig-storage
	// TODO: 记录该特性（何时在测试中使用此特性）
	Volumes = framework.WithFeature(framework.ValidFeatures.Add("Volumes"))

	// Owner: sig-windows
	// 表示带有此特性标记的测试依赖 Windows，因此只能在 Windows 节点上运行。
	Windows = framework.WithFeature(framework.ValidFeatures.Add("Windows"))

	// Owner: sig-windows
	// TODO: 记录该特性（何时在测试中使用此特性）
	WindowsHostProcessContainers = framework.WithFeature(framework.ValidFeatures.Add("WindowsHostProcessContainers"))

	// Owner: sig-windows
	// TODO: 记录该特性（何时在测试中使用此特性）
	WindowsHyperVContainers = framework.WithFeature(framework.ValidFeatures.Add("WindowsHyperVContainers"))
)

func init() {
	// 这可以防止在测试中添加额外的临时特性。
	framework.ValidFeatures.Freeze()
}
