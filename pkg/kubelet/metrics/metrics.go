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

package metrics

import (
	"sync"
	"time"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/apis/scheduling"
	"k8s.io/kubernetes/pkg/features"
)

// 该常量块定义 kubelet 各指标的名称。
const (
	FirstNetworkPodStartSLIDurationKey = "first_network_pod_start_sli_duration_seconds"
	KubeletSubsystem                   = "kubelet"
	DRASubsystem                       = "dra"
	NodeNameKey                        = "node_name"
	NodeLabelKey                       = "node"
	NodeStartupPreKubeletKey           = "node_startup_pre_kubelet_duration_seconds"
	NodeStartupPreRegistrationKey      = "node_startup_pre_registration_duration_seconds"
	NodeStartupRegistrationKey         = "node_startup_registration_duration_seconds"
	NodeStartupPostRegistrationKey     = "node_startup_post_registration_duration_seconds"
	NodeStartupKey                     = "node_startup_duration_seconds"
	PodWorkerDurationKey               = "pod_worker_duration_seconds"
	PodStartDurationKey                = "pod_start_duration_seconds"
	PodStartSLIDurationKey             = "pod_start_sli_duration_seconds"
	PodStartTotalDurationKey           = "pod_start_total_duration_seconds"
	CgroupManagerOperationsKey         = "cgroup_manager_duration_seconds"
	PodWorkerStartDurationKey          = "pod_worker_start_duration_seconds"
	PodStatusSyncDurationKey           = "pod_status_sync_duration_seconds"
	PLEGRelistDurationKey              = "pleg_relist_duration_seconds"
	PLEGDiscardEventsKey               = "pleg_discard_events"
	PLEGRelistIntervalKey              = "pleg_relist_interval_seconds"
	PLEGLastSeenKey                    = "pleg_last_seen_seconds"
	PLEGPodRelistDurationKey           = "pleg_pod_relist_duration_seconds"
	EventedPLEGConnErrKey              = "evented_pleg_connection_error_count"
	EventedPLEGConnKey                 = "evented_pleg_connection_success_count"
	EventedPLEGConnLatencyKey          = "evented_pleg_connection_latency_seconds"
	EvictionsKey                       = "evictions"
	EvictionStatsAgeKey                = "eviction_stats_age_seconds"
	PreemptionsKey                     = "preemptions"
	VolumeStatsCapacityBytesKey        = "volume_stats_capacity_bytes"
	VolumeStatsAvailableBytesKey       = "volume_stats_available_bytes"
	VolumeStatsUsedBytesKey            = "volume_stats_used_bytes"
	VolumeStatsInodesKey               = "volume_stats_inodes"
	VolumeStatsInodesFreeKey           = "volume_stats_inodes_free"
	VolumeStatsInodesUsedKey           = "volume_stats_inodes_used"

	RunningPodsKey             = "running_pods"
	RunningContainersKey       = "running_containers"
	DesiredPodCountKey         = "desired_pods"
	ActivePodCountKey          = "active_pods"
	MirrorPodCountKey          = "mirror_pods"
	WorkingPodCountKey         = "working_pods"
	OrphanedRuntimePodTotalKey = "orphaned_runtime_pods_total"
	RestartedPodTotalKey       = "restarted_pods_total"
	ImagePullDurationKey       = "image_pull_duration_seconds"
	CgroupVersionKey           = "cgroup_version"
	CRILosingSupportKey        = "cri_losing_support"

	// 远程运行时操作的指标键
	RuntimeOperationsKey         = "runtime_operations_total"
	RuntimeOperationsDurationKey = "runtime_operations_duration_seconds"
	RuntimeOperationsErrorsKey   = "runtime_operations_errors_total"
	// 设备插件操作的指标键
	DevicePluginRegistrationCountKey  = "device_plugin_registration_total"
	DevicePluginAllocationDurationKey = "device_plugin_alloc_duration_seconds"
	// Pod 资源端点操作的指标键
	PodResourcesEndpointRequestsTotalKey          = "pod_resources_endpoint_requests_total"
	PodResourcesEndpointRequestsListKey           = "pod_resources_endpoint_requests_list"
	PodResourcesEndpointRequestsGetAllocatableKey = "pod_resources_endpoint_requests_get_allocatable"
	PodResourcesEndpointErrorsListKey             = "pod_resources_endpoint_errors_list"
	PodResourcesEndpointErrorsGetAllocatableKey   = "pod_resources_endpoint_errors_get_allocatable"
	PodResourcesEndpointRequestsGetKey            = "pod_resources_endpoint_requests_get"
	PodResourcesEndpointErrorsGetKey              = "pod_resources_endpoint_errors_get"

	// RuntimeClass 相关指标键
	RunPodSandboxDurationKey = "run_podsandbox_duration_seconds"
	RunPodSandboxErrorsKey   = "run_podsandbox_errors_total"

	// 统计已启动的 Pod 与容器总数的指标
	StartedPodsTotalKey             = "started_pods_total"
	StartedPodsErrorsTotalKey       = "started_pods_errors_total"
	StartedContainersTotalKey       = "started_containers_total"
	TerminatedContainersTotalKey    = "terminated_containers_total"
	StartedContainersErrorsTotalKey = "started_containers_errors_total"

	// 统计本 kubelet 的 HostProcess 容器使用情况
	StartedHostProcessContainersTotalKey       = "started_host_process_containers_total"
	StartedHostProcessContainersErrorsTotalKey = "started_host_process_containers_errors_total"

	// 统计用户命名空间（hostUsers = false）Pod 的指标
	StartedUserNamespacedPodsTotalKey       = "started_user_namespaced_pods_total"
	StartedUserNamespacedPodsErrorsTotalKey = "started_user_namespaced_pods_errors_total"

	// 统计本 kubelet 的临时容器使用情况
	ManagedEphemeralContainersKey = "managed_ephemeral_containers"

	// 统计资源管理器分配行为的指标
	ResourceManagerAllocationTotalKey = "resource_manager_allocations_total"

	ResourceManagerAllocationErrorsTotalKey = "resource_manager_allocation_errors_total"

	ResourceManagerContainerAssignmentsTotalKey = "resource_manager_container_assignments_total"

	ResourceManagerPod  = "pod"
	ResourceManagerNode = "node"

	ResourceManagerCPU    = "cpu"
	ResourceManagerMemory = "memory"

	ResourceManagerExclusiveNode = "node_exclusive"
	ResourceManagerSharedPod     = "pod_shared"
	ResourceManagerExclusivePod  = "pod_exclusive"

	// 统计 CPU 管理器行为的指标
	CPUManagerPinningRequestsTotalKey         = "cpu_manager_pinning_requests_total"
	CPUManagerPinningErrorsTotalKey           = "cpu_manager_pinning_errors_total"
	CPUManagerSharedPoolSizeMilliCoresKey     = "cpu_manager_shared_pool_size_millicores"
	CPUManagerExclusiveCPUsAllocationCountKey = "cpu_manager_exclusive_cpu_allocation_count"
	CPUManagerAllocationPerNUMAKey            = "cpu_manager_allocation_per_numa"

	// 统计内存管理器行为的指标
	MemoryManagerPinningRequestsTotalKey = "memory_manager_pinning_requests_total"
	MemoryManagerPinningErrorsTotalKey   = "memory_manager_pinning_errors_total"

	// 统计拓扑管理器行为的指标
	TopologyManagerAdmissionRequestsTotalKey = "topology_manager_admission_requests_total"
	TopologyManagerAdmissionErrorsTotalKey   = "topology_manager_admission_errors_total"
	TopologyManagerAdmissionDurationKey      = "topology_manager_admission_duration_ms"

	// 统计孤儿 Pod 清理的指标
	orphanPodCleanedVolumesKey       = "orphan_pod_cleaned_volumes"
	orphanPodCleanedVolumesErrorsKey = "orphan_pod_cleaned_volumes_errors"

	// 统计被垃圾回收的镜像的指标
	ImageGarbageCollectedTotalKey = "image_garbage_collected_total"

	// 统计计算资源对齐情况的指标
	ContainerAlignedComputeResourcesNameKey          = "container_aligned_compute_resources_count"
	ContainerAlignedComputeResourcesFailureNameKey   = "container_aligned_compute_resources_failure_count"
	ContainerAlignedComputeResourcesScopeLabelKey    = "scope"
	ContainerAlignedComputeResourcesBoundaryLabelKey = "boundary"

	// DRA 操作的指标键
	DRAOperationsDurationKey     = "operations_duration_seconds"
	DRAGRPCOperationsDurationKey = "grpc_operations_duration_seconds"

	// MemoryQoS 相关指标键
	MemoryQoSNodeMemoryMinBytesKey = "memory_qos_node_memory_min_bytes"
	MemoryQoSNodeMemoryLowBytesKey = "memory_qos_node_memory_low_bytes"

	// 指标标签中使用的取值
	Container          = "container"
	InitContainer      = "init_container"
	EphemeralContainer = "ephemeral_container"

	AlignScopePod       = "pod"
	AlignScopeContainer = "container"

	AlignedPhysicalCPU = "physical_cpu"
	AlignedNUMANode    = "numa_node"
	AlignedUncoreCache = "uncore_cache"

	// 统计 kubelet 准入拒绝情况的指标。
	AdmissionRejectionsTotalKey = "admission_rejections_total"

	// 镜像卷相关指标
	ImageVolumeRequestedTotalKey      = "image_volume_requested_total"
	ImageVolumeMountedSucceedTotalKey = "image_volume_mounted_succeed_total"
	ImageVolumeMountedErrorsTotalKey  = "image_volume_mounted_errors_total"

	// [DRAResourceClaimsInUseDesc] 的特殊标签，用于统计 ResourceClaims，不受具体驱动影响。
	DRAResourceClaimsInUseAnyDriver = "<any>"

	// 原地 Pod 扩缩容操作的指标键。
	ContainerRequestedResizesKey        = "container_requested_resizes_total"
	PodResizeDurationMillisecondsKey    = "pod_resize_duration_milliseconds"
	PodPendingResizesKey                = "pod_pending_resizes"
	PodInfeasibleResizesKey             = "pod_infeasible_resizes_total"
	PodInProgressResizesKey             = "pod_in_progress_resizes"
	PodDeferredAcceptedResizesKey       = "pod_deferred_accepted_resizes_total"
	PodDeferredResizeDurationSecondsKey = "pod_deferred_resize_duration_seconds"

	// Pod 证书状态相关的指标键。
	PodCertificateStatesKey = "podcertificate_states"

	PodWatchEventsDroppedKey = "pod_watch_events_dropped_total"
	PodRequestsTotalKey      = "pod_requests_total"
	PodRequestsListKey       = "pod_requests_list_total"
	PodRequestsGetKey        = "pod_requests_get_total"
	PodRequestsWatchKey      = "pod_requests_watch_total"

	// Pod 级资源相关指标键。
	PodLevelResourcesAdmissionTotalKey = "pod_level_resources_admission_total"
)

// PriorityBucket 表示 Pod 扩缩容指标中优先级桶的标签取值。
type PriorityBucket string

const (
	// Pod 扩缩容指标的优先级桶标签取值。
	PriorityBucketSystemCritical PriorityBucket = "system-critical"
	PriorityBucketHigh           PriorityBucket = "high"
	PriorityBucketMedium         PriorityBucket = "medium"
	PriorityBucketNormal         PriorityBucket = "normal"
	PriorityBucketLow            PriorityBucket = "low"
	PriorityBucketVeryLow        PriorityBucket = "very-low"
	PriorityBucketUnknown        PriorityBucket = "unknown"
)

// DeferredResizeResolution 表示 Pod 延迟扩缩容时长指标中解决状态的标签取值。
type DeferredResizeResolution string

const (
	// Pod 延迟扩缩容时长指标的解决状态标签取值。
	DeferredResizeResolutionAccepted   DeferredResizeResolution = "accepted"
	DeferredResizeResolutionReverted   DeferredResizeResolution = "reverted"
	DeferredResizeResolutionTerminated DeferredResizeResolution = "terminated"
)

// imageSizeBucket 表示镜像大小桶的下界（字节）及其标签。
type imageSizeBucket struct {
	lowerBoundInBytes uint64
	label             string
}

var (
	podStartupDurationBuckets = []float64{0.5, 1, 2, 3, 4, 5, 6, 8, 10, 20, 30, 45, 60, 120, 180, 240, 300, 360, 480, 600, 900, 1200, 1800, 2700, 3600}
	imagePullDurationBuckets  = []float64{1, 5, 10, 20, 30, 60, 120, 180, 240, 300, 360, 480, 600, 900, 1200, 1800, 2700, 3600}
	// imageSizeBuckets 是与 image_pull_duration_seconds 指标关联的标签。例如，若拉取的镜像大小介于
	// 1GB 到 5GB 之间，标签即为 "1GB-5GB"。
	imageSizeBuckets = []imageSizeBucket{
		{0, "0-10MB"},
		{10 * 1024 * 1024, "10MB-100MB"},
		{100 * 1024 * 1024, "100MB-500MB"},
		{500 * 1024 * 1024, "500MB-1GB"},
		{1 * 1024 * 1024 * 1024, "1GB-5GB"},
		{5 * 1024 * 1024 * 1024, "5GB-10GB"},
		{10 * 1024 * 1024 * 1024, "10GB-20GB"},
		{20 * 1024 * 1024 * 1024, "20GB-30GB"},
		{30 * 1024 * 1024 * 1024, "30GB-40GB"},
		{40 * 1024 * 1024 * 1024, "40GB-60GB"},
		{60 * 1024 * 1024 * 1024, "60GB-100GB"},
		{100 * 1024 * 1024 * 1024, "GT100GB"},
	}
	// DRADurationBuckets 是 DRA 操作时长指标的桶边界
	// 对应本文件中下方定义的 DRAOperationsDuration 与 DRAGRPCOperationsDuration。
	// 桶的最大值 40 基于 DRA gRPC 调用定义的最长 45 秒 gRPC 超时时间
	// 即 pkg/kubelet/cm/dra/plugin/registration.go 中的 DRA gRPC 调用超时值。
	DRADurationBuckets = metrics.ExponentialBucketsRange(.1, 40, 15)

	// podResizeDurationBuckets 是 pod_resize_duration_milliseconds 指标的桶边界。
	podResizeDurationBuckets = []float64{10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000, 30000, 60000, 120000, 300000, 600000}

	// podDeferredResizeDurationBuckets 是 pod_deferred_resize_duration_seconds 指标的桶边界。
	podDeferredResizeDurationBuckets = []float64{5, 10, 15, 20, 30, 45, 60, 90, 120, 150, 180, 300, 600, 1200, 1800, 3600}
)

var (
	// NodeName 是跟踪节点名称的 Gauge，其值恒为 1。
	NodeName = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeNameKey,
			Help:           "The node's name. The count is always 1.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{NodeLabelKey},
	)
	// ContainersPerPodCount 是跟踪每个 Pod 容器数量的 Histogram。
	ContainersPerPodCount = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "containers_per_pod_count",
			Help:           "The number of containers per pod.",
			Buckets:        metrics.ExponentialBuckets(1, 2, 5),
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PodWorkerDuration 是跟踪同步单个 Pod 所耗时长（秒）的 Histogram。
	// 按操作类型拆分。
	PodWorkerDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodWorkerDurationKey,
			Help:           "Duration in seconds to sync a single pod. Broken down by operation type: create, update, or sync",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// PodStartDuration 是跟踪 Pod 自被 kubelet 首次发现起开始运行所耗时长（秒）的 Histogram。
	// 该时长自 kubelet 首次发现 Pod 起算。
	PodStartDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodStartDurationKey,
			Help:           "Duration in seconds from kubelet seeing a pod for the first time to the pod starting to run",
			Buckets:        podStartupDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PodStartSLIDuration 是跟踪单个 Pod 开始运行所耗时长（秒）的 Histogram，
	// 排除镜像拉取耗时。该指标应反映 "Pod 启动时延 SLI" 的定义，
	// 参见 https://github.com/kubernetes/community/blob/master/sig-scalability/slos/pod_startup_latency.md
	//
	// Pod 启动时延指标的直方图桶边界，单位为秒。这些边界是手工选定的，
	// 大致呈指数级增长，同时在日常单位下仍是整数值。这样可尽量减少桶的数量，
	// 同时仍能准确测量 SLO 中可能使用的阈值，
	// 例如 x% 的 Pod 在 30 秒或 15 分钟内启动等。
	PodStartSLIDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodStartSLIDurationKey,
			Help:           "Duration in seconds to start a pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch",
			Buckets:        podStartupDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{},
	)

	// PodStartTotalDuration 是跟踪单个 Pod 自创建起开始运行所耗时长（秒）的 Histogram，
	// 该时长包含镜像拉取耗时。
	//
	// Pod 启动时延指标的直方图桶边界，单位为秒。这些边界是手工选定的，
	// 大致呈指数级增长，同时在日常单位下仍是整数值。这样可尽量减少桶的数量，
	// 同时仍能准确测量 SLO 中可能使用的阈值，
	// 例如 x% 的 Pod 在 30 秒或 15 分钟内启动等。
	PodStartTotalDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodStartTotalDurationKey,
			Help:           "Duration in seconds to start a pod since creation, including time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch",
			Buckets:        podStartupDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{},
	)

	// FirstNetworkPodStartSLIDuration 是跟踪首个网络 Pod 开始运行所耗时长（秒）的 Gauge，
	// 排除镜像拉取耗时。这是一个内部且临时的指标，用于应对现有网络子系统与 CRI/CNI 实现的限制，该限制将通过 https://github.com/containernetworking/cni/issues/859 解决
	// 参见 https://github.com/containernetworking/cni/issues/859
	// 该指标代表用户在新节点上运行工作负载所观察到的时延。
	// 参见 https://github.com/kubernetes/community/blob/master/sig-scalability/slos/pod_startup_latency.md
	FirstNetworkPodStartSLIDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           FirstNetworkPodStartSLIDurationKey,
			Help:           "Duration in seconds to start the first network pod, excluding time to pull images and run init containers, measured from pod creation timestamp to when all its containers are reported as started and observed via watch",
			StabilityLevel: metrics.INTERNAL,
		},
	)

	// CgroupManagerDuration 是跟踪 cgroup 管理器操作完成所耗时长（秒）的 Histogram。
	// 按方法拆分。
	CgroupManagerDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CgroupManagerOperationsKey,
			Help:           "Duration in seconds for cgroup manager operations. Broken down by method.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// PodWorkerStartDuration 是跟踪从 kubelet 发现 Pod 到启动 worker 所耗时长（秒）的 Histogram。
	PodWorkerStartDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodWorkerStartDurationKey,
			Help:           "Duration in seconds from kubelet seeing a pod to starting a worker.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PodStatusSyncDuration 是跟踪从 Pod 状态生成到同步至 apiserver 所耗时长（秒）的 Histogram。
	// 若在写入 API 之前 Pod 产生了多次状态变更，
	// 则时延从第一次更新算到最后一次事件。
	PodStatusSyncDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodStatusSyncDurationKey,
			Help:           "Duration in seconds to sync a pod status update. Measures time from detection of a change to pod status until the API is successfully updated for that pod, even if multiple intevening changes to pod status occur.",
			Buckets:        []float64{0.010, 0.050, 0.100, 0.500, 1, 5, 10, 20, 30, 45, 60},
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PLEGRelistDuration 是跟踪 kubelet 的 Pod 生命周期事件生成器（PLEG）执行 Pod 列表刷新所耗时长（秒）的 Histogram。
	// 即 Pod 生命周期事件生成器（PLEG）。
	PLEGRelistDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGRelistDurationKey,
			Help:           "Duration in seconds for relisting pods in PLEG.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PLEGDiscardEvents 是跟踪 kubelet 的 Pod 生命周期事件生成器（PLEG）中被丢弃的事件数的 Counter。
	PLEGDiscardEvents = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGDiscardEventsKey,
			Help:           "The number of discard events in PLEG.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PLEGRelistInterval 是跟踪 kubelet 的 Pod 生命周期事件生成器（PLEG）两次列表刷新之间间隔（秒）的 Histogram。
	// 即 Pod 生命周期事件生成器（PLEG）。
	PLEGRelistInterval = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGRelistIntervalKey,
			Help:           "Interval in seconds between relisting in PLEG.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PLEGLastSeen 是给出 kubelet 的 Pod 生命周期事件生成器（PLEG）
	// 最后一次处于活跃状态的 Unix 时间戳的 Gauge。
	PLEGLastSeen = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGLastSeenKey,
			Help:           "Timestamp in seconds when PLEG was last seen active.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PLEGPodRelistDuration 是跟踪 kubelet 的 Pod 生命周期事件生成器（PLEG）刷新单个 Pod 列表所耗时长（秒）的 Histogram。
	// 即 Pod 生命周期事件生成器（PLEG）。
	PLEGPodRelistDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGPodRelistDurationKey,
			Help:           "Duration in seconds for relisting a single pod in PLEG.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)

	// EventedPLEGConnErr 是跟踪与 CRI 运行时建立流式连接过程中所遇错误的 Counter。
	// 该指标统计与 CRI 运行时建立流式连接的错误次数。
	EventedPLEGConnErr = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EventedPLEGConnErrKey,
			Help:           "The number of errors encountered during the establishment of streaming connection with the CRI runtime.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// EventedPLEGConn 是跟踪获取流式客户端以接收 CRI 事件次数的 Counter。
	// 该指标统计成功获取流式客户端的次数。
	EventedPLEGConn = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EventedPLEGConnKey,
			Help:           "The number of times a streaming client was obtained to receive CRI Events.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// EventedPLEGConnLatency 是跟踪与 CRI 运行时建立流式连接时延（秒）的 Histogram。
	// 该时延以秒为单位。
	EventedPLEGConnLatency = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EventedPLEGConnLatencyKey,
			Help:           "The latency of streaming connection with the CRI runtime, measured in seconds.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)

	// RuntimeOperations 是跟踪远程运行时操作累计次数的 Counter。
	// 按操作类型拆分。
	RuntimeOperations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsKey,
			Help:           "Cumulative number of runtime operations by operation type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// RuntimeOperationsDuration 是跟踪远程运行时操作完成所耗时长（秒）的 Histogram。
	// 按操作类型拆分。
	RuntimeOperationsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsDurationKey,
			Help:           "Duration in seconds of runtime operations. Broken down by operation type.",
			Buckets:        metrics.ExponentialBuckets(.005, 2.5, 14),
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// RuntimeOperationsErrors 是跟踪远程运行时操作错误累计次数的 Counter。
	// 按操作类型拆分。
	RuntimeOperationsErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsErrorsKey,
			Help:           "Cumulative number of runtime operation errors by operation type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// Evictions 是跟踪 kubelet 发起的 Pod 驱逐累计次数的 Counter。
	// 按驱逐信号拆分。
	Evictions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EvictionsKey,
			Help:           "Cumulative number of pod evictions by eviction signal",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"eviction_signal"},
	)
	// EvictionStatsAge 是跟踪从采集统计信息到 Pod 被驱逐之间时长（秒）的 Histogram，
	// 该时长基于被采集的统计信息，按驱逐信号拆分。
	EvictionStatsAge = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EvictionStatsAgeKey,
			Help:           "Time between when stats are collected, and when pod is evicted based on those stats by eviction signal",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"eviction_signal"},
	)
	// Preemptions 是跟踪 kubelet 发起的 Pod 抢占累计次数的 Counter。
	// 按抢占信号拆分。一次抢占只针对一种资源记录，各信号之和
	// 即该节点上的抢占次数。
	Preemptions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PreemptionsKey,
			Help:           "Cumulative number of pod preemptions by preemption resource",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"preemption_signal"},
	)
	// DevicePluginRegistrationCount 是跟踪设备插件注册累计次数的 Counter。
	// 按资源名称拆分。
	DevicePluginRegistrationCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           DevicePluginRegistrationCountKey,
			Help:           "Cumulative number of device plugin registrations. Broken down by resource name.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name"},
	)
	// DevicePluginAllocationDuration 是跟踪处理设备插件分配请求所耗时长（秒）的 Histogram。
	// 按资源名称拆分。
	DevicePluginAllocationDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           DevicePluginAllocationDurationKey,
			Help:           "Duration in seconds to serve a device plugin Allocation request. Broken down by resource name.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name"},
	)

	// PodResourcesEndpointRequestsTotalCount 是跟踪 PodResource 端点请求累计次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointRequestsTotalCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsTotalKey,
			Help:           "Cumulative number of requests to the PodResource endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointRequestsListCount 是跟踪 PodResource List() 端点请求次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointRequestsListCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsListKey,
			Help:           "Number of requests to the PodResource List endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointRequestsGetAllocatableCount 是跟踪 PodResource GetAllocatableResources() 端点请求次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointRequestsGetAllocatableCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsGetAllocatableKey,
			Help:           "Number of requests to the PodResource GetAllocatableResources endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointErrorsListCount 是跟踪 PodResource List() 端点返回错误次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointErrorsListCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointErrorsListKey,
			Help:           "Number of requests to the PodResource List endpoint which returned error. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointErrorsGetAllocatableCount 是跟踪 PodResource GetAllocatableResources() 端点返回错误次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointErrorsGetAllocatableCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointErrorsGetAllocatableKey,
			Help:           "Number of requests to the PodResource GetAllocatableResources endpoint which returned error. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointRequestsGetCount 是跟踪 PodResource Get() 端点请求次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointRequestsGetCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsGetKey,
			Help:           "Number of requests to the PodResource Get endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointErrorsGetCount 是跟踪 PodResource Get() 端点返回错误次数的 Counter。
	// 按服务端 API 版本拆分。
	PodResourcesEndpointErrorsGetCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointErrorsGetKey,
			Help:           "Number of requests to the PodResource Get endpoint which returned error. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// RunPodSandboxDuration 是跟踪执行 Pod Sandbox 操作所耗时长（秒）的 Histogram。
	// 按 RuntimeClass.Handler 拆分。
	RunPodSandboxDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: KubeletSubsystem,
			Name:      RunPodSandboxDurationKey,
			Help:      "Duration in seconds of the run_podsandbox operations. Broken down by RuntimeClass.Handler.",
			// 目前使用默认桶，必要时再自定义桶边界。
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"runtime_handler"},
	)
	// RunPodSandboxErrors 是跟踪 Pod Sandbox 操作错误累计次数的 Counter。
	// 按 RuntimeClass.Handler 拆分。
	RunPodSandboxErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunPodSandboxErrorsKey,
			Help:           "Cumulative number of the run_podsandbox operation errors by RuntimeClass.Handler.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"runtime_handler"},
	)

	// RunningPodCount 是跟踪当前拥有运行中 sandbox 的 Pod 数量的 Gauge。
	// 它用于暴露 kubelet 内部状态：容器运行时中有多少 Pod 拥有运行中的容器，主要供调试使用。
	RunningPodCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunningPodsKey,
			Help:           "Number of pods that have a running pod sandbox",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// RunningContainerCount 是跟踪当前运行中容器数量的 Gauge。
	RunningContainerCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunningContainersKey,
			Help:           "Number of containers currently running",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_state"},
	)
	// DesiredPodCount 跟踪 kubelet 认为应当处于运行状态的 Pod 数量。
	DesiredPodCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           DesiredPodCountKey,
			Help:           "The number of pods the kubelet is being instructed to run. static is true if the pod is not from the apiserver.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"static"},
	)
	// ActivePodCount 跟踪 kubelet 在决定是否准入新 Pod 时视为活跃的 Pod 数量。
	ActivePodCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ActivePodCountKey,
			Help:           "The number of pods the kubelet considers active and which are being considered when admitting new pods. static is true if the pod is not from the apiserver.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"static"},
	)
	// MirrorPodCount 跟踪 kubelet 应为静态 Pod 创建的 mirror pod 数量。
	MirrorPodCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           MirrorPodCountKey,
			Help:           "The number of mirror pods the kubelet will try to create (one per admitted static pod)",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// WorkingPodCount 跟踪各生命周期阶段中 Pod 的数量，以及其是否为静态 Pod、是否处于期望、孤儿或仅运行时状态。
	WorkingPodCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           WorkingPodCountKey,
			Help:           "Number of pods the kubelet is actually running, broken down by lifecycle phase, whether the pod is desired, orphaned, or runtime only (also orphaned), and whether the pod is static. An orphaned pod has been removed from local configuration or force deleted in the API and consumes resources that are not otherwise visible.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"lifecycle", "config", "static"},
	)
	// OrphanedRuntimePodTotal 在每次发现运行时中存在未被 pod worker 事先知晓的 Pod 时递增。
	OrphanedRuntimePodTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           OrphanedRuntimePodTotalKey,
			Help:           "Number of pods that have been detected in the container runtime without being already known to the pod worker. This typically indicates the kubelet was restarted while a pod was force deleted in the API or in the local configuration, which is unusual.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// RestartedPodTotal 在每次同一 UID 的 Pod 被删除并重建时递增。
	RestartedPodTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RestartedPodTotalKey,
			Help:           "Number of pods that have been restarted because they were deleted and recreated with the same UID while the kubelet was watching them (common for static pods, extremely uncommon for API pods)",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"static"},
	)
	// StartedPodsTotal 是跟踪 Pod sandbox 创建操作的计数器。
	StartedPodsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedPodsTotalKey,
			Help:           "Cumulative number of pods started",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// StartedPodsErrorsTotal 是跟踪创建 Pod sandbox 失败次数的计数器。
	StartedPodsErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedPodsErrorsTotalKey,
			Help:           "Cumulative number of errors when starting pods",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// StartedContainersTotal 是跟踪容器创建操作的计数器。
	StartedContainersTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedContainersTotalKey,
			Help:           "Cumulative number of containers started",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_type"},
	)
	// TerminatedContainersTotal 是跟踪容器终止次数的计数器。
	TerminatedContainersTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           TerminatedContainersTotalKey,
			Help:           "Cumulative number of container terminations.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_type", "exit_code", "reason"},
	)
	// StartedContainersErrorsTotal 是跟踪创建容器失败次数的计数器。
	StartedContainersErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedContainersErrorsTotalKey,
			Help:           "Cumulative number of errors when starting containers",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_type", "code"},
	)
	// StartedHostProcessContainersTotal 是跟踪 HostProcess 容器创建操作的计数器。
	StartedHostProcessContainersTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedHostProcessContainersTotalKey,
			Help:           "Cumulative number of hostprocess containers started. This metric will only be collected on Windows.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_type"},
	)
	// StartedHostProcessContainersErrorsTotal 是跟踪创建 HostProcess 容器失败次数的计数器。
	StartedHostProcessContainersErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedHostProcessContainersErrorsTotalKey,
			Help:           "Cumulative number of errors when starting hostprocess containers. This metric will only be collected on Windows.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"container_type", "code"},
	)
	// StartedUserNamespacedPodsTotal 是跟踪尝试创建用户命名空间 Pod 数量的计数器。
	StartedUserNamespacedPodsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedUserNamespacedPodsTotalKey,
			Help:           "Cumulative number of pods with user namespaces started. This metric will only be collected on Linux.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// StartedUserNamespacedPodsErrorsTotal 是跟踪创建用户命名空间 Pod 失败次数的计数器。
	StartedUserNamespacedPodsErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           StartedUserNamespacedPodsErrorsTotalKey,
			Help:           "Cumulative number of errors when starting pods with user namespaces. This metric will only be collected on Linux.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// ManagedEphemeralContainers 是指示本 kubelet 管理的临时容器数量的 Gauge。
	ManagedEphemeralContainers = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ManagedEphemeralContainersKey,
			Help:           "Current number of ephemeral containers in pods managed by this kubelet.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// GracefulShutdownStartTime 是记录 kubelet 开始优雅关闭时间的 Gauge。
	GracefulShutdownStartTime = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "graceful_shutdown_start_time_seconds",
			Help:           "Last graceful shutdown start time since unix epoch in seconds",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// GracefulShutdownEndTime 是记录 kubelet 完成优雅关闭时间的 Gauge。
	GracefulShutdownEndTime = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "graceful_shutdown_end_time_seconds",
			Help:           "Last graceful shutdown end time since unix epoch in seconds",
			StabilityLevel: metrics.ALPHA,
		},
	)

	LifecycleHandlerHTTPFallbacks = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "lifecycle_handler_http_fallbacks_total",
			Help:           "The number of times lifecycle handlers successfully fell back to http from https.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// CPUManagerPinningRequestsTotal 跟踪 Pod spec 触发 CPU 管理器进行核心绑定的次数。
	CPUManagerPinningRequestsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CPUManagerPinningRequestsTotalKey,
			Help:           "The number of cpu core allocations which required pinning.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// CPUManagerPinningErrorsTotal 跟踪 Pod spec 需要 CPU 管理器绑定核心但分配失败的次数。
	CPUManagerPinningErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CPUManagerPinningErrorsTotalKey,
			Help:           "The number of cpu core allocations which required pinning failed.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// CPUManagerSharedPoolSizeMilliCores 报告面向非 Guaranteed Pod 的共享 CPU 池当前大小（毫核）。
	CPUManagerSharedPoolSizeMilliCores = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CPUManagerSharedPoolSizeMilliCoresKey,
			Help:           "The size of the shared CPU pool for non-guaranteed QoS pods, in millicores.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// CPUManagerExclusiveCPUsAllocationCount 报告本节点上被排他分配给容器的 CPU 总数。
	CPUManagerExclusiveCPUsAllocationCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CPUManagerExclusiveCPUsAllocationCountKey,
			Help:           "The total number of CPUs exclusively allocated to containers running on this node",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// CPUManagerAllocationPerNUMA 跟踪每个 NUMA 节点分配的 CPU 数量。
	CPUManagerAllocationPerNUMA = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CPUManagerAllocationPerNUMAKey,
			Help:           "Number of CPUs allocated per NUMA node",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{AlignedNUMANode},
	)

	// ContainerAlignedComputeResources 报告按对齐边界授予已对齐资源的分配次数。
	ContainerAlignedComputeResources = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ContainerAlignedComputeResourcesNameKey,
			Help:           "Cumulative number of aligned compute resources allocated to containers by alignment type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{ContainerAlignedComputeResourcesScopeLabelKey, ContainerAlignedComputeResourcesBoundaryLabelKey},
	)

	// ContainerAlignedComputeResourcesFailure 报告按对齐边界未能对齐资源的分配尝试次数。
	ContainerAlignedComputeResourcesFailure = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ContainerAlignedComputeResourcesFailureNameKey,
			Help:           "Cumulative number of failures to allocate aligned compute resources to containers by alignment type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{ContainerAlignedComputeResourcesScopeLabelKey, ContainerAlignedComputeResourcesBoundaryLabelKey},
	)

	MemoryManagerPinningRequestTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           MemoryManagerPinningRequestsTotalKey,
			Help:           "The number of memory pages allocations which required pinning.",
			StabilityLevel: metrics.ALPHA,
		})

	// MemoryManagerPinningErrorsTotal 跟踪 Pod spec 需要内存管理器绑定内存页但分配失败的次数。
	MemoryManagerPinningErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           MemoryManagerPinningErrorsTotalKey,
			Help:           "The number of memory pages allocations which required pinning that failed.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// MemoryQoSNodeMemoryMinBytes 跟踪 Guaranteed Pod 的 cgroup v2 memory.min（硬保护）总量。
	MemoryQoSNodeMemoryMinBytes = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           MemoryQoSNodeMemoryMinBytesKey,
			Help:           "Total cgroup v2 memory.min in bytes for Guaranteed pods. This memory is hard-reserved and never reclaimed by the kernel.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// MemoryQoSNodeMemoryLowBytes 跟踪 Burstable Pod 的 cgroup v2 memory.low（软保护）总量。
	MemoryQoSNodeMemoryLowBytes = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           MemoryQoSNodeMemoryLowBytesKey,
			Help:           "Total cgroup v2 memory.low in bytes for Burstable pods. This memory is soft-reserved and may be reclaimed under extreme pressure.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// TopologyManagerAdmissionRequestsTotal 跟踪 Pod spec 触发拓扑管理器准入 Pod 的次数。
	TopologyManagerAdmissionRequestsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           TopologyManagerAdmissionRequestsTotalKey,
			Help:           "The number of admission requests where resources have to be aligned.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// TopologyManagerAdmissionErrorsTotal 跟踪 Pod spec 需要拓扑管理器准入 Pod 但准入失败的次数。
	TopologyManagerAdmissionErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           TopologyManagerAdmissionErrorsTotalKey,
			Help:           "The number of admission request failures where resources could not be aligned.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// TopologyManagerAdmissionDuration 是跟踪处理 Pod 准入请求所耗时长（毫秒）的 Histogram。
	TopologyManagerAdmissionDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           TopologyManagerAdmissionDurationKey,
			Help:           "Duration in milliseconds to serve a pod admission request.",
			Buckets:        metrics.ExponentialBuckets(.05, 2, 15),
			StabilityLevel: metrics.ALPHA,
		},
	)

	// OrphanPodCleanedVolumes 是上一次清理周期内调用 removeOrphanedPodVolumeDirs 处理的孤儿 Pod 数量。
	OrphanPodCleanedVolumes = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           orphanPodCleanedVolumesKey,
			Help:           "The total number of orphaned Pods whose volumes were cleaned in the last periodic sweep.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// OrphanPodCleanedVolumesErrors 是 removeOrphanedPodVolumeDirs 失败的次数。
	OrphanPodCleanedVolumesErrors = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           orphanPodCleanedVolumesErrorsKey,
			Help:           "The number of orphaned Pods whose volumes failed to be cleaned in the last periodic sweep.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	NodeStartupPreKubeletDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeStartupPreKubeletKey,
			Help:           "Duration in seconds of node startup before kubelet starts.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	NodeStartupPreRegistrationDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeStartupPreRegistrationKey,
			Help:           "Duration in seconds of node startup before registration.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	NodeStartupRegistrationDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeStartupRegistrationKey,
			Help:           "Duration in seconds of node startup during registration.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	NodeStartupPostRegistrationDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeStartupPostRegistrationKey,
			Help:           "Duration in seconds of node startup after registration.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	NodeStartupDuration = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeStartupKey,
			Help:           "Duration in seconds of node startup in total.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	ImageGarbageCollectedTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ImageGarbageCollectedTotalKey,
			Help:           "Total number of images garbage collected by the kubelet, whether through disk usage or image age.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)

	// ImagePullDuration 是跟踪拉取镜像所耗时长（秒）的 Histogram，
	// 包含镜像拉取器等待队列中的耗时。
	// 该指标按镜像大小分桶。
	ImagePullDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ImagePullDurationKey,
			Help:           "Duration in seconds to pull an image.",
			Buckets:        imagePullDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"image_size_in_bytes"},
	)

	LifecycleHandlerSleepTerminated = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "sleep_action_terminated_early_total",
			Help:           "The number of times lifecycle sleep handler got terminated before it finishes",
			StabilityLevel: metrics.ALPHA,
		},
	)

	CgroupVersion = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CgroupVersionKey,
			Help:           "cgroup version on the hosts.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	CRILosingSupport = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CRILosingSupportKey,
			Help:           "the Kubernetes version that the currently running CRI implementation will lose support on if not upgraded.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"version"},
	)

	// DRAOperationsDuration 跟踪 DRA PrepareResources 与 UnprepareResources 请求的时长。
	DRAOperationsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      DRASubsystem,
			Name:           DRAOperationsDurationKey,
			Help:           "Latency histogram in seconds for the duration of handling all ResourceClaims referenced by a pod when the pod starts or stops. Identified by the name of the operation (PrepareResources or UnprepareResources) and separated by the success of the operation. The number of failed operations is provided through the histogram's overall count.",
			Buckets:        DRADurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_name", "is_error"},
	)

	// DRAGRPCOperationsDuration 跟踪 DRA GRPC 操作的时长。
	DRAGRPCOperationsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      DRASubsystem,
			Name:           DRAGRPCOperationsDurationKey,
			Help:           "Duration in seconds of the DRA gRPC operations",
			Buckets:        DRADurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name", "method_name", "grpc_status_code"},
	)

	DRANodePrepareSkipsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      DRASubsystem,
			Name:           "node_prepare_skips_total",
			Help:           "Cumulative number of times NodePrepareResources gRPC calls were skipped due to SkipNodeOperations.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name"},
	)

	DRANodeUnprepareSkipsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      DRASubsystem,
			Name:           "node_unprepare_skips_total",
			Help:           "Cumulative number of times NodeUnprepareResources gRPC calls were skipped due to SkipNodeOperations.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"driver_name"},
	)

	DRAResourceClaimsInUseDesc = metrics.NewDesc(
		metrics.BuildFQName("", DRASubsystem, "resource_claims_in_use"),
		"The number of ResourceClaims that are currently in use on the node, by driver name (driver_name label value) and across all drivers (special value <any> for driver_name). Note that the sum of all by-driver counts is not the total number of in-use ResourceClaims because the same ResourceClaim might use devices from different drivers. Instead, use the count for the <any> driver_name.",
		[]string{"driver_name"},
		nil,
		metrics.ALPHA,
		"",
	)

	// AdmissionRejectionsTotal 跟踪准入失败的次数，目前仅对新增 Pod 进行记录。
	AdmissionRejectionsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           AdmissionRejectionsTotalKey,
			Help:           "Cumulative number pod admission rejections by the Kubelet.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason"},
	)

	// ImageVolumeRequestedTotal 跟踪请求镜像卷的数量。
	ImageVolumeRequestedTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ImageVolumeRequestedTotalKey,
			Help:           "Number of requested image volumes.",
			StabilityLevel: metrics.BETA,
		},
	)

	// ImageVolumeMountedSucceedTotal 跟踪镜像卷挂载成功的数量。
	ImageVolumeMountedSucceedTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ImageVolumeMountedSucceedTotalKey,
			Help:           "Number of successful image volume mounts.",
			StabilityLevel: metrics.BETA,
		},
	)

	// ImageVolumeMountedErrorsTotal 跟踪镜像卷挂载失败的数量。
	ImageVolumeMountedErrorsTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ImageVolumeMountedErrorsTotalKey,
			Help:           "Number of failed image volume mounts.",
			StabilityLevel: metrics.BETA,
		},
	)

	// ContainerRequestedResizes 跟踪容器层级请求扩缩容的累计次数。
	ContainerRequestedResizes = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ContainerRequestedResizesKey,
			Help:           "Number of requested resizes, counted at the container level. Different resources on the same container are counted separately. The 'requirement' label refers to 'memory' or 'limits'; the 'operation' label can be one of 'add', 'remove', 'increase' or 'decrease'.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource", "requirement", "operation"},
	)

	// PodResizeDurationMilliseconds 跟踪 Pod 扩缩容所耗时长（毫秒）。
	PodResizeDurationMilliseconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResizeDurationMillisecondsKey,
			Help:           "Duration in milliseconds to actuate a pod resize",
			Buckets:        podResizeDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"success"},
	)

	// PodPendingResizes 跟踪待处理的 Pod 扩缩容数量。
	PodPendingResizes = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodPendingResizesKey,
			Help:           "Number of pending resizes for pods. Label 'priority_bucket' classifies the pod priority (system-critical: >=2000000000, high: 100000..1999999999, medium: 1..99999, normal: 0/default, low: -999..-1, very-low: <=-1000, unknown: nil pod). Label 'reason' describes the state (deferred, infeasible).",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason", "priority_bucket"},
	)

	// PodInfeasibleResizes 跟踪无法执行的 Pod 扩缩容数量。
	PodInfeasibleResizes = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodInfeasibleResizesKey,
			Help:           "Number of infeasible resizes for pods.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"reason_detail"},
	)

	// PodInProgressResizes 跟踪进行中的 Pod 扩缩容数量。
	PodInProgressResizes = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodInProgressResizesKey,
			Help:           "Number of in-progress resizes for pods.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PodDeferredAcceptedResizes 跟踪已接受但被延迟的 Pod 扩缩容累计次数。
	PodDeferredAcceptedResizes = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodDeferredAcceptedResizesKey,
			Help:           "Cumulative number of resizes that were accepted after being deferred.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"retry_trigger"},
	)

	// PodDeferredResizeDurationSeconds 跟踪 Pod 在完成前保持延迟状态的时长（秒）。
	PodDeferredResizeDurationSeconds = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodDeferredResizeDurationSecondsKey,
			Help:           "Duration in seconds that a pod resize remains deferred before completion. Label 'priority_bucket' classifies the pod priority (system-critical: >=2000000000, high: 100000..1999999999, medium: 1..99999, normal: 0/default, low: -999..-1, very-low: <=-1000, unknown: nil pod). Label 'resolution' describes how the deferred resize was resolved (accepted, reverted, terminated).",
			Buckets:        podDeferredResizeDurationBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resolution", "priority_bucket"},
	)

	// ResourceManagerAllocationsTotal 统计资源管理器执行的排他资源分配总数，
	// 其中 `source` 标签用于区分分配是来自节点级池还是预分配的 Pod 级池，
	// 即区分二者来源。
	ResourceManagerAllocationsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ResourceManagerAllocationTotalKey,
			Help:           "Number of exclusive resource allocations performed by a resource manager.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name", "source"},
	)

	// ResourceManagerAllocationErrorsTotal 统计排他资源分配过程中遇到的错误，
	// 并按预期的分配来源加以区分。
	ResourceManagerAllocationErrorsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ResourceManagerAllocationErrorsTotalKey,
			Help:           "Number of errors encountered during exclusive resource allocation.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name", "source"},
	)

	// ResourceManagerContainerAssignments 统计具有特定类型资源分配的容器总数，
	// 用于观察有多少容器是以排他资源（来自节点池或 Pod 池）分配的，
	// 以及有多少是分配了 Pod 级共享池资源，
	// 二者对比可见。
	ResourceManagerContainerAssignments = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           ResourceManagerContainerAssignmentsTotalKey,
			Help:           "Number of containers with a specific type of resource assignment.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name", "assignment_type"},
	)

	// PodWatchEventsDroppedTotal 跟踪被丢弃的 Pod watch 事件数量。
	PodWatchEventsDroppedTotal = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodWatchEventsDroppedKey,
			Help:           "Cumulative number of pod watch events dropped.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PodRequestsTotal 跟踪 PodsAPI 端点的累计请求次数。
	PodRequestsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodRequestsTotalKey,
			Help:           "Cumulative number of requests to the PodsAPI endpoint. Broken down by server api version and status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version", "status_code"},
	)

	// PodRequestsList 跟踪 ListPods 端点的累计请求次数。
	PodRequestsList = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodRequestsListKey,
			Help:           "Number of requests to the PodsAPI List endpoint. Broken down by server api version and status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version", "status_code"},
	)

	// PodRequestsGet 跟踪 GetPod 端点的累计请求次数。
	PodRequestsGet = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodRequestsGetKey,
			Help:           "Number of requests to the PodsAPI Get endpoint. Broken down by server api version and status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version", "status_code"},
	)

	// PodRequestsWatch 跟踪 WatchPods 端点的累计请求次数。
	PodRequestsWatch = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodRequestsWatchKey,
			Help:           "Number of requests to the PodsAPI Watch endpoint. Broken down by server api version and status code.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version", "status_code"},
	)
	// PodLevelResourcesAdmissionTotal 跟踪 Pod 准入时 KEP-2837（Pod 级资源）特性的采用情况。
	// 仅记录已准入的 Pod。
	// config_mode 按 Pod 上资源的声明方式分类：
	// pod_and_container_level：同时设置了 Pod 级与容器级资源。
	// pod_level：仅设置了 Pod 级资源。
	// container_level：仅设置了容器级资源。
	// 空值：未设置 Pod 级与容器级资源（BestEffort QoS）。
	// 基数有上限：最多 12 条序列（4 种 config_mode × 3 种 qos_class）。
	// 注意：该指标为 ALPHA 级且为临时指标。它仅用于在特性尚新时跟踪采用情况，
	// 计划在 Pod 级资源特性达到 GA 后 2-3 个版本内移除。
	PodLevelResourcesAdmissionTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodLevelResourcesAdmissionTotalKey,
			Help:           "Total number of pods admitted during Kubelet admission, categorized by resource configuration mode (pod_and_container_level, pod_level, container_level, or empty for unconfigured/BestEffort) and QoS class.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"config_mode", "qos_class"},
	)
)

var registerMetrics sync.Once

// Register 注册全部指标。
func Register() {
	// 执行指标注册。
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(FirstNetworkPodStartSLIDuration)
		legacyregistry.MustRegister(NodeName)
		legacyregistry.MustRegister(PodWorkerDuration)
		legacyregistry.MustRegister(PodStartDuration)
		legacyregistry.MustRegister(PodStartSLIDuration)
		legacyregistry.MustRegister(PodStartTotalDuration)
		legacyregistry.MustRegister(ImagePullDuration)
		legacyregistry.MustRegister(ImageGarbageCollectedTotal)
		legacyregistry.MustRegister(NodeStartupPreKubeletDuration)
		legacyregistry.MustRegister(NodeStartupPreRegistrationDuration)
		legacyregistry.MustRegister(NodeStartupRegistrationDuration)
		legacyregistry.MustRegister(NodeStartupPostRegistrationDuration)
		legacyregistry.MustRegister(NodeStartupDuration)
		legacyregistry.MustRegister(CgroupManagerDuration)
		legacyregistry.MustRegister(PodWorkerStartDuration)
		legacyregistry.MustRegister(PodStatusSyncDuration)
		legacyregistry.MustRegister(ContainersPerPodCount)
		legacyregistry.MustRegister(PLEGRelistDuration)
		legacyregistry.MustRegister(PLEGDiscardEvents)
		legacyregistry.MustRegister(PLEGRelistInterval)
		legacyregistry.MustRegister(PLEGLastSeen)
		legacyregistry.MustRegister(PLEGPodRelistDuration)
		legacyregistry.MustRegister(EventedPLEGConnErr)
		legacyregistry.MustRegister(EventedPLEGConn)
		legacyregistry.MustRegister(EventedPLEGConnLatency)
		legacyregistry.MustRegister(RuntimeOperations)
		legacyregistry.MustRegister(RuntimeOperationsDuration)
		legacyregistry.MustRegister(RuntimeOperationsErrors)
		legacyregistry.MustRegister(Evictions)
		legacyregistry.MustRegister(EvictionStatsAge)
		legacyregistry.MustRegister(Preemptions)
		legacyregistry.MustRegister(DevicePluginRegistrationCount)
		legacyregistry.MustRegister(DevicePluginAllocationDuration)
		legacyregistry.MustRegister(RunningContainerCount)
		legacyregistry.MustRegister(RunningPodCount)
		legacyregistry.MustRegister(DesiredPodCount)
		legacyregistry.MustRegister(ActivePodCount)
		legacyregistry.MustRegister(MirrorPodCount)
		legacyregistry.MustRegister(WorkingPodCount)
		legacyregistry.MustRegister(OrphanedRuntimePodTotal)
		legacyregistry.MustRegister(RestartedPodTotal)
		legacyregistry.MustRegister(ManagedEphemeralContainers)
		legacyregistry.MustRegister(PodResourcesEndpointRequestsTotalCount)
		legacyregistry.MustRegister(PodResourcesEndpointRequestsListCount)
		legacyregistry.MustRegister(PodResourcesEndpointRequestsGetAllocatableCount)
		legacyregistry.MustRegister(PodResourcesEndpointErrorsListCount)
		legacyregistry.MustRegister(PodResourcesEndpointErrorsGetAllocatableCount)
		legacyregistry.MustRegister(PodResourcesEndpointRequestsGetCount)
		legacyregistry.MustRegister(PodResourcesEndpointErrorsGetCount)
		if utilfeature.DefaultFeatureGate.Enabled(features.UserNamespacesSupport) {
			legacyregistry.MustRegister(StartedUserNamespacedPodsTotal)
			legacyregistry.MustRegister(StartedUserNamespacedPodsErrorsTotal)
		}
		legacyregistry.MustRegister(StartedPodsTotal)
		legacyregistry.MustRegister(StartedPodsErrorsTotal)
		legacyregistry.MustRegister(StartedContainersTotal)
		legacyregistry.MustRegister(TerminatedContainersTotal)
		legacyregistry.MustRegister(StartedContainersErrorsTotal)
		legacyregistry.MustRegister(StartedHostProcessContainersTotal)
		legacyregistry.MustRegister(StartedHostProcessContainersErrorsTotal)
		legacyregistry.MustRegister(RunPodSandboxDuration)
		legacyregistry.MustRegister(RunPodSandboxErrors)
		legacyregistry.MustRegister(CPUManagerPinningRequestsTotal)
		legacyregistry.MustRegister(CPUManagerPinningErrorsTotal)
		legacyregistry.MustRegister(CPUManagerSharedPoolSizeMilliCores)
		legacyregistry.MustRegister(CPUManagerExclusiveCPUsAllocationCount)
		legacyregistry.MustRegister(CPUManagerAllocationPerNUMA)
		legacyregistry.MustRegister(ContainerAlignedComputeResources)
		legacyregistry.MustRegister(ContainerAlignedComputeResourcesFailure)
		legacyregistry.MustRegister(MemoryManagerPinningRequestTotal)
		legacyregistry.MustRegister(MemoryManagerPinningErrorsTotal)
		if utilfeature.DefaultFeatureGate.Enabled(features.MemoryQoS) {
			legacyregistry.MustRegister(MemoryQoSNodeMemoryMinBytes)
			legacyregistry.MustRegister(MemoryQoSNodeMemoryLowBytes)
		}
		legacyregistry.MustRegister(TopologyManagerAdmissionRequestsTotal)
		legacyregistry.MustRegister(TopologyManagerAdmissionErrorsTotal)
		legacyregistry.MustRegister(TopologyManagerAdmissionDuration)
		legacyregistry.MustRegister(OrphanPodCleanedVolumes)
		legacyregistry.MustRegister(OrphanPodCleanedVolumesErrors)

		if utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdown) &&
			utilfeature.DefaultFeatureGate.Enabled(features.GracefulNodeShutdownBasedOnPodPriority) {
			legacyregistry.MustRegister(GracefulShutdownStartTime)
			legacyregistry.MustRegister(GracefulShutdownEndTime)
		}

		legacyregistry.MustRegister(LifecycleHandlerHTTPFallbacks)
		legacyregistry.MustRegister(LifecycleHandlerSleepTerminated)
		legacyregistry.MustRegister(CgroupVersion)

		if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
			legacyregistry.MustRegister(
				DRAOperationsDuration,
				DRAGRPCOperationsDuration,
				DRANodePrepareSkipsTotal,
				DRANodeUnprepareSkipsTotal,
			)
		}

		legacyregistry.MustRegister(AdmissionRejectionsTotal)

		if utilfeature.DefaultFeatureGate.Enabled(features.ImageVolume) {
			legacyregistry.MustRegister(ImageVolumeRequestedTotal)
			legacyregistry.MustRegister(ImageVolumeMountedSucceedTotal)
			legacyregistry.MustRegister(ImageVolumeMountedErrorsTotal)
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScaling) {
			legacyregistry.MustRegister(ContainerRequestedResizes)
			legacyregistry.MustRegister(PodResizeDurationMilliseconds)
			legacyregistry.MustRegister(PodPendingResizes)
			legacyregistry.MustRegister(PodInfeasibleResizes)
			legacyregistry.MustRegister(PodInProgressResizes)
			legacyregistry.MustRegister(PodDeferredAcceptedResizes)
			legacyregistry.MustRegister(PodDeferredResizeDurationSeconds)
		}

		if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResourceManagers) {
			legacyregistry.MustRegister(ResourceManagerAllocationsTotal)
			legacyregistry.MustRegister(ResourceManagerAllocationErrorsTotal)
			legacyregistry.MustRegister(ResourceManagerContainerAssignments)
		}

		legacyregistry.MustRegister(PodWatchEventsDroppedTotal)
		legacyregistry.MustRegister(PodRequestsTotal)
		legacyregistry.MustRegister(PodRequestsList)
		legacyregistry.MustRegister(PodRequestsGet)
		legacyregistry.MustRegister(PodRequestsWatch)
		if utilfeature.DefaultFeatureGate.Enabled(features.PodLevelResources) {
			legacyregistry.MustRegister(PodLevelResourcesAdmissionTotal)
		}
	})
}

// RegisterCollectors 将给定的稳定指标采集器注册到 legacyregistry。
func RegisterCollectors(collectors ...metrics.StableCollector) {
	legacyregistry.CustomMustRegister(collectors...)
}

// GetGather 返回指标采集器，供本包之外的测试使用。
func GetGather() metrics.Gatherer {
	return legacyregistry.DefaultGatherer
}

// SinceInSeconds 返回自指定起始时间以来的秒数。
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// SetNodeName 将 NodeName Gauge 设置为 1。
func SetNodeName(name types.NodeName) {
	NodeName.WithLabelValues(string(name)).Set(1)
}

// GetImageSizeBucket 将镜像字节大小映射到预先定义的镜像大小桶标签；
// 大小为 0 时返回 "N/A"，无法匹配任何桶时返回空字符串。
func GetImageSizeBucket(sizeInBytes uint64) string {
	if sizeInBytes == 0 {
		return "N/A"
	}

	for i := len(imageSizeBuckets) - 1; i >= 0; i-- {
		if sizeInBytes > imageSizeBuckets[i].lowerBoundInBytes {
			return imageSizeBuckets[i].label
		}
	}

	// 当 sizeInBytes 为 0（获取镜像大小出错）时返回空字符串
	return ""
}

// GetPriorityBucketLabel 根据 Pod 的优先级值返回对应的优先级桶标签。
// 映射关系：
// system-critical：priority >= 2000000000（SystemCriticalPriority）
// high：100000 <= priority < 2000000000
// medium：1 <= priority < 100000
// normal：priority == 0 或使用默认值（DefaultPriorityWhenNoDefaultClassExists）
// low：-999 <= priority <= -1
// very-low：priority <= -1000
// unknown：Pod 为 nil
func GetPriorityBucketLabel(pod *v1.Pod) PriorityBucket {
	if pod == nil {
		return PriorityBucketUnknown
	}
	if pod.Spec.Priority == nil || *pod.Spec.Priority == scheduling.DefaultPriorityWhenNoDefaultClassExists {
		return PriorityBucketNormal
	}
	p := *pod.Spec.Priority
	switch {
	case p >= scheduling.SystemCriticalPriority:
		return PriorityBucketSystemCritical
	case p >= 100000:
		return PriorityBucketHigh
	case p > 0:
		return PriorityBucketMedium
	case p > -1000:
		return PriorityBucketLow
	default:
		return PriorityBucketVeryLow
	}
}
