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

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

// 该 const 块定义 kubelet 各项指标的名称。
const (
	KubeletSubsystem           = "kubelet"
	NodeNameKey                = "node_name"
	NodeLabelKey               = "node"
	PodWorkerDurationKey       = "pod_worker_duration_seconds"
	PodStartDurationKey        = "pod_start_duration_seconds"
	CgroupManagerOperationsKey = "cgroup_manager_duration_seconds"
	PodWorkerStartDurationKey  = "pod_worker_start_duration_seconds"
	PLEGRelistDurationKey      = "pleg_relist_duration_seconds"
	PLEGDiscardEventsKey       = "pleg_discard_events"
	PLEGRelistIntervalKey      = "pleg_relist_interval_seconds"
	PLEGLastSeenKey            = "pleg_last_seen_seconds"
	EvictionsKey               = "evictions"
	EvictionStatsAgeKey        = "eviction_stats_age_seconds"
	PreemptionsKey             = "preemptions"
	RunningPodsKey             = "running_pods"
	RunningContainersKey       = "running_containers"
	// 远程 runtime 操作的指标键
	RuntimeOperationsKey         = "runtime_operations_total"
	RuntimeOperationsDurationKey = "runtime_operations_duration_seconds"
	RuntimeOperationsErrorsKey   = "runtime_operations_errors_total"
	// device plugin 操作的指标键
	DevicePluginRegistrationCountKey  = "device_plugin_registration_total"
	DevicePluginAllocationDurationKey = "device_plugin_alloc_duration_seconds"
	// pod resources 操作的指标键
	PodResourcesEndpointRequestsTotalKey          = "pod_resources_endpoint_requests_total"
	PodResourcesEndpointRequestsListKey           = "pod_resources_endpoint_requests_list"
	PodResourcesEndpointRequestsGetAllocatableKey = "pod_resources_endpoint_requests_get_allocatable"
	PodResourcesEndpointErrorsListKey             = "pod_resources_endpoint_errors_list"
	PodResourcesEndpointErrorsGetAllocatableKey   = "pod_resources_endpoint_errors_get_allocatable"

	// RuntimeClass 相关的指标键
	RunPodSandboxDurationKey = "run_podsandbox_duration_seconds"
	RunPodSandboxErrorsKey   = "run_podsandbox_errors_total"
)

const (
	// 子系统名称。
	pvControllerSubsystem = "pv_collector"

	// 指标名称。
	totalPVKey    = "total_pv_count"
	boundPVKey    = "bound_pv_count"
	unboundPVKey  = "unbound_pv_count"
	boundPVCKey   = "bound_pvc_count"
	unboundPVCKey = "unbound_pvc_count"

	// 标签名称。
	namespaceLabel    = "namespace"
	storageClassLabel = "storage_class"
	pluginNameLabel   = "plugin_name"
	volumeModeLabel   = "volume_mode"

	// 无法确定插件名称时使用的字符串
	pluginNameNotAvailable = "N/A"
)

const (
	requestKind         = "request_kind"
	priorityLevel       = "priority_level"
	flowSchema          = "flow_schema"
	phase               = "phase"
	LabelNamePhase      = "phase"
	LabelValueWaiting   = "waiting"
	LabelValueExecuting = "executing"
)

var (
	defObjectives = map[float64]float64{0.5: 0.5, 0.75: 0.75}
	testBuckets   = []float64{0, 0.5, 1.0}
	testLabels    = []string{"a", "b", "c"}
	maxAge        = 2 * time.Minute

	// NodeName 是记录节点名称的 Gauge，样本数恒为 1。
	NodeName = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           NodeNameKey,
			Help:           "The node's name. The count is always 1.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{NodeLabelKey},
	)
	// ContainersPerPodCount 是统计每个 Pod 中容器数量的 Histogram。
	ContainersPerPodCount = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "containers_per_pod_count",
			Help:           "The number of containers per pod.",
			Buckets:        testBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PodWorkerDuration 是记录同步单个 Pod 耗时（秒）的 Histogram。
	// 按操作类型细分。
	PodWorkerDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodWorkerDurationKey,
			Help:           "Duration in seconds to sync a single pod. Broken down by operation type: create, update, or sync",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		testLabels,
	)
	// healthcheck 是记录 k8s 健康检查结果的 Prometheus Gauge 指标。
	healthcheck = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Namespace:      "kubernetes",
			Name:           "healthcheck",
			Help:           "This metric records the result of a single healthcheck.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"name", "type"},
	)

	// healthchecksTotal 是统计 k8s 健康检查结果的 Prometheus Counter 指标。
	healthchecksTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Namespace:      "kubernetes",
			Name:           "healthchecks_total",
			Help:           "This metric records the results of all healthcheck.",
			StabilityLevel: metrics.BETA,
		},
		[]string{"name", "type", "status"},
	)
	// PodWorkerDuration 是记录同步单个 Pod 耗时（秒）的 Histogram。
	// 按操作类型细分。
	SummaryMaxAge = metrics.NewSummary(
		&metrics.SummaryOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "max_age",
			Help:           "Duration in seconds to sync a single pod. Broken down by operation type: create, update, or sync",
			StabilityLevel: metrics.BETA,
			MaxAge:         2 * time.Hour,
		},
	)

	// PodWorkerDuration 是记录同步单个 Pod 耗时（秒）的 Histogram。
	// 按操作类型细分。
	SummaryMaxAgeConst = metrics.NewSummary(
		&metrics.SummaryOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "max_age_const",
			Help:           "Duration in seconds to sync a single pod. Broken down by operation type: create, update, or sync",
			StabilityLevel: metrics.BETA,
			MaxAge:         maxAge,
		},
	)
	// PodStartDuration 是记录单个 Pod 从 Pending 到 Running 耗时（秒）的 Histogram。
	PodStartDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodStartDurationKey,
			Help:           "Duration in seconds for a single pod to go from pending to running.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// CgroupManagerDuration 是记录 cgroup manager 操作耗时（秒）的 Histogram。
	// 按方法细分。
	CgroupManagerDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           CgroupManagerOperationsKey,
			Help:           "Duration in seconds for cgroup manager operations. Broken down by method.",
			Buckets:        metrics.ExponentialBucketsRange(0.01, 10, 10),
			StabilityLevel: metrics.BETA,
		},
		[]string{"operation_type"},
	)
	// PodWorkerStartDuration 是记录从发现 Pod 到启动 worker 耗时（秒）的 Histogram。
	PodWorkerStartDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodWorkerStartDurationKey,
			Help:           "Duration in seconds from seeing a pod to starting a worker.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PriorityLevelExecutionSeatsGaugeVec 观察各优先级在执行期间占用的席位数
	PriorityLevelExecutionSeatsGaugeVec = metrics.NewTimingHistogramVec(
		&metrics.TimingHistogramOpts{
			Namespace: "namespace",
			Subsystem: "subsystem",
			Name:      "priority_level_seat_utilization",
			Help:      "Observations, at the end of every nanosecond, of utilization of seats for any stage of execution (but only initial stage for WATCHes)",
			// 桶边界包含 0.99 和 1.0，使 PromQL 的 histogram_quantile 能反映饱和状态
			Buckets:        []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1},
			ConstLabels:    map[string]string{phase: "executing"},
			StabilityLevel: metrics.BETA,
		},
		[]string{"priorityLevel"},
	)

	// PriorityLevelExecutionSeatsGaugeVec 观察各优先级在执行期间占用的席位数
	TestConstLabels = metrics.NewTimingHistogramVec(
		&metrics.TimingHistogramOpts{
			Namespace: "test",
			Subsystem: "const",
			Name:      "label",
			Help:      "Observations, at the end of every nanosecond, of utilization of seats for any stage of execution (but only initial stage for WATCHes)",
			// 桶边界包含 0.99 和 1.0，使 PromQL 的 histogram_quantile 能反映饱和状态
			Buckets:        []float64{0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.99, 1},
			ConstLabels:    map[string]string{"somestring": "executing", phase: "blah"},
			StabilityLevel: metrics.BETA,
		},
		[]string{"priorityLevel"},
	)
	// PLEGRelistDuration 是记录 kubelet 的 Pod 生命周期事件生成器（PLEG）
	// 重新列举 Pod 耗时（秒）的 Histogram。
	PLEGRelistDuration = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGRelistDurationKey,
			Help:           "Duration in seconds for relisting pods in PLEG.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
	)
	// PLEGDiscardEvents 是统计 kubelet PLEG 丢弃事件数的 Counter。
	PLEGDiscardEvents = metrics.NewCounter(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGDiscardEventsKey,
			Help:           "The number of discard events in PLEG.",
			StabilityLevel: metrics.ALPHA,
		},
	)

	// PLEGRelistInterval 是记录 kubelet PLEG 相邻两次重新列举间隔（秒）的
	// 重新列举 Pod 耗时（秒）的 Histogram。
	PLEGRelistInterval = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "test_histogram_metric",
			Help:           "Interval in seconds between relisting in PLEG.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.STABLE,
		},
	)
	// PLEGLastSeen 是记录 kubelet PLEG 最后一次活跃的
	// Unix 时间戳的 Gauge。
	PLEGLastSeen = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PLEGLastSeenKey,
			Help:           "Timestamp in seconds when PLEG was last seen active.",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// RuntimeOperations 是统计远程 runtime 操作累计次数的 Counter。
	// 按操作类型细分。
	RuntimeOperations = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsKey,
			Help:           "Cumulative number of runtime operations by operation type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// RuntimeOperationsDuration 是记录远程 runtime 操作耗时（秒）的 Histogram。
	// 按操作类型细分。
	RuntimeOperationsDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsDurationKey,
			Help:           "Duration in seconds of runtime operations. Broken down by operation type.",
			Buckets:        metrics.ExponentialBuckets(.005, 2.5, 14),
			StabilityLevel: metrics.BETA,
		},
		[]string{"operation_type"},
	)
	// RuntimeOperationsErrors 是统计远程 runtime 操作累计错误数的 Counter。
	// 按操作类型细分。
	RuntimeOperationsErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RuntimeOperationsErrorsKey,
			Help:           "Cumulative number of runtime operation errors by operation type.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"operation_type"},
	)
	// Evictions 是统计 kubelet 发起的 Pod 驱逐累计次数的 Counter。
	// 按驱逐信号细分。
	Evictions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           EvictionsKey,
			Help:           "Cumulative number of pod evictions by eviction signal",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"eviction_signal"},
	)
	// EvictionStatsAge 是记录采集统计数据到据此驱逐 Pod 之间间隔（秒）的 Histogram，
	// 按驱逐信号细分。
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
	// Preemptions 是统计 kubelet 发起的 Pod 抢占累计次数的 Counter。
	// 按抢占信号细分。一次抢占只记录一种资源，所有信号之和
	// 即为该节点上的抢占次数。
	Preemptions = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PreemptionsKey,
			Help:           "Cumulative number of pod preemptions by preemption resource",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"preemption_signal"},
	)
	// MultiLineHelp 用于验证能否解析多行字符串
	MultiLineHelp = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem: KubeletSubsystem,
			Name:      "multiline",
			Help: "Cumulative number of pod preemptions by preemption resource " +
				"asdf asdf asdf " +
				"asdfas dfasdf",
			StabilityLevel: metrics.STABLE,
		},
		[]string{"preemption_signal"},
	)
	// DevicePluginRegistrationCount 是统计 device plugin 注册累计次数的 Counter。
	// 按资源名称细分。
	DevicePluginRegistrationCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           DevicePluginRegistrationCountKey,
			Help:           "Cumulative number of device plugin registrations. Broken down by resource name.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"resource_name"},
	)
	// DevicePluginAllocationDuration 是记录处理 device plugin 分配请求耗时（秒）的 Histogram。
	// 按资源名称细分。
	DevicePluginAllocationDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem:      KubeletSubsystem,
			Name:           DevicePluginAllocationDurationKey,
			Help:           "Duration in seconds to serve a device plugin Allocation request. Broken down by resource name.",
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.BETA,
		},
		[]string{"resource_name"},
	)

	// TestSummary 是统计 device plugin 注册累计次数的 Summary。
	// 按资源名称细分。
	TestSummary = metrics.NewSummary(
		&metrics.SummaryOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "summary_metric_test",
			Help:           "Cumulative number of device plugin registrations. Broken down by resource name.",
			StabilityLevel: metrics.STABLE,
		},
	)
	// TestSummaryVec 是记录处理 device plugin 分配请求耗时（秒）的 SummaryVec。
	// 按资源名称细分。
	TestSummaryVec = metrics.NewSummaryVec(
		&metrics.SummaryOpts{
			Subsystem:      KubeletSubsystem,
			Name:           "summary_vec_metric_test",
			Help:           "Duration in seconds to serve a device plugin Allocation request. Broken down by resource name.",
			Objectives:     defObjectives,
			MaxAge:         metrics.DefMaxAge,
			AgeBuckets:     metrics.DefAgeBuckets,
			BufCap:         metrics.DefBufCap,
			StabilityLevel: metrics.STABLE,
		},
		[]string{"resource_name"},
	)

	// PodResourcesEndpointRequestsTotalCount 是统计 PodResource 各端点请求累计次数的 Counter。
	// 按服务端 API 版本细分。
	PodResourcesEndpointRequestsTotalCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsTotalKey,
			Help:           "Cumulative number of requests to the PodResource endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointRequestsListCount 是统计 PodResource List() 端点请求数的 Counter。
	// 按服务端 API 版本细分。
	PodResourcesEndpointRequestsListCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsListKey,
			Help:           "Number of requests to the PodResource List endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointRequestsGetAllocatableCount 是统计 PodResource GetAllocatableResources() 端点请求数的 Counter。
	// 按服务端 API 版本细分。
	PodResourcesEndpointRequestsGetAllocatableCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointRequestsGetAllocatableKey,
			Help:           "Number of requests to the PodResource GetAllocatableResources endpoint. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointErrorsListCount 是统计 PodResource List() 端点返回错误数的 Counter。
	// 按服务端 API 版本细分。
	PodResourcesEndpointErrorsListCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointErrorsListKey,
			Help:           "Number of requests to the PodResource List endpoint which returned error. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// PodResourcesEndpointErrorsGetAllocatableCount 是统计 PodResource GetAllocatableResources() 端点返回错误数的 Counter。
	// 按服务端 API 版本细分。
	PodResourcesEndpointErrorsGetAllocatableCount = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           PodResourcesEndpointErrorsGetAllocatableKey,
			Help:           "Number of requests to the PodResource GetAllocatableResources endpoint which returned error. Broken down by server api version.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"server_api_version"},
	)

	// RunPodSandboxDuration 是记录运行 Pod Sandbox 操作耗时（秒）的 Histogram。
	// 按 RuntimeClass.Handler 细分。
	RunPodSandboxDuration = metrics.NewHistogramVec(
		&metrics.HistogramOpts{
			Subsystem: KubeletSubsystem,
			Name:      RunPodSandboxDurationKey,
			Help:      "Duration in seconds of the run_podsandbox operations. Broken down by RuntimeClass.Handler.",
			// 暂用 DefBuckets，必要时再定制桶边界。
			Buckets:        metrics.DefBuckets,
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"runtime_handler"},
	)
	// RunPodSandboxErrors 是统计 Pod Sandbox 操作累计错误数的 Counter。
	// 按 RuntimeClass.Handler 细分。
	RunPodSandboxErrors = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunPodSandboxErrorsKey,
			Help:           "Cumulative number of the run_podsandbox operation errors by RuntimeClass.Handler.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"runtime_handler"},
	)

	// RunningPodCount 是记录当前 sandbox 正在运行的 Pod 数量的 Gauge，
	// 用于暴露 kubelet 内部状态（容器运行时中有多少个 Pod 在运行容器），主要供调试使用。
	RunningPodCount = metrics.NewGauge(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunningPodsKey,
			Help:           "Number of pods that have a running pod sandbox",
			StabilityLevel: metrics.ALPHA,
		},
	)
	// RunningContainerCount 是记录当前运行中容器数量的 Gauge
	RunningContainerCount = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      KubeletSubsystem,
			Name:           RunningContainersKey,
			Help:           "Number of containers currently running",
			StabilityLevel: metrics.BETA,
		},
		[]string{"container_state"},
	)

	NetworkProgrammingLatency = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: "kube_proxy",
			Name:      "network_programming_duration_seconds",
			Help:      "In Cluster Network Programming Latency in seconds",
			Buckets: merge(
				metrics.LinearBuckets(0.25, 0.25, 2), // 0.25s, 0.50s
				metrics.LinearBuckets(1, 1, 59),      // 1s, 2s, 3s, ... 59s
				metrics.LinearBuckets(60, 5, 12),     // 60s, 65s, 70s, ... 115s
				metrics.LinearBuckets(120, 30, 7),    // 2min, 2.5min, 3min, ..., 5min
			),
			StabilityLevel: metrics.BETA,
		},
	)

	NetworkProgrammingLatency2 = metrics.NewHistogram(
		&metrics.HistogramOpts{
			Subsystem: "kube_proxy",
			Name:      "network_programming_duration_seconds2",
			Help:      "In Cluster Network Programming Latency in seconds",
			Buckets: metrics.MergeBuckets(
				metrics.LinearBuckets(0.25, 0.25, 2), // 0.25s, 0.50s
				[]float64{1, 5, 10, 59},              // 1s, 2s, 3s, ... 59s
				metrics.LinearBuckets(60, 5, 12),     // 60s, 65s, 70s, ... 115s
				metrics.LinearBuckets(120, 30, 7),    // 2min, 2.5min, 3min, ..., 5min
			),
			StabilityLevel: metrics.BETA,
		},
	)

	volumeManagerTotalVolumes = "volume_manager_total_volumes"

	_ = metrics.NewDesc(
		volumeManagerTotalVolumes,
		"Number of volumes in Volume Manager",
		[]string{"plugin_name", "state"},
		nil,
		metrics.STABLE, "",
	)

	_ = metrics.NewDesc(
		metrics.BuildFQName("test", "beta", "desc"),
		"Number of volumes in Volume Manager",
		nil,
		map[string]string{"alalala": "lalalal"},
		metrics.BETA, "",
	)
	_ = metrics.NewDesc(
		"test_desc_alpha",
		"Number of volumes in Volume Manager",
		[]string{"plugin_name", "state"},
		map[string]string{"alalala": "lalalal"},
		metrics.ALPHA, "",
	)

	_ = metrics.NewDesc(
		metrics.BuildFQName("", KubeletSubsystem, "volume_stats_capacity_bytes"),
		"Capacity in bytes of the volume",
		[]string{"namespace", "persistentvolumeclaim"}, nil,
		metrics.BETA, "",
	)
)

var registerMetrics sync.Once

// Register 注册所有指标。
func Register(collectors ...metrics.StableCollector) {
	// 注册指标。
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(NodeName)
		legacyregistry.MustRegister(PodWorkerDuration)
		legacyregistry.MustRegister(PodStartDuration)
		legacyregistry.MustRegister(healthcheck)
		legacyregistry.MustRegister(healthchecksTotal)
		legacyregistry.MustRegister(CgroupManagerDuration)
		legacyregistry.MustRegister(PodWorkerStartDuration)
		legacyregistry.MustRegister(ContainersPerPodCount)
		legacyregistry.MustRegister(PLEGRelistDuration)
		legacyregistry.MustRegister(PLEGDiscardEvents)
		legacyregistry.MustRegister(PLEGRelistInterval)
		legacyregistry.MustRegister(PLEGLastSeen)
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
		legacyregistry.MustRegister(RunPodSandboxDuration)
		legacyregistry.MustRegister(RunPodSandboxErrors)
		legacyregistry.MustRegister(NetworkProgrammingLatency)
		for _, collector := range collectors {
			legacyregistry.CustomMustRegister(collector)
		}
		legacyregistry.RawMustRegister(metrics.NewGaugeFunc(
			&metrics.GaugeOpts{
				Subsystem: "kubelet",
				Name:      "certificate_manager_client_ttl_seconds",
				Help: "Gauge of the TTL (time-to-live) of the Kubelet's client certificate. " +
					"The value is in seconds until certificate expiry (negative if already expired). " +
					"If client certificate is invalid or unused, the value will be +INF.",
				StabilityLevel: metrics.BETA,
			},
			func() float64 {
				return 0
			},
		))
		_ = metrics.Labels{
			"probe_type": "1",
			"container":  "2",
			"pod":        "podName",
			"namespace":  "space",
			"pod_uid":    "123",
		}
	})
}

// GetGather 返回 gatherer，供包外测试用例使用。
func GetGather() metrics.Gatherer {
	return legacyregistry.DefaultGatherer
}

// SinceInSeconds 返回自指定起始时刻以来的秒数。
func SinceInSeconds(start time.Time) float64 {
	return time.Since(start).Seconds()
}

// SetNodeName 将 NodeName Gauge 设为 1。
func SetNodeName(name types.NodeName) {
	NodeName.WithLabelValues(string(name)).Set(1)
}

func Blah() metrics.ObserverMetric {
	return EvictionStatsAge.With(metrics.Labels{"plugins": "ASDf"})
}

func merge(slices ...[]float64) []float64 {
	result := make([]float64, 1)
	for _, s := range slices {
		result = append(result, s...)
	}
	return result
}
