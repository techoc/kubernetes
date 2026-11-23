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

// 本包定义 kubelet 统计信息的 API 类型，是 StatsSummary API 的对外数据模型。
// 这些类型由 cAdvisor/CRI 采样后组装，最终通过 kubelet 的 /stats/summary 端点暴露，
// 供 HPA、kubectl top 等消费方读取。
package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Summary is a top-level container for holding NodeStats and PodStats.
//
// Summary 是统计接口的顶层聚合结果：节点级统计与所有 Pod 统计放在同一个响应里，
// 消费方一次请求就能拿到完整快照，避免逐 Pod 请求带来的开销。
type Summary struct {
	// Overall node stats.
	Node NodeStats `json:"node"`
	// Per-pod stats.
	Pods []PodStats `json:"pods"`
}

// NodeStats holds node-level unprocessed sample stats.
//
// NodeStats 保存节点级的原始采样值（累计量为主），消费方若要算速率需自行用两次采样做差。
// 各资源字段用指针表示，便于区分“采集失败/不支持”和“取值为 0”。
type NodeStats struct {
	// Reference to the measured Node.
	NodeName string `json:"nodeName"`
	// Stats of system daemons tracked as raw containers.
	// The system containers are named according to the SystemContainer* constants.
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	SystemContainers []ContainerStats `json:"systemContainers,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// The time at which data collection for the node-scoped (i.e. aggregate) stats was (re)started.
	StartTime metav1.Time `json:"startTime"`
	// Stats pertaining to CPU resources.
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// Stats pertaining to memory (RAM) resources.
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// Stats pertaining to IO resources.
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// Stats pertaining to network resources.
	// +optional
	Network *NetworkStats `json:"network,omitempty"`
	// Stats pertaining to total usage of filesystem resources on the rootfs used by node k8s components.
	// NodeFs.Used is the total bytes used on the filesystem.
	// +optional
	Fs *FsStats `json:"fs,omitempty"`
	// Stats about the underlying container runtime.
	// +optional
	Runtime *RuntimeStats `json:"runtime,omitempty"`
	// Stats about the rlimit of system.
	// +optional
	Rlimit *RlimitStats `json:"rlimit,omitempty"`
	// Stats pertaining to swap resources. This is reported to non-windows systems only.
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// RlimitStats are stats rlimit of OS.
//
// RlimitStats 记录内核对进程/PID 数量的硬限制，供 kubelet 判断节点是否接近 PID 耗尽，
// 触发 Pod 驱逐前的关键判据。
type RlimitStats struct {
	Time metav1.Time `json:"time"`

	// The max number of extant process (threads, precisely on Linux) of OS. See RLIMIT_NPROC in getrlimit(2).
	// The operating system ceiling on the number of process IDs that can be assigned.
	// On Linux, tasks (either processes or threads) consume 1 PID each.
	MaxPID *int64 `json:"maxpid,omitempty"`
	// The number of running process (threads, precisely on Linux) in the OS.
	NumOfRunningProcesses *int64 `json:"curproc,omitempty"`
}

// RuntimeStats are stats pertaining to the underlying container runtime.
//
// RuntimeStats 把运行时占用的文件系统单独列出：镜像层与容器可写层可能共用同一文件系统，
// 也可能各自独立，分开统计便于定位磁盘被哪一类数据撑满。
type RuntimeStats struct {
	// Stats about the underlying filesystem where container images are stored.
	// This filesystem could be the same as the primary (root) filesystem.
	// Usage here refers to the total number of bytes occupied by images on the filesystem.
	// +optional
	ImageFs *FsStats `json:"imageFs,omitempty"`
	// Stats about the underlying filesystem where container's writeable layer is stored.
	// This filesystem could be the same as the primary (root) filesystem or the ImageFS.
	// Usage here refers to the total number of bytes occupied by the writeable layer on the filesystem.
	// +optional
	ContainerFs *FsStats `json:"containerFs,omitempty"`
}

// 系统容器名既用于标识统计归属，也用于与 cAdvisor 的容器层级对应；
// 之所以不直接用二进制名，是为了让消费方按固定枚举解析这些统计。
const (
	// SystemContainerKubelet is the container name for the system container tracking Kubelet usage.
	SystemContainerKubelet = "kubelet"
	// SystemContainerRuntime is the container name for the system container tracking the runtime (e.g. docker) usage.
	SystemContainerRuntime = "runtime"
	// SystemContainerMisc is the container name for the system container tracking non-kubernetes processes.
	SystemContainerMisc = "misc"
	// SystemContainerPods is the container name for the system container tracking user pods.
	SystemContainerPods = "pods"
	// SystemContainerWindowsGlobalCommitMemory (only used on Windows) is the container name for the system container
	// tracking global commit memory usage and is used for memory-pressure eviction.
	SystemContainerWindowsGlobalCommitMemory = "windows-global-commit-memory"
)

// ProcessStats are stats pertaining to processes.
//
// ProcessStats 只统计进程数量，是判断节点/Pod 是否触及 PID 限制的最小字段集；
// 用指针是为了在采集器未实现该功能时省略整个字段。
type ProcessStats struct {
	// Number of processes
	// +optional
	ProcessCount *uint64 `json:"process_count,omitempty"`
}

// PodStats holds pod-level unprocessed sample stats.
//
// PodStats 既包含 Pod cgroup 聚合值，也包含各容器的明细：聚合值覆盖 Pod 级开销，
// 所以不等于容器值简单相加。
type PodStats struct {
	// Reference to the measured Pod.
	PodRef PodReference `json:"podRef"`
	// The time at which data collection for the pod-scoped (e.g. network) stats was (re)started.
	StartTime metav1.Time `json:"startTime"`
	// Stats of containers in the measured pod.
	// +patchMergeKey=name
	// +patchStrategy=merge
	Containers []ContainerStats `json:"containers" patchStrategy:"merge" patchMergeKey:"name"`
	// Stats pertaining to CPU resources consumed by pod cgroup (which includes all containers' resource usage and pod overhead).
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// Stats pertaining to memory (RAM) resources consumed by pod cgroup (which includes all containers' resource usage and pod overhead).
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// Stats pertaining to IO resources consumed by pod cgroup (which includes all containers' resource usage and pod overhead).
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// Stats pertaining to network resources.
	// +optional
	Network *NetworkStats `json:"network,omitempty"`
	// Stats pertaining to volume usage of filesystem resources.
	// VolumeStats.UsedBytes is the number of bytes used by the Volume
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	VolumeStats []VolumeStats `json:"volume,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// EphemeralStorage reports the total filesystem usage for the containers and emptyDir-backed volumes in the measured Pod.
	// +optional
	EphemeralStorage *FsStats `json:"ephemeral-storage,omitempty"`
	// ProcessStats pertaining to processes.
	// +optional
	ProcessStats *ProcessStats `json:"process_stats,omitempty"`
	// Stats pertaining to swap resources. This is reported to non-windows systems only.
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// ContainerStats holds container-level unprocessed sample stats.
//
// ContainerStats 以容器为粒度组织统计；Rootfs 与 Logs 分开列出，
// 因为二者通常落在不同目录/限额上，排障时需要独立看各自用量。
type ContainerStats struct {
	// Reference to the measured container.
	Name string `json:"name"`
	// The time at which data collection for this container was (re)started.
	StartTime metav1.Time `json:"startTime"`
	// Stats pertaining to CPU resources.
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// Stats pertaining to memory (RAM) resources.
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// Stats pertaining to IO resources.
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// Metrics for Accelerators. Each Accelerator corresponds to one element in the array.
	Accelerators []AcceleratorStats `json:"accelerators,omitempty"`
	// Stats pertaining to container rootfs usage of filesystem resources.
	// Rootfs.UsedBytes is the number of bytes used for the container write layer.
	// +optional
	Rootfs *FsStats `json:"rootfs,omitempty"`
	// Stats pertaining to container logs usage of filesystem resources.
	// Logs.UsedBytes is the number of bytes used for the container logs.
	// +optional
	Logs *FsStats `json:"logs,omitempty"`
	// User defined metrics that are exposed by containers in the pod. Typically, we expect only one container in the pod to be exposing user defined metrics. In the event of multiple containers exposing metrics, they will be combined here.
	// +patchMergeKey=name
	// +patchStrategy=merge
	UserDefinedMetrics []UserDefinedMetric `json:"userDefinedMetrics,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// Stats pertaining to swap resources. This is reported to non-windows systems only.
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// PodReference contains enough information to locate the referenced pod.
//
// PodReference 同时带 UID，避免同名 Pod 重建后被误认为同一个对象。
type PodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// InterfaceStats contains resource value data about interface.
//
// InterfaceStats 的字节/错误计数都是网卡启动以来的累计值，
// 速率须由消费方对相邻采样点做差得出。
type InterfaceStats struct {
	// The name of the interface
	Name string `json:"name"`
	// Cumulative count of bytes received.
	// +optional
	RxBytes *uint64 `json:"rxBytes,omitempty"`
	// Cumulative count of receive errors encountered.
	// +optional
	RxErrors *uint64 `json:"rxErrors,omitempty"`
	// Cumulative count of bytes transmitted.
	// +optional
	TxBytes *uint64 `json:"txBytes,omitempty"`
	// Cumulative count of transmit errors encountered.
	// +optional
	TxErrors *uint64 `json:"txErrors,omitempty"`
}

// NetworkStats contains data about network resources.
//
// NetworkStats 内嵌 InterfaceStats 表达默认网卡，是为了兼容只关心默认网卡的旧消费方；
// Interfaces 则提供完整明细，二者数据会重复，不应累加。
type NetworkStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`

	// Stats for the default interface, if found
	InterfaceStats `json:""`

	Interfaces []InterfaceStats `json:"interfaces,omitempty"`
}

// CPUStats contains data about CPU usage.
//
// CPUStats 同时给出瞬时用量与累计用量：前者便于直接展示，
// 后者不含采样窗口信息，适合消费方自行计算任意时段的平均值。
type CPUStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// Total CPU usage (sum of all cores) averaged over the sample window.
	// The "core" unit can be interpreted as CPU core-nanoseconds per second.
	// +optional
	UsageNanoCores *uint64 `json:"usageNanoCores,omitempty"`
	// Cumulative CPU usage (sum of all cores) since object creation.
	// +optional
	UsageCoreNanoSeconds *uint64 `json:"usageCoreNanoSeconds,omitempty"`
	// CPU PSI stats.
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// MemoryStats contains data about memory usage.
//
// MemoryStats 区分 usage 与 workingSet：前者包含可回收的 cache，
// 后者更接近 cgroup 内存压力下真正会被回收的量，驱逐决策看后者。
type MemoryStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// Available memory for use.  This is defined as the memory limit - workingSetBytes.
	// If memory limit is undefined, the available bytes is omitted.
	// +optional
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	// Total memory in use. This includes all memory regardless of when it was accessed.
	// +optional
	UsageBytes *uint64 `json:"usageBytes,omitempty"`
	// The amount of working set memory. This includes recently accessed memory,
	// dirty memory, and kernel memory. WorkingSetBytes is <= UsageBytes
	// +optional
	WorkingSetBytes *uint64 `json:"workingSetBytes,omitempty"`
	// The amount of anonymous and swap cache memory (includes transparent
	// hugepages).
	// +optional
	RSSBytes *uint64 `json:"rssBytes,omitempty"`
	// Cumulative number of minor page faults.
	// +optional
	PageFaults *uint64 `json:"pageFaults,omitempty"`
	// Cumulative number of major page faults.
	// +optional
	MajorPageFaults *uint64 `json:"majorPageFaults,omitempty"`
	// Memory PSI stats.
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// IOStats contains data about IO usage.
//
// IOStats 当前仅承载 PSI 数据：cAdvisor 不能跨所有块设备给出可靠的读写速率，
// 用压力stall 信息比上报含义模糊的计数更有用。
type IOStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// IO PSI stats.
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// PSI statistics for an individual resource.
//
// PSIStats 区分 full 与 some：前者要求所有任务同时受阻，
// 后者只要部分任务受阻，两者结合才能判断资源压力是局部还是整体。
type PSIStats struct {
	// PSI data for all tasks in the cgroup.
	Full PSIData `json:"full"`
	// PSI data for some tasks in the cgroup.
	Some PSIData `json:"some"`
}

// PSI data for an individual resource.
//
// PSIData 的 avg10/60/300 覆盖不同时间尺度：短窗口反映突发压力，
// 长窗口过滤毛刺，便于消费方按自身需求选用。
type PSIData struct {
	// Total time duration for tasks in the cgroup have waited due to congestion.
	// Unit: nanoseconds.
	Total uint64 `json:"total"`
	// The average (in %) tasks have waited due to congestion over a 10 second window.
	Avg10 float64 `json:"avg10"`
	// The average (in %) tasks have waited due to congestion over a 60 second window.
	Avg60 float64 `json:"avg60"`
	// The average (in %) tasks have waited due to congestion over a 300 second window.
	Avg300 float64 `json:"avg300"`
}

// SwapStats contains data about memory usage
//
// SwapStats 只在非 Windows 节点上报：Windows 的交换语义与 Linux cgroup 不同，
// 强行统一会让消费方拿到含义错误的数据。
type SwapStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// Available swap memory for use.  This is defined as the <swap-limit> - <current-swap-usage>.
	// If swap limit is undefined, this value is omitted.
	// +optional
	SwapAvailableBytes *uint64 `json:"swapAvailableBytes,omitempty"`
	// Total swap memory in use.
	// +optional
	SwapUsageBytes *uint64 `json:"swapUsageBytes,omitempty"`
}

// AcceleratorStats contains stats for accelerators attached to the container.
//
// AcceleratorStats 描述单个加速卡；一个容器可挂多张卡，所以该类型出现在数组中，
// 并由 ID 区分具体设备。
type AcceleratorStats struct {
	// Make of the accelerator (nvidia, amd, google etc.)
	Make string `json:"make"`

	// Model of the accelerator (tesla-p100, tesla-k80 etc.)
	Model string `json:"model"`

	// ID of the accelerator.
	ID string `json:"id"`

	// Total accelerator memory.
	// unit: bytes
	MemoryTotal uint64 `json:"memoryTotal"`

	// Total accelerator memory allocated.
	// unit: bytes
	MemoryUsed uint64 `json:"memoryUsed"`

	// Percent of time over the past sample period (10s) during which
	// the accelerator was actively processing.
	DutyCycle uint64 `json:"dutyCycle"`
}

// VolumeStats contains data about Volume filesystem usage.
//
// VolumeStats 内嵌 FsStats 以复用文件系统字段；PVCRef 仅在使用 PVC 时填充，
// 让消费方可以关联到存储对象而不必反查 Pod spec。
type VolumeStats struct {
	// Embedded FsStats
	FsStats `json:""`
	// Name is the name given to the Volume
	// +optional
	Name string `json:"name,omitempty"`
	// Reference to the PVC, if one exists
	// +optional
	PVCRef *PVCReference `json:"pvcRef,omitempty"`
}

// PVCReference contains enough information to describe the referenced PVC.
//
// PVCReference 不含 UID，因为统计只需定位到当前 PVC 对象，
// 短生命周期的采样数据没必要绑定对象身份。
type PVCReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// FsStats contains data about filesystem usage.
//
// FsStats 同时给出容量/空闲与“本任务用量”：后者受共享文件系统影响，
// 不能用 CapacityBytes - AvailableBytes 反推，容器 rootfs 场景尤其明显。
type FsStats struct {
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// AvailableBytes represents the storage space available (bytes) for the filesystem.
	// +optional
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	// CapacityBytes represents the total capacity (bytes) of the filesystems underlying storage.
	// +optional
	CapacityBytes *uint64 `json:"capacityBytes,omitempty"`
	// UsedBytes represents the bytes used for a specific task on the filesystem.
	// This may differ from the total bytes used on the filesystem and may not equal CapacityBytes - AvailableBytes.
	// e.g. For ContainerStats.Rootfs this is the bytes used by the container rootfs on the filesystem.
	// +optional
	UsedBytes *uint64 `json:"usedBytes,omitempty"`
	// InodesFree represents the free inodes in the filesystem.
	// +optional
	InodesFree *uint64 `json:"inodesFree,omitempty"`
	// Inodes represents the total inodes in the filesystem.
	// +optional
	Inodes *uint64 `json:"inodes,omitempty"`
	// InodesUsed represents the inodes used by the filesystem
	// This may not equal Inodes - InodesFree because this filesystem may share inodes with other "filesystems"
	// e.g. For ContainerStats.Rootfs, this is the inodes used only by that container, and does not count inodes used by other containers.
	InodesUsed *uint64 `json:"inodesUsed,omitempty"`
}

// UserDefinedMetricType defines how the metric should be interpreted by the user.
//
// UserDefinedMetricType 告诉消费方如何解释数值：瞬时值、累计计数器还是区间速率，
// 三者做差/求平均的方式不同，不能只看 Value 字段猜。
type UserDefinedMetricType string

// 三种类型对应三种聚合语义，插件必须如实标注，否则消费方绘制的趋势图会失真。
const (
	// MetricGauge is an instantaneous value. May increase or decrease.
	MetricGauge UserDefinedMetricType = "gauge"

	// MetricCumulative is a counter-like value that is only expected to increase.
	MetricCumulative UserDefinedMetricType = "cumulative"

	// MetricDelta is a rate over a time period.
	MetricDelta UserDefinedMetricType = "delta"
)

// UserDefinedMetricDescriptor contains metadata that describes a user defined metric.
//
// UserDefinedMetricDescriptor 把指标的元信息独立出来，供消费方在看到具体采样值前
// 就知道如何展示单位与标签。
type UserDefinedMetricDescriptor struct {
	// The name of the metric.
	Name string `json:"name"`

	// Type of the metric.
	Type UserDefinedMetricType `json:"type"`

	// Display Units for the stats.
	Units string `json:"units"`

	// Metadata labels associated with this metric.
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// UserDefinedMetric represents a metric defined and generated by users.
//
// UserDefinedMetric 内嵌描述符再加时间序列值，避免每条采样重复携带名称/类型等元数据。
type UserDefinedMetric struct {
	UserDefinedMetricDescriptor `json:""`
	// The time at which these stats were updated.
	Time metav1.Time `json:"time"`
	// Value of the metric. Float64s have 53 bit precision.
	// We do not foresee any metrics exceeding that value.
	Value float64 `json:"value"`
}
