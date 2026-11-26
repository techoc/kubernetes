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

// Summary 是承载 NodeStats 与 PodStats 的顶层容器。
// Summary 是统计接口的顶层聚合结果：节点级统计与所有 Pod 统计放在同一个响应里，
// 消费方一次请求就能拿到完整快照，避免逐 Pod 请求带来的开销。
type Summary struct {
	// 节点整体统计。
	Node NodeStats `json:"node"`
	// 每个 Pod 的统计。
	Pods []PodStats `json:"pods"`
}

// NodeStats 保存节点级未加工采样统计。
// NodeStats 保存节点级的原始采样值（累计量为主），消费方若要算速率需自行用两次采样做差。
// 各资源字段用指针表示，便于区分“采集失败/不支持”和“取值为 0”。
type NodeStats struct {
	// 被统计节点的引用。
	NodeName string `json:"nodeName"`
	// 作为原始容器跟踪的系统守护进程统计。
	// 系统容器名遵循 SystemContainer* 常量。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	SystemContainers []ContainerStats `json:"systemContainers,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// 节点级（聚合）统计的数据采集（重新）启动时间。
	StartTime metav1.Time `json:"startTime"`
	// CPU 资源相关统计。
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// 内存（RAM）资源相关统计。
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// IO 资源相关统计。
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// 网络资源相关统计。
	// +optional
	Network *NetworkStats `json:"network,omitempty"`
	// 节点 k8s 组件使用的 rootfs 上文件系统总用量统计。
	// NodeFs.Used 是文件系统上已用的总字节数。
	// +optional
	Fs *FsStats `json:"fs,omitempty"`
	// 底层容器运行时相关统计。
	// +optional
	Runtime *RuntimeStats `json:"runtime,omitempty"`
	// 系统 rlimit 相关统计。
	// +optional
	Rlimit *RlimitStats `json:"rlimit,omitempty"`
	// 交换空间相关统计，仅在非 Windows 系统上上报。
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// RlimitStats 是操作系统 rlimit 的统计。
// RlimitStats 记录内核对进程/PID 数量的硬限制，供 kubelet 判断节点是否接近 PID 耗尽，
// 触发 Pod 驱逐前的关键判据。
type RlimitStats struct {
	Time metav1.Time `json:"time"`

	// 操作系统当前可存在的最大进程数（Linux 上精确说是线程数），见 getrlimit(2) 的 RLIMIT_NPROC。
	// 操作系统可分配的 PID 上限。
	// Linux 上任务（进程或线程）各消耗 1 个 PID。
	MaxPID *int64 `json:"maxpid,omitempty"`
	// 操作系统中正在运行的进程数（Linux 上精确说是线程数）。
	NumOfRunningProcesses *int64 `json:"curproc,omitempty"`
}

// RuntimeStats 是底层容器运行时的统计。
// RuntimeStats 把运行时占用的文件系统单独列出：镜像层与容器可写层可能共用同一文件系统，
// 也可能各自独立，分开统计便于定位磁盘被哪一类数据撑满。
type RuntimeStats struct {
	// 存放容器镜像的文件系统相关统计。
	// 该文件系统可能与主（根）文件系统相同。
	// Usage 指镜像在文件系统上占用的总字节数。
	// +optional
	ImageFs *FsStats `json:"imageFs,omitempty"`
	// 存放容器可写层的文件系统相关统计。
	// 该文件系统可能与主（根）文件系统或 ImageFS 相同。
	// Usage 指可写层在文件系统上占用的总字节数。
	// +optional
	ContainerFs *FsStats `json:"containerFs,omitempty"`
}

// 系统容器名既用于标识统计归属，也用于与 cAdvisor 的容器层级对应；
// 之所以不直接用二进制名，是为了让消费方按固定枚举解析这些统计。
const (
	// SystemContainerKubelet 是跟踪 Kubelet 用量的系统容器名。
	SystemContainerKubelet = "kubelet"
	// SystemContainerRuntime 是跟踪运行时（如 docker）用量的系统容器名。
	SystemContainerRuntime = "runtime"
	// SystemContainerMisc 是跟踪非 Kubernetes 进程用量的系统容器名。
	SystemContainerMisc = "misc"
	// SystemContainerPods 是跟踪用户 Pod 用量的系统容器名。
	SystemContainerPods = "pods"
	// SystemContainerWindowsGlobalCommitMemory（仅 Windows）是跟踪全局提交内存用量的系统容器名，
	// 用于内存压力驱逐。
	SystemContainerWindowsGlobalCommitMemory = "windows-global-commit-memory"
)

// ProcessStats 是进程相关统计。
// ProcessStats 只统计进程数量，是判断节点/Pod 是否触及 PID 限制的最小字段集；
// 用指针是为了在采集器未实现该功能时省略整个字段。
type ProcessStats struct {
	// 进程数。
	// +optional
	ProcessCount *uint64 `json:"process_count,omitempty"`
}

// PodStats 保存 Pod 级未加工采样统计。
// PodStats 既包含 Pod cgroup 聚合值，也包含各容器的明细：聚合值覆盖 Pod 级开销，
// 所以不等于容器值简单相加。
type PodStats struct {
	// 被统计 Pod 的引用。
	PodRef PodReference `json:"podRef"`
	// Pod 级（如网络）统计的数据采集（重新）启动时间。
	StartTime metav1.Time `json:"startTime"`
	// 被统计 Pod 中各容器的统计。
	// +patchMergeKey=name
	// +patchStrategy=merge
	Containers []ContainerStats `json:"containers" patchStrategy:"merge" patchMergeKey:"name"`
	// Pod cgroup 消耗的 CPU 资源统计（含所有容器用量与 Pod 开销）。
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// Pod cgroup 消耗的内存资源统计（含所有容器用量与 Pod 开销）。
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// Pod cgroup 消耗的 IO 资源统计（含所有容器用量与 Pod 开销）。
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// 网络资源相关统计。
	// +optional
	Network *NetworkStats `json:"network,omitempty"`
	// 文件系统卷用量统计。
	// VolumeStats.UsedBytes 是卷已用的字节数。
	// +optional
	// +patchMergeKey=name
	// +patchStrategy=merge
	VolumeStats []VolumeStats `json:"volume,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// EphemeralStorage 报告被统计 Pod 中容器与 emptyDir 卷的文件系统总用量。
	// +optional
	EphemeralStorage *FsStats `json:"ephemeral-storage,omitempty"`
	// 进程相关 ProcessStats。
	// +optional
	ProcessStats *ProcessStats `json:"process_stats,omitempty"`
	// 交换空间相关统计，仅在非 Windows 系统上上报。
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// ContainerStats 保存容器级未加工采样统计。
// ContainerStats 以容器为粒度组织统计；Rootfs 与 Logs 分开列出，
// 因为二者通常落在不同目录/限额上，排障时需要独立看各自用量。
type ContainerStats struct {
	// 被统计容器的引用。
	Name string `json:"name"`
	// 该容器数据采集（重新）启动的时间。
	StartTime metav1.Time `json:"startTime"`
	// CPU 资源相关统计。
	// +optional
	CPU *CPUStats `json:"cpu,omitempty"`
	// 内存（RAM）资源相关统计。
	// +optional
	Memory *MemoryStats `json:"memory,omitempty"`
	// IO 资源相关统计。
	// +optional
	IO *IOStats `json:"io,omitempty"`
	// 加速器指标。每个加速器对应数组中的一个元素。
	Accelerators []AcceleratorStats `json:"accelerators,omitempty"`
	// 容器 rootfs 的文件系统用量统计。
	// Rootfs.UsedBytes 是容器写层占用的字节数。
	// +optional
	Rootfs *FsStats `json:"rootfs,omitempty"`
	// 容器日志文件系统的用量统计。
	// Logs.UsedBytes 是容器日志占用的字节数。
	// +optional
	Logs *FsStats `json:"logs,omitempty"`
	// Pod 中容器暴露的用户自定义指标。通常期望只有一个容器暴露；若有多个，结果在此合并。
	// +patchMergeKey=name
	// +patchStrategy=merge
	UserDefinedMetrics []UserDefinedMetric `json:"userDefinedMetrics,omitempty" patchStrategy:"merge" patchMergeKey:"name"`
	// 交换空间相关统计，仅在非 Windows 系统上上报。
	// +optional
	Swap *SwapStats `json:"swap,omitempty"`
}

// PodReference 包含定位被引用 Pod 所需的信息。
// PodReference 同时带 UID，避免同名 Pod 重建后被误认为同一个对象。
type PodReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

// InterfaceStats 包含某网络接口的资源数据。
// InterfaceStats 的字节/错误计数都是网卡启动以来的累计值，
// 速率须由消费方对相邻采样点做差得出。
type InterfaceStats struct {
	// 接口名。
	Name string `json:"name"`
	// 累计接收字节数。
	// +optional
	RxBytes *uint64 `json:"rxBytes,omitempty"`
	// 累计接收错误数。
	// +optional
	RxErrors *uint64 `json:"rxErrors,omitempty"`
	// 累计发送字节数。
	// +optional
	TxBytes *uint64 `json:"txBytes,omitempty"`
	// 累计发送错误数。
	// +optional
	TxErrors *uint64 `json:"txErrors,omitempty"`
}

// NetworkStats 包含网络资源数据。
// NetworkStats 内嵌 InterfaceStats 表达默认网卡，是为了兼容只关心默认网卡的旧消费方；
// Interfaces 则提供完整明细，二者数据会重复，不应累加。
type NetworkStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`

	// 默认接口的统计（若找到）。
	InterfaceStats `json:""`

	Interfaces []InterfaceStats `json:"interfaces,omitempty"`
}

// CPUStats 包含 CPU 用量数据。
// CPUStats 同时给出瞬时用量与累计用量：前者便于直接展示，
// 后者不含采样窗口信息，适合消费方自行计算任意时段的平均值。
type CPUStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// 采样窗口内平均总 CPU 用量（所有核之和）。
	// "core" 单位可理解为每秒 CPU 核心纳秒数。
	// +optional
	UsageNanoCores *uint64 `json:"usageNanoCores,omitempty"`
	// 自对象创建以来的累计 CPU 用量（所有核之和）。
	// +optional
	UsageCoreNanoSeconds *uint64 `json:"usageCoreNanoSeconds,omitempty"`
	// CPU PSI 统计。
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// MemoryStats 包含内存用量数据。
// MemoryStats 区分 usage 与 workingSet：前者包含可回收的 cache，
// 后者更接近 cgroup 内存压力下真正会被回收的量，驱逐决策看后者。
type MemoryStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// 可用内存，定义为 memory limit - workingSetBytes。
	// 未设置内存 limit 时省略该值。
	// +optional
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	// 已用内存总量，含所有内存，不论访问时间。
	// +optional
	UsageBytes *uint64 `json:"usageBytes,omitempty"`
	// 工作集内存量，含最近访问的内存、脏页和内核内存。
	// WorkingSetBytes <= UsageBytes。
	// +optional
	WorkingSetBytes *uint64 `json:"workingSetBytes,omitempty"`
	// 匿名页与 swap cache 内存量（含透明大页）。
	// +optional
	RSSBytes *uint64 `json:"rssBytes,omitempty"`
	// 累计次要缺页次数。
	// +optional
	PageFaults *uint64 `json:"pageFaults,omitempty"`
	// 累计主要缺页次数。
	// +optional
	MajorPageFaults *uint64 `json:"majorPageFaults,omitempty"`
	// 内存 PSI 统计。
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// IOStats 包含 IO 用量数据。
// IOStats 当前仅承载 PSI 数据：cAdvisor 不能跨所有块设备给出可靠的读写速率，
// 用压力stall 信息比上报含义模糊的计数更有用。
type IOStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// IO PSI 统计。
	// +optional
	PSI *PSIStats `json:"psi,omitempty"`
}

// 单个资源的 PSI 统计。
// PSIStats 区分 full 与 some：前者要求所有任务同时受阻，
// 后者只要部分任务受阻，两者结合才能判断资源压力是局部还是整体。
type PSIStats struct {
	// cgroup 中所有任务的 PSI 数据。
	Full PSIData `json:"full"`
	// cgroup 中部分任务的 PSI 数据。
	Some PSIData `json:"some"`
}

// 单个资源的 PSI 数据。
// PSIData 的 avg10/60/300 覆盖不同时间尺度：短窗口反映突发压力，
// 长窗口过滤毛刺，便于消费方按自身需求选用。
type PSIData struct {
	// cgroup 中任务因拥塞而等待的总时长。
	// 单位：纳秒。
	Total uint64 `json:"total"`
	// 10 秒窗口内任务因拥塞等待的平均百分比。
	Avg10 float64 `json:"avg10"`
	// 60 秒窗口内任务因拥塞等待的平均百分比。
	Avg60 float64 `json:"avg60"`
	// 300 秒窗口内任务因拥塞等待的平均百分比。
	Avg300 float64 `json:"avg300"`
}

// SwapStats 包含内存用量数据。
// SwapStats 只在非 Windows 节点上报：Windows 的交换语义与 Linux cgroup 不同，
// 强行统一会让消费方拿到含义错误的数据。
type SwapStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// 可用 swap 内存，定义为 <swap-limit> - <current-swap-usage>。
	// 未设置 swap limit 时省略该值。
	// +optional
	SwapAvailableBytes *uint64 `json:"swapAvailableBytes,omitempty"`
	// 已用 swap 内存总量。
	// +optional
	SwapUsageBytes *uint64 `json:"swapUsageBytes,omitempty"`
}

// AcceleratorStats 包含容器所挂加速器的统计。
// AcceleratorStats 描述单个加速卡；一个容器可挂多张卡，所以该类型出现在数组中，
// 并由 ID 区分具体设备。
type AcceleratorStats struct {
	// 加速器厂商（nvidia、amd、google 等）。
	Make string `json:"make"`

	// 加速器型号（tesla-p100、tesla-k80 等）。
	Model string `json:"model"`

	// 加速器 ID。
	ID string `json:"id"`

	// 加速器总显存。
	// 单位：字节。
	MemoryTotal uint64 `json:"memoryTotal"`

	// 已分配的加速器总显存。
	// 单位：字节。
	MemoryUsed uint64 `json:"memoryUsed"`

	// 上个采样周期（10s）内加速器活跃处理的时间百分比。
	//
	DutyCycle uint64 `json:"dutyCycle"`
}

// VolumeStats 包含卷文件系统用量数据。
// VolumeStats 内嵌 FsStats 以复用文件系统字段；PVCRef 仅在使用 PVC 时填充，
// 让消费方可以关联到存储对象而不必反查 Pod spec。
type VolumeStats struct {
	// 内嵌 FsStats。
	FsStats `json:""`
	// Name 是卷的名字。
	// +optional
	Name string `json:"name,omitempty"`
	// PVC 的引用（若存在）。
	// +optional
	PVCRef *PVCReference `json:"pvcRef,omitempty"`
}

// PVCReference 包含描述被引用 PVC 所需的信息。
// PVCReference 不含 UID，因为统计只需定位到当前 PVC 对象，
// 短生命周期的采样数据没必要绑定对象身份。
type PVCReference struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
}

// FsStats 包含文件系统用量数据。
// FsStats 同时给出容量/空闲与“本任务用量”：后者受共享文件系统影响，
// 不能用 CapacityBytes - AvailableBytes 反推，容器 rootfs 场景尤其明显。
type FsStats struct {
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// AvailableBytes 表示文件系统可用存储空间（字节）。
	// +optional
	AvailableBytes *uint64 `json:"availableBytes,omitempty"`
	// CapacityBytes 表示文件系统底层存储的总容量（字节）。
	// +optional
	CapacityBytes *uint64 `json:"capacityBytes,omitempty"`
	// UsedBytes 表示某任务在文件系统上占用的字节数。
	// 它可能不同于文件系统总用量，也不等于 CapacityBytes - AvailableBytes。
	// 例如 ContainerStats.Rootfs 中是容器 rootfs 占用的字节数。
	// +optional
	UsedBytes *uint64 `json:"usedBytes,omitempty"`
	// InodesFree 是文件系统中空闲 inode 数。
	// +optional
	InodesFree *uint64 `json:"inodesFree,omitempty"`
	// Inodes 是文件系统中 inode 总数。
	// +optional
	Inodes *uint64 `json:"inodes,omitempty"`
	// InodesUsed 是文件系统已用 inode 数。
	// 它可能不等于 Inodes - InodesFree，因为该文件系统可能与其它"文件系统"共享 inode。
	// 例如 ContainerStats.Rootfs 中只统计该容器用的 inode，不含其它容器。
	InodesUsed *uint64 `json:"inodesUsed,omitempty"`
}

// UserDefinedMetricType 定义用户如何理解该指标。
// UserDefinedMetricType 告诉消费方如何解释数值：瞬时值、累计计数器还是区间速率，
// 三者做差/求平均的方式不同，不能只看 Value 字段猜。
type UserDefinedMetricType string

// 三种类型对应三种聚合语义，插件必须如实标注，否则消费方绘制的趋势图会失真。
const (
	// MetricGauge 是瞬时值，可增可减。
	MetricGauge UserDefinedMetricType = "gauge"

	// MetricCumulative 是类似计数器的值，只应增加。
	MetricCumulative UserDefinedMetricType = "cumulative"

	// MetricDelta 是一段时间内的速率。
	MetricDelta UserDefinedMetricType = "delta"
)

// UserDefinedMetricDescriptor 包含描述用户自定义指标的元数据。
// UserDefinedMetricDescriptor 把指标的元信息独立出来，供消费方在看到具体采样值前
// 就知道如何展示单位与标签。
type UserDefinedMetricDescriptor struct {
	// 指标名。
	Name string `json:"name"`

	// 指标类型。
	Type UserDefinedMetricType `json:"type"`

	// 统计的显示单位。
	Units string `json:"units"`

	// 与该指标关联的元数据标签。
	// +optional
	Labels map[string]string `json:"labels,omitempty"`
}

// UserDefinedMetric 表示用户定义并生成的指标。
// UserDefinedMetric 内嵌描述符再加时间序列值，避免每条采样重复携带名称/类型等元数据。
type UserDefinedMetric struct {
	UserDefinedMetricDescriptor `json:""`
	// 这些统计的更新时间。
	Time metav1.Time `json:"time"`
	// 指标的值。Float64 有 53 位精度。
	// 预计不会有指标超过该精度。
	Value float64 `json:"value"`
}
