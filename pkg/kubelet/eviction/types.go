/*
Copyright 2016 The Kubernetes Authors.

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

//go:generate mockery
package eviction

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
	statsapi "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
)

// fsStatsType 定义要收集的文件系统统计类型。
type fsStatsType string

const (
	// 标识 Pod 本地卷的统计。
	fsStatsLocalVolumeSource fsStatsType = "localVolumeSource"
	// 标识 Pod 日志的统计。
	fsStatsLogs fsStatsType = "logs"
	// 标识容器可写层的统计。
	fsStatsRoot fsStatsType = "root"
	// 标识容器只读层（镜像）的统计。
	fsStatsImages fsStatsType = "images"
)

// Config 保存驱逐配置。
type Config struct {
	// 退出压力状态前 kubelet 需要等待的时长。
	PressureTransitionPeriod time.Duration
	// 软驱逐阈值触发时，终止 Pod 允许的最大宽限期（秒）。
	MaxPodGracePeriodSeconds int64
	// 触发驱逐的监控条件集合。
	Thresholds []evictionapi.Threshold
	// 为 true 时接入内核 memcg 通知来判断是否越过内存阈值。
	KernelMemcgNotification bool
	// 包含所有 Pod 的 cgroup。
	PodCgroupRoot string
}

// Manager 评估节点上是否已达到影响稳定性的驱逐阈值。
type Manager interface {
	// 以指定间隔启动监控驱逐阈值的控制循环。
	Start(ctx context.Context, diskInfoProvider DiskInfoProvider, podFunc ActivePodsFunc, podCleanedUpFunc PodCleanedUpFunc, monitoringInterval time.Duration)

	// 节点处于内存压力时返回 true。
	IsUnderMemoryPressure() bool

	// 节点处于磁盘压力时返回 true。
	IsUnderDiskPressure() bool

	// 节点处于 PID 压力时返回 true。
	IsUnderPIDPressure() bool
}

// DiskInfoProvider 负责告知 manager 磁盘配置。
type DiskInfoProvider interface {
	// imagefs 与 rootfs 位于不同设备时返回 true。
	HasDedicatedImageFs(ctx context.Context) (bool, error)
	// 容器文件系统与 rootfs 位于不同设备时返回 true。
	HasDedicatedContainerFs(ctx context.Context) (bool, error)
}

// ImageGC 负责回收未使用的镜像。
type ImageGC interface {
	// 删除未使用的镜像。
	DeleteUnusedImages(ctx context.Context) error
}

// ContainerGC 负责回收未使用的容器。
type ContainerGC interface {
	// 删除所有未使用的容器，包括属于已终止但未删除 Pod 的容器。
	DeleteAllUnusedContainers(ctx context.Context) error
}

// KillPodFunc 用于终止 Pod。
// 先更新 Pod 状态，再按指定宽限期终止。
// 该函数必须阻塞到 Pod 被终止或遇到错误为止。
// 参数：pod 为待终止的 Pod；status 为要关联的目标状态（即终止原因）；
// gracePeriodOverride 覆盖 Pod spec 中的宽限期。
type KillPodFunc func(pod *v1.Pod, isEvicted bool, gracePeriodOverride *int64, fn func(*v1.PodStatus)) error

// MirrorPodFunc 返回给定静态 Pod 的镜像 Pod，
// 以及该 Pod 是否被 pod manager 所知。
type MirrorPodFunc func(*v1.Pod) (*v1.Pod, bool)

// ActivePodsFunc 返回绑定到 kubelet 且处于活跃状态（非终止）的 Pod。
type ActivePodsFunc func() []*v1.Pod

// PodCleanedUpFunc 在 Pod 关联的所有资源都已回收时返回 true。
type PodCleanedUpFunc func(*v1.Pod) bool

// statsFunc 返回输入 Pod 的使用统计（若已知）。
type statsFunc func(pod *v1.Pod) (statsapi.PodStats, bool)

// rankFunc 按驱逐顺序对 Pod 排序。
type rankFunc func(pods []*v1.Pod, stats statsFunc)

// signalObservation 是一次资源使用观测。
type signalObservation struct {
	// 资源容量。
	capacity *resource.Quantity
	// 可用资源。
	available *resource.Quantity
	// 观测时间。
	time metav1.Time
}

// signalObservations 把信号映射到观测到的数量。
type signalObservations map[evictionapi.Signal]signalObservation

// thresholdsObservedAt 记录每个阈值被观测到的时间。
type thresholdsObservedAt map[evictionapi.Threshold]time.Time

// nodeConditionsObservedAt 记录每个节点状况被观测到的时间。
type nodeConditionsObservedAt map[v1.NodeConditionType]time.Time

// nodeReclaimFunc 在不影响 Pod 的前提下从节点回收资源。
type nodeReclaimFunc func(ctx context.Context) error

// nodeReclaimFuncs 是有序的 nodeReclaimFunc 列表。
type nodeReclaimFuncs []nodeReclaimFunc

// CgroupNotifier 根据 cgroup 事件生成通知。
type CgroupNotifier interface {
	// 让 notifier 开始在 eventCh 上发送通知。
	Start(ctx context.Context, eventCh chan<- struct{})
	// 停止所有进程并清理 notifier 关联的文件描述符。
	Stop()
}

// NotifierFactory 创建 CgroupNotifier。
type NotifierFactory interface {
	// 创建一个 CgroupNotifier：当指定 cgroup 中该属性的阈值被越过时产生事件。
	NewCgroupNotifier(logger klog.Logger, path, attribute string, threshold int64) (CgroupNotifier, error)
}

// ThresholdNotifier 根据内存驱逐阈值管理 CgroupNotifier，
// 并在越过阈值时执行回调。
type ThresholdNotifier interface {
	// 当 CgroupNotifier 报告事件时调用通知函数。
	Start(ctx context.Context)
	// 根据给定指标更新内存 cgroup 阈值；
	// 使用最新指标可让通知在驱逐阈值处更精确地触发。
	UpdateThreshold(ctx context.Context, summary *statsapi.Summary) error
	// 生成描述该内存阈值通知器的字符串。
	Description() string
}
