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

package stats

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/record"
	"k8s.io/component-helpers/storage/ephemeral"
	"k8s.io/klog/v2"
	stats "k8s.io/kubelet/pkg/apis/stats/v1alpha1"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
	utiltrace "k8s.io/utils/trace"
)

// volumeStatCalculator 在后台周期性计算指定 Pod 的卷指标并缓存结果。
type volumeStatCalculator struct {
	statsProvider Provider
	jitterPeriod  time.Duration
	pod           *v1.Pod
	stopChannel   chan struct{}
	startO        sync.Once
	stopO         sync.Once
	latest        atomic.Value
	eventRecorder record.EventRecorder
}

// PodVolumeStats 封装 Pod 的卷统计，分别包含本地临时卷与持久卷两个列表。
type PodVolumeStats struct {
	EphemeralVolumes  []stats.VolumeStats
	PersistentVolumes []stats.VolumeStats
}

// newVolumeStatCalculator 创建新的卷统计计算器。
func newVolumeStatCalculator(statsProvider Provider, jitterPeriod time.Duration, pod *v1.Pod, eventRecorder record.EventRecorder) *volumeStatCalculator {
	return &volumeStatCalculator{
		statsProvider: statsProvider,
		jitterPeriod:  jitterPeriod,
		pod:           pod,
		stopChannel:   make(chan struct{}),
		eventRecorder: eventRecorder,
	}
}

// StartOnce 启动后台周期性卷统计，直到调用 StopOnce 才停止。
func (s *volumeStatCalculator) StartOnce(logger klog.Logger) *volumeStatCalculator {
	// 用 sync.Once 保证只启动一个后台 goroutine。
	s.startO.Do(func() {
		go wait.JitterUntil(func() {
			s.calcAndStoreStats(logger)
		}, s.jitterPeriod, 1.0, true, s.stopChannel)
	})
	return s
}

// StopOnce 停止后台卷统计；正在执行的一轮计算会先完成。
func (s *volumeStatCalculator) StopOnce() *volumeStatCalculator {
	s.stopO.Do(func() {
		// 关闭停止通道，JitterUntil 循环随之退出。
		close(s.stopChannel)
	})
	return s
}

// GetLatest 从缓存返回最近的 PodVolumeStats。
func (s *volumeStatCalculator) GetLatest() (PodVolumeStats, bool) {
	result := s.latest.Load()
	// 尚未计算过时返回 false，调用方据此区分“无数据”与“零值”。
	if result == nil {
		return PodVolumeStats{}, false
	}
	return result.(PodVolumeStats), true
}

// calcAndStoreStats 计算指定 Pod 的 PodVolumeStats 并写入 s.latest 缓存。
// 若 Pod 引用了 PVC，同时更新这些 PVC 对应的 Prometheus 指标。
func (s *volumeStatCalculator) calcAndStoreStats(logger klog.Logger) {
	// 获取 Pod 的全部普通卷与块设备卷。
	volumes, found := s.statsProvider.ListVolumesForPod(s.pod.UID)
	blockVolumes, bvFound := s.statsProvider.ListBlockVolumesForPod(s.pod.UID)
	// 两类卷都不存在时无需更新缓存。
	if !found && !bvFound {
		return
	}

	// 合并普通卷与支持指标采集的块设备卷。
	metricVolumes := make(map[string]volume.MetricsProvider)

	if found {
		for name, v := range volumes {
			metricVolumes[name] = v
		}
	}
	if bvFound {
		for name, v := range blockVolumes {
			// 只有实现 MetricsProvider 接口的块设备卷才纳入采集。
			if _, ok := v.(volume.MetricsProvider); ok {
				// 部分驱动从文件系统模式卷继承了 MetricsProvider 接口，但在块设备模式下并未实现；
				// 检查 SupportsMetrics() 可以避免在这种情况下 panic。
				if v.SupportsMetrics() {
					metricVolumes[name] = v
				}
			}
		}
	}

	// 按卷名索引 Pod 的卷规格，供后续查找 PVC 引用。
	volumesSpec := make(map[string]v1.Volume, len(s.pod.Spec.Volumes))
	for _, v := range s.pod.Spec.Volumes {
		volumesSpec[v.Name] = v
	}

	// 逐个卷调用 GetMetrics，并把结果转换为 VolumeStats.FsStats。
	var ephemeralStats []stats.VolumeStats
	var persistentStats []stats.VolumeStats
	for name, v := range metricVolumes {
		metric, err := func() (*volume.Metrics, error) {
			// 单个卷的指标采集可能耗时较长，超过 1 秒时记录 trace 日志。
			trace := utiltrace.New(fmt.Sprintf("Calculate volume metrics of %v for pod %v/%v", name, s.pod.Namespace, s.pod.Name))
			defer trace.LogIfLong(1 * time.Second)
			return v.GetMetrics()
		}()
		if err != nil {
			// 不支持指标采集的卷属于预期情况，不记录错误日志。
			if !volume.IsNotSupported(err) {
				logger.V(4).Info("Failed to calculate volume metrics", "pod", klog.KObj(s.pod), "podUID", s.pod.UID, "volumeName", name, "err", err)
			}
			continue
		}
		// 查找卷规格，为引用 PVC 的卷补充 PVCReference。
		volSpec := volumesSpec[name]
		var pvcRef *stats.PVCReference
		if pvcSource := volSpec.PersistentVolumeClaim; pvcSource != nil {
			pvcRef = &stats.PVCReference{
				Name:      pvcSource.ClaimName,
				Namespace: s.pod.GetNamespace(),
			}
			// 临时卷的 PVC 名称由 Pod 名与卷名自动生成。
		} else if volSpec.Ephemeral != nil {
			pvcRef = &stats.PVCReference{
				Name:      ephemeral.VolumeClaimName(s.pod, &volSpec),
				Namespace: s.pod.GetNamespace(),
			}
		}
		volumeStats := s.parsePodVolumeStats(name, pvcRef, metric, volSpec)
		// 本地临时卷归入临时卷列表，其余归入持久卷列表。
		if util.IsLocalEphemeralVolume(volSpec) {
			ephemeralStats = append(ephemeralStats, volumeStats)
		} else {
			persistentStats = append(persistentStats, volumeStats)
		}

	}

	// 原子地更新缓存，读取方无需加锁。
	s.latest.Store(PodVolumeStats{EphemeralVolumes: ephemeralStats,
		PersistentVolumes: persistentStats})
}

// parsePodVolumeStats 把内部的 volume.Metrics 转换为对外的 stats.VolumeStats 结构。
func (s *volumeStatCalculator) parsePodVolumeStats(podName string, pvcRef *stats.PVCReference, metric *volume.Metrics, volSpec v1.Volume) stats.VolumeStats {

	var (
		available, capacity, used, inodes, inodesFree, inodesUsed uint64
	)

	// 指标字段可能为 nil，逐项判空后转换为零值。
	if metric.Available != nil {
		available = uint64(metric.Available.Value())
	}
	if metric.Capacity != nil {
		capacity = uint64(metric.Capacity.Value())
	}
	if metric.Used != nil {
		used = uint64(metric.Used.Value())
	}
	if metric.Inodes != nil {
		inodes = uint64(metric.Inodes.Value())
	}
	if metric.InodesFree != nil {
		inodesFree = uint64(metric.InodesFree.Value())
	}
	if metric.InodesUsed != nil {
		inodesUsed = uint64(metric.InodesUsed.Value())
	}

	volumeStats := stats.VolumeStats{
		Name:   podName,
		PVCRef: pvcRef,
		FsStats: stats.FsStats{Time: metric.Time, AvailableBytes: &available, CapacityBytes: &capacity,
			UsedBytes: &used, Inodes: &inodes, InodesFree: &inodesFree, InodesUsed: &inodesUsed},
	}

	return volumeStats
}
