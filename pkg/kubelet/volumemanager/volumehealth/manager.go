/*
Copyright The Kubernetes Authors.

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

package volumehealth

import (
	"context"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/volume/csi"
)

// StatusUpdater 更新 kubelet 状态缓存中的 Pod 卷健康信息。
type StatusUpdater interface {
	// SetPodVolumeHealth 写入指定 Pod 卷的健康状况。
	SetPodVolumeHealth(logger klog.Logger, podUID types.UID, volumeName string, conditions []v1.VolumeHealthCondition) bool
}

// CSINodeUpdater 更新 CSINode.Status.StorageHealth。
type CSINodeUpdater interface {
	// UpdateCSINodeStorageHealth 写入指定驱动的存储健康状况。
	UpdateCSINodeStorageHealth(driverName string, conditions []storagev1.StorageHealthCondition) error
}

// HealthClientFactory 为已注册驱动创建 CSI 健康客户端。
type HealthClientFactory func(driverName string) csi.HealthClient

// Manager 周期性探测 CSI 卷与存储健康，
// 并把结果写入 PodStatus.VolumeHealth 与 CSINode.Status.StorageHealth。
type Manager interface {
	// Run 启动周期性探测，直到 ctx 取消。
	Run(ctx context.Context)
}

// manager 是 Manager 的实现。
type manager struct {
	dsw            cache.DesiredStateOfWorld
	asw            cache.ActualStateOfWorld
	statusUpdater  StatusUpdater
	probeInterval  time.Duration
	clientFactory  HealthClientFactory
	listDrivers    func() []string
	csiNodeUpdater func() CSINodeUpdater
}

// NewManager 创建卷健康管理器。
// probeInterval 通常取 kubelet 的 VolumeStatsAggPeriod；statusUpdater 可以为 nil
// （此时仍执行探测，但不写 Pod 状态）；clientFactory/listDrivers/csiNodeUpdater
// 为 nil 时使用 CSI 插件的默认实现。
func NewManager(
	dsw cache.DesiredStateOfWorld,
	asw cache.ActualStateOfWorld,
	statusUpdater StatusUpdater,
	probeInterval time.Duration,
) Manager {
	// 非法间隔回退到 1 分钟，避免忙循环。
	if probeInterval <= 0 {
		probeInterval = time.Minute
	}
	return &manager{
		dsw:           dsw,
		asw:           asw,
		statusUpdater: statusUpdater,
		probeInterval: probeInterval,
		clientFactory: csi.NewHealthClient,
		listDrivers:   csi.ListRegisteredDrivers,
		csiNodeUpdater: func() CSINodeUpdater {
			return csi.GetNodeInfoManager()
		},
	}
}

// Run 注册指标并启动后台调谐循环，直到 ctx 取消。
func (m *manager) Run(ctx context.Context) {
	registerHealthMetrics()
	logger := klog.FromContext(ctx)
	logger.Info("Starting CSI volume health manager", "probeInterval", m.probeInterval)
	// 周期执行探测；主协程阻塞等待退出信号。
	go wait.UntilWithContext(ctx, m.reconcile, m.probeInterval)
	<-ctx.Done()
	logger.Info("Shutting down CSI volume health manager")
}

// reconcile 并发执行卷健康与存储健康探测，等待两者完成。
func (m *manager) reconcile(ctx context.Context) {
	var wg sync.WaitGroup
	wg.Go(func() { m.probeVolumeHealth(ctx) })
	wg.Go(func() { m.probeStorageHealth(ctx) })
	wg.Wait()
}

// probeVolumeHealth 遍历需要上报健康的卷并调用 CSI 驱动探测。
func (m *manager) probeVolumeHealth(ctx context.Context) {
	logger := klog.FromContext(ctx)
	volumes := cache.GetVolumesToReportHealth(m.dsw, m.asw)

	for _, vol := range volumes {
		// 信息不完整的卷跳过，避免向驱动发起无效调用。
		if vol.Pod == nil || vol.OuterVolumeName == "" || vol.CSIVolumeHandle == "" {
			continue
		}

		client := m.clientFactory(vol.DriverName)
		// 驱动不支持卷健康时跳过。
		supported, err := client.NodeSupportsVolumeHealth(ctx)
		if err != nil {
			logger.V(4).Info("Failed to check volume health capability",
				"driver", vol.DriverName, "err", err)
			continue
		}
		if !supported {
			continue
		}

		// 探测失败时保留上次的状态，避免把暂时错误当成健康状态。
		conditions, err := client.NodeGetVolumeHealth(ctx, vol.CSIVolumeHandle, vol.StagingPath, vol.PublishPath)
		if err != nil {
			logger.V(4).Info("NodeGetVolumeHealth failed; leaving previous conditions unchanged",
				"driver", vol.DriverName, "volume", vol.OuterVolumeName, "err", err)
			continue
		}

		// 去重与 PATCH 抑制由 status_manager.SetPodVolumeHealth 负责。
		if m.statusUpdater != nil {
			m.statusUpdater.SetPodVolumeHealth(logger, vol.Pod.UID, vol.OuterVolumeName, conditions)
		}
	}
}

// probeStorageHealth 遍历已注册驱动并探测存储后端健康。
func (m *manager) probeStorageHealth(ctx context.Context) {
	logger := klog.FromContext(ctx)
	// 没有 CSINode 更新器时无需探测。
	updater := m.csiNodeUpdater()
	if updater == nil {
		return
	}
	for _, driverName := range m.listDrivers() {
		client := m.clientFactory(driverName)
		// 驱动不支持存储健康时跳过。
		supported, err := client.NodeSupportsStorageHealth(ctx)
		if err != nil {
			logger.V(4).Info("Failed to check storage health capability",
				"driver", driverName, "err", err)
			continue
		}
		if !supported {
			continue
		}

		// 探测失败时保留上次状态。
		backendHealth, err := client.NodeGetStorageHealth(ctx, nil)
		if err != nil {
			logger.V(4).Info("NodeGetStorageHealth failed; leaving previous conditions unchanged",
				"driver", driverName, "err", err)
			continue
		}

		// 把健康条件转换为指标标签并上报。
		gaugeKeys := make([]storageHealthKey, 0, len(backendHealth))
		for _, c := range backendHealth {
			gaugeKeys = append(gaugeKeys, storageHealthKey{status: string(c.Status), reason: c.Reason})
		}
		setStorageHealthGauges(driverName, gaugeKeys)

		// 去重由 nodeinfomanager.UpdateCSINodeStorageHealth 负责。
		if err := updater.UpdateCSINodeStorageHealth(driverName, backendHealth); err != nil {
			logger.Error(err, "Failed to update CSINode storage health", "driver", driverName)
		}
	}
}
