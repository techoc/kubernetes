/*
Copyright 2025 The Kubernetes Authors.

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

package csi

import (
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// csiNodeUpdater 监听 CSIDriver 对象的变化，并按 NodeAllocatableUpdatePeriodSeconds
// 配置管理每个驱动的定时任务协程，周期更新 CSINodeDriver.Allocatable 信息。
type csiNodeUpdater struct {
	// 用于监听 CSIDriver 对象的 informer。
	driverInformer cache.SharedIndexInformer

	// 驱动名到更新协程停止信道的映射。
	driverUpdaters sync.Map

	// 保证更新器只启动一次。
	once sync.Once
}

// NewCSINodeUpdater 创建一个新的 csiNodeUpdater，driverInformer 不能为空。
func NewCSINodeUpdater(driverInformer cache.SharedIndexInformer) (*csiNodeUpdater, error) {
	if driverInformer == nil {
		return nil, fmt.Errorf("driverInformer must not be nil")
	}
	return &csiNodeUpdater{
		driverInformer: driverInformer,
		driverUpdaters: sync.Map{},
	}, nil
}

// Run 通过注册事件处理器启动 csiNodeUpdater，用 sync.Once 保证只注册一次。
func (u *csiNodeUpdater) Run() {
	u.once.Do(func() {
		_, err := u.driverInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
			AddFunc:    u.onDriverAdd,
			UpdateFunc: u.onDriverUpdate,
			DeleteFunc: u.onDriverDelete,
		})
		if err != nil {
			klog.ErrorS(err, "Failed to add event handler for CSI driver informer")
			return
		}
		klog.V(4).InfoS("csiNodeUpdater initialized successfully")
	})
}

// onDriverAdd 处理新增 CSIDriver 对象的事件。
func (u *csiNodeUpdater) onDriverAdd(obj interface{}) {
	driver, ok := obj.(*v1.CSIDriver)
	if !ok {
		return
	}
	klog.V(7).InfoS("onDriverAdd event", "driver", driver.Name)
	u.syncDriverUpdater(driver.Name)
}

// onDriverUpdate 处理 CSIDriver 对象的更新事件。
func (u *csiNodeUpdater) onDriverUpdate(oldObj, newObj interface{}) {
	oldDriver, ok := oldObj.(*v1.CSIDriver)
	if !ok {
		return
	}
	newDriver, ok := newObj.(*v1.CSIDriver)
	if !ok {
		return
	}

	// 仅当更新周期变化时才需要重建定时协程，其他字段变化不影响更新逻辑。
	oldPeriod := getNodeAllocatableUpdatePeriod(oldDriver)
	newPeriod := getNodeAllocatableUpdatePeriod(newDriver)
	if oldPeriod != newPeriod {
		klog.V(4).InfoS("NodeAllocatableUpdatePeriodSeconds updated", "driver", newDriver.Name, "oldPeriod", oldPeriod, "newPeriod", newPeriod)
		u.syncDriverUpdater(newDriver.Name)
	}
}

// onDriverDelete 处理 CSIDriver 对象的删除事件。
func (u *csiNodeUpdater) onDriverDelete(obj interface{}) {
	driver, ok := obj.(*v1.CSIDriver)
	if !ok {
		return
	}
	klog.V(7).InfoS("onDriverDelete event", "driver", driver.Name)
	u.syncDriverUpdater(driver.Name)
}

// syncDriverUpdater 重新判断给定驱动的定时更新协程是否应该运行。
// 它由 informer 事件（Add/Update/Delete）以及插件注册/注销共同触发。
func (u *csiNodeUpdater) syncDriverUpdater(driverName string) {
	// 只有装在本节点的驱动才需要更新 CSINode。
	if !isDriverInstalled(driverName) {
		klog.V(4).InfoS("Driver not installed; stopping csiNodeUpdater", "driver", driverName)
		u.unregisterDriver(driverName)
		return
	}

	// 从 informer 缓存中读取 CSIDriver，避免直接访问 API Server。
	obj, exists, err := u.driverInformer.GetStore().GetByKey(driverName)
	if err != nil {
		u.unregisterDriver(driverName)
		klog.ErrorS(err, "Error retrieving CSIDriver from store", "driver", driverName)
		return
	}
	if !exists {
		klog.InfoS("CSIDriver object not found; stopping csiNodeUpdater", "driver", driverName)
		u.unregisterDriver(driverName)
		return
	}
	driver, ok := obj.(*v1.CSIDriver)
	if !ok {
		klog.ErrorS(fmt.Errorf("invalid CSIDriver object type"), "failed to cast CSIDriver object", "driver", driverName)
		return
	}

	// 未配置更新周期（0）时不需要启动定时任务。
	period := getNodeAllocatableUpdatePeriod(driver)
	if period == 0 {
		klog.V(7).InfoS("NodeAllocatableUpdatePeriodSeconds is not configured; disabling updates", "driver", driverName)
		u.unregisterDriver(driverName)
		return
	}

	newStopCh := make(chan struct{})
	prevStopCh, loaded := u.driverUpdaters.Swap(driverName, newStopCh)
	// 先停掉旧协程，再用新周期启动，避免多个协程同时更新同一驱动。
	if loaded && prevStopCh != nil {
		if stopCh, ok := prevStopCh.(chan struct{}); ok {
			close(stopCh)
		}
	}

	// 用新的停止信道启动定时更新协程。
	go u.runPeriodicUpdate(driverName, period, newStopCh)
}

// unregisterDriver 停止给定驱动正在运行的定时更新协程。
func (u *csiNodeUpdater) unregisterDriver(driverName string) {
	prev, loaded := u.driverUpdaters.LoadAndDelete(driverName)
	if loaded && prev != nil {
		if stopCh, ok := prev.(chan struct{}); ok {
			close(stopCh)
		}
	}
}

// runPeriodicUpdate 运行某个驱动的定时更新循环，收到停止信号后退出。
func (u *csiNodeUpdater) runPeriodicUpdate(driverName string, period time.Duration, stopCh <-chan struct{}) {
	ticker := time.NewTicker(period)
	defer ticker.Stop()
	klog.V(7).InfoS("Starting periodic updates for driver", "driver", driverName, "period", period)
	for {
		select {
		case <-ticker.C:
			if err := updateCSIDriver(driverName); err != nil {
				klog.ErrorS(err, "Failed to update CSIDriver", "driver", driverName)
			}
		case <-stopCh:
			klog.V(4).InfoS("Stopping periodic updates for driver", "driver", driverName, "period", period)
			return
		}
	}
}

// isDriverInstalled 通过全局 csiDrivers 映射判断驱动是否已安装在当前节点。
func isDriverInstalled(driverName string) bool {
	_, ok := csiDrivers.Get(driverName)
	return ok
}

// getNodeAllocatableUpdatePeriod 从 CSIDriver 读取更新周期，未配置时返回 0。
// 将秒数转换为 time.Duration，便于直接用于 ticker。
func getNodeAllocatableUpdatePeriod(driver *v1.CSIDriver) time.Duration {
	if driver == nil || driver.Spec.NodeAllocatableUpdatePeriodSeconds == nil {
		return 0
	}
	return time.Duration(*driver.Spec.NodeAllocatableUpdatePeriodSeconds) * time.Second
}
