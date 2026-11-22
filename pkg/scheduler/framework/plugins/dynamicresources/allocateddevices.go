/*
Copyright 2024 The Kubernetes Authors.

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

package dynamicresources

import (
	"sync"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/tools/cache"
	"k8s.io/dynamic-resource-allocation/structured"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	schedutil "k8s.io/kubernetes/pkg/scheduler/util"
	"k8s.io/utils/ptr"
)

// foreachAllocatedDevice 对 ResourceClaim 分配结果中每个独占分配的设备调用回调。
// 以 admin access 分配的设备可与其他 claim 共享，不视为独占，回调会被跳过。
// claim 尚未分配时该函数什么都不做。
func foreachAllocatedDevice(claim *resourceapi.ResourceClaim,
	dedicatedDeviceCallback func(deviceID structured.DeviceID),
	enabledConsumableCapacity bool,
	sharedDeviceCallback func(structured.SharedDeviceID),
	consumedCapacityCallback func(structured.DeviceConsumedCapacity)) {
	if claim.Status.Allocation == nil {
		return
	}
	for _, result := range claim.Status.Allocation.Devices.Results {
		// 1.31 不设置该字段，1.32 起总是设置。为兼容 1.31 而额外查 request 不值得，
		// 毕竟 1.31 集群里仍存在且恰好在此处有影响的分配结果几乎不可能出现。
		if ptr.Deref(result.AdminAccess, false) {
			// 不视为已独占分配。
			continue
		}
		deviceID := structured.MakeDeviceID(result.Driver, result.Pool, result.Device)

		// 现有调用方都不需要中途停止遍历，因此不提供该能力，免得徒增开销。

		// 启用 DRAConsumableCapacity 时，分别调用共享设备与已消耗容量回调。
		if enabledConsumableCapacity {
			shared := result.ShareID != nil
			if shared {
				sharedDeviceID := structured.MakeSharedDeviceID(deviceID, result.ShareID)
				sharedDeviceCallback(sharedDeviceID)
				if result.ConsumedCapacity != nil {
					deviceConsumedCapacity := structured.NewDeviceConsumedCapacity(deviceID, result.ConsumedCapacity)
					consumedCapacityCallback(deviceConsumedCapacity)
				}
				continue
			}
		}

		// 否则按独占设备处理。
		dedicatedDeviceCallback(deviceID)
	}
}

// allocatedDevices 监听缓存事件并维护所有已分配设备的集合。
// 这样比每次 PreFilter 都调用 List、去重字符串、重建集合更便宜。
//
// 为便于检测并发变更，每次修改都会递增 revision，类似 apiserver 的 ResourceVersion。
// Get 与 Capacities 会一并返回当前 revision，调用方据此即可判断先前的结果是否过期，
// 无需再次取值比对。
//
// 所有方法线程安全；Get 返回的是拷贝。
type allocatedDevices struct {
	logger klog.Logger

	mutex                     sync.RWMutex
	revision                  int64
	ids                       sets.Set[structured.DeviceID]
	shareIDs                  sets.Set[structured.SharedDeviceID]
	capacities                structured.ConsumedCapacityCollection
	enabledConsumableCapacity bool
}

func newAllocatedDevices(logger klog.Logger) *allocatedDevices {
	return &allocatedDevices{
		logger:                    logger,
		ids:                       sets.New[structured.DeviceID](),
		shareIDs:                  sets.New[structured.SharedDeviceID](),
		capacities:                structured.NewConsumedCapacityCollection(),
		enabledConsumableCapacity: utilfeature.DefaultFeatureGate.Enabled(features.DRAConsumableCapacity),
	}
}

// Get 返回已独占分配设备集合的拷贝以及当前 revision。
func (a *allocatedDevices) Get() (sets.Set[structured.DeviceID], int64) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.ids.Clone(), a.revision
}

// GetSharedDeviceIDs 返回已分配共享设备 ID 集合的拷贝以及当前 revision。
func (a *allocatedDevices) GetSharedDeviceIDs() (sets.Set[structured.SharedDeviceID], int64) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.shareIDs.Clone(), a.revision
}

// Capacities 返回已消耗容量集合的拷贝以及当前 revision。
func (a *allocatedDevices) Capacities() (structured.ConsumedCapacityCollection, int64) {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.capacities.Clone(), a.revision
}

// Revision 返回当前 revision，用于判断缓存结果是否仍是最新的。
func (a *allocatedDevices) Revision() int64 {
	a.mutex.RLock()
	defer a.mutex.RUnlock()

	return a.revision
}

func (a *allocatedDevices) handlers() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    a.onAdd,
		UpdateFunc: a.onUpdate,
		DeleteFunc: a.onDelete,
	}
}

func (a *allocatedDevices) onAdd(obj any) {
	claim, _, err := schedutil.As[*resourceapi.ResourceClaim](obj, nil)
	if err != nil {
		// 理论上不会发生。
		a.logger.Error(err, "unexpected object in allocatedDevices.onAdd")
		return
	}

	if claim.Status.Allocation != nil {
		a.addDevices(claim)
	}
}

func (a *allocatedDevices) onUpdate(oldObj, newObj any) {
	originalClaim, modifiedClaim, err := schedutil.As[*resourceapi.ResourceClaim](oldObj, newObj)
	if err != nil {
		// 理论上不会发生。
		a.logger.Error(err, "unexpected object in allocatedDevices.onUpdate")
		return
	}

	switch {
	case originalClaim.Status.Allocation == nil && modifiedClaim.Status.Allocation != nil:
		a.addDevices(modifiedClaim)
	case originalClaim.Status.Allocation != nil && modifiedClaim.Status.Allocation == nil:
		a.removeDevices(originalClaim)
	default:
		// 无需处理：分配结果要么都为空，要么都不为空且内容相同（分配结果不可变）。
	}
}

func (a *allocatedDevices) onDelete(obj any) {
	claim, _, err := schedutil.As[*resourceapi.ResourceClaim](obj, nil)
	if err != nil {
		// 理论上不会发生。
		a.logger.Error(err, "unexpected object in allocatedDevices.onDelete")
		return
	}

	a.removeDevices(claim)
}

func (a *allocatedDevices) addDevices(claim *resourceapi.ResourceClaim) {
	if claim.Status.Allocation == nil {
		return
	}
	// 先在锁外算好要做的事情，尽量缩短持锁时间。
	deviceIDs := make([]structured.DeviceID, 0, 20)
	var shareIDs []structured.SharedDeviceID
	var deviceCapacities []structured.DeviceConsumedCapacity
	if a.enabledConsumableCapacity {
		shareIDs = make([]structured.SharedDeviceID, 0, 20)
		deviceCapacities = make([]structured.DeviceConsumedCapacity, 0, 20)
	}
	foreachAllocatedDevice(claim,
		func(deviceID structured.DeviceID) {
			a.logger.V(6).Info("Observed device allocation", "device", deviceID, "claim", klog.KObj(claim))
			deviceIDs = append(deviceIDs, deviceID)
		},
		a.enabledConsumableCapacity,
		func(sharedDeviceID structured.SharedDeviceID) {
			a.logger.V(6).Info("Observed shared device allocation", "shared device", sharedDeviceID, "claim", klog.KObj(claim))
			shareIDs = append(shareIDs, sharedDeviceID)
		},
		func(capacity structured.DeviceConsumedCapacity) {
			a.logger.V(6).Info("Observed consumed capacity", "device", capacity.DeviceID, "consumed capacity", capacity.ConsumedCapacity, "claim", klog.KObj(claim))
			deviceCapacities = append(deviceCapacities, capacity)
		},
	)

	if len(deviceIDs) == 0 && len(shareIDs) == 0 && len(deviceCapacities) == 0 {
		return
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.revision++
	for _, deviceID := range deviceIDs {
		a.ids.Insert(deviceID)
	}
	for _, shareID := range shareIDs {
		a.shareIDs.Insert(shareID)
	}
	for _, capacity := range deviceCapacities {
		a.capacities.Insert(capacity)
	}
}

func (a *allocatedDevices) removeDevices(claim *resourceapi.ResourceClaim) {
	if claim.Status.Allocation == nil {
		return
	}

	// 先在锁外算好要做的事情，尽量缩短持锁时间。
	deviceIDs := make([]structured.DeviceID, 0, 20)
	var shareIDs []structured.SharedDeviceID
	var deviceCapacities []structured.DeviceConsumedCapacity
	if a.enabledConsumableCapacity {
		shareIDs = make([]structured.SharedDeviceID, 0, 20)
		deviceCapacities = make([]structured.DeviceConsumedCapacity, 0, 20)
	}
	foreachAllocatedDevice(claim,
		func(deviceID structured.DeviceID) {
			a.logger.V(6).Info("Observed device deallocation", "device", deviceID, "claim", klog.KObj(claim))
			deviceIDs = append(deviceIDs, deviceID)
		},
		a.enabledConsumableCapacity,
		func(sharedDeviceID structured.SharedDeviceID) {
			a.logger.V(6).Info("Observed shared device deallocation", "shared device", sharedDeviceID, "claim", klog.KObj(claim))
			shareIDs = append(shareIDs, sharedDeviceID)
		},
		func(capacity structured.DeviceConsumedCapacity) {
			a.logger.V(6).Info("Observed consumed capacity release", "device id", capacity.DeviceID, "consumed capacity", capacity.ConsumedCapacity, "claim", klog.KObj(claim))
			deviceCapacities = append(deviceCapacities, capacity)
		})

	if len(deviceIDs) == 0 && len(shareIDs) == 0 && len(deviceCapacities) == 0 {
		return
	}

	a.mutex.Lock()
	defer a.mutex.Unlock()
	a.revision++
	for _, deviceID := range deviceIDs {
		a.ids.Delete(deviceID)
	}
	for _, shareID := range shareIDs {
		a.shareIDs.Delete(shareID)
	}
	for _, capacity := range deviceCapacities {
		a.capacities.Remove(capacity)
	}
}
