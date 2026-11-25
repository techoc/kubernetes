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

package internal

// 该文件应当移动到 pkg/scheduler/framework/autoscaler_constract 下（目录名沿用原文），
// 详见 https://github.com/kubernetes/kubernetes/issues/133161。

import (
	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/dynamic-resource-allocation/structured/schedulerapi"
)

// 以下类型别名指向实际维护这些定义的 schedulerapi 包，
// 确保对它们的修改需要自动扩缩容器批准。
// DeviceID 是设备唯一标识的别名。
// SharedDeviceID 是共享设备分配标识的别名。
// AllocatedState 是已分配资源状态的别名。
// ConsumedCapacity 是已消费容量映射的别名。
// ConsumedCapacityCollection 是按设备聚合的已消费容量集合的别名。
// DeviceConsumedCapacity 是单个设备已消费容量的别名。
type DeviceID = schedulerapi.DeviceID
type SharedDeviceID = schedulerapi.SharedDeviceID
type AllocatedState = schedulerapi.AllocatedState
type ConsumedCapacity = schedulerapi.ConsumedCapacity
type ConsumedCapacityCollection = schedulerapi.ConsumedCapacityCollection
type DeviceConsumedCapacity = schedulerapi.DeviceConsumedCapacity

// 以下包装函数委托给 schedulerapi 包。
// MakeDeviceID 构造设备 ID，委托给 schedulerapi 包。
func MakeDeviceID(driver, pool, device string) DeviceID {
	return schedulerapi.MakeDeviceID(driver, pool, device)
}

// MakeSharedDeviceID 构造共享设备 ID，委托给 schedulerapi 包。
func MakeSharedDeviceID(deviceID DeviceID, shareID *types.UID) SharedDeviceID {
	return schedulerapi.MakeSharedDeviceID(deviceID, shareID)
}

// NewConsumedCapacity 创建空的已消费容量映射，委托给 schedulerapi 包。
func NewConsumedCapacity() ConsumedCapacity {
	return schedulerapi.NewConsumedCapacity()
}

// NewConsumedCapacityCollection 创建空的已消费容量集合，委托给 schedulerapi 包。
func NewConsumedCapacityCollection() ConsumedCapacityCollection {
	return schedulerapi.NewConsumedCapacityCollection()
}

// NewDeviceConsumedCapacity 由设备 ID 与容量映射构造设备已消费容量，委托给 schedulerapi 包。
func NewDeviceConsumedCapacity(deviceID DeviceID, consumedCapacity map[resourceapi.QualifiedName]resource.Quantity) DeviceConsumedCapacity {
	return schedulerapi.NewDeviceConsumedCapacity(deviceID, consumedCapacity)
}

// IsDeviceAllocated 判断设备是否已分配，既考虑整设备分配，
// 也考虑启用可消费容量后的部分消费情形。
func IsDeviceAllocated(deviceID DeviceID, allocatedState *AllocatedState) bool {
	// 先检查是否为整设备分配（传统情形）。
	if allocatedState.AllocatedDevices.Has(deviceID) {
		return true
	}

	// 再检查是否通过共享分配被部分消费（可消费容量情形）。
	// 需要判断是否存在对应本设备的共享设备 ID。
	for sharedDeviceID := range allocatedState.AllocatedSharedDeviceIDs {
		if sharedDeviceID.GetDeviceID() == deviceID {
			return true
		}
	}

	// 对调度器生成的状态，已消费容量与共享设备 ID 一同记录；
	// 保留此检查以维持 IsDeviceAllocated 的语义，并兼容手工构造的状态及未来的状态生产者。
	if _, hasConsumedCapacity := allocatedState.AggregatedCapacity[deviceID]; hasConsumedCapacity {
		return true
	}

	// 三种情况都不满足，视为未分配。
	return false
}

// GenerateShareID 生成一个新的共享 ID；该工具函数保留在 internal 包中。
func GenerateShareID() *types.UID {
	// 生成一个新的 UUID 作为共享 ID。
	newUID := uuid.NewUUID()
	// 返回指向该 UUID 的指针。
	return &newUID
}
