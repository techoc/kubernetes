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

package stable

import (
	resourceapi "k8s.io/api/resource/v1"
)

// sliceUsesCompatibilityGroups reports whether any device in the slice declares
// compatibility groups on one of its counter consumptions.
//
// While the DRADeviceCompatibilityGroups feature is disabled, GatherPools
// ignores such slices entirely: this allocator cannot validate co-allocation
// against compatibility groups, so those devices must not be allocated, and
// ignoring the slice makes the pool incomplete, which prevents allocating the
// pool's other devices, too. Whether any of the devices are already allocated
// is deliberately irrelevant, keeping the outcome independent of allocation
// order. This lets the feature be enabled later without deleting pods.
// sliceUsesCompatibilityGroups 判断切片中是否有设备在其计数器消费声明里携带兼容组。
// 只要任一 ConsumesCounters 条目声明了兼容组即返回 true；否则返回 false。
func sliceUsesCompatibilityGroups(slice *resourceapi.ResourceSlice) bool {
	// 遍历切片中的所有设备。
	for _, device := range slice.Spec.Devices {
		// 检查该设备的每一次计数器消费。
		for _, deviceCounterConsumption := range device.ConsumesCounters {
			// 一旦发现有兼容组声明即可提前返回。
			if len(deviceCounterConsumption.CompatibilityGroups) > 0 {
				return true
			}
		}
	}
	// 没有任何设备声明兼容组。
	return false
}

// slicesWithoutCompatibilityGroups returns the slices which do not match
// sliceUsesCompatibilityGroups, preserving their order.
// slicesWithoutCompatibilityGroups 过滤掉声明了兼容组的切片，并按原顺序保留其余切片。
func slicesWithoutCompatibilityGroups(slices []*resourceapi.ResourceSlice) []*resourceapi.ResourceSlice {
	// 预分配结果容量，最多与输入等长。
	filtered := make([]*resourceapi.ResourceSlice, 0, len(slices))
	// 保持输入切片的相对顺序。
	for _, slice := range slices {
		// 使用兼容组的切片需要被排除。
		if sliceUsesCompatibilityGroups(slice) {
			continue
		}
		// 保留不使用兼容组的切片。
		filtered = append(filtered, slice)
	}
	return filtered
}
