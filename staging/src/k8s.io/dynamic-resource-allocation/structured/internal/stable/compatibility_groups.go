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

// sliceUsesCompatibilityGroups 判断切片中是否有设备在其计数器消费声明里携带兼容组。
// 只要任一 ConsumesCounters 条目声明了兼容组即返回 true；否则返回 false。
// 特性关闭时 GatherPools 会忽略这类切片：本分配器无法校验共同分配，忽略后池变为不完整，
// 也会阻止该池其他设备被分配。无论设备是否已分配都采用相同处理，以保持结果与分配顺序无关，
// 便于日后启用特性而无需删除 Pod。
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
