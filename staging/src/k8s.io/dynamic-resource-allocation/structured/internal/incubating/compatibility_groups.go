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

package incubating

import (
	resourceapi "k8s.io/api/resource/v1"
)

// sliceUsesCompatibilityGroups 报告切片中是否有设备在其计数器消耗上声明了兼容性组。
// 特性关闭时 GatherPools 会整体忽略这类切片：本分配器无法校验兼容性组的共同分配，
// 故这些设备不可被分配；忽略切片会令池不完整，从而阻止该池其他设备被分配。
// 是否有设备已分配被有意忽略，以保持结果与分配顺序无关，便于后续启用特性而不删除 Pod。
// 遍历切片内所有设备及其计数器消耗。
func sliceUsesCompatibilityGroups(slice *resourceapi.ResourceSlice) bool {
	// 遍历单台设备的计数器消耗声明。
	for _, device := range slice.Spec.Devices {
		// 任一声明带有兼容性组即视为使用了该特性。
		for _, deviceCounterConsumption := range device.ConsumesCounters {
			if len(deviceCounterConsumption.CompatibilityGroups) > 0 {
				return true
			}
		}
	}
	// 没有设备使用兼容性组。
	return false
}

// slicesWithoutCompatibilityGroups 过滤掉使用兼容性组的切片，并保持原有顺序。
func slicesWithoutCompatibilityGroups(slices []*resourceapi.ResourceSlice) []*resourceapi.ResourceSlice {
	// 预分配结果切片，容量上限为输入长度。
	filtered := make([]*resourceapi.ResourceSlice, 0, len(slices))
	// 按顺序遍历输入切片。
	for _, slice := range slices {
		// 使用兼容性组的切片被跳过。
		if sliceUsesCompatibilityGroups(slice) {
			continue
		}
		// 保留不使用兼容性组的切片。
		filtered = append(filtered, slice)
	}
	return filtered
}
