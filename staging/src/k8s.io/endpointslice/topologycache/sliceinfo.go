/*
Copyright 2021 The Kubernetes Authors.

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

package topologycache

import (
	discovery "k8s.io/api/discovery/v1"
)

// SliceInfo 保存协调过程中与 EndpointSlice 相关的信息。
type SliceInfo struct {
	// ServiceKey 表示所属 Service 的键。
	// AddressType 表示这些 EndpointSlice 的地址类型。
	// ToCreate 表示需要新建的 EndpointSlice 列表。
	// ToUpdate 表示需要更新的 EndpointSlice 列表。
	// Unchanged 表示无需变更的 EndpointSlice 列表。
	ServiceKey  string
	AddressType discovery.AddressType
	ToCreate    []*discovery.EndpointSlice
	ToUpdate    []*discovery.EndpointSlice
	Unchanged   []*discovery.EndpointSlice
}

// getTotalReadyEndpoints 统计待创建、待更新和未变更的 EndpointSlice 中就绪端点的总数。
func (si *SliceInfo) getTotalReadyEndpoints() int {
	totalEndpoints := 0
	// 累加待创建切片中的就绪端点数。
	for _, slice := range si.ToCreate {
		totalEndpoints += numReadyEndpoints(slice.Endpoints)
	}
	// 累加待更新切片中的就绪端点数。
	for _, slice := range si.ToUpdate {
		totalEndpoints += numReadyEndpoints(slice.Endpoints)
	}
	// 累加未变更切片中的就绪端点数。
	for _, slice := range si.Unchanged {
		totalEndpoints += numReadyEndpoints(slice.Endpoints)
	}
	return totalEndpoints
}

// getAllocatedHintsByZone 汇总未变更切片中当前已分配的提示，并在必要时将切片标记为需要更新。
// 当出现以下任一情况时需要更新切片：端点没有可用区提示；提示指向已不再需要分配的可用区；
// 或者现有提示会导致在已更新/新建切片的基础上无法满足最小分配量。
func (si *SliceInfo) getAllocatedHintsByZone(allocations map[string]allocation) EndpointZoneInfo {
	allocatedHintsByZone := EndpointZoneInfo{}

	// 使用原地过滤移除不再属于“未变更”集合的切片，j 记录保留下来的切片数量。
	// 技巧说明：https://github.com/golang/go/wiki/SliceTricks#filter-in-place
	j := 0
	for _, slice := range si.Unchanged {
		// 计算该切片按可用区分配的提示；返回 nil 表示提示已失效，需要更新该切片。
		hintsByZone := getHintsByZone(slice, allocatedHintsByZone, allocations)
		if hintsByZone == nil {
			// 失效时深拷贝切片并加入待更新列表，避免修改 informer 缓存中的原对象。
			si.ToUpdate = append(si.ToUpdate, slice.DeepCopy())
		} else {
			// 仍有效的切片原地保留，并把它贡献的提示数累加到按可用区的统计中。
			si.Unchanged[j] = slice
			j++
			for zone, numHints := range hintsByZone {
				allocatedHintsByZone[zone] += numHints
			}
		}
	}

	// 截断 Unchanged，只保留仍为未变更的切片，并返回汇总结果。
	si.Unchanged = si.Unchanged[:j]
	return allocatedHintsByZone
}
