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
	"fmt"
	"math"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2"
)

// RemoveHintsFromSlices removes topology hints on EndpointSlices and returns
// updated lists of EndpointSlices to create and update.
// RemoveHintsFromSlices 清除 EndpointSlice 上的拓扑提示，并返回更新后的待创建与待更新切片列表。
func RemoveHintsFromSlices(si *SliceInfo) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice) {
	// Remove hints on all EndpointSlices we were already going to change.
	// 对本来就要变更的切片（待创建 + 待更新）直接清空所有端点的提示。
	slices := append(si.ToCreate, si.ToUpdate...)
	for _, slice := range slices {
		for i := range slice.Endpoints {
			slice.Endpoints[i].Hints = nil
		}
	}

	// Remove hints on all unchanged EndpointSlices and mark them for update
	// if any already had hints. We use j to track the number/index of slices
	// that are still unchanged.
	// 对未变更切片：若其中已有提示，则清除提示并改为待更新；j 用于原地压缩仍未变更的切片。
	j := 0
	for _, slice := range si.Unchanged {
		changed := false
		for i, endpoint := range slice.Endpoints {
			if endpoint.Hints != nil {
				// Unchanged slices are still direct copies from informer cache.
				// Need to deep copy before we make any modifications to avoid
				// accidentally changing informer cache.
				// 未变更切片仍是 informer 缓存中的直接引用，修改前必须深拷贝，避免污染缓存。
				slice = slice.DeepCopy()
				slice.Endpoints[i].Hints = nil
				changed = true
			}
		}
		// 有变更的切片进入待更新列表，否则原地保留在未变更列表中。
		if changed {
			si.ToUpdate = append(si.ToUpdate, slice)
		} else {
			si.Unchanged[j] = slice
			j++
		}
	}

	// truncate si.Unchanged so it only includes slices that are still
	// unchanged.
	// 截断 Unchanged，只保留确实未变更的切片。
	si.Unchanged = si.Unchanged[:j]

	return si.ToCreate, si.ToUpdate
}

// FormatWithAddressType foramts a given string by adding an addressType to the end of it.
// FormatWithAddressType 在给定字符串末尾追加地址类型，用于区分事件消息对应的地址类型。
func FormatWithAddressType(s string, addressType discovery.AddressType) string {
	return fmt.Sprintf("%s, addressType: %s", s, addressType)
}

// redistributeHints redistributes hints based in the provided EndpointSlices.
// It allocates endpoints from the provided givingZones to the provided
// receivingZones. This returns a map that represents the changes in allocated
// endpoints by zone.
// redistributeHints 在给定切片上重分配提示：把 givingZones 中的端点分配给 receivingZones，
// 返回按可用区记录的端点分配变化量（接收区增加、给出区减少）。
func redistributeHints(logger klog.Logger, slices []*discovery.EndpointSlice, givingZones, receivingZones map[string]int) map[string]int {
	redistributions := map[string]int{}

	// 遍历所有可分配切片中的端点，寻找可由给出区转让给接收区的端点。
	for _, slice := range slices {
		for i, endpoint := range slice.Endpoints {
			// 未就绪端点不参与分配，直接清空提示。
			if !EndpointReady(endpoint) {
				endpoint.Hints = nil
				continue
			}
			// 给出区或接收区为空时已无需继续分配，直接返回当前变化量。
			if len(givingZones) == 0 || len(receivingZones) == 0 {
				return redistributions
			}
			if endpoint.Zone == nil || *endpoint.Zone == "" {
				// This should always be caught earlier in AddHints()
				// 缺少可用区信息理论上已在 AddHints 中拦截，这里仅记录并跳过。
				logger.Info("Endpoint found without zone specified")
				continue
			}

			// 若该端点所在可用区仍需要给出端点，则把它改指向一个仍缺少端点的接收区。
			givingZone := *endpoint.Zone
			numToGive, ok := givingZones[givingZone]
			if ok && numToGive > 0 {
				for receivingZone, numToReceive := range receivingZones {
					if numToReceive > 0 {
						// 改写端点的可用区提示为接收区。
						slice.Endpoints[i].Hints = &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: receivingZone}}}
						// 给出/接收计数递减；减到 0 时从映射中删除该项。
						if numToGive == 1 {
							delete(givingZones, givingZone)
						} else {
							givingZones[givingZone]--
						}
						if numToReceive == 1 {
							delete(receivingZones, receivingZone)
						} else {
							receivingZones[receivingZone]--
						}

						// 记录变化量：接收区增加、给出区减少各一个端点。
						redistributions[receivingZone]++
						redistributions[givingZone]--

						break
					}
				}
			}
		}
	}
	return redistributions
}

// getGivingAndReceivingZones returns the number of endpoints each zone should
// give to other zones along with the number of endpoints each zone should
// receive from other zones. This is calculated with the provided allocations
// (desired state) and allocatedHintsByZone (current state).
// getGivingAndReceivingZones 返回每个可用区应给出/接收的端点数，由期望分配 allocations 与当前分配 allocatedHintsByZone 计算得出。
func getGivingAndReceivingZones(allocations map[string]allocation, allocatedHintsByZone map[string]int) (map[string]int, map[string]int) {
	// 1. Determine the precise number of additional endpoints each zone has
	//    (giving) or needs (receiving).
	// 第一步：以浮点精度计算每个可用区多出（给出）或缺少（接收）的端点数。
	givingZonesDesired := map[string]float64{}
	receivingZonesDesired := map[string]float64{}

	for zone, allocation := range allocations {
		allocatedHints, _ := allocatedHintsByZone[zone]
		target := allocation.desired
		// 当前分配超过期望的部分记为给出量，不足的部分记为接收量。
		if float64(allocatedHints) > target {
			givingZonesDesired[zone] = float64(allocatedHints) - target
		} else if float64(allocatedHints) < target {
			receivingZonesDesired[zone] = target - float64(allocatedHints)
		}
	}

	// 2. Convert the precise numbers needed into ints representing real
	//    endpoints given from one zone to another.
	// 第二步：把浮点需求转换为整数个真实端点，得到给出区与接收区的计数。
	givingZones := map[string]int{}
	receivingZones := map[string]int{}

	// 循环选取给出量/接收量最大的可用区，逐次配对并递减，直到剩余需求不足一个端点。
	for {
		givingZone, numToGive := getMost(givingZonesDesired)
		receivingZone, numToReceive := getMost(receivingZonesDesired)

		// return early if any of the following are true:
		// - giving OR receiving zone are unspecified
		// - giving AND receiving zones have less than 1 endpoint left to give or receive
		// - giving OR receiving zones have less than 0.5 endpoints left to give or receive
		// 以下任一情况成立时提前结束：给出区或接收区不存在；两侧都不足 1 个端点；任一侧不足 0.5 个端点。
		if givingZone == "" || receivingZone == "" || (numToGive < 1.0 && numToReceive < 1.0) || numToGive < 0.5 || numToReceive < 0.5 {
			break
		}

		// 配对成功：两侧的实际给出/接收计数加一，剩余浮点需求各减一。
		givingZones[givingZone]++
		givingZonesDesired[givingZone]--
		receivingZones[receivingZone]++
		receivingZonesDesired[receivingZone]--
	}

	return givingZones, receivingZones
}

// getMost accepts a map[string]float64 and returns the string and float64 that
// represent the greatest value in this provided map. This function is not very
// efficient but it is expected that len() will rarely be greater than 2.
// getMost 返回映射中最大值对应的键和值；实现为线性扫描，因可用区数量通常很少，效率可以接受。
func getMost(zones map[string]float64) (string, float64) {
	zone := ""
	num := 0.0
	// 线性扫描找出最大值；若所有值都不大于 0，则返回空字符串和 0。
	for z, n := range zones {
		if n > num {
			zone = z
			num = n
		}
	}

	return zone, num
}

// getHintsByZone returns the number of hints allocated to each zone by the
// provided EndpointSlice. This function returns nil to indicate that the
// current allocations are invalid and that the EndpointSlice needs to be
// updated. This could be caused by:
// - A hint for a zone that no longer requires any allocations.
// - An endpoint with no hints.
// - Hints that would make minimum allocations impossible.
// getHintsByZone 统计该 EndpointSlice 为各可用区分配的提示数；返回 nil 表示当前分配无效、切片需要更新。
// 无效原因包括：提示指向不再需要分配的可用区、端点没有提示，或提示会使最小分配量无法实现。
func getHintsByZone(slice *discovery.EndpointSlice, allocatedHintsByZone EndpointZoneInfo, allocations map[string]allocation) map[string]int {
	hintsByZone := map[string]int{}
	// 逐个端点校验提示：未就绪端点跳过，无提示或提示指向未知可用区则返回 nil。
	for _, endpoint := range slice.Endpoints {
		if !EndpointReady(endpoint) {
			continue
		}
		if endpoint.Hints == nil || len(endpoint.Hints.ForZones) == 0 {
			return nil
		}

		// 累加每个提示对应可用区的数量；若可用区不在分配方案中同样视为无效。
		for i := range endpoint.Hints.ForZones {
			zone := endpoint.Hints.ForZones[i].Name
			if _, ok := allocations[zone]; !ok {
				return nil
			}
			hintsByZone[zone]++
		}
	}

	// 二次校验：加上本切片已有的提示后不得超过该可用区的分配上限。
	for zone, numHints := range hintsByZone {
		alreadyAllocated, _ := allocatedHintsByZone[zone]
		allocation, ok := allocations[zone]
		if !ok || (numHints+alreadyAllocated) > allocation.maximum {
			return nil
		}
	}

	return hintsByZone
}

// serviceOverloaded returns true if the Service has an insufficient amount of
// endpoints for any zone.
// serviceOverloaded 判断该 Service 在任一可用区上的端点数是否低于按 CPU 比例计算的最小需求。
func serviceOverloaded(ezi EndpointZoneInfo, zoneRatios map[string]float64) bool {
	// 没有端点分布时视为未过载。
	if len(ezi) == 0 {
		return false
	}
	// 没有可用区比例却存在端点时视为过载。
	if len(zoneRatios) == 0 {
		return true
	}

	// 统计该 Service 的端点总数。
	totalEndpoints := 0.0
	for _, numEndpoints := range ezi {
		totalEndpoints += float64(numEndpoints)
	}

	// 逐可用区校验：缺失该区或端点数低于考虑过载阈值后的最小值，均判定为过载。
	for zone, ratio := range zoneRatios {
		svcEndpoints, ok := ezi[zone]
		if !ok {
			return true
		}
		minEndpoints := math.Ceil(totalEndpoints * ratio * (1 / (1 + overloadThreshold)))
		if svcEndpoints < int(minEndpoints) {
			return true
		}
	}

	return false
}

// isNodeReady returns true if a node is ready; false otherwise.
// isNodeReady 检查节点的 Ready 条件是否为 True。
func isNodeReady(node *v1.Node) bool {
	// 遍历节点状态条件，找到 NodeReady 后立即返回其状态是否为 True。
	for _, c := range node.Status.Conditions {
		if c.Type == v1.NodeReady {
			return c.Status == v1.ConditionTrue
		}
	}
	// 没有 Ready 条件时视为未就绪。
	return false
}

// numReadyEndpoints returns the number of Endpoints that are ready from the
// specified list.
// numReadyEndpoints 统计给定端点列表中处于就绪状态的端点数量。
func numReadyEndpoints(endpoints []discovery.Endpoint) int {
	var total int
	// 仅统计 Ready 条件存在且为 true 的端点。
	for _, ep := range endpoints {
		if ep.Conditions.Ready != nil && *ep.Conditions.Ready {
			total++
		}
	}
	return total
}

// EndpointReady returns true if an Endpoint has the Ready condition set to
// true.
// EndpointReady 判断单个端点的 Ready 条件是否被显式设置为 true。
func EndpointReady(endpoint discovery.Endpoint) bool {
	return endpoint.Conditions.Ready != nil && *endpoint.Conditions.Ready
}
