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
	"sync"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

const (
	// overloadThreshold represents the maximum overload any individual endpoint
	// should be exposed to.
	// overloadThreshold 表示单个端点可承受的最大过载比例。
	overloadThreshold float64 = 0.2
)

// TopologyCache tracks the distribution of Nodes and endpoints across zones.
// TopologyCache 记录节点与端点在各可用区之间的分布情况。
type TopologyCache struct {
	// lock 保护本结构体中所有可变字段的并发访问。
	// sufficientNodeInfo 表示目前的节点信息是否足以支撑拓扑感知提示。
	// cpuByZone 记录每个可用区内就绪节点的可分配 CPU 总量。
	// cpuRatiosByZone 记录每个可用区占集群总 CPU 的比例。
	// endpointsByService 按 Service 键和地址类型记录端点在各可用区的分布。
	// hintsPopulatedByService 记录已填充过提示的 Service 键集合。
	lock                    sync.Mutex
	sufficientNodeInfo      bool
	cpuByZone               map[string]*resource.Quantity
	cpuRatiosByZone         map[string]float64
	endpointsByService      map[string]map[discovery.AddressType]EndpointZoneInfo
	hintsPopulatedByService sets.Set[string]
}

// EndpointZoneInfo tracks the distribution of endpoints across zones for a
// Service.
// EndpointZoneInfo 记录某个 Service 的端点在各可用区之间的分布（可用区名到端点数的映射）。
type EndpointZoneInfo map[string]int

// allocation describes the number of endpoints that should be allocated for a
// zone.
// allocation 描述某个可用区应分配的端点数量范围。
type allocation struct {
	// minimum 表示该可用区最少需要分配的端点数。
	// maximum 表示该可用区最多可以分配的端点数。
	// desired 表示该可用区理想情况下期望的端点数。
	minimum int
	maximum int
	desired float64
}

// NewTopologyCache initializes a new TopologyCache.
// NewTopologyCache 初始化并返回一个新的 TopologyCache。
func NewTopologyCache() *TopologyCache {
	// 初始化各映射与集合，避免后续写入时出现 nil map panic。
	return &TopologyCache{
		cpuByZone:               map[string]*resource.Quantity{},
		cpuRatiosByZone:         map[string]float64{},
		endpointsByService:      map[string]map[discovery.AddressType]EndpointZoneInfo{},
		hintsPopulatedByService: sets.Set[string]{},
	}
}

// GetOverloadedServices returns a list of Service keys that refer to Services
// that have crossed the overload threshold for any zone.
// GetOverloadedServices 返回在任何可用区上越过过载阈值的 Service 键列表。
func (t *TopologyCache) GetOverloadedServices() []string {
	t.lock.Lock()
	defer t.lock.Unlock()

	svcKeys := []string{}
	// 遍历每个 Service 及其各地址类型的端点分布，发现任一地址类型过载即记录该 Service。
	for svcKey, eziByAddrType := range t.endpointsByService {
		for _, ezi := range eziByAddrType {
			if serviceOverloaded(ezi, t.cpuRatiosByZone) {
				svcKeys = append(svcKeys, svcKey)
				break
			}
		}
	}

	return svcKeys
}

// AddHints adds or updates topology hints on EndpointSlices and returns updated
// lists of EndpointSlices to create and update.
// AddHints 在 EndpointSlice 上添加或更新拓扑提示，并返回更新后的待创建与待更新切片列表。
func (t *TopologyCache) AddHints(logger klog.Logger, si *SliceInfo) ([]*discovery.EndpointSlice, []*discovery.EndpointSlice, []*EventBuilder) {
	// 先统计就绪端点总数，再据此计算各可用区的分配方案。
	totalEndpoints := si.getTotalReadyEndpoints()
	allocations, allocationsEvent := t.getAllocations(totalEndpoints)
	events := []*EventBuilder{}
	// 若无法得到合法分配（节点信息不足、可用区过少等），则移除提示并直接返回事件。
	if allocationsEvent != nil {
		logger.Info(allocationsEvent.Message+", removing hints", "key", si.ServiceKey, "addressType", si.AddressType)
		allocationsEvent.Message = FormatWithAddressType(allocationsEvent.Message, si.AddressType)
		events = append(events, allocationsEvent)
		t.RemoveHints(si.ServiceKey, si.AddressType)
		slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
		return slicesToCreate, slicesToUpdate, events
	}

	// 汇总未变更切片中已有的提示，并标记其中需要更新的切片。
	allocatedHintsByZone := si.getAllocatedHintsByZone(allocations)

	// 收集所有可被重新分配提示的切片（待创建 + 待更新）。
	allocatableSlices := si.ToCreate
	for _, slice := range si.ToUpdate {
		allocatableSlices = append(allocatableSlices, slice)
	}

	// step 1: assign same-zone hints for all endpoints as a starting point.
	// 第一步：以“端点所在可用区”作为起点，为就绪端点分配同可用区提示。
	for _, slice := range allocatableSlices {
		for i, endpoint := range slice.Endpoints {
			// 未就绪的端点不需要提示，直接清空。
			if !EndpointReady(endpoint) {
				endpoint.Hints = nil
				continue
			}
			// 端点缺少可用区信息时无法分配提示：记录警告事件、清除提示并返回。
			if endpoint.Zone == nil || *endpoint.Zone == "" {
				logger.Info("Endpoint found without zone specified, removing hints", "key", si.ServiceKey, "addressType", si.AddressType)
				events = append(events, &EventBuilder{
					EventType: v1.EventTypeWarning,
					Reason:    "TopologyAwareHintsDisabled",
					Message:   FormatWithAddressType(NoZoneSpecified, si.AddressType),
				})
				t.RemoveHints(si.ServiceKey, si.AddressType)
				slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
				return slicesToCreate, slicesToUpdate, events
			}

			// 累加该可用区已分配的提示数，并写入同可用区提示。
			allocatedHintsByZone[*endpoint.Zone]++
			slice.Endpoints[i].Hints = &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: *endpoint.Zone}}}
		}
	}

	// step 2. Identify which zones need to donate slices and which need more.
	// 第二步：根据期望分配与当前分配的差距，找出需要给出端点与需要接收端点的可用区。
	givingZones, receivingZones := getGivingAndReceivingZones(allocations, allocatedHintsByZone)

	// step 3. Redistribute endpoints based on data from step 2.
	// 第三步：在可分配切片上执行提示重分配，得到各可用区提示数的变化量。
	redistributions := redistributeHints(logger, allocatableSlices, givingZones, receivingZones)

	// 将重分配造成的增减量累加回按可用区的提示统计。
	for zone, diff := range redistributions {
		allocatedHintsByZone[zone] += diff
	}

	// 若最终没有任何可用区获得提示，则视为分配失败，清除提示并返回事件。
	if len(allocatedHintsByZone) == 0 {
		logger.V(2).Info("No hints allocated for zones, removing them", "key", si.ServiceKey, "addressType", si.AddressType)
		events = append(events, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   FormatWithAddressType(NoAllocatedHintsForZones, si.AddressType),
		})
		t.RemoveHints(si.ServiceKey, si.AddressType)
		slicesToCreate, slicesToUpdate := RemoveHintsFromSlices(si)
		return slicesToCreate, slicesToUpdate, events
	}

	// 加锁保存本次分配结果；同时记录保存前是否已启用提示。
	t.lock.Lock()
	defer t.lock.Unlock()
	hintsEnabled := t.hasPopulatedHintsLocked(si.ServiceKey)
	t.setHintsLocked(si.ServiceKey, si.AddressType, allocatedHintsByZone)

	// if hints were not enabled before, we publish an event to indicate we enabled them.
	// 如果此前未启用过提示，则发布一个“已启用”事件通知外部。
	if !hintsEnabled {
		logger.Info("Topology Aware Hints has been enabled, adding hints.", "key", si.ServiceKey, "addressType", si.AddressType)
		events = append(events, &EventBuilder{
			EventType: v1.EventTypeNormal,
			Reason:    "TopologyAwareHintsEnabled",
			Message:   FormatWithAddressType(TopologyAwareHintsEnabled, si.AddressType),
		})
	}
	return si.ToCreate, si.ToUpdate, events
}

// SetHints sets topology hints for the provided serviceKey and addrType in this
// cache.
// SetHints 在缓存中为指定的 serviceKey 和 addrType 设置拓扑提示。
func (t *TopologyCache) SetHints(serviceKey string, addrType discovery.AddressType, allocatedHintsByZone EndpointZoneInfo) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// 委托给不加锁的内部实现，避免重复加锁。
	t.setHintsLocked(serviceKey, addrType, allocatedHintsByZone)
}

// setHintsLocked 在已持有锁的前提下写入提示分布，并标记该 Service 已有提示。
func (t *TopologyCache) setHintsLocked(serviceKey string, addrType discovery.AddressType, allocatedHintsByZone EndpointZoneInfo) {
	// 若该 Service 尚无地址类型映射，先创建。
	_, ok := t.endpointsByService[serviceKey]
	if !ok {
		t.endpointsByService[serviceKey] = map[discovery.AddressType]EndpointZoneInfo{}
	}
	// 写入该地址类型对应的提示分布。
	t.endpointsByService[serviceKey][addrType] = allocatedHintsByZone

	// 记录该 Service 已填充过提示，供事件上报判断。
	t.hintsPopulatedByService.Insert(serviceKey)
}

// RemoveHints removes topology hints for the provided serviceKey and addrType
// from this cache.
// RemoveHints 从缓存中移除指定 serviceKey 和 addrType 对应的拓扑提示。
func (t *TopologyCache) RemoveHints(serviceKey string, addrType discovery.AddressType) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// 先删除该地址类型的提示分布；若该 Service 已无任何地址类型，则删除整条记录。
	_, ok := t.endpointsByService[serviceKey]
	if ok {
		delete(t.endpointsByService[serviceKey], addrType)
	}
	if len(t.endpointsByService[serviceKey]) == 0 {
		delete(t.endpointsByService, serviceKey)
	}
	// 同时从“已填充提示”集合中移除该 Service。
	t.hintsPopulatedByService.Delete(serviceKey)
}

// SetNodes updates the Node distribution for the TopologyCache.
// SetNodes 根据给定的节点列表更新 TopologyCache 中按可用区分布的节点 CPU 信息。
func (t *TopologyCache) SetNodes(logger klog.Logger, nodes []*v1.Node) {
	cpuByZone := map[string]*resource.Quantity{}
	sufficientNodeInfo := true

	totalCPU := resource.Quantity{}

	// 遍历节点，跳过被排除或就绪状态不满足的节点，并统计各可用区的可分配 CPU。
	for _, node := range nodes {
		// 带有排除标签的节点不参与容量计算。
		if hasExcludedLabels(node.Labels) {
			logger.V(6).Info("Ignoring node because it has an excluded label", "node", klog.KObj(node))
			continue
		}
		// 未就绪的节点不参与容量计算。
		if !isNodeReady(node) {
			logger.V(6).Info("Ignoring node because it is not ready", "node", klog.KObj(node))
			continue
		}

		nodeCPU := node.Status.Allocatable.Cpu()
		zone, ok := node.Labels[v1.LabelTopologyZone]

		// TODO(robscott): Figure out if there's an acceptable proportion of
		// nodes with inadequate information. The current logic means that as
		// soon as we find any node without a zone or allocatable CPU specified,
		// we bail out entirely. Bailing out at this level will make our cluster
		// wide ratios nil, which would result in slices for all Services having
		// their hints removed.
		// 缺少可用区或可分配 CPU 信息时整体放弃计算：将清空统计并终止遍历，
		// 这会使集群 CPU 比例变为 nil，进而导致所有 Service 的提示被移除。
		if !ok || zone == "" || nodeCPU.IsZero() {
			cpuByZone = map[string]*resource.Quantity{}
			sufficientNodeInfo = false
			logger.Info("Can't get CPU or zone information for node", "node", klog.KObj(node))
			break
		}

		// 累加集群总 CPU，并把当前节点 CPU 归入所属可用区。
		totalCPU.Add(*nodeCPU)
		if _, ok = cpuByZone[zone]; !ok {
			cpuByZone[zone] = nodeCPU
		} else {
			cpuByZone[zone].Add(*nodeCPU)
		}
	}

	t.lock.Lock()
	defer t.lock.Unlock()

	// 信息不足（无 CPU、可用区少于两个或半途放弃）时清空节点比例，禁用拓扑提示。
	if totalCPU.IsZero() || !sufficientNodeInfo || len(cpuByZone) < 2 {
		logger.V(2).Info("Insufficient node info for topology hints", "totalZones", len(cpuByZone), "totalCPU", totalCPU.String(), "sufficientNodeInfo", sufficientNodeInfo)
		t.sufficientNodeInfo = false
		t.cpuByZone = nil
		t.cpuRatiosByZone = nil

	} else {
		// 信息充分时保存分布，并用毫核数计算每个可用区占集群总 CPU 的比例。
		t.sufficientNodeInfo = sufficientNodeInfo
		t.cpuByZone = cpuByZone

		t.cpuRatiosByZone = map[string]float64{}
		for zone, cpu := range cpuByZone {
			t.cpuRatiosByZone[zone] = float64(cpu.MilliValue()) / float64(totalCPU.MilliValue())
		}
	}
}

// HasPopulatedHints checks whether there are populated hints for a given service in the cache.
// HasPopulatedHints 检查缓存中指定 Service 是否已填充过提示。
func (t *TopologyCache) HasPopulatedHints(serviceKey string) bool {
	t.lock.Lock()
	defer t.lock.Unlock()

	return t.hasPopulatedHintsLocked(serviceKey)
}

// hasPopulatedHintsLocked 在已持有锁的前提下查询 Service 是否已填充过提示。
func (t *TopologyCache) hasPopulatedHintsLocked(serviceKey string) bool {
	return t.hintsPopulatedByService.Has(serviceKey)
}

// getAllocations returns a set of minimum and maximum allocations per zone. If
// it is not possible to provide allocations that are below the overload
// threshold, a nil value will be returned.
// getAllocations 返回每个可用区的最小与最大分配量；若无法在不超出过载阈值的前提下完成分配，则返回 nil。
func (t *TopologyCache) getAllocations(numEndpoints int) (map[string]allocation, *EventBuilder) {
	t.lock.Lock()
	defer t.lock.Unlock()

	// it is similar to checking !t.sufficientNodeInfo
	// CPU 比例为空等价于节点信息不足，此时无法计算分配方案并返回警告事件。
	if t.cpuRatiosByZone == nil {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   InsufficientNodeInfo,
		}
	}
	// 可用区不足两个时无法进行跨可用区分配。
	if len(t.cpuRatiosByZone) < 2 {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   NodesReadyInOneZoneOnly,
		}
	}
	// 端点数少于可用区数时，无法满足每个可用区至少分配一个端点。
	if len(t.cpuRatiosByZone) > numEndpoints {
		return nil, &EventBuilder{
			EventType: v1.EventTypeWarning,
			Reason:    "TopologyAwareHintsDisabled",
			Message:   fmt.Sprintf("%s (%d endpoints, %d zones)", InsufficientNumberOfEndpoints, numEndpoints, len(t.cpuRatiosByZone)),
		}
	}

	// 逐个可用区计算最小分配量，同时累计总和并检查是否超出端点总数。
	remainingMinEndpoints := numEndpoints
	minTotal := 0
	allocations := map[string]allocation{}

	for zone, ratio := range t.cpuRatiosByZone {
		// 理想值按 CPU 占比乘以端点总数；最小值为理想值按过载上限折算后向上取整。
		desired := ratio * float64(numEndpoints)
		minimum := int(math.Ceil(desired * (1 / (1 + overloadThreshold))))
		allocations[zone] = allocation{
			minimum: minimum,
			desired: math.Max(desired, float64(minimum)),
		}
		// 若各可用区最小分配量之和超过端点总数，则无法完成分配。
		minTotal += minimum
		remainingMinEndpoints -= minimum
		if remainingMinEndpoints < 0 {
			return nil, &EventBuilder{
				EventType: v1.EventTypeWarning,
				Reason:    "TopologyAwareHintsDisabled",
				Message:   fmt.Sprintf("%s (%d endpoints, %d zones)", MinAllocationExceedsOverloadThreshold, numEndpoints, len(t.cpuRatiosByZone)),
			}
		}
	}

	// 把扣除最小分配量后剩余的端点额度作为每个可用区的可分配上限。
	for zone, allocation := range allocations {
		allocation.maximum = allocation.minimum + numEndpoints - minTotal
		allocations[zone] = allocation
	}

	return allocations, nil
}

// Nodes with any of these labels set to any value will be excluded from
// topology capacity calculations.
// hasExcludedLabels 判断节点是否带有需要排除的标签；带有任一排除标签的节点不参与拓扑容量计算。
func hasExcludedLabels(labels map[string]string) bool {
	// 没有标签时自然无需排除。
	if len(labels) == 0 {
		return false
	}
	// 控制平面节点（control-plane 标签）不计入容量。
	if _, ok := labels["node-role.kubernetes.io/control-plane"]; ok {
		return true
	}
	// 旧版本的 master 标签同样需要排除。
	if _, ok := labels["node-role.kubernetes.io/master"]; ok {
		return true
	}
	return false
}
