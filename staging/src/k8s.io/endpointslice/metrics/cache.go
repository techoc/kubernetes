/*
Copyright 2019 The Kubernetes Authors.

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

package metrics

import (
	"math"
	"sync"

	"k8s.io/apimachinery/pkg/types"
	endpointsliceutil "k8s.io/endpointslice/util"
)

// NewCache 使用指定的 endpointsPerSlice 创建一个新的 Cache。
func NewCache(endpointsPerSlice int32) *Cache {
	// 初始化缓存：记录单切片端点上限、按 Service 索引的端口缓存，以及按 trafficDistribution 分组的 Service 集合。
	return &Cache{
		maxEndpointsPerSlice:          endpointsPerSlice,
		cache:                         map[types.NamespacedName]*ServicePortCache{},
		servicesByTrafficDistribution: make(map[string]map[types.NamespacedName]bool),
	}
}

// Cache 记录期望端点总数以及 EndpointSlice 端点分发效率相关的指标值。
type Cache struct {
	// maxEndpointsPerSlice 表示单个 EndpointSlice 中应添加的最大端点数。
	maxEndpointsPerSlice int32

	// lock 保护对 numEndpoints、numSlicesActual、numSlicesDesired、cache 和 servicesByTrafficDistribution 的修改。
	lock sync.Mutex
	// numEndpoints 表示 EndpointSlices 中存储的端点总数。
	numEndpoints int
	// numSlicesActual 表示 EndpointSlices 的实际总数。
	numSlicesActual int
	// numSlicesDesired 表示 EndpointSlices 的期望总数。
	numSlicesDesired int
	// cache 以 Service 的 NamespacedName 为键，保存对应的 ServicePortCache。
	cache map[types.NamespacedName]*ServicePortCache
	// servicesByTrafficDistribution 按 trafficDistribution 字段对所有 Service 分组。
	// 其类型应读作 map[trafficDistribution]setOfServices，即每种流量分布对应一个 Service 集合。
	servicesByTrafficDistribution map[string]map[types.NamespacedName]bool
}

// ServicePortCache 按唯一的 Service Port 组合记录期望端点总数以及 EndpointSlice 端点分发效率。
type ServicePortCache struct {
	// items 以端口组合的 PortMapKey 为键，保存各端口组合对应的端点与切片统计。
	items map[endpointsliceutil.PortMapKey]EfficiencyInfo
}

// EfficiencyInfo 保存端点数和切片数，用于计算期望端点总数与 EndpointSlice 端点分发效率。
type EfficiencyInfo struct {
	// Endpoints 表示该端口组合下的端点数量。
	// Slices 表示该端口组合下的 EndpointSlice 数量。
	Endpoints int
	Slices    int
}

// NewServicePortCache 初始化并返回一个空的 ServicePortCache。
func NewServicePortCache() *ServicePortCache {
	return &ServicePortCache{
		items: map[endpointsliceutil.PortMapKey]EfficiencyInfo{},
	}
}

// Set 将指定 PortMapKey 对应的 EfficiencyInfo 写入缓存。
func (spc *ServicePortCache) Set(pmKey endpointsliceutil.PortMapKey, eInfo EfficiencyInfo) {
	spc.items[pmKey] = eInfo
}

// totals 汇总该 ServicePortCache 中所有端口组合的端点总数与切片总数。
func (spc *ServicePortCache) totals(maxEndpointsPerSlice int) (int, int, int) {
	var actualSlices, desiredSlices, endpoints int
	// 遍历各端口组合：累加端点数、实际切片数，并按理想分配计算期望切片数。
	for _, eInfo := range spc.items {
		endpoints += eInfo.Endpoints
		actualSlices += eInfo.Slices
		desiredSlices += numDesiredSlices(eInfo.Endpoints, maxEndpointsPerSlice)
	}
	// 即使没有任何端点，也始终保留一个占位切片，因此期望切片数至少为 1。
	if desiredSlices == 0 {
		desiredSlices = 1
	}
	return actualSlices, desiredSlices, endpoints
}

// UpdateServicePortCache 将指定 Service 的 ServicePortCache 写入全局缓存，并更新相应指标。
// 参数：serviceNN 是代表该 Service 的 NamespacedName；spCache 是该 Service 对应的端口缓存。
func (c *Cache) UpdateServicePortCache(serviceNN types.NamespacedName, spCache *ServicePortCache) {
	// 先加锁，保证缓存与聚合指标的更新对其他 goroutine 原子可见。
	c.lock.Lock()
	defer c.lock.Unlock()

	var prevActualSlices, prevDesiredSlices, prevEndpoints int
	// 若该 Service 已有缓存，先统计旧缓存的切片数与端点数，供后续做差值更新。
	if existingSPCache, ok := c.cache[serviceNN]; ok {
		prevActualSlices, prevDesiredSlices, prevEndpoints = existingSPCache.totals(int(c.maxEndpointsPerSlice))
	}

	// 统计新缓存的当前值。
	currActualSlices, currDesiredSlices, currEndpoints := spCache.totals(int(c.maxEndpointsPerSlice))
	// 用新值减去被替换的旧值，保持端点总数与切片总数处于最新状态。
	c.numEndpoints = c.numEndpoints + currEndpoints - prevEndpoints

	// 同理，以差值方式更新期望与实际切片数，避免全量重算。
	c.numSlicesDesired += currDesiredSlices - prevDesiredSlices
	c.numSlicesActual += currActualSlices - prevActualSlices

	// 用新缓存覆盖旧缓存，并将最新聚合值同步到 Prometheus 指标。
	c.cache[serviceNN] = spCache
	c.updateMetrics()
}

// UpdateTrafficDistributionForService 更新指定 Service 在 servicesByTrafficDistribution 中的分类。
// trafficDistributionPtr 为 nil 表示该 Service 不再使用任何 trafficDistribution。
func (c *Cache) UpdateTrafficDistributionForService(serviceNN types.NamespacedName, trafficDistributionPtr *string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 无论走哪种分支，都确保指标在返回前被刷新。
	defer c.updateMetrics()

	// 先将该 Service 从所有旧的流量分布集合中移除。
	for _, serviceSet := range c.servicesByTrafficDistribution {
		delete(serviceSet, serviceNN)
	}

	// 未提供 trafficDistribution 时，删除旧引用后直接返回。
	if trafficDistributionPtr == nil {
		return
	}

	// 写入新的分类；若该取值尚无对应集合，则先创建集合。
	trafficDistribution := *trafficDistributionPtr
	serviceSet, ok := c.servicesByTrafficDistribution[trafficDistribution]
	if !ok {
		serviceSet = make(map[types.NamespacedName]bool)
		c.servicesByTrafficDistribution[trafficDistribution] = serviceSet
	}
	serviceSet[serviceNN] = true
}

// DeleteService 从全局缓存中移除某个 Service 的所有引用，并更新相应指标。
func (c *Cache) DeleteService(serviceNN types.NamespacedName) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 先从所有流量分布集合中删除该 Service 的引用。
	for _, serviceSet := range c.servicesByTrafficDistribution {
		delete(serviceSet, serviceNN)
	}

	// 若存在端口缓存，则减去该 Service 贡献的统计值后再删除缓存条目。
	if spCache, ok := c.cache[serviceNN]; ok {
		actualSlices, desiredSlices, endpoints := spCache.totals(int(c.maxEndpointsPerSlice))
		c.numEndpoints = c.numEndpoints - endpoints
		c.numSlicesDesired -= desiredSlices
		c.numSlicesActual -= actualSlices
		c.updateMetrics()
		delete(c.cache, serviceNN)
	}
}

// updateMetrics 用当前 Cache 中的聚合值更新指标。
// 调用方必须持有 lock。
func (c *Cache) updateMetrics() {
	// 将实际切片数、期望切片数与端点总数写入对应的 Gauge。
	NumEndpointSlices.WithLabelValues().Set(float64(c.numSlicesActual))
	DesiredEndpointSlices.WithLabelValues().Set(float64(c.numSlicesDesired))
	EndpointsDesired.WithLabelValues().Set(float64(c.numEndpoints))

	// 流量分布指标需要先清空旧标签序列，再按当前分类逐项重设，避免残留已消失的标签值。
	ServicesCountByTrafficDistribution.Reset()
	for trafficDistribution, services := range c.servicesByTrafficDistribution {
		ServicesCountByTrafficDistribution.WithLabelValues(trafficDistribution).Set(float64(len(services)))
	}
}

// numDesiredSlices 计算在理想端点分配情况下会存在的 EndpointSlice 数量。
func numDesiredSlices(numEndpoints, maxEndpointsPerSlice int) int {
	// 没有端点时不需要任何切片。
	if numEndpoints == 0 {
		return 0
	}
	// 端点数未超过单切片上限时，一个切片即可容纳。
	if numEndpoints <= maxEndpointsPerSlice {
		return 1
	}
	// 其余情况按上限向上取整，得到所需的最少切片数。
	return int(math.Ceil(float64(numEndpoints) / float64(maxEndpointsPerSlice)))
}
