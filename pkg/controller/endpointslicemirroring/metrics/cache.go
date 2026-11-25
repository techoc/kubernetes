/*
Copyright 2020 The Kubernetes Authors.

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

// NewCache 创建一个使用指定每切片端点上限的空 Cache。
func NewCache(endpointsPerSlice int32) *Cache {
	return &Cache{
		maxEndpointsPerSlice: endpointsPerSlice,
		cache:                map[types.NamespacedName]*EndpointPortCache{},
	}
}

// Cache 统计期望端点总数以及 EndpointSlice 端点分布的紧凑程度，
// 供镜像控制器上报全局聚合指标。
type Cache struct {
	// maxEndpointsPerSlice 是单个 EndpointSlice 允许容纳的最大端点数。
	maxEndpointsPerSlice int32

	// lock 保护 numEndpoints 与 cache 的并发读写。
	lock sync.Mutex
	// numEndpoints 记录当前已存入 EndpointSlice 的端点总数。
	numEndpoints int
	// cache 按 Service 的 NamespacedName 保存各 Service 的端口维度统计。
	cache map[types.NamespacedName]*EndpointPortCache
}

// EndpointPortCache 按每个唯一的 Service 端口组合，
// 保存期望端点总数与 EndpointSlice 分布效率所需的数据。
type EndpointPortCache struct {
	items map[endpointsliceutil.PortMapKey]EfficiencyInfo
}

// EfficiencyInfo 保存端点数与切片数，用于计算期望切片数和分布效率。
type EfficiencyInfo struct {
	// Endpoints 是该端口组合对应的期望端点总数。
	Endpoints int
	// Slices 是这些端点实际分布到的 EndpointSlice 数量。
	Slices int
}

// NewEndpointPortCache 初始化并返回一个空的 EndpointPortCache。
func NewEndpointPortCache() *EndpointPortCache {
	return &EndpointPortCache{
		items: map[endpointsliceutil.PortMapKey]EfficiencyInfo{},
	}
}

// Set 写入指定端口组合对应的统计信息。
func (spc *EndpointPortCache) Set(pmKey endpointsliceutil.PortMapKey, eInfo EfficiencyInfo) {
	spc.items[pmKey] = eInfo
}

// numEndpoints 返回该端口维度缓存中的端点总数。
func (spc *EndpointPortCache) numEndpoints() int {
	num := 0
	for _, eInfo := range spc.items {
		num += eInfo.Endpoints
	}
	return num
}

// UpdateEndpointPortCache 更新全局缓存中某个 Service 的端口统计并刷新指标。
// 参数：
// * endpointsNN 是对应 Endpoints 资源的 NamespacedName；
// * epCache 是该 Endpoints 资源最新的端口维度统计。
func (c *Cache) UpdateEndpointPortCache(endpointsNN types.NamespacedName, epCache *EndpointPortCache) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 先取出旧统计，便于用差值增量更新总数而不必重新遍历整个缓存。
	prevNumEndpoints := 0
	if existingEPCache, ok := c.cache[endpointsNN]; ok {
		prevNumEndpoints = existingEPCache.numEndpoints()
	}

	currNumEndpoints := epCache.numEndpoints()
	// 总数只做增量调整：加上新值、减去被替换的旧值，
	// 避免每次更新都重算所有 Service 的端点数。
	c.numEndpoints = c.numEndpoints + currNumEndpoints - prevNumEndpoints

	c.cache[endpointsNN] = epCache
	c.updateMetrics()
}

// DeleteEndpoints 从全局缓存中移除某个 Endpoints 资源的记录并刷新指标。
func (c *Cache) DeleteEndpoints(endpointsNN types.NamespacedName) {
	c.lock.Lock()
	defer c.lock.Unlock()

	// 仅在记录确实存在时才扣减总数，防止重复删除使计数变负。
	if spCache, ok := c.cache[endpointsNN]; ok {
		c.numEndpoints = c.numEndpoints - spCache.numEndpoints()
		delete(c.cache, endpointsNN)
		c.updateMetrics()
	}
}

// metricsUpdate 保存期望与实际 EndpointSlice 数量。
type metricsUpdate struct {
	desired, actual int
}

// desiredAndActualSlices 基于当前缓存计算期望与实际切片数。
// 调用方必须持有 lock。
func (c *Cache) desiredAndActualSlices() metricsUpdate {
	mUpdate := metricsUpdate{}
	for _, spCache := range c.cache {
		for _, eInfo := range spCache.items {
			mUpdate.actual += eInfo.Slices
			mUpdate.desired += numDesiredSlices(eInfo.Endpoints, int(c.maxEndpointsPerSlice))
		}
	}
	return mUpdate
}

// updateMetrics 用缓存中的当前值刷新全局指标。
// 调用方必须持有 lock。
func (c *Cache) updateMetrics() {
	mUpdate := c.desiredAndActualSlices()
	NumEndpointSlices.WithLabelValues().Set(float64(mUpdate.actual))
	DesiredEndpointSlices.WithLabelValues().Set(float64(mUpdate.desired))
	EndpointsDesired.WithLabelValues().Set(float64(c.numEndpoints))
}

// numDesiredSlices 按理想分布（每片最多 maxPerSlice 个端点）
// 计算容纳这些端点所需的切片数。
func numDesiredSlices(numEndpoints, maxPerSlice int) int {
	return int(math.Ceil(float64(numEndpoints) / float64(maxPerSlice)))
}
