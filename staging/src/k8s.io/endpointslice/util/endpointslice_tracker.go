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

package util

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
)

const (
	// deletionExpected 作为 generation 的哨兵值，表示期望该 EndpointSlice 被删除。
	deletionExpected = -1
)

// GenerationsBySlice 按 EndpointSlice UID 记录期望的 generation，
// 值为 -1（deletionExpected）时表示期望该分片被删除。
type GenerationsBySlice map[types.UID]int64

// EndpointSliceTracker 跟踪各 Service 的 EndpointSlice generation，
// 用于判断控制器是否已经处理了某个分片变更。
type EndpointSliceTracker struct {
	// 互斥锁保护 generationsByService 的并发访问。
	lock sync.Mutex
	// 以 Service 的命名空间名为键，保存其下所有 EndpointSlice 的期望 generation。
	generationsByService map[types.NamespacedName]GenerationsBySlice
}

// NewEndpointSliceTracker 创建并初始化一个空的 EndpointSliceTracker。
func NewEndpointSliceTracker() *EndpointSliceTracker {
	return &EndpointSliceTracker{
		// 初始化按 Service 索引的 generation 映射。
		generationsByService: map[types.NamespacedName]GenerationsBySlice{},
	}
}

// Has 判断跟踪器中是否记录了指定 EndpointSlice 的 generation。
func (est *EndpointSliceTracker) Has(endpointSlice *discovery.EndpointSlice) bool {
	est.lock.Lock()
	// 全程加锁保护共享映射。
	defer est.lock.Unlock()

	gfs, ok := est.GenerationsForSliceUnsafe(endpointSlice)
	// 先定位所属 Service 的 generation 表。
	if !ok {
		return false
	}
	_, ok = gfs[endpointSlice.UID]
	// 再判断该 UID 是否存在。
	return ok
}

// ShouldSync 在跟踪器没有记录或记录的 generation 落后于当前对象时返回 true。
func (est *EndpointSliceTracker) ShouldSync(endpointSlice *discovery.EndpointSlice) bool {
	est.lock.Lock()
	// 加锁读取。
	defer est.lock.Unlock()

	gfs, ok := est.GenerationsForSliceUnsafe(endpointSlice)
	// 找不到所属 Service 时必须同步。
	if !ok {
		return true
	}
	g, ok := gfs[endpointSlice.UID]
	// 未记录该 UID 或当前 generation 更大时需要同步。
	return !ok || endpointSlice.Generation > g
}

// StaleSlices 检查传入的 EndpointSlice 是否与跟踪状态一致；
// 发现旧 generation、待删除或缺失分片时返回 true。
// 已带删除时间戳、即 deletionExpected 的切片不计入期待删除。
func (est *EndpointSliceTracker) StaleSlices(service *v1.Service, endpointSlices []*discovery.EndpointSlice) bool {
	est.lock.Lock()
	// 加锁读取。
	defer est.lock.Unlock()

	nn := types.NamespacedName{Name: service.Name, Namespace: service.Namespace}
	// 构造 Service 的命名空间名并取出对应记录。
	gfs, ok := est.generationsByService[nn]
	if !ok {
		// 没有该 Service 的记录说明没有待处理的变更。
		return false
	}
	providedSlices := map[types.UID]int64{}
	// 先记录本次提供的分片及其 generation。
	for _, endpointSlice := range endpointSlices {
		// 逐个检查：期望删除或跟踪的 generation 更新都说明缓存已过时。
		providedSlices[endpointSlice.UID] = endpointSlice.Generation
		g, ok := gfs[endpointSlice.UID]
		if ok && (g == deletionExpected || g > endpointSlice.Generation) {
			return true
		}
	}
	for uid, generation := range gfs {
		// 再检查跟踪器中是否存在本次未提供的分片（待删除的除外）。
		if generation == deletionExpected {
			continue
		}
		_, ok := providedSlices[uid]
		if !ok {
			return true
		}
	}
	return false
}

// Update 记录指定 EndpointSlice 的当前 generation。
func (est *EndpointSliceTracker) Update(endpointSlice *discovery.EndpointSlice) {
	est.lock.Lock()
	// 加锁写入。
	defer est.lock.Unlock()

	gfs, ok := est.GenerationsForSliceUnsafe(endpointSlice)
	// 所属 Service 尚无记录时先创建映射。

	if !ok {
		gfs = GenerationsBySlice{}
		est.generationsByService[getServiceNN(endpointSlice)] = gfs
	}
	// 更新该 UID 对应的 generation。
	gfs[endpointSlice.UID] = endpointSlice.Generation
}

// DeleteService 删除某个 Service 下跟踪的全部 generation。
func (est *EndpointSliceTracker) DeleteService(namespace, name string) {
	est.lock.Lock()
	// 加锁写入。
	defer est.lock.Unlock()

	serviceNN := types.NamespacedName{Name: name, Namespace: namespace}
	// 按命名空间名删除整个条目。
	delete(est.generationsByService, serviceNN)
}

// ExpectDeletion 将指定 EndpointSlice 的期望 generation 标记为 deletionExpected。
func (est *EndpointSliceTracker) ExpectDeletion(endpointSlice *discovery.EndpointSlice) {
	est.lock.Lock()
	// 加锁写入。
	defer est.lock.Unlock()

	gfs, ok := est.GenerationsForSliceUnsafe(endpointSlice)
	// 所属 Service 尚无记录时先创建映射。

	if !ok {
		gfs = GenerationsBySlice{}
		est.generationsByService[getServiceNN(endpointSlice)] = gfs
	}
	// 写入删除哨兵值。
	gfs[endpointSlice.UID] = deletionExpected
}

// HandleDeletion 清理已删除分片的跟踪记录；若之前确实期望其删除则返回 true。
func (est *EndpointSliceTracker) HandleDeletion(endpointSlice *discovery.EndpointSlice) bool {
	est.lock.Lock()
	// 加锁写入。
	defer est.lock.Unlock()

	gfs, ok := est.GenerationsForSliceUnsafe(endpointSlice)
	// 找到所属 Service 后删除该 UID，并核对之前的期望值。

	if ok {
		// 取出旧值后立即删除记录。
		g, ok := gfs[endpointSlice.UID]
		delete(gfs, endpointSlice.UID)
		if ok && g != deletionExpected {
			// 若旧值不是删除哨兵，说明这次删除未被告知，返回 false。
			return false
		}
	}

	return true
}

// GenerationsForSliceUnsafe 返回指定 EndpointSlice 所属 Service 的 generation 表；
// 调用方必须已持有锁。
func (est *EndpointSliceTracker) GenerationsForSliceUnsafe(endpointSlice *discovery.EndpointSlice) (GenerationsBySlice, bool) {
	serviceNN := getServiceNN(endpointSlice)
	// 由 EndpointSlice 推导 Service 的命名空间名并查表。
	generations, ok := est.generationsByService[serviceNN]
	return generations, ok
}

// getServiceNN 根据 EndpointSlice 的标签与命名空间还原其所属 Service 的 NamespacedName。
func getServiceNN(endpointSlice *discovery.EndpointSlice) types.NamespacedName {
	serviceName, _ := endpointSlice.Labels[discovery.LabelServiceName]
	// service-name 标签缺失时会得到空名称，调用方需保证标签存在。
	return types.NamespacedName{Name: serviceName, Namespace: endpointSlice.Namespace}
}
