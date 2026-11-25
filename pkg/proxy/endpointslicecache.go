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

package proxy

import (
	"fmt"
	"reflect"
	"sort"
	"sync"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/features"
	utilnet "k8s.io/utils/net"
)

// EndpointSliceCache 缓存代理所需的 EndpointSlice 信息，区分已应用与待应用的变更。
type EndpointSliceCache struct {
	// lock 保护 trackerByServiceMap 的并发访问。
	lock sync.Mutex

	// trackerByServiceMap 是本缓存的核心，按 Service 名称（第一层键，含命名空间）
	// 和 EndpointSlice 名称（第二层键）分组保存切片跟踪器。端点可能在切片间迁移，
	// 因此必须按切片分别缓存，避免把刚迁移到其他切片的端点误判为已删除。
	trackerByServiceMap map[types.NamespacedName]*endpointSliceTracker

	makeEndpointInfo makeEndpointFunc
	nodeName         string
}

// endpointSliceTracker 分别记录已被代理应用的 EndpointSlice 与已更新但尚未应用的待处理切片。
type endpointSliceTracker struct {
	applied endpointSliceDataByName
	pending endpointSliceDataByName
}

// endpointSliceDataByName 按 EndpointSlice 名称索引其数据。
type endpointSliceDataByName map[string]*endpointSliceData

// endpointSliceData 表示单个 EndpointSlice 的更新或删除；remove 为 true 表示删除。
type endpointSliceData struct {
	endpointSlice *discovery.EndpointSlice
	remove        bool
}

// NewEndpointSliceCache 初始化 EndpointSliceCache；未指定构造函数时使用默认的端点信息构造方式。
func NewEndpointSliceCache(nodeName string, makeEndpointInfo makeEndpointFunc) *EndpointSliceCache {
	// 调用方未定制时回退到默认实现，保证缓存始终能构造 Endpoint 信息。
	if makeEndpointInfo == nil {
		makeEndpointInfo = standardEndpointInfo
	}
	return &EndpointSliceCache{
		trackerByServiceMap: map[types.NamespacedName]*endpointSliceTracker{},
		nodeName:            nodeName,
		makeEndpointInfo:    makeEndpointInfo,
	}
}

// newEndpointSliceTracker 初始化一个空的 endpointSliceTracker。
func newEndpointSliceTracker() *endpointSliceTracker {
	return &endpointSliceTracker{
		applied: endpointSliceDataByName{},
		pending: endpointSliceDataByName{},
	}
}

// standardEndpointInfo 是默认的 makeEndpointFunc，直接返回基础端点信息。
func standardEndpointInfo(ep *BaseEndpointInfo, _ *ServicePortName) Endpoint {
	return ep
}

// updatePending 把 EndpointSlice 的新增、更新或删除记录到待处理集合，返回是否产生了实际变化。
func (cache *EndpointSliceCache) updatePending(endpointSlice *discovery.EndpointSlice, remove bool) bool {
	serviceKey, sliceKey, err := endpointSliceCacheKeys(endpointSlice)
	if err != nil {
		klog.ErrorS(err, "Error getting endpoint slice cache keys")
		return false
	}

	esData := &endpointSliceData{endpointSlice, remove}

	cache.lock.Lock()
	defer cache.lock.Unlock()

	// 该 Service 首次出现时先建立对应的跟踪器。
	if _, ok := cache.trackerByServiceMap[serviceKey]; !ok {
		cache.trackerByServiceMap[serviceKey] = newEndpointSliceTracker()
	}

	// 数据没变时不入队，避免触发无意义的代理规则重算。
	changed := cache.esDataChanged(serviceKey, sliceKey, esData)

	if changed {
		cache.trackerByServiceMap[serviceKey].pending[sliceKey] = esData
	}

	return changed
}

// checkoutChanges 取出所有待处理的端点变更并把它们标记为已应用。
func (cache *EndpointSliceCache) checkoutChanges() map[types.NamespacedName]*endpointsChange {
	changes := make(map[types.NamespacedName]*endpointsChange)

	cache.lock.Lock()
	defer cache.lock.Unlock()

	for serviceNN, esTracker := range cache.trackerByServiceMap {
		// 没有待处理变更的 Service 无需生成 diff。
		if len(esTracker.pending) == 0 {
			continue
		}

		change := &endpointsChange{}

		// 先用已应用的状态计算变更前的端点快照。
		change.previous = cache.getEndpointsMap(serviceNN, esTracker.applied)

		for name, sliceData := range esTracker.pending {
			if sliceData.remove {
				delete(esTracker.applied, name)
			} else {
				esTracker.applied[name] = sliceData
			}

			delete(esTracker.pending, name)
			// 该 Service 已无任何切片数据时回收跟踪器，防止缓存无限增长。
			if len(esTracker.applied) == 0 && len(esTracker.pending) == 0 {
				delete(cache.trackerByServiceMap, serviceNN)
			}
		}

		change.current = cache.getEndpointsMap(serviceNN, esTracker.applied)
		changes[serviceNN] = change
	}

	return changes
}

// spToEndpointMap 先按 ServicePortName、再按端点的字符串表示对 Endpoint 分组。
type spToEndpointMap map[ServicePortName]map[string]Endpoint

// getEndpointsMap 根据给定的 EndpointSlice 集合计算对应的 EndpointsMap。
func (cache *EndpointSliceCache) getEndpointsMap(serviceNN types.NamespacedName, sliceDataByName endpointSliceDataByName) EndpointsMap {
	endpointInfoBySP := cache.endpointInfoByServicePort(serviceNN, sliceDataByName)
	return endpointsMapFromEndpointInfo(endpointInfoBySP)
}

// endpointInfoByServicePort 按 Service 端口名称和端点地址对端点信息分组。
func (cache *EndpointSliceCache) endpointInfoByServicePort(serviceNN types.NamespacedName, sliceDataByName endpointSliceDataByName) spToEndpointMap {
	endpointInfoBySP := spToEndpointMap{}

	for _, sliceData := range sliceDataByName {
		for _, port := range sliceData.endpointSlice.Ports {
			// 端口名是构造 ServicePortName 的必要条件，缺失时只能忽略该端口。
			if port.Name == nil {
				klog.ErrorS(nil, "Ignoring port with nil name", "portName", port.Name)
				continue
			}
			// TODO: 处理端口为 nil 的情况，使其表示所有端口。
			// 零值或空端口无法用于建立转发规则，跳过。
			if port.Port == nil || *port.Port == int32(0) {
				klog.ErrorS(nil, "Ignoring invalid endpoint port", "portName", *port.Name)
				continue
			}

			svcPortName := ServicePortName{
				NamespacedName: serviceNN,
				Port:           *port.Name,
				Protocol:       *port.Protocol,
			}

			endpointInfoBySP[svcPortName] = cache.addEndpoints(&svcPortName, int(*port.Port), endpointInfoBySP[svcPortName], sliceData.endpointSlice.Endpoints)
		}
	}

	return endpointInfoBySP
}

// addEndpoints 把切片中的端点按唯一地址加入端点集合，并应用本地性、就绪状态等语义。
func (cache *EndpointSliceCache) addEndpoints(svcPortName *ServicePortName, portNum int, endpointSet map[string]Endpoint, endpoints []discovery.Endpoint) map[string]Endpoint {
	if endpointSet == nil {
		endpointSet = map[string]Endpoint{}
	}

	for _, endpoint := range endpoints {
		// 没有地址的端点无法用于转发，直接忽略。
		if len(endpoint.Addresses) == 0 {
			klog.ErrorS(nil, "Ignoring invalid endpoint port with empty address", "endpoint", endpoint)
			continue
		}

		isLocal := endpoint.NodeName != nil && cache.isLocal(*endpoint.NodeName)

		ready := endpoint.Conditions.Ready == nil || *endpoint.Conditions.Ready
		serving := endpoint.Conditions.Serving == nil || *endpoint.Conditions.Serving
		terminating := endpoint.Conditions.Terminating != nil && *endpoint.Conditions.Terminating

		var zoneHints, nodeHints sets.Set[string]
		// 拓扑提示仅在存在时收集；节点提示还受特性门控限制。
		if endpoint.Hints != nil {
			if len(endpoint.Hints.ForZones) > 0 {
				zoneHints = sets.New[string]()
				for _, zone := range endpoint.Hints.ForZones {
					zoneHints.Insert(zone.Name)
				}
			}
			if len(endpoint.Hints.ForNodes) > 0 && utilfeature.DefaultFeatureGate.Enabled(features.PreferSameTrafficDistribution) {
				nodeHints = sets.New[string]()
				for _, node := range endpoint.Hints.ForNodes {
					nodeHints.Insert(node.Name)
				}
			}
		}

		endpointIP := utilnet.ParseIPSloppy(endpoint.Addresses[0]).String()
		endpointInfo := newBaseEndpointInfo(endpointIP, portNum, isLocal,
			ready, serving, terminating, zoneHints, nodeHints)

		// 端点在切片间迁移期间可能短暂同时出现在两个切片中；无法可靠判断哪个更新，
		// 但第二次切片更新到达后该状态会自行消解。另一方面，Pod 网络可能在旧 Pod
		// 完全删除前复用其 IP，导致两个不同端点指向同一地址；此时优先选择非终止中的端点。
		if _, exists := endpointSet[endpointInfo.String()]; !exists || !terminating {
			endpointSet[endpointInfo.String()] = cache.makeEndpointInfo(endpointInfo, svcPortName)
		}
	}

	return endpointSet
}

// isLocal 判断端点所在节点是否为当前节点。
func (cache *EndpointSliceCache) isLocal(nodeName string) bool {
	return len(cache.nodeName) > 0 && nodeName == cache.nodeName
}

// esDataChanged 判断 esData 是否需要作为新的待处理值写入缓存。
func (cache *EndpointSliceCache) esDataChanged(serviceKey types.NamespacedName, sliceKey string, esData *endpointSliceData) bool {
	if _, ok := cache.trackerByServiceMap[serviceKey]; ok {
		appliedData, appliedOk := cache.trackerByServiceMap[serviceKey].applied[sliceKey]
		pendingData, pendingOk := cache.trackerByServiceMap[serviceKey].pending[sliceKey]

		// 已有待处理值时，只需判断是否与其相同，避免覆盖尚未应用的更新。
		if pendingOk {
			return !reflect.DeepEqual(esData, pendingData)
		}

		// 没有待处理值时，再与已应用的值比较。
		if appliedOk {
			return !reflect.DeepEqual(esData, appliedData)
		}
	}

	// 缓存中不存在且标记为删除，说明无需任何处理。
	if esData.remove {
		return false
	}

	// 缓存中不存在且不是删除操作，说明这是新增数据。
	return true
}

// endpointsMapFromEndpointInfo 根据按 Service 端口和地址分组的端点信息生成 EndpointsMap。
func endpointsMapFromEndpointInfo(endpointInfoBySP map[ServicePortName]map[string]Endpoint) EndpointsMap {
	endpointsMap := EndpointsMap{}

	// 将分组结果转换为端点列表，并对 IP 排序。
	for svcPortName, endpointSet := range endpointInfoBySP {
		if len(endpointSet) > 0 {
			endpointsMap[svcPortName] = []Endpoint{}
			for _, endpointInfo := range endpointSet {
				endpointsMap[svcPortName] = append(endpointsMap[svcPortName], endpointInfo)

			}
			// 固定端点顺序，使前后快照的 diff 结果稳定。
			sort.Sort(byEndpoint(endpointsMap[svcPortName]))
		}
	}

	return endpointsMap
}

// endpointSliceCacheKeys 从 EndpointSlice 提取缓存所需的 Service 键和切片键。
func endpointSliceCacheKeys(endpointSlice *discovery.EndpointSlice) (types.NamespacedName, string, error) {
	var err error
	serviceName, ok := endpointSlice.Labels[discovery.LabelServiceName]
	// 缺少 Service 名称标签的切片无法归属到任何 Service，必须报错。
	if !ok || serviceName == "" {
		err = fmt.Errorf("no %s label set on endpoint slice: %s", discovery.LabelServiceName, endpointSlice.Name)
	} else if endpointSlice.Namespace == "" || endpointSlice.Name == "" {
		err = fmt.Errorf("expected EndpointSlice name and namespace to be set: %v", endpointSlice)
	}
	return types.NamespacedName{Namespace: endpointSlice.Namespace, Name: serviceName}, endpointSlice.Name, err
}

// byEndpoint 按端点的字符串表示排序，实现 sort.Interface。
type byEndpoint []Endpoint

// Len 返回待排序端点的数量。
func (e byEndpoint) Len() int {
	return len(e)
}

// Swap 交换两个端点的位置。
func (e byEndpoint) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

// Less 按端点字符串表示确定排序顺序。
func (e byEndpoint) Less(i, j int) bool {
	return e[i].String() < e[j].String()
}
