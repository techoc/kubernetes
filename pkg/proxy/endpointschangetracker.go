/*
Copyright 2017 The Kubernetes Authors.

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

// Package proxy 提供代理规则同步所需的公共数据结构：
// EndpointsChangeTracker 缓存尚未应用的 EndpointSlice 变更，
// EndpointsMap 维护当前 ServicePort 到 Endpoint 的映射。
package proxy

import (
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy/metrics"
)

// EndpointsChangeTracker 保存任意数量 Endpoint 的未提交变更状态，按命名空间和名字索引。
type EndpointsChangeTracker struct {
	// lock 保护 lastChangeTriggerTimes
	lock sync.Mutex

	// processEndpointsMapChange 在应用变更时由 Update 对每处变更调用。
	// 该回调不应修改 EndpointsMap，只能利用变更信息做 proxier 特有的清理工作。
	processEndpointsMapChange processEndpointsMapChangeFunc

	// addressType 是本代理跟踪的 EndpointSlice 地址族类型
	addressType discovery.AddressType

	// endpointSliceCache 保存 EndpointSlice 的简化表示。
	endpointSliceCache *EndpointSliceCache

	// lastChangeTriggerTimes 记录 Service 的 NamespacedName 到触发其 EndpointSlice
	// 变更的各触发时刻的映射，用于计算 network-programming-latency 指标。
	lastChangeTriggerTimes map[types.NamespacedName][]time.Time
	// trackerStartTime 是 EndpointsChangeTracker 的创建时刻；
	// 借助它可以忽略 tracker 创建之前发生的变更，避免产生无意义的延迟指标。
	trackerStartTime time.Time
}

// makeEndpointFunc 让 proxier 可以基于基础 endpoint 信息构造自定义的 Endpoint 实现。
type makeEndpointFunc func(info *BaseEndpointInfo, svcPortName *ServicePortName) Endpoint

// processEndpointsMapChangeFunc 在应用 EndpointSlice 变更时通知 proxier 做特定清理。
type processEndpointsMapChangeFunc func(oldEndpointsMap, newEndpointsMap EndpointsMap)

// NewEndpointsChangeTracker 创建一个 EndpointsChangeTracker。
// 根据 IP 地址族选择要跟踪的 EndpointSlice 类型，并记录创建时刻用于过滤过期的触发时间。
func NewEndpointsChangeTracker(ipFamily v1.IPFamily, nodeName string, makeEndpointInfo makeEndpointFunc, processEndpointsMapChange processEndpointsMapChangeFunc) *EndpointsChangeTracker {
	addressType := discovery.AddressTypeIPv4
	// 一个 tracker 只处理单一地址族，IPv6 与 IPv4 的 EndpointSlice 不能混用。
	if ipFamily == v1.IPv6Protocol {
		addressType = discovery.AddressTypeIPv6
	}

	return &EndpointsChangeTracker{
		addressType:               addressType,
		lastChangeTriggerTimes:    make(map[types.NamespacedName][]time.Time),
		trackerStartTime:          time.Now(),
		processEndpointsMapChange: processEndpointsMapChange,
		endpointSliceCache:        NewEndpointSliceCache(nodeName, makeEndpointInfo),
	}
}

// EndpointSliceUpdate 根据 removeSlice 添加/更新或删除 EndpointSlice，更新待处理变更。
// 返回值表示本次更新是否引入了需要同步的变更；
// 注意它与 ServiceChangeTracker.Update() 的返回值语义不同：后者表示 tracker 中是否还有待同步的变更。
func (ect *EndpointsChangeTracker) EndpointSliceUpdate(endpointSlice *discovery.EndpointSlice, removeSlice bool) bool {
	// 每个 tracker 只处理一种地址族，其余的由对应地址族的 tracker 负责。
	if endpointSlice.AddressType != ect.addressType {
		klog.V(4).InfoS("Ignoring unsupported EndpointSlice", "endpointSlice", klog.KObj(endpointSlice), "type", endpointSlice.AddressType, "expected", ect.addressType)
		return false
	}

	namespacedName, _, err := endpointSliceCacheKeys(endpointSlice)
	if err != nil {
		klog.InfoS("Error getting endpoint slice cache keys", "err", err)
		return false
	}

	metrics.EndpointChangesTotal.Inc()

	ect.lock.Lock()
	defer ect.lock.Unlock()

	changeNeeded := ect.endpointSliceCache.updatePending(endpointSlice, removeSlice)

	if changeNeeded {
		// 只有在确实产生变更时才增加待处理计数，避免重复事件抬高指标。
		metrics.EndpointChangesPending.Inc()
		// 删除场景下注解里的 LastChangeTriggerTime 必然来自上一次更新时间，
		// 并非我们想度量的触发时刻，因此直接丢弃。
		// TODO(wojtek-t, robscott): Address the problem for EndpointSlice deletion
		// when other EndpointSlice for that service still exist.
		// 删除时旧注解时间戳没有意义，直接丢弃；新增/更新时只接受 tracker 启动之后的时间戳，
		// 避免重启前遗留的注解被算作本次的编程延迟。
		if removeSlice {
			delete(ect.lastChangeTriggerTimes, namespacedName)
		} else if t := getLastChangeTriggerTime(endpointSlice.Annotations); !t.IsZero() && t.After(ect.trackerStartTime) {
			ect.lastChangeTriggerTimes[namespacedName] =
				append(ect.lastChangeTriggerTimes[namespacedName], t)
		}
	}

	return changeNeeded
}

// checkoutChanges 返回所有待处理的 endpointsChange 映射；调用后这些变更即视为已取出。
// 由于规则同步期间不允许并发写入，此处不需要加锁。
func (ect *EndpointsChangeTracker) checkoutChanges() map[types.NamespacedName]*endpointsChange {
	metrics.EndpointChangesPending.Set(0)

	return ect.endpointSliceCache.checkoutChanges()
}

// checkoutTriggerTimes 把本地缓存的触发时间合并到传入的映射中，并清空本地缓存。
// 相同 key 的时间切片会在尾部追加，而不是覆盖已有记录。
func (ect *EndpointsChangeTracker) checkoutTriggerTimes(lastChangeTriggerTimes *map[types.NamespacedName][]time.Time) {
	ect.lock.Lock()
	defer ect.lock.Unlock()

	for k, v := range ect.lastChangeTriggerTimes {
		prev, ok := (*lastChangeTriggerTimes)[k]
		if !ok {
			(*lastChangeTriggerTimes)[k] = v
		} else {
			(*lastChangeTriggerTimes)[k] = append(prev, v...)
		}
	}
	ect.lastChangeTriggerTimes = make(map[types.NamespacedName][]time.Time)
}

// getLastChangeTriggerTime 解析 EndpointsLastChangeTriggerTime 注解并返回对应的 time.Time；
// 注解缺失或格式错误时返回零值时间（上游代码会忽略零值）。
func getLastChangeTriggerTime(annotations map[string]string) time.Time {
	// TODO(#81360): ignore case when Endpoint is deleted.
	if _, ok := annotations[v1.EndpointsLastChangeTriggerTime]; !ok {
		// Endpoints 对象可能根本没有该注解；此时返回零值，由上游代码忽略。
		return time.Time{}
	}
	val, err := time.Parse(time.RFC3339Nano, annotations[v1.EndpointsLastChangeTriggerTime])
	if err != nil {
		klog.ErrorS(err, "Error while parsing EndpointsLastChangeTriggerTimeAnnotation",
			"value", annotations[v1.EndpointsLastChangeTriggerTime])
		// 解析失败时 val 为零值，上游代码会忽略它。
	}
	return val
}

// endpointsChange 保存自上次规则同步以来某个 Service 的全部 endpoint 变更。
// 变更是累积的：previous 是应用变更前的状态，current 是应用变更后的状态。
type endpointsChange struct {
	previous EndpointsMap
	current  EndpointsMap
}

// UpdateEndpointsMapResult 是应用 endpoints 变更后的结果汇总。
type UpdateEndpointsMapResult struct {
	// UpdatedServices 列出自上次 Update 以来 endpoint 有增删改变更的所有 Service。
	UpdatedServices sets.Set[types.NamespacedName]
	// 只要有任一 UDP ServicePort 的 endpoint 发生变化就置 true，用于尽量减少 conntrack 清理调用次数。
	ConntrackCleanupRequired bool
	// 所有发生变更的 endpoints 对象的触发时间列表，用于导出 network programming latency。
	// NOTE(oxddr): this can be simplified to []time.Time if memory consumption becomes an issue.
	LastChangeTriggerTimes map[types.NamespacedName][]time.Time
}

// EndpointsMap 将 ServicePortName 映射到该端口对应的全部 Endpoint。
type EndpointsMap map[ServicePortName][]Endpoint

// Update 根据 ect 中的待处理变更更新 em，返回自上次 Update 以来的差异信息，
// 对每处变更触发 processEndpointsMapChange 回调，并清空待处理变更。
// 通过先 unmerge 旧状态再 merge 新状态，保证映射内容与最新的 EndpointSlice 一致。
func (em EndpointsMap) Update(ect *EndpointsChangeTracker) UpdateEndpointsMapResult {
	result := UpdateEndpointsMapResult{
		UpdatedServices:        sets.New[types.NamespacedName](),
		LastChangeTriggerTimes: make(map[types.NamespacedName][]time.Time),
	}
	if ect == nil {
		return result
	}

	changes := ect.checkoutChanges()
	for nn, change := range changes {
		if ect.processEndpointsMapChange != nil {
			ect.processEndpointsMapChange(change.previous, change.current)
		}
		result.UpdatedServices.Insert(nn)

		em.unmerge(change.previous)
		em.merge(change.current)

		// 只要有一个 UDP ServicePort 变更就足够触发清理；置 true 后无需再检查后续变更。
		if result.ConntrackCleanupRequired {
			continue
		}
		// 变更前存在 UDP 端口时，也可能需要清理旧连接。
		for svcPort := range change.previous {
			if svcPort.NamespacedName == nn && svcPort.Protocol == v1.ProtocolUDP {
				result.ConntrackCleanupRequired = true
				break
			}
		}
		// 变更后新增 UDP 端口同样需要清理标记。
		for svcPort := range change.current {
			if svcPort.NamespacedName == nn && svcPort.Protocol == v1.ProtocolUDP {
				result.ConntrackCleanupRequired = true
				break
			}
		}
	}
	ect.checkoutTriggerTimes(&result.LastChangeTriggerTimes)

	return result
}

// merge 确保当前 EndpointsMap 包含传入映射中的所有 <service, endpoints> 对；
// 同名 ServicePort 直接被新列表覆盖。
func (em EndpointsMap) merge(other EndpointsMap) {
	for svcPortName := range other {
		epList := other[svcPortName]
		if _, exists := em[svcPortName]; !exists {
			klog.V(3).InfoS("Adding new service endpoints", "portName", svcPortName, "endpoints", formatEndpointsList(epList))
		} else {
			klog.V(3).InfoS("Updating existing service endpoints", "portName", svcPortName, "endpoints", formatEndpointsList(epList))
		}
		em[svcPortName] = epList
	}
}

// unmerge 从当前 EndpointsMap 中删除传入映射包含的所有 <service, endpoints> 对。
func (em EndpointsMap) unmerge(other EndpointsMap) {
	for svcPortName := range other {
		klog.V(3).InfoS("Removing service endpoints", "portName", svcPortName)
		delete(em, svcPortName)
	}
}

// formatEndpointsList 将 Endpoint 列表转换为字符串列表，便于日志输出。
func formatEndpointsList(endpoints []Endpoint) []string {
	var formattedList []string
	for _, ep := range endpoints {
		formattedList = append(formattedList, ep.String())
	}
	return formattedList
}

// getLocalReadyEndpointIPs 返回每个 Service 的本地就绪 endpoint IP 集合；
// “本地”指 endpoint 与 kube-proxy 运行在同一节点上。
func (em EndpointsMap) getLocalReadyEndpointIPs() map[types.NamespacedName]sets.Set[string] {
	localIPs := make(map[types.NamespacedName]sets.Set[string])
	for svcPortName, epList := range em {
		for _, ep := range epList {
			// 只有就绪 endpoint 才计入健康检查信号：终止中的 endpoint 虽可能仍在承载流量，
			// 但节点上若只剩这类 endpoint，健康检查就应当失败。
			if !ep.IsReady() {
				continue
			}

			if ep.IsLocal() {
				nsn := svcPortName.NamespacedName
				if localIPs[nsn] == nil {
					localIPs[nsn] = sets.New[string]()
				}
				localIPs[nsn].Insert(ep.IP())
			}
		}
	}
	return localIPs
}

// LocalReadyEndpoints 返回每个 Service 名字到其本地就绪 endpoint 数量的映射。
func (em EndpointsMap) LocalReadyEndpoints() map[types.NamespacedName]int {
	// TODO: If this will appear to be computationally expensive, consider
	// computing this incrementally similarly to endpointsMap.

	// 必须先通过 getLocalEndpointIPs 按 IP 去重：EndpointsMap 按 IP+端口组织，
	// 而指向 10.0.0.1:80 和 10.0.0.1:443 的 Service 应算作 1 个而非 2 个 endpoint。

	eps := make(map[types.NamespacedName]int)
	localIPs := em.getLocalReadyEndpointIPs()
	for nsn, ips := range localIPs {
		eps[nsn] = len(ips)
	}
	return eps
}
