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
// ServiceChangeTracker 累积尚未应用的 Service 变更，
// ServicePortMap 维护当前 ServicePort 到 ServicePort 信息的映射。
package proxy

import (
	"reflect"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/proxy/metrics"
	proxyutil "k8s.io/kubernetes/pkg/proxy/util"
)

// ServiceChangeTracker 保存任意数量 Service 的未提交变更状态，按命名空间和名字索引。
type ServiceChangeTracker struct {
	// lock 保护 items。
	lock sync.Mutex
	// items 将 Service 映射到其对应的 serviceChange。
	items map[types.NamespacedName]*serviceChange

	// makeServiceInfo 允许 proxier 在处理 Service 时注入自定义信息。
	makeServiceInfo makeServicePortFunc
	// processServiceMapChange 在应用变更时由 Update 对每处变更调用。
	// 该回调不应修改 ServicePortMap，只能利用变更信息做 proxier 特有的清理工作。
	processServiceMapChange processServiceMapChangeFunc

	ipFamily v1.IPFamily
}

// makeServicePortFunc 让 proxier 可以基于基础 ServicePort 信息构造自定义实现。
type makeServicePortFunc func(*v1.ServicePort, *v1.Service, *BaseServicePortInfo) ServicePort

// processServiceMapChangeFunc 在应用 Service 变更时通知 proxier 做特定清理。
type processServiceMapChangeFunc func(previous, current ServicePortMap)

// serviceChange 保存自上次规则同步以来某个 Service 的全部变更。
// 变更是累积的：previous 是应用变更前的状态，current 是所有变更应用后的状态。
type serviceChange struct {
	previous ServicePortMap
	current  ServicePortMap
}

// NewServiceChangeTracker 创建一个 ServiceChangeTracker，记录 IP 地址族并保存注入的构造函数与清理回调。
func NewServiceChangeTracker(ipFamily v1.IPFamily, makeServiceInfo makeServicePortFunc, processServiceMapChange processServiceMapChangeFunc) *ServiceChangeTracker {
	return &ServiceChangeTracker{
		items:                   make(map[types.NamespacedName]*serviceChange),
		makeServiceInfo:         makeServiceInfo,
		ipFamily:                ipFamily,
		processServiceMapChange: processServiceMapChange,
	}
}

// Update 根据 <previous, current> Service 对更新 tracker（两者之一可为 nil，但不能同时为 nil）。
// 返回值表示 tracker 中是否存在需要同步的变更，这些变更未必由本次调用引入；
// 注意这与 EndpointChangeTracker.EndpointSliceUpdate() 的返回值语义不同。
func (sct *ServiceChangeTracker) Update(previous, current *v1.Service) bool {
	// 新旧对象都不存在不符合预期，直接忽略，避免后续解引用 svc 时崩溃。
	if previous == nil && current == nil {
		return false
	}

	// 删除事件只有 previous，新增事件只有 current，任选非空的一个用于打日志和定位 key。
	svc := current
	if svc == nil {
		svc = previous
	}
	metrics.ServiceChangesTotal.Inc()
	namespacedName := types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}

	sct.lock.Lock()
	defer sct.lock.Unlock()

	change, exists := sct.items[namespacedName]
	if !exists {
		change = &serviceChange{}
		change.previous = sct.serviceToServiceMap(previous)
		sct.items[namespacedName] = change
	}
	change.current = sct.serviceToServiceMap(current)
	// 转换后的端口映射相同说明没有实质变化，撤销本次记录以免无谓触发同步。
	if reflect.DeepEqual(change.previous, change.current) {
		delete(sct.items, namespacedName)
	} else {
		klog.V(4).InfoS("Service updated ports", "service", klog.KObj(svc), "portCount", len(change.current))
	}
	metrics.ServiceChangesPending.Set(float64(len(sct.items)))
	return len(sct.items) > 0
}

// ServicePortMap 将 ServicePortName 映射到对应的 ServicePort。
type ServicePortMap map[ServicePortName]ServicePort

// UpdateServiceMapResult 是应用 Service 变更后的结果汇总。
type UpdateServiceMapResult struct {
	// UpdatedServices 列出自上次 Update 以来新增、更新或删除的所有 Service。
	UpdatedServices sets.Set[types.NamespacedName]
}

// HealthCheckNodePorts 返回 Service 名字到 HealthCheckNodePort 的映射，
// 只包含 sm 中该端口非零的 Service。
func (sm ServicePortMap) HealthCheckNodePorts() map[types.NamespacedName]uint16 {
	// TODO: 若确认计算开销过大，参照 svcPortMap 改为增量计算。
	// 目前每次遍历全量映射，仅在确认开销可接受前保持简单实现。
	ports := make(map[types.NamespacedName]uint16)
	for svcPortName, info := range sm {
		if info.HealthCheckNodePort() != 0 {
			ports[svcPortName.NamespacedName] = uint16(info.HealthCheckNodePort())
		}
	}
	return ports
}

// serviceToServiceMap 将单个 Service 对象转换为 ServicePortMap。
// 会跳过无需代理的 Service 以及当前地址族没有集群 IP 的 Service。
//
// 注意：不得修改 Service 对象。
func (sct *ServiceChangeTracker) serviceToServiceMap(service *v1.Service) ServicePortMap {
	if service == nil {
		return nil
	}

	// 外部流量绕过 kube-proxy 的 Service 没有可代理的端口。
	if proxyutil.ShouldSkipService(service) {
		return nil
	}

	// 双栈 Service 可能没有当前 tracker 地址族的集群 IP，此时本 tracker 不应处理它。
	clusterIP := proxyutil.GetClusterIPByFamily(sct.ipFamily, service)
	if clusterIP == "" {
		return nil
	}

	svcPortMap := make(ServicePortMap)
	svcName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	for i := range service.Spec.Ports {
		servicePort := &service.Spec.Ports[i]
		svcPortName := ServicePortName{NamespacedName: svcName, Port: servicePort.Name, Protocol: servicePort.Protocol}
		baseSvcInfo := newBaseServiceInfo(service, sct.ipFamily, servicePort)
		// proxier 可选择注入更丰富的 ServicePort 实现；未注入时使用基础实现。
		if sct.makeServiceInfo != nil {
			svcPortMap[svcPortName] = sct.makeServiceInfo(servicePort, service, baseSvcInfo)
		} else {
			svcPortMap[svcPortName] = baseSvcInfo
		}
	}
	return svcPortMap
}

// Update 按 sct 中的待处理变更更新 ServicePortMap，返回自上次 Update 以来的差异信息，
// 对每处变更触发 processServiceMapChange 回调，并在应用后清空变更映射。
func (sm ServicePortMap) Update(sct *ServiceChangeTracker) UpdateServiceMapResult {
	sct.lock.Lock()
	defer sct.lock.Unlock()

	result := UpdateServiceMapResult{
		UpdatedServices: sets.New[types.NamespacedName](),
	}

	for nn, change := range sct.items {
		if sct.processServiceMapChange != nil {
			sct.processServiceMapChange(change.previous, change.current)
		}
		result.UpdatedServices.Insert(nn)

		sm.merge(change.current)
		// 调用 unmerge() 前先从旧状态中剔除当前也已存在的端口，
		// 避免把“更新”事件当成删除处理。
		change.previous.filter(change.current)
		sm.unmerge(change.previous)
	}
	// 变更已应用到映射，清空以便下一轮同步只处理新的变更。
	sct.items = make(map[types.NamespacedName]*serviceChange)
	metrics.ServiceChangesPending.Set(0)

	return result
}

// merge 将 other 中的元素加入当前 ServicePortMap；键冲突时总是以 other 为准。
// 换句话说，当前映射中与 other 冲突的元素会被覆盖更新。
func (sm *ServicePortMap) merge(other ServicePortMap) {
	for svcPortName, info := range other {
		existingInfo, exists := (*sm)[svcPortName]
		if !exists {
			klog.V(4).InfoS("Adding new service port", "portName", svcPortName, "servicePort", info)
		} else {
			klog.V(4).InfoS("Updating existing service port", "portName", svcPortName, "servicePort", info)
			if existingInfo.NodePort() != 0 && info.NodePort() == 0 {
				klog.V(4).InfoS("Clearing nodePort for service port name", "portName", svcPortName, "nodePort", existingInfo.NodePort())
			}
		}
		if info.NodePort() != 0 {
			klog.V(4).InfoS("Setting nodePort for service port name", "portName", svcPortName, "nodePort", info.NodePort())
		}
		(*sm)[svcPortName] = info
	}
}

// filter 按传入映射的键从当前 ServicePortMap 中删除对应元素，用于识别真正的删除事件。
func (sm *ServicePortMap) filter(other ServicePortMap) {
	for svcPortName := range *sm {
		// 当前仍存在的端口属于更新事件，不应被当作删除。
		if _, ok := other[svcPortName]; ok {
			delete(*sm, svcPortName)
		}
	}
}

// unmerge 从当前 ServicePortMap 中删除 other 包含的所有元素。
func (sm *ServicePortMap) unmerge(other ServicePortMap) {
	for svcPortName, info := range other {
		_, exists := (*sm)[svcPortName]
		if exists {
			klog.V(4).InfoS("Removing service port", "portName", svcPortName)
			if info.NodePort() != 0 {
				klog.V(4).InfoS("Clearing nodePort for service port name", "portName", svcPortName, "nodePort", info.NodePort())
			}
			delete(*sm, svcPortName)
		} else {
			klog.ErrorS(nil, "Service port does not exists", "portName", svcPortName)
		}
	}
}
