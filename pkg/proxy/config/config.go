/*
Copyright 2014 The Kubernetes Authors.

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

package config

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	v1informers "k8s.io/client-go/informers/core/v1"
	discoveryv1informers "k8s.io/client-go/informers/discovery/v1"
	networkingv1informers "k8s.io/client-go/informers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// ServiceHandler 是接收 Service 对象变更通知的对象的抽象接口。
type ServiceHandler interface {
	// OnServiceAdd 在观察到新建 Service 对象时调用。
	OnServiceAdd(service *v1.Service)
	// OnServiceUpdate 在观察到已有 Service 对象被修改时调用。
	OnServiceUpdate(oldService, service *v1.Service)
	// OnServiceDelete 在观察到已有 Service 对象被删除时调用。
	OnServiceDelete(service *v1.Service)
	// OnServiceSynced 在初始事件全部处理完毕、状态完全同步到本地缓存后调用一次。
	OnServiceSynced()
}

// EndpointSliceHandler 是接收 EndpointSlice 对象变更通知的对象的抽象接口。
type EndpointSliceHandler interface {
	// OnEndpointSliceAdd 在观察到新建 EndpointSlice 对象时调用。
	OnEndpointSliceAdd(endpointSlice *discoveryv1.EndpointSlice)
	// OnEndpointSliceUpdate 在观察到已有 EndpointSlice 对象被修改时调用。
	OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice *discoveryv1.EndpointSlice)
	// OnEndpointSliceDelete 在观察到已有 EndpointSlice 对象被删除时调用。
	OnEndpointSliceDelete(endpointSlice *discoveryv1.EndpointSlice)
	// OnEndpointSlicesSynced 在初始事件全部处理完毕、状态完全同步到本地缓存后调用一次。
	OnEndpointSlicesSynced()
}

// EndpointSliceConfig 跟踪一组 EndpointSlice 配置，并把变更分发给已注册的处理程序。
type EndpointSliceConfig struct {
	listerSynced  cache.InformerSynced
	eventHandlers []EndpointSliceHandler
	logger        klog.Logger
}

// NewEndpointSliceConfig 创建新的 EndpointSliceConfig，并向 informer 注册事件回调。
func NewEndpointSliceConfig(ctx context.Context, endpointSliceInformer discoveryv1informers.EndpointSliceInformer, resyncPeriod time.Duration) *EndpointSliceConfig {
	result := &EndpointSliceConfig{
		logger: klog.FromContext(ctx),
	}

	handlerRegistration, _ := endpointSliceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    result.handleAddEndpointSlice,
			UpdateFunc: result.handleUpdateEndpointSlice,
			DeleteFunc: result.handleDeleteEndpointSlice,
		},
		resyncPeriod,
	)

	result.listerSynced = handlerRegistration.HasSynced

	return result
}

// RegisterEventHandler 注册在每次 EndpointSlice 变更时调用的处理程序。
func (c *EndpointSliceConfig) RegisterEventHandler(handler EndpointSliceHandler) {
	c.eventHandlers = append(c.eventHandlers, handler)
}

// Run 等待缓存同步完成后通知所有处理程序。
func (c *EndpointSliceConfig) Run(stopCh <-chan struct{}) {
	c.logger.Info("Starting endpoint slice config controller")

	if !cache.WaitForNamedCacheSync("endpoint slice config", stopCh, c.listerSynced) {
		return
	}

	for _, h := range c.eventHandlers {
		c.logger.V(3).Info("Calling handler.OnEndpointSlicesSynced()")
		h.OnEndpointSlicesSynced()
	}
}

// handleAddEndpointSlice 处理 EndpointSlice 新增事件并通知所有处理程序。
func (c *EndpointSliceConfig) handleAddEndpointSlice(obj interface{}) {
	endpointSlice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
		return
	}
	for _, h := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnEndpointSliceAdd", "endpoints", klog.KObj(endpointSlice))
		h.OnEndpointSliceAdd(endpointSlice)
	}
}

// handleUpdateEndpointSlice 处理 EndpointSlice 更新事件，同时传递新旧对象。
func (c *EndpointSliceConfig) handleUpdateEndpointSlice(oldObj, newObj interface{}) {
	oldEndpointSlice, ok := oldObj.(*discoveryv1.EndpointSlice)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", oldObj))
		return
	}
	newEndpointSlice, ok := newObj.(*discoveryv1.EndpointSlice)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", newObj))
		return
	}
	for _, h := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnEndpointSliceUpdate")
		h.OnEndpointSliceUpdate(oldEndpointSlice, newEndpointSlice)
	}
}

// handleDeleteEndpointSlice 处理 EndpointSlice 删除事件并通知所有处理程序。
func (c *EndpointSliceConfig) handleDeleteEndpointSlice(obj interface{}) {
	endpointSlice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		// informer 漏掉删除事件时会用 tombstone 包裹最后已知状态，需从其中取回原对象。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
			return
		}
		if endpointSlice, ok = tombstone.Obj.(*discoveryv1.EndpointSlice); !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %T", obj))
			return
		}
	}
	for _, h := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnEndpointsDelete")
		h.OnEndpointSliceDelete(endpointSlice)
	}
}

// ServiceConfig 跟踪一组 Service 配置，并把变更分发给已注册的处理程序。
type ServiceConfig struct {
	listerSynced  cache.InformerSynced
	eventHandlers []ServiceHandler
	logger        klog.Logger
}

// NewServiceConfig 创建新的 ServiceConfig，并向 informer 注册事件回调。
func NewServiceConfig(ctx context.Context, serviceInformer v1informers.ServiceInformer, resyncPeriod time.Duration) *ServiceConfig {
	result := &ServiceConfig{
		logger: klog.FromContext(ctx),
	}

	handlerRegistration, _ := serviceInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    result.handleAddService,
			UpdateFunc: result.handleUpdateService,
			DeleteFunc: result.handleDeleteService,
		},
		resyncPeriod,
	)

	result.listerSynced = handlerRegistration.HasSynced

	return result
}

// RegisterEventHandler 注册在每次 Service 变更时调用的处理程序。
func (c *ServiceConfig) RegisterEventHandler(handler ServiceHandler) {
	c.eventHandlers = append(c.eventHandlers, handler)
}

// Run 等待缓存同步完成后通知所有处理程序。
func (c *ServiceConfig) Run(stopCh <-chan struct{}) {
	c.logger.Info("Starting service config controller")

	if !cache.WaitForNamedCacheSync("service config", stopCh, c.listerSynced) {
		return
	}

	for i := range c.eventHandlers {
		c.logger.V(3).Info("Calling handler.OnServiceSynced()")
		c.eventHandlers[i].OnServiceSynced()
	}
}

// handleAddService 处理 Service 新增事件并通知所有处理程序。
func (c *ServiceConfig) handleAddService(obj interface{}) {
	service, ok := obj.(*v1.Service)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
		return
	}
	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnServiceAdd")
		c.eventHandlers[i].OnServiceAdd(service)
	}
}

// handleUpdateService 处理 Service 更新事件，同时传递新旧对象。
func (c *ServiceConfig) handleUpdateService(oldObj, newObj interface{}) {
	oldService, ok := oldObj.(*v1.Service)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", oldObj))
		return
	}
	service, ok := newObj.(*v1.Service)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", newObj))
		return
	}
	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnServiceUpdate")
		c.eventHandlers[i].OnServiceUpdate(oldService, service)
	}
}

// handleDeleteService 处理 Service 删除事件并通知所有处理程序。
func (c *ServiceConfig) handleDeleteService(obj interface{}) {
	service, ok := obj.(*v1.Service)
	if !ok {
		// 删除事件可能以 tombstone 形式送达，需要解包后才能交给处理程序。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
			return
		}
		if service, ok = tombstone.Obj.(*v1.Service); !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
			return
		}
	}
	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnServiceDelete")
		c.eventHandlers[i].OnServiceDelete(service)
	}
}

// NodeHandler 是接收 Node 对象变更通知的对象的抽象接口。
type NodeHandler interface {
	// OnNodeChange 在观察到 Node 对象新建或修改时调用。
	OnNodeChange(node *v1.Node)
	// OnNodeDelete 在观察到已有 Node 对象被删除时调用。
	OnNodeDelete(node *v1.Node)
	// OnNodeSynced 在初始事件全部处理完毕、状态完全同步到本地缓存后调用一次。
	OnNodeSynced()
}

// NodeConfig 跟踪一组 Node 配置，通过通道接收节点的设置、新增和删除操作，并在变更时调用已注册的处理程序。
type NodeConfig struct {
	listerSynced  cache.InformerSynced
	eventHandlers []NodeHandler
	logger        klog.Logger
}

// NewNodeConfig 创建新的 NodeConfig，并向 informer 注册事件回调。
func NewNodeConfig(ctx context.Context, nodeInformer v1informers.NodeInformer, resyncPeriod time.Duration) *NodeConfig {
	result := &NodeConfig{
		logger: klog.FromContext(ctx),
	}

	handlerRegistration, _ := nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { result.handleChangeNode(obj) },
			UpdateFunc: func(_, newObj interface{}) { result.handleChangeNode(newObj) },
			DeleteFunc: result.handleDeleteNode,
		},
		resyncPeriod,
	)

	result.listerSynced = handlerRegistration.HasSynced

	return result
}

// RegisterEventHandler 注册在每次 Node 变更时调用的处理程序。
func (c *NodeConfig) RegisterEventHandler(handler NodeHandler) {
	c.eventHandlers = append(c.eventHandlers, handler)
}

// Run 等待缓存同步完成后通知所有处理程序。
func (c *NodeConfig) Run(stopCh <-chan struct{}) {
	c.logger.Info("Starting node config controller")

	if !cache.WaitForNamedCacheSync("node config", stopCh, c.listerSynced) {
		return
	}

	for i := range c.eventHandlers {
		c.logger.V(3).Info("Calling handler.OnNodeSynced()")
		c.eventHandlers[i].OnNodeSynced()
	}
}

// handleChangeNode 把节点新增和更新事件统一作为变更通知分发。
func (c *NodeConfig) handleChangeNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
		return
	}
	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnNodeChange")
		c.eventHandlers[i].OnNodeChange(node)
	}
}

// handleDeleteNode 处理 Node 删除事件并通知所有处理程序。
func (c *NodeConfig) handleDeleteNode(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		// 缓存不同步时删除事件只有 tombstone，需解包出被删除的 Node。
		tombstone, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
			return
		}
		if node, ok = tombstone.Obj.(*v1.Node); !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
			return
		}
	}
	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnNodeDelete")
		c.eventHandlers[i].OnNodeDelete(node)
	}
}

// ServiceCIDRHandler 是接收 ServiceCIDR 对象变更通知的对象的抽象接口。
type ServiceCIDRHandler interface {
	// OnServiceCIDRsChanged 在任意 ServiceCIDR 发生变更时调用，并提供完整的 CIDR 列表。
	OnServiceCIDRsChanged(cidrs []string)
}

// ServiceCIDRConfig 跟踪集群中所有 ServiceCIDR 对象的 CIDR 集合。
type ServiceCIDRConfig struct {
	listerSynced  cache.InformerSynced
	eventHandlers []ServiceCIDRHandler
	mu            sync.Mutex
	cidrs         sets.Set[string]
	logger        klog.Logger
}

// NewServiceCIDRConfig 创建新的 ServiceCIDRConfig，并把增删改事件归并到同一个处理函数。
func NewServiceCIDRConfig(ctx context.Context, serviceCIDRInformer networkingv1informers.ServiceCIDRInformer, resyncPeriod time.Duration) *ServiceCIDRConfig {
	result := &ServiceCIDRConfig{
		cidrs:  sets.New[string](),
		logger: klog.FromContext(ctx),
	}

	handlerRegistration, _ := serviceCIDRInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				result.handleServiceCIDREvent(nil, obj)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				result.handleServiceCIDREvent(oldObj, newObj)
			},
			DeleteFunc: func(obj interface{}) {
				result.handleServiceCIDREvent(obj, nil)
			},
		},
		resyncPeriod,
	)

	result.listerSynced = handlerRegistration.HasSynced

	return result
}

// RegisterEventHandler 注册在每次 ServiceCIDR 变更时调用的处理程序。
func (c *ServiceCIDRConfig) RegisterEventHandler(handler ServiceCIDRHandler) {
	c.eventHandlers = append(c.eventHandlers, handler)
}

// Run 等待缓存同步完成后通知所有处理程序。
func (c *ServiceCIDRConfig) Run(stopCh <-chan struct{}) {
	c.logger.Info("Starting serviceCIDR config controller")

	if !cache.WaitForNamedCacheSync("serviceCIDR config", stopCh, c.listerSynced) {
		return
	}
	// 同步完成后主动触发一次，确保初始状态（哪怕是空集合）也能通知到处理程序。
	c.handleServiceCIDREvent(nil, nil)
}

// handleServiceCIDREvent 统一处理 ServiceCIDR 的新增、更新和删除事件，并通知下游处理程序。
func (c *ServiceCIDRConfig) handleServiceCIDREvent(oldObj, newObj interface{}) {
	var oldServiceCIDR, newServiceCIDR *networkingv1.ServiceCIDR
	var ok bool

	if oldObj != nil {
		oldServiceCIDR, ok = oldObj.(*networkingv1.ServiceCIDR)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", oldObj))
			return
		}
	}

	if newObj != nil {
		newServiceCIDR, ok = newObj.(*networkingv1.ServiceCIDR)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", newObj))
			return
		}
	}

	// 多个 informer 回调可能并发到达，加锁保护共享的 cidrs 集合及其一致快照。
	c.mu.Lock()
	defer c.mu.Unlock()

	if oldServiceCIDR != nil {
		c.cidrs.Delete(oldServiceCIDR.Spec.CIDRs...)
	}

	if newServiceCIDR != nil {
		c.cidrs.Insert(newServiceCIDR.Spec.CIDRs...)
	}

	for i := range c.eventHandlers {
		c.logger.V(4).Info("Calling handler.OnServiceCIDRsChanged")
		c.eventHandlers[i].OnServiceCIDRsChanged(c.cidrs.UnsortedList())
	}
}

// NodeTopologyHandler 是接收与代理相关的节点拓扑标签变更通知的对象的抽象接口。
type NodeTopologyHandler interface {
	// OnTopologyChange 在代理相关的节点拓扑标签发生变化时调用，并提供变更后的标签集合。
	OnTopologyChange(topologyLabels map[string]string)
}

// NodeTopologyConfig 跟踪与代理路由相关的节点拓扑标签（如拓扑区域）。
type NodeTopologyConfig struct {
	listerSynced   cache.InformerSynced
	eventHandlers  []NodeTopologyHandler
	topologyLabels map[string]string
	logger         klog.Logger
}

// NewNodeTopologyConfig 创建新的 NodeTopologyConfig。
func NewNodeTopologyConfig(ctx context.Context, nodeInformer v1informers.NodeInformer, resyncPeriod time.Duration) *NodeTopologyConfig {
	return newNodeTopologyConfig(ctx, nodeInformer, resyncPeriod, nil)
}

// newNodeTopologyConfig 实现 NewNodeTopologyConfig，额外接收一个在事件处理完成后调用的回调，仅供测试使用。
func newNodeTopologyConfig(ctx context.Context, nodeInformer v1informers.NodeInformer, resyncPeriod time.Duration, callback func()) *NodeTopologyConfig {
	result := &NodeTopologyConfig{
		logger:         klog.FromContext(ctx),
		topologyLabels: make(map[string]string),
	}

	handlerRegistration, _ := nodeInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				result.handleNodeEvent(obj)
				if callback != nil {
					callback()
				}
			},
			UpdateFunc: func(_, newObj interface{}) {
				result.handleNodeEvent(newObj)
				if callback != nil {
					callback()
				}
			},
			// 节点删除不会改变集群级拓扑标签，因此无需处理删除事件。
			DeleteFunc: func(_ interface{}) {},
		},
		resyncPeriod,
	)
	result.listerSynced = handlerRegistration.HasSynced

	return result
}

// RegisterEventHandler 注册在 Node 对象变更时调用的处理程序。
func (n *NodeTopologyConfig) RegisterEventHandler(handler NodeTopologyHandler) {
	n.eventHandlers = append(n.eventHandlers, handler)
}

// handleNodeEvent 从 Node 对象中提取代理相关的拓扑标签，并在标签变化时通知下游处理程序。
func (n *NodeTopologyConfig) handleNodeEvent(obj interface{}) {
	node, ok := obj.(*v1.Node)
	if !ok {
		utilruntime.HandleError(fmt.Errorf("unexpected object type: %v", obj))
		return
	}

	topologyLabels := make(map[string]string)
	if _, ok = node.Labels[v1.LabelTopologyZone]; ok {
		topologyLabels[v1.LabelTopologyZone] = node.Labels[v1.LabelTopologyZone]
	}

	// 拓扑标签未变化时跳过通知，避免触发无意义的路由重建。
	if reflect.DeepEqual(n.topologyLabels, topologyLabels) {
		return
	}

	n.topologyLabels = topologyLabels
	for i := range n.eventHandlers {
		n.logger.V(4).Info("Calling handler.OnTopologyChange")
		n.eventHandlers[i].OnTopologyChange(n.topologyLabels)
	}
}
