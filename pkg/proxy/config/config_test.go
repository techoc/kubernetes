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

// 本文件定义 Service、EndpointSlice、Node、ServiceCIDR 与节点拓扑的事件处理器桩，
// 并通过 fake watch 验证各配置源在多处理器、增删改变更场景下的通知行为。
package config

import (
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	networkingv1 "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	informers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	klogtesting "k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// ServiceHandlerMock 记录 Service 当前状态并在同步完成后把排序后的快照推送到 updated 通道，
// 供测试断言；process 便于在构造时替换快照去向。
type ServiceHandlerMock struct {
	lock sync.Mutex

	state   map[types.NamespacedName]*v1.Service
	synced  bool
	updated chan []*v1.Service
	process func([]*v1.Service)
}

// NewServiceHandlerMock 创建带缓冲通道的处理器桩；缓冲避免快照推送阻塞 informer 回调 goroutine。
func NewServiceHandlerMock() *ServiceHandlerMock {
	shm := &ServiceHandlerMock{
		state:   make(map[types.NamespacedName]*v1.Service),
		updated: make(chan []*v1.Service, 5),
	}
	shm.process = func(services []*v1.Service) {
		shm.updated <- services
	}
	return shm
}

// OnServiceAdd 把新增 Service 按命名空间/名称记入状态并尝试推送快照。
func (h *ServiceHandlerMock) OnServiceAdd(service *v1.Service) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	h.state[namespacedName] = service
	h.sendServices()
}

// OnServiceUpdate 用新对象覆盖同名 Service 状态；旧对象在此桩中无需参与断言。
func (h *ServiceHandlerMock) OnServiceUpdate(oldService, service *v1.Service) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	h.state[namespacedName] = service
	h.sendServices()
}

// OnServiceDelete 从状态中删除指定 Service 并推送最新快照。
func (h *ServiceHandlerMock) OnServiceDelete(service *v1.Service) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: service.Namespace, Name: service.Name}
	delete(h.state, namespacedName)
	h.sendServices()
}

// OnServiceSynced 标记初始 list 已完成；此前不推送快照，避免中间态干扰断言。
func (h *ServiceHandlerMock) OnServiceSynced() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.synced = true
	h.sendServices()
}

// sendServices 在已同步的前提下推送按名称排序的快照；排序是因为 map 遍历顺序不稳定，而测试期望顺序确定。
func (h *ServiceHandlerMock) sendServices() {
	if !h.synced {
		return
	}
	services := make([]*v1.Service, 0, len(h.state))
	for _, svc := range h.state {
		services = append(services, svc)
	}
	slices.SortFunc(services, func(a, b *v1.Service) int { return strings.Compare(a.Name, b.Name) })
	h.process(services)
}

// ValidateServices 持续消费快照直到与期望一致：生产端会覆盖消费者未及时取走的旧快照，
// N 次更新可能只观察到 1 次有效快照，因此需要循环而非单次读取。
func (h *ServiceHandlerMock) ValidateServices(t *testing.T, expectedServices []*v1.Service) {
	// 消费者落后时旧快照会被新快照覆盖，故不能假定每次更新都能观察到。
	var services []*v1.Service
	for {
		select {
		case services = <-h.updated:
			if reflect.DeepEqual(services, expectedServices) {
				return
			}
		// 主动设置较短超时，避免被测试框架 5 分钟硬超时打断并给出更清晰的错误。
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Timed out. Expected %#v, Got %#v", expectedServices, services)
			return
		}
	}
}

// 测试新增 Service 时配置源会通知已注册处理器，且快照包含该 Service。
func TestNewServiceAddedAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	// 注入 fake watch，使事件由测试代码显式投递而非依赖默认 list/watch 行为。
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("services", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	// 测试结束时关闭 stopCh 以停止 informer 与 config 的后台 goroutine。
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewServiceConfig(ctx, sharedInformers.Core().V1().Services(), time.Minute)
	handler := NewServiceHandlerMock()
	config.RegisterEventHandler(handler)
	sharedInformers.Start(stopCh)
	// Run 在单独 goroutine 中运行；同步先在 informer 回调里完成，再向通道发送快照。
	go config.Run(stopCh)

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}},
	}
	fakeWatch.Add(service)
	handler.ValidateServices(t, []*v1.Service{service})
}

// 测试 Service 新增与删除：处理器应随着 watch 事件依次看到单对象、双对象、再回到单对象的快照。
func TestServiceAddedRemovedSetAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	// 注入 fake watch，使事件由测试代码显式投递而非依赖默认 list/watch 行为。
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("services", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	// 测试结束时关闭 stopCh 以停止 informer 与 config 的后台 goroutine。
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewServiceConfig(ctx, sharedInformers.Core().V1().Services(), time.Minute)
	handler := NewServiceHandlerMock()
	config.RegisterEventHandler(handler)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	service1 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}},
	}
	fakeWatch.Add(service1)
	handler.ValidateServices(t, []*v1.Service{service1})

	// 名称 bar 排在 foo 之前，期望快照按名称排序为 [service2, service1]。
	service2 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 20}}},
	}
	fakeWatch.Add(service2)
	services := []*v1.Service{service2, service1}
	handler.ValidateServices(t, services)

	// 删除 foo 后仅剩 bar，验证删除事件触发快照更新。
	fakeWatch.Delete(service1)
	services = []*v1.Service{service2}
	handler.ValidateServices(t, services)
}

// 测试同一配置源的多个事件处理器：每个处理器都应独立收到相同的完整 Service 快照。
func TestNewServicesMultipleHandlersAddedAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	// 注入 fake watch，使事件由测试代码显式投递而非依赖默认 list/watch 行为。
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("services", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	// 测试结束时关闭 stopCh 以停止 informer 与 config 的后台 goroutine。
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewServiceConfig(ctx, sharedInformers.Core().V1().Services(), time.Minute)
	handler := NewServiceHandlerMock()
	handler2 := NewServiceHandlerMock()
	// 注册两个处理器，验证事件分发不是只发给第一个注册者。
	config.RegisterEventHandler(handler)
	config.RegisterEventHandler(handler2)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	service1 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}},
	}
	service2 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 20}}},
	}
	fakeWatch.Add(service1)
	fakeWatch.Add(service2)

	services := []*v1.Service{service2, service1}
	handler.ValidateServices(t, services)
	handler2.ValidateServices(t, services)
}

// EndpointSliceHandlerMock 记录 EndpointSlice 状态并在同步后推送排序快照，
// 结构与 ServiceHandlerMock 对应，便于复用相同的验证方式。
type EndpointSliceHandlerMock struct {
	lock sync.Mutex

	state   map[types.NamespacedName]*discoveryv1.EndpointSlice
	synced  bool
	updated chan []*discoveryv1.EndpointSlice
	process func([]*discoveryv1.EndpointSlice)
}

// NewEndpointSliceHandlerMock 创建带缓冲通道的 EndpointSlice 处理器桩，防止推送阻塞回调。
func NewEndpointSliceHandlerMock() *EndpointSliceHandlerMock {
	ehm := &EndpointSliceHandlerMock{
		state:   make(map[types.NamespacedName]*discoveryv1.EndpointSlice),
		updated: make(chan []*discoveryv1.EndpointSlice, 5),
	}
	ehm.process = func(endpoints []*discoveryv1.EndpointSlice) {
		ehm.updated <- endpoints
	}
	return ehm
}

// OnEndpointSliceAdd 记录新增的 EndpointSlice 并推送最新快照。
func (h *EndpointSliceHandlerMock) OnEndpointSliceAdd(slice *discoveryv1.EndpointSlice) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: slice.Namespace, Name: slice.Name}
	h.state[namespacedName] = slice
	h.sendEndpointSlices()
}

// OnEndpointSliceUpdate 以新对象覆盖同名 EndpointSlice 的状态，旧对象不参与断言。
func (h *EndpointSliceHandlerMock) OnEndpointSliceUpdate(oldSlice, slice *discoveryv1.EndpointSlice) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: slice.Namespace, Name: slice.Name}
	h.state[namespacedName] = slice
	h.sendEndpointSlices()
}

// OnEndpointSliceDelete 从状态中移除 EndpointSlice 并推送快照。
func (h *EndpointSliceHandlerMock) OnEndpointSliceDelete(slice *discoveryv1.EndpointSlice) {
	h.lock.Lock()
	defer h.lock.Unlock()
	namespacedName := types.NamespacedName{Namespace: slice.Namespace, Name: slice.Name}
	delete(h.state, namespacedName)
	h.sendEndpointSlices()
}

// OnEndpointSlicesSynced 标记初始同步完成，之后才允许向通道发送快照。
func (h *EndpointSliceHandlerMock) OnEndpointSlicesSynced() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.synced = true
	h.sendEndpointSlices()
}

// sendEndpointSlices 按名称排序快照后推送；排序抵消 map 遍历的不确定性并保证断言稳定。
func (h *EndpointSliceHandlerMock) sendEndpointSlices() {
	if !h.synced {
		return
	}
	endpointSlices := make([]*discoveryv1.EndpointSlice, 0, len(h.state))
	for _, eps := range h.state {
		endpointSlices = append(endpointSlices, eps)
	}
	slices.SortFunc(endpointSlices, func(a, b *discoveryv1.EndpointSlice) int { return strings.Compare(a.Name, b.Name) })
	h.process(endpointSlices)
}

// ValidateEndpointSlices 反复读取快照直至与期望一致；消费落后时旧快照会被生产端覆盖，
// 且自行设置超时以免落入测试框架的 5 分钟硬超时。
func (h *EndpointSliceHandlerMock) ValidateEndpointSlices(t *testing.T, expectedSlices []*discoveryv1.EndpointSlice) {
	// 旧快照可能被覆盖，所以必须循环等待而不是只读取一次。
	var slices []*discoveryv1.EndpointSlice
	for {
		select {
		case slices = <-h.updated:
			if reflect.DeepEqual(slices, expectedSlices) {
				return
			}
		// 用受控超时失败并输出期望/实际值，避免笼统的测试框架超时堆栈。
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Timed out. Expected %#v, Got %#v", expectedSlices, slices)
			return
		}
	}
}

// 测试 EndpointSlice 多处理器通知：两个处理器都应收到相同的双对象排序快照。
func TestNewEndpointsMultipleHandlersAddedAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	// 注入 fake watch 以便测试代码手动投递 EndpointSlice 事件。
	client.PrependWatchReactor("endpointslices", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewEndpointSliceConfig(ctx, sharedInformers.Discovery().V1().EndpointSlices(), time.Minute)
	handler := NewEndpointSliceHandlerMock()
	handler2 := NewEndpointSliceHandlerMock()
	// 注册两个处理器，验证事件分发不是只发给第一个注册者。
	config.RegisterEventHandler(handler)
	config.RegisterEventHandler(handler2)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	endpoints1 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}, {
			Addresses: []string{"2.2.2.2"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	endpoints2 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"3.3.3.3"},
		}, {
			Addresses: []string{"4.4.4.4"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	fakeWatch.Add(endpoints1)
	fakeWatch.Add(endpoints2)

	endpoints := []*discoveryv1.EndpointSlice{endpoints2, endpoints1}
	handler.ValidateEndpointSlices(t, endpoints)
	handler2.ValidateEndpointSlices(t, endpoints)
}

// 测试 EndpointSlice 的新增、修改与删除：每一步变更都应向所有处理器广播最新快照。
func TestNewEndpointsMultipleHandlersAddRemoveSetAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	// 注入 fake watch 以便测试代码手动投递 EndpointSlice 事件。
	client.PrependWatchReactor("endpointslices", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewEndpointSliceConfig(ctx, sharedInformers.Discovery().V1().EndpointSlices(), time.Minute)
	handler := NewEndpointSliceHandlerMock()
	handler2 := NewEndpointSliceHandlerMock()
	// 注册两个处理器，验证事件分发不是只发给第一个注册者。
	config.RegisterEventHandler(handler)
	config.RegisterEventHandler(handler2)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	endpoints1 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}, {
			Addresses: []string{"2.2.2.2"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	endpoints2 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"3.3.3.3"},
		}, {
			Addresses: []string{"4.4.4.4"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	fakeWatch.Add(endpoints1)
	fakeWatch.Add(endpoints2)

	endpoints := []*discoveryv1.EndpointSlice{endpoints2, endpoints1}
	handler.ValidateEndpointSlices(t, endpoints)
	handler2.ValidateEndpointSlices(t, endpoints)

	// 新增第三个 EndpointSlice，期望快照按名称排序为 bar、foo、foobar。
	endpoints3 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "foobar"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"5.5.5.5"},
		}, {
			Addresses: []string{"6.6.6.6"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	fakeWatch.Add(endpoints3)
	endpoints = []*discoveryv1.EndpointSlice{endpoints2, endpoints1, endpoints3}
	handler.ValidateEndpointSlices(t, endpoints)
	handler2.ValidateEndpointSlices(t, endpoints)

	// 用同名新对象替换 foo，模拟端点集合收缩为一个地址。
	endpoints1v2 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{"7.7.7.7"},
		}},
		Ports: []discoveryv1.EndpointPort{{Port: ptr.To[int32](80)}},
	}
	fakeWatch.Modify(endpoints1v2)
	endpoints = []*discoveryv1.EndpointSlice{endpoints2, endpoints1v2, endpoints3}
	handler.ValidateEndpointSlices(t, endpoints)
	handler2.ValidateEndpointSlices(t, endpoints)

	// 删除 bar，验证快照从中移除该 EndpointSlice。
	fakeWatch.Delete(endpoints2)
	endpoints = []*discoveryv1.EndpointSlice{endpoints1v2, endpoints3}
	handler.ValidateEndpointSlices(t, endpoints)
	handler2.ValidateEndpointSlices(t, endpoints)
}

// NodeHandlerMock 保存 Node 状态并向通道推送排序快照，用于验证节点配置源的通知。
type NodeHandlerMock struct {
	lock sync.Mutex

	state   map[string]*v1.Node
	synced  bool
	updated chan []*v1.Node
	process func([]*v1.Node)
}

// NewNodeHandlerMock 创建带缓冲通道的 Node 处理器桩，避免阻塞 informer 回调。
func NewNodeHandlerMock() *NodeHandlerMock {
	h := &NodeHandlerMock{
		state:   make(map[string]*v1.Node),
		updated: make(chan []*v1.Node, 5),
	}
	h.process = func(nodes []*v1.Node) {
		h.updated <- nodes
	}
	return h
}

// OnNodeChange 处理 Node 的新增与更新；两者都以当前对象覆盖同名记录。
func (h *NodeHandlerMock) OnNodeChange(node *v1.Node) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.state[node.Name] = node
	h.sendNodes()
}

// OnNodeDelete 移除指定 Node 并推送更新后的快照。
func (h *NodeHandlerMock) OnNodeDelete(node *v1.Node) {
	h.lock.Lock()
	defer h.lock.Unlock()
	delete(h.state, node.Name)
	h.sendNodes()
}

// OnNodeSynced 标记节点初始同步完成，仅在此之后推送快照。
func (h *NodeHandlerMock) OnNodeSynced() {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.synced = true
	h.sendNodes()
}

// sendNodes 按节点名排序后推送快照，消除 map 遍历顺序带来的不确定性。
func (h *NodeHandlerMock) sendNodes() {
	if !h.synced {
		return
	}
	nodes := make([]*v1.Node, 0, len(h.state))
	for _, svc := range h.state {
		nodes = append(nodes, svc)
	}
	slices.SortFunc(nodes, func(a, b *v1.Node) int { return strings.Compare(a.Name, b.Name) })
	h.process(nodes)
}

// ValidateNodes 循环消费节点快照直到匹配期望；旧快照可能被覆盖且测试框架有 5 分钟硬超时。
func (h *NodeHandlerMock) ValidateNodes(t *testing.T, expectedNodes []*v1.Node) {
	// 生产端覆盖旧快照意味着 N 次变更可能少于 N 次有效通知。
	var nodes []*v1.Node
	for {
		select {
		case nodes = <-h.updated:
			if reflect.DeepEqual(nodes, expectedNodes) {
				return
			}
		// 自定义超时以便失败时直接给出期望与实际节点列表。
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Timed out. Expected %#v, Got %#v", expectedNodes, nodes)
			return
		}
	}
}

// 测试节点配置源：多个处理器在节点新增、地址更新与删除时都应收到一致的排序快照。
func TestNewNodesMultipleHandlersAddRemoveSetAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	// 注入 fake watch 以便测试代码手动投递 Node 事件。
	client.PrependWatchReactor("nodes", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewNodeConfig(ctx, sharedInformers.Core().V1().Nodes(), time.Minute)
	handler := NewNodeHandlerMock()
	handler2 := NewNodeHandlerMock()
	// 注册两个处理器，验证事件分发不是只发给第一个注册者。
	config.RegisterEventHandler(handler)
	config.RegisterEventHandler(handler2)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	nodes1 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: "1.1.1.1",
				},
				{
					Type:    v1.NodeExternalIP,
					Address: "2.2.2.2",
				},
			},
		},
	}
	nodes2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "bar"},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: "3.3.3.3",
				},
				{
					Type:    v1.NodeInternalIP,
					Address: "fc00::4",
				},
			},
		},
	}
	fakeWatch.Add(nodes1)
	fakeWatch.Add(nodes2)

	nodes := []*v1.Node{nodes2, nodes1}
	handler.ValidateNodes(t, nodes)
	handler2.ValidateNodes(t, nodes)

	// 新增 foobar 节点，期望快照按名称顺序扩展为 bar、foo、foobar。
	nodes3 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "foobar"},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: "5.5.5.5",
				},
			},
		},
	}
	fakeWatch.Add(nodes3)
	nodes = []*v1.Node{nodes2, nodes1, nodes3}
	handler.ValidateNodes(t, nodes)
	handler2.ValidateNodes(t, nodes)

	// 更新 foo 节点的地址列表，验证按名称覆盖后快照使用新地址。
	nodes1v2 := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Status: v1.NodeStatus{
			Addresses: []v1.NodeAddress{
				{
					Type:    v1.NodeInternalIP,
					Address: "6.6.6.6",
				},
			},
		},
	}
	fakeWatch.Modify(nodes1v2)
	nodes = []*v1.Node{nodes2, nodes1v2, nodes3}
	handler.ValidateNodes(t, nodes)
	handler2.ValidateNodes(t, nodes)

	// 删除 bar 节点，确认两个处理器都收到收缩后的列表。
	fakeWatch.Delete(nodes2)
	nodes = []*v1.Node{nodes1v2, nodes3}
	handler.ValidateNodes(t, nodes)
	handler2.ValidateNodes(t, nodes)
}

// ServiceCIDRHandlerMock 收集 ServiceCIDR 变更后的 CIDR 列表，
// 在推送前复制并排序，避免共享底层数组被后续事件改写。
type ServiceCIDRHandlerMock struct {
	lock sync.Mutex

	state   []string
	updated chan []string
	process func([]string)
}

// NewServiceCIDRHandlerMock 创建带缓冲通道的 ServiceCIDR 处理器桩。
func NewServiceCIDRHandlerMock() *ServiceCIDRHandlerMock {
	h := &ServiceCIDRHandlerMock{
		updated: make(chan []string, 5),
	}
	h.process = func(serviceCIDRs []string) {
		h.updated <- serviceCIDRs
	}
	return h
}

// OnServiceCIDRsChanged 保存当前全部 ServiceCIDR 列表并推送快照。
func (h *ServiceCIDRHandlerMock) OnServiceCIDRsChanged(serviceCIDRs []string) {
	h.lock.Lock()
	defer h.lock.Unlock()
	h.state = serviceCIDRs
	h.sendServiceCIDRs()
}

// sendServiceCIDRs 复制状态后排序再推送：复制防止并发修改，排序让期望值与实际值可比。
func (h *ServiceCIDRHandlerMock) sendServiceCIDRs() {
	serviceCIDRs := append([]string{}, h.state...)
	slices.Sort(serviceCIDRs)
	h.process(serviceCIDRs)
}

// ValidateServiceCIDRs 循环读取直到 CIDR 快照匹配期望；不匹配时记录中间值便于定位，超时后明确报错。
func (h *ServiceCIDRHandlerMock) ValidateServiceCIDRs(t *testing.T, expectedServiceCIDRs []string) {
	// 旧快照会被新快照覆盖，因此循环比对而不是假设每次变更都有一一对应的通知。
	var serviceCIDRs []string
	for {
		select {
		case serviceCIDRs = <-h.updated:
			if reflect.DeepEqual(serviceCIDRs, expectedServiceCIDRs) {
				return
			}
			t.Logf("Expected %#v, Got %#v", expectedServiceCIDRs, serviceCIDRs)
		// 使用受控超时并输出期望与实际 CIDR，避免框架硬超时掩盖原因。
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("Timed out. Expected %#v, Got %#v", expectedServiceCIDRs, serviceCIDRs)
			return
		}
	}
}

// 测试 ServiceCIDR 配置源：CIDR 对象的新增、修改与删除都应通知所有处理器，
// 且快照是全部 ServiceCIDR 中 CIDR 字段的排序聚合。
func TestNewServiceCIDRsMultipleHandlersAddRemoveSetAndNotified(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("servicecidrs", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	config := NewServiceCIDRConfig(ctx, sharedInformers.Networking().V1().ServiceCIDRs(), time.Minute)
	handler := NewServiceCIDRHandlerMock()
	handler2 := NewServiceCIDRHandlerMock()
	// 注册两个处理器，验证事件分发不是只发给第一个注册者。
	config.RegisterEventHandler(handler)
	config.RegisterEventHandler(handler2)
	sharedInformers.Start(stopCh)
	go config.Run(stopCh)

	serviceCIDRs1 := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: networkingv1.ServiceCIDRSpec{
			CIDRs: []string{"1.1.1.0/24"},
		},
	}
	serviceCIDRs2 := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{Name: "bar"},
		Spec: networkingv1.ServiceCIDRSpec{
			CIDRs: []string{"2.2.2.0/24", "fc00::/64"},
		},
	}
	fakeWatch.Add(serviceCIDRs1)
	fakeWatch.Add(serviceCIDRs2)

	serviceCIDRs := []string{"1.1.1.0/24", "2.2.2.0/24", "fc00::/64"}
	handler.ValidateServiceCIDRs(t, serviceCIDRs)
	handler2.ValidateServiceCIDRs(t, serviceCIDRs)

	// 新增对象带来两个 CIDR，期望合并后按字典序插入快照。
	serviceCIDRs3 := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{Name: "foobar"},
		Spec: networkingv1.ServiceCIDRSpec{
			CIDRs: []string{"3.3.3.0/24", "2001:db8::/64"},
		},
	}
	fakeWatch.Add(serviceCIDRs3)
	serviceCIDRs = []string{"1.1.1.0/24", "2.2.2.0/24", "2001:db8::/64", "3.3.3.0/24", "fc00::/64"}
	handler.ValidateServiceCIDRs(t, serviceCIDRs)
	handler2.ValidateServiceCIDRs(t, serviceCIDRs)

	// 更新 foo 后其旧 CIDR 必须被新值替换，而非追加到列表中。
	serviceCIDRs1v2 := &networkingv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec: networkingv1.ServiceCIDRSpec{
			CIDRs: []string{"4.4.4.0/24"},
		},
	}
	fakeWatch.Modify(serviceCIDRs1v2)
	serviceCIDRs = []string{"2.2.2.0/24", "2001:db8::/64", "3.3.3.0/24", "4.4.4.0/24", "fc00::/64"}
	handler.ValidateServiceCIDRs(t, serviceCIDRs)
	handler2.ValidateServiceCIDRs(t, serviceCIDRs)

	// 删除 bar 后其两个 CIDR 都不应再出现在快照中。
	fakeWatch.Delete(serviceCIDRs2)
	serviceCIDRs = []string{"2001:db8::/64", "3.3.3.0/24", "4.4.4.0/24"}
	handler.ValidateServiceCIDRs(t, serviceCIDRs)
	handler2.ValidateServiceCIDRs(t, serviceCIDRs)
}

// nodeTopologyHandlerMock 记录最近一次收到的拓扑标签，用于验证节点拓扑配置源的过滤逻辑。
type nodeTopologyHandlerMock struct {
	topologyLabels map[string]string
}

// OnTopologyChange 直接保存回调传入的拓扑标签，便于断言内容是否为空及具体值。
func (n *nodeTopologyHandlerMock) OnTopologyChange(topologyLabels map[string]string) {
	n.topologyLabels = topologyLabels
}

// waitForInvocation 等待事件处理器完成一次调用；超时返回错误，避免测试卡在框架硬超时上。
func waitForInvocation(invoked <-chan struct{}) error {
	select {
	// 使用受控超时以便返回明确的错误信息，而不是等测试框架的 5 分钟硬超时。
	case <-time.After(wait.ForeverTestTimeout):
		return fmt.Errorf("timed out waiting for event handler to process update")
	case <-invoked:
		return nil
	}
}

// 测试节点拓扑配置：只有与 kube-proxy 相关的 zone 标签变更才应触发通知，
// region 标签或普通标签的变化必须被过滤。
func TestNewNodeTopologyConfig(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	client := fake.NewClientset()
	fakeWatch := watch.NewFake()
	// 注入 fake watch 以便测试代码手动投递 Node 事件。
	client.PrependWatchReactor("nodes", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)
	nodeInformer := sharedInformers.Core().V1().Nodes()
	// 回调在事件被处理器处理完后触发，用作同步点，确保断言前变更已生效。
	invoked := make(chan struct{})
	config := newNodeTopologyConfig(ctx, nodeInformer, time.Minute, func() {
		// 该回调发生在 handler 处理完毕后；写入通道并由 waitForInvocation 等待，
		// 这样断言不会与异步的 informer 回调发生竞态。
		invoked <- struct{}{}
	})

	handler := &nodeTopologyHandlerMock{
		topologyLabels: make(map[string]string),
	}
	config.RegisterEventHandler(handler)
	sharedInformers.Start(stopCh)

	testNodeName := "test-node"

	// 仅含普通标签的节点不应触发拓扑通知，拓扑标签应为空。
	fakeWatch.Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
			Labels: map[string]string{
				v1.LabelInstanceType: "m4.large",
				v1.LabelOSStable:     "linux",
			},
		},
	})
	err := waitForInvocation(invoked)
	require.NoError(t, err)
	require.Empty(t, handler.topologyLabels)

	// region 标签对 kube-proxy 无用，同样不应触发通知。
	fakeWatch.Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
			Labels: map[string]string{
				v1.LabelInstanceType:   "m4.large",
				v1.LabelOSStable:       "linux",
				v1.LabelTopologyRegion: "us-east-1",
			},
		},
	})
	err = waitForInvocation(invoked)
	require.NoError(t, err)
	require.Empty(t, handler.topologyLabels)

	// zone 标签是 kube-proxy 关注的拓扑键，应收到通知且只包含该标签。
	fakeWatch.Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
			Labels: map[string]string{
				v1.LabelInstanceType: "c6.large",
				v1.LabelOSStable:     "windows",
				v1.LabelTopologyZone: "us-west-2a",
			},
		},
	})

	err = waitForInvocation(invoked)
	require.NoError(t, err)
	require.Len(t, handler.topologyLabels, 1)
	require.Equal(t, map[string]string{
		v1.LabelTopologyZone: "us-west-2a",
	}, handler.topologyLabels)

	// 即使同时带有 zone，也只保留 zone；kube-proxy 不做基于 region 的拓扑路由。
	fakeWatch.Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
			Labels: map[string]string{
				v1.LabelInstanceType:   "m3.medium",
				v1.LabelOSStable:       "windows",
				v1.LabelTopologyRegion: "us-east-1",
				v1.LabelTopologyZone:   "us-east-1b",
			},
		},
	})
	err = waitForInvocation(invoked)
	require.NoError(t, err)
	require.Len(t, handler.topologyLabels, 1)
	require.Equal(t, map[string]string{
		v1.LabelTopologyZone: "us-east-1b",
	}, handler.topologyLabels)

	// 仅普通标签改变不应改变拓扑结果，标签内容仍为上一轮的 zone。
	fakeWatch.Add(&v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: testNodeName,
			Labels: map[string]string{
				v1.LabelInstanceType:   "m3.large",
				v1.LabelOSStable:       "windows",
				v1.LabelTopologyRegion: "us-east-1",
				v1.LabelTopologyZone:   "us-east-1b",
			},
		},
	})
	err = waitForInvocation(invoked)
	require.NoError(t, err)
	require.Len(t, handler.topologyLabels, 1)
}

// 后续补充中断能被及时处理的相关单元测试。
