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

// 本文件通过 fake 客户端与 informer 驱动 Service/EndpointSlice 配置源，
// 验证事件处理器在数据增删改变更时能收到完整且同步后的状态快照。
package config

import (
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	klogtesting "k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// 测试 Service 配置源：通过 fake watch 依次增删改变更多个 Service，
// 校验处理器每次都能收到按名称排序的完整 Service 列表。
func TestNewServicesSourceApi_UpdatesAndMultipleServices(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	service1v1 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "s1"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}}}
	service1v2 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "s1"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 20}}}}
	service2 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "s2"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 30}}}}

	// 前置 watch reactor，使 informer 从 fakeWatch 取事件；若走默认的 list reactor 则无法手动投递增删改变更。
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("services", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	// 必须在测试退出时关停 informer/config，否则后台 goroutine 会泄漏并干扰后续用例。
	defer close(stopCh)

	handler := NewServiceHandlerMock()

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	serviceConfig := NewServiceConfig(ctx, sharedInformers.Core().V1().Services(), time.Minute)
	serviceConfig.RegisterEventHandler(handler)
	sharedInformers.Start(stopCh)
	go serviceConfig.Run(stopCh)

	// 投递首个 Service 并校验快照。
	fakeWatch.Add(service1v1)
	handler.ValidateServices(t, []*v1.Service{service1v1})

	// 新增第二个 Service，期望处理器看到两个对象。
	fakeWatch.Add(service2)
	handler.ValidateServices(t, []*v1.Service{service1v1, service2})

	// 修改按 key 覆盖旧对象，期望快照中 service1 更新为 v2。
	fakeWatch.Modify(service1v2)
	handler.ValidateServices(t, []*v1.Service{service1v2, service2})

	// 删除 service1 后仅剩 service2，验证删除事件也从快照中移除。
	fakeWatch.Delete(service1v2)
	handler.ValidateServices(t, []*v1.Service{service2})

	// 全部删除后应收到空列表而非没有通知，以此确认删除事件并非被静默忽略。
	fakeWatch.Delete(service2)
	handler.ValidateServices(t, []*v1.Service{})
}

// 测试 EndpointSlice 配置源：模拟 EndpointSlice 的添加、修改与删除，
// 校验处理器收到的快照与事件顺序一致且反映最新端点数据。
func TestNewEndpointsSourceApi_UpdatesAndMultipleEndpoints(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	endpoints1v1 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "e1"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{
				"1.2.3.4",
			},
		}},
		Ports: []discoveryv1.EndpointPort{{
			Port:     ptr.To[int32](8080),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
	}
	endpoints1v2 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "e1"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{
				"1.2.3.4",
				"4.3.2.1",
			},
		}},
		Ports: []discoveryv1.EndpointPort{{
			Port:     ptr.To[int32](8080),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
	}
	endpoints2 := &discoveryv1.EndpointSlice{
		ObjectMeta:  metav1.ObjectMeta{Namespace: "testnamespace", Name: "e2"},
		AddressType: discoveryv1.AddressTypeIPv4,
		Endpoints: []discoveryv1.Endpoint{{
			Addresses: []string{
				"5.6.7.8",
			},
		}},
		Ports: []discoveryv1.EndpointPort{{
			Port:     ptr.To[int32](8080),
			Protocol: ptr.To(v1.ProtocolTCP),
		}},
	}

	// 挂接 fake watch 到 endpointslices 资源，使后续事件可直接由测试代码投递。
	client := fake.NewSimpleClientset()
	fakeWatch := watch.NewFake()
	client.PrependWatchReactor("endpointslices", ktesting.DefaultWatchReactor(fakeWatch, nil))

	stopCh := make(chan struct{})
	defer close(stopCh)

	handler := NewEndpointSliceHandlerMock()

	sharedInformers := informers.NewSharedInformerFactory(client, time.Minute)

	endpointsliceConfig := NewEndpointSliceConfig(ctx, sharedInformers.Discovery().V1().EndpointSlices(), time.Minute)
	endpointsliceConfig.RegisterEventHandler(handler)
	sharedInformers.Start(stopCh)
	go endpointsliceConfig.Run(stopCh)

	// 投递第一个 EndpointSlice 并校验初始快照。
	fakeWatch.Add(endpoints1v1)
	handler.ValidateEndpointSlices(t, []*discoveryv1.EndpointSlice{endpoints1v1})

	// 新增第二个 EndpointSlice，快照应按名称包含两个对象。
	fakeWatch.Add(endpoints2)
	handler.ValidateEndpointSlices(t, []*discoveryv1.EndpointSlice{endpoints1v1, endpoints2})

	// 修改 e1 增加端点地址，快照应使用新版本对象替换同名旧对象。
	fakeWatch.Modify(endpoints1v2)
	handler.ValidateEndpointSlices(t, []*discoveryv1.EndpointSlice{endpoints1v2, endpoints2})

	// 删除 e1，确认快照同步收缩到只剩 e2。
	fakeWatch.Delete(endpoints1v2)
	handler.ValidateEndpointSlices(t, []*discoveryv1.EndpointSlice{endpoints2})

	// 全部删除后必须收到空列表，防止删除事件丢失而无人察觉。
	fakeWatch.Delete(endpoints2)
	handler.ValidateEndpointSlices(t, []*discoveryv1.EndpointSlice{})
}

// 测试初始同步：fake 客户端预置两个 Service 和两个 EndpointSlice，
// 验证 handler 先收到全部初始对象的 OnAdd/OnSynced 通知，Run 时再收到完整快照。
func TestInitialSync(t *testing.T) {
	_, ctx := klogtesting.NewTestContext(t)
	svc1 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}},
	}
	svc2 := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
		Spec:       v1.ServiceSpec{Ports: []v1.ServicePort{{Protocol: "TCP", Port: 10}}},
	}
	eps1 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "foo"},
	}
	eps2 := &discoveryv1.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{Namespace: "testnamespace", Name: "bar"},
	}

	expectedSvcState := map[types.NamespacedName]*v1.Service{
		{Name: svc1.Name, Namespace: svc1.Namespace}: svc1,
		{Name: svc2.Name, Namespace: svc2.Namespace}: svc2,
	}
	expectedEpsState := map[types.NamespacedName]*discoveryv1.EndpointSlice{
		{Name: eps1.Name, Namespace: eps1.Namespace}: eps1,
		{Name: eps2.Name, Namespace: eps2.Namespace}: eps2,
	}

	// 预置对象供 informer 首次 list，乱序传入 eps2/eps1 以检验同步逻辑不依赖输入顺序。
	client := fake.NewSimpleClientset(svc1, svc2, eps2, eps1)
	sharedInformers := informers.NewSharedInformerFactory(client, 0)

	svcConfig := NewServiceConfig(ctx, sharedInformers.Core().V1().Services(), 0)
	svcHandler := NewServiceHandlerMock()
	svcConfig.RegisterEventHandler(svcHandler)

	epsConfig := NewEndpointSliceConfig(ctx, sharedInformers.Discovery().V1().EndpointSlices(), 0)
	epsHandler := NewEndpointSliceHandlerMock()
	epsConfig.RegisterEventHandler(epsHandler)

	stopCh := make(chan struct{})
	defer close(stopCh)
	sharedInformers.Start(stopCh)

	// 轮询等待所有初始 Service 都已进入 handler 状态；informer 事件是异步的，直接断言会竞态。
	err := wait.PollImmediate(time.Millisecond*10, wait.ForeverTestTimeout, func() (bool, error) {
		svcHandler.lock.Lock()
		defer svcHandler.lock.Unlock()
		if reflect.DeepEqual(svcHandler.state, expectedSvcState) {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatal("Timed out waiting for the completion of handler `OnServiceAdd`")
	}

	// 同理等待 EndpointSlice 初始事件处理完毕，再触发 Run。
	err = wait.PollImmediate(time.Millisecond*10, wait.ForeverTestTimeout, func() (bool, error) {
		epsHandler.lock.Lock()
		defer epsHandler.lock.Unlock()
		if reflect.DeepEqual(epsHandler.state, expectedEpsState) {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatal("Timed out waiting for the completion of handler `OnEndpointsAdd`")
	}

	// 只有 OnSynced 之后 Run 才发送快照，因此上面必须先等到初始事件完成。
	svcConfig.Run(stopCh)
	epsConfig.Run(stopCh)

	// 从通道取出快照并转为按 NamespacedName 索引的 map，消除列表顺序影响。
	gotSvc := <-svcHandler.updated
	gotSvcState := make(map[types.NamespacedName]*v1.Service, len(gotSvc))
	for _, svc := range gotSvc {
		gotSvcState[types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}] = svc
	}
	if !reflect.DeepEqual(gotSvcState, expectedSvcState) {
		t.Fatalf("Expected service state: %v\nGot: %v\n", expectedSvcState, gotSvcState)
	}

	// 对 EndpointSlice 做同样的转换后再比对，确保初始状态完全一致。
	gotEps := <-epsHandler.updated
	gotEpsState := make(map[types.NamespacedName]*discoveryv1.EndpointSlice, len(gotEps))
	for _, eps := range gotEps {
		gotEpsState[types.NamespacedName{Namespace: eps.Namespace, Name: eps.Name}] = eps
	}
	if !reflect.DeepEqual(gotEpsState, expectedEpsState) {
		t.Fatalf("Expected endpoints state: %v\nGot: %v\n", expectedEpsState, gotEpsState)
	}
}
