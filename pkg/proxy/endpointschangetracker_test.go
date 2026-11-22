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

package proxy

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/utils/ptr"
)

// addEndpointSlice 直接向变更跟踪器提交一个 EndpointSlice 的新增（remove=false）。
// 通过 FakeProxier 转发，让测试无需关心跟踪器的具体调用方式。
func (proxier *FakeProxier) addEndpointSlice(slice *discovery.EndpointSlice) {
	proxier.endpointsChanges.EndpointSliceUpdate(slice, false)
}

// updateEndpointSlice 模拟 EndpointSlice 的更新。
// 跟踪器按名字缓存 slice，因此 oldSlice 仅体现调用方语义，实际提交的仍是新 slice。
func (proxier *FakeProxier) updateEndpointSlice(oldSlice, slice *discovery.EndpointSlice) {
	proxier.endpointsChanges.EndpointSliceUpdate(slice, false)
}

// deleteEndpointSlice 向变更跟踪器提交一次 EndpointSlice 删除（remove=true）。
// 删除与新增走同一更新入口，仅由 remove 标志区分。
func (proxier *FakeProxier) deleteEndpointSlice(slice *discovery.EndpointSlice) {
	proxier.endpointsChanges.EndpointSliceUpdate(slice, true)
}

// TestGetLocalEndpointIPs 验证 getLocalReadyEndpointIPs 能按服务聚合本机就绪端点的 IP。
// 覆盖空映射、未命名端口、多端口去重、多服务以及未就绪/终止端点被排除等场景；
// 同一 IP 出现在多个端口时只应计入一次，因此结果用集合表示。
func TestGetLocalEndpointIPs(t *testing.T) {
	testCases := []struct {
		endpointsMap EndpointsMap
		expected     map[types.NamespacedName]sets.Set[string]
	}{{
		// 用例[0]：空映射，结果应为空。
		endpointsMap: EndpointsMap{},
		expected:     map[types.NamespacedName]sets.Set[string]{},
	}, {
		// 用例[1]：未命名端口，且端点不在本机，结果应为空。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expected: map[types.NamespacedName]sets.Set[string]{},
	}, {
		// 用例[2]：未命名端口的端点在本机，应返回其 IP。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expected: map[types.NamespacedName]sets.Set[string]{
			{Namespace: "ns1", Name: "ep1"}: sets.New[string]("1.1.1.1"),
		},
	}, {
		// 用例[3]：同一服务下两个命名端口，分别含非本机与本机端点，应只返回本机 IP。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "1.1.1.2", port: 11, endpoint: "1.1.1.2:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: false, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expected: map[types.NamespacedName]sets.Set[string]{
			{Namespace: "ns1", Name: "ep1"}: sets.New[string]("1.1.1.2"),
		},
	}, {
		// 用例[4]：多命名端口、多 IP、多服务混合，逐服务聚合本机 IP，并排除非本机端点。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "2.2.2.22", port: 22, endpoint: "2.2.2.22:22", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.3", port: 23, endpoint: "2.2.2.3:23", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.4", port: 44, endpoint: "4.4.4.4:44", isLocal: true, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "4.4.4.5", port: 44, endpoint: "4.4.4.5:44", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.6", port: 45, endpoint: "4.4.4.6:45", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expected: map[types.NamespacedName]sets.Set[string]{
			{Namespace: "ns2", Name: "ep2"}: sets.New[string]("2.2.2.2", "2.2.2.22", "2.2.2.3"),
			{Namespace: "ns4", Name: "ep4"}: sets.New[string]("4.4.4.4", "4.4.4.6"),
		},
	}, {
		// 用例[5]：在上例基础上加入未就绪且正在终止的端点，验证这类端点不会计入本机就绪 IP。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "2.2.2.22", port: 22, endpoint: "2.2.2.22:22", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.3", port: 23, endpoint: "2.2.2.3:23", isLocal: true, ready: false, serving: true, terminating: true},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.4", port: 44, endpoint: "4.4.4.4:44", isLocal: true, ready: true, serving: true, terminating: false},
				&BaseEndpointInfo{ip: "4.4.4.5", port: 44, endpoint: "4.4.4.5:44", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.6", port: 45, endpoint: "4.4.4.6:45", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expected: map[types.NamespacedName]sets.Set[string]{
			{Namespace: "ns2", Name: "ep2"}: sets.New[string]("2.2.2.2", "2.2.2.22"),
			{Namespace: "ns4", Name: "ep4"}: sets.New[string]("4.4.4.4", "4.4.4.6"),
		},
	}, {
		// 用例[6]：所有端点都在终止中，因此不应返回任何本机就绪 IP。
		endpointsMap: EndpointsMap{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: false, serving: true, terminating: true},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: false, serving: true, terminating: true},
				&BaseEndpointInfo{ip: "2.2.2.22", port: 22, endpoint: "2.2.2.22:22", isLocal: true, ready: false, serving: true, terminating: true},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "2.2.2.3", port: 23, endpoint: "2.2.2.3:23", isLocal: true, ready: false, serving: true, terminating: true},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.4", port: 44, endpoint: "4.4.4.4:44", isLocal: true, ready: false, serving: true, terminating: true},
				&BaseEndpointInfo{ip: "4.4.4.5", port: 44, endpoint: "4.4.4.5:44", isLocal: false, ready: false, serving: true, terminating: true},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolTCP): []Endpoint{
				&BaseEndpointInfo{ip: "4.4.4.6", port: 45, endpoint: "4.4.4.6:45", isLocal: true, ready: false, serving: true, terminating: true},
			},
		},
		expected: make(map[types.NamespacedName]sets.Set[string], 0),
	}}

	for tci, tc := range testCases {
		// 执行被测函数并比对期望结果。
		localIPs := tc.endpointsMap.getLocalReadyEndpointIPs()

		if !reflect.DeepEqual(localIPs, tc.expected) {
			t.Errorf("[%d] expected %#v, got %#v", tci, tc.expected, localIPs)
		}
	}
}

// makeTestEndpointSlice 构造一个最小可用的 EndpointSlice 测试夹具：
// 用 name 和序号拼出唯一名字，打上服务名标签并指定 IPv4 地址类型；
// epsFunc 让每个用例只填充自己关心的端点/端口字段，避免重复样板代码。
func makeTestEndpointSlice(namespace, name string, slice int, epsFunc func(*discovery.EndpointSlice)) *discovery.EndpointSlice {
	eps := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:        fmt.Sprintf("%s-%d", name, slice),
			Namespace:   namespace,
			Annotations: map[string]string{},
			Labels: map[string]string{
				discovery.LabelServiceName: name,
			},
		},
		AddressType: discovery.AddressTypeIPv4,
	}
	epsFunc(eps)
	return eps
}

// TestUpdateEndpointsMap 是端点映射增量更新的主表驱动测试。
// 对每个用例先把此前的 EndpointSlice 灌入缓存并断言初始映射正确（否则后续差异无法归因），
// 再按 nil 与否调用新增/删除/更新处理器，最后同时校验：结果映射、变更服务集合、
// 是否需要清理 conntrack，以及本机就绪端点计数。
func TestUpdateEndpointsMap(t *testing.T) {
	emptyEndpoint := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{}
	}
	unnamedPort := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	unnamedPortReady := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(true),
				Serving:     ptr.To(true),
				Terminating: ptr.To(false),
			},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	unnamedPortTerminating := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			Conditions: discovery.EndpointConditions{
				Ready:       ptr.To(false),
				Serving:     ptr.To(true),
				Terminating: ptr.To(true),
			},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	unnamedPortLocal := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To(""),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortLocal := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPort := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortRenamed := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11-2"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortRenumbered := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	namedPortsLocalNoLocal := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}, {
			Addresses: []string{"1.1.1.2"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsets_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsets_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.2"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsWithLocal_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsWithLocal_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.2"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsMultiplePortsLocal_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsMultiplePortsLocal_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.3"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsIPsPorts1_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}, {
			Addresses: []string{"1.1.1.2"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsIPsPorts1_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.3"},
		}, {
			Addresses: []string{"1.1.1.4"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p13"),
			Port:     ptr.To[int32](13),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p14"),
			Port:     ptr.To[int32](14),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	multipleSubsetsIPsPorts2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"2.2.2.1"},
		}, {
			Addresses: []string{"2.2.2.2"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p21"),
			Port:     ptr.To[int32](21),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore2_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"2.2.2.2"},
			NodeName:  ptr.To(testHostname),
		}, {
			Addresses: []string{"2.2.2.22"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p22"),
			Port:     ptr.To[int32](22),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore2_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"2.2.2.3"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p23"),
			Port:     ptr.To[int32](23),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore4_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"4.4.4.4"},
			NodeName:  ptr.To(testHostname),
		}, {
			Addresses: []string{"4.4.4.5"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexBefore4_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"4.4.4.6"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p45"),
			Port:     ptr.To[int32](45),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexAfter1_s1 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
		}, {
			Addresses: []string{"1.1.1.11"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p11"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexAfter1_s2 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.2"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p12"),
			Port:     ptr.To[int32](12),
			Protocol: ptr.To(v1.ProtocolUDP),
		}, {
			Name:     ptr.To("p122"),
			Port:     ptr.To[int32](122),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexAfter3 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"3.3.3.3"},
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p33"),
			Port:     ptr.To[int32](33),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}
	complexAfter4 := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"4.4.4.4"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("p44"),
			Port:     ptr.To[int32](44),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}

	testCases := []struct {
		// previousEndpointSlices 与 currentEndpointSlices 按位置成对使用，
		// 根据同一位置的值是否为 nil 决定调用哪个 OnEndpointSlice* 处理器，
		// 两者长度必须相等；nil/nil 表示无变化，nil/非 nil 表示新增，非 nil/nil 表示删除。
		name                             string
		previousEndpointSlices           []*discovery.EndpointSlice
		currentEndpointSlices            []*discovery.EndpointSlice
		previousEndpointsMap             map[ServicePortName][]*BaseEndpointInfo
		expectedResult                   map[ServicePortName][]*BaseEndpointInfo
		expectedConntrackCleanupRequired bool
		expectedLocalEndpoints           map[types.NamespacedName]int
		expectedChangedEndpoints         sets.Set[types.NamespacedName]
	}{{
		name:                             "empty",
		previousEndpointsMap:             map[ServicePortName][]*BaseEndpointInfo{},
		expectedResult:                   map[ServicePortName][]*BaseEndpointInfo{},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New[types.NamespacedName](),
	}, {
		name: "no change, unnamed port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPort),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPort),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New[types.NamespacedName](),
	}, {
		name: "no change, named port, local",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortLocal),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortLocal),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
		expectedChangedEndpoints: sets.New[types.NamespacedName](),
	}, {
		name: "no change, multiple slices",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsets_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsets_s2),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsets_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsets_s2),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New[types.NamespacedName](),
	}, {
		name: "no change, multiple slices, multiple ports, local",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsetsMultiplePortsLocal_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsetsMultiplePortsLocal_s2),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsetsMultiplePortsLocal_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsetsMultiplePortsLocal_s2),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 13, endpoint: "1.1.1.3:13", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 13, endpoint: "1.1.1.3:13", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
		expectedChangedEndpoints: sets.New[types.NamespacedName](),
	}, {
		name: "no change, multiple services, slices, IPs, and ports",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsetsIPsPorts1_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsetsIPsPorts1_s2),
			makeTestEndpointSlice("ns2", "ep2", 1, multipleSubsetsIPsPorts2),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsetsIPsPorts1_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsetsIPsPorts1_s2),
			makeTestEndpointSlice("ns2", "ep2", 1, multipleSubsetsIPsPorts2),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 11, endpoint: "1.1.1.2:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 13, endpoint: "1.1.1.3:13", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.4", port: 13, endpoint: "1.1.1.4:13", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 14, endpoint: "1.1.1.3:14", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.4", port: 14, endpoint: "1.1.1.4:14", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{ip: "2.2.2.1", port: 21, endpoint: "2.2.2.1:21", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "2.2.2.2", port: 21, endpoint: "2.2.2.2:21", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{ip: "2.2.2.1", port: 22, endpoint: "2.2.2.1:22", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 11, endpoint: "1.1.1.2:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p13", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 13, endpoint: "1.1.1.3:13", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.4", port: 13, endpoint: "1.1.1.4:13", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p14", v1.ProtocolUDP): {
				{ip: "1.1.1.3", port: 14, endpoint: "1.1.1.3:14", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.4", port: 14, endpoint: "1.1.1.4:14", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p21", v1.ProtocolUDP): {
				{ip: "2.2.2.1", port: 21, endpoint: "2.2.2.1:21", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "2.2.2.2", port: 21, endpoint: "2.2.2.2:21", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{ip: "2.2.2.1", port: 22, endpoint: "2.2.2.1:22", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: false,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 2,
			makeNSN("ns2", "ep2"): 1,
		},
		expectedChangedEndpoints: sets.New[types.NamespacedName](),
	}, {
		name: "add an EndpointSlice",
		previousEndpointSlices: []*discovery.EndpointSlice{
			nil,
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPortLocal),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
		expectedChangedEndpoints: sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "remove an EndpointSlice",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPortLocal),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			nil,
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedResult:                   map[ServicePortName][]*BaseEndpointInfo{},
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "add an IP and port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortsLocalNoLocal),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 11, endpoint: "1.1.1.2:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
		expectedChangedEndpoints: sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "remove an IP and port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortsLocalNoLocal),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 11, endpoint: "1.1.1.2:11", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 12, endpoint: "1.1.1.1:12", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "add a slice to an endpoint",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
			nil,
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsetsWithLocal_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsetsWithLocal_s2),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns1", "ep1"): 1,
		},
		expectedChangedEndpoints: sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "remove a slice from an endpoint",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, multipleSubsets_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, multipleSubsets_s2),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
			nil,
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "rename a port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortRenamed),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11-2", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "renumber a port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPort),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, namedPortRenumbered),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 22, endpoint: "1.1.1.1:22", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "complex add and remove",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, complexBefore1),
			nil,

			makeTestEndpointSlice("ns2", "ep2", 1, complexBefore2_s1),
			makeTestEndpointSlice("ns2", "ep2", 2, complexBefore2_s2),

			nil,
			nil,

			makeTestEndpointSlice("ns4", "ep4", 1, complexBefore4_s1),
			makeTestEndpointSlice("ns4", "ep4", 2, complexBefore4_s2),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, complexAfter1_s1),
			makeTestEndpointSlice("ns1", "ep1", 2, complexAfter1_s2),

			nil,
			nil,

			makeTestEndpointSlice("ns3", "ep3", 1, complexAfter3),
			nil,

			makeTestEndpointSlice("ns4", "ep4", 1, complexAfter4),
			nil,
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p22", v1.ProtocolUDP): {
				{ip: "2.2.2.22", port: 22, endpoint: "2.2.2.22:22", isLocal: true, ready: true, serving: true, terminating: false},
				{ip: "2.2.2.2", port: 22, endpoint: "2.2.2.2:22", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns2", "ep2", "p23", v1.ProtocolUDP): {
				{ip: "2.2.2.3", port: 23, endpoint: "2.2.2.3:23", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{ip: "4.4.4.4", port: 44, endpoint: "4.4.4.4:44", isLocal: true, ready: true, serving: true, terminating: false},
				{ip: "4.4.4.5", port: 44, endpoint: "4.4.4.5:44", isLocal: true, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p45", v1.ProtocolUDP): {
				{ip: "4.4.4.6", port: 45, endpoint: "4.4.4.6:45", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "p11", v1.ProtocolUDP): {
				{ip: "1.1.1.11", port: 11, endpoint: "1.1.1.11:11", isLocal: false, ready: true, serving: true, terminating: false},
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p12", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 12, endpoint: "1.1.1.2:12", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns1", "ep1", "p122", v1.ProtocolUDP): {
				{ip: "1.1.1.2", port: 122, endpoint: "1.1.1.2:122", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns3", "ep3", "p33", v1.ProtocolUDP): {
				{ip: "3.3.3.3", port: 33, endpoint: "3.3.3.3:33", isLocal: false, ready: true, serving: true, terminating: false},
			},
			makeServicePortName("ns4", "ep4", "p44", v1.ProtocolUDP): {
				{ip: "4.4.4.4", port: 44, endpoint: "4.4.4.4:44", isLocal: true, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints: map[types.NamespacedName]int{
			makeNSN("ns4", "ep4"): 1,
		},
		expectedChangedEndpoints: sets.New(makeNSN("ns1", "ep1"), makeNSN("ns2", "ep2"), makeNSN("ns3", "ep3"), makeNSN("ns4", "ep4")),
	}, {
		name: "change from 0 endpoint address to 1 unnamed port",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, emptyEndpoint),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPort),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "change from ready to terminating pod",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPortReady),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPortTerminating),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: true, serving: true, terminating: false},
			},
		},
		expectedResult: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: false, serving: true, terminating: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	}, {
		name: "change from terminating to empty pod",
		previousEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, unnamedPortTerminating),
		},
		currentEndpointSlices: []*discovery.EndpointSlice{
			makeTestEndpointSlice("ns1", "ep1", 1, emptyEndpoint),
		},
		previousEndpointsMap: map[ServicePortName][]*BaseEndpointInfo{
			makeServicePortName("ns1", "ep1", "", v1.ProtocolUDP): {
				{ip: "1.1.1.1", port: 11, endpoint: "1.1.1.1:11", isLocal: false, ready: false, serving: true, terminating: true},
			},
		},
		expectedConntrackCleanupRequired: true,
		expectedResult:                   map[ServicePortName][]*BaseEndpointInfo{},
		expectedLocalEndpoints:           map[types.NamespacedName]int{},
		expectedChangedEndpoints:         sets.New(makeNSN("ns1", "ep1")),
	},
	}

	for tci, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 用零值时间启动跟踪器：早于该时间的注解事件应被忽略，
			// 避免历史事件污染 triggerTime 断言。
			fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

			// 先灌入全部旧版 slice，确认初始映射与用例声明一致；
			// 否则后续观察到的差异可能来自初始状态而非本次更新。
			for i := range tc.previousEndpointSlices {
				if tc.previousEndpointSlices[i] != nil {
					fp.addEndpointSlice(tc.previousEndpointSlices[i])
				}
			}
			// 先消费一次更新，建立“旧状态”基线；
			// 后面的差异才能确定来自本次 slice 变更。
			fp.endpointsMap.Update(fp.endpointsChanges)
			compareEndpointsMapsStr(t, fp.endpointsMap, tc.previousEndpointsMap)

			// 随后按位置调用对应的增/删/改处理器，把状态推进到目标状态。
			// 两个切片必须按位置配对，否则无法判断每个位置是新增、删除还是更新。
			if len(tc.previousEndpointSlices) != len(tc.currentEndpointSlices) {
				t.Fatalf("[%d] different lengths of previous and current endpoints", tci)
				return
			}

			for i := range tc.previousEndpointSlices {
				prev, curr := tc.previousEndpointSlices[i], tc.currentEndpointSlices[i]
				switch {
				// 两侧都为 nil 表示这个位置没有 slice，跳过即可。
				case prev == nil && curr == nil:
					continue
				case prev == nil:
					fp.addEndpointSlice(curr)
				case curr == nil:
					fp.deleteEndpointSlice(prev)
				default:
					fp.updateEndpointSlice(prev, curr)
				}
			}

			result := fp.endpointsMap.Update(fp.endpointsChanges)
			newMap := fp.endpointsMap
			compareEndpointsMapsStr(t, newMap, tc.expectedResult)
			// 变更服务集合决定是否触发代理刷新，必须精确匹配，不能多报也不能漏报。
			if !result.UpdatedServices.Equal(tc.expectedChangedEndpoints) {
				t.Errorf("[%d] expected changed endpoints %q, got %q", tci, tc.expectedChangedEndpoints.UnsortedList(), result.UpdatedServices.UnsortedList())
			}
			// conntrack 清理是昂贵的副作用，端点变化时不应误报也不应漏报。
			if result.ConntrackCleanupRequired != tc.expectedConntrackCleanupRequired {
				t.Errorf("[%d] expected conntrackCleanupRequired to be %t, got %t", tci, tc.expectedConntrackCleanupRequired, result.ConntrackCleanupRequired)
			}
			// 本机就绪端点计数用于 topology aware routing，需单独校验。
			localReadyEndpoints := fp.endpointsMap.LocalReadyEndpoints()
			if !reflect.DeepEqual(localReadyEndpoints, tc.expectedLocalEndpoints) {
				t.Errorf("[%d] expected local ready endpoints %v, got %v", tci, tc.expectedLocalEndpoints, localReadyEndpoints)
			}
		})
	}
}

// TestLastChangeTriggerTime 验证 EndpointsMap.Update 能从带注解的 EndpointSlice 中提取
// 最后变更触发时间，并按服务分别记录时间列表。
// 覆盖仅新增、新增后更新、多服务独立记录、缺少注解、早于跟踪器启动的旧事件、
// 增删抵消以及删除后重新新增等情形。
func TestLastChangeTriggerTime(t *testing.T) {
	startTime := time.Date(2018, 01, 01, 0, 0, 0, 0, time.UTC)
	t_1 := startTime.Add(-time.Second)
	t0 := startTime.Add(time.Second)
	t1 := t0.Add(time.Second)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)

	// 构造带 LastChangeTriggerTime 注解的 slice，注解值按 RFC3339Nano 格式化，
	// 与生产代码中 EndpointSlice 的注解格式保持一致。
	createEndpoints := func(namespace, name string, triggerTime time.Time) *discovery.EndpointSlice {
		return &discovery.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
				Annotations: map[string]string{
					v1.EndpointsLastChangeTriggerTime: triggerTime.Format(time.RFC3339Nano),
				},
				Labels: map[string]string{
					discovery.LabelServiceName: name,
				},
			},
			AddressType: discovery.AddressTypeIPv4,
			Endpoints: []discovery.Endpoint{{
				Addresses: []string{"1.1.1.1"},
			}},
			Ports: []discovery.EndpointPort{{
				Name:     ptr.To("p11"),
				Port:     ptr.To[int32](11),
				Protocol: ptr.To(v1.ProtocolTCP),
			}},
		}
	}

	createName := func(namespace, name string) types.NamespacedName {
		return types.NamespacedName{Namespace: namespace, Name: name}
	}

	// 深拷贝后修改端口并刷新触发时间，模拟一次真实的端点更新；
	// 端口变化确保更新本身是可被检测到的。
	modifyEndpoints := func(slice *discovery.EndpointSlice, triggerTime time.Time) *discovery.EndpointSlice {
		e := slice.DeepCopy()
		(*e.Ports[0].Port)++
		e.Annotations[v1.EndpointsLastChangeTriggerTime] = triggerTime.Format(time.RFC3339Nano)
		return e
	}

	testCases := []struct {
		name     string
		scenario func(fp *FakeProxier)
		expected map[types.NamespacedName][]time.Time
	}{
		{
			name: "Single addEndpoints",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t0)
				fp.addEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{createName("ns", "ep1"): {t0}},
		},
		{
			name: "addEndpoints then updatedEndpoints",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t0)
				fp.addEndpointSlice(e)

				e1 := modifyEndpoints(e, t1)
				fp.updateEndpointSlice(e, e1)
			},
			expected: map[types.NamespacedName][]time.Time{createName("ns", "ep1"): {t0, t1}},
		},
		{
			name: "Add two endpoints then modify one",
			scenario: func(fp *FakeProxier) {
				e1 := createEndpoints("ns", "ep1", t1)
				fp.addEndpointSlice(e1)

				e2 := createEndpoints("ns", "ep2", t2)
				fp.addEndpointSlice(e2)

				e11 := modifyEndpoints(e1, t3)
				fp.updateEndpointSlice(e1, e11)
			},
			expected: map[types.NamespacedName][]time.Time{createName("ns", "ep1"): {t1, t3}, createName("ns", "ep2"): {t2}},
		},
		{
			name: "Endpoints without annotation set",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t1)
				delete(e.Annotations, v1.EndpointsLastChangeTriggerTime)
				fp.addEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{},
		},
		{
			name: "Endpoints create before tracker started",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t_1)
				fp.addEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{},
		},
		{
			name: "addEndpoints then deleteEndpoints",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t1)
				fp.addEndpointSlice(e)
				fp.deleteEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{},
		},
		{
			name: "add then delete then add again",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t1)
				fp.addEndpointSlice(e)
				fp.deleteEndpointSlice(e)
				e = modifyEndpoints(e, t2)
				fp.addEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{createName("ns", "ep1"): {t2}},
		},
		{
			name: "delete",
			scenario: func(fp *FakeProxier) {
				e := createEndpoints("ns", "ep1", t1)
				fp.deleteEndpointSlice(e)
			},
			expected: map[types.NamespacedName][]time.Time{},
		},
	}

	for _, tc := range testCases {
		// 用固定 startTime 启动，保证“早于启动时间”的触发时间能被稳定识别。
		fp := newFakeProxier(v1.IPv4Protocol, startTime)

		tc.scenario(fp)

		result := fp.endpointsMap.Update(fp.endpointsChanges)
		got := result.LastChangeTriggerTimes

		if !reflect.DeepEqual(got, tc.expected) {
			t.Errorf("%s: Invalid LastChangeTriggerTimes, expected: %v, got: %v",
				tc.name, tc.expected, result.LastChangeTriggerTimes)
		}
	}
}

// TestEndpointSliceUpdate 直接测试 EndpointsChangeTracker.EndpointSliceUpdate：
// 校验对空状态新增、重复新增、非法地址类型、部分重叠的新增/删除、就绪状态迁移等场景
// 的返回值（是否产生变更）以及 checkoutChanges 得到的前后映射。
func TestEndpointSliceUpdate(t *testing.T) {
	// 把地址类型改为 FQDN，用于验证 IPv4 跟踪器会忽略不匹配的 slice。
	fqdnSlice := generateEndpointSlice("svc1", "ns1", 2, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)})
	fqdnSlice.AddressType = discovery.AddressTypeFQDN

	testCases := map[string]struct {
		startingSlices         []*discovery.EndpointSlice
		endpointsChangeTracker *EndpointsChangeTracker
		namespacedName         types.NamespacedName
		paramEndpointSlice     *discovery.EndpointSlice
		paramRemoveSlice       bool
		expectedReturnVal      bool
		expectedCurrentChange  map[ServicePortName][]*BaseEndpointInfo
	}{
		// 从空状态开始：加入一个尚不存在的简单 slice 应产生变更。
		"add a simple slice that doesn't already exist": {
			startingSlices:         []*discovery.EndpointSlice{},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 重复加入已有 slice 属于无变化，current 变更应为 nil。
		"add the same slice that already exists": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      false,
			expectedCurrentChange:  nil,
		},
		// IPv4 跟踪器应忽略 FQDN 类型的 slice，确保只处理有效地址类型。
		"add an FQDN slice (invalid address type)": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     fqdnSlice,
			paramRemoveSlice:       false,
			expectedReturnVal:      false,
			expectedCurrentChange:  nil,
		},
		// 向已有状态加入与现有 slice 部分重叠的新 slice，验证端点按 IP 合并。
		"add a slice that overlaps with existing state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 80, endpoint: "10.0.1.5:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: true, ready: true, serving: true, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 443, endpoint: "10.0.1.4:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 443, endpoint: "10.0.1.5:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 443, endpoint: "10.0.2.1:443", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 443, endpoint: "10.0.2.2:443", isLocal: true, ready: true, serving: true, terminating: false},
				},
			},
		},
		// slice 与端口都部分重叠时，仅在对应端口上补齐端点，其余端口保持原值。
		"add a slice that overlaps with existing state and partial ports": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSliceWithOffset("svc1", "ns1", 3, 1, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 80, endpoint: "10.0.1.5:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: true, ready: true, serving: true, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 443, endpoint: "10.0.2.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 443, endpoint: "10.0.2.2:443", isLocal: true, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 删除与现有状态部分重叠的 slice：只移除该 slice 贡献的端点，保留其他 slice 的端点。
		"remove a slice that overlaps with existing state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       true,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: true, ready: true, serving: true, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.2.1", port: 443, endpoint: "10.0.2.1:443", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 443, endpoint: "10.0.2.2:443", isLocal: true, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 删除一个根本不存在的 slice 应是无操作，不产生变更。
		"remove a slice that doesn't even exist in current state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 3, 5, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       true,
			expectedReturnVal:      false,
			expectedCurrentChange:  nil,
		},
		// 从全部就绪迁移到全部未就绪，验证 ready/serving 标志变化会被报告。
		"transition all endpoints to unready state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 3, 1, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: true, ready: false, serving: false, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: true, ready: false, serving: false, terminating: false},
				},
			},
		},
		// 从全部未就绪迁移到全部就绪，验证状态恢复同样被报告。
		"transition all endpoints to ready state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 2, 1, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 2, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 从部分就绪迁移到更多就绪，验证只有状态变化的端点被更新。
		"transition some endpoints to ready state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 2, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 2, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 3, 3, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: true, ready: false, serving: false, terminating: false},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 443, endpoint: "10.0.2.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 443, endpoint: "10.0.2.2:443", isLocal: true, ready: false, serving: false, terminating: false},
				},
			},
		},
		// 从部分就绪迁移到部分终止，验证 terminating 与 ready/serving 标志的组合正确。
		"transition some endpoints to terminating state": {
			startingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 2, 2, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
				generateEndpointSlice("svc1", "ns1", 2, 2, 2, 2, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "host1", nil, nil),
			namespacedName:         types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			paramEndpointSlice:     generateEndpointSlice("svc1", "ns1", 1, 3, 3, 2, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			paramRemoveSlice:       false,
			expectedReturnVal:      true,
			expectedCurrentChange: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: false, serving: true, terminating: true},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: true, ready: false, serving: false, terminating: true},
				},
				makeServicePortName("ns1", "svc1", "port-1", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", isLocal: true, ready: false, serving: true, terminating: true},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", isLocal: true, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 443, endpoint: "10.0.2.1:443", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 443, endpoint: "10.0.2.2:443", isLocal: true, ready: false, serving: false, terminating: true},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			// 先把起始 slice 提升为已应用状态，确保之前的操作不会混入本次变更。
			initializeCache(tc.endpointsChangeTracker.endpointSliceCache, tc.startingSlices)

			// 被测调用：返回值表示这次更新是否真的改变了跟踪器状态。
			got := tc.endpointsChangeTracker.EndpointSliceUpdate(tc.paramEndpointSlice, tc.paramRemoveSlice)
			if !reflect.DeepEqual(got, tc.expectedReturnVal) {
				t.Errorf("EndpointSliceUpdate return value got: %v, want %v", got, tc.expectedReturnVal)
			}

			// 取出由 pendingSlices 产生的变更，校验其 previous/current 映射。
			changes := tc.endpointsChangeTracker.checkoutChanges()
			if tc.expectedCurrentChange == nil {
				if len(changes) != 0 {
					t.Errorf("Expected %s to have no changes", tc.namespacedName)
				}
			} else {
				if _, exists := changes[tc.namespacedName]; !exists {
					t.Fatalf("Expected %s to have changes", tc.namespacedName)
				}
				compareEndpointsMapsStr(t, changes[tc.namespacedName].current, tc.expectedCurrentChange)
			}
		})
	}
}

// TestCheckoutChanges 验证 checkoutChanges 的行为：
// 先应用一批 slice 并取出（清空）变更，再提交待处理 slice，
// 确认第二次取出时只得到以已应用状态为 previous、新提交状态为 current 的变更记录。
func TestCheckoutChanges(t *testing.T) {
	svcPortName0 := ServicePortName{types.NamespacedName{Namespace: "ns1", Name: "svc1"}, "port-0", v1.ProtocolTCP}
	svcPortName1 := ServicePortName{types.NamespacedName{Namespace: "ns1", Name: "svc1"}, "port-1", v1.ProtocolTCP}

	testCases := map[string]struct {
		endpointsChangeTracker *EndpointsChangeTracker
		expectedChanges        []*endpointsChange
		items                  map[types.NamespacedName]*endpointsChange
		appliedSlices          []*discovery.EndpointSlice
		pendingSlices          []*discovery.EndpointSlice
	}{
		"empty slices": {
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "", nil, nil),
			expectedChanges:        []*endpointsChange{},
			appliedSlices:          []*discovery.EndpointSlice{},
			pendingSlices:          []*discovery.EndpointSlice{},
		},
		"adding initial slice": {
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "", nil, nil),
			expectedChanges: []*endpointsChange{{
				previous: EndpointsMap{},
				current: EndpointsMap{
					svcPortName0: []Endpoint{
						&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", ready: false, serving: true, terminating: true},
						&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", ready: false, serving: false, terminating: false},
					},
				},
			}},
			appliedSlices: []*discovery.EndpointSlice{},
			pendingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 3, 2, []string{"host1"}, []*int32{ptr.To[int32](80)}),
			},
		},
		"removing port in update": {
			endpointsChangeTracker: NewEndpointsChangeTracker(v1.IPv4Protocol, "", nil, nil),
			expectedChanges: []*endpointsChange{{
				previous: EndpointsMap{
					svcPortName0: []Endpoint{
						&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", ready: false, serving: false, terminating: false},
					},
					svcPortName1: []Endpoint{
						&BaseEndpointInfo{ip: "10.0.1.1", port: 443, endpoint: "10.0.1.1:443", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.2", port: 443, endpoint: "10.0.1.2:443", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.3", port: 443, endpoint: "10.0.1.3:443", ready: false, serving: false, terminating: false},
					},
				},
				current: EndpointsMap{
					svcPortName0: []Endpoint{
						&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", ready: true, serving: true, terminating: false},
						&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", ready: false, serving: false, terminating: false},
					},
				},
			}},
			appliedSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 3, 999, []string{"host1"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			pendingSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 3, 999, []string{"host1"}, []*int32{ptr.To[int32](80)}),
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, slice := range tc.appliedSlices {
				tc.endpointsChangeTracker.EndpointSliceUpdate(slice, false)
			}
			// 先取出并清空上一批 slice 产生的变更，
			// 这样下一次 checkout 只包含 pendingSlices 引入的新变化。
			tc.endpointsChangeTracker.checkoutChanges()
			for _, slice := range tc.pendingSlices {
				tc.endpointsChangeTracker.EndpointSliceUpdate(slice, false)
			}
			changes := tc.endpointsChangeTracker.checkoutChanges()

			if len(tc.expectedChanges) != len(changes) {
				t.Fatalf("Expected %d changes, got %d", len(tc.expectedChanges), len(changes))
			}

			for _, change := range changes {
				// 所有用例最多只有一个服务的变更，因此只要进入循环，
				// 用 expectedChanges[0] 作为唯一期望值即可。
				expectedChange := tc.expectedChanges[0]

				if !reflect.DeepEqual(change.previous, expectedChange.previous) {
					t.Errorf("Expected change.previous: %+v, got: %+v", expectedChange.previous, change.previous)
				}

				if !reflect.DeepEqual(change.current, expectedChange.current) {
					t.Errorf("Expected change.current: %+v, got: %+v", expectedChange.current, change.current)
				}
			}
		})
	}
}

// 测试辅助函数。

// compareEndpointsMapsStr 逐服务、逐端口、逐端点比较实际映射与期望映射。
// 端点比较聚焦 endpoint、isLocal、ready、serving、terminating 这些测试关心的字段；
// 失败时输出足够详细的上下文，便于定位是数量、顺序还是标志位不符。
func compareEndpointsMapsStr(t *testing.T, newMap EndpointsMap, expected map[ServicePortName][]*BaseEndpointInfo) {
	t.Helper()
	if len(newMap) != len(expected) {
		t.Fatalf("expected %d results, got %d: %v", len(expected), len(newMap), newMap)
	}
	endpointEqual := func(a, b *BaseEndpointInfo) bool {
		return a.endpoint == b.endpoint && a.isLocal == b.isLocal && a.ready == b.ready && a.serving == b.serving && a.terminating == b.terminating
	}
	for x := range expected {
		if len(newMap[x]) != len(expected[x]) {
			t.Logf("Endpoints %+v", newMap[x])
			t.Fatalf("expected %d endpoints for %v, got %d", len(expected[x]), x, len(newMap[x]))
		} else {
			for i := range expected[x] {
				newEp, ok := newMap[x][i].(*BaseEndpointInfo)
				if !ok {
					t.Fatalf("Failed to cast endpointInfo")
				}
				if !endpointEqual(newEp, expected[x][i]) {
					t.Fatalf("expected new[%v][%d] to be %v, got %v "+
						"(IsLocal expected %v, got %v) (Ready expected %v, got %v) (Serving expected %v, got %v) (Terminating expected %v got %v)",
						x, i, expected[x][i], newEp, expected[x][i].isLocal, newEp.isLocal, expected[x][i].ready, newEp.ready,
						expected[x][i].serving, newEp.serving, expected[x][i].terminating, newEp.terminating)
				}
			}
		}
	}
}

// initializeCache 用给定 slice 预填缓存，然后把 pending 提升为 applied 并清空 pending，
// 从而构造出“这些 slice 已经被消费过”的状态，供后续增量更新测试使用。
func initializeCache(endpointSliceCache *EndpointSliceCache, endpointSlices []*discovery.EndpointSlice) {
	for _, endpointSlice := range endpointSlices {
		endpointSliceCache.updatePending(endpointSlice, false)
	}

	for _, tracker := range endpointSliceCache.trackerByServiceMap {
		tracker.applied = tracker.pending
		tracker.pending = endpointSliceDataByName{}
	}
}
