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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// TestEndpointsMapFromESC 验证从 EndpointSlice 生成 EndpointsMap 的行为，
// 覆盖多端口、切片重叠、重复 IP、就绪/终止状态以及跨命名空间过滤等场景。
func TestEndpointsMapFromESC(t *testing.T) {
	testCases := map[string]struct {
		endpointSlices []*discovery.EndpointSlice
		hostname       string
		namespacedName types.NamespacedName
		expectedMap    map[ServicePortName][]*BaseEndpointInfo
	}{
		"1 slice, 2 hosts, ports 80,443": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80), ptr.To[int32](443)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
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
		"2 slices, same port": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns1", 2, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.1", port: 80, endpoint: "10.0.2.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.2", port: 80, endpoint: "10.0.2.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.2.3", port: 80, endpoint: "10.0.2.3:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 两个切片存在部分重叠端点时，结果应为两者端点的并集。
		// 重叠部分只保留一份。
		"2 overlapping slices, same port": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns1", 1, 4, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 两个切片的端点完全重叠，但第一个切片的未就绪端点更多。
		// 只要某个端点被标记为就绪就应加入 EndpointsMap，
		// 即使另一个匹配端点的 conditions.Ready 不为 true。
		// 该场景用于验证去重时“就绪优先”的语义。
		"2 slices, overlapping endpoints, some endpoints unready in 1 or both": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 10, 3, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns1", 1, 10, 6, 999, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.10", port: 80, endpoint: "10.0.1.10:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 80, endpoint: "10.0.1.5:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.6", port: 80, endpoint: "10.0.1.6:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.7", port: 80, endpoint: "10.0.1.7:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.8", port: 80, endpoint: "10.0.1.8:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.9", port: 80, endpoint: "10.0.1.9:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 两个切片端点完全重叠，其中部分未就绪、部分正在终止。
		"2 slices, overlapping endpoints, some endpoints unready and some endpoints terminating": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 10, 3, 5, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns1", 1, 10, 6, 5, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.10", port: 80, endpoint: "10.0.1.10:80", isLocal: false, ready: false, serving: true, terminating: true},
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 80, endpoint: "10.0.1.5:80", isLocal: false, ready: false, serving: true, terminating: true},
					&BaseEndpointInfo{ip: "10.0.1.6", port: 80, endpoint: "10.0.1.6:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.7", port: 80, endpoint: "10.0.1.7:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.8", port: 80, endpoint: "10.0.1.8:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.9", port: 80, endpoint: "10.0.1.9:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		"2 slices, overlapping endpoints, all unready": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 10, 1, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns1", 1, 10, 1, 999, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.10", port: 80, endpoint: "10.0.1.10:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.4", port: 80, endpoint: "10.0.1.4:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.5", port: 80, endpoint: "10.0.1.5:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.6", port: 80, endpoint: "10.0.1.6:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.7", port: 80, endpoint: "10.0.1.7:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.8", port: 80, endpoint: "10.0.1.8:80", isLocal: false, ready: false, serving: false, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.9", port: 80, endpoint: "10.0.1.9:80", isLocal: false, ready: false, serving: false, terminating: false},
				},
			},
		},
		"3 slices with different services and namespaces": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc2", "ns1", 2, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
				generateEndpointSlice("svc1", "ns2", 3, 3, 999, 999, []string{}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.3", port: 80, endpoint: "10.0.1.3:80", isLocal: false, ready: true, serving: true, terminating: false},
				},
			},
		},
		// 确保 nil 端口值不会导致异常。将来它可能表示“所有端口”，
		// 但该行为目前尚未实现。
		// 这里期望为空映射，避免实现变更时静默通过。
		"Nil port should not break anything": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{nil}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{},
		},
		// 确保 IP 相同但端口不同的端点都能被正确返回。
		"Different endpoints with duplicate IPs should not be filtered": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSliceWithOffset("svc1", "ns1", 1, 1, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80)}),
				generateEndpointSliceWithOffset("svc1", "ns1", 2, 1, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](8080)}),
			},
			expectedMap: map[ServicePortName][]*BaseEndpointInfo{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					&BaseEndpointInfo{ip: "10.0.1.1", port: 80, endpoint: "10.0.1.1:80", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.1", port: 8080, endpoint: "10.0.1.1:8080", isLocal: false, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 80, endpoint: "10.0.1.2:80", isLocal: true, ready: true, serving: true, terminating: false},
					&BaseEndpointInfo{ip: "10.0.1.2", port: 8080, endpoint: "10.0.1.2:8080", isLocal: true, ready: true, serving: true, terminating: false},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			esCache := NewEndpointSliceCache(tc.hostname, nil)

			// 先建立深拷贝基线，稍后断言缓存没有改动输入的切片。
			cmc := newCacheMutationCheck(tc.endpointSlices)
			for _, endpointSlice := range tc.endpointSlices {
				esCache.updatePending(endpointSlice, false)
			}

			// 从 pending 快照生成映射，避免已 checkout 的状态干扰断言。
			compareEndpointsMapsStr(t, esCache.getEndpointsMap(tc.namespacedName, esCache.trackerByServiceMap[tc.namespacedName].pending), tc.expectedMap)
			cmc.Check(t)
		})
	}
}

// TestEndpointInfoByServicePort 验证端点按服务端口分组去重的逻辑，
// 尤其检查重复 IP 与重复端点时优先选择非终止端点。
func TestEndpointInfoByServicePort(t *testing.T) {
	testCases := map[string]struct {
		namespacedName types.NamespacedName
		endpointSlices []*discovery.EndpointSlice
		hostname       string
		expectedMap    spToEndpointMap
	}{
		"simple use case with 3 endpoints": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSlice("svc1", "ns1", 1, 3, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80)}),
			},
			expectedMap: spToEndpointMap{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					"10.0.1.1:80": &BaseEndpointInfo{
						ip:          "10.0.1.1",
						port:        80,
						endpoint:    "10.0.1.1:80",
						isLocal:     false,
						ready:       true,
						serving:     true,
						terminating: false,
					},
					"10.0.1.2:80": &BaseEndpointInfo{
						ip:          "10.0.1.2",
						port:        80,
						endpoint:    "10.0.1.2:80",
						isLocal:     true,
						ready:       true,
						serving:     true,
						terminating: false,
					},
					"10.0.1.3:80": &BaseEndpointInfo{
						ip:          "10.0.1.3",
						port:        80,
						endpoint:    "10.0.1.3:80",
						isLocal:     false,
						ready:       true,
						serving:     true,
						terminating: false,
					},
				},
			},
		},
		"4 different slices with duplicate IPs": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				generateEndpointSliceWithOffset("svc1", "ns1", 1, 1, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](80)}),
				generateEndpointSliceWithOffset("svc1", "ns1", 2, 1, 2, 999, 999, []string{"host1", "host2"}, []*int32{ptr.To[int32](8080)}),
			},
			expectedMap: spToEndpointMap{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					"10.0.1.1:80": &BaseEndpointInfo{
						ip:          "10.0.1.1",
						port:        80,
						endpoint:    "10.0.1.1:80",
						isLocal:     false,
						ready:       true,
						serving:     true,
						terminating: false,
					},
					"10.0.1.2:80": &BaseEndpointInfo{
						ip:          "10.0.1.2",
						port:        80,
						endpoint:    "10.0.1.2:80",
						isLocal:     true,
						ready:       true,
						serving:     true,
						terminating: false,
					},
					"10.0.1.1:8080": &BaseEndpointInfo{
						ip:          "10.0.1.1",
						port:        8080,
						endpoint:    "10.0.1.1:8080",
						isLocal:     false,
						ready:       true,
						serving:     true,
						terminating: false,
					},
					"10.0.1.2:8080": &BaseEndpointInfo{
						ip:          "10.0.1.2",
						port:        8080,
						endpoint:    "10.0.1.2:8080",
						isLocal:     true,
						ready:       true,
						serving:     true,
						terminating: false,
					},
				},
			},
		},
		"with duplicate Endpoints, prefer non-terminating": {
			namespacedName: types.NamespacedName{Name: "svc1", Namespace: "ns1"},
			hostname:       "host1",
			endpointSlices: []*discovery.EndpointSlice{
				func() *discovery.EndpointSlice {
					es := generateEndpointSliceWithOffset("svc1", "ns1", 1, 1, 1, 999, 999, []string{"host1"}, []*int32{ptr.To[int32](80)})

					// 复制同一个端点，构造仅有条件不同的重复项
					ep1 := es.Endpoints[0]
					ep1.Conditions.Ready = ptr.To(false)
					ep1.Conditions.Serving = ptr.To(true)
					ep1.Conditions.Terminating = ptr.To(true)

					ep2 := es.Endpoints[0]
					ep2.Conditions.Ready = ptr.To(true)
					ep2.Conditions.Serving = ptr.To(true)
					ep2.Conditions.Terminating = ptr.To(false)

					ep3 := es.Endpoints[0]
					ep3.Conditions.Ready = ptr.To(false)
					ep3.Conditions.Serving = ptr.To(false)
					ep3.Conditions.Terminating = ptr.To(true)

					// ep2 是非终止端点，应被优先选中
					es.Endpoints = []discovery.Endpoint{ep1, ep2, ep3}
					return es
				}(),
			},
			expectedMap: spToEndpointMap{
				makeServicePortName("ns1", "svc1", "port-0", v1.ProtocolTCP): {
					"10.0.1.1:80": &BaseEndpointInfo{
						ip:          "10.0.1.1",
						port:        80,
						endpoint:    "10.0.1.1:80",
						isLocal:     true,
						ready:       true,
						serving:     true,
						terminating: false,
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			esCache := NewEndpointSliceCache(tc.hostname, nil)

			for _, endpointSlice := range tc.endpointSlices {
				esCache.updatePending(endpointSlice, false)
			}

			// 断言按 IP:端口 去重后，重复端点中保留了非终止的那一个。
			got := esCache.endpointInfoByServicePort(tc.namespacedName, esCache.trackerByServiceMap[tc.namespacedName].pending)
			if !reflect.DeepEqual(got, tc.expectedMap) {
				t.Errorf("endpointInfoByServicePort does not match. Want: %+v, Got: %+v", tc.expectedMap, got)
			}
		})
	}
}

// TestEsDataChanged 验证 esDataChanged 能识别端口与端点的内容及顺序变化，
// 同时确认顺序未变时不会产生误判。
func TestEsDataChanged(t *testing.T) {
	p80 := int32(80)
	p443 := int32(443)
	port80 := discovery.EndpointPort{Port: &p80, Name: ptr.To("http"), Protocol: ptr.To(v1.ProtocolTCP)}
	port443 := discovery.EndpointPort{Port: &p443, Name: ptr.To("https"), Protocol: ptr.To(v1.ProtocolTCP)}
	endpoint1 := discovery.Endpoint{Addresses: []string{"10.0.1.0"}}
	endpoint2 := discovery.Endpoint{Addresses: []string{"10.0.1.1"}}

	objMeta := metav1.ObjectMeta{
		Name:      "foo",
		Namespace: "bar",
		Labels:    map[string]string{discovery.LabelServiceName: "svc1"},
	}

	testCases := map[string]struct {
		cache         *EndpointSliceCache
		initialSlice  *discovery.EndpointSlice
		updatedSlice  *discovery.EndpointSlice
		expectChanged bool
	}{
		"identical slices, ports only": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port80},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port80},
			},
			expectChanged: false,
		},
		"identical slices, ports out of order": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443, port80},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port80, port443},
			},
			expectChanged: true,
		},
		"port removed": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443, port80},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
			},
			expectChanged: true,
		},
		"port added": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443, port80},
			},
			expectChanged: true,
		},
		"identical with endpoints": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint1, endpoint2},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint1, endpoint2},
			},
			expectChanged: false,
		},
		"identical with endpoints out of order": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint1, endpoint2},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint2, endpoint1},
			},
			expectChanged: true,
		},
		"identical with endpoint added": {
			cache: NewEndpointSliceCache("", nil),
			initialSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint1},
			},
			updatedSlice: &discovery.EndpointSlice{
				ObjectMeta: objMeta,
				Ports:      []discovery.EndpointPort{port443},
				Endpoints:  []discovery.Endpoint{endpoint2, endpoint1},
			},
			expectChanged: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			cmc := newCacheMutationCheck([]*discovery.EndpointSlice{tc.initialSlice})

			// 先把初始切片写入缓存并提交，使其进入可比较的基线状态。
			if tc.initialSlice != nil {
				tc.cache.updatePending(tc.initialSlice, false)
				tc.cache.checkoutChanges()
			}

			serviceKey, sliceKey, err := endpointSliceCacheKeys(tc.updatedSlice)
			if err != nil {
				t.Fatalf("Expected no error calling endpointSliceCacheKeys(): %v", err)
			}

			// 用新切片的包装对象与缓存基线比较，且不修改缓存中的原始对象。
			esData := &endpointSliceData{tc.updatedSlice, false}
			changed := tc.cache.esDataChanged(serviceKey, sliceKey, esData)

			if tc.expectChanged != changed {
				t.Errorf("Expected esDataChanged() to return %t, got %t", tc.expectChanged, changed)
			}

			cmc.Check(t)
		})
	}
}

// generateEndpointSliceWithOffset 生成测试用 EndpointSlice：
// 端点 IP 的第三段由 offset 决定，就绪与终止状态按给定模数周期性设置，
// hosts 则用于给端点分配节点名。
func generateEndpointSliceWithOffset(serviceName, namespace string, sliceNum, offset, numEndpoints, unreadyMod int, terminatingMod int, hosts []string, portNums []*int32) *discovery.EndpointSlice {
	endpointSlice := &discovery.EndpointSlice{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("%s-%d", serviceName, sliceNum),
			Namespace: namespace,
			Labels:    map[string]string{discovery.LabelServiceName: serviceName},
		},
		Ports:       []discovery.EndpointPort{},
		AddressType: discovery.AddressTypeIPv4,
		Endpoints:   []discovery.Endpoint{},
	}

	for i, portNum := range portNums {
		endpointSlice.Ports = append(endpointSlice.Ports, discovery.EndpointPort{
			Name:     ptr.To(fmt.Sprintf("port-%d", i)),
			Port:     portNum,
			Protocol: ptr.To(v1.ProtocolTCP),
		})
	}

	for i := 1; i <= numEndpoints; i++ {
		readyCondition := i%unreadyMod != 0
		terminatingCondition := i%terminatingMod == 0

		ready := ptr.To(readyCondition && !terminatingCondition)
		serving := ptr.To(readyCondition)
		terminating := ptr.To(terminatingCondition)

		endpoint := discovery.Endpoint{
			Addresses: []string{fmt.Sprintf("10.0.%d.%d", offset, i)},
			Conditions: discovery.EndpointConditions{
				Ready:       ready,
				Serving:     serving,
				Terminating: terminating,
			},
		}

		if len(hosts) > 0 {
			hostname := hosts[i%len(hosts)]
			endpoint.NodeName = &hostname
		}

		endpointSlice.Endpoints = append(endpointSlice.Endpoints, endpoint)
	}

	return endpointSlice
}

// generateEndpointSlice 是 generateEndpointSliceWithOffset 的便捷封装，
// 默认用切片编号同时作为 IP 段的偏移量。
func generateEndpointSlice(serviceName, namespace string, sliceNum, numEndpoints, unreadyMod int, terminatingMod int, hosts []string, portNums []*int32) *discovery.EndpointSlice {
	return generateEndpointSliceWithOffset(serviceName, namespace, sliceNum, sliceNum, numEndpoints, unreadyMod, terminatingMod, hosts, portNums)
}

// cacheMutationCheck 用于确保缓存中的对象在整个测试过程中未被修改。
// 它为每个对象保存一份深拷贝作为比较基线。
type cacheMutationCheck struct {
	objects []cacheObject
}

// cacheObject 同时保存原对象及其深拷贝，
// 以便检测原对象是否被意外修改。
type cacheObject struct {
	original runtime.Object
	deepCopy runtime.Object
}

// newCacheMutationCheck 用一组 EndpointSlice 初始化 cacheMutationCheck。
func newCacheMutationCheck(endpointSlices []*discovery.EndpointSlice) cacheMutationCheck {
	cmc := cacheMutationCheck{}
	for _, endpointSlice := range endpointSlices {
		cmc.Add(endpointSlice)
	}
	return cmc
}

// Add 把 runtime.Object 及其深拷贝一并登记到 cacheMutationCheck 中。
// 深拷贝必须在对象尚未被修改前创建。
func (cmc *cacheMutationCheck) Add(o runtime.Object) {
	cmc.objects = append(cmc.objects, cacheObject{
		original: o,
		deepCopy: o.DeepCopyObject(),
	})
}

// Check 断言登记的所有对象都未被修改。
func (cmc *cacheMutationCheck) Check(t *testing.T) {
	for _, o := range cmc.objects {
		if !reflect.DeepEqual(o.original, o.deepCopy) {
			// 缓存对象不可原地修改，任何变更都必须先深拷贝。
			// 否则共享引用的调用方会看到意外的数据变化。
			t.Errorf("Cached object was unexpectedly mutated. Original: %+v, Mutated: %+v", o.deepCopy, o.original)
		}
	}
}

// TestEndpointSliceCacheClearedCorrectly 验证删除全部 EndpointSlice 后，
// 缓存中的服务跟踪条目会被彻底清理。
func TestEndpointSliceCacheClearedCorrectly(t *testing.T) {
	// 该初始化函数给切片填入一个带节点名的端点和一个 UDP 端口。
	initEndpointsPorts := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("n1"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}

	testCases := []struct {
		name               string
		currEndpointSlices []*discovery.EndpointSlice
	}{
		{
			name: "one endpoint slice",
			currEndpointSlices: []*discovery.EndpointSlice{
				makeTestEndpointSlice("ns1", "ep1", 1, initEndpointsPorts),
			},
		},
		{
			name: "two endpoint slices, same namespace",
			currEndpointSlices: []*discovery.EndpointSlice{
				makeTestEndpointSlice("ns1", "ep1", 1, initEndpointsPorts),
				makeTestEndpointSlice("ns1", "ep2", 2, initEndpointsPorts),
			},
		},
		{
			name: "two endpoint slices, same service",
			currEndpointSlices: []*discovery.EndpointSlice{
				makeTestEndpointSlice("ns1", "ep1", 1, initEndpointsPorts),
				makeTestEndpointSlice("ns1", "ep1", 2, initEndpointsPorts),
			},
		},
		{
			name: "two endpoint slices, different namespace",
			currEndpointSlices: []*discovery.EndpointSlice{
				makeTestEndpointSlice("ns1", "ep1", 1, initEndpointsPorts),
				makeTestEndpointSlice("ns2", "ep1", 2, initEndpointsPorts),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 走完“添加并提交、再删除并提交”的完整流程后再检查残留。
			fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

			for _, epSlice := range tc.currEndpointSlices {
				fp.addEndpointSlice(epSlice)
			}
			fp.endpointsMap.Update(fp.endpointsChanges)

			for _, epSlice := range tc.currEndpointSlices {
				fp.deleteEndpointSlice(epSlice)
			}
			fp.endpointsMap.Update(fp.endpointsChanges)

			// 断言服务跟踪条目已被彻底清除，避免残留影响后续同步。
			if len(fp.endpointsChanges.endpointSliceCache.trackerByServiceMap) != 0 {
				t.Errorf("expected: endpointSliceCache does not have any entries, got: %v", fp.endpointsChanges.endpointSliceCache.trackerByServiceMap)
			}
		})
	}
}

// TestSameServiceEndpointSliceCacheClearedCorrectly 验证同一服务下有多个切片时，
// 只删除其中一个不会把整个服务的缓存条目清掉。
func TestSameServiceEndpointSliceCacheClearedCorrectly(t *testing.T) {
	// 同样给切片填入完整数据，确保删除一个切片后该服务仍有端点。
	initEndpointsPorts := func(eps *discovery.EndpointSlice) {
		eps.Endpoints = []discovery.Endpoint{{
			Addresses: []string{"1.1.1.1"},
			NodeName:  ptr.To(testHostname),
		}}
		eps.Ports = []discovery.EndpointPort{{
			Name:     ptr.To("n1"),
			Port:     ptr.To[int32](11),
			Protocol: ptr.To(v1.ProtocolUDP),
		}}
	}

	currEndpointSlices := []*discovery.EndpointSlice{
		makeTestEndpointSlice("ns1", "svc1", 1, initEndpointsPorts),
		makeTestEndpointSlice("ns1", "svc1", 2, initEndpointsPorts),
	}

	fp := newFakeProxier(v1.IPv4Protocol, time.Time{})

	for _, epSlice := range currEndpointSlices {
		fp.addEndpointSlice(epSlice)
	}
	fp.endpointsMap.Update(fp.endpointsChanges)

	// 只删除第一个切片，服务本身仍然存在。
	fp.deleteEndpointSlice(currEndpointSlices[0])

	fp.endpointsMap.Update(fp.endpointsChanges)

	// 期望仍保留该服务的一个条目。
	if len(fp.endpointsChanges.endpointSliceCache.trackerByServiceMap) != 1 {
		t.Errorf("expected: endpointSliceCache to have one entries, got: %v", fp.endpointsChanges.endpointSliceCache.trackerByServiceMap)
	}
}
