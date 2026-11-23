/*
Copyright 2021 The Kubernetes Authors.

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

package topologycache

import (
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	v1 "k8s.io/api/core/v1"
	discovery "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// TestAddHints 验证 TopologyCache.AddHints 在不同节点 CPU 比例和 SliceInfo 输入下，
// 能否正确地为待创建/更新的 EndpointSlice 写入拓扑感知 hint，并产生相应的事件。
// 构造的输入覆盖无节点信息、端点数与 zone 配额不匹配、就绪/未就绪/状态未知端点混合，
// 以及跨三个 zone 且同时存在 ToCreate 与 ToUpdate 的场景；
// 断言返回的 slice 与事件符合预期，且缓存内按地址类型记录的每 zone 端点数正确。
func TestAddHints(t *testing.T) {
	// 表驱动用例：给出 zone 的 CPU 比例、待处理的 SliceInfo 以及预期的 slice/事件/缓存状态。
	testCases := []struct {
		// name: 测试用例名称
		// cpuRatiosByZone: 注入缓存的各 zone CPU 占比，nil 表示节点信息不足
		// sliceInfo: 待 AddHints 处理的 SliceInfo（含 ServiceKey、地址类型及三组 slice）
		// expectedEndpointsByAddrType: 期望缓存中按地址类型统计的每 zone 端点数，nil 表示不应有记录
		// expectedSlicesToCreate: 期望返回的待创建 slice（其端点应带正确 hint）
		// expectedSlicesToUpdate: 期望返回的待更新 slice（其端点应带正确 hint）
		// expectedEvents: 期望产生的事件列表
		name                        string
		cpuRatiosByZone             map[string]float64
		sliceInfo                   *SliceInfo
		expectedEndpointsByAddrType map[discovery.AddressType]EndpointZoneInfo
		expectedSlicesToCreate      []*discovery.EndpointSlice
		expectedSlicesToUpdate      []*discovery.EndpointSlice
		expectedEvents              []*EventBuilder
	}{{
		// 空输入：无 zone 比例、SliceInfo 无任何 slice，期望产生节点信息不足的告警事件且缓存无端点记录。
		name:            "empty",
		cpuRatiosByZone: nil,
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
		},
		expectedEndpointsByAddrType: nil,
		expectedSlicesToCreate:      []*discovery.EndpointSlice{},
		expectedSlicesToUpdate:      []*discovery.EndpointSlice{},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeWarning,
				Reason:    "TopologyAwareHintsDisabled",
				Message:   InsufficientNodeInfo,
			},
		},
	}, {
		// 有 slice 待创建但无 zone 比例：不应添加 hint，仍报节点信息不足告警。
		name:            "slice to create, no zone ratios",
		cpuRatiosByZone: nil,
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
			ToCreate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}},
		},
		expectedEndpointsByAddrType: nil,
		expectedSlicesToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		expectedSlicesToUpdate: []*discovery.EndpointSlice{},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeWarning,
				Reason:    "TopologyAwareHintsDisabled",
				Message:   InsufficientNodeInfo,
			},
		},
	}, {
		// 两个 Ready 端点分布于 zone-a/b，但三个 zone 的比例要求至少 3 个端点：禁用 hint 并报端点数不足告警。
		name: "slice to create with 2 endpoints, zone ratios require 3",
		cpuRatiosByZone: map[string]float64{
			"zone-a": 0.3,
			"zone-b": 0.4,
			"zone-c": 0.3,
		},
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
			ToCreate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.2.4"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}},
		},
		expectedEndpointsByAddrType: nil,
		expectedSlicesToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.4"},
				Zone:       ptr.To("zone-b"),
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		expectedSlicesToUpdate: []*discovery.EndpointSlice{},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeWarning,
				Reason:    "TopologyAwareHintsDisabled",
				Message:   InsufficientNumberOfEndpoints,
			},
		},
	}, {
		// 两个 Ready 端点各自所在 zone 的 hint 分配：zone-a/b 各计 1，端点被写入本 zone 的 hint，并产生启用告警。
		name: "slice to create with 2 endpoints, zone ratios only require 2",
		cpuRatiosByZone: map[string]float64{
			"zone-a": 0.45,
			"zone-b": 0.55,
		},
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
			ToCreate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.2.4"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}},
		},
		expectedEndpointsByAddrType: map[discovery.AddressType]EndpointZoneInfo{
			discovery.AddressTypeIPv4: {
				"zone-a": 1,
				"zone-b": 1,
			},
		},
		expectedSlicesToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.4"},
				Zone:       ptr.To("zone-b"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		expectedSlicesToUpdate: []*discovery.EndpointSlice{},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeNormal,
				Reason:    "TopologyAwareHintsEnabled",
				Message:   TopologyAwareHintsEnabled,
			},
		},
	}, {
		// 混入 1 个未 Ready 与 1 个状态未知端点：只给两个 Ready 端点写 hint，其余端点 hint 保持 nil。
		name: "slice to create with 2 ready, 1 unready, 1 unknown endpoints, zone ratios only require 2",
		cpuRatiosByZone: map[string]float64{
			"zone-a": 0.45,
			"zone-b": 0.55,
		},
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
			ToCreate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.2.4"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.2.5"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
				}, {
					Addresses: []string{"10.1.2.6"},
					Zone:      ptr.To("zone-b"),
				}},
			}},
		},
		expectedEndpointsByAddrType: map[discovery.AddressType]EndpointZoneInfo{
			discovery.AddressTypeIPv4: {
				"zone-a": 1,
				"zone-b": 1,
			},
		},
		expectedSlicesToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.4"},
				Zone:       ptr.To("zone-b"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.5"},
				Zone:       ptr.To("zone-b"),
				Hints:      nil,
				Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
			}, {
				Addresses: []string{"10.1.2.6"},
				Zone:      ptr.To("zone-b"),
				Hints:     nil,
			}},
		}},
		expectedSlicesToUpdate: []*discovery.EndpointSlice{},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeNormal,
				Reason:    "TopologyAwareHintsEnabled",
				Message:   TopologyAwareHintsEnabled,
			},
		},
	}, {
		// 三 zone 且同时有待创建与待更新 slice：期望按各 zone 端点数写入 hint，部分 zone-a 端点被重新分配 hint 到 zone-b。
		name: "slices to create and update within 3 zone threshold",
		cpuRatiosByZone: map[string]float64{
			"zone-a": 0.35,
			"zone-b": 0.35,
			"zone-c": 0.30,
		},
		sliceInfo: &SliceInfo{
			ServiceKey:  "ns/svc",
			AddressType: discovery.AddressTypeIPv4,
			ToCreate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.2.4"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}, {
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.1.3.3"},
					Zone:       ptr.To("zone-c"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.3.4"},
					Zone:       ptr.To("zone-c"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.1.3.4"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}},
			ToUpdate: []*discovery.EndpointSlice{{
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.2.2.3"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.2.2.4"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}, {
				Endpoints: []discovery.Endpoint{{
					Addresses:  []string{"10.2.3.3"},
					Zone:       ptr.To("zone-b"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.2.3.4"},
					Zone:       ptr.To("zone-c"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}, {
					Addresses:  []string{"10.2.3.4"},
					Zone:       ptr.To("zone-a"),
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				}},
			}},
		},
		expectedEndpointsByAddrType: map[discovery.AddressType]EndpointZoneInfo{
			discovery.AddressTypeIPv4: {
				"zone-a": 4,
				"zone-b": 3,
				"zone-c": 3,
			},
		},
		expectedSlicesToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.4"},
				Zone:       ptr.To("zone-b"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}, {
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.3.3"},
				Zone:       ptr.To("zone-c"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-c"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.3.4"},
				Zone:       ptr.To("zone-c"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-c"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.3.4"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		expectedSlicesToUpdate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.2.2.3"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.2.2.4"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}, {
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.2.3.3"},
				Zone:       ptr.To("zone-b"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.2.3.4"},
				Zone:       ptr.To("zone-c"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-c"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.2.3.4"},
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		expectedEvents: []*EventBuilder{
			{
				EventType: v1.EventTypeNormal,
				Reason:    "TopologyAwareHintsEnabled",
				Message:   TopologyAwareHintsEnabled,
			},
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 为每个用例新建独立的 TopologyCache，避免相互污染。
			cache := NewTopologyCache()
			// 注入用例给定的各 zone CPU 比例，作为分配 hint 的依据。
			cache.cpuRatiosByZone = tc.cpuRatiosByZone

			// 创建测试用日志上下文。
			logger, _ := ktesting.NewTestContext(t)
			// 调用被测方法，拿到待创建/待更新的 slice 以及产生的事件。
			slicesToCreate, slicesToUpdate, events := cache.AddHints(logger, tc.sliceInfo)

			// 断言待创建与待更新 slice 的端点（含 hint）与期望等价。
			expectEquivalentSlices(t, slicesToCreate, tc.expectedSlicesToCreate)
			expectEquivalentSlices(t, slicesToUpdate, tc.expectedSlicesToUpdate)
			// 断言产生的事件类型与原因符合预期。
			compareExpectedEvents(t, tc.expectedEvents, events)

			// 从缓存中取出该 Service 对应的按地址类型统计的端点分布。
			endpointsByAddrType, ok := cache.endpointsByService[tc.sliceInfo.ServiceKey]
			if tc.expectedEndpointsByAddrType == nil {
				// 期望不记录端点：确认缓存中不存在该 Service 的记录。
				if ok {
					t.Errorf("Expected no endpoints for Service %s, got %+v", tc.sliceInfo.ServiceKey, endpointsByAddrType)
				}
			} else {
				// 期望有记录：先校验地址类型的数量。
				if len(tc.expectedEndpointsByAddrType) != len(endpointsByAddrType) {
					t.Fatalf("Expected endpoints for %d address types, got %d", len(tc.expectedEndpointsByAddrType), len(endpointsByAddrType))
				}
				// 逐个地址类型校验其下各 zone 的端点数。
				for addrType, expectedEndpointZoneInfo := range tc.expectedEndpointsByAddrType {
					endpointZoneInfo, ok := endpointsByAddrType[addrType]
					if !ok {
						t.Fatalf("Expected endpoints for %s address type, got none", addrType)
					}

					// 校验该地址类型下 zone 的数量。
					if len(expectedEndpointZoneInfo) != len(endpointZoneInfo) {
						t.Fatalf("Expected endpoints for %d zones, got %d", len(expectedEndpointZoneInfo), len(endpointZoneInfo))
					}

					// 逐个 zone 校验端点数量是否与期望一致。
					for zone, expectedNum := range expectedEndpointZoneInfo {
						num, ok := endpointZoneInfo[zone]
						if !ok {
							t.Fatalf("Expected endpoints for %s zone, got none", zone)
						}
						if num != expectedNum {
							t.Errorf("Expected %d endpoints for %s zone, got %d", expectedNum, zone, num)
						}
					}
				}
			}
		})
	}
}

// TestSetNodes 验证 TopologyCache.SetNodes 能否根据节点列表判断信息是否充分，
// 并正确汇总每个可用区的 CPU 总量与各区占比。
// 构造的节点覆盖空列表、单节点、单 zone、双 zone，以及未就绪节点和控制面节点的过滤场景，
// 还包含多节点多 zone 的 CPU 累加；断言 sufficientNodeInfo、cpuByZone 与 cpuRatiosByZone 均与期望一致。
func TestSetNodes(t *testing.T) {
	// nodeInfo 是构造 v1.Node 的简化输入：区域、可分配 CPU、就绪状态与节点标签。
	type nodeInfo struct {
		// zone: 节点所属可用区，写入 Node 的 topology zone 标签
		// cpu: 节点可分配的 CPU 资源量
		// ready: 节点 Ready 条件的状态
		// labels: 附加标签，用于标记控制面节点等
		zone   string
		cpu    resource.Quantity
		ready  v1.ConditionStatus
		labels map[string]string
	}

	// 表驱动用例：给出构造用的节点信息以及期望的信息充分性、CPU 总量与 CPU 占比。
	testCases := []struct {
		// name: 测试用例名称
		// nodes: 用于构造 Node 列表的简化节点信息
		// expectSufficientNodeInfo: 期望 SetNodes 判定节点信息是否充分
		// expectedCPUByZone: 期望按 zone 汇总的 CPU 总量，nil 表示不应有记录
		// expectedRatios: 期望按 zone 计算的 CPU 占比，nil 表示不应有记录
		name                     string
		nodes                    []nodeInfo
		expectSufficientNodeInfo bool
		expectedCPUByZone        map[string]*resource.Quantity
		expectedRatios           map[string]float64
	}{{
		// 空节点列表：信息不足，CPU 与占比均为 nil。
		name:                     "empty",
		nodes:                    []nodeInfo{},
		expectSufficientNodeInfo: false,
		expectedCPUByZone:        nil,
		expectedRatios:           nil,
	}, {
		// 单节点：仅一个 zone 有节点，信息不足。
		name: "single node",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		},
		expectSufficientNodeInfo: false,
		expectedCPUByZone:        nil,
		expectedRatios:           nil,
	}, {
		// 单 zone 两个节点：仍只有一个 zone，信息不足。
		name: "single zone",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		},
		expectSufficientNodeInfo: false,
		expectedCPUByZone:        nil,
		expectedRatios:           nil,
	}, {
		// 两个 zone 各有 1 个就绪节点：信息充分，CPU 各 1，占比各 0.5。
		name: "2 zones",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		},
		expectSufficientNodeInfo: true,
		expectedCPUByZone: map[string]*resource.Quantity{
			"zone-a": resource.NewQuantity(1, resource.BinarySI),
			"zone-b": resource.NewQuantity(1, resource.BinarySI),
		},
		expectedRatios: map[string]float64{
			"zone-a": 0.5,
			"zone-b": 0.5,
		},
	}, {
		// 两个 zone 中各有一个未就绪/就绪节点：可用节点只覆盖一个 zone，信息不足。
		name: "2 zones, unready node in 1, ready node in 1",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionFalse},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		},
		expectSufficientNodeInfo: false,
		expectedCPUByZone:        nil,
		expectedRatios:           nil,
	}, {
		// 两个 zone 中各有一个控制面/普通节点：控制面节点被过滤，信息不足。
		name: "2 zones, control plane node in 1, ready node in 1",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue,
				labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}},
		},
		expectSufficientNodeInfo: false,
		expectedCPUByZone:        nil,
		expectedRatios:           nil,
	}, {
		// zone-b 额外有一个未就绪节点：过滤后两个 zone 仍各有就绪节点，信息充分，CPU 各 1、占比各 0.5。
		name: "2 zones, unready node in 1, ready node in 2",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionFalse},
		},
		expectSufficientNodeInfo: true,
		expectedCPUByZone: map[string]*resource.Quantity{
			"zone-a": resource.NewQuantity(1, resource.BinarySI),
			"zone-b": resource.NewQuantity(1, resource.BinarySI),
		},
		expectedRatios: map[string]float64{
			"zone-a": 0.5,
			"zone-b": 0.5,
		},
	}, {
		// zone-b 额外有一个控制面节点：过滤后两个 zone 仍各有可用节点，信息充分且占比各 0.5。
		name: "2 zones, control plane node in 1, ready node in 2",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue,
				labels: map[string]string{"node-role.kubernetes.io/control-plane": ""}},
		},
		expectSufficientNodeInfo: true,
		expectedCPUByZone: map[string]*resource.Quantity{
			"zone-a": resource.NewQuantity(1, resource.BinarySI),
			"zone-b": resource.NewQuantity(1, resource.BinarySI),
		},
		expectedRatios: map[string]float64{
			"zone-a": 0.5,
			"zone-b": 0.5,
		},
	}, {
		// 三个 zone 分别有 4、2、1 个节点：按 CPU 容量累加得到 5000/4500/500m，占比 0.5/0.45/0.05。
		name: "3 zones, 4 nodes in 1, 2 nodes in 1, 1 node in 1",
		nodes: []nodeInfo{
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
			{zone: "zone-a", cpu: resource.MustParse("2000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("3000m"), ready: v1.ConditionTrue},
			{zone: "zone-b", cpu: resource.MustParse("1500m"), ready: v1.ConditionTrue},
			{zone: "zone-c", cpu: resource.MustParse("500m"), ready: v1.ConditionTrue},
		},
		expectSufficientNodeInfo: true,
		expectedCPUByZone: map[string]*resource.Quantity{
			"zone-a": resource.NewMilliQuantity(5000, resource.BinarySI),
			"zone-b": resource.NewMilliQuantity(4500, resource.BinarySI),
			"zone-c": resource.NewMilliQuantity(500, resource.BinarySI),
		},
		expectedRatios: map[string]float64{
			"zone-a": 0.5,
			"zone-b": 0.45,
			"zone-c": 0.05,
		},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 为每个用例新建独立缓存。
			cache := NewTopologyCache()
			// 将简化的 nodeInfo 逐条转换为真实的 v1.Node 对象。
			nodes := make([]*v1.Node, 0, len(tc.nodes))
			for _, node := range tc.nodes {
				// 使用用例给定的标签，若为 nil 则初始化空 map 以免写入 zone 标签时 panic。
				labels := node.labels
				if labels == nil {
					labels = map[string]string{}
				}
				// 若用例指定了 zone，则写入 Node 的 topology zone 标签。
				if node.zone != "" {
					labels[v1.LabelTopologyZone] = node.zone
				}
				// 构造 NodeReady 条件，携带用例给定的状态（True/False）。
				conditions := []v1.NodeCondition{{
					Type:   v1.NodeReady,
					Status: node.ready,
				}}
				// 构造可分配资源列表，仅设置 CPU。
				allocatable := v1.ResourceList{
					v1.ResourceCPU: node.cpu,
				}
				// 组装完整的 v1.Node 并加入待传入的列表。
				nodes = append(nodes, &v1.Node{
					ObjectMeta: metav1.ObjectMeta{
						Labels: labels,
					},
					Status: v1.NodeStatus{
						Allocatable: allocatable,
						Conditions:  conditions,
					},
				})
			}

			// 创建测试用日志上下文。
			logger, _ := ktesting.NewTestContext(t)
			// 调用被测方法，让缓存根据节点列表更新状态。
			cache.SetNodes(logger, nodes)

			// 断言缓存对节点信息充分性的判定与期望一致。
			if cache.sufficientNodeInfo != tc.expectSufficientNodeInfo {
				t.Errorf("Expected sufficientNodeInfo to be %t, got %t", tc.expectSufficientNodeInfo, cache.sufficientNodeInfo)
			}

			// 断言按 zone 的 CPU 占比：先区分 nil 与非 nil，再逐项比对数值。
			if cache.cpuRatiosByZone == nil || tc.expectedRatios == nil {
				if (cache.cpuRatiosByZone == nil) != (tc.expectedRatios == nil) {
					t.Errorf("Expected %+v, got %+v", tc.expectedRatios, cache.cpuRatiosByZone)
				}
			} else {
				// 期望有占比数据时，先校验 zone 数量。
				if len(cache.cpuRatiosByZone) != len(tc.expectedRatios) {
					t.Errorf("Expected ratios with %d zones, got %d", len(tc.expectedRatios), len(cache.cpuRatiosByZone))
				}
				// 逐个 zone 校验实际占比数值。
				for zone, expectedRatio := range tc.expectedRatios {
					actualRatio, ok := cache.cpuRatiosByZone[zone]
					if !ok {
						t.Errorf("Expected ratio for %s zone, got none", zone)
					} else if actualRatio != expectedRatio {
						t.Errorf("Expected ratio to be %f, got %f", expectedRatio, actualRatio)
					}
				}
			}

			// 断言按 zone 的 CPU 总量：同样先区分 nil 与非 nil，再逐项比对。
			if cache.cpuByZone == nil || tc.expectedCPUByZone == nil {
				if (cache.cpuByZone == nil) != (tc.expectedCPUByZone == nil) {
					t.Errorf("Expected %+v, got %+v", tc.expectedCPUByZone, cache.cpuByZone)
				}
			} else {
				// 期望有 CPU 数据时，先校验 zone 数量。
				if len(cache.cpuByZone) != len(tc.expectedCPUByZone) {
					t.Errorf("Expected CPU with %d zones, got %d", len(tc.expectedCPUByZone), len(cache.cpuByZone))
				}
				// 逐个 zone 校验 CPU 总量是否相等（使用 Quantity.Equal 比较）。
				for zone, expectedCPU := range tc.expectedCPUByZone {
					actualCPU, ok := cache.cpuByZone[zone]
					if !ok {
						t.Errorf("Expected CPU for %s zone, got none", zone)
					} else if !actualCPU.Equal(*expectedCPU) {
						t.Errorf("Expected CPU to be %d, got %d", expectedCPU.MilliValue(), actualCPU.MilliValue())
					}
				}
			}
		})
	}
}

// TestTopologyCacheRace 通过并发调用 SetNodes、AddHints 与 HasPopulatedHints 来暴露
// TopologyCache 内部的竞态条件（主要用于 go test -race 检测，而非显式断言结果）。
// 构造一个含两个 Ready 端点（分属 zone-a/b）的 SliceInfo 与跨三 zone 的节点列表，
// 三个 goroutine 同时对同一个缓存进行读写。
func TestTopologyCacheRace(t *testing.T) {
	// 构造供 AddHints 并发使用的 SliceInfo：两个 Ready 端点分属 zone-a 与 zone-b。
	sliceInfo := &SliceInfo{
		ServiceKey:  "ns/svc",
		AddressType: discovery.AddressTypeIPv4,
		ToCreate: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Addresses:  []string{"10.1.2.3"},
				Zone:       ptr.To("zone-a"),
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Addresses:  []string{"10.1.2.4"},
				Zone:       ptr.To("zone-b"),
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}}}
	// nodeInfo 是构造并发输入节点的简化结构：区域、CPU 与就绪状态。
	type nodeInfo struct {
		// zone: 节点所属可用区
		// cpu: 节点可分配的 CPU 资源量
		// ready: 节点 Ready 条件的状态
		zone  string
		cpu   resource.Quantity
		ready v1.ConditionStatus
	}
	// 跨三个 zone 的节点列表，CPU 总量分别为 5000m、4500m、500m。
	nodeInfos := []nodeInfo{
		{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		{zone: "zone-a", cpu: resource.MustParse("1000m"), ready: v1.ConditionTrue},
		{zone: "zone-a", cpu: resource.MustParse("2000m"), ready: v1.ConditionTrue},
		{zone: "zone-b", cpu: resource.MustParse("3000m"), ready: v1.ConditionTrue},
		{zone: "zone-b", cpu: resource.MustParse("1500m"), ready: v1.ConditionTrue},
		{zone: "zone-c", cpu: resource.MustParse("500m"), ready: v1.ConditionTrue},
	}

	// 创建供三个 goroutine 共享的同一个缓存。
	cache := NewTopologyCache()
	// 将简化节点信息转换为真实的 v1.Node 列表。
	nodes := []*v1.Node{}
	for _, node := range nodeInfos {
		// 初始化标签并写入 zone。
		labels := map[string]string{}
		if node.zone != "" {
			labels[v1.LabelTopologyZone] = node.zone
		}
		// 构造 Ready 条件与只含 CPU 的可分配资源列表。
		conditions := []v1.NodeCondition{{
			Type:   v1.NodeReady,
			Status: node.ready,
		}}
		allocatable := v1.ResourceList{
			v1.ResourceCPU: node.cpu,
		}
		// 组装 v1.Node 并加入列表。
		nodes = append(nodes, &v1.Node{
			ObjectMeta: metav1.ObjectMeta{
				Labels: labels,
			},
			Status: v1.NodeStatus{
				Allocatable: allocatable,
				Conditions:  conditions,
			},
		})
	}

	// 三个 goroutine 并发读写同一缓存：更新节点、添加 hint、查询是否已有 hint。
	logger, _ := ktesting.NewTestContext(t)
	// 并发写入节点信息与 CPU 比例。
	go func() {
		cache.SetNodes(logger, nodes)
	}()
	// 并发为 slice 添加 hint，更新缓存中的端点统计。
	go func() {
		cache.AddHints(logger, sliceInfo)
	}()
	// 并发查询该 Service 是否已有 hint，验证读取路径的线程安全性。
	go func() {
		cache.HasPopulatedHints(sliceInfo.ServiceKey)
	}()
}

// Test Helpers

// expectEquivalentSlices 辅助断言两组 EndpointSlice 等价：先比对 slice 数量，
// 再逐个 slice 比对端点数量，最后用 reflect.DeepEqual 比较每个端点（包括 hint 与 Conditions）。
func expectEquivalentSlices(t *testing.T, actualSlices, expectedSlices []*discovery.EndpointSlice) {
	t.Helper()

	// 先校验 slice 数量，数量不符时直接致命失败。
	if len(actualSlices) != len(expectedSlices) {
		t.Fatalf("Expected %d slices, got %d", len(expectedSlices), len(actualSlices))
	}

	// 按位置逐个 slice 比对。
	for i, expectedSlice := range expectedSlices {
		actualSlice := actualSlices[i]

		// 端点数量不符则报错并跳过该 slice 的逐端点比对。
		if len(expectedSlice.Endpoints) != len(actualSlice.Endpoints) {
			t.Errorf("Expected %d endpoints, got %d", len(expectedSlice.Endpoints), len(actualSlice.Endpoints))
			continue
		}
		// 逐端点做深度比较，确保 hint 等字段完全一致。
		for j, expectedEndpoint := range expectedSlice.Endpoints {
			actualEndpoint := actualSlice.Endpoints[j]
			if !reflect.DeepEqual(actualEndpoint, expectedEndpoint) {
				t.Errorf("Endpoints didn't match\nExpected: %+v\nGot: %+v", expectedEndpoint, actualEndpoint)
			}
		}
	}
}

// compareExpectedEvents 辅助断言实际事件与期望事件一致：比对数量，
// 忽略 Message 字段做结构化 diff，再单独校验实际消息以期望消息为前缀。
func compareExpectedEvents(t *testing.T, expectedEvents, events []*EventBuilder) {
	// 先校验事件数量。
	if len(expectedEvents) != len(events) {
		t.Errorf("Expected %d event, got %d", len(expectedEvents), len(events))
	}
	// 逐个事件比对；若数量不匹配，此处按实际事件数遍历。
	for i, event := range events {
		// 忽略 Message 字段比较事件的其余结构，输出差异。
		if diff := cmp.Diff(event, expectedEvents[i], cmpopts.IgnoreFields(EventBuilder{}, "Message")); diff != "" {
			t.Errorf("Unexpected event (-want,+got):\n%s", diff)
		}
		// 消息单独校验：实际消息必须以期望消息为前缀，且期望消息不能为空。
		if got, want := event.Message, expectedEvents[i].Message; !strings.HasPrefix(got, want) || want == "" {
			t.Errorf("Unexpected event message:\ngot %q want a message with %q prefix", got, want)
		}
	}
}
