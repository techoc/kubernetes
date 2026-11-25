/*
Copyright 2024 The Kubernetes Authors.

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

package trafficdist

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/utils/ptr"
)

// TestReconcileHints 以表驱动方式验证 ReconcileHints 在不同分发策略与不同切片类别下的协调行为。
func TestReconcileHints(t *testing.T) {
	testCases := []struct {
		// name 是用例名称；trafficDistribution 是 Service 的分发策略（nil 表示未设置）；
		// slicesToCreate、slicesToUpdate、slicesUnchanged 分别是待创建、待更新、未变化的 EndpointSlice 输入。
		name string

		trafficDistribution *string
		slicesToCreate      []*discoveryv1.EndpointSlice
		slicesToUpdate      []*discoveryv1.EndpointSlice
		slicesUnchanged     []*discoveryv1.EndpointSlice

		// 以下三个 want 字段分别是三组切片的期望结果。
		wantSlicesToCreate  []*discoveryv1.EndpointSlice
		wantSlicesToUpdate  []*discoveryv1.EndpointSlice
		wantSlicesUnchanged []*discoveryv1.EndpointSlice
	}{
		{
			// 意图：PreferClose 只生成 zone 级提示，不生成 node 级提示。
			// 输入：2 个待创建切片、2 个待更新切片，Endpoint 分布在 zone-a/b/c，均就绪且带 NodeName。
			// 断言：返回的两组切片中，每个 Endpoint 的 Hints.ForZones 恰好包含自身 zone。
			name: "should set zone hints with PreferClose",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-c"}}},
						},
					},
				},
			},
		},
		{
			// 意图：PreferSameZone 与 PreferClose 行为一致，只写 ForZones。
			// 输入：与上一用例相同的 zone-a/b/c 就绪 Endpoint（待创建 + 待更新）。
			// 断言：即使设置了 NodeName，也不应生成 ForNodes，只应生成同 zone 的 ForZones。
			name: "should set zone hints with PreferSameZone",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferSameZone),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-c"}}},
						},
					},
				},
			},
		},
		{
			// 意图：PreferSameNode 同时生成 zone 与 node 两级提示。
			// 输入：同样是 zone-a/b/c、node-1..8 的就绪 Endpoint（待创建 + 待更新）。
			// 断言：每个 Endpoint 的 Hints 同时包含自身 zone 的 ForZones 和自身 NodeName 的 ForNodes。
			name: "should set zone and node hints with PreferSameNode",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferSameNode),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-1"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-b"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-2"}},
							},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-3"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-b"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-4"}},
							},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-5"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-a"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-6"}},
							},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-b"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-7"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForZones: []discoveryv1.ForZone{{Name: "zone-c"}},
								ForNodes: []discoveryv1.ForNode{{Name: "node-8"}},
							},
						},
					},
				},
			},
		},
		{
			// 意图：hints 错误或缺失时，unchanged 切片需被深拷贝并转入更新列表后修正。
			// 输入：待更新切片中 zone-a 的 Endpoint 带有错误的 zone-b 提示；两个 unchanged 切片分别带有错误提示和缺失提示。
			// 断言：三个切片都出现在 wantSlicesToUpdate 中，且 hints 全部修正为与自身 zone 一致。
			name: "should correct incorrect hints with PreferClose",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}}, // 按新启发式算法判断为错误的提示
						},
					},
				},
			},
			slicesUnchanged: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-c"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-c"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-c"}}},
						},
					},
				},
			},
		},
		{
			// 意图：没有 zone 信息时，PreferClose 不会退化为生成 node 级提示。
			// 输入：待创建和待更新的 Endpoint 均只有 NodeName，没有 Zone。
			// 断言：所有 Endpoint 的 Hints 都保持为 nil，输入切片内容不变。
			name: "should not create hints with PreferClose if there are no zones",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
		},
		{
			// 意图：没有 zone 信息时，PreferSameNode 仍生成 node 级提示。
			// 输入：待创建和待更新的 Endpoint 均只有 NodeName，没有 Zone。
			// 断言：每个 Endpoint 的 Hints 只含 ForNodes（自身 NodeName），ForZones 为空。
			name: "should create only node hints with PreferSameNode if there are no zones",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferSameNode),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-1"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-2"}},
							},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-3"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.4"},
							NodeName:   ptr.To("node-4"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-4"}},
							},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-5"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.6"},
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-6"}},
							},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.7"},
							NodeName:   ptr.To("node-7"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-7"}},
							},
						},
						{
							Addresses:  []string{"10.0.0.8"},
							NodeName:   ptr.To("node-8"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints: &discoveryv1.EndpointHints{
								ForNodes: []discoveryv1.ForNode{{Name: "node-8"}},
							},
						},
					},
				},
			},
		},
		{
			// 意图：非就绪 Endpoint 不参与就近分发，不应因缺少 hints 而触发更新。
			// 输入：一个 unchanged 切片，含 Ready=false 且无 Zone 提示的 Endpoint。
			// 断言：该切片仍留在 slicesUnchanged，待更新列表为空。
			name: "unready endpoints should not trigger updates",

			trafficDistribution: ptr.To(corev1.ServiceTrafficDistributionPreferClose),
			slicesUnchanged: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)}, // endpoint 未就绪
						},
					},
				},
			},
			wantSlicesUnchanged: []*discoveryv1.EndpointSlice{ // 因此不应产生更新
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(false)},
						},
					},
				},
			},
		},
		{
			// 意图：无法识别的分发策略回退到默认启发式，清除所有已有 hints。
			// 输入：trafficDistribution 为 "Unknown"，待创建与待更新切片中的 Endpoint 均带 zone 提示。
			// 断言：返回的两组切片中所有 Endpoint 的 Hints 都被移除。
			name: "should remove hints when trafficDistribution is unrecognized",

			trafficDistribution: ptr.To("Unknown"),
			slicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
			},
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
			},
			wantSlicesToCreate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.1"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-1"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
						{
							Addresses:  []string{"10.0.0.2"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-2"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.3"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-3"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
		},
		{
			// 意图：trafficDistribution 未设置时同样回退到默认启发式，清除所有 hints。
			// 输入：trafficDistribution 为 nil；待更新切片与 unchanged 切片各含一个带 zone 提示的就绪 Endpoint。
			// 断言：两个切片都出现在待更新结果中，且 hints 均被移除；unchanged 结果为空。
			name: "should remove hints when trafficDistribution is unset",

			trafficDistribution: nil,
			slicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-a"}}},
						},
					},
				},
			},
			slicesUnchanged: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
							Hints:      &discoveryv1.EndpointHints{ForZones: []discoveryv1.ForZone{{Name: "zone-b"}}},
						},
					},
				},
			},
			wantSlicesToUpdate: []*discoveryv1.EndpointSlice{
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.5"},
							Zone:       ptr.To("zone-a"),
							NodeName:   ptr.To("node-5"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
				{
					Endpoints: []discoveryv1.Endpoint{
						{
							Addresses:  []string{"10.0.0.6"},
							Zone:       ptr.To("zone-b"),
							NodeName:   ptr.To("node-6"),
							Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
						},
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		// 每个用例独立运行，tc.name 作为子测试名。
		t.Run(tc.name, func(t *testing.T) {
			// 调用被测函数，拿到三组切片的实际结果。
			gotSlicesToCreate, gotSlicesToUpdate, gotSlicesUnchanged := ReconcileHints(tc.trafficDistribution, tc.slicesToCreate, tc.slicesToUpdate, tc.slicesUnchanged)

			// 逐组断言：cmp.Diff 输出 (-want, +got)；cmpopts.EquateEmpty 使 nil 与空切片等价。
			if diff := cmp.Diff(tc.wantSlicesToCreate, gotSlicesToCreate, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ReconcileHints(...) returned unexpected diff in 'slicesToCreate': (-want, +got)\n%v", diff)
			}
			// 待更新切片既要包含原本就要更新的项，也要包含从 unchanged 转入的项。
			if diff := cmp.Diff(tc.wantSlicesToUpdate, gotSlicesToUpdate, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ReconcileHints(...) returned unexpected diff in 'slicesToUpdate': (-want, +got)\n%v", diff)
			}
			// 需要更新的切片必须从 unchanged 中剔除。
			if diff := cmp.Diff(tc.wantSlicesUnchanged, gotSlicesUnchanged, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("ReconcileHints(...) returned unexpected diff in 'slicesUnchanged': (-want, +got)\n%v", diff)
			}
		})
	}
}

// TestReconcileHints_doesNotMutateUnchangedSlices 验证传入 slicesUnchanged 的对象不会被原地修改。
func TestReconcileHints_doesNotMutateUnchangedSlices(t *testing.T) {
	// 构造一个缺少 hints 且带 zone 的就绪 Endpoint，按 PreferClose 本应触发更新。
	originalEps := &discoveryv1.EndpointSlice{
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses:  []string{"10.0.0.1"},
				Zone:       ptr.To("zone-a"),
				NodeName:   ptr.To("node-1"),
				Conditions: discoveryv1.EndpointConditions{Ready: ptr.To(true)},
			},
		},
	}
	// 深拷贝一份作为基准，用于事后对比原始对象是否被改动。
	clonedEps := originalEps.DeepCopy()

	// 调用时只传入 unchanged 切片；函数应深拷贝后再写入 hints，而非修改入参对象。
	ReconcileHints(ptr.To(corev1.ServiceTrafficDistributionPreferClose), nil, nil, []*discoveryv1.EndpointSlice{originalEps})
	// 断言：原对象与拷贝完全一致，说明 informer 缓存中的对象未被污染。
	if diff := cmp.Diff(clonedEps, originalEps); diff != "" {
		t.Errorf("ReconcileHints(...) modified objects within slicesUnchanged, want objects within slicesUnchanged to remain unmodified: (-want, +got)\n%v", diff)
	}
}
