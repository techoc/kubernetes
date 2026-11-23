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
	"testing"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/ptr"
)

// Test_redistributeHints 验证 redistributeHints 能把 hint 从 givingZones 迁出并分配给 receivingZones，
// 且返回的每 zone 再分配数量与预期一致（迁出为负数、迁入为正数）。
// 构造的输入覆盖空输入、单端点跨 zone 迁移，以及一个 zone 的多个端点分散迁移到两个 zone 的情形。
func Test_redistributeHints(t *testing.T) {
	// 表驱动用例：给出待迁移的 slice、迁出/迁入 zone 配额及期望的再分配结果。
	testCases := []struct {
		// name: 测试用例名称
		// slices: 携带 Zone/Hints/Ready 信息的 EndpointSlice 列表，作为迁移的端点来源
		// givingZones: 需要迁出端点的 zone 及其数量
		// receivingZones: 需要接收端点的 zone 及其数量
		// expectedRedistributions: 期望的每个 zone 的净迁移数量
		name                    string
		slices                  []*discovery.EndpointSlice
		givingZones             map[string]int
		receivingZones          map[string]int
		expectedRedistributions map[string]int
	}{{
		// 空输入：无 slice、无迁出迁入，结果应为空 map。
		name:                    "empty",
		slices:                  []*discovery.EndpointSlice{},
		givingZones:             map[string]int{},
		receivingZones:          map[string]int{},
		expectedRedistributions: map[string]int{},
	}, {
		// 单端点：zone-a 的一个 Ready 端点迁往 zone-b，期望 a 为 -1、b 为 +1。
		name: "single endpoint",
		slices: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		givingZones:             map[string]int{"zone-a": 1},
		receivingZones:          map[string]int{"zone-b": 1},
		expectedRedistributions: map[string]int{"zone-a": -1, "zone-b": 1},
	}, {
		// 多端点：zone-a 的两个端点分别迁往 zone-b、zone-c，期望 a 为 -2、b/c 各 +1。
		name: "endpoints from 1 zone redistributed to 2 other zones",
		slices: []*discovery.EndpointSlice{{
			Endpoints: []discovery.Endpoint{{
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		}},
		givingZones:             map[string]int{"zone-a": 2},
		receivingZones:          map[string]int{"zone-b": 1, "zone-c": 1},
		expectedRedistributions: map[string]int{"zone-a": -2, "zone-b": 1, "zone-c": 1},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 创建测试用日志上下文，供被测函数记录日志。
			logger, _ := ktesting.NewTestContext(t)
			// 调用被测函数，得到每个 zone 的实际再分配数量。
			actualRedistributions := redistributeHints(logger, tc.slices, tc.givingZones, tc.receivingZones)

			// 先校验结果 map 中 zone 的个数与期望一致。
			if len(actualRedistributions) != len(tc.expectedRedistributions) {
				t.Fatalf("Expected redistributions for %d zones, got %d (%+v)", len(tc.expectedRedistributions), len(actualRedistributions), actualRedistributions)
			}

			// 再逐个 zone 校验具体的再分配数量。
			for zone, expectedNum := range tc.expectedRedistributions {
				actualNum, _ := actualRedistributions[zone]
				if actualNum != expectedNum {
					t.Errorf("Expected redistribution of %d for zone %s, got %d", expectedNum, zone, actualNum)
				}
			}
		})
	}
}

// Test_getGivingAndReceivingZones 验证 getGivingAndReceivingZones 根据各 zone 的期望配额(desired)
// 与实际已分配 hint 数，正确算出需要迁出(move)端点与接收端点的 zone 及数量。
// 构造的输入涵盖空输入、无需再平衡、同 zone 优先、多个 zone 需求/盈余的比较，
// 以及分配数超过期望、desired 为负、已分配数为负等异常数据；断言两个返回 map 均与期望一致。
func Test_getGivingAndReceivingZones(t *testing.T) {
	// 表驱动用例：给出各 zone 的分配预期与当前分配情况，以及期望的给出/接收 zone 数量。
	testCases := []struct {
		// name: 测试用例名称
		// allocations: 每个 zone 的分配描述（此处主要使用 desired 期望端点数）
		// allocatedHintsByZone: 每个 zone 当前已分配的 hint 数量
		// expectedGivingZones: 期望的迁出 zone 及其可迁出数量
		// expectedReceivingZones: 期望的接收 zone 及其需要接收的数量
		name                   string
		allocations            map[string]allocation
		allocatedHintsByZone   map[string]int
		expectedGivingZones    map[string]int
		expectedReceivingZones map[string]int
	}{{
		// 空输入：无 zone、无分配，两个结果 map 都应为空。
		name:                   "empty",
		allocations:            map[string]allocation{},
		allocatedHintsByZone:   map[string]int{},
		expectedGivingZones:    map[string]int{},
		expectedReceivingZones: map[string]int{},
	}, {
		// 简单分配：三个 zone 各分配 1 个，且各自 desired 略大于 1，无需再平衡。
		name: "simple allocation with no need for rebalancing",
		allocations: map[string]allocation{
			"zone-a": {desired: 1.2},
			"zone-b": {desired: 1.1},
			"zone-c": {desired: 1.0},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		expectedGivingZones:    map[string]int{},
		expectedReceivingZones: map[string]int{},
	}, {
		// 同 zone 优先：即使多给一个端点能让分布更均匀，也优先把 hint 留给端点所在 zone。
		name: "preference for same zone even when giving an extra endpoint would result in slightly better distribution",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.1},
			"zone-b": {desired: 5.1},
			"zone-c": {desired: 5.8},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": 16},
		expectedGivingZones:    map[string]int{"zone-a": 10},
		expectedReceivingZones: map[string]int{"zone-b": 5, "zone-c": 5},
	}, {
		// 两个 zone 的需求都不足 1 个端点时，优先分给缺口更大的 zone（zone-b 分 6、zone-c 分 5）。
		name: "when 2 zones need < 1 endpoint, give to zone that needs endpoint most",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.0},
			"zone-b": {desired: 5.6},
			"zone-c": {desired: 5.4},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": 16},
		expectedGivingZones:    map[string]int{"zone-a": 11},
		expectedReceivingZones: map[string]int{"zone-b": 6, "zone-c": 5},
	}, {
		// 两个 zone 都有盈余时，从盈余更多的 zone 迁出（zone-c 出 3、zone-b 出 2），补给 zone-a。
		name: "when 2 zones have extra endpoints, give from zone with most extra",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.0},
			"zone-b": {desired: 5.6},
			"zone-c": {desired: 5.4},
		},
		allocatedHintsByZone:   map[string]int{"zone-b": 8, "zone-c": 8},
		expectedGivingZones:    map[string]int{"zone-b": 2, "zone-c": 3},
		expectedReceivingZones: map[string]int{"zone-a": 5},
	}, {
		// 异常数据：已分配数(6)超过期望(5)，不应触发任何迁移。
		name: "ensure function can handle unexpected data (more allocated than allocations)",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.0},
			"zone-b": {desired: 5.0},
			"zone-c": {desired: 5.0},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": 6, "zone-b": 6, "zone-c": 6},
		expectedGivingZones:    map[string]int{},
		expectedReceivingZones: map[string]int{},
	}, {
		// 异常数据：desired 为负数，不应触发任何迁移。
		name: "ensure function can handle unexpected data (negative allocations)",
		allocations: map[string]allocation{
			"zone-a": {desired: -5.0},
			"zone-b": {desired: -5.0},
			"zone-c": {desired: -5.0},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": 6, "zone-b": 6, "zone-c": 6},
		expectedGivingZones:    map[string]int{},
		expectedReceivingZones: map[string]int{},
	}, {
		// 异常数据：已分配数为负数，同样视为无需迁移。
		name: "ensure function can handle unexpected data (negative allocated)",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.0},
			"zone-b": {desired: 5.0},
			"zone-c": {desired: 5.0},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": -4, "zone-b": -3, "zone-c": -2},
		expectedGivingZones:    map[string]int{},
		expectedReceivingZones: map[string]int{},
	}, {
		// 异常数据：单个 zone 已分配数为负，其余 zone 仍按盈余量向它迁移。
		name: "ensure function can handle unexpected data (negative for 1 zone)",
		allocations: map[string]allocation{
			"zone-a": {desired: 5.0},
			"zone-b": {desired: 5.0},
			"zone-c": {desired: 5.0},
		},
		allocatedHintsByZone:   map[string]int{"zone-a": -40, "zone-b": 20, "zone-c": 20},
		expectedGivingZones:    map[string]int{"zone-b": 15, "zone-c": 15},
		expectedReceivingZones: map[string]int{"zone-a": 30},
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 调用被测函数，得到迁出 zone 与接收 zone 两个结果 map。
			actualGivingZones, actualReceivingZones := getGivingAndReceivingZones(tc.allocations, tc.allocatedHintsByZone)

			// 校验迁出 zone 的数量与期望一致。
			if !reflect.DeepEqual(actualGivingZones, tc.expectedGivingZones) {
				t.Errorf("Expected %+v giving zones, got %+v", tc.expectedGivingZones, actualGivingZones)
			}
			// 校验接收 zone 的数量与期望一致。
			if !reflect.DeepEqual(actualReceivingZones, tc.expectedReceivingZones) {
				t.Errorf("Expected %+v receiving zones, got %+v", tc.expectedReceivingZones, actualReceivingZones)
			}
		})
	}
}

// Test_getHintsByZone 验证 getHintsByZone 能基于 slice 中端点的 Hint、zone 与 Ready 状态，
// 统计出各 zone 需要分配的 hint 数量；当 hint 指向不存在的 zone、hint 为空或会使最小分配无法满足时返回 nil。
// 构造的用例覆盖空输入、单/多 zone hint、混入未 Ready 与状态未知端点，以及多种非法 hint 情形；
// 断言返回的按 zone 计数与期望完全一致（区分 nil 与空 map）。
func Test_getHintsByZone(t *testing.T) {
	// 表驱动用例：给出 slice、当前按 zone 的 hint 分配情况、各 zone 的分配上限及期望结果。
	testCases := []struct {
		// name: 测试用例名称
		// slice: 携带端点 Zone/Hints/Ready 信息的 EndpointSlice
		// allocatedHintsByZone: 当前已按 zone 统计的 hint 数量
		// allocations: 每个 zone 的分配描述（此处主要使用 maximum 上限）
		// expectedHintsByZone: 期望的按 zone hint 数量，nil 表示判定为非法
		name                 string
		slice                discovery.EndpointSlice
		allocatedHintsByZone EndpointZoneInfo
		allocations          map[string]allocation
		expectedHintsByZone  map[string]int
	}{{
		// 空输入：空 slice、空分配，期望得到空 map。
		name:                 "empty",
		slice:                discovery.EndpointSlice{},
		allocations:          map[string]allocation{},
		allocatedHintsByZone: EndpointZoneInfo{},
		expectedHintsByZone:  map[string]int{},
	}, {
		// 单 zone hint：一个 Ready 端点的 hint 指向 zone-a，期望 zone-a 计 1。
		name: "single zone hint",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{{
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{"zone-a": 1},
		expectedHintsByZone: map[string]int{
			"zone-a": 1,
		},
	}, {
		// 单 zone hint + 1 个未 Ready、1 个状态未知端点：只统计 Ready 端点，期望仍为 zone-a 计 1。
		name: "single zone hint with 1 unready endpoint and 1 unknown endpoint",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{{
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
			}, {
				Zone:       ptr.To("zone-a"),
				Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
				Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
			}, {
				Zone:  ptr.To("zone-a"),
				Hints: &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
			}},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{"zone-a": 1},
		expectedHintsByZone: map[string]int{
			"zone-a": 1,
		},
	}, {
		// 多个 zone hint：三个 Ready 端点分别指向 zone-a、zone-b、zone-b，期望 a 计 1、b 计 2。
		name: "multiple zone hints",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
				{
					Zone:       ptr.To("zone-b"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-b"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
			},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
			"zone-b": {maximum: 3},
			"zone-c": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		expectedHintsByZone: map[string]int{
			"zone-a": 1,
			"zone-b": 2,
		},
	}, {
		// 非法：hint 指向已不再需要分配的 zone（zone-non-existent），期望返回 nil。
		name: "invalid by zones that no longer requires any allocations",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-non-existent"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
			},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{"zone-a": 1, "zone-b": 1, "zone-c": 1},
		expectedHintsByZone:  nil,
	}, {
		// 非法：端点的 Hints 为 nil，期望返回 nil。
		name: "invalid by endpoints with nil hints",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{
				{
					Zone:       ptr.To("zone-a"),
					Hints:      nil,
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
			},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{},
		expectedHintsByZone:  nil,
	}, {
		// 非法：端点的 ForZones 为空列表（无任何 hint），期望返回 nil。
		name: "invalid by endpoint with no hints",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
			},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 3},
		},
		allocatedHintsByZone: EndpointZoneInfo{},
		expectedHintsByZone:  nil,
	}, {
		// 非法：两个端点都指向 zone-a，加上已分配的 1 个共 3 个，超过 zone-a 的 maximum=2，期望返回 nil。
		name: "invalid by hints that would make minimum allocations impossible",
		slice: discovery.EndpointSlice{
			Endpoints: []discovery.Endpoint{
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
				{
					Zone:       ptr.To("zone-a"),
					Hints:      &discovery.EndpointHints{ForZones: []discovery.ForZone{{Name: "zone-a"}}},
					Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
				},
			},
		},
		allocations: map[string]allocation{
			"zone-a": {maximum: 2},
		},
		allocatedHintsByZone: EndpointZoneInfo{"zone-a": 1},
		expectedHintsByZone:  nil,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 调用被测函数，传入 slice、当前分配情况及各 zone 的分配上限。
			actualHintsByZone := getHintsByZone(&tc.slice, tc.allocatedHintsByZone, tc.allocations)

			if !reflect.DeepEqual(actualHintsByZone, tc.expectedHintsByZone) {
				// %#v for distinguishing between nil and empty map
				// 使用 %#v 输出以便区分 nil map 与空 map。
				t.Errorf("Expected %#v hints by zones, got %#v", tc.expectedHintsByZone, actualHintsByZone)
			}
		})
	}
}
