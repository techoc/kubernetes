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
	"fmt"
	"testing"

	discovery "k8s.io/api/discovery/v1"
	"k8s.io/utils/ptr"
)

// Test_getTotalReadyEndpoints 验证 SliceInfo.getTotalReadyEndpoints 只统计处于 Ready 状态的端点。
// 构造的 SliceInfo 覆盖空对象、单组 slice、多组 slice 以及混合 Ready/未 Ready 端点等情形，
// 先借助测试辅助函数核对端点总数，再断言被测方法返回的 Ready 端点数与期望一致。
func Test_getTotalReadyEndpoints(t *testing.T) {
	// 表驱动用例：每组给出输入 SliceInfo、期望的 Ready 端点数以及用于校验辅助函数的端点总数。
	testCases := []struct {
		// name: 测试用例名称
		// si: 被测的 SliceInfo，包含 ToCreate、ToUpdate、Unchanged 三组 EndpointSlice
		// expectedTotalReady: 期望统计到的 Ready 端点数量
		// expectedTotal: 期望的端点总数，实质上是用来验证测试辅助函数本身
		name               string
		si                 *SliceInfo
		expectedTotalReady int
		expectedTotal      int // this is really just testing the test helper
	}{{
		// 空 SliceInfo：三组 slice 均为空，期望两类计数都是 0。
		name:          "empty",
		si:            &SliceInfo{},
		expectedTotal: 0,
	}, {
		// 仅含一个空 slice（0 个 Ready、0 个未 Ready）：验证空 slice 的贡献为 0。
		name: "empty slice",
		si: &SliceInfo{
			ToCreate: []*discovery.EndpointSlice{sliceWithNEndpoints(0, 0)},
		},
		expectedTotalReady: 0,
		expectedTotal:      0,
	}, {
		// 两个仅含 Ready 端点的 slice：验证跨 slice 的 Ready 端点累加。
		name: "multiple slices",
		si: &SliceInfo{
			ToCreate: []*discovery.EndpointSlice{sliceWithNEndpoints(15, 0), sliceWithNEndpoints(8, 0)},
		},
		expectedTotalReady: 23,
		expectedTotal:      23,
	}, {
		// ToCreate、ToUpdate、Unchanged 三组 slice 同时存在且端点全部 Ready：验证三组都被纳入统计。
		name: "slices for all",
		si: &SliceInfo{
			ToCreate:  []*discovery.EndpointSlice{sliceWithNEndpoints(15, 0), sliceWithNEndpoints(8, 0)},
			ToUpdate:  []*discovery.EndpointSlice{sliceWithNEndpoints(2, 0)},
			Unchanged: []*discovery.EndpointSlice{sliceWithNEndpoints(100, 0), sliceWithNEndpoints(90, 0)},
		},
		expectedTotalReady: 215,
		expectedTotal:      215,
	}, {
		// 三组 slice 中都掺入未 Ready 端点：Ready 总数(211)小于端点总数(234)，验证只计 Ready。
		name: "slices for all with some unready",
		si: &SliceInfo{
			ToCreate:  []*discovery.EndpointSlice{sliceWithNEndpoints(15, 3), sliceWithNEndpoints(5, 4)},
			ToUpdate:  []*discovery.EndpointSlice{sliceWithNEndpoints(3, 8)},
			Unchanged: []*discovery.EndpointSlice{sliceWithNEndpoints(98, 2), sliceWithNEndpoints(90, 6)},
		},
		expectedTotalReady: 211,
		expectedTotal:      234,
	}}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 先用 countEndpoints 汇总三组 slice 的端点总数，作为对辅助函数的自检。
			actualTotal := countEndpoints(tc.si.ToCreate) + countEndpoints(tc.si.ToUpdate) + countEndpoints(tc.si.Unchanged)

			if actualTotal != tc.expectedTotal {
				// This likely means that sliceWithNEndpoints or countEndpoints are not working as expected
				// 总数不符说明构造 fixture 或 countEndpoints 有问题，此时后续断言不可信。
				t.Errorf("Problem with test or test helper. Expected %d total endpoints, got %d", tc.expectedTotal, actualTotal)
			}

			// 调用被测方法统计 Ready 端点数。
			actualTotalReady := tc.si.getTotalReadyEndpoints()
			// 断言 Ready 端点数与用例期望一致。
			if actualTotalReady != tc.expectedTotalReady {
				t.Errorf("Expected %d, got %d", tc.expectedTotalReady, actualTotalReady)
			}
		})
	}
}

// helpers

// sliceWithNEndpoints 构造一个含 ready 个 Ready 端点、unready 个未 Ready 端点的 EndpointSlice。
// 端点地址按序号递增生成（10.1.2.<i>），仅 Conditions.Ready 用于区分两组端点。
func sliceWithNEndpoints(ready, unready int) *discovery.EndpointSlice {
	// 预分配长度为两类端点数量之和的切片，前 ready 个为 Ready，其余为未 Ready。
	endpoints := make([]discovery.Endpoint, ready+unready)

	// 填充 Ready 端点：Conditions.Ready 为 true。
	for i := 0; i < ready; i++ {
		endpoints[i] = discovery.Endpoint{
			Addresses:  []string{fmt.Sprintf("10.1.2.%d", i)},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(true)},
		}
	}

	// 填充未 Ready 端点：紧接在 Ready 端点之后，Conditions.Ready 为 false。
	for i := 0; i < unready; i++ {
		endpoints[ready+i] = discovery.Endpoint{
			Addresses:  []string{fmt.Sprintf("10.1.2.%d", ready+i)},
			Conditions: discovery.EndpointConditions{Ready: ptr.To(false)},
		}
	}

	// 将构造好的端点列表包装成 EndpointSlice 返回。
	return &discovery.EndpointSlice{
		Endpoints: endpoints,
	}
}

// countEndpoints 统计一组 EndpointSlice 中所有端点的总数，不区分是否 Ready。
func countEndpoints(slices []*discovery.EndpointSlice) int {
	total := 0
	// 逐个 slice 累加其 Endpoints 长度。
	for _, slice := range slices {
		total += len(slice.Endpoints)
	}
	// 返回累加得到的端点总数。
	return total
}
