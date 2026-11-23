/*
Copyright 2025 The Kubernetes Authors.

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

package resourceclaim

import (
	"testing"

	resourceapi "k8s.io/api/resource/v1"
)

// TestTolerationToleratesTaint 以表驱动方式测试 ToleratesTaint 的匹配规则，
// 覆盖 Exists 匹配任意 value、空 key 通配、Equal 精确匹配、value 不匹配与 effect 不匹配等场景。
func TestTolerationToleratesTaint(t *testing.T) {

	// 每条用例包含容忍度、污点以及期望的匹配结果。
	testCases := []struct {
		description     string
		toleration      resourceapi.DeviceToleration
		taint           resourceapi.DeviceTaint
		expectTolerated bool
	}{
		// 场景：key/effect 相同且 operator 为 Exists，污点无 value，期望容忍。
		{
			description: "toleration and taint have the same key and effect, and operator is Exists, and taint has no value, expect tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "foo",
				Operator: resourceapi.DeviceTolerationOpExists,
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
			expectTolerated: true,
		},
		// 场景：key/effect 相同且 operator 为 Exists，污点带有 value，期望容忍。
		{
			description: "toleration and taint have the same key and effect, and operator is Exists, and taint has some value, expect tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "foo",
				Operator: resourceapi.DeviceTolerationOpExists,
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Value:  "bar",
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
			expectTolerated: true,
		},
		// 场景：容忍度 key 为空且 operator 为 Exists，匹配任意 key 的污点，期望容忍。
		{
			description: "toleration and taint have the same effect, toleration has empty key and operator is Exists, means match all taints, expect tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "",
				Operator: resourceapi.DeviceTolerationOpExists,
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Value:  "bar",
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
			expectTolerated: true,
		},
		// 场景：key、effect、value 均相同且 operator 为 Equal，期望容忍。
		{
			description: "toleration and taint have the same key, effect and value, and operator is Equal, expect tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "foo",
				Operator: resourceapi.DeviceTolerationOpEqual,
				Value:    "bar",
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Value:  "bar",
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
			expectTolerated: true,
		},
		// 场景：key、effect 相同但 value 不同且 operator 为 Equal，期望不容忍。
		{
			description: "toleration and taint have the same key and effect, but different values, and operator is Equal, expect not tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "foo",
				Operator: resourceapi.DeviceTolerationOpEqual,
				Value:    "value1",
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Value:  "value2",
				Effect: resourceapi.DeviceTaintEffectNoSchedule,
			},
			expectTolerated: false,
		},
		// 场景：key、value 相同但 effect 不同且 operator 为 Equal，期望不容忍。
		{
			description: "toleration and taint have the same key and value, but different effects, and operator is Equal, expect not tolerated",
			toleration: resourceapi.DeviceToleration{
				Key:      "foo",
				Operator: resourceapi.DeviceTolerationOpEqual,
				Value:    "bar",
				Effect:   resourceapi.DeviceTaintEffectNoSchedule,
			},
			taint: resourceapi.DeviceTaint{
				Key:    "foo",
				Value:  "bar",
				Effect: resourceapi.DeviceTaintEffectNoExecute,
			},
			expectTolerated: false,
		},
	}
	// 遍历所有用例，以 description 作为子测试名称执行。
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			// 调用被测函数，将实际结果与期望值比较；失败时输出容忍度与污点详情。
			if tolerated := ToleratesTaint(tc.toleration, tc.taint); tc.expectTolerated != tolerated {
				t.Errorf("expect %v, got %v: toleration %+v, taint %s", tc.expectTolerated, tolerated, tc.toleration, tc.taint)
			}
		})
	}
}
