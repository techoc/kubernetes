/*
Copyright The Kubernetes Authors.

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

// 本测试文件验证 extendWebSocketsToKubeletFeature 的四个接口方法：
// 名称与需求声明必须精确匹配 feature gate，Discover 完全取决于该 gate，
// 而调度和更新推断都不需要它（该能力与具体 pod 无关）。
package extendwebsocketstokubelet

import (
	"testing"

	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestRequirements 同时校验 Name 与 Requirements：
// 两者必须引用同一个 feature gate 常量，否则“声明的能力”和“实际探测的能力”会脱节。
func TestRequirements(t *testing.T) {
	feature := &extendWebSocketsToKubeletFeature{}
	if feature.Name() != ExtendWebSocketsToKubeletFeatureGate {
		t.Fatalf("expected Name to be %s, got %s", ExtendWebSocketsToKubeletFeatureGate, feature.Name())
	}
	reqs := feature.Requirements()
	if reqs == nil {
		t.Fatalf("Feature %s returned nil Requirements", feature.Name())
	}

	// 声明“恰好一个”gate：多声明或无声明都会让调度侧的必要条件判断失真。
	if len(reqs.EnabledFeatureGates) != 1 || reqs.EnabledFeatureGates[0] != ExtendWebSocketsToKubeletFeatureGate {
		t.Fatalf("Feature %s Requirements should declare exactly the %s feature gate", feature.Name(), ExtendWebSocketsToKubeletFeatureGate)
	}
}

// TestDiscover 用表驱动覆盖 gate 开/关两种取值：
// Discover 是该特性唯一的开关来源，不需要构造 runtime features 或 pod 信息。
func TestDiscover(t *testing.T) {
	tests := []struct {
		name               string
		featureGateEnabled bool
		expected           bool
	}{
		{
			name:               "feature enabled",
			featureGateEnabled: true,
			expected:           true,
		},
		{
			name:               "feature disabled",
			featureGateEnabled: false,
			expected:           false,
		},
	}

	feature := &extendWebSocketsToKubeletFeature{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &types.NodeConfiguration{
				FeatureGates: types.FeatureGateMap{ExtendWebSocketsToKubeletFeatureGate: tc.featureGateEnabled},
			}
			enabled := feature.Discover(config)
			if want, got := tc.expected, enabled; want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestInferForSchedulingAndUpdate 验证两个推断方法恒为 false：
// WebSocket 传输能力对 Pod 透明，任意 Pod 都不因它而需要特定节点。
func TestInferForSchedulingAndUpdate(t *testing.T) {
	feature := &extendWebSocketsToKubeletFeature{}
	podInfo := &types.PodInfo{}
	if feature.InferForScheduling(podInfo) {
		t.Fatalf("expect InferForScheduling to be false")
	}
	if feature.InferForUpdate(nil, podInfo) {
		t.Fatalf("expect InferForUpdate to be false")
	}
}

// TestMaxVersion 验证没有版本上限：该特性不与某个具体的 kubelet 版本绑定。
func TestMaxVersion(t *testing.T) {
	feature := &extendWebSocketsToKubeletFeature{}
	if feature.MaxVersion() != nil {
		t.Fatalf("expect MaxVersion to be nil")
	}
}
