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

package usernamespaceshostnetwork

import (
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// fakeFeatureGate 只实现 types.FeatureGate 需要的 Enabled 方法，
// 让测试可以在不依赖真实 feature gate 注册表的情况下精确控制每个门控的开关。
type fakeFeatureGate struct {
	features map[string]bool
}

// Enabled 返回指定特性门是否被打开；未设置的门默认返回 false。
func (m *fakeFeatureGate) Enabled(key string) bool {
	return m.features[key]
}

// TestDiscoverFeature 锁定 Discover 的合取语义：门控打开且运行时支持，两者缺一不可。
// 分别覆盖“只开一边”的两种情况，确保实现不会退化成只看其中一个条件。
func TestDiscoverFeature(t *testing.T) {
	// 各用例分别覆盖：门控关闭（runtime 支持也无用）、门控开启但 runtime 不支持、
	// 两者都满足，以及仅 runtime 侧开关变化的情况。
	tests := []struct {
		name            string
		featureGate     bool
		runtimeFeatures types.RuntimeFeatures
		expected        bool
	}{
		{
			name:        "feature gate disabled",
			featureGate: false,
			runtimeFeatures: types.RuntimeFeatures{
				UserNamespacesHostNetwork: true,
			},
			expected: false,
		},
		{
			name:        "feature gate enabled but no runtime support",
			featureGate: true,
			runtimeFeatures: types.RuntimeFeatures{
				UserNamespacesHostNetwork: false,
			},
			expected: false,
		},
		{
			name:        "feature gate enabled and runtime supports it",
			featureGate: true,
			runtimeFeatures: types.RuntimeFeatures{
				UserNamespacesHostNetwork: true,
			},
			expected: true,
		},
		{
			name:        "runtime support is on",
			featureGate: true,
			runtimeFeatures: types.RuntimeFeatures{
				UserNamespacesHostNetwork: true,
			},
			expected: true,
		},
		{
			name:        "runtime support is off",
			featureGate: true,
			runtimeFeatures: types.RuntimeFeatures{
				UserNamespacesHostNetwork: false,
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFG := &fakeFeatureGate{
				features: map[string]bool{
					UserNamespacesHostNetworkSupportFeatureGate: tt.featureGate,
				},
			}

			cfg := &types.NodeConfiguration{
				FeatureGates:    mockFG,
				RuntimeFeatures: tt.runtimeFeatures,
			}

			result := Feature.Discover(cfg)
			if result != tt.expected {
				t.Fatalf("Feature.Discover() = %v, want %v", result, tt.expected)
			}
		})
	}
}

// TestRequirements 验证 Requirements 与 Discover 的依赖保持一致：
// 既声明了特性门控，也声明了必需的运行时能力。否则调度侧可能误判哪些节点满足要求。
func TestRequirements(t *testing.T) {
	reqs := Feature.Requirements()
	if reqs == nil {
		t.Fatalf("Requirements returned nil")
	}
	if len(reqs.EnabledFeatureGates) != 1 || reqs.EnabledFeatureGates[0] != UserNamespacesHostNetworkSupportFeatureGate {
		t.Fatalf("unexpected required feature gates: %v", reqs.EnabledFeatureGates)
	}
	if reqs.RequiredRuntimeFeatures == nil || !reqs.RequiredRuntimeFeatures.UserNamespacesHostNetwork {
		t.Fatalf("unexpected required runtime features: %v", reqs.RequiredRuntimeFeatures)
	}
}

// TestName 验证对外声明的特性名与常量 UserNamespacesHostNetworkSupport 一致。
// TestName 验证对外声明的特性名与 UserNamespacesHostNetworkSupport 常量一致，
// 这是节点声明与外部文档之间的契约，不能被静默改写。
// TestName 校验 Feature.Name 返回的名字与声明常量一致。
// TestName 验证 Feature.Name 与 UserNamespacesHostNetworkSupport 常量一致。
func TestName(t *testing.T) {
	if Feature.Name() != UserNamespacesHostNetworkSupport {
		t.Fatalf("expected Name to be %s, got %s", UserNamespacesHostNetworkSupport, Feature.Name())
	}
}

// TestInferForScheduling 验证只有当 Pod 同时使用 hostNetwork 且显式禁用 hostUsers 时，
// 调度才需要这个特性。用指针区分“未设置”与显式 false：nil 表示默认值（共享宿主用户命名空间），
// 不应触发该特性的需求，这是这里保留 nil 用例的原因。
func TestInferForScheduling(t *testing.T) {
	falseVal := false
	trueVal := true

	// 四个用例覆盖 hostNetwork 与 hostUsers 的组合：
	// 只有 HostNetwork=true 且 HostUsers=false（显式启用 user namespace）才需要该能力；
	// HostUsers=nil 代表默认（共享宿主命名空间），不应触发。
	tests := []struct {
		name        string
		hostNetwork bool
		hostUsers   *bool
		expected    bool
	}{
		{
			name:        "HostNetwork true, HostUsers false",
			hostNetwork: true,
			hostUsers:   &falseVal,
			expected:    true,
		},
		{
			name:        "HostNetwork true, HostUsers nil",
			hostNetwork: true,
			hostUsers:   nil,
			expected:    false,
		},
		{
			name:        "HostNetwork true, HostUsers true",
			hostNetwork: true,
			hostUsers:   &trueVal,
			expected:    false,
		},
		{
			name:        "HostNetwork false, HostUsers false",
			hostNetwork: false,
			hostUsers:   &falseVal,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			podSpec := &v1.PodSpec{
				HostNetwork: tt.hostNetwork,
				HostUsers:   tt.hostUsers,
			}
			podInfo := &types.PodInfo{Spec: podSpec}
			result := Feature.InferForScheduling(podInfo)
			if result != tt.expected {
				t.Fatalf("InferForScheduling() = %v, want %v", result, tt.expected)
			}
		})
	}
}
