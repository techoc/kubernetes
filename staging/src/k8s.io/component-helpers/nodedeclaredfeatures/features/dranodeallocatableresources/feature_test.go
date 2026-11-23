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

package dranodeallocatableresources

import (
	"testing"

	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestRequirements 防止实现和声明脱节：Requirements 是调度侧推断节点能力的依据，
// 一旦忘记声明特性门，Discover 的结果就无从解释。
func TestRequirements(t *testing.T) {
	feature := &draNodeAllocatableResourcesFeature{}
	if feature.Name() != DRANodeAllocatableResourcesFeature {
		t.Fatalf("expected Name to be %s, got %s", DRANodeAllocatableResourcesFeature, feature.Name())
	}
	reqs := feature.Requirements()
	if reqs == nil {
		t.Fatalf("Feature %s returned nil Requirements", feature.Name())
	}

	if len(reqs.EnabledFeatureGates) != 1 || reqs.EnabledFeatureGates[0] != DRANodeAllocatableResourcesFeature {
		t.Fatalf("Feature %s Requirements should declare exactly the %s feature gate", feature.Name(), DRANodeAllocatableResourcesFeature)
	}
}

// TestDiscover 覆盖特性门开、关两端：该特性完全由门控决定，
// 任何一端出错都会让节点能力声明对整个集群失真。
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

	feature := &draNodeAllocatableResourcesFeature{}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &types.NodeConfiguration{
				FeatureGates: types.FeatureGateMap{DRANodeAllocatableResourcesFeature: tc.featureGateEnabled},
			}
			enabled := feature.Discover(config)
			if want, got := tc.expected, enabled; want != got {
				t.Fatalf("want=%v,got=%v", want, got)
			}
		})
	}
}

// TestInferForSchedulingAndUpdate 锁定两个推断方法恒为 false：
// 节点是否具备该能力只由门控决定，Pod 本身无法表达需求，
// 一旦这里返回 true，就会把不相关的 Pod 也标记为需要该特性。
func TestInferForSchedulingAndUpdate(t *testing.T) {
	feature := &draNodeAllocatableResourcesFeature{}
	podInfo := &types.PodInfo{}
	if feature.InferForScheduling(podInfo) {
		t.Fatalf("expect InferForScheduling to be false")
	}
	if feature.InferForUpdate(nil, podInfo) {
		t.Fatalf("expect InferForUpdate to be false")
	}
}

// TestMaxVersion 断言没有版本上限：该能力与 Kubernetes 版本无关，
// 若误设上限，高版本集群会错误地不再声明这个特性。
func TestMaxVersion(t *testing.T) {
	feature := &draNodeAllocatableResourcesFeature{}
	if feature.MaxVersion() != nil {
		t.Fatalf("expect MaxVersion to be nil")
	}
}
