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

package features_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/component-helpers/nodedeclaredfeatures/features"
	testhelpers "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	"k8s.io/component-helpers/nodedeclaredfeatures/types"
)

// TestFeatureRequirementsConsistency 校验每个特性的 Discover 方法
// 确实检查了它在 Requirements 中声明的那些门控，并返回正确的布尔值。
// 这是“声明”与“实现”之间的一致性契约：调度侧只依据 Requirements 推断，
// 一旦 Discover 漏查某个门控，声明就会失真。
func TestFeatureRequirementsConsistency(t *testing.T) {
	for _, registeredFeature := range features.AllFeatures {
		t.Run(registeredFeature.Name(), func(t *testing.T) {
			reqs := registeredFeature.Requirements()
			if reqs == nil {
				t.Fatalf("Feature %s returned nil Requirements", registeredFeature.Name())
			}

			if len(reqs.EnabledFeatureGates) == 0 {
				t.Fatalf("Feature %s must declare at least one feature gate in Requirements", registeredFeature.Name())
			}

			// 场景一：Requirements 声明的门全部打开
			// 先把 Requirements 声明的门全部打开，Discover 必须为真；
			// 否则说明声明与实现不一致，调度侧按声明做的推断就会落空。
			mockFG := testhelpers.NewMockFeatureGate(t)
			for _, gate := range reqs.EnabledFeatureGates {
				mockFG.SetEnabled(gate, true)
			}

			discoverCfg := &types.NodeConfiguration{
				FeatureGates: mockFG,
				Version:      version.MustParse("1.36.0"),
			}
			if reqs.RequiredRuntimeFeatures != nil {
				discoverCfg.RuntimeFeatures = *reqs.RequiredRuntimeFeatures
			}

			featureEnabled := registeredFeature.Discover(discoverCfg)
			if !featureEnabled {
				t.Fatalf("Discover should return true when all requirements are met for feature %s", registeredFeature.Name())
			}

			// 场景二：逐个关闭依赖门
			// 逐个关掉每个依赖门，Discover 必须为假：
			// 这些门是“与”关系，只要缺一个节点就不该声明该特性。
			gateToDisable := reqs.EnabledFeatureGates[0]
			disabledMockFG := testhelpers.NewMockFeatureGate(t)
			for _, gate := range reqs.EnabledFeatureGates {
				expectedReturn := true
				if gate == gateToDisable {
					expectedReturn = false
				}
				disabledMockFG.SetEnabled(gate, expectedReturn)
			}

			discoverCfg.FeatureGates = disabledMockFG
			featureEnabled = registeredFeature.Discover(discoverCfg)
			if featureEnabled {
				t.Fatalf("Discover should return false when gate %s is disabled for feature %s", gateToDisable, registeredFeature.Name())
			}

		})
	}
}
