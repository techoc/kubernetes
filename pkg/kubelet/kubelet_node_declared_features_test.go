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

package kubelet

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	ndf "k8s.io/component-helpers/nodedeclaredfeatures"
	ndffeatures "k8s.io/component-helpers/nodedeclaredfeatures/features"
	ndftesting "k8s.io/component-helpers/nodedeclaredfeatures/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/cm"
)

// TestDeclaredFeatureDiscovery 验证节点声明特性发现同时受特性门控状态和 kubelet 版本上限约束。
func TestDeclaredFeatureDiscovery(t *testing.T) {
	// 以 InPlacePodLevelResourcesVerticalScaling 作为被发现的示例特性。
	podLevelResourcesIPPRFeatureGate := features.InPlacePodLevelResourcesVerticalScaling
	// 固定特性的最大支持版本，便于构造低于和高于该版本的 kubelet 版本。
	featureMaxVersion := version.MustParseSemantic("v1.36.0")
	// createMockFeature 构造一个仅依据门控状态判断是否可发现的模拟特性。
	createMockFeature := func(t *testing.T, name string, cfg *ndf.NodeConfiguration) *ndftesting.MockFeature {
		mockFeature := ndftesting.NewMockFeature(t)
		mockFeature.SetName(name)
		mockFeature.SetMaxVersion(featureMaxVersion)
		mockFeature.SetDiscover(func(*ndf.NodeConfiguration) bool { return cfg.FeatureGates.Enabled(name) })
		return mockFeature
	}

	// 覆盖门控开启/关闭以及 kubelet 版本高于特性最大版本三种组合。
	testcases := []struct {
		name                         string
		podLevelResourcesIPPREnabled bool
		featureDiscovered            bool
		kubeletVersion               *version.Version
	}{
		{
			name:                         "feature gate enabled, feature discovered",
			podLevelResourcesIPPREnabled: true,
			featureDiscovered:            true,
			kubeletVersion:               featureMaxVersion.SubtractMinor(1),
		},
		{
			name:                         "feature gate enabled, feature not discovered as kubelet version higher than max version",
			podLevelResourcesIPPREnabled: true,
			featureDiscovered:            false,
			kubeletVersion:               featureMaxVersion.AddMinor(1),
		},
		{
			name:                         "feature disabled and not discovered",
			podLevelResourcesIPPREnabled: false,
			featureDiscovered:            false,
			kubeletVersion:               featureMaxVersion.SubtractMinor(1),
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 每个子测试独立设置门控，避免用例之间相互影响。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, podLevelResourcesIPPRFeatureGate, tc.podLevelResourcesIPPREnabled)

			cfg := &ndf.NodeConfiguration{
				FeatureGates: FeatureGateAdapter{
					FeatureGate: utilfeature.DefaultFeatureGate,
				},
				Version: tc.kubeletVersion,
			}
			registeredFeatures := []ndf.Feature{
				createMockFeature(t, string(podLevelResourcesIPPRFeatureGate), cfg),
			}

			// 使用真实 kubelet 结构，并替换容器管理器与特性发现框架。
			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			defer testKubelet.Cleanup()
			kubelet := testKubelet.kubelet
			fakeCM := cm.NewFakeContainerManagerWithNodeConfig(cm.NodeConfig{})
			kubelet.containerManager = fakeCM
			// 只注册模拟特性，确保发现结果只受该特性影响。
			framework := ndf.New(registeredFeatures)
			kubelet.nodeDeclaredFeaturesFramework = framework
			kubelet.version = tc.kubeletVersion

			features := kubelet.discoverNodeDeclaredFeatures()
			if tc.featureDiscovered {
				assert.Contains(t, features, string(podLevelResourcesIPPRFeatureGate))
			} else {
				assert.NotContains(t, features, string(podLevelResourcesIPPRFeatureGate))
			}
		})
	}
}

// TestExtendWebSocketsToKubeletFeatureDiscovery 验证 ExtendWebSocketsToKubelet 门控开启时该特性会被节点声明。
func TestExtendWebSocketsToKubeletFeatureDiscovery(t *testing.T) {
	// 门控开启与关闭两种场景。
	testcases := []struct {
		name                          string
		extendWebSocketsToKubeletGate bool
		expectFeature                 bool
	}{
		{
			name:                          "ExtendWebSocketsToKubelet feature enabled",
			extendWebSocketsToKubeletGate: true,
			expectFeature:                 true,
		},
		{
			name:                          "ExtendWebSocketsToKubelet feature disabled",
			extendWebSocketsToKubeletGate: false,
			expectFeature:                 false,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			// 按用例设置门控状态。
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.ExtendWebSocketsToKubelet, tc.extendWebSocketsToKubeletGate)

			testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
			defer testKubelet.Cleanup()
			kubelet := testKubelet.kubelet

			// 注册全部真实特性，验证默认特性集合中的该项发现结果。
			framework := ndf.New(ndffeatures.AllFeatures)
			kubelet.nodeDeclaredFeaturesFramework = framework

			features := kubelet.discoverNodeDeclaredFeatures()
			if tc.expectFeature {
				assert.Contains(t, features, "ExtendWebSocketsToKubelet")
			} else {
				assert.NotContains(t, features, "ExtendWebSocketsToKubelet")
			}
		})
	}
}
