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
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/component-base/featuregate" // Import for the type conversion
	"k8s.io/component-helpers/nodedeclaredfeatures"
)

// FeatureGateAdapter 把 component-base 的 FeatureGate 适配为 nodedeclaredfeatures 的 FeatureGate 接口。
// 由于库代码中不能使用 featuregate.Feature（禁止的导入），需要这层适配。
type FeatureGateAdapter struct {
	featuregate.FeatureGate
}

// Enabled 实现 nodedeclaredfeatures.FeatureGate 接口。
func (a FeatureGateAdapter) Enabled(key string) bool {
	// 把字符串 key 转换为 featuregate.Feature 后查询。
	return a.FeatureGate.Enabled(featuregate.Feature(key))
}

// discoverNodeDeclaredFeatures 使用发现库计算最终要声明的节点特性集合。
func (kl *Kubelet) discoverNodeDeclaredFeatures() []string {
	// 适配默认特性门控，供节点声明特性库按字符串查询特性状态。
	adaptedFG := FeatureGateAdapter{FeatureGate: utilfeature.DefaultFeatureGate}

	runtimeFeatures := nodedeclaredfeatures.RuntimeFeatures{}
	if features := kl.runtimeState.runtimeFeatures(); features != nil {
		runtimeFeatures.UserNamespacesHostNetwork = features.UserNamespacesHostNetwork
		runtimeFeatures.MountOptions = features.MountOptions
		// 只传递运行时探测到的特性，避免把未探测项误报为支持。
	}

	cfg := &nodedeclaredfeatures.NodeConfiguration{
		FeatureGates:    adaptedFG,
		Version:         kl.version,
		RuntimeFeatures: runtimeFeatures,
	}
	return kl.nodeDeclaredFeaturesFramework.DiscoverNodeFeatures(cfg)
}
