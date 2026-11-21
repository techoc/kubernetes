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

package v1alpha1

import (
	"testing"

	kubectrlmgrconfigv1alpha1 "k8s.io/kube-controller-manager/config/v1alpha1"
)

// TestRecommendedDefaultDeviceTaintEvictionControllerConfiguration 验证
// 默认配置函数会把并发同步数设置为推荐的 8。
func TestRecommendedDefaultDeviceTaintEvictionControllerConfiguration(t *testing.T) {
	// 从零值配置开始，确保被检查的值确实来自默认函数而非预设值。
	config := new(kubectrlmgrconfigv1alpha1.DeviceTaintEvictionControllerConfiguration)
	RecommendedDefaultDeviceTaintEvictionControllerConfiguration(config)
	if config.ConcurrentSyncs != 8 {
		t.Errorf("incorrect default value, expected 8 but got %v", config.ConcurrentSyncs)
	}
}
