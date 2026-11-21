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

// TestRecommendedDefaultResourceClaimControllerConfiguration 验证推荐默认值生效：
// 传入零值配置后，并发同步数应被设置为 50。
func TestRecommendedDefaultResourceClaimControllerConfiguration(t *testing.T) {
	// 从零值开始，确保被测函数确实写入了默认值，而不是保留了输入值。
	config := new(kubectrlmgrconfigv1alpha1.ResourceClaimControllerConfiguration)
	RecommendedDefaultResourceClaimControllerConfiguration(config)
	// 断言默认值精确为 50；默认值变更时须同步更新此测试。
	if config.ConcurrentSyncs != 50 {
		t.Errorf("incorrect default value, expected 50 but got %v", config.ConcurrentSyncs)
	}
}
