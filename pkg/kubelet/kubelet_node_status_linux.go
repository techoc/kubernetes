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

package kubelet

import (
	"github.com/moby/sys/userns"

	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/pkg/features"
)

// getOSSpecificLabels 在 Linux 上没有额外的操作系统相关节点标签。
func getOSSpecificLabels() (map[string]string, error) {
	return nil, nil
}

// runningInUserNS 在 kubelet 运行于用户命名空间时返回指向 true 的指针；
// 未启用 KubeletInUserNamespace 特性时返回 nil。
func (kl *Kubelet) runningInUserNS() *bool {
	if !utilfeature.DefaultFeatureGate.Enabled(features.KubeletInUserNamespace) {
		return nil
	}
	return new(userns.RunningInUserNS())
}
