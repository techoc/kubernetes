/*
Copyright 2018 The Kubernetes Authors.

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

package phases

import (
	"k8s.io/component-base/version"

	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
)

// SetKubernetesVersion 获取当前 kubeadm 版本并设置为配置中的 KubeadmVersion，
// 但若已设置为非默认值则保留。
func SetKubernetesVersion(cfg *kubeadmapiv1.ClusterConfiguration) {

	if cfg.KubernetesVersion != kubeadmapiv1.DefaultKubernetesVersion && cfg.KubernetesVersion != "" {
		return
	}
	cfg.KubernetesVersion = version.Get().String()
}
