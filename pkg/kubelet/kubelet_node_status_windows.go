//go:build windows

/*
Copyright 2019 The Kubernetes Authors.

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
	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/kubelet/winstats"
)

// getOSSpecificLabels 返回 Windows 构建版本标签（kubernetes.io/os 之外的 Windows 专有标签）。
func getOSSpecificLabels() (map[string]string, error) {
	osInfo, err := winstats.GetOSInfo()
	if err != nil {
		return nil, err
	}

	return map[string]string{v1.LabelWindowsBuild: osInfo.GetBuild()}, nil
}

// runningInUserNS 在 Windows 上不支持用户命名空间，始终返回 nil。
func (kl *Kubelet) runningInUserNS() *bool {
	return nil
}
