//go:build !windows && !linux

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

package validation

import (
	"fmt"

	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// validateKubeletOSConfiguration 校验操作系统相关的 kubelet 配置，非法时返回错误。
// 非 Linux 平台不支持单进程 OOM kill、用户命名空间和默认 Pod sysctl。
func validateKubeletOSConfiguration(kc *kubeletconfig.KubeletConfiguration) error {
	if kc.SingleProcessOOMKill != nil {
		return fmt.Errorf("invalid configuration: singleProcessOOMKill is only supported on linux")
	}

	if kc.UserNamespaces != nil {
		return fmt.Errorf("invalid configuration: userNamespaces is only supported on linux")
	}

	if len(kc.DefaultPodSysctls) > 0 {
		return fmt.Errorf("invalid configuration: defaultPodSysctls is only supported on linux")
	}

	return nil
}
