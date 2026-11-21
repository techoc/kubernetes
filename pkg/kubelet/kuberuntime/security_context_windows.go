//go:build windows

/*
Copyright 2020 The Kubernetes Authors.

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

package kuberuntime

import (
	"context"
	"fmt"
	"strings"

	"k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/util/format"
	"k8s.io/kubernetes/pkg/securitycontext"
)

// Windows 上被视为 root 的用户名；用户名比较不区分大小写。
var (
	windowsRootUserName = "ContainerAdministrator"
)

// verifyRunAsNonRoot 在 Windows 上校验 RunAsNonRoot。
// 参见 https://github.com/kubernetes/enhancements/tree/master/keps/sig-windows/116-windows-node-support#v1container
// Windows 没有 root 用户，无法像 Linux 那样用 uid=0 判断 root 身份。
// 按 sig-windows 的讨论，目前把 ContainerAdministrator 视为 Windows 容器的 root 用户，
// 后续再根据最佳实践优化该逻辑。
// https://docs.google.com/document/d/1Tjxzjjuy4SQsFSUVXZbvqVb64hjNAG5CQX8bK7Yda9w
// 注意：Windows 上的用户名不区分大小写！
func verifyRunAsNonRoot(ctx context.Context, pod *v1.Pod, container *v1.Container, uid *int64, username string) error {
	logger := klog.FromContext(ctx)
	effectiveSc := securitycontext.DetermineEffectiveSecurityContext(pod, container)
	// 未设置该选项或允许以 root 运行时，直接通过。
	if effectiveSc == nil || effectiveSc.RunAsNonRoot == nil || !*effectiveSc.RunAsNonRoot {
		return nil
	}
	if effectiveSc.RunAsUser != nil {
		logger.Info("Windows container does not support SecurityContext.RunAsUser, please use SecurityContext.WindowsOptions",
			"pod", klog.KObj(pod), "containerName", container.Name)
	}
	if effectiveSc.SELinuxOptions != nil {
		logger.Info("Windows container does not support SecurityContext.SELinuxOptions, please use SecurityContext.WindowsOptions",
			"pod", klog.KObj(pod), "containerName", container.Name)
	}
	if effectiveSc.RunAsGroup != nil {
		logger.Info("Windows container does not support SecurityContext.RunAsGroup", "pod", klog.KObj(pod), "containerName", container.Name)
	}
	// 若 Pod 或容器设置了 runAsUserName，确认它不是 ContainerAdministrator。
	if effectiveSc.WindowsOptions != nil {
		if effectiveSc.WindowsOptions.RunAsUserName != nil {
			if strings.EqualFold(*effectiveSc.WindowsOptions.RunAsUserName, windowsRootUserName) {
				return fmt.Errorf("container's runAsUserName (%s) which will be regarded as root identity and will break non-root policy (pod: %q, container: %s)", *effectiveSc.WindowsOptions.RunAsUserName, format.Pod(pod), container.Name)
			}
			return nil
		}
	}
	// 若未设置 runAsUserName，则确认镜像默认用户不是 ContainerAdministrator。
	if len(username) > 0 && strings.EqualFold(username, windowsRootUserName) {
		return fmt.Errorf("container's runAsUser (%s) which will be regarded as root identity and will break non-root policy (pod: %q, container: %s)", username, format.Pod(pod), container.Name)
	}
	return nil
}
