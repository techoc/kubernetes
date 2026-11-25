/*
Copyright 2016 The Kubernetes Authors.

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

package storage

import (
	"fmt"
	"os"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/test/e2e/framework"
)

var (
	// 测试中使用的非管理员 Windows 用户。这是 Linux 非 root UID 用法在 Windows 上的等价做法。
	nonAdminTestUserName = "ContainerUser"
	// 测试中使用的非 root UID。
	nonRootTestUserID = int64(1000)
)

// setPodNonRootUser 将 Pod 配置为以非 root 用户运行。
// 对 Windows，将 RunAsUserName 字段设为 ContainerUser；对 Linux，将 RunAsUser 字段设为 1000。
func setPodNonRootUser(pod *v1.Pod) {
	if framework.NodeOSDistroIs("windows") {
		pod.Spec.SecurityContext.WindowsOptions = &v1.WindowsSecurityContextOptions{RunAsUserName: &nonAdminTestUserName}
	} else {
		pod.Spec.SecurityContext.RunAsUser = &nonRootTestUserID
	}
}

// getFileModeRegex 返回一个与文件 mode 相关的正则，mounttest Pod 的输出应能匹配它。
// 如果给定的 mask 为 nil，正则将包含默认的 OS 文件 mode：Linux 为 0644，Windows 为 0775。
func getFileModeRegex(filePath string, mask *int32) string {
	var (
		linuxMask   int32
		windowsMask int32
	)
	if mask == nil {
		linuxMask = int32(0644)
		windowsMask = int32(0775)
	} else {
		linuxMask = *mask
		windowsMask = *mask
	}

	linuxOutput := fmt.Sprintf("mode of file \"%s\": %v", filePath, os.FileMode(linuxMask))
	windowsOutput := fmt.Sprintf("mode of Windows file \"%v\": %s", filePath, os.FileMode(windowsMask))

	return fmt.Sprintf("(%s|%s)", linuxOutput, windowsOutput)
}

// createMounts 创建一个只含单个元素的 v1.VolumeMount 列表。
func createMounts(volumeName, volumeMountPath string, readOnly bool) []v1.VolumeMount {
	return []v1.VolumeMount{
		{
			Name:      volumeName,
			MountPath: volumeMountPath,
			ReadOnly:  readOnly,
		},
	}
}
