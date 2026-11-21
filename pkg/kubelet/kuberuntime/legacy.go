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

package kuberuntime

import (
	"fmt"
	"path/filepath"
	"strings"

	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// 本文件实现向后兼容所需的函数，因此直接导入了多个 kubernetes 包。

const (
	// legacyContainerLogsDir 是容器日志的旧路径，与 kubelet.containerLogsDir 相同。
	legacyContainerLogsDir = "/var/log/containers"
	// legacyLogSuffix 是旧版日志文件的后缀。
	legacyLogSuffix = "log"

	// ext4MaxFileNameLen 是 Linux ext4 允许的最大文件名长度。
	ext4MaxFileNameLen = 255
)

// legacyLogSymlink 拼接旧版容器日志路径，仅用于旧集群的日志支持。
func legacyLogSymlink(containerID string, containerName, podName, podNamespace string) string {
	return logSymlink(legacyContainerLogsDir, kubecontainer.BuildPodFullName(podName, podNamespace),
		containerName, containerID)
}

// getContainerIDFromLegacyLogSymlink 从旧版日志软链中解析容器 ID，解析失败时返回错误。
func getContainerIDFromLegacyLogSymlink(logSymlink string) (string, error) {
	parts := strings.Split(logSymlink, "-")
	if len(parts) == 0 {
		return "", fmt.Errorf("unable to find separator in %q", logSymlink)
	}
	containerIDWithSuffix := parts[len(parts)-1]
	suffix := fmt.Sprintf(".%s", legacyLogSuffix)
	if !strings.HasSuffix(containerIDWithSuffix, suffix) {
		return "", fmt.Errorf("%q doesn't end with %q", logSymlink, suffix)
	}
	containerIDWithoutSuffix := strings.TrimSuffix(containerIDWithSuffix, suffix)
	// 容器 ID 最短 6 个字符即可用于查找容器。
	if len(containerIDWithoutSuffix) < 6 {
		return "", fmt.Errorf("container Id %q is too short", containerIDWithoutSuffix)
	}
	return containerIDWithoutSuffix, nil
}

// logSymlink 拼接容器日志软链路径，并在超过 ext4 文件名上限时截断。
func logSymlink(containerLogsDir, podFullName, containerName, containerID string) string {
	suffix := fmt.Sprintf(".%s", legacyLogSuffix)
	logPath := fmt.Sprintf("%s_%s-%s", podFullName, containerName, containerID)
	// Linux ext4 上文件名不能超过 255 个字符，超长时截断。
	if len(logPath) > ext4MaxFileNameLen-len(suffix) {
		logPath = logPath[:ext4MaxFileNameLen-len(suffix)]
	}
	return filepath.Join(containerLogsDir, logPath+suffix)
}
