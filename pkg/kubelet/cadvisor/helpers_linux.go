//go:build linux

/*
Copyright 2017 The Kubernetes Authors.

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

package cadvisor

import (
	"fmt"
	"strings"

	cadvisorfs "github.com/google/cadvisor/lib/fs"
)

// imageFsInfoProvider 能把配置的运行时转换为对应的镜像文件系统标签。
type imageFsInfoProvider struct {
	runtimeEndpoint string
}

// ImageFsInfoLabel 返回配置运行时的镜像文件系统标签；
// 对远程运行时，也会处理 cAdvisor 原生支持的其他运行时。
func (i *imageFsInfoProvider) ImageFsInfoLabel() (string, error) {
	if detectCrioWorkaround(i) {
		return cadvisorfs.LabelCrioImages, nil
	}
	return "", fmt.Errorf("no imagefs label for configured runtime")
}

// ContainerFsInfoLabel 返回配置运行时的容器文件系统标签；
// 对远程运行时，也会处理 cAdvisor 原生支持的其他运行时。
func (i *imageFsInfoProvider) ContainerFsInfoLabel() (string, error) {
	if detectCrioWorkaround(i) {
		return cadvisorfs.LabelCrioContainers, nil
	}
	return "", fmt.Errorf("no containerfs label for configured runtime")
}

// detectCrioWorkaround 是让 cAdvisor 获取 cri-o 统计数据的临时兼容方案，
// 后续应当移除。参见 https://github.com/kubernetes/kubernetes/issues/51798。
func detectCrioWorkaround(i *imageFsInfoProvider) bool {
	return strings.HasSuffix(i.runtimeEndpoint, CrioSocketSuffix)
}

// NewImageFsInfoProvider 返回指定运行时配置的 ImageFsInfoProvider。
func NewImageFsInfoProvider(runtimeEndpoint string) ImageFsInfoProvider {
	return &imageFsInfoProvider{runtimeEndpoint: runtimeEndpoint}
}
