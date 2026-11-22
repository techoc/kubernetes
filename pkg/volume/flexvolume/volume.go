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

package flexvolume

import (
	"k8s.io/mount-utils"
	utilstrings "k8s.io/utils/strings"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/kubernetes/pkg/volume"
)

// flexVolume 表示已附加到某个 Pod 的 flexvolume 卷实例。
type flexVolume struct {
	// driverName 是插件驱动的名称。
	driverName string
	// execPath 是用于配置卷的驱动可执行文件路径。
	execPath string
	// mounter 提供挂载实际块设备所需的接口。
	mounter mount.Interface
	// podName 是 Pod 名称（如果可用）。
	podName string
	// podUID 是 Pod 的 UID。
	podUID types.UID
	// podNamespace 是 Pod 的命名空间（如果可用）。
	podNamespace string
	// podServiceAccountName 是 Pod 的 ServiceAccount 名称（如果可用）。
	podServiceAccountName string
	// volName 是 Pod 中该卷的名称。
	volName string
	// plugin 是底层插件。
	plugin *flexVolumePlugin
	// MetricsProvider 提供卷指标能力。
	volume.MetricsProvider
}

// 实现 volume.Volume 接口。

// GetPath 返回该卷在 Pod 目录中的挂载路径。
func (f *flexVolume) GetPath() string {
	name := f.driverName
	return f.plugin.host.GetPodVolumeDir(f.podUID, utilstrings.EscapeQualifiedName(name), f.volName)
}
