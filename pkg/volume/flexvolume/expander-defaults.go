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

package flexvolume

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// expanderDefaults 是未实现扩容能力的 flexVolume 插件使用的默认扩展器。
type expanderDefaults struct {
	plugin *flexVolumePlugin
}

// newExpanderDefaults 为指定插件创建默认扩展器。
func newExpanderDefaults(plugin *flexVolumePlugin) *expanderDefaults {
	return &expanderDefaults{plugin}
}

// ExpandVolumeDevice 直接返回目标大小：默认实现不会在设备层做任何实际扩容。
func (e *expanderDefaults) ExpandVolumeDevice(spec *volume.Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error) {
	klog.Warning(logPrefix(e.plugin), "using default expand for volume ", spec.Name(), ", to size ", newSize, " from ", oldSize)
	return newSize, nil
}

// NodeExpand 的默认实现返回一个通用的扩容指示，触发 operation executor 执行通用文件系统扩容。
func (e *expanderDefaults) NodeExpand(rsOpt volume.NodeResizeOptions) (bool, error) {
	klog.Warning(logPrefix(e.plugin), "using default filesystem resize for volume ", rsOpt.VolumeSpec.Name(), ", at ", rsOpt.DevicePath)
	fsVolume, err := util.CheckVolumeModeFilesystem(rsOpt.VolumeSpec)
	if err != nil {
		return false, fmt.Errorf("error checking VolumeMode: %v", err)
	}
	// 非文件系统卷无需调整文件系统大小。
	if !fsVolume {
		return true, nil
	}

	_, err = util.GenericResizeFS(e.plugin.host, rsOpt.DevicePath, rsOpt.DeviceMountPath)
	if err != nil {
		return false, err
	}
	return true, nil
}
