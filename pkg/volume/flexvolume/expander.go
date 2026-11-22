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
	"strconv"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/kubernetes/pkg/volume"
)

// ExpandVolumeDevice 调用驱动扩容底层块设备，驱动不支持该命令时回退到默认实现。
func (plugin *flexVolumePlugin) ExpandVolumeDevice(spec *volume.Spec, newSize resource.Quantity, oldSize resource.Quantity) (resource.Quantity, error) {
	call := plugin.NewDriverCall(expandVolumeCmd)
	call.AppendSpec(spec, plugin.host, nil)

	devicePath, err := plugin.getDeviceMountPath(spec)
	if err != nil {
		return newSize, err
	}
	call.Append(devicePath)
	call.Append(strconv.FormatInt(newSize.Value(), 10))
	call.Append(strconv.FormatInt(oldSize.Value(), 10))

	_, err = call.Run()
	// 老驱动可能没有实现 expand-volume，回退到默认（无操作）实现以兼容它们。
	if isCmdNotSupportedErr(err) {
		return newExpanderDefaults(plugin).ExpandVolumeDevice(spec, newSize, oldSize)
	}
	return newSize, err
}

// NodeExpand 调用驱动扩容节点上的文件系统，驱动不支持该命令时回退到默认实现。
func (plugin *flexVolumePlugin) NodeExpand(rsOpt volume.NodeResizeOptions) (bool, error) {
	// 调用时 PV 的 Capacity 已经更新为目标大小，因此驱动拿到的是新容量，
	// 当前实际大小需要驱动自己从底层存储查询。

	if rsOpt.VolumeSpec.PersistentVolume == nil {
		return false, fmt.Errorf("PersistentVolume not found for spec: %s", rsOpt.VolumeSpec.Name())
	}

	call := plugin.NewDriverCall(expandFSCmd)
	call.AppendSpec(rsOpt.VolumeSpec, plugin.host, nil)
	call.Append(rsOpt.DevicePath)
	call.Append(rsOpt.DeviceMountPath)
	call.Append(strconv.FormatInt(rsOpt.NewSize.Value(), 10))
	call.Append(strconv.FormatInt(rsOpt.OldSize.Value(), 10))

	_, err := call.Run()
	// 同 ExpandVolumeDevice：未实现 expand-fs 的驱动走默认实现。
	if isCmdNotSupportedErr(err) {
		return newExpanderDefaults(plugin).NodeExpand(rsOpt)
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
