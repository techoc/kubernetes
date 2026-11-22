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
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/volume"
)

type flexVolumeAttacher struct {
	plugin *flexVolumeAttachablePlugin
}

var _ volume.Attacher = &flexVolumeAttacher{}

var _ volume.DeviceMounter = &flexVolumeAttacher{}

// Attach 调用驱动的 attach 命令；驱动不支持时回退到默认实现。
func (a *flexVolumeAttacher) Attach(spec *volume.Spec, hostName types.NodeName) (string, error) {

	call := a.plugin.NewDriverCall(attachCmd)
	call.AppendSpec(spec, a.plugin.host, nil)
	call.Append(string(hostName))

	status, err := call.Run()
	if isCmdNotSupportedErr(err) {
		return (*attacherDefaults)(a).Attach(spec, hostName)
	} else if err != nil {
		return "", err
	}
	return status.DevicePath, err
}

// WaitForAttach 调用驱动的 waitForAttach 命令并等待完成。
func (a *flexVolumeAttacher) WaitForAttach(spec *volume.Spec, devicePath string, _ *v1.Pod, timeout time.Duration) (string, error) {
	call := a.plugin.NewDriverCallWithTimeout(waitForAttachCmd, timeout)
	call.Append(devicePath)
	call.AppendSpec(spec, a.plugin.host, nil)

	status, err := call.Run()
	if isCmdNotSupportedErr(err) {
		return (*attacherDefaults)(a).WaitForAttach(spec, devicePath, timeout)
	} else if err != nil {
		return "", err
	}
	return status.DevicePath, nil
}

// GetDeviceMountPath 返回设备在该节点上的全局挂载路径。
func (a *flexVolumeAttacher) GetDeviceMountPath(spec *volume.Spec) (string, error) {
	return a.plugin.getDeviceMountPath(spec)
}

// MountDevice 将设备挂载到全局路径，重复调用直接跳过。
func (a *flexVolumeAttacher) MountDevice(spec *volume.Spec, devicePath string, deviceMountPath string, _ volume.DeviceMounterArgs) error {
	// 已经挂载过就不再重复调用驱动。
	alreadyMounted, err := prepareForMount(a.plugin.host.GetMounter(), deviceMountPath)
	if err != nil {
		return err
	}
	if alreadyMounted {
		return nil
	}

	call := a.plugin.NewDriverCall(mountDeviceCmd)
	call.Append(deviceMountPath)
	call.Append(devicePath)
	call.AppendSpec(spec, a.plugin.host, nil)

	_, err = call.Run()
	if isCmdNotSupportedErr(err) {
		// 不支持 attach 的驱动其 devicePath 为空，此时无需调用 mountDevice。
		if devicePath != "" {
			return (*attacherDefaults)(a).MountDevice(spec, devicePath, deviceMountPath, a.plugin.host.GetMounter())
		}
		return nil
	}
	return err
}

// VolumesAreAttached 逐个调用驱动 isAttached，检查卷是否仍挂载在指定节点。
func (a *flexVolumeAttacher) VolumesAreAttached(specs []*volume.Spec, nodeName types.NodeName) (map[*volume.Spec]bool, error) {
	volumesAttachedCheck := make(map[*volume.Spec]bool)
	for _, spec := range specs {
		volumesAttachedCheck[spec] = true

		call := a.plugin.NewDriverCall(isAttached)
		call.AppendSpec(spec, a.plugin.host, nil)
		call.Append(string(nodeName))

		status, err := call.Run()
		if isCmdNotSupportedErr(err) {
			return nil, nil
		} else if err == nil {
			if !status.Attached {
				volumesAttachedCheck[spec] = false
				klog.V(2).Infof("VolumesAreAttached: check volume (%q) is no longer attached", spec.Name())
			}
		} else {
			return nil, err
		}
	}
	return volumesAttachedCheck, nil
}
