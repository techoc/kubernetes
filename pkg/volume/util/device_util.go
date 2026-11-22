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

package util

// DeviceUtil 提供常用设备查询操作的工具接口。
type DeviceUtil interface {
	// FindMultipathDeviceForDevice 查找设备所属的多路径父设备。
	FindMultipathDeviceForDevice(disk string) string
	// FindSlaveDevicesOnMultipath 返回多路径设备管理的从属设备列表。
	FindSlaveDevicesOnMultipath(disk string) []string
	// GetISCSIPortalHostMapForTarget 返回 target 对应的 iSCSI portal 与 SCSI 主机号映射。
	GetISCSIPortalHostMapForTarget(targetIqn string) (map[string]int, error)
	// FindDevicesForISCSILun 返回 target 上指定 LUN 对应的所有块设备。
	FindDevicesForISCSILun(targetIqn string, lun int) ([]string, error)
}

// deviceHandler 通过注入的 IoUtil 访问 sysfs，便于测试。
type deviceHandler struct {
	getIo IoUtil
}

// NewDeviceHandler 创建一个使用给定 IoUtil 的 DeviceUtil 实现。
func NewDeviceHandler(io IoUtil) DeviceUtil {
	return &deviceHandler{getIo: io}
}
