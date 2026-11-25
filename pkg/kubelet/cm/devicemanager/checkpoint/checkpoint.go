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

package checkpoint

import (
	"encoding/json"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager"
	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/checksum"
)

// DeviceManagerCheckpoint 定义读取 Pod 设备分配信息的操作。
type DeviceManagerCheckpoint interface {
	checkpointmanager.Checkpoint
	// GetData 返回 Pod 设备条目与已注册设备。
	GetData() ([]PodDevicesEntry, map[string][]string)
}

// DevicesPerNUMA 表示设备插件按 NUMA 节点上报的设备 ID。
type DevicesPerNUMA map[int64][]string

// PodDevicesEntry 关联 Pod 信息与设备分配。
type PodDevicesEntry struct {
	// PodUID 是 Pod 的 UID。
	PodUID string
	// ContainerName 是容器名称。
	ContainerName string
	// ResourceName 是设备资源名称。
	ResourceName string
	// DeviceIDs 是各 NUMA 节点上的设备 ID。
	DeviceIDs DevicesPerNUMA
	// AllocResp 是设备插件返回的分配响应。
	AllocResp []byte
}

// checkpointData 用于在 checkpoint 文件中保存 Pod 到设备的分配信息。
// TODO：需要变更 checkpoint 格式时增加版本控制。
type checkpointData struct {
	PodDeviceEntries  []PodDevicesEntry
	RegisteredDevices map[string][]string
}

// Data 保存 checkpoint 数据及其 checksum。
type Data struct {
	// Data 是实际的 checkpoint 内容。
	Data checkpointData
	// Checksum 是 Data 的校验和。
	Checksum checksum.Checksum
}

// NewDevicesPerNUMA 创建空的 DevicesPerNUMA 映射。
func NewDevicesPerNUMA() DevicesPerNUMA {
	return make(DevicesPerNUMA)
}

// Devices 返回所有 NUMA 节点的设备 ID 集合。
func (dev DevicesPerNUMA) Devices() sets.Set[string] {
	result := sets.New[string]()

	// 合并所有 NUMA 节点的设备 ID。
	for _, devs := range dev {
		result.Insert(devs...)
	}
	return result
}

// New 返回 Checkpoint 实例，始终指向最新版本。
func New(devEntries []PodDevicesEntry, devices map[string][]string) DeviceManagerCheckpoint {
	return newV2(devEntries, devices)
}

// newV2 创建 v2 版本的 checkpoint。
func newV2(devEntries []PodDevicesEntry, devices map[string][]string) DeviceManagerCheckpoint {
	return &Data{
		Data: checkpointData{
			PodDeviceEntries:  devEntries,
			RegisteredDevices: devices,
		},
	}
}

// MarshalCheckpoint 序列化 checkpoint 数据。
func (cp *Data) MarshalCheckpoint() ([]byte, error) {
	// 序列化前重新计算 checksum，保证与 Data 一致。
	cp.Checksum = checksum.New(cp.Data)
	return json.Marshal(*cp)
}

// UnmarshalCheckpoint 反序列化 checkpoint 数据。
func (cp *Data) UnmarshalCheckpoint(blob []byte) error {
	return json.Unmarshal(blob, cp)
}

// VerifyChecksum 校验存储的 checksum 与重新计算的结果是否一致。
func (cp *Data) VerifyChecksum() error {
	return cp.Checksum.Verify(cp.Data)
}

// GetData 以最新 checkpoint 格式返回设备条目与已注册设备，
// 而不是磁盘上保存的原始格式。
func (cp *Data) GetData() ([]PodDevicesEntry, map[string][]string) {
	return cp.Data.PodDeviceEntries, cp.Data.RegisteredDevices
}
