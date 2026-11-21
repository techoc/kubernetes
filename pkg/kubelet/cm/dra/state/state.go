/*
Copyright 2024 The Kubernetes Authors.

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

package state

import (
	"slices"
	"time"

	resourceapi "k8s.io/api/resource/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
)

// ClaimInfoStateList 是 ClaimInfoState 的列表。
type ClaimInfoStateList []ClaimInfoState

// ClaimInfoState 保存某个 ResourceClaim 在 kubelet 本地的状态。
// +k8s:deepcopy-gen=true
type ClaimInfoState struct {
	// ClaimUID 是 ResourceClaim 的 UID。
	ClaimUID types.UID

	// ClaimName 是 ResourceClaim 的名称。
	ClaimName string

	// Namespace 是 ResourceClaim 所在的命名空间。
	Namespace string

	// PodUIDs 是引用该资源的 Pod UID 集合。
	PodUIDs sets.Set[string]

	// DriverState 保存所有在该 Claim 中有分配结果的驱动信息，
	// 即使驱动没有为其分配结果提供设备。
	DriverState map[string]DriverState
}

// DriverState 用于在 checkpoint 中保存按设备组织的 Claim 状态。
// +k8s:deepcopy-gen=true
type DriverState struct {
	// Devices 是已分配的设备列表。
	Devices []Device
	// SkipNodeOperations 允许 kubelet 在已分配设备不需要时跳过
	// 节点本地 gRPC 调用（NodePrepareResources 和/或 NodeUnprepareResources）。
	SkipNodeOperations []resourceapi.SkipNodeOperation `json:"skipNodeOperations,omitempty"`
}

// Skips 判断给定操作或通配符 '*' 是否出现在 SkipNodeOperations 中。
func (s DriverState) Skips(op resourceapi.SkipNodeOperation) bool {
	return slices.Contains(s.SkipNodeOperations, resourceapi.SkipNodeOperationAll) ||
		slices.Contains(s.SkipNodeOperations, op)
}

// Device 描述 DRA 驱动向 kubelet 上报的 Claim 中已分配设备。
// RequestName、ShareID 和 CDI 设备 ID 均为可选。
// +k8s:deepcopy-gen=true
type Device struct {
	// PoolName 是设备所属的设备池名称。
	PoolName string
	// DeviceName 是设备名称。
	DeviceName string
	// ShareID 是共享设备的 ID，非共享设备为 nil。
	ShareID *types.UID
	// RequestNames 是请求该设备的 Request 名称列表。
	RequestNames []string
	// CDIDeviceIDs 是设备对应的 CDI 设备 ID 列表。
	CDIDeviceIDs []string
}

// DevicesHealthMap 是驱动名称到其设备健康状态的映射。
type DevicesHealthMap map[string]DriverHealthState

// DriverHealthState 保存某个驱动所有设备的健康信息。
type DriverHealthState struct {
	// Devices 把设备的唯一键（"<pool>/<device>"）映射到其健康状态。
	Devices map[string]DeviceHealth
}

// DeviceHealthStatus 表示设备健康状态。
type DeviceHealthStatus string

const (
	// DeviceHealthStatusHealthy 表示设备健康。
	DeviceHealthStatusHealthy DeviceHealthStatus = "Healthy"
	// DeviceHealthStatusUnhealthy 表示设备不健康。
	DeviceHealthStatusUnhealthy DeviceHealthStatus = "Unhealthy"
	// DeviceHealthStatusUnknown 表示设备健康状态未知。
	DeviceHealthStatusUnknown DeviceHealthStatus = "Unknown"
)

// DeviceHealth 保存单个设备的健康信息。
type DeviceHealth struct {
	// PoolName 是设备所属设备池的名称。
	PoolName string

	// DeviceName 是设备名称。
	// 系统内的完整标识为 '<driver name>/<pool name>/<device name>'。
	DeviceName string

	// Health 是设备的健康状态，取值为 "Healthy"、"Unhealthy" 或 "Unknown"。
	Health DeviceHealthStatus

	// LastUpdated 记录该设备健康状态最近一次更新的时间。
	LastUpdated time.Time

	// HealthCheckTimeout 是设备健康检查的超时时间。
	// 零值表示使用默认超时（DefaultHealthTimeout），
	// 以保证与已有数据的向后兼容。
	HealthCheckTimeout time.Duration

	// Message 提供设备健康状态的附加说明，可选，可为空。
	Message string
}
