/*
Copyright 2019 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PersistentVolumeBinderControllerConfiguration 包含描述 PersistentVolumeBinderController 的配置项。
type PersistentVolumeBinderControllerConfiguration struct {
	// PVClaimBinderSyncPeriod 是同步持久卷（PV）与持久卷声明（PVC）的周期。
	PVClaimBinderSyncPeriod metav1.Duration
	// VolumeConfiguration 保存与卷相关特性的配置。
	VolumeConfiguration VolumeConfiguration
}

// VolumeConfiguration 包含用于配置所有卷插件的全部枚举标志。
// controller-manager 二进制会据此创建多个 volume.VolumeConfig 实例，每个实例仅包含某个插件所需的配置，
// 再传递给对应的插件。只有 ControllerManager 二进制知道支持哪些插件，以及哪些标志属于哪个插件。
type VolumeConfiguration struct {
	// EnableHostPathProvisioning 在无云提供商环境下启用 HostPath 类型 PV 的自动供给。
	// 这样可以测试并开发供给相关特性。HostPath 供给不受官方支持，无法在多节点集群中工作，
	// 除测试和开发外不应挪作他用。
	EnableHostPathProvisioning bool
	// EnableDynamicProvisioning 在支持动态供给的环境中启用卷的动态供给，默认为 true。
	EnableDynamicProvisioning bool
	// PersistentVolumeRecyclerConfiguration 保存持久卷回收器插件的配置。
	PersistentVolumeRecyclerConfiguration PersistentVolumeRecyclerConfiguration
	// FlexVolumePluginDir 是 flex 卷插件查找第三方卷插件的完整目录路径。
	FlexVolumePluginDir string
}

// PersistentVolumeRecyclerConfiguration 包含描述持久卷回收器插件的配置项。
type PersistentVolumeRecyclerConfiguration struct {
	// MaximumRetry 是 PV 回收失败时回收器执行的最大重试次数。
	MaximumRetry int32
	// MinimumTimeoutNFS 是 NFS 回收器 Pod 使用的最小 ActiveDeadlineSeconds。
	MinimumTimeoutNFS int32
	// PodTemplateFilePathNFS 是用作 NFS 持久卷回收模板的 Pod 定义文件路径。
	PodTemplateFilePathNFS string
	// IncrementTimeoutNFS 是 NFS 清理器 Pod 每增加 1 Gi 容量时，ActiveDeadlineSeconds 的增量。
	IncrementTimeoutNFS int32
	// PodTemplateFilePathHostPath 是用作 HostPath 持久卷回收模板的 Pod 定义文件路径。
	// 该项仅供开发与测试，无法在多节点集群中工作。
	PodTemplateFilePathHostPath string
	// MinimumTimeoutHostPath 是 HostPath 回收器 Pod 使用的最小 ActiveDeadlineSeconds。
	// 该项仅供开发与测试，无法在多节点集群中工作。
	MinimumTimeoutHostPath int32
	// IncrementTimeoutHostPath 是 HostPath 清理器 Pod 每增加 1 Gi 容量时，ActiveDeadlineSeconds 的增量。
	// 该项仅供开发与测试，无法在多节点集群中工作。
	IncrementTimeoutHostPath int32
}
