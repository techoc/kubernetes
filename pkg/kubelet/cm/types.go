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

package cm

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	"k8s.io/utils/cpuset"
)

// ResourceConfig 保存所有受支持的 cgroup 资源配置参数。
type ResourceConfig struct {
	// 内存上限（字节）。
	Memory *int64
	// CPU 集合（该 cgroup 可使用的 CPU）。
	CPUSet cpuset.CPUSet
	// CPU 权重（相对于其他容器的相对值）。
	CPUShares *uint64
	// CPU 硬上限（微秒），即一个周期内允许使用的 CPU 时间。
	CPUQuota *int64
	// CPU 配额周期。
	CPUPeriod *uint64
	// HugePageLimit 记录页大小（字节）到上限（字节）的映射。
	HugePageLimit map[int64]int64
	// PID 数量上限。
	PidsLimit *int64
	// Unified 保存 cgroup v2 统一层级的参数。
	Unified map[string]string
}

// CgroupName 是 cgroup 在驱动特定转换之前的抽象名称。
// 它由各层级组件组成的字符串列表表示，例如：
// {"kubepods", "burstable", "pod1234-abcd-5678-efgh"}
type CgroupName []string

// CgroupConfig 保存 cgroup 配置信息。
// 该对象同时供 systemd 和原始 cgroupfs 两种
// CgroupManager 实现使用。
type CgroupConfig struct {
	// 驱动特定转换之前的完整名称。
	Name CgroupName
	// ResourceParameters 包含要应用的各类 cgroup 设置。
	ResourceParameters *ResourceConfig
}

// CgroupManager 提供 cgroup 管理能力，
// 支持 cgroup 的创建、删除和更新。
type CgroupManager interface {
	// Create 创建 cgroup 并应用配置。
	// 它只创建叶子 cgroup，并要求父 cgroup 已存在。
	Create(klog.Logger, *CgroupConfig) error
	// Destroy 销毁 cgroup。
	Destroy(klog.Logger, *CgroupConfig) error
	// Update 更新 cgroup 配置。
	Update(klog.Logger, *CgroupConfig) error
	// Validate 检查 cgroup 是否有效。
	Validate(name CgroupName) error
	// Exists 检查 cgroup 是否已存在。
	Exists(name CgroupName) bool
	// Name 返回驱动特定转换之后宿主机上的字面 cgroupfs 名称。
	// systemd 实现应完成相应转换：例如传入 {"foo", "bar"} 时，
	// 应转换为类似 foo.slice/foo-bar.slice 的名称。
	Name(name CgroupName) string
	// CgroupName 把宿主机上的字面 cgroupfs 名称转换为内部标识。
	CgroupName(name string) CgroupName
	// Pids 扫描所有子系统，找出属于指定 cgroup 的 PID。
	Pids(logger klog.Logger, name CgroupName) []int
	// ReduceCPULimits 把 CPU CFS 配额降到最小份额。
	ReduceCPULimits(logger klog.Logger, cgroupName CgroupName) error
	// MemoryUsage 从 cgroupfs 读取指定 cgroup 的当前内存用量。
	MemoryUsage(name CgroupName) (int64, error)
	// GetCgroupConfig 读取指定资源类型在 cgroup 上生效的配置值。
	GetCgroupConfig(name CgroupName, resource v1.ResourceName) (*ResourceConfig, error)
	// SetCgroupConfig 为 cgroup 设置指定资源类型的配置。
	SetCgroupConfig(logger klog.Logger, name CgroupName, resourceConfig *ResourceConfig) error
	// Version 返回宿主机上 cgroup 实现的版本号。
	Version() int
}

// QOSContainersInfo 保存各 QoS 类别的父 cgroup 名称。
type QOSContainersInfo struct {
	// Guaranteed 是 Guaranteed QoS 类 Pod 的父 cgroup。
	Guaranteed CgroupName
	// BestEffort 是 BestEffort QoS 类 Pod 的父 cgroup。
	BestEffort CgroupName
	// Burstable 是 Burstable QoS 类 Pod 的父 cgroup。
	Burstable CgroupName
}

// PodContainerManager 存储并管理 Pod 级别的容器（cgroup）。
// Pod worker 通过它创建和销毁 Pod 对应的容器。
type PodContainerManager interface {
	// GetPodContainerName 返回 Pod 的 CgroupName 标识及其在宿主机上的字面 cgroupfs 名称。
	GetPodContainerName(*v1.Pod) (CgroupName, string)

	// EnsureExists 确保 Pod 的 cgroup 存在（在启用 QoS cgroup 层级时）。
	// 若 Pod cgroup 尚不存在则创建它。
	EnsureExists(logger klog.Logger, pod *v1.Pod) error

	// Exists 判断 Pod 的 cgroup 是否存在。
	Exists(*v1.Pod) bool

	// Destroy 销毁指定 cgroup 名称对应的 Pod 容器（cgroup）。
	Destroy(logger klog.Logger, name CgroupName) error

	// ReduceCPULimits 把 CPU CFS 配额降到最小份额。
	ReduceCPULimits(logger klog.Logger, name CgroupName) error

	// GetAllPodsFromCgroups 依据 cgroupfs 的当前状态枚举 Pod UID 到其 cgroup 的映射。
	GetAllPodsFromCgroups(logger klog.Logger) (map[types.UID]CgroupName, error)

	// IsPodCgroup 判断字面 cgroupfs 名称是否对应某个 Pod，并返回其 UID。
	IsPodCgroup(cgroupfs string) (bool, types.UID)

	// GetPodCgroupMemoryUsage 返回 Pod cgroup 的内存用量。
	GetPodCgroupMemoryUsage(pod *v1.Pod) (uint64, error)

	// GetPodCgroupConfig 读取指定资源类型在 Pod cgroup 上生效的配置值。
	GetPodCgroupConfig(pod *v1.Pod, resource v1.ResourceName) (*ResourceConfig, error)

	// SetPodCgroupConfig 为 Pod cgroup 设置指定资源类型的配置。
	SetPodCgroupConfig(logger klog.Logger, pod *v1.Pod, resourceConfig *ResourceConfig) error
}
