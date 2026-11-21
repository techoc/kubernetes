//go:build !linux

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
	"errors"

	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// unsupportedCgroupManager 是非 Linux 平台上的空实现。
type unsupportedCgroupManager struct{}

// errNotSupported 表示当前构建不支持 cgroup 管理。
var errNotSupported = errors.New("Cgroup Manager is not supported in this build")

// 编译期断言：unsupportedCgroupManager 实现了 CgroupManager 接口。
var _ CgroupManager = &unsupportedCgroupManager{}

// CgroupSubsystems 描述宿主机的 cgroup 挂载信息。
type CgroupSubsystems struct {
	// Mounts 保存挂载信息。
	Mounts []interface{}
	// MountPoints 是控制器名称到挂载点的映射。
	MountPoints map[string]string
}

// NewCgroupManager 返回不支持 cgroup 的空实现。
func NewCgroupManager(_ interface{}) CgroupManager {
	// 非 Linux 平台没有 cgroup 支持，直接返回空实现。
	return &unsupportedCgroupManager{}
}

// Version 返回 0，表示没有可用的 cgroup 实现。
func (m *unsupportedCgroupManager) Version() int {
	return 0
}

// Name 返回空字符串。
func (m *unsupportedCgroupManager) Name(_ CgroupName) string {
	return ""
}

// Validate 始终返回不支持错误。
func (m *unsupportedCgroupManager) Validate(_ CgroupName) error {
	return errNotSupported
}

// Exists 始终返回 false。
func (m *unsupportedCgroupManager) Exists(_ CgroupName) bool {
	return false
}

// Destroy 是空操作。
func (m *unsupportedCgroupManager) Destroy(_ klog.Logger, _ *CgroupConfig) error {
	return nil
}

// Update 是空操作。
func (m *unsupportedCgroupManager) Update(_ klog.Logger, _ *CgroupConfig) error {
	return nil
}

// Create 始终返回不支持错误。
func (m *unsupportedCgroupManager) Create(_ klog.Logger, _ *CgroupConfig) error {
	return errNotSupported
}

// MemoryUsage 返回 -1 和不支持错误。
func (m *unsupportedCgroupManager) MemoryUsage(_ CgroupName) (int64, error) {
	return -1, errNotSupported
}

// Pids 返回 nil。
func (m *unsupportedCgroupManager) Pids(_ klog.Logger, _ CgroupName) []int {
	return nil
}

// CgroupName 返回空的 cgroup 名称。
func (m *unsupportedCgroupManager) CgroupName(name string) CgroupName {
	return CgroupName([]string{})
}

// ReduceCPULimits 是空操作。
func (m *unsupportedCgroupManager) ReduceCPULimits(_ klog.Logger, cgroupName CgroupName) error {
	return nil
}

// GetCgroupConfig 始终返回不支持错误。
func (m *unsupportedCgroupManager) GetCgroupConfig(name CgroupName, resource v1.ResourceName) (*ResourceConfig, error) {
	return nil, errNotSupported
}

// SetCgroupConfig 始终返回不支持错误。
func (m *unsupportedCgroupManager) SetCgroupConfig(logger klog.Logger, name CgroupName, resourceConfig *ResourceConfig) error {
	return errNotSupported
}

// RootCgroupName 是 cgroup 层级的根名称。
var RootCgroupName = CgroupName([]string{})

// NewCgroupName 在基础名称后追加组件，返回新的 cgroup 名称。
func NewCgroupName(base CgroupName, components ...string) CgroupName {
	return append(append([]string{}, base...), components...)
}

// ToSystemd 返回空字符串，非 Linux 平台不支持 systemd 名称转换。
func (cgroupName CgroupName) ToSystemd() string {
	return ""
}

// ParseSystemdToCgroupName 返回 nil，非 Linux 平台不支持解析。
func ParseSystemdToCgroupName(name string) CgroupName {
	return nil
}

// ToCgroupfs 返回空字符串。
func (cgroupName CgroupName) ToCgroupfs() string {
	return ""
}

// ParseCgroupfsToCgroupName 返回 nil。
func ParseCgroupfsToCgroupName(name string) CgroupName {
	return nil
}

// IsSystemdStyleName 始终返回 false。
func IsSystemdStyleName(name string) bool {
	return false
}
