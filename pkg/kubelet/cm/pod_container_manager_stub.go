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
)

// podContainerManagerStub 是 PodContainerManager 的空实现。
type podContainerManagerStub struct {
}

var _ PodContainerManager = &podContainerManagerStub{}

// Exists 始终返回 true。
func (m *podContainerManagerStub) Exists(_ *v1.Pod) bool {
	return true
}

// EnsureExists 为空实现。
func (m *podContainerManagerStub) EnsureExists(_ klog.Logger, _ *v1.Pod) error {
	return nil
}

// GetPodContainerName 返回空的 cgroup 名称。
func (m *podContainerManagerStub) GetPodContainerName(_ *v1.Pod) (CgroupName, string) {
	return nil, ""
}

// Destroy 为空实现。
func (m *podContainerManagerStub) Destroy(_ klog.Logger, _ CgroupName) error {
	return nil
}

// ReduceCPULimits 为空实现。
func (m *podContainerManagerStub) ReduceCPULimits(_ klog.Logger, _ CgroupName) error {
	return nil
}

// GetAllPodsFromCgroups 返回空集合。
func (m *podContainerManagerStub) GetAllPodsFromCgroups(_ klog.Logger) (map[types.UID]CgroupName, error) {
	return nil, nil
}

// IsPodCgroup 始终返回 false 和空 UID。
func (m *podContainerManagerStub) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	return false, types.UID("")
}

// GetPodCgroupMemoryUsage 返回 0。
func (m *podContainerManagerStub) GetPodCgroupMemoryUsage(_ *v1.Pod) (uint64, error) {
	return 0, nil
}

// GetPodCgroupMemoryLimit 返回 0。
func (m *podContainerManagerStub) GetPodCgroupMemoryLimit(_ *v1.Pod) (uint64, error) {
	return 0, nil
}

// GetPodCgroupCpuLimit 返回全零。
func (m *podContainerManagerStub) GetPodCgroupCpuLimit(_ *v1.Pod) (int64, uint64, uint64, error) {
	return 0, 0, 0, nil
}

// SetPodCgroupMemoryLimit 为空实现。
func (m *podContainerManagerStub) SetPodCgroupMemoryLimit(_ *v1.Pod, _ int64) error {
	return nil
}

// SetPodCgroupCPULimit 为空实现。
func (m *podContainerManagerStub) SetPodCgroupCPULimit(_ klog.Logger, _ *v1.Pod, _ *int64, _, _ *uint64) error {
	return nil
}
