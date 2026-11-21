/*
Copyright 2021 The Kubernetes Authors.

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
	"reflect"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// FakePodContainerManager 是 PodContainerManager 的测试实现，记录调用并维护简单的 cgroup 映射。
type FakePodContainerManager struct {
	sync.Mutex
	// CalledFunctions 记录被调用的方法名，便于测试断言。
	CalledFunctions []string
	// Cgroups 记录 Pod UID 到 cgroup 名称的映射。
	Cgroups map[types.UID]CgroupName
}

// 编译期断言：FakePodContainerManager 实现了 PodContainerManager 接口。
var _ PodContainerManager = &FakePodContainerManager{}

// NewFakePodContainerManager 创建并初始化测试用的容器管理器。
func NewFakePodContainerManager() *FakePodContainerManager {
	return &FakePodContainerManager{
		Cgroups: make(map[types.UID]CgroupName),
	}
}

// AddPodFromCgroups 把 Pod 以 Pod 名作为 cgroup 名称登记到映射中。
func (m *FakePodContainerManager) AddPodFromCgroups(pod *kubecontainer.Pod) {
	m.Lock()
	defer m.Unlock()
	m.Cgroups[pod.ID] = []string{pod.Name}
}

// Exists 记录调用并始终返回 true。
func (m *FakePodContainerManager) Exists(_ *v1.Pod) bool {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "Exists")
	return true
}

// EnsureExists 记录调用并返回成功。
func (m *FakePodContainerManager) EnsureExists(_ klog.Logger, _ *v1.Pod) error {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "EnsureExists")
	return nil
}

// GetPodContainerName 记录调用并返回空名称。
func (m *FakePodContainerManager) GetPodContainerName(_ *v1.Pod) (CgroupName, string) {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "GetPodContainerName")
	return nil, ""
}

// Destroy 记录调用并删除名称匹配的 cgroup 记录。
func (m *FakePodContainerManager) Destroy(_ klog.Logger, name CgroupName) error {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "Destroy")
	// 按名称匹配并删除对应 cgroup 记录。
	for key, cgname := range m.Cgroups {
		if reflect.DeepEqual(cgname, name) {
			delete(m.Cgroups, key)
			return nil
		}
	}
	return nil
}

// ReduceCPULimits 记录调用并返回成功。
func (m *FakePodContainerManager) ReduceCPULimits(_ klog.Logger, _ CgroupName) error {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "ReduceCPULimits")
	return nil
}

// GetAllPodsFromCgroups 返回 cgroup 映射的副本。
func (m *FakePodContainerManager) GetAllPodsFromCgroups(_ klog.Logger) (map[types.UID]CgroupName, error) {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "GetAllPodsFromCgroups")
	// 返回副本，避免与外部共享 map 触发竞态检测。
	grp := make(map[types.UID]CgroupName)
	for key, value := range m.Cgroups {
		grp[key] = value
	}
	return grp, nil
}

// IsPodCgroup 记录调用并始终返回 false。
func (m *FakePodContainerManager) IsPodCgroup(cgroupfs string) (bool, types.UID) {
	m.Lock()
	defer m.Unlock()
	m.CalledFunctions = append(m.CalledFunctions, "IsPodCgroup")
	return false, types.UID("")
}

// GetPodCgroupMemoryUsage 记录调用并返回 0。
func (cm *FakePodContainerManager) GetPodCgroupMemoryUsage(_ *v1.Pod) (uint64, error) {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPodCgroupMemoryUsage")
	return 0, nil
}

// GetPodCgroupConfig 记录调用并返回 nil 配置。
func (cm *FakePodContainerManager) GetPodCgroupConfig(_ *v1.Pod, _ v1.ResourceName) (*ResourceConfig, error) {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "GetPodCgroupConfig")
	return nil, nil
}

// SetPodCgroupConfig 记录调用并返回成功。
func (cm *FakePodContainerManager) SetPodCgroupConfig(_ klog.Logger, pod *v1.Pod, resourceConfig *ResourceConfig) error {
	cm.Lock()
	defer cm.Unlock()
	cm.CalledFunctions = append(cm.CalledFunctions, "SetPodCgroupConfig")
	return nil
}
