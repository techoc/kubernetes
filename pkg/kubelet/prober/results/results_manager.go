/*
Copyright 2015 The Kubernetes Authors.

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

package results

import (
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
)

// Manager 提供探测结果缓存与更新通道。
type Manager interface {
	// Get 返回指定容器缓存的探测结果。
	Get(kubecontainer.ContainerID) (Result, bool)
	// Set 更新指定容器的缓存结果。
	// pod 仅用于随更新一起发送。
	Set(kubecontainer.ContainerID, Result, *v1.Pod)
	// Remove 清除指定容器的缓存结果。
	Remove(kubecontainer.ContainerID)
	// Updates 返回结果变化（不含删除）时接收更新的通道。
	// 注意：当前实现只支持单个更新通道。
	Updates() <-chan Update
}

// Result 是探测结果的类型。
type Result int

const (
	// Unknown 编码为 -1。
	Unknown Result = iota - 1

	// Success 编码为 0。
	Success

	// Failure 编码为 1。
	Failure
)

// String 返回探测结果的可读名称。
func (r Result) String() string {
	switch r {
	case Success:
		return "Success"
	case Failure:
		return "Failure"
	default:
		return "UNKNOWN"
	}
}

// ToPrometheusType 把探测结果转换为便于 Prometheus 理解的数值。
func (r Result) ToPrometheusType() float64 {
	switch r {
	case Success:
		return 0
	case Failure:
		return 1
	default:
		return -1
	}
}

// Update 是通过 Updates 通道发送的一次结果更新。
type Update struct {
	// ContainerID 是产生该更新的容器。
	ContainerID kubecontainer.ContainerID
	// Result 是新的探测结果。
	Result Result
	// PodUID 是容器所属 Pod 的 UID。
	PodUID types.UID
}

// manager 是 Manager 接口的默认实现。
type manager struct {
	// 保护 cache 的读写锁。
	sync.RWMutex
	// 容器 ID 到探测结果的映射。
	cache map[kubecontainer.ContainerID]Result
	// 结果更新通道。
	updates chan Update
}

// 编译期断言：manager 实现了 Manager 接口。
var _ Manager = &manager{}

// NewManager 创建并返回一个空的结果管理器。
func NewManager() Manager {
	// 更新通道带缓冲，避免探测循环因消费者短暂停顿而阻塞。
	return &manager{
		cache:   make(map[kubecontainer.ContainerID]Result),
		updates: make(chan Update, 20),
	}
}

// Get 返回容器缓存的探测结果，并报告其是否存在。
func (m *manager) Get(id kubecontainer.ContainerID) (Result, bool) {
	m.RLock()
	defer m.RUnlock()
	result, found := m.cache[id]
	return result, found
}

// Set 更新容器探测结果，仅在结果变化时发送更新。
func (m *manager) Set(id kubecontainer.ContainerID, result Result, pod *v1.Pod) {
	if m.setInternal(id, result) {
		// 结果变化才通知消费者，避免无意义的重复事件。
		m.updates <- Update{id, result, pod.UID}
	}
}

// setInternal 在持锁状态下更新缓存，并返回是否需要发送更新。
func (m *manager) setInternal(id kubecontainer.ContainerID, result Result) bool {
	m.Lock()
	defer m.Unlock()
	prev, exists := m.cache[id]
	if !exists || prev != result {
		m.cache[id] = result
		return true
	}
	return false
}

// Remove 删除容器的缓存结果。
func (m *manager) Remove(id kubecontainer.ContainerID) {
	m.Lock()
	defer m.Unlock()
	delete(m.cache, id)
}

// Updates 返回结果更新通道。
func (m *manager) Updates() <-chan Update {
	return m.updates
}
