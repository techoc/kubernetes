/*
Copyright 2014 The Kubernetes Authors.

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

package capabilities

import (
	"sync"
)

// Capabilities 定义系统中可用的能力集合。
// 目前这些能力是全局的，未来可能改为按用户区分。
type Capabilities struct {
	// AllowPrivileged 是否允许特权容器。
	AllowPrivileged bool

	// PrivilegedSources 定义允许使用特权能力（如宿主网络、共享宿主 IPC/PID 命名空间）的 Pod 来源。
	PrivilegedSources PrivilegedSources

	// PerConnectionBandwidthLimitBytesPerSec 限制每个连接的吞吐量（目前仅用于 proxy、exec、attach）。
	PerConnectionBandwidthLimitBytesPerSec int64
}

// PrivilegedSources 定义被允许申请特定特权能力（宿主网络、共享宿主 IPC/PID 命名空间）的 Pod 来源。
type PrivilegedSources struct {
	// HostNetworkSources 允许使用宿主网络的 Pod 来源列表。
	HostNetworkSources []string

	// HostPIDSources 允许使用宿主 PID 命名空间的 Pod 来源列表。
	HostPIDSources []string

	// HostIPCSources 允许使用宿主 IPC 命名空间的 Pod 来源列表。
	HostIPCSources []string
}

// capInstance 保存全局唯一的能力配置。
// once 保证 Initialize 只在整个二进制生命周期内生效一次；
// lock 保护配置读写，避免与测试用的 ResetForTest 产生数据竞争。
var capInstance struct {
	once         sync.Once
	lock         sync.Mutex
	capabilities *Capabilities
}

// Initialize 初始化能力集合。每个二进制只能执行一次，后续调用会被忽略。
func Initialize(c Capabilities) {
	// 用 sync.Once 保证重复调用不会覆盖已生效的配置。
	capInstance.once.Do(func() {
		capInstance.capabilities = &c
	})
}

// Setup 设置能力集合，是对 Initialize 的便捷封装。
func Setup(allowPrivileged bool, perConnectionBytesPerSec int64) {
	Initialize(Capabilities{
		AllowPrivileged:                        allowPrivileged,
		PerConnectionBandwidthLimitBytesPerSec: perConnectionBytesPerSec,
	})
}

// ResetForTest 把能力配置重置到未初始化状态，仅供测试使用。
// 通过加锁确保与 Get 的并发安全。
func ResetForTest() {
	capInstance.lock.Lock()
	defer capInstance.lock.Unlock()
	capInstance.capabilities = nil
	// 同时重置 once，使测试可以重新调用 Initialize。
	capInstance.once = sync.Once{}
}

// Get 返回系统能力配置的只读副本。
func Get() Capabilities {
	capInstance.lock.Lock()
	defer capInstance.lock.Unlock()
	// 尚未初始化时返回一套安全的默认配置（不允许任何特权）。
	// 这里不能直接调用 Initialize：它会再次走 once 逻辑，
	// 若之前被测试重置过，就会把 SetForTests 设置的值覆盖掉。
	if capInstance.capabilities == nil {
		Initialize(Capabilities{
			AllowPrivileged: false,
			PrivilegedSources: PrivilegedSources{
				HostNetworkSources: []string{},
				HostPIDSources:     []string{},
				HostIPCSources:     []string{},
			},
		})
	}
	return *capInstance.capabilities
}
