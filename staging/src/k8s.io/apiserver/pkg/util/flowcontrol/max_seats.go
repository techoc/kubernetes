/*
Copyright 2023 The Kubernetes Authors.

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

package flowcontrol

import (
	"sync"
)

// MaxSeatsTracker 用于记录工作估算器给出的每个优先级可分配的最大 seats。
// 工作估算器（work estimator）会为请求估算所需并发份额（seats）；
// 这里记录每个优先级历史估算的最大值，用于避免估算抖动导致的过度限流。
type MaxSeatsTracker interface {
	// GetMaxSeats 返回给定优先级下请求应占用的最大 seats。
	GetMaxSeats(priorityLevelName string) uint64

	// SetMaxSeats 配置某优先级的最大 seats。
	SetMaxSeats(priorityLevelName string, maxSeats uint64)

	// ForgetPriorityLevel 移除某优先级的最大 seats 记录。
	ForgetPriorityLevel(priorityLevelName string)
}

// maxSeatsTracker 用读写锁保护 map：读取远多于写入，RWMutex 可降低热路径开销。
type maxSeatsTracker struct {
	sync.RWMutex

	maxSeats map[string]uint64
}

// NewMaxSeatsTracker 创建空的 tracker；未记录的优先级返回零值 0。
func NewMaxSeatsTracker() MaxSeatsTracker {
	return &maxSeatsTracker{
		maxSeats: make(map[string]uint64),
	}
}

// GetMaxSeats 返回指定优先级记录的最大 seats；不存在时返回 0。
func (m *maxSeatsTracker) GetMaxSeats(plName string) uint64 {
	m.RLock()
	defer m.RUnlock()

	return m.maxSeats[plName]
}

// SetMaxSeats 覆盖指定优先级的最大 seats。
func (m *maxSeatsTracker) SetMaxSeats(plName string, maxSeats uint64) {
	m.Lock()
	defer m.Unlock()

	m.maxSeats[plName] = maxSeats
}

// ForgetPriorityLevel 在优先级配置被删除时清理记录，防止 map 随配置变更无限增长。
func (m *maxSeatsTracker) ForgetPriorityLevel(plName string) {
	m.Lock()
	defer m.Unlock()

	delete(m.maxSeats, plName)
}
