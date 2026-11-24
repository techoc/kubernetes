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

package portallocator

import (
	"errors"
	"fmt"

	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/klog/v2"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
)

// Interface 定义端口范围分配契约；所有实现都必须支持并发安全调用。
type Interface interface {
	Allocate(int) error
	AllocateNext() (int, error)
	Release(int) error
	ForEach(func(int))
	Has(int) bool
	Destroy()
	EnableMetrics()
}

var (
	ErrFull              = errors.New("range is full")
	ErrAllocated         = errors.New("provided port is already allocated")
	ErrMismatchedNetwork = errors.New("the provided port range does not match the current port range")
)

type ErrNotInRange struct {
	ValidPorts string
}

func (e *ErrNotInRange) Error() string {
	return fmt.Sprintf("provided port is not in the valid range. The range of valid ports is %s", e.ValidPorts)
}

type PortAllocator struct {
	portRange net.PortRange

	alloc allocator.Interface

	// metrics 负责记录端口分配指标，也可以禁用。
	metrics metricsRecorderInterface
}

// PortAllocator 将实际 TCP/UDP 端口映射到从 0 开始的偏移位图，并维护静态指定与自动分配的统计指标。
var _ Interface = &PortAllocator{}

// New 根据 net.PortRange 创建端口分配器，并调用 allocatorFactory 构建底层分配存储。
// New 根据端口范围创建底层整数分配器，并计算动态分配优先区的 offset。
func New(pr net.PortRange, allocatorFactory allocator.AllocatorWithOffsetFactory) (*PortAllocator, error) {
	max := pr.Size
	rangeSpec := pr.String()

	a := &PortAllocator{
		portRange: pr,
		metrics:   &emptyMetricsRecorder{},
	}

	var err error
	a.alloc, err = allocatorFactory(max, rangeSpec, calculateRangeOffset(pr))
	if err != nil {
		return nil, err
	}

	return a, err
}

// NewInMemory 创建仅在内存中维护状态的端口分配器。
func NewInMemory(pr net.PortRange) (*PortAllocator, error) {
	return New(pr, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
		return allocator.NewAllocationMapWithOffset(max, rangeSpec, offset), nil
	})
}

// NewFromSnapshot 创建端口分配器，并使用给定快照初始化分配状态。
func NewFromSnapshot(snap *api.RangeAllocation) (*PortAllocator, error) {
	pr, err := net.ParsePortRange(snap.Range)
	if err != nil {
		return nil, err
	}
	r, err := NewInMemory(*pr)
	if err != nil {
		return nil, err
	}
	if err := r.Restore(*pr, snap.Data); err != nil {
		return nil, err
	}
	return r, nil
}

// Free 返回端口范围内剩余可分配的端口数量。
func (r *PortAllocator) Free() int {
	return r.alloc.Free()
}

// Used 返回端口范围内已分配的端口数量。
func (r *PortAllocator) Used() int {
	return r.portRange.Size - r.alloc.Free()
}

// Allocate 尝试预留指定端口。端口不在当前范围内时返回 ErrNotInRange，已被占用时返回 ErrAllocated，
// 端口池耗尽时返回 ErrFull。
// Allocate 先确认真实端口落在配置范围内，再将端口换算成 offset 并执行静态预留。
func (r *PortAllocator) Allocate(port int) error {
	ok, offset := r.contains(port)
	if !ok {
		// 更新端口分配指标。
		r.metrics.incrementAllocationErrors("static")

		// 在错误信息中附带有效端口范围。
		validPorts := r.portRange.String()
		return &ErrNotInRange{validPorts}
	}

	allocated, err := r.alloc.Allocate(offset)
	if err != nil {
		// 更新端口分配指标。
		r.metrics.incrementAllocationErrors("static")
		return err
	}
	if !allocated {
		// 更新端口分配指标。
		r.metrics.incrementAllocationErrors("static")
		return ErrAllocated
	}

	// 更新端口分配指标。
	r.metrics.incrementAllocations("static")
	r.metrics.setAllocated(r.Used())
	r.metrics.setAvailable(r.Free())

	return nil
}

// AllocateNext 从端口池中自动分配一个端口；端口池耗尽时返回 ErrFull。
// AllocateNext 从底层位图申请一个偏移量，再加上 PortRange.Base 转回真实 NodePort。
func (r *PortAllocator) AllocateNext() (int, error) {
	offset, ok, err := r.alloc.AllocateNext()
	if err != nil {
		r.metrics.incrementAllocationErrors("dynamic")
		return 0, err
	}
	if !ok {
		r.metrics.incrementAllocationErrors("dynamic")
		return 0, ErrFull
	}

	// 更新端口分配指标。
	r.metrics.incrementAllocations("dynamic")
	r.metrics.setAllocated(r.Used())
	r.metrics.setAvailable(r.Free())

	return r.portRange.Base + offset, nil
}

// ForEach 对每个已分配端口调用指定回调函数。
func (r *PortAllocator) ForEach(fn func(int)) {
	r.alloc.ForEach(func(offset int) {
		fn(r.portRange.Base + offset)
	})
}

// Release 将端口归还端口池。释放未分配或不属于当前范围的端口时不执行任何操作，也不会返回错误。
func (r *PortAllocator) Release(port int) error {
	ok, offset := r.contains(port)
	if !ok {
		klog.Warningf("port is not in the range when release it. port: %v", port)
		return nil
	}

	err := r.alloc.Release(offset)
	if err == nil {
		// 更新端口分配指标。
		r.metrics.setAllocated(r.Used())
		r.metrics.setAvailable(r.Free())
	}
	return err
}

// Has 判断指定端口是否已分配；若已分配，再次调用 Allocate(port) 将返回 ErrAllocated。
func (r *PortAllocator) Has(port int) bool {
	ok, offset := r.contains(port)
	if !ok {
		return false
	}

	return r.alloc.Has(offset)
}

// Snapshot 导出端口池当前状态。
// Snapshot 将当前端口范围和位图数据写入 RangeAllocation，供 etcd 分配注册表持久化。
func (r *PortAllocator) Snapshot(dst *api.RangeAllocation) error {
	snapshottable, ok := r.alloc.(allocator.Snapshottable)
	if !ok {
		return fmt.Errorf("not a snapshottable allocator")
	}
	rangeString, data := snapshottable.Snapshot()
	dst.Range = rangeString
	dst.Data = data
	return nil
}

// Restore 将端口池恢复到之前捕获的状态。若传入端口范围与快照对应范围不完全一致，则返回 ErrMismatchedNetwork。
// Restore 要求端口范围与当前配置完全一致，防止将旧范围的 offset 位图误用于新 NodePort 范围。
func (r *PortAllocator) Restore(pr net.PortRange, data []byte) error {
	if pr.String() != r.portRange.String() {
		return ErrMismatchedNetwork
	}
	snapshottable, ok := r.alloc.(allocator.Snapshottable)
	if !ok {
		return fmt.Errorf("not a snapshottable allocator")
	}
	return snapshottable.Restore(pr.String(), data)
}

// contains 判断端口是否位于当前范围内；若在范围内，返回 true 和对应偏移量，否则返回 false 和 nil。
func (r *PortAllocator) contains(port int) (bool, int) {
	if !r.portRange.Contains(port) {
		return false, 0
	}

	offset := port - r.portRange.Base
	return true, offset
}

// Destroy 关闭底层分配器。
func (r *PortAllocator) Destroy() {
	r.alloc.Destroy()
}

// EnableMetrics 启用指标记录。
func (r *PortAllocator) EnableMetrics() {
	registerMetrics()
	r.metrics = &metricsRecorder{}
}

// calculateRangeOffset 根据端口范围大小估算静态分配预留区长度，计算公式为 min(max(min, rangeSize/step), max)。
// 结果不会小于 min 或大于 max，并在两者之间随范围大小阶梯式变化；任一参数无效时返回 0。
func calculateRangeOffset(pr net.PortRange) int {
	// min(max(min, rangeSize/step), max) 公式使用的默认参数。
	const (
		min  = 16
		max  = 128
		step = 32
	)

	rangeSize := pr.Size
	// offset 必须始终小于端口范围的总大小。
	if rangeSize <= min {
		return 0
	}

	offset := rangeSize / step
	if offset < min {
		return min
	}
	if offset > max {
		return max
	}
	return int(offset)
}
