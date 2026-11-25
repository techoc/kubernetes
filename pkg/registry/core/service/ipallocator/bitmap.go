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

package ipallocator

import (
	"fmt"
	"math/big"
	"net"

	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/registry/core/service/allocator"
	netutils "k8s.io/utils/net"
)

// Range 将一段连续 IP 地址范围映射为偏移位图，可原子地完成分配。
// 例如 10.0.0.0/24 共包含 256 个地址，排除网络地址和广播地址后，可分配地址数为 254（即 r.max）。
// 位图偏移量从可分配地址起点开始编号：偏移量 0 对应首个可用地址，最后一位对应末尾可用地址。
type Range struct {
	net *net.IPNet
	// base 缓存 CIDR 起始 IP 的 big.Int 表示。
	base *big.Int
	// max 表示该 CIDR 范围内可分配 IP 地址的数量。
	max int
	// family 表示该地址范围所属的 IP 协议族（IPv4 或 IPv6）。
	family api.IPFamily

	alloc allocator.Interface
	// metrics 负责记录分配器指标，也可以禁用。
	metrics metricsRecorderInterface
}

var _ Interface = (*Range)(nil)

// New 根据 CIDR 计算可分配主机数量及动态/保留区边界，并调用 allocatorFactory 创建底层分配存储。
func New(cidr *net.IPNet, allocatorFactory allocator.AllocatorWithOffsetFactory) (*Range, error) {
	max := netutils.RangeSize(cidr)
	base := netutils.BigForIP(cidr.IP)
	rangeSpec := cidr.String()
	var family api.IPFamily

	if netutils.IsIPv6CIDR(cidr) {
		family = api.IPv6Protocol
		// 分配器需要为每个地址维护一位，因此限制位图最大容量，避免占用过多内存。
		if max > 65536 {
			max = 65536
		}
	} else {
		family = api.IPv4Protocol
		// IPv4 网络的广播地址不可分配。不能简单地调用 Allocate() 将其标记为占用，
		// 因为这样可能允许后续通过 Release() 将其释放。
		max--
	}

	// 网络地址（通常以 .0 结尾）不可分配。不能简单地调用 Allocate() 将其标记为占用，
	// 否则后续可能通过 Release() 将其释放。
	base.Add(base, big.NewInt(1))
	max--

	// 掩码覆盖整个地址空间的 CIDR 可能导致计算结果为负数。
	if max < 0 {
		max = 0
	}

	r := Range{
		net:     cidr,
		base:    base,
		max:     maximum(0, int(max)),
		family:  family,
		metrics: &emptyMetricsRecorder{}, // 默认禁用指标记录。
	}

	offset := calculateRangeOffset(cidr)

	var err error
	r.alloc, err = allocatorFactory(r.max, rangeSpec, offset)
	if err != nil {
		return nil, err
	}
	return &r, nil
}

// NewInMemory 创建仅在内存中维护状态的 IP 分配器。
func NewInMemory(cidr *net.IPNet) (*Range, error) {
	return New(cidr, func(max int, rangeSpec string, offset int) (allocator.Interface, error) {
		return allocator.NewAllocationMapWithOffset(max, rangeSpec, offset), nil
	})
}

// NewFromSnapshot 创建 IP 地址范围，并使用给定快照初始化分配状态。
func NewFromSnapshot(snap *api.RangeAllocation) (*Range, error) {
	_, ipnet, err := netutils.ParseCIDRSloppy(snap.Range)
	if err != nil {
		return nil, err
	}
	r, err := NewInMemory(ipnet)
	if err != nil {
		return nil, err
	}
	if err := r.Restore(ipnet, snap.Data); err != nil {
		return nil, err
	}
	return r, nil
}

func maximum(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Free 返回该地址范围内剩余可分配的 IP 数量。
func (r *Range) Free() int {
	return r.alloc.Free()
}

// Used 返回该地址范围内已分配的 IP 数量。
func (r *Range) Used() int {
	return r.max - r.alloc.Free()
}

// CIDR 返回该分配器管理的 CIDR。
func (r *Range) CIDR() net.IPNet {
	return *r.net
}

// DryRun 返回该地址范围的试运行视图；操作不会持久化到实际分配状态。
func (r *Range) DryRun() Interface {
	return dryRunRange{r}
}

// 为使调用处的代码更清晰，提供该辅助方法。
const dryRunTrue = true
const dryRunFalse = false

// Allocate 尝试预留指定 IP。地址不属于当前范围时返回 ErrNotInRange，已被占用时返回 ErrAllocated，
// 地址池耗尽时返回 ErrFull。
func (r *Range) Allocate(ip net.IP) error {
	return r.allocate(ip, dryRunFalse)
}

func (r *Range) allocate(ip net.IP, dryRun bool) error {
	label := r.CIDR()
	ok, offset := r.contains(ip)
	if !ok {
		if !dryRun {
			// 更新分配器指标。
			r.metrics.incrementAllocationErrors(label.String(), "static")
		}
		return &ErrNotInRange{ip, r.net.String()}
	}
	if dryRun {
		// 不必额外检查 IP 是否空闲：该检查存在竞争窗口，进一步传递状态也无法保证后续分配成功。
		return nil
	}

	allocated, err := r.alloc.Allocate(offset)
	if err != nil {
		// 更新分配器指标。
		r.metrics.incrementAllocationErrors(label.String(), "static")

		return err
	}
	if !allocated {
		// 更新分配器指标。
		r.metrics.incrementAllocationErrors(label.String(), "static")

		return ErrAllocated
	}
	// 更新分配器指标。
	r.metrics.incrementAllocations(label.String(), "static")
	r.metrics.setAllocated(label.String(), r.Used())
	r.metrics.setAvailable(label.String(), r.Free())

	return nil
}

// AllocateNext 从地址池中自动分配一个 IP；地址池耗尽时返回 ErrFull。
func (r *Range) AllocateNext() (net.IP, error) {
	return r.allocateNext(dryRunFalse)
}

func (r *Range) allocateNext(dryRun bool) (net.IP, error) {
	label := r.CIDR()
	if dryRun {
		// 不额外预先查找空闲地址：该检查存在竞争窗口，无法保证后续分配一定成功。
		return r.CIDR().IP, nil
	}

	offset, ok, err := r.alloc.AllocateNext()
	if err != nil {
		// 更新分配器指标。
		r.metrics.incrementAllocationErrors(label.String(), "dynamic")

		return nil, err
	}
	if !ok {
		// 更新分配器指标。
		r.metrics.incrementAllocationErrors(label.String(), "dynamic")

		return nil, ErrFull
	}
	// 更新分配器指标。
	r.metrics.incrementAllocations(label.String(), "dynamic")
	r.metrics.setAllocated(label.String(), r.Used())
	r.metrics.setAvailable(label.String(), r.Free())

	return netutils.AddIPOffset(r.base, offset), nil
}

// Release 将 IP 归还地址池。释放未分配或不属于当前范围的 IP 时不执行任何操作，也不会返回错误。
func (r *Range) Release(ip net.IP) error {
	return r.release(ip, dryRunFalse)
}

func (r *Range) release(ip net.IP, dryRun bool) error {
	ok, offset := r.contains(ip)
	if !ok {
		return nil
	}
	if dryRun {
		return nil
	}

	err := r.alloc.Release(offset)
	if err == nil {
		// 更新分配器指标。
		label := r.CIDR()
		r.metrics.setAllocated(label.String(), r.Used())
		r.metrics.setAvailable(label.String(), r.Free())
	}
	return err
}

// ForEach 对每个已分配的 IP 调用指定回调函数。
func (r *Range) ForEach(fn func(net.IP)) {
	r.alloc.ForEach(func(offset int) {
		ip, _ := netutils.GetIndexedIP(r.net, offset+1) // 加 1 是因为 Range 不存储索引为 0 的 IP。
		fn(ip)
	})
}

// Has 判断指定 IP 是否已分配；若已分配，再次调用 Allocate(ip) 将返回 ErrAllocated。
func (r *Range) Has(ip net.IP) bool {
	ok, offset := r.contains(ip)
	if !ok {
		return false
	}

	return r.alloc.Has(offset)
}

// IPFamily 返回该地址范围所属的 IP 协议族。
func (r *Range) IPFamily() api.IPFamily {
	return r.family
}

// Snapshot 导出地址池当前状态。
func (r *Range) Snapshot(dst *api.RangeAllocation) error {
	snapshottable, ok := r.alloc.(allocator.Snapshottable)
	if !ok {
		return fmt.Errorf("not a snapshottable allocator")
	}
	rangeString, data := snapshottable.Snapshot()
	dst.Range = rangeString
	dst.Data = data
	return nil
}

// Restore 将地址池恢复到之前捕获的状态。若传入的 IPNet 与快照对应的网络范围不完全一致，则返回 ErrMismatchedNetwork。
func (r *Range) Restore(net *net.IPNet, data []byte) error {
	if !net.IP.Equal(r.net.IP) || net.Mask.String() != r.net.Mask.String() {
		return ErrMismatchedNetwork
	}
	snapshottable, ok := r.alloc.(allocator.Snapshottable)
	if !ok {
		return fmt.Errorf("not a snapshottable allocator")
	}
	if err := snapshottable.Restore(net.String(), data); err != nil {
		return fmt.Errorf("restoring snapshot encountered %v", err)
	}
	return nil
}

// contains 判断 IP 是否位于可分配范围内；若在范围内，返回 true 和对应偏移量，否则返回 false 和 nil。
// CIDR 的首地址和末地址不包含在可分配范围内。
func (r *Range) contains(ip net.IP) (bool, int) {
	if !r.net.Contains(ip) {
		return false, 0
	}

	offset := calculateIPOffset(r.base, ip)
	if offset < 0 || offset >= r.max {
		return false, 0
	}
	return true, offset
}

// Destroy 关闭底层分配器。
func (r *Range) Destroy() {
	r.alloc.Destroy()
}

// EnableMetrics 启用指标记录。
func (r *Range) EnableMetrics() {
	registerMetrics()
	r.metrics = &metricsRecorder{}
}

// calculateIPOffset 将 IP 地址映射成相对于 base 的整数偏移量，使 base + offset = ip；要求 ip 不小于 base。
func calculateIPOffset(base *big.Int, ip net.IP) int {
	return int(big.NewInt(0).Sub(netutils.BigForIP(ip), base).Int64())
}

// calculateRangeOffset 根据 CIDR 地址总量计算静态预留区大小，自动分配优先使用动态区。
// 预留区长度按 min(max(min, cidrSize/step), max) 计算，并随 CIDR 大小阶梯式变化。
func calculateRangeOffset(cidr *net.IPNet) int {
	// min(max($min, cidrSize/$step), $max) 的默认参数。
	const (
		min  = 16
		max  = 256
		step = 16
	)

	cidrSize := netutils.RangeSize(cidr)
	// 可分配地址数量始终小于 CIDR 所包含的地址总数。
	// 例如 /28 CIDR 共包含 16 个地址，但网络地址和广播地址不可分配。
	if cidrSize <= min {
		return 0
	}

	offset := cidrSize / step
	if offset < min {
		return min
	}
	if offset > max {
		return max
	}
	return int(offset)
}

// dryRunRange 是 Interface 的试运行适配层，不持久化实际状态。
type dryRunRange struct {
	real *Range
}

func (dry dryRunRange) Allocate(ip net.IP) error {
	return dry.real.allocate(ip, dryRunTrue)
}

func (dry dryRunRange) AllocateNext() (net.IP, error) {
	return dry.real.allocateNext(dryRunTrue)
}

func (dry dryRunRange) Release(ip net.IP) error {
	return dry.real.release(ip, dryRunTrue)
}

func (dry dryRunRange) ForEach(cb func(net.IP)) {
	dry.real.ForEach(cb)
}

func (dry dryRunRange) CIDR() net.IPNet {
	return dry.real.CIDR()
}

func (dry dryRunRange) IPFamily() api.IPFamily {
	return dry.real.IPFamily()
}

func (dry dryRunRange) DryRun() Interface {
	return dry
}

func (dry dryRunRange) Has(ip net.IP) bool {
	return dry.real.Has(ip)
}

func (dry dryRunRange) Destroy() {
}

func (dry dryRunRange) EnableMetrics() {
}
