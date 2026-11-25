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

package allocator

import (
	"errors"
	"fmt"
	"math/big"
	"math/rand"
	"sync"
	"time"
)

// AllocationBitmap 使用 big.Int 的每一位表示一个连续资源块中的资源偏移量，并可原子地完成分配。
//
// 每个资源都对应一个偏移量；内部使用位图表示，每个偏移量对应一位。
//
// 资源被占用时，对应偏移位设为 1。
// r.count 始终等于位图中值为 1 的位数，也可以随时通过统计 r.allocated 中的置位数重新计算。
//
// TODO：考虑使用游程编码（RLE）压缩位图，以减少分配器的内存占用。
type AllocationBitmap struct {
	// strategy 定义如何从资源范围内选择下一个可用资源。
	strategy bitAllocator
	// max 表示当前资源池可分配资源的总数。
	max int
	// rangeSpec 是资源范围描述，与 RangeAllocation.Range 字段对应。
	rangeSpec string

	// lock 保护下面的成员，确保并发访问安全。
	lock sync.Mutex
	// count 表示当前已分配的资源数量。
	count int
	// allocated 是资源占用位图，每一位表示对应偏移量是否已分配。
	allocated *big.Int
}

// AllocationBitmap 实现 Interface 和 Snapshottable 接口。
var _ Interface = &AllocationBitmap{}
var _ Snapshottable = &AllocationBitmap{}

// bitAllocator 定义在位图中搜索并选择有效空闲资源的策略。
type bitAllocator interface {
	AllocateBit(allocated *big.Int, max, count int) (int, bool)
}

// NewAllocationMap 使用随机扫描策略创建资源分配位图。
func NewAllocationMap(max int, rangeSpec string) *AllocationBitmap {
	return NewAllocationMapWithOffset(max, rangeSpec, 0)
}

// NewAllocationMapWithOffset 创建带分段优先级的资源池，并通过 offset 将位图划分为两个区间。
// 自动分配优先从 offset 之后的动态区选择，耗尽后才使用前面的保留区；offset 必须小于位图的总大小。
func NewAllocationMapWithOffset(max int, rangeSpec string, offset int) *AllocationBitmap {
	a := AllocationBitmap{
		strategy: randomScanStrategyWithOffset{
			rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
			offset: offset,
		},
		allocated: big.NewInt(0),
		count:     0,
		max:       max,
		rangeSpec: rangeSpec,
	}

	return &a
}

// Allocate 预留调用方指定的偏移量，并在同一把锁内完成越界检查、位图写入和计数更新。
// 本次成功分配时返回 true；资源已被占用时返回 false。
func (r *AllocationBitmap) Allocate(offset int) (bool, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	// max 表示当前资源池可分配资源的总数。
	if offset < 0 || offset >= r.max {
		return false, fmt.Errorf("offset %d out of range [0,%d]", offset, r.max)
	}
	if r.allocated.Bit(offset) == 1 {
		return false, nil
	}
	r.allocated = r.allocated.SetBit(r.allocated, offset, 1)
	r.count++
	return true, nil
}

// AllocateNext 委托 bitAllocator 选择空闲位，再在锁保护下置位并递增已分配计数。
// 资源池耗尽时返回 (0, false, nil)。
func (r *AllocationBitmap) AllocateNext() (int, bool, error) {
	r.lock.Lock()
	defer r.lock.Unlock()

	next, ok := r.strategy.AllocateBit(r.allocated, r.max, r.count)
	if !ok {
		return 0, false, nil
	}
	r.count++
	r.allocated = r.allocated.SetBit(r.allocated, next, 1)
	return next, true, nil
}

// Release 将资源归还到资源池。释放尚未分配或超出范围的偏移量时不执行任何操作，也不会返回错误。
func (r *AllocationBitmap) Release(offset int) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.allocated.Bit(offset) == 0 {
		return nil
	}

	r.allocated = r.allocated.SetBit(r.allocated, offset, 0)
	r.count--
	return nil
}

const (
	// 计算 big.Word 的字节数。
	notZero   = uint64(^big.Word(0))
	wordPower = (notZero>>8)&1 + (notZero>>16)&1 + (notZero>>32)&1
	wordSize  = 1 << wordPower
)

// ForEach 对每个已分配的偏移量调用回调函数。遍历期间会持有分配器锁，因此不能在回调中修改该位图。
func (r *AllocationBitmap) ForEach(fn func(int)) {
	r.lock.Lock()
	defer r.lock.Unlock()

	words := r.allocated.Bits()
	for wordIdx, word := range words {
		bit := 0
		for word > 0 {
			if (word & 1) != 0 {
				fn((wordIdx * wordSize * 8) + bit)
				word = word &^ 1
			}
			bit++
			word = word >> 1
		}
	}
}

// Has 判断指定偏移量是否已分配；若已分配，对其调用 Allocate(offset) 将失败。
func (r *AllocationBitmap) Has(offset int) bool {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.allocated.Bit(offset) == 1
}

// Free 返回资源范围内尚未分配的资源数量。
func (r *AllocationBitmap) Free() int {
	r.lock.Lock()
	defer r.lock.Unlock()
	return r.max - r.count
}

// Snapshot 导出资源范围描述和位图字节；count 可在恢复时通过 countBits 重新计算。
func (r *AllocationBitmap) Snapshot() (string, []byte) {
	r.lock.Lock()
	defer r.lock.Unlock()

	return r.rangeSpec, r.allocated.Bytes()
}

// Restore 将资源池恢复到之前捕获的状态，仅接受相同的 rangeSpec，避免误加载其他资源范围的位图。
func (r *AllocationBitmap) Restore(rangeSpec string, data []byte) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.rangeSpec != rangeSpec {
		return errors.New("the provided range does not match the current range")
	}

	r.allocated = big.NewInt(0).SetBytes(data)
	r.count = countBits(r.allocated)

	return nil
}

// Destroy 在关闭时释放分配器持有的资源；当前实现没有额外清理工作。
func (r *AllocationBitmap) Destroy() {
}

// randomScanStrategy 从资源范围内随机选择起点，然后向前扫描空闲偏移量；到达范围末尾时会回绕到起点继续搜索。
type randomScanStrategy struct {
	rand *rand.Rand
}

func (rss randomScanStrategy) AllocateBit(allocated *big.Int, max, count int) (int, bool) {
	if count >= max {
		return 0, false
	}
	offset := rss.rand.Intn(max)
	for i := 0; i < max; i++ {
		at := (offset + i) % max
		if allocated.Bit(at) == 0 {
			return at, true
		}
	}
	return 0, false
}

var _ bitAllocator = randomScanStrategy{}

// randomScanStrategyWithOffset 随机选择起点并向前扫描空闲偏移量。资源范围被划分为两个区间：优先从上方动态分配区分配，
// 并在该区间内回绕搜索；如果动态区间已满，再尝试从下方区间分配。
type randomScanStrategyWithOffset struct {
	rand   *rand.Rand
	offset int
}

// 先随机扫描动态区 [offset,max)，优先避免占用静态预留区；动态区耗尽后才回退到 [0,offset)。
func (rss randomScanStrategyWithOffset) AllocateBit(allocated *big.Int, max, count int) (int, bool) {
	if count >= max {
		return 0, false
	}
	// 上方子区间的大小；自动分配时优先使用该区间。
	subrangeMax := max - rss.offset
	// 尝试从上方区间 [offset, max) 中分配资源。
	start := rss.rand.Intn(subrangeMax)
	for i := 0; i < subrangeMax; i++ {
		at := rss.offset + ((start + i) % subrangeMax)
		if allocated.Bit(at) == 0 {
			return at, true
		}
	}

	start = rss.rand.Intn(rss.offset)
	// 上方子区间已满；放弃前再尝试从第一个区间分配。
	for i := 0; i < rss.offset; i++ {
		at := (start + i) % rss.offset
		if allocated.Bit(at) == 0 {
			return at, true
		}
	}
	return 0, false
}

var _ bitAllocator = randomScanStrategyWithOffset{}
