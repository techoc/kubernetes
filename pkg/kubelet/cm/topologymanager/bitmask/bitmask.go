/*
Copyright 2019 The Kubernetes Authors.

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

package bitmask

import (
	"fmt"
	"math/bits"
	"strconv"
)

// BitMask 接口允许提示提供者为 TopologyHint 创建位掩码。
type BitMask interface {
	// Add 将一个或多个位加入掩码。
	Add(bits ...int) error
	// Remove 从掩码中移除一个或多个位。
	Remove(bits ...int) error
	// And 对当前掩码与给定掩码逐个执行按位与。
	And(masks ...BitMask)
	// Or 对当前掩码与给定掩码逐个执行按位或。
	Or(masks ...BitMask)
	// Clear 将掩码的所有位清零。
	Clear()
	// Fill 将掩码的所有位置一。
	Fill()
	// IsEqual 判断两个掩码是否相等。
	IsEqual(mask BitMask) bool
	// IsEmpty 判断掩码是否为空（所有位为零）。
	IsEmpty() bool
	// IsSet 判断指定位是否已置一。
	IsSet(bit int) bool
	// AnySet 判断给定的一组位中是否有任意一位已置一。
	AnySet(bits []int) bool
	// IsNarrowerThan 判断当前掩码是否比给定掩码更窄。
	IsNarrowerThan(mask BitMask) bool
	// IsLessThan 判断当前掩码的数值是否更小（低位更多）。
	IsLessThan(mask BitMask) bool
	// IsGreaterThan 判断当前掩码的数值是否更大（高位更多）。
	IsGreaterThan(mask BitMask) bool
	// String 返回掩码的二进制字符串表示。
	String() string
	// Count 返回掩码中已置一的位数。
	Count() int
	// GetBits 返回所有已置一的位号。
	GetBits() []int
}

// bitMask 是基于 uint64 的位掩码实现，最多可表示 64 个 NUMA 节点。
type bitMask uint64

// NewEmptyBitMask 创建一个空的 BitMask。
func NewEmptyBitMask() BitMask {
	s := bitMask(0)
	return &s
}

// NewBitMask 创建包含指定位的 BitMask。
func NewBitMask(bits ...int) (BitMask, error) {
	s := bitMask(0)
	err := (&s).Add(bits...)
	if err != nil {
		return nil, err
	}
	return &s, nil
}

// Add 将给定编号的位加入 BitMask。
func (s *bitMask) Add(bits ...int) error {
	mask := *s
	for _, i := range bits {
		// 位号必须落在 0-63 范围内，否则无法用 uint64 表示。
		if i < 0 || i >= 64 {
			return fmt.Errorf("bit number must be in range 0-63")
		}
		mask |= 1 << uint64(i)
	}
	// 全部位校验通过后再写回，避免部分修改导致掩码处于不一致状态。
	*s = mask
	return nil
}

// Remove 从 BitMask 中移除指定编号的位。
func (s *bitMask) Remove(bits ...int) error {
	mask := *s
	for _, i := range bits {
		if i < 0 || i >= 64 {
			return fmt.Errorf("bit number must be in range 0-63")
		}
		// 用 &^ 清除对应位。
		mask &^= 1 << uint64(i)
	}
	*s = mask
	return nil
}

// And 对掩码与给定掩码逐个执行按位与。
func (s *bitMask) And(masks ...BitMask) {
	for _, m := range masks {
		*s &= *m.(*bitMask)
	}
}

// Or 对掩码与给定掩码逐个执行按位或。
func (s *bitMask) Or(masks ...BitMask) {
	for _, m := range masks {
		*s |= *m.(*bitMask)
	}
}

// Clear 将掩码的所有位重置为零。
func (s *bitMask) Clear() {
	*s = 0
}

// Fill 将掩码的所有位置一。
func (s *bitMask) Fill() {
	*s = bitMask(^uint64(0))
}

// IsEmpty 判断掩码是否所有位都为零。
func (s *bitMask) IsEmpty() bool {
	return *s == 0
}

// IsSet 判断指定位是否已置一。
func (s *bitMask) IsSet(bit int) bool {
	// 越界位一律视为未置一，避免位移越界。
	if bit < 0 || bit >= 64 {
		return false
	}
	return (*s & (1 << uint64(bit))) > 0
}

// AnySet 判断给定的一组位中是否有任意一位已置一。
func (s *bitMask) AnySet(bits []int) bool {
	for _, b := range bits {
		if s.IsSet(b) {
			return true
		}
	}
	return false
}

// IsEqual 判断两个掩码是否相等。
func (s *bitMask) IsEqual(mask BitMask) bool {
	return *s == *mask.(*bitMask)
}

// IsNarrowerThan 判断一个掩码是否比另一个更窄。
//
// 若一个掩码置一的位数更少，则称其更“窄”。若两个掩码置一的位数相同，
// 则置一低位更多的掩码获胜。
func (s *bitMask) IsNarrowerThan(mask BitMask) bool {
	if s.Count() == mask.Count() {
		return s.IsLessThan(mask)
	}
	return s.Count() < mask.Count()
}

// IsLessThan 判断哪个位掩码置一的低位更多（数值更小者低位更多）。
func (s *bitMask) IsLessThan(mask BitMask) bool {
	return *s < *mask.(*bitMask)
}

// IsGreaterThan 判断哪个位掩码置一的高位更多（数值更大者高位更多）。
func (s *bitMask) IsGreaterThan(mask BitMask) bool {
	return *s > *mask.(*bitMask)
}

// String 将掩码转换为二进制字符串。
func (s *bitMask) String() string {
	grouping := 2
	for shift := 64 - grouping; shift > 0; shift -= grouping {
		// 从高位向低位按 2 位一组扫描，找到能容纳当前值的输出位宽。
		if *s > (1 << uint(shift)) {
			return fmt.Sprintf("%0"+strconv.Itoa(shift+grouping)+"b", *s)
		}
	}
	return fmt.Sprintf("%0"+strconv.Itoa(grouping)+"b", *s)
}

// Count 返回掩码中置一的位数。
func (s *bitMask) Count() int {
	return bits.OnesCount64(uint64(*s))
}

// GetBits 返回所有置一位的编号，按升序排列。
func (s *bitMask) GetBits() []int {
	var bits []int
	for i := uint64(0); i < 64; i++ {
		if (*s & (1 << i)) > 0 {
			bits = append(bits, int(i))
		}
	}
	return bits
}

// And 是包级函数，返回 first 与其余掩码按位与的结果。
func And(first BitMask, masks ...BitMask) BitMask {
	s := *first.(*bitMask)
	s.And(masks...)
	return &s
}

// Or 是包级函数，返回 first 与其余掩码按位或的结果。
func Or(first BitMask, masks ...BitMask) BitMask {
	s := *first.(*bitMask)
	s.Or(masks...)
	return &s
}

// IterateBitMasks 遍历给定位集合的所有非空子集，并对每个子集构造的掩码调用回调。
func IterateBitMasks(bits []int, callback func(BitMask)) {
	var iterate func(bits, accum []int, size int)
	// iterate 从 bits 中选取 size 个元素追加到 accum，选满后触发回调。
	iterate = func(bits, accum []int, size int) {
		// 组合达到目标大小时构造掩码并回调。
		if len(accum) == size {
			mask, _ := NewBitMask(accum...)
			callback(mask)
			return
		}
		for i := range bits {
			// 只从当前位之后继续选取，保证组合不重复且按升序生成。
			iterate(bits[i+1:], append(accum, bits[i]), size)
		}
	}

	// 按子集大小从 1 到 n 依次生成，覆盖所有非空子集。
	for i := 1; i <= len(bits); i++ {
		iterate(bits, []int{}, i)
	}
}
