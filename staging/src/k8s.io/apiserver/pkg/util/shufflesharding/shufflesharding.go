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

package shufflesharding

import (
	"fmt"
	"math"
)

// MaxHashBits 限制从 hash 值中使用的位数。用全部位时，最后一张牌在两半间的分布
// 最差可达 2:3；限制到 60 位可把偏差压到 32:33。
//
// 这是公平性参数：取模运算天然会让高位余数略微偏向小值，限制到 60 位可把偏差压到
// 可接受范围。改动该值会改变同一 hash 下的发牌结果，属于对外行为变更。
const MaxHashBits = 60

// RequiredEntropyBits 粗略而略偏保守地估算洗牌分片所需消耗的 hash 位数。
// 结果仅在 1 <= handSize <= deckSize <= 1<<26 时有意义。
//
// 上界估计（log2(deck)*hand），偏保守，用于在构造 Dealer 时提前拒绝熵不足的参数。
func RequiredEntropyBits(deckSize, handSize int) int {
	return int(math.Ceil(math.Log2(float64(deckSize)) * float64(handSize)))
}

// Dealer 保存洗牌分片所需的参数并提供相应方法，线程安全。
//
// 线程安全来自不可变性：Deal 只读字段，全部中间状态都在栈上。
type Dealer struct {
	deckSize int
	handSize int
}

// NewDealer 用给定的 deckSize 与 handSize 构造 Dealer，参数非法时返回错误：
// 1. deckSize 或 handSize 非正数
// 2. handSize 大于 deckSize
// 3. deckSize 过大（超过 1<<26）
// 4. 所需熵位数超过 MaxHashBits
//
// 参数上限同时防止整数溢出与退化为“全量发牌”，保证发牌结果在给定 hash 下确定。
func NewDealer(deckSize, handSize int) (*Dealer, error) {
	if deckSize <= 0 || handSize <= 0 {
		return nil, fmt.Errorf("deckSize %d or handSize %d is not positive", deckSize, handSize)
	}
	if handSize > deckSize {
		return nil, fmt.Errorf("handSize %d is greater than deckSize %d", handSize, deckSize)
	}
	if deckSize > 1<<26 {
		return nil, fmt.Errorf("deckSize %d is impractically large", deckSize)
	}
	if RequiredEntropyBits(deckSize, handSize) > MaxHashBits {
		return nil, fmt.Errorf("required entropy bits of deckSize %d and handSize %d is greater than %d", deckSize, handSize, MaxHashBits)
	}

	return &Dealer{
		deckSize: deckSize,
		handSize: handSize,
	}, nil
}

// Deal 以给定 hashValue 为熵源洗牌并发牌，牌堆与手牌大小由 Dealer 决定。
// 该函数同步地逐张调用 pick；每张牌用 [0, deckSize) 内的整数标识。
// 例如 deckSize=128、handSize=4 时可能依次调用 pick(14)、pick(73)、pick(119)、pick(26)。
//
// 算法分两步：先反复对递减的牌堆大小取余，得到无放回抽样的“余数序列”；
// 再把余数映射回原始编号——前面的牌被抽走后后面的编号要相应后移，否则会重复或越界。
func (d *Dealer) Deal(hashValue uint64, pick func(int)) {
	// handSize 最大为 15
	//
	// 定长数组避免每次发牌都分配；15 来自 handSize 的业务上限（与 MaxHashBits 一起保证熵足够）。
	var remainders [15]int

	for i := 0; i < d.handSize; i++ {
		hashValueNext := hashValue / uint64(d.deckSize-i)
		remainders[i] = int(hashValue - uint64(d.deckSize-i)*hashValueNext)
		hashValue = hashValueNext
	}

	for i := 0; i < d.handSize; i++ {
		card := remainders[i]
		// 把第 i 次抽取的余数还原为原始牌堆中的编号：每有一个更早抽出的牌编号不大于它，
		// 当前编号就要 +1。
		for j := i; j > 0; j-- {
			if card >= remainders[j-1] {
				card++
			}
		}
		pick(card)
	}
}

// DealIntoHand 按 Dealer 的参数洗牌发牌，以 hashValue 为熵源，返回 `int` 切片。
// 传入长度恰好等于 handSize 的切片会直接复用，不产生分配；nil 或过短会扩容，
// 过长则返回其子切片。
//
// 复用调用方传入的切片是热路径上的分配优化；返回值的长度才是有效牌数，
// 调用方不应假定返回切片与入参长度一致。
func (d *Dealer) DealIntoHand(hashValue uint64, hand []int) []int {
	h := hand[:0]
	d.Deal(hashValue, func(card int) { h = append(h, card) })
	return h
}
