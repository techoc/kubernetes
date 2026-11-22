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

// MaxHashBits is the max bit length which can be used from hash value.
// If we use all bits of hash value, the critical(last) card shuffled by
// Dealer will be uneven to 2:3 (first half:second half) at most,
// in order to reduce this unevenness to 32:33, we set MaxHashBits to 60 here.
//
// 这是公平性参数：取模运算天然会让高位余数略微偏向小值，限制到 60 位可把偏差压到
// 可接受范围。改动该值会改变同一 hash 下的发牌结果，属于对外行为变更。
const MaxHashBits = 60

// RequiredEntropyBits makes a quick and slightly conservative estimate of the number
// of bits of hash value that are consumed in shuffle sharding a deck of the given size
// to a hand of the given size.  The result is meaningful only if
// 1 <= handSize <= deckSize <= 1<<26.
//
// 上界估计（log2(deck)*hand），偏保守，用于在构造 Dealer 时提前拒绝熵不足的参数。
func RequiredEntropyBits(deckSize, handSize int) int {
	return int(math.Ceil(math.Log2(float64(deckSize)) * float64(handSize)))
}

// Dealer contains some necessary parameters and provides some methods for shuffle sharding.
// Dealer is thread-safe.
//
// 线程安全来自不可变性：Deal 只读字段，全部中间状态都在栈上。
type Dealer struct {
	deckSize int
	handSize int
}

// NewDealer will create a Dealer with the given deckSize and handSize, will return error when
// deckSize or handSize is invalid as below.
// 1. deckSize or handSize is not positive
// 2. handSize is greater than deckSize
// 3. deckSize is impractically large (greater than 1<<26)
// 4. required entropy bits of deckSize and handSize is greater than MaxHashBits
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

// Deal shuffles a card deck and deals a hand of cards, using the given hashValue as the source of entropy.
// The deck size and hand size are properties of the Dealer.
// This function synchronously makes sequential calls to pick, one for each dealt card.
// Each card is identified by an integer in the range [0, deckSize).
// For example, for deckSize=128 and handSize=4 this function might call pick(14); pick(73); pick(119); pick(26).
//
// 算法分两步：先反复对递减的牌堆大小取余，得到无放回抽样的“余数序列”；
// 再把余数映射回原始编号——前面的牌被抽走后后面的编号要相应后移，否则会重复或越界。
func (d *Dealer) Deal(hashValue uint64, pick func(int)) {
	// 15 is the largest possible value of handSize
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

// DealIntoHand shuffles and deals according to the Dealer's parameters,
// using the given hashValue as the source of entropy and then
// returns the dealt cards as a slice of `int`.
// If `hand` has the correct length as Dealer's handSize, it will be used as-is and no allocations will be made.
// If `hand` is nil or too small, it will be extended (performing an allocation).
// If `hand` is too large, a sub-slice will be returned.
//
// 复用调用方传入的切片是热路径上的分配优化；返回值的长度才是有效牌数，
// 调用方不应假定返回切片与入参长度一致。
func (d *Dealer) DealIntoHand(hashValue uint64, hand []int) []int {
	h := hand[:0]
	d.Deal(hashValue, func(card int) { h = append(h, card) })
	return h
}
