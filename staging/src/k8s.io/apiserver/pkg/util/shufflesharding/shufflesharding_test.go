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
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

// TestRequiredEntropyBits 验证熵位需求计算：handSize 张互不相同的牌放入 deckSize
// 张牌的牌堆，所需熵为 deckSize 的 handSize 次幂取 2 的对数向上取整。
// 这两个用例同时也是 MaxHashBits 边界的参照点（60 恰好是上限）。
func TestRequiredEntropyBits(t *testing.T) {
	tests := []struct {
		name         string
		deckSize     int
		handSize     int
		expectedBits int
	}{
		{
			"deckSize: 1024 handSize: 6",
			1024,
			6,
			60,
		},
		{
			"deckSize: 512 handSize: 8",
			512,
			8,
			72,
		},
	}

	for _, test := range tests {
		bits := RequiredEntropyBits(test.deckSize, test.handSize)
		if bits != test.expectedBits {
			t.Errorf("test %s fails: expected %v but got %v", test.name, test.expectedBits, bits)
			return
		}
	}
}

// TestNewDealer 覆盖构造 Dealer 的所有拒绝条件：
// 非正参数、手牌多于牌堆、牌堆过大、所需熵超过哈希位上限，以及一个正常用例。
// 错误信息按值比较，等于锁定了错误文案这一对测试可见的契约。
func TestNewDealer(t *testing.T) {
	tests := []struct {
		name     string
		deckSize int
		handSize int
		err      error
	}{
		{
			"deckSize <= 0",
			-100,
			8,
			fmt.Errorf("deckSize -100 or handSize 8 is not positive"),
		},
		{
			"handSize <= 0",
			100,
			0,
			fmt.Errorf("deckSize 100 or handSize 0 is not positive"),
		},
		{
			"handSize is greater than deckSize",
			100,
			101,
			fmt.Errorf("handSize 101 is greater than deckSize 100"),
		},
		{
			"deckSize is impractically large",
			1 << 27,
			2,
			fmt.Errorf("deckSize 134217728 is impractically large"),
		},
		{
			"required entropy bits is greater than MaxHashBits",
			512,
			8,
			fmt.Errorf("required entropy bits of deckSize 512 and handSize 8 is greater than 60"),
		},
		{
			"deckSize: 1024 handSize: 6",
			1024,
			6,
			nil,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewDealer(test.deckSize, test.handSize)
			if !reflect.DeepEqual(err, test.err) {
				t.Errorf("test %s fails: expected %v but got %v", test.name, test.err, err)
				return
			}
		})
	}
}

// TestCardDuplication 验证发牌的两个不变量：手牌数量正确、卡片在牌堆范围内且互不重复。
// 分片（shuffle sharding）的正确性依赖这两点——一旦重复，隔离假设即告失效。
func TestCardDuplication(t *testing.T) {
	tests := []struct {
		name     string
		deckSize int
		handSize int
	}{
		{
			"deckSize = handSize = 4",
			4,
			4,
		},
		{
			"deckSize = handSize = 8",
			8,
			8,
		},
		{
			"deckSize = handSize = 10",
			10,
			10,
		},
		{
			"deckSize = handSize = 12",
			12,
			12,
		},
		{
			"deckSize = 128, handSize = 8",
			128,
			8,
		},
		{
			"deckSize = 256, handSize = 7",
			256,
			7,
		},
		{
			"deckSize = 512, handSize = 6",
			512,
			6,
		},
	}
	for _, test := range tests {
		hashValue := rand.Uint64()
		t.Run(test.name, func(t *testing.T) {
			dealer, err := NewDealer(test.deckSize, test.handSize)
			if err != nil {
				t.Errorf("fail to create Dealer: %v", err)
				return
			}
			hand := make([]int, 0)
			hand = dealer.DealIntoHand(hashValue, hand)

			// check cards number
			if len(hand) != int(test.handSize) {
				t.Errorf("test case %s fails in cards number", test.name)
				return
			}

			// 检查卡牌范围与重复情况。
			cardMap := make(map[int]struct{})
			for _, card := range hand {
				if card < 0 || card >= int(test.deckSize) {
					t.Errorf("test case %s fails in range check", test.name)
					return
				}
				cardMap[card] = struct{}{}
			}
			if len(cardMap) != int(test.handSize) {
				t.Errorf("test case %s fails in duplication check", test.name)
				return
			}

		})
	}
}

// ff 计算下降阶乘 `n!/(n-m)!`，要求 n 为正、m 在 [0, n] 范围内，
// 且结果能放进 int。
// 且结果能放进 int。
func ff(n, m int) int {
	ans := 1
	for f := n; f > n-m; f-- {
		ans *= f
	}
	return ans
}

// TestUniformDistribution 穷举整个哈希空间，验证每手牌的命中次数都落在
// [floor, ceil] 的极窄区间内，即发牌近似均匀。
//
// 这是分布质量的核心证据：若映射偏斜，热门手牌会集中流量，破坏分片隔离效果。
// 对 deckSize=64 额外把哈希空间放大到"恰好覆盖"的规模，使断言更严格。
func TestUniformDistribution(t *testing.T) {
	const spare = 64 - MaxHashBits
	tests := []struct {
		deckSize, handSize int
		hashMax            int
	}{
		{64, 3, 1 << uint(math.Ceil(math.Log2(float64(ff(64, 3))))+spare)},
		{128, 3, ff(128, 3)},
		{50, 4, ff(50, 4)},
	}
	for _, test := range tests {
		dealer, err := NewDealer(test.deckSize, test.handSize)
		if err != nil {
			t.Errorf("fail to create Dealer: %v", err)
			return
		}
		handCoordinateMap := make(map[int]int) // 把编码后的手牌映射到出现次数。

		// 计算理论命中区间：哈希空间除以组合数，再乘每组的排列数。
		fallingFactorial := ff(test.deckSize, test.handSize)
		permutations := ff(test.handSize, test.handSize)
		allCoordinateCount := fallingFactorial / permutations
		nff := float64(test.hashMax) / float64(fallingFactorial)
		minCount := permutations * int(math.Floor(nff))
		maxCount := permutations * int(math.Ceil(nff))
		aHand := make([]int, test.handSize)
		for i := 0; i < test.hashMax; i++ {
			aHand = dealer.DealIntoHand(uint64(i), aHand)
			// 排序后手牌被视为组合而非排列，才能按"组合"统计命中次数。
			sort.IntSlice(aHand).Sort()
			handCoordinate := 0
			for _, card := range aHand {
				handCoordinate = handCoordinate<<7 + card
			}
			handCoordinateMap[handCoordinate]++
		}
		numHandsSeen := len(handCoordinateMap)

		t.Logf("Deck size = %v, hand size = %v, number of possible hands = %d, number of hands seen = %d, number of deals = %d, expected count range = [%v, %v]", test.deckSize, test.handSize, allCoordinateCount, numHandsSeen, test.hashMax, minCount, maxCount)

		// histogram 把（某手牌出现的次数）映射到（出现该次数的手牌数量）。
		histogram := make(map[int]int)
		for _, count := range handCoordinateMap {
			histogram[count] = histogram[count] + 1
		}

		var goodSum int
		for count := minCount; count <= maxCount; count++ {
			goodSum += histogram[count]
		}

		goodPct := 100 * float64(goodSum) / float64(numHandsSeen)

		t.Logf("good percentage = %v, histogram = %v", goodPct, histogram)
		// 要求全部手牌都在中心区间内，而不只是"大多数"——均匀性是硬性要求。
		if goodSum != numHandsSeen {
			t.Errorf("Only %v percent of the hands got a central count", goodPct)
		}
	}
}

// TestDealer_DealIntoHand 验证 DealIntoHand 对输入切片的处理：
// 无论传入 nil、长度不足、恰好或超长，最终返回的手牌长度都恰为 handSize。
// 该函数会复用并调整调用方切片，这些用例锁定了复用语义。
func TestDealer_DealIntoHand(t *testing.T) {
	dealer, _ := NewDealer(6, 6)

	tests := []struct {
		name         string
		hand         []int
		expectedSize int
	}{
		{
			"nil slice",
			nil,
			6,
		},
		{
			"empty slice",
			make([]int, 0),
			6,
		},
		{
			"size: 6 cap: 6 slice",
			make([]int, 6),
			6,
		},
		{
			"size: 6 cap: 12 slice",
			make([]int, 6, 12),
			6,
		},
		{
			"size: 4 cap: 4 slice",
			make([]int, 4),
			6,
		},
		{
			"size: 4 cap: 12 slice",
			make([]int, 4, 12),
			6,
		},
		{
			"size: 10 cap: 10 slice",
			make([]int, 10),
			6,
		},
		{
			"size: 10 cap: 12 slice",
			make([]int, 10, 12),
			6,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := dealer.DealIntoHand(0, test.hand)
			if len(h) != test.expectedSize {
				t.Errorf("test %s fails: expetced size %d but got %d", test.name, test.expectedSize, len(h))
				return
			}
		})
	}
}
