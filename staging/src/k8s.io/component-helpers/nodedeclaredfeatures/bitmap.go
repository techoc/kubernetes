/*
Copyright The Kubernetes Authors.

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

// Package nodedeclaredfeatures 负责把节点声明的能力（DeclaredFeatures）映射为紧凑的
// 位图集合，并据此推断 Pod 可以使用哪些特性。
//
// 核心思路是先用 FeatureMapper 给每个特性分配稳定的位下标，再用 Bitmap 做集合运算，
// 使“节点支持哪些特性”和“Pod 需要哪些特性”的比对可以在常数级位操作内完成。
package nodedeclaredfeatures

import (
	"fmt"
	"slices"
)

// bitmap 用固定长度的 uint64 切片承载特征集合，核心目的是让集合运算（差集、子集、相等）
// 退化成按字的位运算，避免为每个特征维护一个 map。
//
// words 存放位数据，size 单独记录逻辑位数。不能只看 len(words)：一个字能装 64 位，
// 逻辑位数不一定正好是 64 的倍数，末尾未使用的位不属于集合，比较时必须能区分开。
type bitmap struct {
	// words 以 uint64 字为单位存放位数据。
	words []uint64
	// size 是逻辑位数；末尾未使用的位不属于集合。
	size int
}

// newBitmap 创建一个可容纳 size 个逻辑比特位的新位图。
//
// 位数向上取整到 64 的倍数来分配字，这样任意一个合法下标都能通过 i/64 落到某个字里。
// size 为负说明调用方的特征数量本身算错了，属于编程错误，直接 panic 比返回半个可用对象好排查。
func newBitmap(size int) bitmap {
	if size < 0 {
		panic(fmt.Sprintf("bitmap size must be positive: %d", size))
	}
	return bitmap{make([]uint64, (size+63)/64), size}
}

// Set 置位指定下标对应的比特。
//
// 越界写入会落到错误的字甚至跳到别的特征上，静默处理会让结果难以追查，
// 因此这里把越界当成调用方 bug 直接 panic。
func (b bitmap) Set(i int) {
	if i < 0 || i >= b.size {
		panic(fmt.Sprintf("bitmap index out of range: %d", i))
	}

	b.words[i/64] |= 1 << (i % 64)
}

// Get 返回指定下标处比特位的值。
//
// 与 Set 对称：读取不存在的位没有合理语义，早期报错比返回 false 掩盖错误更省事。
func (b bitmap) Get(i int) bool {
	if i < 0 || i >= b.size {
		panic(fmt.Sprintf("bitmap index out of range: %d", i))
	}

	return b.words[i/64]&(1<<(i%64)) != 0
}

// Difference 返回一个新位图，包含 b 中置位而 other 中未置位的比特。
//
// 差集用按位清除（&^）一次处理一个字。两个集合的位位置来自同一套特征编号，
// 位数不同说明它们由不同版本的表构造而来，位置语义已对不上，只能报错。
// 这里返回新对象而不是原地修改，调用方可以安全地保留原集合。
func (b bitmap) Difference(other bitmap) (bitmap, error) {
	if b.size != other.size {
		return bitmap{}, fmt.Errorf("bitmap size mismatch: %d != %d", b.size, other.size)
	}

	diff := newBitmap(b.size)
	for i := range b.words {
		diff.words[i] = b.words[i] &^ other.words[i]
	}
	return diff, nil
}

// IsSubset 检查 b 是否为 other 的子集。
//
// b 中没有出现在 other 里的位即为不满足要求的特征。&^ 先清掉 other 覆盖的位，
// 只要剩下的字非零就说明有缺口，可以立刻返回 false。
func (b bitmap) IsSubset(other bitmap) (bool, error) {
	if b.size != other.size {
		return false, fmt.Errorf("bitmap size mismatch; %d != %d", b.size, other.size)
	}
	for i := range b.words {
		if b.words[i]&^other.words[i] != 0 {
			return false, nil
		}
	}
	return true, nil
}

// Equal 在 b 与 other 的置位完全一致时返回 true。
//
// size 不一致直接判不等；一致时末尾未使用的位必然同为 0，
// 因此逐字比较整个 words 即可，无需单独屏蔽最高字的尾部位。
func (b bitmap) Equal(other bitmap) bool {
	if b.size != other.size {
		return false
	}

	for i := range b.words {
		if b.words[i] != other.words[i] {
			return false
		}
	}
	return true
}

// IsEmpty 在位图没有任何置位时返回 true。
//
// 只看 words 是否全零就够了：size 只界定有意义的位范围，
// 未使用的位从未被置起，不会造成误判。
func (b bitmap) IsEmpty() bool {
	for i := range b.words {
		if b.words[i] != 0 {
			return false
		}
	}
	return true
}

// Clone 返回位图的深拷贝。
//
// words 是切片，赋值只会共享底层数组；调用方需要在副本上继续 Set 时，
// 必须真正复制一份，否则会连原集合一起改掉。size 是标量，直接带上即可。
func (b bitmap) Clone() bitmap {
	return bitmap{
		words: slices.Clone(b.words),
		size:  b.size,
	}
}

// String 返回位图的二进制字符串表示，最左侧字符对应第 0 位（第一个特征）。
//
// 输出按逻辑 size 而不是 len(words)*64 生成，避免把最高字里未使用的位打印出来。
// 下标 0 对应节点声明列表中的第一个特征，便于人眼和列表对照。
func (b bitmap) String() string {
	buf := make([]byte, b.size)
	for i := 0; i < b.size; i++ {
		if (b.words[i/64]>>(uint(i)%64))&1 != 0 {
			buf[i] = '1'
		} else {
			buf[i] = '0'
		}
	}
	return string(buf)
}
