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

package nodedeclaredfeatures

import (
	"fmt"
	"testing"
)

// TestNewBitmap 卡的是向上取整规则与 64 的边界：feature 列表会被映射到
// uint64 词数组，少分配一个词会在 Set 高位时越界，多分配则浪费且
// 让 Equal/IsEmpty 这类按词比较的判断行为不同。
func TestNewBitmap(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		expected int // 期望的 uint64 字数量
	}{
		{"Size 0", 0, 0},
		{"Size 1", 1, 1},
		{"Size 63", 63, 1},
		{"Size 64", 64, 1}, // (64+63)/64 = 1
		{"Size 65", 65, 2},
		{"Size 128", 128, 2}, // (128+63)/64 = 2
		{"Size 129", 129, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBitmap(tt.size)
			if len(b.words) != tt.expected {
				t.Errorf("len(b) = %d, want %d", len(b.words), tt.expected)
			}
		})
	}
}

// TestBitmap_GetSet 用 0、63、64、127 这些跨词边界的下标，专门盯住
// “词内偏移 + 词索引”的计算；只测连续小下标会把 i/64 与 i%64 写反的 bug 漏掉。
// 越界下标用 panic 而不是返回错误，是因为位图是内部数据结构，
// 越界意味着调用方传错了 feature 索引，属于编程错误，越早炸越好。
func TestBitmap_GetSet(t *testing.T) {
	b := newBitmap(128)
	indices := []int{0, 63, 64, 127}

	// 初始状态下所有目标位都应为 false。
	for _, i := range indices {
		if b.Get(i) {
			t.Errorf("expected bit %d to be false initially", i)
		}
	}

	// 逐位置位，覆盖每个字的首位与末位。
	for _, i := range indices {
		b.Set(i)
	}

	for _, i := range indices {
		if !b.Get(i) {
			t.Errorf("expected bit %d to be true after Set", i)
		}
	}

	// 确认未置位的邻居仍是 false，排除位偏移算错污染相邻位。
	for _, i := range []int{1, 62, 65} {
		if b.Get(i) {
			t.Errorf("expected bit %d to be false", i)
		}
	}

	// 越界边界：-1 和刚好等于 size 的 128，读写都必须 panic。
	for _, i := range []int{-1, 128} {
		msg := fmt.Sprintf("bitmap index out of range: %d", i)
		t.Run(fmt.Sprintf("Panic_Set_%d", i), func(t *testing.T) {
			defer expectPanic(t, msg)
			b.Set(i)
		})
		t.Run(fmt.Sprintf("Panic_Get_%d", i), func(t *testing.T) {
			defer expectPanic(t, msg)
			b.Get(i)
		})
	}
}

// TestBitmap_DifferenceSubset 把 Difference 和 IsSubset 放在一起测，
// 因为线上语义是“差集为空 <=> 是子集”，两条路径必须保持一致。
// 同时验证尺寸不同的位图直接报错，防止把不同 feature 注册表下的集合混在一起比较。
func TestBitmap_DifferenceSubset(t *testing.T) {
	tests := []struct {
		name         string
		bSize        int
		otherSize    int
		bSets        []int
		otherSets    []int
		expectedErr  bool
		expectedDiff []int // 期望的差集；为空当且仅当 b 是 other 的子集。
	}{
		{
			name:        "Mismatch size",
			bSize:       64,
			otherSize:   128,
			expectedErr: true,
		},
		{
			name:         "Disjoint sets",
			bSize:        64,
			otherSize:    64,
			bSets:        []int{1, 2},
			otherSets:    []int{3, 4},
			expectedDiff: []int{1, 2},
		},
		{
			name:         "Overlap",
			bSize:        64,
			otherSize:    64,
			bSets:        []int{1, 2, 3},
			otherSets:    []int{2, 3, 4},
			expectedDiff: []int{1}, // 2 和 3 被 other 覆盖，差集中消失
		},
		{
			name:         "Identical",
			bSize:        64,
			otherSize:    64,
			bSets:        []int{1, 2},
			otherSets:    []int{1, 2},
			expectedDiff: []int{},
		},
		{
			name:         "Subset",
			bSize:        64,
			otherSize:    64,
			bSets:        []int{1, 2},
			otherSets:    []int{1, 2, 3},
			expectedDiff: []int{},
		},
		{
			name:         "Superset",
			bSize:        64,
			otherSize:    64,
			bSets:        []int{1, 2, 3},
			otherSets:    []int{1, 2},
			expectedDiff: []int{3},
		},
		{
			name:         "Empty contains Empty",
			bSize:        64,
			otherSize:    64,
			expectedDiff: []int{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBitmap(tt.bSize)
			for _, i := range tt.bSets {
				b.Set(i)
			}
			other := newBitmap(tt.otherSize)
			for _, i := range tt.otherSets {
				other.Set(i)
			}

			// 校验差集：先确认错误路径，再逐位比对并统计置位数。
			diff, err := b.Difference(other)
			if tt.expectedErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				for _, i := range tt.expectedDiff {
					if !diff.Get(i) {
						t.Errorf("Difference: Index %d should be set", i)
					}
				}
				// 统计置位总数，防止多位同时置错却恰好通过逐位检查。
				count := 0
				for i := 0; i < tt.bSize; i++ {
					if diff.Get(i) {
						count++
					}
				}
				if count != len(tt.expectedDiff) {
					t.Errorf("Difference: Unexpected number of bits set, got %d, want %d", count, len(tt.expectedDiff))
				}
			}

			// 校验子集判断，并与差集结果互相印证（差集为空 <=> 是子集）。
			result, err := b.IsSubset(other)
			if tt.expectedErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				if result {
					t.Error("expected result to be false on error")
				}
			} else {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				expectedResult := len(tt.expectedDiff) == 0
				if result != expectedResult {
					t.Errorf("IsSubset result mismatch: got %v, want %v", result, expectedResult)
				}
			}
		})
	}
}

// TestBitmap_Equal 额外覆盖尺寸不同的情况：位图相等不只是“已置位的下标集合相同”，
// 注册表长度不同时也必须先判不等，否则高位词可能没被比较到。
func TestBitmap_Equal(t *testing.T) {
	b1 := newBitmap(64)
	b1.Set(1)

	b2 := newBitmap(64)
	b2.Set(1)

	b3 := newBitmap(64)
	b3.Set(2)

	// 尺寸不同的位图必须直接判不等，否则高位词可能没被比较到。
	b4 := newBitmap(128)
	b4.Set(1)

	if !b1.Equal(b2) {
		t.Error("expected b1 to equal b2")
	}
	if b1.Equal(b3) {
		t.Error("expected b1 not to equal b3")
	}
	if b1.Equal(b4) {
		t.Error("expected b1 not to equal b4")
	}
}

// TestBitmap_IsEmpty 覆盖两种状态：全零位图判空，以及置位后不再为空。
// 这里同时验证了 IsEmpty 不依赖 size 的具体取值，只看是否存在置位。
func TestBitmap_IsEmpty(t *testing.T) {
	b := newBitmap(64)
	if !b.IsEmpty() {
		t.Error("expected b to be empty")
	}

	b.Set(10)
	if b.IsEmpty() {
		t.Error("expected b not to be empty")
	}
}

// TestBitmap_Clone 验证拷贝是真拷贝：调用方拿到 FeatureSet 后可能就地 Set，
// 如果只复制了 words 切片的头部，改动会穿透回原始集合，
// 让“节点声明的能力”和“pod 需要的能力”互相污染。
func TestBitmap_Clone(t *testing.T) {
	b := newBitmap(64)
	b.Set(1)

	clone := b.Clone()
	if !b.Equal(clone) {
		t.Error("expected clone to equal original")
	}

	// 在副本上置位，原始位图必须保持不变（验证是深拷贝而非共享底层数组）。
	clone.Set(2)
	if clone.Equal(b) {
		t.Error("expected modified clone not to equal original")
	}
	if b.Get(2) {
		t.Error("expected original not to have bit 2 set")
	}
	if !clone.Get(2) {
		t.Error("expected clone to have bit 2 set")
	}
}

// TestBitmap_String 固定字符串的位序约定：最左边是 0 号位。
// 长度必须严格等于 size，多一位或少一位都会让日志/测试快照看起来合理但下标错位，
// 65 位的用例专门检查跨词后没有丢位或补零。
func TestBitmap_String(t *testing.T) {
	tests := []struct {
		name     string
		size     int
		sets     []int
		expected string
	}{
		{
			name:     "Size 5, bit 0 and 4 set",
			size:     5,
			sets:     []int{0, 4},
			expected: "10001",
		},
		{
			name:     "Size 10, no bits set",
			size:     10,
			sets:     []int{},
			expected: "0000000000",
		},
		{
			name:     "Size 65, bit 0 and 64 set",
			size:     65,
			sets:     []int{0, 64},
			expected: "10000000000000000000000000000000000000000000000000000000000000001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := newBitmap(tt.size)
			for _, s := range tt.sets {
				b.Set(s)
			}
			if got := b.String(); got != tt.expected {
				t.Errorf("got %q, want %q", got, tt.expected)
			}
		})
	}
}

// expectPanic 直接比对 recover 到的值与期望的消息字符串，
// 这样断言覆盖的是 panic 的具体原因，而不是“随便炸了个东西”。
func expectPanic(t *testing.T, expected interface{}) {
	r := recover()
	if r == nil {
		t.Errorf("expected panic %v, but did not panic", expected)
	} else if r != expected {
		t.Errorf("expected panic %v, but got %v", expected, r)
	}
}
