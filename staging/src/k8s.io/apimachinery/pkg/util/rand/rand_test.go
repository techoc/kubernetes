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

package rand

import (
	"math/rand"
	"strings"
	"testing"
)

const (
	// maxRangeTestCount 是范围类测试的重复次数。随机函数无法穷举验证，
	// 用较多次采样提高发现越界或分布异常的概率，同时保持测试耗时可控。
	maxRangeTestCount = 500
	// testStringLength 是基准测试中使用的字符串长度，代表实际场景的典型量级。
	testStringLength = 32
)

// TestString 验证 String 生成的字符串长度准确，且字符均来自约定的安全字符集。
//
// 该字符集刻意去掉了易混淆的字符（如 0/o、1/l），以便随机名称可被人类准确抄写。
func TestString(t *testing.T) {
	valid := "bcdfghjklmnpqrstvwxz2456789"
	for _, l := range []int{0, 1, 2, 10, 123} {
		s := String(l)
		// 长度为 0 也必须返回空串而不是 panic，调用方可能传入零值。
		if len(s) != l {
			t.Errorf("expected string of size %d, got %q", l, s)
		}
		for _, c := range s {
			if !strings.ContainsRune(valid, c) {
				t.Errorf("expected valid characters, got %v", c)
			}
		}
	}
}

// 确认非法输入会触发 panic。
func TestRangePanic(t *testing.T) {
	defer func() {
		if err := recover(); err == nil {
			t.Errorf("Panic didn't occur!")
		}
	}()
	// 0 不是合法的上界，应当 panic。
	Intn(0)
}

// TestIntn 验证 Intn 的结果落在 [0, max) 区间内。
func TestIntn(t *testing.T) {
	//0 无效。
	for _, max := range []int{1, 2, 10, 123} {
		inrange := Intn(max)
		// 对 max=1 而言只有 0 是合法结果，边界值必须覆盖。
		if inrange < 0 || inrange > max {
			t.Errorf("%v out of range (0,%v)", inrange, max)
		}
	}
}

// TestPerm 验证 Perm 与 math/rand 在相同种子下产生完全一致的排列。
//
// 使用固定种子是为了让结果可复现：Perm 的语义要求确定性，
// 若与标准库实现出现偏差，会导致依赖随机排列的测试变得不稳定。
func TestPerm(t *testing.T) {
	Seed(5)
	r := rand.New(rand.NewSource(5))
	for i := 1; i < 20; i++ {
		actual := Perm(i)
		expected := r.Perm(i)
		for j := 0; j < i; j++ {
			if actual[j] != expected[j] {
				t.Errorf("Perm call result is unexpected")
			}
		}
	}
}

// TestIntnRange 反复采样，验证 IntnRange 的结果落在 [min, max) 区间内。
func TestIntnRange(t *testing.T) {
	//0 无效。
	for min, max := range map[int]int{1: 2, 10: 123, 100: 500} {
		for i := 0; i < maxRangeTestCount; i++ {
			inrange := IntnRange(min, max)
			// 上界为开区间，等于 max 即为越界。
			if inrange < min || inrange >= max {
				t.Errorf("%v out of range (%v,%v)", inrange, min, max)
			}
		}
	}
}

// TestInt63nRange 与 TestIntnRange 相同，但覆盖 int64 版本，验证大范围取值不会溢出。
func TestInt63nRange(t *testing.T) {
	//0 无效。
	for min, max := range map[int64]int64{1: 2, 10: 123, 100: 500} {
		for i := 0; i < maxRangeTestCount; i++ {
			inrange := Int63nRange(min, max)
			if inrange < min || inrange >= max {
				t.Errorf("%v out of range (%v,%v)", inrange, min, max)
			}
		}
	}
}

// BenchmarkRandomStringGeneration 衡量固定长度随机字符串的生成开销。
// 基准测试前重置计时器，生成结束后停止计时，避免把框架开销计入结果；
// 最后检查结果非空，防止编译器把整个循环优化掉。
func BenchmarkRandomStringGeneration(b *testing.B) {
	b.ResetTimer()
	var s string
	for i := 0; i < b.N; i++ {
		s = String(testStringLength)
	}
	b.StopTimer()
	if len(s) == 0 {
		b.Fatal(s)
	}
}
