/*
Copyright 2017 The Kubernetes Authors.

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

package fuzzer

import "testing"

// TestValueFuzz 验证 ValueFuzz 的改写边界：它必须就地改写容器里的可变元素（指针指向的对象、map 的条目、
// 切片的元素），但绝不能替换结构体字段本身持有的指针、map 和切片，也不能让这些引用与原对象断开。
// 后半部分是关键——替换引用会让调用方持有的引用失效，从而掩盖调用方“以为在改原对象、实际改的是副本”的错误。
func TestValueFuzz(t *testing.T) {
	type (
		Y struct {
			I int
			B bool
			F float32
			U uint
		}
		X struct {
			Ptr   *X
			Y     Y
			Map   map[string]int
			Slice []int
		}
	)

	// 构造一个同时带指针、map、切片的样本，覆盖 ValueFuzz 可能触及的全部三类引用。
	x := X{
		Ptr:   &X{},
		Map:   map[string]int{"foo": 42},
		Slice: []int{1, 2, 3},
	}

	// 在改写前固定引用，用于稍后断言这些引用本身没有被替换掉。
	p := x.Ptr
	m := x.Map
	s := x.Slice

	ValueFuzz(x)

	// 引用指向的内容必须已被改写，说明 ValueFuzz 确实穿透引用、改到了共享状态。
	if x.Ptr.Y.I == 0 {
		t.Errorf("x.Ptr.Y.I should have changed")
	}

	if x.Map["foo"] == 42 {
		t.Errorf("x.Map[foo] should have changed")
	}

	if x.Slice[0] == 1 {
		t.Errorf("x.Slice[0] should have changed")
	}

	// 指针身份必须保持不变；若变了，说明 ValueFuzz 分配了新对象而不是就地改写。
	if x.Ptr != p {
		t.Errorf("x.Ptr changed")
	}

	// 通过外部引用写入，验证 map 仍是同一张表；值被改回后 x.Map 也必须同步看到，证明没有拷贝隔离。
	m["foo"] = 7
	if x.Map["foo"] != m["foo"] {
		t.Errorf("x.Map changed")
	}
	s[0] = 7
	if x.Slice[0] != s[0] {
		t.Errorf("x.Slice changed")
	}
}
