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

package mergepatch

import (
	"fmt"
	"testing"
)

// TestHasConflicts 验证 HasConflicts 在类型/值/嵌套结构上的冲突判定：
// 同类型且值相等不算冲突；类型不同、值不同、标量与集合混用均算冲突。
// 注意 HasConflicts 是交换对称的，因此对调 A/B 也应得到相同结果。
func TestHasConflicts(t *testing.T) {
	testCases := []struct {
		A   interface{}
		B   interface{}
		Ret bool
	}{
		// 标量：同类型同值不冲突。
		{A: "hello", B: "hello", Ret: false},
		{A: "hello", B: "hell", Ret: true},
		{A: "hello", B: nil, Ret: true},
		{A: "hello", B: int64(1), Ret: true},
		{A: "hello", B: float64(1.0), Ret: true},
		{A: "hello", B: false, Ret: true},
		{A: int64(1), B: int64(1), Ret: false},
		{A: nil, B: nil, Ret: false},
		{A: false, B: false, Ret: false},
		{A: float64(3), B: float64(3), Ret: false},

		// 列表：元素逐项比较，类型不同即冲突。
		{A: "hello", B: []interface{}{}, Ret: true},
		{A: []interface{}{int64(1)}, B: []interface{}{}, Ret: true},
		{A: []interface{}{}, B: []interface{}{}, Ret: false},
		{A: []interface{}{int64(1)}, B: []interface{}{int64(1)}, Ret: false},
		{A: map[string]interface{}{}, B: []interface{}{int64(1)}, Ret: true},

		// map：只比较双方都存在的键；单边新增键不算冲突。
		{A: map[string]interface{}{}, B: map[string]interface{}{"a": int64(1)}, Ret: false},
		{A: map[string]interface{}{"a": int64(1)}, B: map[string]interface{}{"a": int64(1)}, Ret: false},
		{A: map[string]interface{}{"a": int64(1)}, B: map[string]interface{}{"a": int64(2)}, Ret: true},
		{A: map[string]interface{}{"a": int64(1)}, B: map[string]interface{}{"b": int64(2)}, Ret: false},

		// 嵌套列表在 map 内部逐项比较。
		{
			A:   map[string]interface{}{"a": []interface{}{int64(1)}},
			B:   map[string]interface{}{"a": []interface{}{int64(1)}},
			Ret: false,
		},
		{
			A:   map[string]interface{}{"a": []interface{}{int64(1)}},
			B:   map[string]interface{}{"a": []interface{}{}},
			Ret: true,
		},
		{
			A:   map[string]interface{}{"a": []interface{}{int64(1)}},
			B:   map[string]interface{}{"a": int64(1)},
			Ret: true,
		},

		// 含多个键的 map 与列表。
		{
			A:   map[string]interface{}{"a": int64(1), "b": int64(2)},
			B:   map[string]interface{}{"a": int64(1), "b": int64(0)},
			Ret: true,
		},
		{
			A:   map[string]interface{}{"a": int64(1), "b": int64(2)},
			B:   map[string]interface{}{"a": int64(1), "b": int64(2)},
			Ret: false,
		},
		{
			A:   map[string]interface{}{"a": int64(1), "b": int64(2)},
			B:   map[string]interface{}{"a": int64(1), "b": int64(0), "c": int64(3)},
			Ret: true,
		},
		{
			A:   map[string]interface{}{"a": int64(1), "b": int64(2)},
			B:   map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3)},
			Ret: false,
		},
		{
			A:   map[string]interface{}{"a": []interface{}{int64(1), int64(2)}},
			B:   map[string]interface{}{"a": []interface{}{int64(1), int64(0)}},
			Ret: true,
		},
		{
			A:   map[string]interface{}{"a": []interface{}{int64(1), int64(2)}},
			B:   map[string]interface{}{"a": []interface{}{int64(1), int64(2)}},
			Ret: false,
		},

		// 数值类型不可互换：调用方应保证 left/right 的数值类型一致。
		{A: int64(0), B: float64(0), Ret: true},
		// 其他类型同样不可互换。
		{A: int64(0), B: "0", Ret: true},
		{A: int64(0), B: nil, Ret: true},
		{A: int64(0), B: false, Ret: true},
		{A: "true", B: true, Ret: true},
		{A: "null", B: nil, Ret: true},
	}

	for _, testCase := range testCases {
		testStr := fmt.Sprintf("A = %#v, B = %#v", testCase.A, testCase.B)
		// HasConflicts 内部使用 map 遍历，键序不确定；重复运行多次以覆盖不同顺序。
		for try := 0; try < 10; try++ {
			out, err := HasConflicts(testCase.A, testCase.B)
			if err != nil {
				t.Errorf("%v: unexpected error: %v", testStr, err)
				break
			}
			if out != testCase.Ret {
				t.Errorf("%v: expected %t got %t", testStr, testCase.Ret, out)
				break
			}
			// 对调参数验证对称性。
			out, err = HasConflicts(testCase.B, testCase.A)
			if err != nil {
				t.Errorf("%v: unexpected error: %v", testStr, err)
				break
			}
			if out != testCase.Ret {
				t.Errorf("%v: expected reversed %t got %t", testStr, testCase.Ret, out)
				break
			}
		}
	}
}
