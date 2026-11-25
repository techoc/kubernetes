/*
Copyright 2025 The Kubernetes Authors.

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

package validate

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// 测试用结构体，覆盖可比较、不可比较、带 key 与带指针字段等情形。
type TestStruct struct {
	I int
	D string
}

type TestStructWithKey struct {
	Key string
	I   int
	D   string
}

type NonComparableKey struct {
	I *int
}

type NonComparableStruct struct {
	I int
	S []string
}

type NonComparableStructWithKey struct {
	Key string
	I   int
	S   []string
}

type NonComparableStructWithPtr struct {
	I int
	P *int
}

// TestEachValSliceVal 验证 EachValSliceVal 对值切片逐项回调：
// 创建场景下 oldVal 必须为 nil，且回调次数等于元素个数（空/nil 切片为零次）。
func TestEachValSliceVal(t *testing.T) {
	testEachValSliceVal(t, "valid", []int{11, 12, 13})
	testEachValSliceVal(t, "valid", []string{"a", "b", "c"})
	testEachValSliceVal(t, "valid", []TestStruct{{11, "a"}, {12, "b"}, {13, "c"}})

	testEachValSliceVal(t, "empty", []int{})
	testEachValSliceVal(t, "empty", []string{})
	testEachValSliceVal(t, "empty", []TestStruct{})

	testEachValSliceVal[int](t, "nil", nil)
	testEachValSliceVal[string](t, "nil", nil)
	testEachValSliceVal[TestStruct](t, "nil", nil)

	testEachValSliceValUpdate(t, "valid", []int{11, 12, 13})
	testEachValSliceValUpdate(t, "valid", []string{"a", "b", "c"})
	testEachValSliceValUpdate(t, "valid", []TestStruct{{11, "a"}, {12, "b"}, {13, "c"}})

	testEachValSliceValUpdate(t, "empty", []int{})
	testEachValSliceValUpdate(t, "empty", []string{})
	testEachValSliceValUpdate(t, "empty", []TestStruct{})

	testEachValSliceValUpdate[int](t, "nil", nil)
	testEachValSliceValUpdate[string](t, "nil", nil)
	testEachValSliceValUpdate[TestStruct](t, "nil", nil)
}

// testEachValSliceVal 是创建场景的通用辅助：回调必须收到 nil oldVal，且调用次数与元素数一致。
func testEachValSliceVal[T any](t *testing.T, name string, input []T) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			if oldVal != nil {
				t.Errorf("expected nil oldVal, got %v", *oldVal)
			}
			calls++
			return nil
		}
		_ = EachValSliceVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, nil, nil, vfn)
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// testEachValSliceValUpdate 是更新场景的通用辅助：
// 把 old 逆序后传入，验证匹配函数能把每项与其旧值配对（oldVal == newVal），且调用次数不变。
func testEachValSliceValUpdate[T any](t *testing.T, name string, input []T) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			if oldVal == nil {
				t.Fatalf("expected non-nil oldVal")
			}
			if !reflect.DeepEqual(*newVal, *oldVal) {
				t.Errorf("expected oldVal == newVal, got %v, %v", *oldVal, *newVal)
			}
			calls++
			return nil
		}
		old := make([]T, len(input))
		copy(old, input)
		slices.Reverse(old)
		match := func(a, b *T) bool { return reflect.DeepEqual(*a, *b) }
		_ = EachValSliceVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, old, match, match, vfn)
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// TestEachValSliceValRatcheting 验证更新时对 “未变化的元素” 跳过校验（ratcheting）：
// 仅调整顺序、删减元素或替换指针等语义等价的情形都不应触发回调。
func TestEachValSliceValRatcheting(t *testing.T) {
	testEachValSliceValRatcheting(t, "ComparableStruct same data different order",
		[]TestStruct{
			{11, "a"}, {12, "b"}, {13, "c"},
		},
		[]TestStruct{
			{11, "a"}, {13, "c"}, {12, "b"},
		},
		SemanticDeepEqual,
		nil,
	)
	testEachValSliceValRatcheting(t, "ComparableStruct less data in new, exist in old",
		[]TestStruct{
			{11, "a"}, {12, "b"}, {13, "c"},
		},
		[]TestStruct{
			{11, "a"}, {13, "c"},
		},
		DirectEqual,
		nil,
	)
	testEachValSliceValRatcheting(t, "Comparable struct with key same data different order",
		[]TestStructWithKey{
			{Key: "a", I: 11, D: "a"}, {Key: "b", I: 12, D: "b"}, {Key: "c", I: 13, D: "c"},
		},
		[]TestStructWithKey{
			{Key: "a", I: 11, D: "a"}, {Key: "c", I: 13, D: "c"}, {Key: "b", I: 12, D: "b"},
		},
		MatchFunc[*TestStructWithKey](func(a, b *TestStructWithKey) bool {
			return a.Key == b.Key
		}),
		DirectEqual,
	)
	testEachValSliceValRatcheting(t, "Comparable struct with key less data in new, exist in old",
		[]TestStructWithKey{
			{Key: "a", I: 11, D: "a"}, {Key: "b", I: 12, D: "b"}, {Key: "c", I: 13, D: "c"},
		},
		[]TestStructWithKey{
			{Key: "a", I: 11, D: "a"}, {Key: "c", I: 13, D: "c"},
		},
		MatchFunc[*TestStructWithKey](func(a, b *TestStructWithKey) bool {
			return a.Key == b.Key
		}),
		DirectEqual,
	)
	testEachValSliceValRatcheting(t, "NonComparableStruct same data different order",
		[]NonComparableStruct{
			{I: 11, S: []string{"a"}}, {I: 12, S: []string{"b"}}, {I: 13, S: []string{"c"}},
		},
		[]NonComparableStruct{
			{I: 11, S: []string{"a"}}, {I: 13, S: []string{"c"}}, {I: 12, S: []string{"b"}},
		},
		SemanticDeepEqual,
		nil,
	)
	testEachValSliceValRatcheting(t, "NonComparableStructWithKey same data different order",
		[]NonComparableStructWithKey{
			{Key: "a", I: 11, S: []string{"a"}}, {Key: "b", I: 12, S: []string{"b"}}, {Key: "c", I: 13, S: []string{"c"}},
		},
		[]NonComparableStructWithKey{
			{Key: "a", I: 11, S: []string{"a"}}, {Key: "b", I: 12, S: []string{"b"}}, {Key: "c", I: 13, S: []string{"c"}},
		},
		MatchFunc[*NonComparableStructWithKey](func(a, b *NonComparableStructWithKey) bool {
			return a.Key == b.Key
		}),
		SemanticDeepEqual,
	)

}

// testEachValSliceValRatcheting 用会立即报错的回调驱动 EachValSliceVal；
// 若 ratcheting 生效，回调不应被调用，因此错误列表必须为空。
func testEachValSliceValRatcheting[T any](t *testing.T, name string, old, new []T, match, equiv MatchFunc[*T]) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			return field.ErrorList{field.Invalid(fldPath, *newVal, "expected no calls")}
		}
		errs := EachValSliceVal(context.Background(), operation.Operation{Type: operation.Update}, field.NewPath("test"), new, old, match, equiv, vfn)
		if len(errs) > 0 {
			t.Errorf("expected no errors, got %d: %s", len(errs), fmtErrs(errs))
		}
	})
}

// TestEachMapVal 验证 EachMapVal 对 map 值逐项回调：创建场景下 oldVal 为 nil，回调次数等于条目数。
func TestEachMapVal(t *testing.T) {
	testEachMapVal(t, "valid", map[string]int{"one": 11, "two": 12, "three": 13})
	testEachMapVal(t, "valid", map[string]string{"A": "a", "B": "b", "C": "c"})
	testEachMapVal(t, "valid", map[string]TestStruct{"one": {11, "a"}, "two": {12, "b"}, "three": {13, "c"}})

	testEachMapVal(t, "empty", map[string]int{})
	testEachMapVal(t, "empty", map[string]string{})
	testEachMapVal(t, "empty", map[string]TestStruct{})

	testEachMapVal[int](t, "nil", nil)
	testEachMapVal[string](t, "nil", nil)
	testEachMapVal[TestStruct](t, "nil", nil)
}

// testEachMapVal 是 EachMapVal 创建场景的通用辅助。
func testEachMapVal[T any](t *testing.T, name string, input map[string]T) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			if oldVal != nil {
				t.Errorf("expected nil oldVal, got %v", *oldVal)
			}
			calls++
			return nil
		}
		_ = EachMapVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, nil, vfn)
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// TestEachMapValRatcheting 验证更新时仅对 “新增或值有变化的条目” 调用校验：
// 语义等价的 map（顺序不同、旧条目被删除、指针值相等）应零调用；
// nil 与空 map 视为等价，而 nil -> 非空会触发每个新条目的校验。
func TestEachMapValRatcheting(t *testing.T) {
	testEachMapValRatcheting(t, "primitive same data",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13, "two": 12},
		DirectEqual,
		0,
	)
	testEachMapValRatcheting(t, "primitive less data in new, exist in old",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13},
		DirectEqual,
		0,
	)
	testEachMapValRatcheting(t, "primitive new data, not exist in old",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13, "two": 12, "four": 14},
		DirectEqual,
		1,
	)
	testEachMapValRatcheting(t, "non comparable value, same data",
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"two":   {I: 12, S: []string{"b"}},
			"three": {I: 13, S: []string{"c"}},
		},
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"three": {I: 13, S: []string{"c"}},
			"two":   {I: 12, S: []string{"b"}},
		},
		SemanticDeepEqual,
		0,
	)
	testEachMapValRatcheting(t, "non comparable value, less data in new, exist in old",
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"two":   {I: 12, S: []string{"b"}},
			"three": {I: 13, S: []string{"c"}},
		},
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"three": {I: 13, S: []string{"c"}},
		},
		SemanticDeepEqual,
		0,
	)
	testEachMapValRatcheting(t, "non comparable value, new data, not exist in old",
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"two":   {I: 12, S: []string{"b"}},
			"three": {I: 13, S: []string{"c"}},
		},
		map[string]NonComparableStruct{
			"one":   {I: 11, S: []string{"a"}},
			"three": {I: 13, S: []string{"c"}},
			"two":   {I: 12, S: []string{"b"}},
			"four":  {I: 14, S: []string{"d"}},
		},
		SemanticDeepEqual,
		1,
	)
	testEachMapValRatcheting(t, "struct with pointer field, same value different pointer",
		map[string]NonComparableStructWithPtr{
			"one": {I: 11, P: new(1)},
			"two": {I: 12, P: new(2)},
		},
		map[string]NonComparableStructWithPtr{
			"one": {I: 11, P: new(1)},
			"two": {I: 12, P: new(2)},
		},
		SemanticDeepEqual,
		0,
	)
	testEachMapValRatcheting(t, "nil map to empty map",
		nil,
		map[string]int{},
		DirectEqual,
		0,
	)

	testEachMapValRatcheting(t, "nil map to non-empty map",
		nil,
		map[string]int{"one": 1},
		DirectEqual,
		1, // 新条目应执行校验。
	)

	testEachMapValRatcheting(t, "empty map to nil map",
		map[string]int{},
		nil,
		DirectEqual,
		0,
	)

	testEachMapValRatcheting(t, "non-empty map to nil map",
		map[string]int{"one": 1},
		nil,
		DirectEqual,
		0,
	)
}

// testEachMapValRatcheting 统计 EachMapVal 在更新场景下实际触发的校验次数，并与期望值比较。
func testEachMapValRatcheting[K ~string, V any](t *testing.T, name string, old, new map[K]V, equiv MatchFunc[*V], wantCalls int) {
	t.Helper()
	var zero V
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *V) field.ErrorList {
			calls++
			return nil
		}
		_ = EachMapVal(context.Background(), operation.Operation{Type: operation.Update}, field.NewPath("test"), new, old, equiv, vfn)
		if calls != wantCalls {
			t.Errorf("expected %d calls, got %d", wantCalls, calls)
		}
	})
}

// StringType 用于验证 ~string 约束下自定义 key 类型同样可用。
type StringType string

// TestEachMapKey 验证 EachMapKey 遍历所有 key，且创建场景下 oldVal 为 nil。
func TestEachMapKey(t *testing.T) {
	testEachMapKey(t, "valid", map[string]int{"one": 11, "two": 12, "three": 13})
	testEachMapKey(t, "valid", map[StringType]string{"A": "a", "B": "b", "C": "c"})
}

// testEachMapKey 是 EachMapKey 创建场景的通用辅助。
func testEachMapKey[K ~string, V any](t *testing.T, name string, input map[K]V) {
	t.Helper()
	var zero K
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *K) field.ErrorList {
			if oldVal != nil {
				t.Errorf("expected nil oldVal, got %v", *oldVal)
			}
			calls++
			return nil
		}
		_ = EachMapKey(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, vfn)
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// TestEachMapKeyRatcheting 验证更新时已存在于旧 map 中的 key 不再校验，只有新增 key 触发回调。
func TestEachMapKeyRatcheting(t *testing.T) {
	testEachMapKeyRatcheting(t, "same data, 0 validation calls",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13, "two": 12},
		0,
	)
	testEachMapKeyRatcheting(t, "less data in new, exist in old, 0 validation calls",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13},
		0,
	)
	testEachMapKeyRatcheting(t, "new data, not exist in old, 1 validation call",
		map[string]int{"one": 11, "two": 12, "three": 13},
		map[string]int{"one": 11, "three": 13, "two": 12, "four": 14},
		1,
	)
}

// testEachMapKeyRatcheting 统计 EachMapKey 在更新场景下实际触发的校验次数。
func testEachMapKeyRatcheting[K ~string, V any](t *testing.T, name string, old, new map[K]V, wantCalls int) {
	t.Helper()
	var zero V
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *K) field.ErrorList {
			calls++
			return nil
		}
		_ = EachMapKey(context.Background(), operation.Operation{Type: operation.Update}, field.NewPath("test"), new, old, vfn)
		if calls != wantCalls {
			t.Errorf("expected %d calls, got %d", wantCalls, calls)
		}
	})
}

// TestValSliceUniqueComparableValues 验证可比较类型的切片唯一性检查：
// 每个重复值都应产生一条错误，分别用直接比较与反射深比较两种 equiv 实现。
func TestValSliceUniqueComparableValues(t *testing.T) {
	testValSliceUnique(t, "int_nil", []int(nil), 0)
	testValSliceUnique(t, "int_empty", []int{}, 0)
	testValSliceUnique(t, "int_uniq", []int{1, 2, 3}, 0)
	testValSliceUnique(t, "int_dup", []int{1, 2, 3, 2, 1}, 2)

	testValSliceUnique(t, "string_nil", []string(nil), 0)
	testValSliceUnique(t, "string_empty", []string{}, 0)
	testValSliceUnique(t, "string_uniq", []string{"a", "b", "c"}, 0)
	testValSliceUnique(t, "string_dup", []string{"a", "a", "c", "b", "a"}, 2)

	type isComparable struct {
		I int
		S string
	}

	testValSliceUnique(t, "struct_nil", []isComparable(nil), 0)
	testValSliceUnique(t, "struct_empty", []isComparable{}, 0)
	testValSliceUnique(t, "struct_uniq", []isComparable{{1, "a"}, {2, "b"}, {3, "c"}}, 0)
	testValSliceUnique(t, "struct_dup", []isComparable{{1, "a"}, {2, "b"}, {3, "c"}, {2, "b"}, {1, "a"}}, 2)
}

// testValSliceUnique 以 direct 与 reflect 两种比较方式各跑一遍，期望错误数一致。
func testValSliceUnique[T comparable](t *testing.T, name string, input []T, wantErrs int) {
	t.Helper()
	t.Run(fmt.Sprintf("%s(direct)", name), func(t *testing.T) {
		errs := ValSliceUnique(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, DirectEqual)
		if len(errs) != wantErrs {
			t.Errorf("expected %d errors, got %d: %s", wantErrs, len(errs), fmtErrs(errs))
		}
	})
	t.Run(fmt.Sprintf("%s(reflect)", name), func(t *testing.T) {
		errs := ValSliceUnique(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, SemanticDeepEqual)
		if len(errs) != wantErrs {
			t.Errorf("expected %d errors, got %d: %s", wantErrs, len(errs), fmtErrs(errs))
		}
	})
}

// TestValSliceUniqueNonComparableValues 验证含切片字段（不可比较）的结构体只能走反射深比较路径。
func TestValSliceUniqueNonComparableValues(t *testing.T) {
	type nonComparable struct {
		I int
		S []string
	}

	testValSliceUniqueByReflect(t, "noncomp_nil", []nonComparable(nil), 0)
	testValSliceUniqueByReflect(t, "noncomp_empty", []nonComparable{}, 0)
	testValSliceUniqueByReflect(t, "noncomp_uniq", []nonComparable{{1, []string{"a"}}, {2, []string{"b"}}, {3, []string{"c"}}}, 0)
	testValSliceUniqueByReflect(t, "noncomp_dup", []nonComparable{
		{1, []string{"a"}},
		{2, []string{"b"}},
		{3, []string{"c"}},
		{2, []string{"b"}},
		{1, []string{"a"}}}, 2)
}

// testValSliceUniqueByReflect 仅使用反射深比较运行唯一性检查。
func testValSliceUniqueByReflect[T any](t *testing.T, name string, input []T, wantErrs int) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(%T)", name, zero), func(t *testing.T) {
		errs := ValSliceUnique(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, SemanticDeepEqual)
		if len(errs) != wantErrs {
			t.Errorf("expected %d errors, got %d: %s", wantErrs, len(errs), fmtErrs(errs))
		}
	})
}

// TestEachPtrSliceVal 验证指针切片遍历：nil 元素被跳过，只对非 nil 元素回调。
func TestEachPtrSliceVal(t *testing.T) {
	testEachPtrSliceVal(t, "valid", []*int{new(11), new(12), new(13)})
	testEachPtrSliceVal(t, "valid", []*string{new("a"), new("b"), new("c")})
	testEachPtrSliceVal(t, "valid", []*TestStruct{{11, "a"}, {12, "b"}, {13, "c"}})

	testEachPtrSliceVal(t, "empty", []*int{})
	testEachPtrSliceVal[int](t, "nil", nil)

	// 测试 nil 元素：应被跳过，只有两个非 nil 元素触发回调
	t.Run("nil elements", func(t *testing.T) {
		input := []*int{new(11), nil, new(13)}
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *int) field.ErrorList {
			calls++
			return nil
		}
		errs := EachPtrSliceVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, nil, nil, vfn)
		if len(errs) != 0 {
			t.Errorf("expected 0 errors, got %d", len(errs))
		}
		if calls != 2 {
			t.Errorf("expected 2 calls, got %d", calls)
		}
	})

	testEachPtrSliceValUpdate(t, "valid", []*int{new(11), new(12), new(13)})
}

// testEachPtrSliceVal 是指针切片创建场景的通用辅助。
func testEachPtrSliceVal[T any](t *testing.T, name string, input []*T) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(*%T)", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			if oldVal != nil {
				t.Errorf("expected nil oldVal, got %v", *oldVal)
			}
			calls++
			return nil
		}
		errs := EachPtrSliceVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, nil, nil, vfn)
		if len(errs) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(errs), errs)
		}
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// testEachPtrSliceValUpdate 是指针切片更新场景的通用辅助：old 逆序后按值匹配到对应旧元素。
func testEachPtrSliceValUpdate[T any](t *testing.T, name string, input []*T) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(*%T) update", name, zero), func(t *testing.T) {
		calls := 0
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			if oldVal == nil {
				t.Fatalf("expected non-nil oldVal")
			}
			if !reflect.DeepEqual(*newVal, *oldVal) {
				t.Errorf("expected oldVal == newVal, got %v, %v", *oldVal, *newVal)
			}
			calls++
			return nil
		}
		old := make([]*T, len(input))
		copy(old, input)
		slices.Reverse(old)
		match := func(a, b *T) bool { return reflect.DeepEqual(*a, *b) }
		errs := EachPtrSliceVal(context.Background(), operation.Operation{}, field.NewPath("test"), input, old, match, match, vfn)
		if len(errs) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(errs), errs)
		}
		if calls != len(input) {
			t.Errorf("expected %d calls, got %d", len(input), calls)
		}
	})
}

// TestEachPtrSliceValRatcheting 验证指针切片在仅调整顺序时不会触发校验。
func TestEachPtrSliceValRatcheting(t *testing.T) {
	testEachPtrSliceValRatcheting(t, "ComparableStruct same data different order",
		[]*TestStruct{{11, "a"}, {12, "b"}, {13, "c"}},
		[]*TestStruct{{11, "a"}, {13, "c"}, {12, "b"}},
		SemanticDeepEqual,
		nil,
	)
}

// testEachPtrSliceValRatcheting 用会报错的回调驱动 EachPtrSliceVal，验证 ratcheting 生效时零调用。
func testEachPtrSliceValRatcheting[T any](t *testing.T, name string, old, new []*T, match, equiv MatchFunc[*T]) {
	t.Helper()
	var zero T
	t.Run(fmt.Sprintf("%s(*%T)", name, zero), func(t *testing.T) {
		vfn := func(ctx context.Context, op operation.Operation, fldPath *field.Path, newVal, oldVal *T) field.ErrorList {
			return field.ErrorList{field.Invalid(fldPath, *newVal, "expected no calls")}
		}
		errs := EachPtrSliceVal(context.Background(), operation.Operation{Type: operation.Update}, field.NewPath("test"), new, old, match, equiv, vfn)
		if len(errs) != 0 {
			t.Errorf("expected 0 errors, got %d: %v", len(errs), errs)
		}
	})
}

// TestPtrSliceUnique 验证指针切片唯一性：按指向的值判重，nil 元素不参与比较也不产生错误。
func TestPtrSliceUnique(t *testing.T) {
	testPtrSliceUnique(t, "int_nil", []*int(nil), 0, 0)
	testPtrSliceUnique(t, "int_empty", []*int{}, 0, 0)
	testPtrSliceUnique(t, "int_uniq", []*int{new(1), new(2), new(3)}, 0, 0)
	testPtrSliceUnique(t, "int_dup", []*int{new(1), new(2), new(3), new(2), new(1)}, 2, 0)
	testPtrSliceUnique(t, "int_nil_element", []*int{new(1), nil, new(3), nil}, 0, 0)
	testPtrSliceUnique(t, "int_dup_and_nil", []*int{new(1), nil, new(1), nil}, 1, 0)
}

// testPtrSliceUnique 分别统计重复（Duplicate）与必填（Required）两类错误数量。
func testPtrSliceUnique[T comparable](t *testing.T, name string, input []*T, wantDupErrs, wantReqErrs int) {
	t.Helper()
	t.Run(fmt.Sprintf("%s(direct)", name), func(t *testing.T) {
		errs := PtrSliceUnique(context.Background(), operation.Operation{}, field.NewPath("test"), input, nil, DirectEqual)
		gotDup, gotReq := countErrors(errs)
		if gotDup != wantDupErrs || gotReq != wantReqErrs {
			t.Errorf("expected %d dup, %d req errors; got %d dup, %d req: %s", wantDupErrs, wantReqErrs, gotDup, gotReq, fmtErrs(errs))
		}
	})
}

// countErrors 按错误类型统计 Duplicate 与 Required 错误数。
func countErrors(errs field.ErrorList) (dup, req int) {
	for _, err := range errs {
		switch err.Type {
		case field.ErrorTypeDuplicate:
			dup++
		case field.ErrorTypeRequired:
			req++
		}
	}
	return
}

// TestPtrSliceNoNils 验证 PtrSliceNoNils 为每个 nil 元素生成一条 Required 错误，非 nil 元素不报错。
func TestPtrSliceNoNils(t *testing.T) {
	tests := []struct {
		name     string
		input    []*int
		wantErrs int
	}{
		{"nil", nil, 0},
		{"empty", []*int{}, 0},
		{"no_nil", []*int{new(1), new(2)}, 0},
		{"has_nil", []*int{new(1), nil, new(3), nil}, 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			errs := PtrSliceNoNils(context.Background(), operation.Operation{}, field.NewPath("test"), tt.input, nil)
			if len(errs) != tt.wantErrs {
				t.Errorf("expected %d errors, got %d: %v", tt.wantErrs, len(errs), errs)
			}
			for _, err := range errs {
				if err.Type != field.ErrorTypeRequired {
					t.Errorf("expected Required error, got %v", err.Type)
				}
			}
		})
	}
}
