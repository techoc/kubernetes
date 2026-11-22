/*
Copyright 2024 The Kubernetes Authors.

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
	"slices"
	"sort"

	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// MatchFunc is a function that compares two values of the same type,
// according to some criteria, and returns true if they match.
//
// match 既可用于完整比较，也可只比较作为唯一标识的字段，
// 后者需要配合 equiv 才能正确实现 ratcheting。
type MatchFunc[T any] func(T, T) bool

// EachValSliceVal performs validation on each element of a slice of values
// using the provided validation function.
//
// For update operations, the match function finds corresponding values in
// oldSlice for each value in newSlice. This comparison can be either full or
// partial (e.g., matching only specific struct fields that serve as a unique
// identifier). If match is nil, validation proceeds without considering old
// values, and the equiv function is not used.
//
// For update operations, the equiv function checks if a new value is
// equivalent to its corresponding old value, enabling validation ratcheting.
// If equiv is nil but match is provided, the match function is assumed to
// perform full value comparison.
//
// The match and equiv functions will never be called with nil arguments.
//
// Note: The slice element type must be non-nilable.
//
// 这是切片版 ratcheting 的核心：只有 update 且能在旧值中找到对应元素、
// 并且确认其等价时才跳过校验，从而允许既有数据继续通过新加的校验规则。
func EachValSliceVal[T any](ctx context.Context, op operation.Operation, fldPath *field.Path, newSlice, oldSlice []T,
	match, equiv MatchFunc[*T], validator ValidateFunc[*T]) field.ErrorList {
	var errs field.ErrorList
	for i := range newSlice {
		// 取地址而不是复制，保证 validator 看到的是新切片中的实际元素。
		val := &newSlice[i]

		var old *T
		if match != nil && len(oldSlice) > 0 {
			old = lookup(oldSlice, val, match)
		}
		// If the operation is an update, for validation ratcheting, skip re-validating if the old
		// value exists and either:
		// 1. The match function provides full comparison (equiv is nil)
		// 2. The equiv function confirms the values are equivalent (either directly or semantically)
		//
		// The equiv function provides equality comparison when match uses partial comparison.
		//
		// equiv 为 nil 意味着 match 已经做了完整比较，找到即等价。
		if op.Type == operation.Update && old != nil && (equiv == nil || equiv(val, old)) {
			continue
		}
		errs = append(errs, validator(ctx, op, fldPath.Index(i), val, old)...)
	}
	return errs
}

// EachPtrSliceVal performs validation on each element of a slice of pointers
// using the provided validation function.
//
// For update operations, the match function finds corresponding values in
// oldSlice for each value in newSlice. This comparison can be either full or
// partial (e.g., matching only specific struct fields that serve as a unique
// identifier). If match is nil, validation proceeds without considering old
// values, and the equiv function is not used.
//
// For update operations, the equiv function checks if a new value is
// equivalent to its corresponding old value, enabling validation ratcheting.
// If equiv is nil but match is provided, the match function is assumed to
// perform full value comparison.
//
// The match and equiv functions will never be called with nil arguments.
//
// 指针切片版本额外容忍 nil 元素：nil 的必填性由 PtrSliceNoNils 单独负责，
// 这里跳过可避免在解引用 nil 时 panic，也避免重复报错。
func EachPtrSliceVal[T any](ctx context.Context, op operation.Operation, fldPath *field.Path, newSlice, oldSlice []*T,
	match, equiv MatchFunc[*T], validator ValidateFunc[*T]) field.ErrorList {
	var errs field.ErrorList
	for i := range newSlice {
		val := newSlice[i]
		if val == nil {
			// Ignore nil items; they are supposed to have been checked by PtrSliceNoNils.
			continue
		}

		var old *T
		if match != nil && len(oldSlice) > 0 {
			old = lookupPointer(oldSlice, val, match)
		}
		// ratcheting 逻辑与值切片一致，只是旧值来自指针切片。
		if op.Type == operation.Update && old != nil && (equiv == nil || equiv(val, old)) {
			continue
		}
		errs = append(errs, validator(ctx, op, fldPath.Index(i), val, old)...)
	}
	return errs
}

// lookup returns a pointer to the first element in the list that matches the
// target, according to the provided comparison function, or else nil.
//
// 返回首个匹配项是约定：当列表中存在多个"相同标识"的元素时，
// 唯一性校验会另行报告，这里无需关心。
func lookup[T any](list []T, target *T, match MatchFunc[*T]) *T {
	for i := range list {
		if match(&list[i], target) {
			return &list[i]
		}
	}
	return nil
}

// lookupPointer returns the first non-nil element in the list that matches the
// target, according to the provided comparison function, or else nil.
// Nil elements in the list are skipped.
//
// 旧数据中可能残留 nil 指针，直接跳过比报错更符合 ratcheting 的宽松语义。
func lookupPointer[T any](list []*T, target *T, match MatchFunc[*T]) *T {
	for i := range list {
		if list[i] == nil {
			// We can't really do anything about nil entries in the old list,
			// just skip them.
			continue
		}
		if match(list[i], target) {
			return list[i]
		}
	}
	return nil
}

// EachMapVal validates each value in newMap using the specified validation
// function, passing the corresponding old value from oldMap if the key exists in oldMap.
// For update operations, it implements validation ratcheting by skipping validation
// when the old value exists and the equiv function confirms the values are equivalent.
// The value-type of the map is assumed to not be nilable.
//
// The equiv function will never be called with nil arguments.
//
// If equiv is nil, value-based ratcheting is disabled and all values will be validated.
//
// map 的键是 string 别名，键本身由 EachMapKey 负责校验，这里只校验值。
func EachMapVal[K ~string, V any](ctx context.Context, op operation.Operation, fldPath *field.Path, newMap, oldMap map[K]V,
	equiv MatchFunc[*V], validator ValidateFunc[*V]) field.ErrorList {
	var errs field.ErrorList
	for key, val := range newMap {
		var old *V
		if o, found := oldMap[key]; found {
			// 复制一份再取地址，避免把 range 变量的地址交给 validator 后混淆。
			old = &o
		}
		// If the operation is an update, for validation ratcheting, skip re-validating if the old
		// value is found and the equiv function confirms the values are equivalent.
		if op.Type == operation.Update && old != nil && equiv != nil && equiv(&val, old) {
			continue
		}
		errs = append(errs, validator(ctx, op, fldPath.Key(string(key)), &val, old)...)
	}
	return errs
}

// EachMapKey validates each element of newMap with the specified
// validation function.
//
// 只对新增的键调用校验函数：键一旦存在于旧 map，就视为已通过（ratcheting）。
func EachMapKey[K ~string, T any](ctx context.Context, op operation.Operation, fldPath *field.Path, newMap, oldMap map[K]T,
	validator ValidateFunc[*K]) field.ErrorList {
	var errs field.ErrorList
	for key := range newMap {
		var old *K
		if _, found := oldMap[key]; found {
			old = &key
		}
		// If the operation is an update, for validation ratcheting, skip re-validating if
		// the key is found in oldMap.
		if op.Type == operation.Update && old != nil {
			continue
		}
		// Note: the field path is the field, not the key.
		errs = append(errs, validator(ctx, op, fldPath, &key, nil)...)
	}
	return errs
}

// ValSliceUnique verifies that each element of a slice of values is unique,
// according to the match function. It compares every element of the slice with
// every other element and returns errors for non-unique items.
//
// The match function will never be called with nil arguments.
//
// 复杂度 O(n^2) 但切片通常很小；只把"后出现"的重复项记为错误，
// 保证每个重复组只报告一次，而不是所有两两组合。
func ValSliceUnique[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, newSlice, _ []T, match MatchFunc[*T]) field.ErrorList {
	var dups []int
	for i := range newSlice {
		for j := i + 1; j < len(newSlice); j++ {
			// 延迟分配 dups，无重复时避免任何额外分配。
			if match(&newSlice[i], &newSlice[j]) {
				if dups == nil {
					dups = make([]int, 0, len(newSlice))
				}
				// 同一个 j 可能与多个 i 重复，去重以免对同一位置报多次。
				if !slices.Contains(dups, j) {
					dups = append(dups, j)
				}
			}
		}
	}

	var errs field.ErrorList
	// 按索引排序，使错误顺序稳定，便于测试和用户阅读。
	sort.Ints(dups)
	for _, i := range dups {
		var val any = newSlice[i]
		// TODO: we don't want the whole item to be logged in the error, just
		// the key(s). Unfortunately, the way errors are rendered, it comes out
		// as something like "map[string]any{...}" which is not very nice. Once
		// that is fixed, we can consider adding a way for this function to
		// specify that just the keys should be rendered in the error.
		errs = append(errs, field.Duplicate(fldPath.Index(i), val))
	}
	return errs
}

// PtrSliceUnique verifies that each element of a slice of pointers is unique,
// according to the match function. It compares every element of the slice with
// every other element and returns errors for non-unique items.
//
// The match function will never be called with nil arguments.
//
// nil 元素由 PtrSliceNoNils 负责，这里跳过，防止 match 被传入 nil。
func PtrSliceUnique[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, newSlice, _ []*T, match MatchFunc[*T]) field.ErrorList {
	var errs field.ErrorList
	var dups []int
	for i := range newSlice {
		if newSlice[i] == nil {
			// Ignore nil items; they are supposed to have been checked by PtrSliceNoNils.
			continue
		}
		for j := i + 1; j < len(newSlice); j++ {
			if newSlice[j] == nil {
				continue
			}
			if match(newSlice[i], newSlice[j]) {
				if dups == nil {
					dups = make([]int, 0, len(newSlice))
				}
				if !slices.Contains(dups, j) {
					dups = append(dups, j)
				}
			}
		}
	}

	sort.Ints(dups)
	for _, i := range dups {
		var val any = newSlice[i]
		errs = append(errs, field.Duplicate(fldPath.Index(i), val))
	}
	return errs
}

// SemanticDeepEqual is a MatchFunc that uses equality.Semantic.DeepEqual to
// compare two values.
// This wrapper is needed because MatchFunc requires a function that takes two
// arguments of specific type T, while equality.Semantic.DeepEqual takes
// arguments of type interface{}/any. The wrapper satisfies the type
// constraints of MatchFunc while leveraging the underlying semantic equality
// logic. It can be used by any other function that needs to call DeepEqual.
//
// 直接调用 equality.Semantic.DeepEqual 需要 interface{} 参数，
// 泛型函数类型不匹配，因此用这层包装把 *T 转成 any。
func SemanticDeepEqual[T any](a, b T) bool {
	return equality.Semantic.DeepEqual(a, b)
}

// DirectEqual is a MatchFunc that dereferences two pointers and uses the ==
// operator to compare the values. If both pointers are nil, it returns true.
// If one pointer is nil and the other is not, it returns false.
// It can be used by any other function that needs to compare two pointees
// directly.
func DirectEqual[T comparable](a, b *T) bool {
	// 先比较指针本身，既处理双 nil，也快速通过同一对象。
	if a == b {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

// PtrSliceNoNils returns a Required error for each nil element in a slice of
// pointers.
func PtrSliceNoNils[T any](_ context.Context, _ operation.Operation, fldPath *field.Path, newSlice, _ []*T) (errs field.ErrorList) {
	for i := range newSlice {
		if newSlice[i] == nil {
			errs = append(errs, field.Required(fldPath.Index(i), ""))
		}
	}
	return
}
