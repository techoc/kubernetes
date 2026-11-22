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
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/api/operation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// multiKeyItem 是带两个"键字段"的测试元素，用来模拟 listType=map/listKey 的场景：
// K1/K2 共同构成唯一标识，V 是会被校验的普通值。
type multiKeyItem struct {
	K1 string `json:"k1"`
	K2 string `json:"k2"`
	V  int    `json:"v"`
}

// TestValSliceItem 验证 ValSliceItem 只对 match 选中的元素调用 validator，
// 且能正确区分新增、修改与未匹配的情况。
func TestValSliceItem(t *testing.T) {
	testCases := []struct {
		name      string
		new       []multiKeyItem
		old       []multiKeyItem
		match     MatchItemFunc[*multiKeyItem]
		validator func(context.Context, operation.Operation, *field.Path, *multiKeyItem, *multiKeyItem) field.ErrorList
		expected  field.ErrorList
	}{
		{
			// match 完全不命中，validator 不应被调用。
			name: "no match",
			new: []multiKeyItem{
				{K1: "a", K2: "1", V: 1},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "target"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, _, _ *multiKeyItem) field.ErrorList {
				return field.ErrorList{field.Invalid(fp, nil, "err")}
			},
			expected: nil,
		},
		{
			// 命中新增元素：旧值不存在（o == nil），索引应指向新切片中的位置。
			name: "new item with matching keys",
			new: []multiKeyItem{
				{K1: "a", K2: "1", V: 1},
				{K1: "target", K2: "target2", V: 2},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "target" && i.K2 == "target2"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, o *multiKeyItem) field.ErrorList {
				if n != nil && o == nil {
					return field.ErrorList{field.Invalid(fp, n.K1, "added")}
				}
				return nil
			},
			expected: field.ErrorList{field.Invalid(field.NewPath("").Index(1), "target", "added")},
		},
		{
			// 命中已存在元素，新旧值都会被传入，便于实现不可变字段等校验。
			name: "updated item - same keys different values",
			new: []multiKeyItem{
				{K1: "a", K2: "1", V: 1},
				{K1: "update", K2: "target2", V: 20},
			},
			old: []multiKeyItem{
				{K1: "a", K2: "1", V: 1},
				{K1: "update", K2: "target2", V: 2},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "update" && i.K2 == "target2"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, o *multiKeyItem) field.ErrorList {
				if n != nil && o != nil && n.V != o.V {
					return field.ErrorList{field.Invalid(fp.Child("v"), n.V, "changed")}
				}
				return nil
			},
			expected: field.ErrorList{field.Invalid(field.NewPath("").Index(1).Child("v"), 20, "changed")},
		},
		{
			// For completeness as listType=map && listKey=... required tags prevents dupes.
			// 重复键在正常 API 中会被 listKey 唯一性校验拦截，这里验证即使出现也只处理首个命中。
			name: "first match only - multiple items with same keys",
			new: []multiKeyItem{
				{K1: "dup", K2: "target2", V: 1},
				{K1: "dup", K2: "target2", V: 2},
			},
			old: []multiKeyItem{
				{K1: "dup", K2: "target2", V: 10},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "dup" && i.K2 == "target2"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, o *multiKeyItem) field.ErrorList {
				if n != nil && o != nil {
					return field.ErrorList{field.Invalid(fp, n.V, "value")}
				}
				return nil
			},
			expected: field.ErrorList{field.Invalid(field.NewPath("").Index(0), 1, "value")},
		},
		{
			// 空新列表意味着元素被删除；ValSliceItem 不负责删除语义，因此 validator 不会被调用。
			name: "nil new list",
			new:  nil,
			old: []multiKeyItem{
				{K1: "exists", K2: "target2", V: 1},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "exists" && i.K2 == "target2"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, o *multiKeyItem) field.ErrorList {
				if n == nil && o != nil {
					return field.ErrorList{field.Invalid(fp, nil, "deleted")}
				}
				return nil
			},
			expected: nil,
		},
		{
			name:  "empty lists",
			new:   []multiKeyItem{},
			old:   []multiKeyItem{},
			match: func(i *multiKeyItem) bool { return true },
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, _, _ *multiKeyItem) field.ErrorList {
				return field.ErrorList{field.Invalid(fp, nil, "err")}
			},
			expected: nil,
		},
		{
			name:  "nil lists",
			new:   nil,
			old:   nil,
			match: func(i *multiKeyItem) bool { return true },
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, _, _ *multiKeyItem) field.ErrorList {
				return field.ErrorList{field.Invalid(fp, nil, "err")}
			},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// 统一用 Update，覆盖新旧值配对的路径。
			op := operation.Operation{Type: operation.Update}
			fp := field.NewPath("")

			got := ValSliceItem(ctx, op, fp, tc.new, tc.old, tc.match, SemanticDeepEqual, tc.validator)

			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}

// TestPtrSliceItem 是 ValSliceItem 的指针版本：额外验证 nil 元素被跳过，
// 避免把 nil 传给 match/validator。
func TestPtrSliceItem(t *testing.T) {
	testCases := []struct {
		name      string
		new       []*multiKeyItem
		old       []*multiKeyItem
		match     MatchItemFunc[*multiKeyItem]
		validator func(context.Context, operation.Operation, *field.Path, *multiKeyItem, *multiKeyItem) field.ErrorList
		expected  field.ErrorList
	}{
		{
			name: "no match",
			new: []*multiKeyItem{
				{K1: "a", K2: "1", V: 1},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "target"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, _, _ *multiKeyItem) field.ErrorList {
				return field.ErrorList{field.Invalid(fp, nil, "err")}
			},
			expected: nil,
		},
		{
			name: "new item with matching keys",
			new: []*multiKeyItem{
				{K1: "a", K2: "1", V: 1},
				{K1: "target", K2: "target2", V: 2},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "target" && i.K2 == "target2"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, o *multiKeyItem) field.ErrorList {
				if n != nil && o == nil {
					return field.ErrorList{field.Invalid(fp, n.K1, "added")}
				}
				return nil
			},
			expected: field.ErrorList{field.Invalid(field.NewPath("").Index(1), "target", "added")},
		},
		{
			// nil 元素被忽略，但后续真实元素的索引仍按原位置计算。
			name: "nil element in new list (ignored)",
			new: []*multiKeyItem{
				nil,
				{K1: "target", V: 1},
			},
			match: func(i *multiKeyItem) bool {
				return i.K1 == "target"
			},
			validator: func(_ context.Context, _ operation.Operation, fp *field.Path, n, _ *multiKeyItem) field.ErrorList {
				return field.ErrorList{field.Invalid(fp, n.K1, "matched")}
			},
			expected: field.ErrorList{field.Invalid(field.NewPath("").Index(1), "target", "matched")},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			op := operation.Operation{Type: operation.Update}
			fp := field.NewPath("")

			got := PtrSliceItem(ctx, op, fp, tc.new, tc.old, tc.match, SemanticDeepEqual, tc.validator)

			if !reflect.DeepEqual(got, tc.expected) {
				t.Errorf("got %v want %v", got, tc.expected)
			}
		})
	}
}
