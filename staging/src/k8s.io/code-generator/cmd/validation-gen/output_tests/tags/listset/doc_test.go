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

package listset

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 listType=set 的唯一性约束：重复元素应在首次重复处报 Duplicate。
// 覆盖可比较类型、不可比较类型，以及“看似可比较但实际按指针比较”的陷阱。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 空结构体合法。
	st.Value(&Struct{}).ExpectValid()

	// 无重复时全部合法；不可比较类型应走语义深比较。
	st.Value(&Struct{
		SliceStringField:     []string{"aaa", "bbb"},
		SliceIntField:        []int{1, 2},
		SliceComparableField: []ComparableStruct{{"aaa"}, {"bbb"}},
		SliceNonComparableField: []NonComparableStruct{
			{[]string{"aaa", "bbb"}},
			{[]string{"bbb", "aaa"}},
		},
	}).ExpectValid()

	// 两个指针值相同但地址不同：直接 == 比较会误判为不同，
	// 因此需要按值比较，才能识别为重复。
	ptrS1 := ptr.To("same value")
	ptrS2 := ptr.To("same value")
	st.Value(&Struct{
		SliceStringField:     []string{"aaa", "bbb", "ccc", "ccc", "bbb", "aaa"},
		SliceIntField:        []int{1, 2, 3, 3, 2, 1},
		SliceComparableField: []ComparableStruct{{"aaa"}, {"bbb"}, {"ccc"}, {"ccc"}, {"bbb"}, {"aaa"}},
		SliceNonComparableField: []NonComparableStruct{
			{[]string{"aaa", "111"}},
			{[]string{"bbb", "222"}},
			{[]string{"ccc", "333"}},
			{[]string{"ccc", "333"}},
			{[]string{"bbb", "222"}},
			{[]string{"aaa", "111"}},
		},
		SliceFalselyComparableField: []FalselyComparableStruct{
			{StringPtrField: ptrS1},
			{StringPtrField: ptrS2},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		// 索引 3 起为重复元素。
		field.Duplicate(field.NewPath("sliceStringField").Index(3), nil),
		field.Duplicate(field.NewPath("sliceStringField").Index(4), nil),
		field.Duplicate(field.NewPath("sliceStringField").Index(5), nil),
		field.Duplicate(field.NewPath("sliceIntField").Index(3), nil),
		field.Duplicate(field.NewPath("sliceIntField").Index(4), nil),
		field.Duplicate(field.NewPath("sliceIntField").Index(5), nil),
		field.Duplicate(field.NewPath("sliceComparableField").Index(3), nil),
		field.Duplicate(field.NewPath("sliceComparableField").Index(4), nil),
		field.Duplicate(field.NewPath("sliceComparableField").Index(5), nil),
		field.Duplicate(field.NewPath("sliceNonComparableField").Index(3), nil),
		field.Duplicate(field.NewPath("sliceNonComparableField").Index(4), nil),
		field.Duplicate(field.NewPath("sliceNonComparableField").Index(5), nil),
		// 指针字段按值比较，索引 1 与 0 值相同。
		field.Duplicate(field.NewPath("sliceFalselyComparableField").Index(1), nil),
	})
}

// TestSetCorrelation 验证 set 语义下新旧值的关联：元素顺序变化不算修改，
// 因此不可变约束不会误报。
func TestSetCorrelation(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	structNew := ImmutableStruct{SliceSetComparableField: []ComparableStruct{{"aaa"}, {"bbb"}}}
	structOld := ImmutableStruct{SliceSetComparableField: []ComparableStruct{{"bbb"}, {"aaa"}}}
	st.Value(&structNew).OldValue(&structOld).ExpectValid()

	structNew = ImmutableStruct{SliceSetNonComparableField: []NonComparableStruct{{[]string{"aaa"}}, {[]string{"bbb"}}}}
	structOld = ImmutableStruct{SliceSetNonComparableField: []NonComparableStruct{{[]string{"bbb"}}, {[]string{"aaa"}}}}
	st.Value(&structNew).OldValue(&structOld).ExpectValid()

	structNew = ImmutableStruct{SliceSetPrimitiveField: []int{1, 2}}
	structOld = ImmutableStruct{SliceSetPrimitiveField: []int{2, 1}}
	st.Value(&structNew).OldValue(&structOld).ExpectValid()

	// 指针值相同但地址不同，按语义比较仍视为未变化。
	structNew = ImmutableStruct{SliceSetFalselyComparableField: []FalselyComparableStruct{{StringPtrField: ptr.To("same value")}}}
	structOld = ImmutableStruct{SliceSetFalselyComparableField: []FalselyComparableStruct{{StringPtrField: ptr.To("same value")}}}
	st.Value(&structNew).OldValue(&structOld).ExpectValid()
}
