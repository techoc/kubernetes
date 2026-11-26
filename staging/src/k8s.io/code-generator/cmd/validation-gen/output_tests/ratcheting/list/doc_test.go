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

package list

import (
	"testing"

	"k8s.io/utils/ptr"
)

// Test_StructSlice 验证列表字段的 ratcheting（存量容忍）语义：
// 新值中的非法元素仍报错，但未发生变化的旧值不报错；同时覆盖 atomic、
// set、map 三类列表以及可比较/不可比较/指针键元素的错误路径。
func Test_StructSlice(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 所有列表字段都含非法元素，确认每类列表都产生预期路径的错误。
	invalidStructSlice := &StructSlice{
		AtomicSliceStringField:        []StringType{""},
		AtomicSliceTypeField:          IntSliceType{1},
		AtomicSliceComparableField:    []ComparableStruct{{IntField: 1}},
		AtomicSliceNonComparableField: []NonComparableStruct{{IntPtrField: ptr.To(1)}},
		SetSliceComparableField:       []ComparableStruct{{IntField: 1}},
		SetSliceNonComparableField:    []NonComparableStruct{{IntPtrField: ptr.To(1)}},
		MapSliceComparableField:       []ComparableStructWithKey{{Key: "x", IntField: 1}},
		MapSliceNonComparableField:    []NonComparableStructWithKey{{Key: "x", IntPtrField: ptr.To(1)}},
		MapSlicePtrKeyField:           []PtrKeyStruct{{Key: ptr.To("x"), Data: "y"}},
		MapSliceMixedKeyField:         []MixedKeyStruct{{Key1: ptr.To("x"), Key2: "y", Data: "z"}},
	}
	st.Value(invalidStructSlice).ExpectValidateFalseByPath(map[string][]string{
		"atomicSliceStringField[0]":        {"field AtomicSliceStringField[*]"},
		"atomicSliceTypeField[0]":          {"field AtomicSliceTypeField[*]"},
		"atomicSliceComparableField[0]":    {"field AtomicSliceComparableField[*]"},
		"atomicSliceNonComparableField[0]": {"field AtomicSliceNonComparableField[*]", "type NonComparableStruct"},
		"setSliceComparableField[0]":       {"field SetSliceComparableField[*]"},
		"setSliceNonComparableField[0]":    {"field SetSliceNonComparableField[*]", "type NonComparableStruct"},
		"mapSliceComparableField[0]":       {"field MapSliceComparableField[*]"},
		"mapSliceNonComparableField[0]":    {"field MapSliceNonComparableField[*]", "type NonComparableStructWithKey"},
		"mapSlicePtrKeyField[0]":           {"field MapSlicePtrKeyField[*]", "type PtrKeyStruct"},
		"mapSliceMixedKeyField[0]":         {"field MapSliceMixedKeyField[*]", "type MixedKeyStruct"},
	})

	// 没有变化。
	st.Value(&invalidStructSlice).OldValue(&invalidStructSlice).ExpectValid()

	// 删除元素后，atomic 列表仍然整体校验，故残留非法元素继续报错；
	// set/map 列表则按元素身份比较，删除不产生新错误。
	//  删除元素 — atomic 列表应报错。
	st.Value(&StructSlice{
		AtomicSliceStringField:        []StringType{""},
		AtomicSliceTypeField:          IntSliceType{1},
		AtomicSliceComparableField:    []ComparableStruct{{IntField: 1}},
		AtomicSliceNonComparableField: []NonComparableStruct{{IntPtrField: ptr.To(1)}},
		SetSliceComparableField:       []ComparableStruct{{IntField: 1}},
		SetSliceNonComparableField:    []NonComparableStruct{{IntPtrField: ptr.To(1)}},
		MapSliceComparableField:       []ComparableStructWithKey{{Key: "x", IntField: 1}},
		MapSliceNonComparableField:    []NonComparableStructWithKey{{Key: "x", IntPtrField: ptr.To(1)}},
		MapSlicePtrKeyField:           []PtrKeyStruct{{Key: ptr.To("x"), Data: "y"}},
		MapSliceMixedKeyField:         []MixedKeyStruct{{Key1: ptr.To("x"), Key2: "y", Data: "z"}},
	}).OldValue(&StructSlice{
		AtomicSliceStringField:        []StringType{"", "x"},
		AtomicSliceTypeField:          IntSliceType{1, 2},
		AtomicSliceComparableField:    []ComparableStruct{{IntField: 2}, {IntField: 1}},
		AtomicSliceNonComparableField: []NonComparableStruct{{IntPtrField: ptr.To(2)}, {IntPtrField: ptr.To(1)}},
		SetSliceComparableField:       []ComparableStruct{{IntField: 2}, {IntField: 1}},
		SetSliceNonComparableField:    []NonComparableStruct{{IntPtrField: ptr.To(2)}, {IntPtrField: ptr.To(1)}},
		MapSliceComparableField:       []ComparableStructWithKey{{Key: "y", IntField: 2}, {Key: "x", IntField: 1}},
		MapSliceNonComparableField:    []NonComparableStructWithKey{{Key: "y", IntPtrField: ptr.To(2)}, {Key: "x", IntPtrField: ptr.To(1)}},
		MapSlicePtrKeyField:           []PtrKeyStruct{{Key: ptr.To("a"), Data: "b"}, {Key: ptr.To("x"), Data: "y"}},
		MapSliceMixedKeyField:         []MixedKeyStruct{{Key1: ptr.To("a"), Key2: "b", Data: "c"}, {Key1: ptr.To("x"), Key2: "y", Data: "z"}},
	}).ExpectValidateFalseByPath(map[string][]string{"atomicSliceStringField[0]": {"field AtomicSliceStringField[*]"},
		"atomicSliceTypeField[0]":          {"field AtomicSliceTypeField[*]"},
		"atomicSliceComparableField[0]":    {"field AtomicSliceComparableField[*]"},
		"atomicSliceNonComparableField[0]": {"field AtomicSliceNonComparableField[*]", "type NonComparableStruct"},
	})

	// 数据相同、仅顺序不同：set/map 按身份比较，应视为无变化。
	// 数据相同，顺序不同。
	st.Value(&StructSlice{
		SetSliceComparableField:    []ComparableStruct{{IntField: 2}, {IntField: 1}},
		SetSliceNonComparableField: []NonComparableStruct{{IntPtrField: ptr.To(2)}, {IntPtrField: ptr.To(1)}},
		MapSliceComparableField:    []ComparableStructWithKey{{Key: "y", IntField: 2}, {Key: "x", IntField: 1}},
		MapSliceNonComparableField: []NonComparableStructWithKey{{Key: "y", IntPtrField: ptr.To(2)}, {Key: "x", IntPtrField: ptr.To(1)}},
		MapSlicePtrKeyField:        []PtrKeyStruct{{Key: ptr.To("b"), Data: "2"}, {Key: ptr.To("a"), Data: "1"}},
		MapSliceMixedKeyField:      []MixedKeyStruct{{Key1: ptr.To("b"), Key2: "2", Data: "B"}, {Key1: ptr.To("a"), Key2: "1", Data: "A"}},
	}).OldValue(&StructSlice{
		SetSliceComparableField:    []ComparableStruct{{IntField: 1}, {IntField: 2}},
		SetSliceNonComparableField: []NonComparableStruct{{IntPtrField: ptr.To(1)}, {IntPtrField: ptr.To(2)}},
		MapSliceComparableField:    []ComparableStructWithKey{{Key: "x", IntField: 1}, {Key: "y", IntField: 2}},
		MapSliceNonComparableField: []NonComparableStructWithKey{{Key: "x", IntPtrField: ptr.To(1)}, {Key: "y", IntPtrField: ptr.To(2)}},
		MapSlicePtrKeyField:        []PtrKeyStruct{{Key: ptr.To("a"), Data: "1"}, {Key: ptr.To("b"), Data: "2"}},
		MapSliceMixedKeyField:      []MixedKeyStruct{{Key1: ptr.To("a"), Key2: "1", Data: "A"}, {Key1: ptr.To("b"), Key2: "2", Data: "B"}},
	}).ExpectValid()
}

// Test_Items 验证列表项级校验的 ratcheting：替换某个 key 的元素会触发
// 新元素的校验错误，而旧元素保持合法。
func Test_Items(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&ItemList{
		Items: []Item{
			{Key: "valid2"},
		},
	}).OldValue(&ItemList{
		Items: []Item{
			{Key: "valid1"},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		"items[0].data": {"field Data"},
	})
}
