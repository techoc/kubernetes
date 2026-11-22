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

package opaque

import (
	"testing"
)

// Test 覆盖 opaque 标签的核心行为：标记为 opaque 的结构体只校验自身，
// 不再下钻到内部字段；opaque 的 slice/map/typedef 变体同理。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 第一组对比：同样形态下 opaque 版本缺少内层字段/类型校验，
	// 只保留字段自身的校验（如 "field Struct.OpaqueStructField"）。
	st.Value(&Struct{
		StructField:                    OtherStruct{},
		StructPtrField:                 &OtherStruct{},
		OpaqueStructField:              OtherStruct{},
		OpaqueStructPtrField:           &OtherStruct{},
		SliceOfStructField:             []OtherStruct{{}, {}},
		SliceOfOpaqueStructField:       []OtherStruct{{}, {}},
		ListMapOfStructField:           []OtherStruct{{"foo"}, {"bar"}},
		ListMapOfOpaqueStructField:     []OtherStruct{{"foo"}, {"bar"}},
		MapOfStringToStructField:       map[OtherString]OtherStruct{"a": {"foo"}, "b": {"bar"}},
		MapOfStringToOpaqueStructField: map[OtherString]OtherStruct{"a": {"foo"}, "b": {"bar"}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"structField":                         {"field Struct.StructField", "type OtherStruct"},
		"structPtrField":                      {"field Struct.StructPtrField", "type OtherStruct"},
		"structField.stringField":             {"field OtherStruct.StringField"},
		"structPtrField.stringField":          {"field OtherStruct.StringField"},
		"opaqueStructField":                   {"field Struct.OpaqueStructField"},
		"opaqueStructPtrField":                {"field Struct.OpaqueStructPtrField"},
		"sliceOfStructField":                  {"field Struct.SliceOfStructField"},
		"sliceOfStructField[0]":               {"field Struct.SliceOfStructField vals", "type OtherStruct"},
		"sliceOfStructField[0].stringField":   {"field OtherStruct.StringField"},
		"sliceOfStructField[1]":               {"field Struct.SliceOfStructField vals", "type OtherStruct"},
		"sliceOfStructField[1].stringField":   {"field OtherStruct.StringField"},
		"sliceOfOpaqueStructField":            {"field Struct.SliceOfOpaqueStructField"},
		"sliceOfOpaqueStructField[0]":         {"field Struct.SliceOfOpaqueStructField vals"},
		"sliceOfOpaqueStructField[1]":         {"field Struct.SliceOfOpaqueStructField vals"},
		"listMapOfStructField":                {"field Struct.ListMapOfStructField"},
		"listMapOfStructField[0]":             {"field Struct.ListMapOfStructField vals", "type OtherStruct"},
		"listMapOfStructField[0].stringField": {"field OtherStruct.StringField"},
		"listMapOfStructField[1]":             {"field Struct.ListMapOfStructField vals", "type OtherStruct"},
		"listMapOfStructField[1].stringField": {"field OtherStruct.StringField"},
		"listMapOfOpaqueStructField":          {"field Struct.ListMapOfOpaqueStructField"},
		"listMapOfOpaqueStructField[0]":       {"field Struct.ListMapOfOpaqueStructField vals"},
		"listMapOfOpaqueStructField[1]":       {"field Struct.ListMapOfOpaqueStructField vals"},
		"mapOfStringToStructField": {
			"field Struct.MapOfStringToStructField",
			"field Struct.MapOfStringToStructField keys",
			"field Struct.MapOfStringToStructField keys",
			"type OtherString",
			"type OtherString",
		},
		"mapOfStringToStructField[a]":             {"field Struct.MapOfStringToStructField vals", "type OtherStruct"},
		"mapOfStringToStructField[a].stringField": {"field OtherStruct.StringField"},
		"mapOfStringToStructField[b]":             {"field Struct.MapOfStringToStructField vals", "type OtherStruct"},
		"mapOfStringToStructField[b].stringField": {"field OtherStruct.StringField"},
		"mapOfStringToOpaqueStructField": {
			"field Struct.MapOfStringToOpaqueStructField",
			"field Struct.MapOfStringToOpaqueStructField keys",
			"field Struct.MapOfStringToOpaqueStructField keys",
		},
		"mapOfStringToOpaqueStructField[a]": {"field Struct.MapOfStringToOpaqueStructField vals"},
		"mapOfStringToOpaqueStructField[b]": {"field Struct.MapOfStringToOpaqueStructField vals"},
	})

	// 只填 listMap 字段：验证 opaque 集合仍会校验元素自身，但不会进入元素内部。
	st.Value(&Struct{
		ListMapOfStructField:       []OtherStruct{{"foo"}, {"foo"}},
		ListMapOfOpaqueStructField: []OtherStruct{{"foo"}, {"foo"}},
	}).ExpectValidateFalseByPath(map[string][]string{
		"structField":                         {"field Struct.StructField", "type OtherStruct"},
		"structField.stringField":             {"field OtherStruct.StringField"},
		"opaqueStructField":                   {"field Struct.OpaqueStructField"},
		"listMapOfStructField":                {"field Struct.ListMapOfStructField"},
		"listMapOfStructField[0]":             {"field Struct.ListMapOfStructField vals", "type OtherStruct"},
		"listMapOfStructField[0].stringField": {"field OtherStruct.StringField"},
		"listMapOfStructField[1]":             {"field Struct.ListMapOfStructField vals", "type OtherStruct"},
		"listMapOfStructField[1].stringField": {"field OtherStruct.StringField"},
		"listMapOfOpaqueStructField":          {"field Struct.ListMapOfOpaqueStructField"},
		"listMapOfOpaqueStructField[0]":       {"field Struct.ListMapOfOpaqueStructField vals"},
		"listMapOfOpaqueStructField[1]":       {"field Struct.ListMapOfOpaqueStructField vals"},
		"mapOfStringToOpaqueStructField":      {"field Struct.MapOfStringToOpaqueStructField"},
		"mapOfStringToStructField":            {"field Struct.MapOfStringToStructField"},
		"sliceOfOpaqueStructField":            {"field Struct.SliceOfOpaqueStructField"},
		"sliceOfStructField":                  {"field Struct.SliceOfStructField"},
	})

	// 顶层 typedef 指针：错误路径为空，只保留类型级校验。
	str := OtherString("foo")
	st.Value(&str).ExpectValidateFalseByPath(map[string][]string{
		"": {"type OtherString"},
	})

	// 顶层结构体指针：类型级 + 字段级校验都执行。
	st.Value(&OtherStruct{}).ExpectValidateFalseByPath(map[string][]string{
		"":            {"type OtherStruct"},
		"stringField": {"field OtherStruct.StringField"},
	})

	// opaque 字段（内联、slice、map 及 typedef 变体）全部应通过，
	// 因为 opaque 会屏蔽内层校验，而这些类型本身没有字段级校验。
	st.Value(&OpaqueFieldsStruct{
		OtherStruct:               OtherStruct{"foo"},
		OpaqueSliceField:          []OtherStruct{{"foo"}},
		OpaqueMapField:            map[OtherString]OtherStruct{"a": {"foo"}},
		TypedefOpaqueStructField:  TypedefOpaqueStruct{"foo"},
		TypedefOpaqueSliceField:   []OtherStruct{{"foo"}},
		TypedefOpaqueMapField:     map[OtherString]OtherStruct{"a": {"foo"}},
		IsolatedOpaqueStructField: OtherStruct{"foo"},
	}).ExpectValid()

	// 无校验类型配合 opaque，同样保持通过。
	st.Value(&OpaqueNoValidationFieldsStruct{
		NoValidationStruct:        NoValidationStruct{"foo"},
		OpaqueSliceField:          []NoValidationStruct{{"foo"}},
		OpaqueMapField:            map[NoValidationString]NoValidationStruct{"a": {"foo"}},
		IsolatedOpaqueStructField: NoValidationStruct{"foo"},
	}).ExpectValid()

	// 类型自身有校验时，opaque 仍保留该类型的类型级校验。
	st.Value(&OpaqueStructWithValidation{
		StringField: "foo",
	}).ExpectValidateFalseByPath(map[string][]string{
		"": {"type OpaqueStructWithValidation"},
	})

	// alias 上的 opaque 属性可通过字段类型传播，但字段路径仍指向使用处。
	st.Value(&ParentWithOpaqueAliasWithValidation{
		Field: OpaqueAliasWithValidation{
			Field: "foo",
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		"field": {"type OpaqueAliasWithValidation"},
	})

	// 直接使用 alias 时，校验落在根路径。
	st.Value(&OpaqueAliasWithValidation{
		Field: "foo",
	}).ExpectValidateFalseByPath(map[string][]string{
		"": {"type OpaqueAliasWithValidation"},
	})

	// 非 opaque 的普通结构体继续执行字段级校验。
	st.Value(&BaseStruct{
		Field: "foo",
	}).ExpectValidateFalseByPath(map[string][]string{
		"field": {"field BaseStruct.Field"},
	})
}
