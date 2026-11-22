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

package defaultbehavior

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test_StructPrimitive 验证默认 ratcheting 行为：创建（无 OldValue）时报错，
// 更新时值未变化则不报错。IntPtrField 每次都是新指针但值相同，用来确认比较按值而非按地址。
func Test_StructPrimitive(t *testing.T) {
	mkTest := func() *StructPrimitive {
		return &StructPrimitive{
			IntField:    1,
			IntPtrField: ptr.To(1), // Different pointers each call, but same value.
		}
	}

	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Invalid(field.NewPath("intField"), "", ""),
		field.Invalid(field.NewPath("intPtrField"), "", ""),
	})
	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}

// Test_StructSlice 覆盖切片字段与 typedef 切片的错误路径归属：
// 字段级错误落在 sliceField，元素级错误落在 sliceField[0]；
// typedef 切片同时继承字段与类型两层的校验。
func Test_StructSlice(t *testing.T) {
	mkTest := func() *StructSlice {
		return &StructSlice{
			SliceField:        []S{""},
			TypeDefSliceField: MySlice{1},
		}
	}

	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectValidateFalseByPath(map[string][]string{
		"sliceField":        {"field sliceField"},
		"sliceField[0]":     {"type S"},
		"typedefSliceField": {"field typedefSliceField", "type MySlice"},
	})

	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}

// Test_StructMap 覆盖 map key/value 的 typedef 校验以及 key 路径（[k]）的渲染。
func Test_StructMap(t *testing.T) {
	mkTest := func() *StructMap {
		return &StructMap{
			MapKeyField:            map[S]string{S("k"): "v"},
			MapValueField:          map[string]S{"k": "v"},
			AliasMapKeyTypeField:   AliasMapKeyType{"k": "v"},
			AliasMapValueTypeField: AliasMapValueType{"k": "v"},
		}
	}

	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectValidateFalseByPath(map[string][]string{
		"aliasMapKeyTypeField":      {"field aliasMapKeyTypeField", "type MapKeyType", "type S"},
		"aliasMapValueTypeField":    {"field aliasMapValueTypeField", "type MapValueType"},
		"aliasMapValueTypeField[k]": {"type S"},
		"mapKeyField":               {"field mapKeyField", "type S"},
		"mapValueField":             {"field mapValueField"},
		"mapValueField[k]":          {"type S"},
	})

	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}

// Test_StructStruct 对比可直接比较与非直接可比较结构体：
// 后者含指针字段，ratcheting 走反射/语义比较，但错误路径必须一致。
func Test_StructStruct(t *testing.T) {
	mkTest := func() *StructStruct {
		return &StructStruct{
			DirectComparableStructField: DirectComparableStruct{
				IntField: 1,
			},
			NonDirectComparableStructField: NonDirectComparableStruct{
				IntPtrField: ptr.To(1),
			},
			DirectComparableStructPtr: &DirectComparableStruct{
				IntField: 1,
			},
			NonDirectComparableStructPtr: &NonDirectComparableStruct{
				IntPtrField: ptr.To(1),
			},
		}
	}

	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectValidateFalseByPath(map[string][]string{
		"directComparableStructField":                   {"field directComparableStructField", "type DirectComparableStruct"},
		"directComparableStructField.intField":          {"field intField"},
		"nonDirectComparableStructField":                {"field nonDirectComparableStructField", "type NonDirectComparableStruct"},
		"nonDirectComparableStructField.intPtrField":    {"field intPtrField"},
		"directComparableStructPtrField":                {"field directComparableStructPtrField", "type DirectComparableStruct"},
		"directComparableStructPtrField.intField":       {"field intField"},
		"nonDirectComparableStructPtrField":             {"field nonDirectComparableStructPtrField", "type NonDirectComparableStruct"},
		"nonDirectComparableStructPtrField.intPtrField": {"field intPtrField"},
		"DirectComparableStruct":                        {"field DirectComparableStruct", "type DirectComparableStruct"},
		"DirectComparableStruct.intField":               {"field intField"},
		"NonDirectComparableStruct":                     {"field NonDirectComparableStruct", "type NonDirectComparableStruct"},
		"NonDirectComparableStruct.intPtrField":         {"field intPtrField"},
	})

	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}

// Test_StructEmbedded 验证内嵌结构体的字段提升：错误路径不含内嵌层名，
// 直接以提升后的字段路径呈现，但嵌套结构体字段仍保留完整路径。
func Test_StructEmbedded(t *testing.T) {
	mkTest := func() *StructEmbedded {
		return &StructEmbedded{
			DirectComparableStruct: DirectComparableStruct{
				IntField: 1,
			},
			NonDirectComparableStruct: NonDirectComparableStruct{
				IntPtrField: ptr.To(1),
			},
			NestedDirectComparableStructField: NestedDirectComparableStruct{
				DirectComparableStructField: DirectComparableStruct{
					IntField: 1,
				},
			},
			NestedNonDirectComparableStructField: NestedNonDirectComparableStruct{
				NonDirectComparableStructField: NonDirectComparableStruct{
					IntPtrField: ptr.To(1),
				},
			},
		}
	}

	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectValidateFalseByPath(map[string][]string{
		"directComparableStruct": {
			"field DirectComparableStruct", "type DirectComparableStruct",
		},
		"directComparableStruct.intField": {
			"field intField",
		},
		"nonDirectComparableStruct": {
			"field NonDirectComparableStruct", "type NonDirectComparableStruct",
		},
		"nonDirectComparableStruct.intPtrField": {
			"field intPtrField",
		},
		"nestedDirectComparableStructField": {
			"field NestedDirectComparableStructField", "type NestedDirectComparableStruct",
		},
		"nestedDirectComparableStructField.directComparableStructField": {
			"field directComparableStructField", "type DirectComparableStruct",
		},
		"nestedDirectComparableStructField.directComparableStructField.intField": {
			"field intField",
		},
		"nestedNonDirectComparableStructField": {
			"field NestedNonDirectComparableStructField", "type NestedNonDirectComparableStruct",
		},
		"nestedNonDirectComparableStructField.nonDirectComparableStructField": {
			"field nonDirectComparableStructField", "type NonDirectComparableStruct",
		},
		"nestedNonDirectComparableStructField.nonDirectComparableStructField.intPtrField": {
			"field intPtrField",
		},
	})

	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}

// Test_Mix 验证同类型内混合可比较/不可比较字段时，OldValue(nil) 仍按创建语义报告错误。
func Test_Mix(t *testing.T) {
	st := localSchemeBuilder.Test(t)
	st.Value(&MixComparableStruct{
		Primitive: "a",
	}).OldValue(nil).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Invalid(field.NewPath("NonComparable"), "", ""),
	})
}
