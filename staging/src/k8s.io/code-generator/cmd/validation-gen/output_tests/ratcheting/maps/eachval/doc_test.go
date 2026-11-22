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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// This is a test package.
package eachval

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test_StructWithMaps 验证 map 值的 ratcheting：同样键在旧值中已报错时，
// 新值仅修改其他键不再重复报错（ExpectValid）。
func Test_StructWithMaps(t *testing.T) {
	mkTest := func() *StructWithMaps {
		return &StructWithMaps{
			MapPrimitiveField: map[string]string{"x": "y"},
			MapTypedefField:   map[string]StringType{"x": "y"},
			MapComparableStructField: map[string]ComparableStruct{
				"x": {IntField: 1},
			},
			MapNonComparableStructField: map[string]NonComparableStruct{
				"x": {IntPtrField: ptr.To(1)},
			},
		}
	}

	// 每种 map 元素类型各触发一次 Invalid：原语、typedef、可比较与不可比较结构体。
	st := localSchemeBuilder.Test(t)
	st.Value(mkTest()).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Invalid(field.NewPath("mapPrimitiveField[x]"), "y", ""),
		field.Invalid(field.NewPath("mapTypedefField[x]"), "y", ""),
		field.Invalid(field.NewPath("mapComparableStructField[x]"), "", ""),
		field.Invalid(field.NewPath("mapNonComparableStructField[x]"), "", ""),
	})
	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()
}
