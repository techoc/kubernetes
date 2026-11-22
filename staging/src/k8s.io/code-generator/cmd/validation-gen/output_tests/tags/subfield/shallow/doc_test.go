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

package shallow

import (
	"testing"

	"k8s.io/utils/ptr"
)

// Test 验证 shallow subfield 校验只作用于字段的直接子字段，
// 不会递归进入更深层结构；同一子字段上的多条 subfield 校验会全部保留。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// StructField 与 StructPtrField 应产生完全对称的错误集合，
	// 指针只是访问路径不同，不应影响子字段校验结果。
	st.Value(&Struct{
		StructField: OtherStruct{
			StringField:  "",
			PointerField: ptr.To(""),
			StructField:  SmallStruct{},
			SliceField:   []string{},
			MapField:     map[string]string{},
		},
		StructPtrField: &OtherStruct{
			StringField:  "",
			PointerField: ptr.To(""),
			StructField:  SmallStruct{},
			SliceField:   []string{},
			MapField:     map[string]string{},
		},
	}).ExpectValidateFalseByPath(map[string][]string{
		"structField.stringField":     {"subfield Struct.StructField.StringField 1", "subfield Struct.StructField.StringField 2"},
		"structField.pointerField":    {"subfield Struct.StructField.PointerField"},
		"structField.structField":     {"subfield Struct.StructField.StructField"},
		"structField.sliceField":      {"subfield Struct.StructField.SliceField"},
		"structField.mapField":        {"subfield Struct.StructField.MapField"},
		"structPtrField.stringField":  {"subfield Struct.StructPtrField.StringField 1", "subfield Struct.StructPtrField.StringField 2"},
		"structPtrField.pointerField": {"subfield Struct.StructPtrField.PointerField"},
		"structPtrField.structField":  {"subfield Struct.StructPtrField.StructField"},
		"structPtrField.sliceField":   {"subfield Struct.StructPtrField.SliceField"},
		"structPtrField.mapField":     {"subfield Struct.StructPtrField.MapField"},
	})
}
