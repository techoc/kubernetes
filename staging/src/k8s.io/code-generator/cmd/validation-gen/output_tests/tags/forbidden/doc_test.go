/*
Copyright The Kubernetes Authors.

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

package forbidden

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 +k8s:forbidden：零值、nil 与空切片/map 视为“未指定”而合法；
// 任何非零值都应报 Forbidden 错误。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 全零值（nil 切片/map）合法。
	st.Value(&Struct{
		// 全部为零值（nil slice/map）。
	}).ExpectValid()

	// 显式零值与空切片/map 同样合法。
	st.Value(&Struct{
		// 显式零值与空 slice/map。
		StringField:           "",
		StringPtrField:        nil,
		StringTypedefField:    "",
		StringTypedefPtrField: nil,
		IntField:              0,
		IntPtrField:           nil,
		IntTypedefField:       0,
		IntTypedefPtrField:    nil,
		BoolField:             false,
		FloatField:            0.0,
		ByteField:             0,
		OtherStructPtrField:   nil,
		SliceField:            []string{},
		SliceTypedefField:     SliceType{},
		ByteArrayField:        []byte{},
		MapField:              map[string]string{},
		MapTypedefField:       MapType{},
	}).ExpectValid()

	// 所有字段都设置为非零值，每个字段都应报告 Forbidden。
	st.Value(&Struct{
		StringField:           "abc",
		StringPtrField:        ptr.To("xyz"),
		StringTypedefField:    StringType("abc"),
		StringTypedefPtrField: ptr.To(StringType("xyz")),
		IntField:              123,
		IntPtrField:           ptr.To(456),
		IntTypedefField:       IntType(123),
		IntTypedefPtrField:    ptr.To(IntType(456)),
		BoolField:             true,
		FloatField:            1.23,
		ByteField:             'a',
		OtherStructPtrField:   &OtherStruct{},
		SliceField:            []string{"a", "b"},
		SliceTypedefField:     SliceType([]string{"a", "b"}),
		ByteArrayField:        []byte("abc"),
		MapField:              map[string]string{"a": "b", "c": "d"},
		MapTypedefField:       MapType(map[string]string{"a": "b", "c": "d"}),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("stringField"), ""),
		field.Forbidden(field.NewPath("stringPtrField"), ""),
		field.Forbidden(field.NewPath("stringTypedefField"), ""),
		field.Forbidden(field.NewPath("stringTypedefPtrField"), ""),
		field.Forbidden(field.NewPath("intField"), ""),
		field.Forbidden(field.NewPath("intPtrField"), ""),
		field.Forbidden(field.NewPath("intTypedefField"), ""),
		field.Forbidden(field.NewPath("intTypedefPtrField"), ""),
		field.Forbidden(field.NewPath("boolField"), ""),
		field.Forbidden(field.NewPath("floatField"), ""),
		field.Forbidden(field.NewPath("byteField"), ""),
		field.Forbidden(field.NewPath("otherStructPtrField"), ""),
		field.Forbidden(field.NewPath("sliceField"), ""),
		field.Forbidden(field.NewPath("sliceTypedefField"), ""),
		field.Forbidden(field.NewPath("byteArrayField"), ""),
		field.Forbidden(field.NewPath("mapField"), ""),
		field.Forbidden(field.NewPath("mapTypedefField"), ""),
	})
}
