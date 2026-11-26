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

package primitivepointers

import (
	"testing"

	field "k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 immutable 校验比较的是指针指向的值而非指针地址：
// 两个内容相同但地址不同的指针应视为未修改。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	structA1 := Struct{
		SP: ptr.To("zero"),
		IP: ptr.To(0),
		BP: ptr.To(false),
		FP: ptr.To(0.0),
	}

	// 数据相同，指针不同
	//
	// 深比较避免误报：immutable 语义针对值，不针对内存地址。
	structA2 := Struct{
		SP: ptr.To("zero"),
		IP: ptr.To(0),
		BP: ptr.To(false),
		FP: ptr.To(0.0),
	}
	// 数据不同。
	structB := Struct{
		SP: ptr.To("one"),
		IP: ptr.To(1),
		BP: ptr.To(true),
		FP: ptr.To(1.1),
	}

	st.Value(&structA1).OldValue(&structA1).ExpectValid()

	st.Value(&structA1).OldValue(&structA2).ExpectValid()

	// 四个字段都变化时各自独立报 immutable 错误。
	st.Value(&structA1).OldValue(&structB).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByDetailSubstring().ByOrigin(), field.ErrorList{
		field.Invalid(field.NewPath("sp"), nil, "").WithOrigin("immutable"),
		field.Invalid(field.NewPath("ip"), nil, "").WithOrigin("immutable"),
		field.Invalid(field.NewPath("bp"), nil, "").WithOrigin("immutable"),
		field.Invalid(field.NewPath("fp"), nil, "").WithOrigin("immutable"),
	})
}
