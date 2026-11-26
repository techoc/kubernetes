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

package typedeftoslice

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Test 验证 minItems 校验先于元素级校验执行，并覆盖 0、边界与超出边界
// 以及 ratcheting 四类场景。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值（nil slice）按长度 0 处理，只有 minItems=10 的字段报错。
	st.Value(&Struct{
		// 全部零值
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooFew(field.NewPath("min10Field"), 0, 10),
		field.TooFew(field.NewPath("min10TypedefField"), 0, 10),
	})

	// 显式 make 的长度 0 与 nil 行为一致；minItems=0 始终合法。
	st.Value(&Struct{
		Min0Field:         make(Min0Type, 0),
		Min10Field:        make(Min10Type, 0),
		Min0TypedefField:  make(Min0TypedefType, 0),
		Min10TypedefField: make(Min10TypedefType, 0),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooFew(field.NewPath("min10Field"), 0, 10),
		field.TooFew(field.NewPath("min10TypedefField"), 0, 10),
	})

	// 长度不足但仍大于 0，错误里带上实际长度。
	st.Value(&Struct{
		Min10Field:        make(Min10Type, 1),
		Min10TypedefField: make(Min10TypedefType, 1),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooFew(field.NewPath("min10Field"), 1, 10),
		field.TooFew(field.NewPath("min10TypedefField"), 1, 10),
	})

	// 差一（9 < 10）。
	st.Value(&Struct{
		Min10Field:        make(Min10Type, 9),
		Min10TypedefField: make(Min10TypedefType, 9),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooFew(field.NewPath("min10Field"), 9, 10),
		field.TooFew(field.NewPath("min10TypedefField"), 9, 10),
	})

	// 恰好达到下界。
	st.Value(&Struct{
		Min10Field:        make(Min10Type, 10),
		Min10TypedefField: make(Min10TypedefType, 10),
	}).ExpectValid()

	testVal := &Struct{
		Min0Field:         make(Min0Type, 1),
		Min10Field:        make(Min10Type, 11),
		Min0TypedefField:  make(Min0TypedefType, 1),
		Min10TypedefField: make(Min10TypedefType, 11),
	}
	st.Value(testVal).ExpectValid()

	// 测试 ratcheting 行为
	//
	// 新旧值相同且都已违规时放行，避免存量对象升级失败。
	st.Value(&Struct{
		Min0Field:         make(Min0Type, 1),
		Min10Field:        make(Min10Type, 1),
		Min0TypedefField:  make(Min0TypedefType, 1),
		Min10TypedefField: make(Min10TypedefType, 1),
	}).OldValue(&Struct{
		Min0Field:         make(Min0Type, 1),
		Min10Field:        make(Min10Type, 1),
		Min0TypedefField:  make(Min0TypedefType, 1),
		Min10TypedefField: make(Min10TypedefType, 1),
	}).ExpectValid()

}
