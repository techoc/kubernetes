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

// Test 验证 +k8s:maxItems 作用于切片 typedef 时，边界值（0/1/9/10/11）
// 与未校验字段的差异，并确认 ratcheting 对未变化的超限旧值放行。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 全零值：未校验字段与上限字段都不应报错。
	st.Value(&Struct{
		// 全部零值
	}).ExpectValid()

	// 空切片（显式 make）同样合法。
	st.Value(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 0),
		Max0Field:         make(Max0Type, 0),
		Max10Field:        make(Max10Type, 0),
		Max0TypedefField:  make(Max0TypedefType, 0),
		Max10TypedefField: make(Max10TypedefType, 0),
	}).ExpectValid()

	st.Value(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 1),
		Max10Field:        make(Max10Type, 1),
		Max10TypedefField: make(Max10TypedefType, 1),
	}).ExpectValid()

	st.Value(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 9),
		Max10Field:        make(Max10Type, 9),
		Max10TypedefField: make(Max10TypedefType, 9),
	}).ExpectValid()

	st.Value(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 10),
		Max10Field:        make(Max10Type, 10),
		Max10TypedefField: make(Max10TypedefType, 10),
	}).ExpectValid()

	// 超过上限：每个受限字段都报 TooMany，未校验字段不报错。
	testVal := &Struct{
		UnvalidatedField:  make(UnvalidatedType, 11),
		Max0Field:         make(Max0Type, 1),
		Max10Field:        make(Max10Type, 11),
		Max0TypedefField:  make(Max0TypedefType, 1),
		Max10TypedefField: make(Max10TypedefType, 11),
	}
	st.Value(testVal).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooMany(field.NewPath("max0Field"), 1, 0),
		field.TooMany(field.NewPath("max10Field"), 11, 10),
		field.TooMany(field.NewPath("max0TypedefField"), 1, 0),
		field.TooMany(field.NewPath("max10TypedefField"), 11, 10),
	})
	// 测试 ratcheting 行为
	// 新旧值都超限且未变化，不应重新报错。
	st.Value(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 1),
		Max0Field:         make(Max0Type, 1),
		Max10Field:        make(Max10Type, 11),
		Max0TypedefField:  make(Max0TypedefType, 1),
		Max10TypedefField: make(Max10TypedefType, 11),
	}).OldValue(&Struct{
		UnvalidatedField:  make(UnvalidatedType, 1),
		Max0Field:         make(Max0Type, 1),
		Max10Field:        make(Max10Type, 11),
		Max0TypedefField:  make(Max0TypedefType, 1),
		Max10TypedefField: make(Max10TypedefType, 11),
	}).ExpectValid()
}
