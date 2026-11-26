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

package minlength

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// Test 验证 minLength 对值、指针、typedef（默认无校验与类型自带校验）
// 以及 string 别名的生成差异，并覆盖长度边界和 ratcheting。
func Test(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 零值字符串长度为 0；所有字段都是 optional 或 minLength 允许 0，
	// 因此空对象合法。
	st.Value(&Struct{
		// 全部零值
	}).ExpectValid()

	// 所有带 minLength 的字段都只放 1 个字符，低于阈值。
	// min2 与 min10 两组分别验证不同阈值，typedef 用来确认校验来自字段
	// 标签还是类型标签。
	st.Value(&Struct{
		Min2Field:                          strings.Repeat("x", 1),
		Min2PtrField:                       ptr.To(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefField:        UnvalidatedStringType(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefPtrField:     ptr.To(UnvalidatedStringType(strings.Repeat("x", 1))),
		Min2ValidatedTypedefField:          Min2Type(strings.Repeat("x", 1)),
		Min2ValidatedTypedefPtrField:       ptr.To(Min2Type(strings.Repeat("x", 1))),
		Min10Field:                         strings.Repeat("x", 1),
		Min10PtrField:                      ptr.To(strings.Repeat("x", 1)),
		Min10UnvalidatedTypedefField:       UnvalidatedStringType(strings.Repeat("x", 1)),
		Min10UnvalidatedTypedefPtrField:    ptr.To(UnvalidatedStringType(strings.Repeat("x", 1))),
		Min10ValidatedTypedefField:         Min10Type(strings.Repeat("x", 1)),
		Min10ValidatedTypedefPtrField:      ptr.To(Min10Type(strings.Repeat("x", 1))),
		Min2UnvalidatedStringAliasField:    strings.Repeat("x", 1),
		Min2UnvalidatedStringAliasPtrField: ptr.To(strings.Repeat("x", 1)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooShort(field.NewPath("min10Field"), "", 10),
		field.TooShort(field.NewPath("min10PtrField"), "", 10),
		field.TooShort(field.NewPath("min10UnvalidatedTypedefField"), "", 10),
		field.TooShort(field.NewPath("min10UnvalidatedTypedefPtrField"), "", 10),
		field.TooShort(field.NewPath("min10ValidatedTypedefField"), "", 10),
		field.TooShort(field.NewPath("min10ValidatedTypedefPtrField"), "", 10),
		field.TooShort(field.NewPath("min2Field"), "x", 2),
		field.TooShort(field.NewPath("min2PtrField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedTypedefField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedTypedefPtrField"), "x", 2),
		field.TooShort(field.NewPath("min2ValidatedTypedefField"), "x", 2),
		field.TooShort(field.NewPath("min2ValidatedTypedefPtrField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedStringAliasField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedStringAliasPtrField"), "x", 2),
	})

	// min2 组仍为 1 字符、min10 组提升到 9 字符，仍全部低于各自阈值。
	st.Value(&Struct{
		Min2Field:                          strings.Repeat("x", 1),
		Min2PtrField:                       ptr.To(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefField:        UnvalidatedStringType(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefPtrField:     ptr.To(UnvalidatedStringType(strings.Repeat("x", 1))),
		Min2ValidatedTypedefField:          Min2Type(strings.Repeat("x", 1)),
		Min2ValidatedTypedefPtrField:       ptr.To(Min2Type(strings.Repeat("x", 1))),
		Min10Field:                         strings.Repeat("x", 9),
		Min10PtrField:                      ptr.To(strings.Repeat("x", 9)),
		Min10UnvalidatedTypedefField:       UnvalidatedStringType(strings.Repeat("x", 9)),
		Min10UnvalidatedTypedefPtrField:    ptr.To(UnvalidatedStringType(strings.Repeat("x", 9))),
		Min10ValidatedTypedefField:         Min10Type(strings.Repeat("x", 9)),
		Min10ValidatedTypedefPtrField:      ptr.To(Min10Type(strings.Repeat("x", 9))),
		Min2UnvalidatedStringAliasField:    strings.Repeat("x", 1),
		Min2UnvalidatedStringAliasPtrField: ptr.To(strings.Repeat("x", 1)),
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooShort(field.NewPath("min10Field"), "", 10),
		field.TooShort(field.NewPath("min10PtrField"), "", 10),
		field.TooShort(field.NewPath("min10UnvalidatedTypedefField"), "", 10),
		field.TooShort(field.NewPath("min10UnvalidatedTypedefPtrField"), "", 10),
		field.TooShort(field.NewPath("min10ValidatedTypedefField"), "", 10),
		field.TooShort(field.NewPath("min10ValidatedTypedefPtrField"), "", 10),
		field.TooShort(field.NewPath("min2Field"), "x", 2),
		field.TooShort(field.NewPath("min2PtrField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedTypedefField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedTypedefPtrField"), "x", 2),
		field.TooShort(field.NewPath("min2ValidatedTypedefField"), "x", 2),
		field.TooShort(field.NewPath("min2ValidatedTypedefPtrField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedStringAliasField"), "x", 2),
		field.TooShort(field.NewPath("min2UnvalidatedStringAliasPtrField"), "x", 2),
	})

	// 全部取恰好达标的长度（min2 取 2，min10 取 10），应当全部通过。
	st.Value(&Struct{
		Min2Field:                          strings.Repeat("x", 2),
		Min2PtrField:                       ptr.To(strings.Repeat("x", 2)),
		Min2UnvalidatedTypedefField:        UnvalidatedStringType(strings.Repeat("x", 2)),
		Min2UnvalidatedTypedefPtrField:     ptr.To(UnvalidatedStringType(strings.Repeat("x", 2))),
		Min2ValidatedTypedefField:          Min2Type(strings.Repeat("x", 2)),
		Min10Field:                         strings.Repeat("x", 10),
		Min10PtrField:                      ptr.To(strings.Repeat("x", 10)),
		Min10UnvalidatedTypedefField:       UnvalidatedStringType(strings.Repeat("x", 10)),
		Min10UnvalidatedTypedefPtrField:    ptr.To(UnvalidatedStringType(strings.Repeat("x", 10))),
		Min10ValidatedTypedefField:         Min10Type(strings.Repeat("x", 10)),
		Min10ValidatedTypedefPtrField:      ptr.To(Min10Type(strings.Repeat("x", 10))),
		Min2UnvalidatedStringAliasField:    strings.Repeat("x", 2),
		Min2UnvalidatedStringAliasPtrField: ptr.To(strings.Repeat("x", 2)),
	}).ExpectValid()

	// 超过阈值（3 > 2，11 > 10）同样合法，且 string 别名不受类型级校验影响。
	testVal := &Struct{
		Min2Field:                          strings.Repeat("x", 3),
		Min2PtrField:                       ptr.To(strings.Repeat("x", 3)),
		Min10Field:                         strings.Repeat("x", 11),
		Min10PtrField:                      ptr.To(strings.Repeat("x", 11)),
		Min2UnvalidatedTypedefField:        UnvalidatedStringType(strings.Repeat("x", 3)),
		Min2UnvalidatedTypedefPtrField:     ptr.To(UnvalidatedStringType(strings.Repeat("x", 3))),
		Min10UnvalidatedTypedefField:       UnvalidatedStringType(strings.Repeat("x", 11)),
		Min10UnvalidatedTypedefPtrField:    ptr.To(UnvalidatedStringType(strings.Repeat("x", 11))),
		Min2ValidatedTypedefField:          Min2Type(strings.Repeat("x", 3)),
		Min2ValidatedTypedefPtrField:       ptr.To(Min2Type(strings.Repeat("x", 3))),
		Min10ValidatedTypedefField:         Min10Type(strings.Repeat("x", 11)),
		Min10ValidatedTypedefPtrField:      ptr.To(Min10Type(strings.Repeat("x", 11))),
		Min2UnvalidatedStringAliasField:    strings.Repeat("x", 3),
		Min2UnvalidatedStringAliasPtrField: ptr.To(strings.Repeat("x", 3)),
	}
	st.Value(testVal).ExpectValid()

	// 测试 ratcheting 行为
	//
	// 新旧值相同（或旧值同样短）时跳过校验；这里旧值均不满足 minLength，
	// 但字段未被改动，因此整体应放行。
	st.Value(&Struct{
		Min2Field:                       strings.Repeat("x", 2),
		Min2PtrField:                    ptr.To(strings.Repeat("x", 2)),
		Min10Field:                      strings.Repeat("x", 11),
		Min10PtrField:                   ptr.To(strings.Repeat("x", 11)),
		Min2UnvalidatedTypedefField:     UnvalidatedStringType(strings.Repeat("x", 2)),
		Min2UnvalidatedTypedefPtrField:  ptr.To(UnvalidatedStringType(strings.Repeat("x", 2))),
		Min10UnvalidatedTypedefField:    UnvalidatedStringType(strings.Repeat("x", 11)),
		Min10UnvalidatedTypedefPtrField: ptr.To(UnvalidatedStringType(strings.Repeat("x", 11))),
		Min2ValidatedTypedefField:       Min2Type(strings.Repeat("x", 2)),
		Min2ValidatedTypedefPtrField:    ptr.To(Min2Type(strings.Repeat("x", 2))),
		Min10ValidatedTypedefField:      Min10Type(strings.Repeat("x", 11)),
		Min10ValidatedTypedefPtrField:   ptr.To(Min10Type(strings.Repeat("x", 11))),
	}).OldValue(&Struct{
		Min2Field:                       strings.Repeat("x", 1),
		Min2PtrField:                    ptr.To(strings.Repeat("x", 1)),
		Min10Field:                      strings.Repeat("x", 9),
		Min10PtrField:                   ptr.To(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefField:     UnvalidatedStringType(strings.Repeat("x", 1)),
		Min2UnvalidatedTypedefPtrField:  ptr.To(UnvalidatedStringType(strings.Repeat("x", 1))),
		Min10UnvalidatedTypedefField:    UnvalidatedStringType(strings.Repeat("x", 9)),
		Min10UnvalidatedTypedefPtrField: ptr.To(UnvalidatedStringType(strings.Repeat("x", 9))),
		Min2ValidatedTypedefField:       Min2Type(strings.Repeat("x", 1)),
		Min2ValidatedTypedefPtrField:    ptr.To(Min2Type(strings.Repeat("x", 1))),
		Min10ValidatedTypedefField:      Min10Type(strings.Repeat("x", 9)),
		Min10ValidatedTypedefPtrField:   ptr.To(Min10Type(strings.Repeat("x", 9))),
	}).ExpectValid()
}
