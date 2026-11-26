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

package mode

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestStrictUnion 验证判别器各取值下的 strict union 语义：
// 选中分支字段必填，未选中分支字段隐式禁止。
func TestStrictUnion(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// mode A：FieldA 必填，FieldB 隐式禁止
	st.Value(&StrictUnion{D1: "A", FieldA: ptr.To("val")}).ExpectValid()
	st.Value(&StrictUnion{D1: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), ""),
	})
	st.Value(&StrictUnion{D1: "A", FieldA: ptr.To("val"), FieldB: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldB"), ""),
	})

	// mode B：FieldA 隐式禁止，FieldB 必填
	st.Value(&StrictUnion{D1: "B", FieldB: ptr.To("val")}).ExpectValid()
	st.Value(&StrictUnion{D1: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("fieldB"), ""),
	})
	st.Value(&StrictUnion{D1: "B", FieldA: ptr.To("val"), FieldB: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldA"), ""),
	})
}

// TestSharedField 验证同一字段可在多个 mode 中声明为合法：
// 只有未声明它的 mode 才会禁止该字段。
func TestSharedField(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 在 A 和 B 中可选
	st.Value(&SharedField{D1: "A"}).ExpectValid()
	st.Value(&SharedField{D1: "A", FieldA: ptr.To("val")}).ExpectValid()
	st.Value(&SharedField{D1: "B"}).ExpectValid()
	st.Value(&SharedField{D1: "B", FieldA: ptr.To("val")}).ExpectValid()

	// 在 C 中禁止
	st.Value(&SharedField{D1: "C", FieldA: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldA"), ""),
	})
}

// TestChainedValidation 验证 mode 内的必填/禁止与字段自身的链式校验共存：
// 选中分支先检查 required，再执行 maxLength。
func TestChainedValidation(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// mode A：必填且 maxLength 5
	st.Value(&ChainedValidation{D1: "A", FieldA: ptr.To("abc")}).ExpectValid()
	st.Value(&ChainedValidation{D1: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), ""),
	})
	st.Value(&ChainedValidation{D1: "A", FieldA: ptr.To("too-long")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooLong(field.NewPath("fieldA"), "too-long", 5),
	})

	// mode B：未列出，因此隐式禁止
	st.Value(&ChainedValidation{D1: "B", FieldA: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldA"), ""),
	})
}

// TestImplicitForbidden 验证“未在 mode 中列出”自动推导为禁止，
// 不需要显式写 forbidden 标签。
func TestImplicitForbidden(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// mode A：可选
	st.Value(&ImplicitForbidden{D1: "A"}).ExpectValid()
	st.Value(&ImplicitForbidden{D1: "A", FieldA: ptr.To("val")}).ExpectValid()

	// mode B：未列出，因此隐式禁止
	st.Value(&ImplicitForbidden{D1: "B", FieldA: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldA"), ""),
	})
}

// TestNonStringDiscriminator 验证判别器可以是 bool 等非字符串类型，
// 生成代码必须按实际类型比较而不是硬编码字符串。
func TestNonStringDiscriminator(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// Bool 模式
	st.Value(&NonStringDiscriminator{D1: true, FieldA: ptr.To("val")}).ExpectValid()
	st.Value(&NonStringDiscriminator{D1: true}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), ""),
	})
	st.Value(&NonStringDiscriminator{D1: false, FieldA: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldA"), ""),
	})
}

// TestMultipleDiscriminators 验证所有判别器同时满足各自 mode 约束，
// 缺少任一必填字段都应独立报错。
func TestMultipleDiscriminators(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&MultipleDiscriminators{
		D1:     "A",
		D2:     "B",
		FieldA: ptr.To("valA"),
		FieldB: ptr.To("valB"),
	}).ExpectValid()

	st.Value(&MultipleDiscriminators{
		D1: "A",
		D2: "B",
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), ""),
		field.Required(field.NewPath("fieldB"), ""),
	})
}

// TestCollections 验证 mode 约束同样适用于 slice/map 字段，
// 未选中分支的集合必须整体禁止。
func TestCollections(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// mode A：集合合法（可选）
	st.Value(&Collections{
		D1: "A",
	}).ExpectValid()

	st.Value(&Collections{
		D1:        "A",
		ListField: []string{"item"},
		MapField:  map[string]string{"key": "val"},
	}).ExpectValid()

	// mode B：未列出，因此隐式禁止
	st.Value(&Collections{
		D1:        "B",
		ListField: []string{"item"},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("listField"), ""),
	})

	st.Value(&Collections{
		D1:       "B",
		MapField: map[string]string{"key": "val"},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.Forbidden(field.NewPath("mapField"), ""),
	})
}

// TestRatcheting 验证 mode 相关校验的 ratcheting 判定：
// 字段未变则跳过，字段或判别器一旦变化就重新完整校验。
func TestRatcheting(t *testing.T) {
	mkTest := func() *ChainedValidation {
		return &ChainedValidation{
			D1:     "A",
			FieldA: ptr.To("too-long-string"),
		}
	}

	st := localSchemeBuilder.Test(t)

	// 1. 新对象非法
	st.Value(mkTest()).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooLong(field.NewPath("fieldA"), "too-long-string", 5),
	})

	// 2. 未变化的更新合法（ratcheting）
	st.Value(mkTest()).OldValue(mkTest()).ExpectValid()

	// 3. 值变化后重新校验（失败）
	mkDifferent := func() *ChainedValidation {
		return &ChainedValidation{
			D1:     "A",
			FieldA: ptr.To("also-too-long"),
		}
	}
	st.Value(mkTest()).OldValue(mkDifferent()).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooLong(field.NewPath("fieldA"), "too-long-string", 5),
	})

	// 4. 判别式变化后重新校验（失败）
	//
	// 即使字段值没变，判别器从 B 切到 A 也必须重新校验，否则会绕过 mode 约束。
	mkDifferentDisc := func() *ChainedValidation {
		return &ChainedValidation{
			D1:     "B", // Discriminator changed from B -> A
			FieldA: ptr.To("too-long-string"),
		}
	}
	st.Value(mkTest()).OldValue(mkDifferentDisc()).ExpectMatches(field.ErrorMatcher{}.ByType().ByField(), field.ErrorList{
		field.TooLong(field.NewPath("fieldA"), "too-long-string", 5),
	})
}
