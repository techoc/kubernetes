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

package modes

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// TestAlpha 验证 alpha 级别的 modeDiscriminator/ifMode 组合：
// 选中模式下必填字段缺失报 Required、未选中模式下字段被设置报 Forbidden，
// 错误都标记为 Alpha。
func TestAlpha(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：mode A 且设置了 FieldA
	st.Value(&AlphaStruct{D1: "A", FieldA: ptr.To("val")}).ExpectValid()

	// 非法：mode A 缺少 FieldA（必填），应为 alpha 级别
	st.Value(&AlphaStruct{D1: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), "").MarkAlpha(),
	})

	// 非法：mode A 设置了 FieldB（禁止），应为 alpha 级别
	st.Value(&AlphaStruct{D1: "A", FieldA: ptr.To("val"), FieldB: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldB"), "").MarkAlpha(),
	})

	// 合法：mode B 且设置了 FieldB
	st.Value(&AlphaStruct{D1: "B", FieldB: ptr.To("val")}).ExpectValid()

	// 非法：mode B 缺少 FieldB（必填），应为 alpha 级别
	st.Value(&AlphaStruct{D1: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("fieldB"), "").MarkAlpha(),
	})
}

// TestBeta 与 TestAlpha 场景相同，但错误级别为 Beta。
func TestBeta(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：mode A 且设置了 FieldA
	st.Value(&BetaStruct{D1: "A", FieldA: ptr.To("val")}).ExpectValid()

	// 非法：mode A 缺少 FieldA（必填），应为 beta 级别
	st.Value(&BetaStruct{D1: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("fieldA"), "").MarkBeta(),
	})

	// 非法：mode A 设置了 FieldB（禁止），应为 beta 级别
	st.Value(&BetaStruct{D1: "A", FieldA: ptr.To("val"), FieldB: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Forbidden(field.NewPath("fieldB"), "").MarkBeta(),
	})

	// 合法：mode B 且设置了 FieldB
	st.Value(&BetaStruct{D1: "B", FieldB: ptr.To("val")}).ExpectValid()

	// 非法：mode B 缺少 FieldB（必填），应为 beta 级别
	st.Value(&BetaStruct{D1: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("fieldB"), "").MarkBeta(),
	})
}

// TestMixedLevels 验证同一结构体内不同字段可拥有不同的稳定性级别：
// 错误级别跟随对应成员标签的级别。
func TestMixedLevels(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法用例
	st.Value(&MixedLevels{Mode: "A", A: ptr.To("val")}).ExpectValid()
	st.Value(&MixedLevels{Mode: "B", B: ptr.To("val")}).ExpectValid()

	// Mode=A，缺少 A -> alpha 错误（字段 A 受 alpha 门控）
	st.Value(&MixedLevels{Mode: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("a"), "").MarkAlpha(),
	})

	// Mode=B，缺少 B -> beta 错误（字段 B 受 beta 门控）
	st.Value(&MixedLevels{Mode: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("b"), "").MarkBeta(),
	})

	// Mode=A 却设置 B（禁止）-> beta 错误（字段 B 的 +k8s:modeDiscriminator 为 beta）
	st.Value(&MixedLevels{Mode: "A", A: ptr.To("val"), B: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Forbidden(field.NewPath("b"), "").MarkBeta(),
	})

	// Mode=B 却设置 A（禁止）-> alpha 错误（字段 A 的 +k8s:modeDiscriminator 为 alpha）
	st.Value(&MixedLevels{Mode: "B", A: ptr.To("val"), B: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Forbidden(field.NewPath("a"), "").MarkAlpha(),
	})
}

// TestCrossLevels 验证 discriminator 与成员字段级别不一致时，
// 错误级别以成员字段自身的标签为准。
func TestCrossLevels(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法用例
	st.Value(&CrossLevels{Kind: "A", A: ptr.To("val")}).ExpectValid()
	st.Value(&CrossLevels{Kind: "B", B: ptr.To("val")}).ExpectValid()

	// Kind=A，缺少 A -> alpha 错误（字段 A 受 alpha 门控，判别式为 beta）
	st.Value(&CrossLevels{Kind: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("a"), "").MarkAlpha(),
	})

	// Kind=B，缺少 B -> alpha 错误（字段 B 受 alpha 门控，判别式为 beta）
	st.Value(&CrossLevels{Kind: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("b"), "").MarkAlpha(),
	})

	// Kind=A 却设置 B（禁止）-> alpha 错误（字段 B 的成员标签为 alpha）
	st.Value(&CrossLevels{Kind: "A", A: ptr.To("val"), B: ptr.To("val")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Forbidden(field.NewPath("b"), "").MarkAlpha(),
	})
}

// TestSameFieldMixed 验证同一字段被不同模式的成员标签引用时，
// 按命中的 mode 决定错误级别。
func TestSameFieldMixed(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法用例
	st.Value(&SameFieldMixed{Mode: "A", Value: ptr.To("val")}).ExpectValid()
	st.Value(&SameFieldMixed{Mode: "B", Value: ptr.To("val")}).ExpectValid()

	// Mode=A，缺少 Value -> alpha 错误（member("A") 受 alpha 门控）
	st.Value(&SameFieldMixed{Mode: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("value"), "").MarkAlpha(),
	})

	// Mode=B，缺少 Value -> beta 错误（member("B") 受 beta 门控）
	st.Value(&SameFieldMixed{Mode: "B"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("value"), "").MarkBeta(),
	})
}

// TestSameValueMixedPayloads 验证同一判别式值上的多个
// payload 校验可以有不同的稳定性级别。
func TestSameValueMixedPayloads(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法：Mode=A 且 Value 已设置、长度足够
	st.Value(&SameValueMixedPayloads{Mode: "A", Value: ptr.To("abc")}).ExpectValid()

	// Mode=A，缺少 Value -> alpha 错误（required 受 alpha 门控）
	st.Value(&SameValueMixedPayloads{Mode: "A"}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.Required(field.NewPath("value"), "").MarkAlpha(),
	})

	// Mode=A，Value 太短 -> beta 错误（minLength 受 beta 门控）
	st.Value(&SameValueMixedPayloads{Mode: "A", Value: ptr.To("ab")}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByValidationStabilityLevel(), field.ErrorList{
		field.TooShort(field.NewPath("value"), "", 3).MarkBeta(),
	})
}
