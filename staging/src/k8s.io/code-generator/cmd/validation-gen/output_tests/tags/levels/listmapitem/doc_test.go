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

package listmapitem

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestListMapItem 验证校验错误的 stability level 由标签来源决定：
// 标准标签产生普通错误，alpha/beta 标签（或对应级别的校验声明）产生
// 带级别标记的错误；两者叠加时取更高（更严格）的级别。
func TestListMapItem(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&ListMapItemStruct{
		StandardItem:    []MapItem{{Key: "foo", Value: 5}},
		AlphaItemTag:    []MapItem{{Key: "foo", Value: 5}},
		AlphaValidation: []MapItem{{Key: "foo", Value: 5}},
		DoubleAlpha:     []MapItem{{Key: "foo", Value: 5}},

		BetaItemTag:    []MapItem{{Key: "foo", Value: 5}},
		BetaValidation: []MapItem{{Key: "foo", Value: 5}},
		DoubleBeta:     []MapItem{{Key: "foo", Value: 5}},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		// 用例 1：标准 Item -> 普通错误
		field.Invalid(field.NewPath("standardItem").Index(0).Child("value"), 5, "").WithOrigin("minimum"),

		// 用例 2：alpha Item 标签 -> alpha 错误
		field.Invalid(field.NewPath("alphaItemTag").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkAlpha(),

		// 用例 3：alpha 校验 -> alpha 错误
		field.Invalid(field.NewPath("alphaValidation").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkAlpha(),

		// 用例 4：双重 alpha -> alpha 错误
		field.Invalid(field.NewPath("doubleAlpha").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkAlpha(),

		// 用例 5：beta Item 标签 -> beta 错误
		field.Invalid(field.NewPath("betaItemTag").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkBeta(),

		// 用例 6：beta 校验 -> beta 错误
		field.Invalid(field.NewPath("betaValidation").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkBeta(),

		// 用例 7：双重 beta -> beta 错误
		field.Invalid(field.NewPath("doubleBeta").Index(0).Child("value"), 5, "").WithOrigin("minimum").MarkBeta(),
	})
}
