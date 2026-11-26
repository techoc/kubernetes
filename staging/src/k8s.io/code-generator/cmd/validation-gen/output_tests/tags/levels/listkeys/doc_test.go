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

package listkeys

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestListKeys 验证 listMapKey 的稳定性级别如何决定重复元素错误的级别：
// 错误级别由 listType 的稳定性决定；单独把 key 声明为 Alpha/Beta
// 不会提升错误级别。复合 key 中每个 key 都参与重复判定。
func TestListKeys(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&ListKeyStruct{
		// 用例 1：alpha listType + 标准键
		AlphaListTypeStandardKey: []MapItem{
			{Key: "a", Value: 1},
			{Key: "a", Value: 2},
		},
		// 用例 2：标准 listType + alpha 键
		StandardListTypeAlphaKey: []MapItem{
			{Key: "b", Value: 1},
			{Key: "b", Value: 2},
		},
		// 用例 3：alpha listType + alpha 键
		AlphaListTypeAlphaKey: []MapItem{
			{Key: "c", Value: 1},
			{Key: "c", Value: 2},
		},
		// 用例 4：标准 listType + alpha 键1 + 标准键2
		StandardListTypeMixedKeys1: []MultiKeyItem{
			{Key1: "d", Key2: 1, Value: 1},
			{Key1: "d", Key2: 1, Value: 2},
		},
		// 用例 5：标准 listType + 标准键1 + alpha 键2
		StandardListTypeMixedKeys2: []MultiKeyItem{
			{Key1: "e", Key2: 1, Value: 1},
			{Key1: "e", Key2: 1, Value: 2},
		},
		// 用例 6：alpha listType + alpha 键1 + 标准键2
		AlphaListTypeMixedKeys1: []MultiKeyItem{
			{Key1: "f", Key2: 1, Value: 1},
			{Key1: "f", Key2: 1, Value: 2},
		},
		// 用例 7：alpha listType + 标准键1 + alpha 键2
		AlphaListTypeMixedKeys2: []MultiKeyItem{
			{Key1: "g", Key2: 1, Value: 1},
			{Key1: "g", Key2: 1, Value: 2},
		},

		// 用例 11：标准 listType + beta 键1 + 标准键2
		StandardListTypeMixedKeysBeta1: []MultiKeyItem{
			{Key1: "k", Key2: 1, Value: 1},
			{Key1: "k", Key2: 1, Value: 2},
		},
		// 用例 12：标准 listType + 标准键1 + beta 键2
		StandardListTypeMixedKeysBeta2: []MultiKeyItem{
			{Key1: "l", Key2: 1, Value: 1},
			{Key1: "l", Key2: 1, Value: 2},
		},
		// 用例 13：beta listType + beta 键1 + 标准键2
		BetaListTypeMixedKeys1: []MultiKeyItem{
			{Key1: "m", Key2: 1, Value: 1},
			{Key1: "m", Key2: 1, Value: 2},
		},
		// 用例 14：beta listType + 标准键1 + beta 键2
		BetaListTypeMixedKeys2: []MultiKeyItem{
			{Key1: "n", Key2: 1, Value: 1},
			{Key1: "n", Key2: 1, Value: 2},
		},

		// 用例 8：beta listType + 标准键
		BetaListTypeStandardKey: []MapItem{
			{Key: "h", Value: 1},
			{Key: "h", Value: 2},
		},
		// 用例 9：标准 listType + beta 键
		StandardListTypeBetaKey: []MapItem{
			{Key: "i", Value: 1},
			{Key: "i", Value: 2},
		},
		// 用例 10：beta listType + beta 键
		BetaListTypeBetaKey: []MapItem{
			{Key: "j", Value: 1},
			{Key: "j", Value: 2},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		// 用例 1：alpha listType + 标准键 -> alpha 错误
		field.Duplicate(field.NewPath("alphaListTypeStandardKey").Index(1), MapItem{Key: "a", Value: 2}).MarkAlpha(),

		// 用例 2：标准 listType + alpha 键 -> 普通错误
		// alpha 键被视为列表校验的功能键。
		field.Duplicate(field.NewPath("standardListTypeAlphaKey").Index(1), MapItem{Key: "b", Value: 2}),

		// 用例 3：alpha listType + alpha 键 -> alpha 错误
		field.Duplicate(field.NewPath("alphaListTypeAlphaKey").Index(1), MapItem{Key: "c", Value: 2}).MarkAlpha(),

		// 用例 4：标准 listType + alpha 键1 + 标准键2 -> 普通错误
		// 两个键都参与唯一性检查。
		field.Duplicate(field.NewPath("standardListTypeMixedKeys1").Index(1), MultiKeyItem{Key1: "d", Key2: 1, Value: 2}),

		// 用例 5：标准 listType + 标准键1 + alpha 键2 -> 普通错误
		field.Duplicate(field.NewPath("standardListTypeMixedKeys2").Index(1), MultiKeyItem{Key1: "e", Key2: 1, Value: 2}),

		// 用例 6：alpha listType + alpha 键1 + 标准键2 -> alpha 错误
		field.Duplicate(field.NewPath("alphaListTypeMixedKeys1").Index(1), MultiKeyItem{Key1: "f", Key2: 1, Value: 2}).MarkAlpha(),

		// 用例 7：alpha listType + 标准键1 + alpha 键2 -> alpha 错误
		field.Duplicate(field.NewPath("alphaListTypeMixedKeys2").Index(1), MultiKeyItem{Key1: "g", Key2: 1, Value: 2}).MarkAlpha(),

		// 用例 11：标准 listType + beta 键1 + 标准键2 -> 普通错误
		field.Duplicate(field.NewPath("standardListTypeMixedKeysBeta1").Index(1), MultiKeyItem{Key1: "k", Key2: 1, Value: 2}),

		// 用例 12：标准 listType + 标准键1 + beta 键2 -> 普通错误
		field.Duplicate(field.NewPath("standardListTypeMixedKeysBeta2").Index(1), MultiKeyItem{Key1: "l", Key2: 1, Value: 2}),

		// 用例 13：beta listType + beta 键1 + 标准键2 -> beta 错误
		field.Duplicate(field.NewPath("betaListTypeMixedKeys1").Index(1), MultiKeyItem{Key1: "m", Key2: 1, Value: 2}).MarkBeta(),

		// 用例 14：beta listType + 标准键1 + beta 键2 -> beta 错误
		field.Duplicate(field.NewPath("betaListTypeMixedKeys2").Index(1), MultiKeyItem{Key1: "n", Key2: 1, Value: 2}).MarkBeta(),

		// 用例 8：beta listType + 标准键 -> beta 错误
		field.Duplicate(field.NewPath("betaListTypeStandardKey").Index(1), MapItem{Key: "h", Value: 2}).MarkBeta(),

		// 用例 9：标准 listType + beta 键 -> 普通错误
		// beta 键被视为列表校验的功能键。
		field.Duplicate(field.NewPath("standardListTypeBetaKey").Index(1), MapItem{Key: "i", Value: 2}),

		// 用例 10：beta listType + beta 键 -> beta 错误
		field.Duplicate(field.NewPath("betaListTypeBetaKey").Index(1), MapItem{Key: "j", Value: 2}).MarkBeta(),
	})
}
