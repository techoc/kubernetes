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

package subfields

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestAlpha 验证 alpha 级别的子字段校验与 union 校验：
// 错误应带上 alpha 稳定性标记，路径应指向具体子字段。
func TestAlpha(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 合法基线：所有值都满足各自阈值。
	st.Value(&Struct{
		Subfield:     SubStruct{Inner: 5},
		SubfieldBeta: SubStruct{Inner: 5},
	}).ExpectValid()

	// 失败被标记为 alpha：路径定位到 subfield.inner。
	// 标记为 alpha 的失败用例
	st.Value(&Struct{
		Subfield:     SubStruct{Inner: 1},
		SubfieldBeta: SubStruct{Inner: 5},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("subfield", "inner"), 1, "").WithOrigin("minimum").MarkAlpha(),
	})

	// union 成员同时被指定时，zeroOrOneOf 错误也应标记为 alpha。
	one := 1
	st.Value(&Struct{
		Subfield:     SubStruct{Inner: 5},
		SubfieldBeta: SubStruct{Inner: 5},
		UnionField: SubUnion{
			Z1: &one,
			Z2: &one,
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("unionField"), &SubUnion{
			Z1: &one, Z2: &one,
		}, "only one of z1, z2 may be specified").WithOrigin("zeroOrOneOf").MarkAlpha(),
	})
}

// TestBeta 验证 beta 级别的子字段校验：错误应带上 beta 稳定性标记。
func TestBeta(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&Struct{
		Subfield:     SubStruct{Inner: 5},
		SubfieldBeta: SubStruct{Inner: 5},
	}).ExpectValid()

	// 失败被标记为 beta：路径定位到 subfieldBeta.inner。
	// 标记为 beta 的失败用例
	st.Value(&Struct{
		Subfield:     SubStruct{Inner: 5},
		SubfieldBeta: SubStruct{Inner: 1},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("subfieldBeta", "inner"), 1, "").WithOrigin("minimum").MarkBeta(),
	})
}
