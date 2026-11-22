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

package unions

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/validation/field"
)

// TestAlpha 验证 union / zeroOrOneOf 在 alpha 级别下的报错：
// 判别器要求的成员缺失，以及多个互斥成员同时出现。
func TestAlpha(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	// 判别器为 DM1 时 M1 必须存在。
	st.Value(&Struct{
		D:  DM1,
		M1: &M1{},
	}).ExpectValid()

	st.Value(&Struct{
		D:  DM1,
		M1: nil, // required by discriminator
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("m1"), nil, "").WithOrigin("union").MarkAlpha(),
	})

	// 结构体字段上的 zeroOrOneOf。
	st.Value(&MyStruct{
		Z1: &Z1{},
		Z2: &Z2{},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(nil, &MyStruct{
			Z1: &Z1{}, Z2: &Z2{},
		}, "only one of z1, z2 may be specified").WithOrigin("zeroOrOneOf").MarkAlpha(),
	})

	// 列表元素上的 zeroOrOneOf。
	st.Value(&MyListStruct{
		Tasks: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "failed", State: "Failed"},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("tasks"), nil, "").WithOrigin("zeroOrOneOf").MarkAlpha(),
	})
}

// TestBeta 与 TestAlpha 用例一一对应，只是全部标签换成 beta 级别，
// 用于确认错误上的稳定性级别随标签正确变化。
func TestBeta(t *testing.T) {
	st := localSchemeBuilder.Test(t)

	st.Value(&UnionStructBeta{
		DBeta:  BetaDM1,
		M1Beta: &BetaM1{},
	}).ExpectValid()

	st.Value(&UnionStructBeta{
		DBeta:  BetaDM1,
		M1Beta: nil, // required by discriminator
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("m1Beta"), nil, "").WithOrigin("union").MarkBeta(),
	})

	st.Value(&MyStructBeta{
		Z1Beta: &BetaZ1{},
		Z2Beta: &BetaZ2{},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(nil, &MyStructBeta{
			Z1Beta: &BetaZ1{}, Z2Beta: &BetaZ2{},
		}, "only one of z1Beta, z2Beta may be specified").WithOrigin("zeroOrOneOf").MarkBeta(),
	})

	st.Value(&MyListStructBeta{
		TasksBeta: []Task{
			{Name: "succeeded", State: "Succeeded"},
			{Name: "failed", State: "Failed"},
		},
	}).ExpectMatches(field.ErrorMatcher{}.ByType().ByField().ByOrigin().ByValidationStabilityLevel(), field.ErrorList{
		field.Invalid(field.NewPath("tasksBeta"), nil, "").WithOrigin("zeroOrOneOf").MarkBeta(),
	})
}
