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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// This is a test package.
// +k8s:validation-gen-nolint
//
// 本包是 ifEnabled/ifDisabled 的基础用例：单个选项、多个选项混合，
// 以及作用在 slice 别名字段上的开关。
package simple

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

// MySlice 用于验证选项开关作用在 slice 别名类型字段上的行为。
type MySlice []string

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:ifEnabled(FeatureX)=+k8s:validateFalse="field Struct.XEnabledField"
	XEnabledField string `json:"xEnabledField"`

	// +k8s:ifDisabled(FeatureX)=+k8s:validateFalse="field Struct.XDisabledField"
	XDisabledField string `json:"xDisabledField"`

	// +k8s:ifEnabled(FeatureY)=+k8s:validateFalse="field Struct.YEnabledField"
	YEnabledField string `json:"yEnabledField"`

	// +k8s:ifDisabled(FeatureY)=+k8s:validateFalse="field Struct.YDisabledField"
	YDisabledField string `json:"yDisabledField"`

	// +k8s:ifEnabled(FeatureX)=+k8s:validateFalse="field Struct.XYMixedField/X"
	// +k8s:ifDisabled(FeatureY)=+k8s:validateFalse="field Struct.XYMixedField/Y"
	XYMixedField string `json:"xyMixedField"`
	// +k8s:ifEnabled(FeatureX)=+k8s:validateFalse="field Struct.NilableAliasField"
	NilableAliasField MySlice `json:"nilableAliasField"`
}
