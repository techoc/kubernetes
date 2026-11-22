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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// This is a test package.
// +k8s:validation-gen-nolint
// 验证内嵌结构体字段的校验标记：T1 内嵌 T2（值）与 *T3（指针），
// 生成器应把内嵌字段的校验正确挂到 T1 上。
package embedded

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// T1 同时内嵌 T2 值与 *T3 指针，用于测试内嵌字段的校验合并。
type T1 struct {
	TypeMeta int

	// NOTE: It's weird to have IntField in both, but Go allows it.
	T2  `json:""`
	*T3 `json:""`
}

// T2 作为值内嵌到 T1，其 IntField 的校验应在 T1 上生效。
type T2 struct {
	// +k8s:validateFalse="T2.IntField"
	IntField int `json:"intField"`
}

// T3 作为指针内嵌到 T1，用于验证生成器对指针内嵌字段的处理。
type T3 struct {
	// +k8s:validateFalse="T3.StringField"
	StringField string `json:"stringField"`

	// +k8s:validateFalse="T3.IntField"
	IntField int `json:"intField"`
}
