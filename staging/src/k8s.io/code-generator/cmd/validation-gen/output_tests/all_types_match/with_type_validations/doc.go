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

// Note: this selects all types in the package.
// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// 这是一个测试包：所有类型都参与生成，验证类型级校验。
// 只有带校验的类型（T1、E1）应出现在生成结果中，T2/E2 即使可被引用也不生成。
// This is a test package.
// +k8s:validation-gen-nolint
package withtypevalidations

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// +k8s:validateFalse="type T1"
// T1 同时有类型级与字段级校验。
type T1 struct {
	// +k8s:validateFalse="field T1.S"
	S string `json:"s"`
}

// Note: this has no validations.
type T2 struct{}

// +k8s:validateFalse="type E1"
type E1 string

// Note: this has no validations.
type E2 string
