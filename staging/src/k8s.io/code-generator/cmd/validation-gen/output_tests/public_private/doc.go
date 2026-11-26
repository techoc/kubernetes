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

// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// publicprivate 包是测试包。
//
// 覆盖导出/非导出字段与类型的组合：私有字段带 json tag 也能被
// 指令 +k8s:validateFalse 命中，私有类型自身也可作为校验目标。
//
// +k8s:validation-gen-nolint
//
//nolint:unused,govet,staticcheck // govet disables structtag check, which checks for use of tags on private fields; staticcheck calls out SA5008: unexported struct field cannot have non-ignored `json:"private"`
package publicprivate

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type T1 struct {
	// +k8s:validateFalse="field T1.Public"
	Public string `json:"public"`

	// +k8s:validateFalse="field T1.private"
	private string `json:"private"`
}

type private struct {
	// +k8s:validateFalse="field private.Public"
	Public string `json:"public"`

	// +k8s:validateFalse="field private.private"
	private string `json:"private"`
}
