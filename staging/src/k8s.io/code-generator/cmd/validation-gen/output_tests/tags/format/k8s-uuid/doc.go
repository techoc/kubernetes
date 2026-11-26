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

// 这是一个测试包。
// k8suuid 验证 k8s-uuid 格式标记在值、指针与类型定义上的生成结果。
// +k8s:validation-gen-nolint
package k8suuid

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// MyType 是含 uuid 格式字段的结构体。
// +k8s:validation:Required
type MyType struct {
	TypeMeta int
	// +k8s:optional
	// +k8s:format=k8s-uuid
	UUIDField string `json:"uuidField"`
	// +k8s:optional
	// +k8s:format=k8s-uuid
	UUIDPtrField *string `json:"uuidPtrField"`
	// 注意：这里没有校验。
	UUIDTypedefField UUIDStringType `json:"uuidTypedefField"`
}

// +k8s:format=k8s-uuid
type UUIDStringType string
