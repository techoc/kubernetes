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
// format 验证 k8s-long-name-caseless 格式标记：允许大小写，长度上限 63。
// +k8s:validation-gen-nolint
package format

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:format=k8s-long-name-caseless
	LongNameField string `json:"longNameField"`

	// +k8s:format=k8s-long-name-caseless
	LongNamePtrField *string `json:"longNamePtrField"`

	// 注意：这里没有校验。
	LongNameTypedefField LongNameStringType `json:"longNameTypedefField"`
}

// +k8s:format=k8s-long-name-caseless
type LongNameStringType string
