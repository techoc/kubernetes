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

// This is a test package.
// +k8s:validation-gen-nolint
// 验证 +k8s:format=k8s-long-name（DNS subdomain，全小写）。
// 字段级与 typedef 级两种声明方式都应生效。
package format

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:format=k8s-long-name
	LongNameField string `json:"longNameField"`

	// 指针字段同样支持格式校验。
	// +k8s:format=k8s-long-name
	LongNamePtrField *string `json:"longNamePtrField"`

	// 校验声明在 typedef 上，字段本身不再重复声明。
	// Note: no validation here
	LongNameTypedefField LongNameStringType `json:"longNameTypedefField"`
}

// LongNameStringType 在类型级声明 long-name 格式，使用该类型的字段继承此校验。
// +k8s:format=k8s-long-name
type LongNameStringType string
