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
// 验证 +k8s:minProperties：仅适用于字符串键的 map，0 表示无约束，
// 上限为 100000。
package minproperties

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// minProperties=0 不产生校验函数。
	// +k8s:minProperties=0
	Min0Field map[string]string `json:"min0Field"`

	// +k8s:minProperties=10
	Min10Field map[string]string `json:"min10Field"`

	// value 为 typedef 时不影响 map 的键类型约束。
	// +k8s:minProperties=0
	Min0TypedefField map[string]StringType `json:"min0TypedefField"`

	// +k8s:minProperties=10
	Min10TypedefField map[string]StringType `json:"min10TypedefField"`
}

// StringType 用作 map value 的 typedef。
type StringType string
