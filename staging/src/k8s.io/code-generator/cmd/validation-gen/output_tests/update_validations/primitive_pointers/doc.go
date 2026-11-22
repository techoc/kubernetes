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

// Package primitivepointers is a test package.
// 中文说明：验证 immutable 作用于指向基本类型的指针字段时的更新校验。
//
// +k8s:validation-gen-nolint
//
//nolint:unused
package primitivepointers

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 的四个指针字段均为 immutable，覆盖 string/int/bool/float64。
type Struct struct {
	// +k8s:immutable
	SP *string `json:"sp"`
	// +k8s:immutable
	IP *int `json:"ip"`
	// +k8s:immutable
	BP *bool `json:"bp"`
	// +k8s:immutable
	FP *float64 `json:"fp"`
}
