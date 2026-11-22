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

// Package nonincluded contains test types for testing subfield field validation tags.
//
// 覆盖“被 subfield 指向的字段类型未被生成器纳入”的场景：
// 外部包类型配合 +k8s:opaqueType，子字段校验仍应正常生成。
// +k8s:validation-gen-nolint
package nonincluded

import (
	"k8s.io/code-generator/cmd/validation-gen/output_tests/_codegenignore/other"
	"k8s.io/code-generator/cmd/validation-gen/testscheme"
)

var localSchemeBuilder = testscheme.New()

type Struct struct {
	// +k8s:subfield(stringField)=+k8s:validateFalse="subfield Struct.(other.StructType).StringField"
	// +k8s:opaqueType
	other.StructType
}
