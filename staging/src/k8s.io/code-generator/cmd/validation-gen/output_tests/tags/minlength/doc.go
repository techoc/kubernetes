/*
Copyright The Kubernetes Authors.

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
// 验证 +k8s:minLength：字段级与 typedef 级声明、指针字段、以及
// minLength=0 作为“已考虑但无约束”的语义标记。
package minlength

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// minLength=0 不产生校验函数，仅表达“明确无最小长度约束”。
	// +k8s:alpha=+k8s:minLength=0
	// +k8s:optional
	Min0PtrField *string `json:"min0PtrField"`

	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2Field string `json:"min2Field"`

	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2PtrField *string `json:"min2PtrField"`

	// +k8s:alpha=+k8s:minLength=10
	// +k8s:optional
	Min10Field string `json:"min10Field"`

	// +k8s:alpha=+k8s:minLength=10
	// +k8s:optional
	Min10PtrField *string `json:"min10PtrField"`

	// 以下字段的 minLength 作用在“未校验的 typedef”上，仅字段级生效。
	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2UnvalidatedTypedefField UnvalidatedStringType `json:"min2UnvalidatedTypedefField"`

	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2UnvalidatedTypedefPtrField *UnvalidatedStringType `json:"min2UnvalidatedTypedefPtrField"`

	// +k8s:alpha=+k8s:minLength=10
	// +k8s:optional
	Min10UnvalidatedTypedefField UnvalidatedStringType `json:"min10UnvalidatedTypedefField"`

	// +k8s:alpha=+k8s:minLength=10
	// +k8s:optional
	Min10UnvalidatedTypedefPtrField *UnvalidatedStringType `json:"min10UnvalidatedTypedefPtrField"`

	// 校验声明在 typedef 上，字段本身不重复声明。
	// Note: no minlength validation here
	// +k8s:optional
	Min2ValidatedTypedefField Min2Type `json:"min2ValidatedTypedefField"`

	// Note: no minlength validation here
	// +k8s:optional
	Min2ValidatedTypedefPtrField *Min2Type `json:"min2ValidatedTypedefPtrField"`

	// Note: no minlength validation here
	// +k8s:optional
	Min10ValidatedTypedefField Min10Type `json:"min10ValidatedTypedefField"`

	// Note: no minlength validation here
	// +k8s:optional
	Min10ValidatedTypedefPtrField *Min10Type `json:"min10ValidatedTypedefPtrField"`

	// 别名（type alias）的字段级校验。
	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2UnvalidatedStringAliasField UnvalidatedStringAlias `json:"min2UnvalidatedStringAliasField"`

	// +k8s:alpha=+k8s:minLength=2
	// +k8s:optional
	Min2UnvalidatedStringAliasPtrField *UnvalidatedStringAlias `json:"min2UnvalidatedStringAliasPtrField"`
}

// UnvalidatedStringType 没有校验，仅用于对照。
// Note: no validation here
type UnvalidatedStringType string

// UnvalidatedStringAlias 是别名（=），用于验证别名与 typedef 的差异。
// Note: no validation here
type UnvalidatedStringAlias = string

// Min2Type 在 typedef 上声明 minLength=2，使用它的字段继承该校验。
// Tests that min length markers on typedefs
// appropriately propagate to fields that use this type
// +k8s:alpha=+k8s:minLength=2
type Min2Type string

// Min10Type 在 typedef 上声明 minLength=10。
// Tests that min length markers on typedefs
// appropriately propagate to fields that use this type
// +k8s:alpha=+k8s:minLength=10
type Min10Type string
