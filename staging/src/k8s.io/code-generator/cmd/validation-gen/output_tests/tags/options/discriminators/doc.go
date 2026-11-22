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

// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包：验证 ifEnabled/ifDisabled 与 modeDiscriminator 的组合——
// 特性开关决定哪一组模式校验生效。
// This is a test package.
// +k8s:validation-gen-nolint
package discriminators

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Struct 同时包含启用与禁用 FeatureZ 时生效的两套判别器。
type Struct struct {
	TypeMeta int

	DiscriminatorField         Discriminator         `json:"discriminatorField"`
	DiscriminatorFieldDisabled DiscriminatorDisabled `json:"discriminatorFieldDisabled"`
}

// Discriminator 在 FeatureZ 启用时生效：Discriminator 字段决定哪个成员可选。
type Discriminator struct {
	// +k8s:ifEnabled(FeatureZ)=+k8s:modeDiscriminator
	Discriminator string `json:"discriminator"`

	// +k8s:ifEnabled(FeatureZ)=+k8s:ifMode("A")=+k8s:optional
	FieldA *string `json:"fieldA"`

	// +k8s:ifEnabled(FeatureZ)=+k8s:ifMode("B")=+k8s:optional
	FieldB *string `json:"fieldB"`
}

// DiscriminatorDisabled 在 FeatureZ 禁用时生效，校验逻辑相同但由 ifDisabled 包裹。
type DiscriminatorDisabled struct {
	// +k8s:ifDisabled(FeatureZ)=+k8s:modeDiscriminator
	Discriminator string `json:"discriminator"`

	// +k8s:ifDisabled(FeatureZ)=+k8s:ifMode("A")=+k8s:optional
	FieldA *string `json:"fieldA"`

	// +k8s:ifDisabled(FeatureZ)=+k8s:ifMode("B")=+k8s:optional
	FieldB *string `json:"fieldB"`
}
