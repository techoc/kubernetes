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

// // 本包是测试夹具，用于验证生成器的行为。
//
// 覆盖 +k8s:optional 与非零 +default 的组合：默认值不能被视为“已设置”，
// 校验器必须区分 nil/零值与显式赋值。
// +k8s:validation-gen-nolint
package nonzerodefaults

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:optional
	// +default="foobar"
	StringField string `json:"stringField"`

	// +k8s:optional
	// +default="foobar"
	StringPtrField *string `json:"stringPtrField"`

	// +k8s:optional
	// +default=123
	IntField int `json:"intField"`

	// +k8s:optional
	// +default=123
	IntPtrField *int `json:"intPtrField"`

	// +k8s:optional
	// +default=true
	BoolField bool `json:"boolField"`

	// +k8s:optional
	// +default=true
	BoolPtrField *bool `json:"boolPtrField"`

	// +k8s:optional
	// +default={"name": "x"}
	StructPtrField *Submarker `json:"structPtrField"`

	// +k8s:optional
	// +default=["foo"]
	SliceField []string `json:"sliceField"`

	// +k8s:optional
	// +default={"k": "v"}
	MapField map[string]string `json:"mapField"`
}

type Submarker struct {
	Name string `json:"name"`
}
