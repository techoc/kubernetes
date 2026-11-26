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
// +k8s:validation-gen-test-fixture=validateFalse

// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
//
// 本包验证同一字段上重复的 validateFalse 标签会全部保留且顺序稳定，
// 以及 eachKey/eachVal 与字段级校验共存时的组合行为。
package multiplevalidations

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// +k8s:validateFalse="type Struct"
type Struct struct {
	TypeMeta int

	// 字段级、key 级、value 级校验各两条：生成器不能去重，
	// 每条标签都对应一次独立的错误上报。
	// +k8s:validateFalse="field Struct.MapField #1"
	// +k8s:validateFalse="field Struct.MapField #2"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapField(keys) #1"
	// +k8s:eachKey=+k8s:validateFalse="field Struct.MapField(keys) #2"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapField[*] #1"
	// +k8s:eachVal=+k8s:validateFalse="field Struct.MapField[*] #2"
	MapField map[string]string `json:"mapField"`

	UnvalidatedMapField []string `json:"UnvalidatedMapField"`
}
