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

// Package sliceofpointers 是 validation-gen 的测试包，覆盖 +k8s:eachVal 作用于
// 指针元素切片：生成代码需同时处理“元素为 nil”与“元素指向结构体/标量”两种情况。
// +k8s:validation-gen-nolint
package sliceofpointers

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListField[*]"
	ListField []*OtherStruct `json:"listField"`

	// +k8s:eachVal=+k8s:validateFalse="field Struct.ListPrimitiveField[*]"
	ListPrimitiveField []*string `json:"listPrimitiveField"`
}

// +k8s:validateFalse="type OtherStruct"
type OtherStruct struct {
	Value string `json:"value"`
}
