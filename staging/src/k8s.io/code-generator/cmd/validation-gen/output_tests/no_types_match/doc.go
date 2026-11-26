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

// 注意：没有类型匹配此规则。
// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// // 本包是测试夹具，用于验证生成器的行为。
//
// 类型过滤器 TypesWithField=TypeMeta 在本包不匹配任何类型，用于验证
// 生成器在“选择器无命中”时输出空实现而不报错。
// +k8s:validation-gen-nolint
package notypesmatch

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type T1 struct {
	// +k8s:validateFalse="from field T1.S"
	S string
	// +k8s:validateFalse="from field T1.T2"
	T2 T2
}

type T2 struct {
	// +k8s:validateFalse="from field T2.S"
	S string
}
