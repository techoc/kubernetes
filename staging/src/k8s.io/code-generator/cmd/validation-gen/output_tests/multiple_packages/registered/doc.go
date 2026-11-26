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

// ../types 校验的注册（canonical）副本。
// +k8s:validation-gen=TypesWithField=TypeMeta
// +k8s:validation-gen-input=k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/types
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
//
// 与 ../external 相对：registered 把校验函数生成到本包（即“规范副本”），
// 通过 validation-gen-input 从 types 包读取类型；external 则把函数生成到
// 类型所在的包，供跨包引用测试。
//
// 这是一个测试包。
// +k8s:validation-gen-nolint
package registered

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()
