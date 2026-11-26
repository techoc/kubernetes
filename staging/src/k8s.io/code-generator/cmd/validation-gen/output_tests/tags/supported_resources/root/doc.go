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

// 注意：这里选中包内全部类型。
// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包。
//
// 无任何 +k8s:supportsSubresource / +k8s:isSubresource 标签时，
// 根资源校验可用，而所有子资源校验都应报错（见 subresource 包）。
// +k8s:validation-gen-nolint
package root

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 验证在没有任何 +k8s:supportsSubresource 或 +k8s:isSubresource 标签时，
// 根资源的校验被允许，而所有 subresource 的校验都会报错。

// T1 是测试类型
type T1 struct {
	// +k8s:validateTrue="field T1.S"
	S string `json:"s"`
}
