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

// 注意：这会选中包内所有类型。
// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// 这是一个测试包：验证 isSubresource 标签让根类型在指定子资源
// （此处为 /scale）上下文中也生成校验。
// // 本包是测试夹具，用于验证生成器的行为。
// +k8s:validation-gen-nolint
package issubresource

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 根资源默认受支持

// +k8s:isSubresource="/scale"

// T1 是测试类型
// T1 是测试类型，其字段校验应在根资源与 /scale 子资源下都生效。
type T1 struct {
	// +k8s:validateTrue="field T1.S"
	S string `json:"s"`
}
