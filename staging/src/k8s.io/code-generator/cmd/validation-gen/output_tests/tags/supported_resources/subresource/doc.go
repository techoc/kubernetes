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

// Note: this selects all types in the package.
// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme

// This is a test package.
// +k8s:validation-gen-nolint
// 验证 supportsSubresource 声明：根资源默认受支持，/status、/scale 与
// 多级路径 /x/y 需显式声明。
package subresource

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// Root resource is supported by default

// 显式声明支持的子资源；未列出的子资源在校验时会报错。
// +k8s:supportsSubresource="/status"
// +k8s:supportsSubresource="/scale"
// +k8s:supportsSubresource="/x/y"

// T1 is a test type
type T1 struct {
	// +k8s:validateTrue="field T1.S"
	S string `json:"s"`
}
