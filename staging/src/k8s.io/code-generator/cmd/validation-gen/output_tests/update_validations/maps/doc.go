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

// +k8s:validation-gen=*
// +k8s:validation-gen-scheme-registry=k8s.io/code-generator/cmd/validation-gen/testscheme.Scheme
// +k8s:validation-gen-test-fixture=validateFalse

// This is a test package.
// +k8s:validation-gen-nolint
// 验证 map 类型字段在更新场景下的校验生成：包级选择所有类型（=*），
// 生成器应能递归处理 map value 结构体的校验。
package maps

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// T1 含一个 map，value 为带校验的结构体 M1。
type T1 struct {
	MSM1 map[string]M1 `json:"msm1"`
}

// M1 的 S 字段带固定失败校验，用于观察 map value 的校验路径。
type M1 struct {
	// +k8s:validateFalse="M1.S"
	S string `json:"s"`
}
