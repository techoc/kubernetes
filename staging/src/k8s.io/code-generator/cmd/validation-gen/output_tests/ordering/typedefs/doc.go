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

// 这是一个测试包：验证 typedef（具名字符串类型）上校验标签的生成顺序，
// 组合方式与 ordering/structs 对应。
// 这是一个测试包。
// +k8s:validation-gen-nolint
package typedefs

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 把这组标签当作 4 个 bit，覆盖所有组合：
//   bit 0：无标志
//   bit 1：ShortCircuit

// 注意：没有校验。
type E00 string

// +k8s:validateFalse="E01, no flags"
type E01 string

// +k8s:validateFalse(flags: "ShortCircuit")="E02, ShortCircuit"
type E02 string

// +k8s:validateFalse="E03, no flags"
// +k8s:validateFalse(flags: "ShortCircuit")="E03, ShortCircuit"
type E03 string

// 注意：这里的顺序是刻意错误的。
// +k8s:validateFalse="EMultiple, no flags 1"
// +k8s:validateFalse(flags: "ShortCircuit")="EMultiple, ShortCircuit 1"
// +k8s:validateFalse="E0, string payload"
// +k8s:validateFalse="EMultiple, no flags 2"
// +k8s:validateFalse(flags: "ShortCircuit")="EMultiple, ShortCircuit 2"
type EMultiple string
