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
// +k8s:validation-gen-test-fixture=validateFalse

// Package cohorts 是 validation-gen 的测试包，验证 cohort 与 ShortCircuit
// 标志下校验函数的输出顺序：同一 cohort 的普通校验先于短路校验，
// 且 cohort 之间按固定次序排列，保证生成结果可复现。
// +k8s:validation-gen-nolint
package cohorts

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

// 注意：以下顺序是刻意打乱的，期望输出顺序为：
//   - T {ShortCircuit,Regular}（根类型）
//   - T c2 {ShortCircuit,Regular}（cohort c2）
//   - T c1 {ShortCircuit,Regular}（cohort c1）
//
// +k8s:validateFalse(cohort: "c2")="type T c2 Regular"
// +k8s:validateFalse(cohort: "c1")="type T c1 Regular"
// +k8s:validateFalse(cohort: "c1", flags: "ShortCircuit")="type T c1 ShortCircuit"
// +k8s:validateFalse(cohort: "c2", flags: "ShortCircuit")="type T c2 ShortCircuit"
// +k8s:validateFalse="type T Regular"
// +k8s:validateFalse(flags: "ShortCircuit")="type T ShortCircuit"
type T struct {
	TypeMeta int

	// 注意：以下顺序是刻意打乱的，期望输出顺序为：
	//   - T.S {ShortCircuit,Regular}（字段 S）（字段 S）
	//   - T.S c2 {ShortCircuit,Regular}（字段 S，cohort c2）（字段 S，cohort c2）
	//   - T.S c1 {ShortCircuit,Regular}（字段 S，cohort c1）（字段 S，cohort c1）
	// +k8s:validateFalse(cohort: "c2")="field T.S c2 Regular"
	// +k8s:validateFalse(cohort: "c1")="field T.S c1 Regular"
	// +k8s:validateFalse(cohort: "c1", flags: "ShortCircuit")="field T.S c1 ShortCircuit"
	// +k8s:validateFalse(cohort: "c2", flags: "ShortCircuit")="field T.S c2 ShortCircuit"
	// +k8s:validateFalse="field T.S Regular"
	// +k8s:validateFalse(flags: "ShortCircuit")="field T.S ShortCircuit"
	S string `json:"s"`
}
