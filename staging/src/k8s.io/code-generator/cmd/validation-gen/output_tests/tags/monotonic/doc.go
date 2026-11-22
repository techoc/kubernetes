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

// This is a test package.
// +k8s:validation-gen-nolint
// 验证 +k8s:monotonic：字段值在更新时只能增大（或保持不变），
// 常与 minimum、optional/required、NoUnset 组合使用。
package monotonic

import "k8s.io/code-generator/cmd/validation-gen/testscheme"

var localSchemeBuilder = testscheme.New()

type Struct struct {
	TypeMeta int

	// +k8s:minimum=0
	// +k8s:monotonic
	IntField int `json:"intField"`

	// +k8s:minimum=0
	// +k8s:monotonic
	Int64Field int64 `json:"int64Field"`

	// +k8s:minimum=0
	// +k8s:monotonic
	Uint64Field uint64 `json:"uint64Field"`

	// optional + NoUnset：一旦设置就不允许再清空，同时受 monotonic 约束。
	// +k8s:optional
	// +k8s:minimum=0
	// +k8s:update=NoUnset
	// +k8s:monotonic
	IntPtrField *int `json:"intPtrField"`

	// 校验声明在 typedef 上，字段继承。
	MonotonicField MonotonicType `json:"monotonicField"`

	MonotonicPtrField *MonotonicType `json:"monotonicPtrField"`

	// +k8s:optional
	// +k8s:minimum=0
	// +k8s:update=NoUnset
	// +k8s:monotonic
	OptionalInt int `json:"optionalInt,omitempty"`

	// +k8s:required
	// +k8s:minimum=0
	// +k8s:monotonic
	RequiredInt int `json:"requiredInt"`

	// +k8s:optional
	// +k8s:minimum=0
	// +k8s:update=NoUnset
	// +k8s:monotonic
	OptionalIntPtr *int `json:"optionalIntPtr"`

	// +k8s:required
	// +k8s:minimum=0
	// +k8s:monotonic
	RequiredIntPtr *int `json:"requiredIntPtr"`

	// 负下界 + monotonic 的组合。
	// +k8s:minimum=-10
	// +k8s:monotonic
	NegativeInt int `json:"negativeInt"`
}

// MonotonicType 在 typedef 上声明 monotonic 与 minimum，使用它的字段继承。
// +k8s:minimum=0
// +k8s:monotonic
type MonotonicType int
