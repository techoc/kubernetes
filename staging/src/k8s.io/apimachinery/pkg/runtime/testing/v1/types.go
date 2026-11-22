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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ExternalSimple 是用于测试的简单外部版本类型。
//
// 内嵌的 TypeMeta 使用空 json tag，因此其 apiVersion/kind 字段会与外层对象平铺，
// 与真实 API 类型的序列化布局保持一致；TestString 则是可被测试随意赋值的普通字段。
type ExternalSimple struct {
	runtime.TypeMeta `json:""`
	TestString       string `json:"testString"`
}
