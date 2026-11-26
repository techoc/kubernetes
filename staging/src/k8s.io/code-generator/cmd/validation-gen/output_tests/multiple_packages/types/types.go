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

// Package types 存放共享输入类型，生成到 ../registered 与 ../external，
// 并被 ../consumer 引用。
//
// 该包只提供类型定义，不生成校验代码；生成目标由引用方的包级标签决定。
package types

// T1 的字段既覆盖嵌套结构体校验，也覆盖 slice 元素校验。
type T1 struct {
	TypeMeta int

	// +k8s:validateFalse="field T1.T2"
	T2 T2 `json:"t2"`

	// +k8s:eachVal=+k8s:validateFalse="field T1.List[*]"
	List []T2 `json:"list"`
}

// T2 被 T1 以值类型和 slice 元素两种方式引用。
type T2 struct {
	// +k8s:validateFalse="field T2.S"
	S string `json:"s"`
}

// T3 没有 TypeMeta，因此只有 external 的 validation-gen=* 会选中它。
type T3 struct {
	// +k8s:validateFalse="field T3.S"
	S string `json:"s"`
}
