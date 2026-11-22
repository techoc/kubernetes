/*
Copyright 2017 The Kubernetes Authors.

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

// +k8s:deepcopy-gen=package

// This is a test package.
// 用于验证 deepcopy-gen 为接口类型生成 DeepCopy 方法，以及包含接口切片的
// 结构体的复制路径。
package interfaces

// Inner 是被字段引用的接口；生成器需为其生成 DeepCopyInner。
type Inner interface {
	Function() float64
	DeepCopyInner() Inner
}

// Ttest 持有接口切片，验证生成器会逐元素调用接口的复制方法。
type Ttest struct {
	I []Inner
}
