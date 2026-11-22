/*
Copyright 2016 The Kubernetes Authors.

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

// Package builtins 是 deepcopy-gen 的测试包，覆盖 Go 内建标量类型的深拷贝生成。
// 生成结果与 expect.go 中的手写期望值逐字比对。
package builtins

// Ttest 汇集各类内建标量，用于验证生成器为值类型选择直接赋值而非递归拷贝。
type Ttest struct {
	Byte byte
	// Int8    int8 // TODO: int8 becomes byte in SnippetWriter
	Int16   int16
	Int32   int32
	Int64   int64
	Uint8   uint8
	Uint16  uint16
	Uint32  uint32
	Uint64  uint64
	Float32 float32
	Float64 float64
	String  string
}
