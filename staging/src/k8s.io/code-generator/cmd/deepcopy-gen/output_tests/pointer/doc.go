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
//
// pointer 包专门覆盖“指针套指针”的组合：*T、**T，以及指针指向 map/slice/struct
// 的情形。deepcopy-gen 生成的代码必须对每一层都先判 nil 再解引用，
// 因此这里列出的字段类型是生成器 doPointer 分支的回归用例。
package pointer

type Ttest struct {
	Builtin   *string
	Ptr       **string
	Map       *map[string]string
	Slice     *[]string
	MapPtr    **map[string]string
	SlicePtr  **[]string
	Struct    *Ttest
	StructPtr **Ttest
}
