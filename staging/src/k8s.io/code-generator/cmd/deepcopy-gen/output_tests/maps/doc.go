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

// 这是一个测试包：覆盖 map 键值对中各种元素类型的 deepcopy 生成，
// 生成的深拷贝结果再由 output_test 与反射深拷贝做对比。
// This is a test package.
package maps

// Ttest 用 map[string]X 覆盖各值类型 X，确保生成器对内置类型、指针、
// 嵌套 map/slice、结构体及其指针都能生成正确的深拷贝。
type Ttest struct {
	Byte map[string]byte
	// Int8    map[string]int8 //TODO: int8 becomes byte in SnippetWriter
	Int16        map[string]int16
	Int32        map[string]int32
	Int64        map[string]int64
	Uint8        map[string]uint8
	Uint16       map[string]uint16
	Uint32       map[string]uint32
	Uint64       map[string]uint64
	Float32      map[string]float32
	Float64      map[string]float64
	String       map[string]string
	StringPtr    map[string]*string
	StringPtrPtr map[string]**string
	Map          map[string]map[string]string
	MapPtr       map[string]*map[string]string
	Slice        map[string][]string
	SlicePtr     map[string]*[]string
	Struct       map[string]Ttest
	StructPtr    map[string]*Ttest
}
