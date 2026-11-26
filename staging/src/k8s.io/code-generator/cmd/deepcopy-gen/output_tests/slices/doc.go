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

// // 本包是测试夹具，用于验证生成器的行为。
package slices

// Ttest 用于验证 deepcopy-gen 对 slice 元素各种形态的处理：
// 值类型、指针、指针的指针、map、嵌套 slice 以及结构体/结构体指针。
// 生成代码必须对 map 与指针字段做深拷贝，对基本类型 slice 直接 make+copy。
type Ttest struct {
	Byte []byte
	// // Int8 []int8 暂不可用：SnippetWriter 把 int8 当作 byte
	Int16        []int16
	Int32        []int32
	Int64        []int64
	Uint8        []uint8
	Uint16       []uint16
	Uint32       []uint32
	Uint64       []uint64
	Float32      []float32
	Float64      []float64
	String       []string
	StringPtr    []*string
	StringPtrPtr []**string
	Map          []map[string]string
	MapPtr       []*map[string]string
	Slice        [][]string
	SlicePtr     []*[]string
	Struct       []Ttest
	StructPtr    []*Ttest
}
