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

package wholepkg

import "k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg"

// 本文件按类型形态分组构造测试类型：值、指针、切片、结构体嵌套、手写
// DeepCopy 方法以及接口实现。生成器对不同形态需要选择不同的拷贝策略
// （直接赋值 / 逐字段拷贝 / 调用类型自身的 DeepCopy / 通过接口方法拷贝），
// 每种策略都在 zz_generated.deepcopy.go 中产生可预期的代码。

// 简单类型
// StructEmpty 无字段，用于验证空结构体的拷贝结果是 `*out = *in`。
type StructEmpty struct{}

// 仅基础类型
// StructPrimitives 只含内建标量，生成器应直接整结构赋值。
type StructPrimitives struct {
	BoolField   bool
	IntField    int
	StringField string
	FloatField  float64
}

// StructPrimitivesAlias 是 StructPrimitives 的命名别名类型。
// 底层类型相同但类型身份不同，生成器需为其单独生成方法而非复用原类型的方法。
type StructPrimitivesAlias StructPrimitives

// StructEmbedStructPrimitives 内嵌值类型，验证内嵌字段按字段名访问而非提升字段。
type StructEmbedStructPrimitives struct {
	StructPrimitives
}

// StructEmbedInt 内嵌非导出内建类型；这种形态没有字段名，只能整体赋值。
type StructEmbedInt struct {
	int //nolint:unused
}

// StructStructPrimitives 以具名字段持有值类型。
type StructStructPrimitives struct {
	StructField StructPrimitives
}

// 手写 DeepCopy 方法
// ManualStruct 自带手写 DeepCopy 方法，生成器必须调用它而不是展开字段。
type ManualStruct struct {
	StringField string
}

// DeepCopy 返回自身副本；因字段均为值类型，直接返回即可。
func (m ManualStruct) DeepCopy() ManualStruct {
	return m
}

// ManualStructAlias 是 ManualStruct 的别名类型，不继承其手写方法。
type ManualStructAlias ManualStruct

// StructEmbedManualStruct 内嵌带手写 DeepCopy 的类型，验证内嵌场景同样走手写方法。
type StructEmbedManualStruct struct {
	ManualStruct
}

// 仅指向基础类型的指针
// StructPrimitivePointers 持有标量指针，生成器需为每个指针分配新地址。
type StructPrimitivePointers struct {
	BoolPtrField   *bool
	IntPtrField    *int
	StringPtrField *string
	FloatPtrField  *float64
}

// StructPrimitivePointersAlias 是 StructPrimitivePointers 的别名类型。
type StructPrimitivePointersAlias StructPrimitivePointers

// StructEmbedStructPrimitivePointers 内嵌含指针字段的值类型。
type StructEmbedStructPrimitivePointers struct {
	StructPrimitivePointers
}

// StructEmbedPointer 内嵌指针类型；生成器必须复制指针指向的值，避免别名共享。
type StructEmbedPointer struct {
	*int
}

// StructStructPrimitivePointers 以具名字段持有指针结构体。
type StructStructPrimitivePointers struct {
	StructField StructPrimitivePointers
}

// 手写 DeepCopy 方法
// ManualSlice 自带手写 DeepCopy，用于验证切片类型的手写方法优先于生成方法。
type ManualSlice []string

// DeepCopy 分配新切片并复制元素，确保与原切片不共享底层数组。
func (m ManualSlice) DeepCopy() ManualSlice {
	r := make(ManualSlice, len(m))
	copy(r, m)
	return r
}

// 切片类型
// StructSlices 覆盖各类切片字段：标量、结构体、指针结构体、嵌套切片与手写 DeepCopy 类型。
// 每种元素类型在生成代码中对应不同的拷贝策略。
type StructSlices struct {
	SliceBoolField                         []bool
	SliceByteField                         []byte
	SliceIntField                          []int
	SliceStringField                       []string
	SliceFloatField                        []float64
	SliceStructPrimitivesField             []StructPrimitives
	SliceStructPrimitivesAliasField        []StructPrimitivesAlias
	SliceStructPrimitivePointersField      []StructPrimitivePointers
	SliceStructPrimitivePointersAliasField []StructPrimitivePointersAlias
	SliceSliceIntField                     [][]int
	SliceManualStructField                 []ManualStruct
	ManualSliceField                       ManualSlice
}

// StructSlicesAlias 是 StructSlices 的别名类型。
type StructSlicesAlias StructSlices

// StructEmbedStructSlices 内嵌含切片的类型。
type StructEmbedStructSlices struct {
	StructSlices
}

// StructStructSlices 以具名字段持有含切片的类型。
type StructStructSlices struct {
	StructField StructSlices
}

// 混合了所有形态
// StructEverything 是前述所有字段形态的组合，用来在一次生成中同时覆盖
// 全部拷贝策略，并检验字段处理顺序与生成代码的可编译性。
type StructEverything struct {
	BoolField                 bool
	IntField                  int
	StringField               string
	FloatField                float64
	StructField               StructPrimitives
	EmptyStructField          StructEmpty
	ManualStructField         ManualStruct
	ManualStructAliasField    ManualStructAlias
	BoolPtrField              *bool
	IntPtrField               *int
	StringPtrField            *string
	FloatPtrField             *float64
	PrimitivePointersField    StructPrimitivePointers
	ManualStructPtrField      *ManualStruct
	ManualStructAliasPtrField *ManualStructAlias
	SliceBoolField            []bool
	SliceByteField            []byte
	SliceIntField             []int
	SliceStringField          []string
	SliceFloatField           []float64
	SlicesField               StructSlices
	SliceManualStructField    []ManualStruct
	ManualSliceField          ManualSlice
}

// An Object
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// StructExplicitObject 通过标签要求额外生成 DeepCopyObject，使其满足 otherpkg.Object。
type StructExplicitObject struct {
	x int //nolint:unused
}

// 一个以非指针形式使用的 Object
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// +k8s:deepcopy-gen:nonpointer-interfaces=true
// StructNonPointerExplicitObject 与上一个类型相同，但接口方法使用值接收者，
// 以覆盖 nonpointer-interfaces 标签的分支。
type StructNonPointerExplicitObject struct {
	x int //nolint:unused
}

// +k8s:deepcopy-gen=false
// StructTypeMeta 通过 false 标签退出生成；它用作内嵌字段，验证“内嵌类型不生成
// 方法但仍需在父类型中被拷贝”的行为。
type StructTypeMeta struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.List
// StructObjectAndList 同时为两个不同接口生成方法，返回类型不同但实现一致。
type StructObjectAndList struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// StructObjectAndObject 重复声明同一接口，验证生成器去重而不是产生重复方法。
type StructObjectAndObject struct {
}

// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/wholepkg.Selector
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/otherpkg.Object
// StructExplicitSelectorExplicitObject 为包内 Selector 与包外 Object 各生成一个方法，
// 覆盖同类型多接口、跨包接口的情形。
type StructExplicitSelectorExplicitObject struct {
	StructTypeMeta
}

// StructInterfaces 以接口类型作为字段，生成器应调用接口方法完成深拷贝。
type StructInterfaces struct {
	ObjectField    otherpkg.Object
	NilObjectField otherpkg.Object
	SelectorField  Selector
}
