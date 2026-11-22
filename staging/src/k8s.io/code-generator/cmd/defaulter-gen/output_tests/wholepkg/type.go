/*
Copyright 2018 The Kubernetes Authors.

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

import (
	"k8s.io/code-generator/cmd/defaulter-gen/output_tests/empty"
)

// 仅包含基本类型
// Only primitives
type StructPrimitives struct {
	empty.TypeMeta
	BoolField   *bool
	IntField    *int
	StringField *string
	FloatField  *float64
}

// StructPrimitivesAlias 是 StructPrimitives 的别名类型，用于验证
// 默认值逻辑能否穿透 typedef。
type StructPrimitivesAlias StructPrimitives

// StructStructPrimitives 内嵌一个具名结构体，验证嵌套默认值的递归调用。
type StructStructPrimitives struct {
	empty.TypeMeta
	StructField StructPrimitives
}

// Pointer 汇集各种指针/值组合，验证默认值生成对指针语义的处理。
// Pointer
type StructPointer struct {
	empty.TypeMeta
	PointerStructPrimitivesField             StructPrimitives
	PointerPointerStructPrimitivesField      *StructPrimitives
	PointerStructPrimitivesAliasField        StructPrimitivesAlias
	PointerPointerStructPrimitivesAliasField StructPrimitivesAlias
	PointerStructStructPrimitives            StructStructPrimitives
	PointerPointerStructStructPrimitives     *StructStructPrimitives
}

// Slices 覆盖元素为值与指针、结构体与别名的 slice 字段。
// Slices
type StructSlices struct {
	empty.TypeMeta
	SliceStructPrimitivesField             []StructPrimitives
	SlicePointerStructPrimitivesField      []*StructPrimitives
	SliceStructPrimitivesAliasField        []StructPrimitivesAlias
	SlicePointerStructPrimitivesAliasField []*StructPrimitivesAlias
	SliceStructStructPrimitives            []StructStructPrimitives
	SlicePointerStructStructPrimitives     []*StructStructPrimitives
}

// Everything 是上述各类字段的并集，作为主要测试入口。
// Everything
type StructEverything struct {
	empty.TypeMeta
	BoolPtrField       *bool
	IntPtrField        *int
	StringPtrField     *string
	FloatPtrField      *float64
	PointerStructField StructPointer
	SliceBoolField     []bool
	SliceByteField     []byte
	SliceIntField      []int
	SliceStringField   []string
	SliceFloatField    []float64
	SlicesStructField  StructSlices
}
