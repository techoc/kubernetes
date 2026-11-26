/*
Copyright 2020 The Kubernetes Authors.

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

package marker

import (
	"k8s.io/code-generator/cmd/defaulter-gen/output_tests/empty"
	"k8s.io/code-generator/cmd/defaulter-gen/output_tests/marker/external3"
)

// Defaulted 覆盖 +default 标记在各种字段形态下的生成结果：值类型、
// 指针、切片、map、嵌套结构体、类型别名与常量引用。
type Defaulted struct {
	empty.TypeMeta

	// +default="bar"
	StringDefault string

	// 默认值被强制为空串，显式指定默认值不会生效。
	// +default=""
	StringEmptyDefault string

	// 即使不指定 default，非 omitempty 字段仍会按零值默认化。
	StringEmpty string

	// +default="default"
	StringPointer *string

	// +default=64
	Int64 *int64

	// +default=32
	Int32 *int32

	// +default=1
	IntDefault int

	// +default=0
	IntEmptyDefault int

	// 默认值被强制为 0。
	IntEmpty int

	// +default=0.5
	FloatDefault float64

	// +default=0.0
	FloatEmptyDefault float64

	FloatEmpty float64

	// +default=["foo", "bar"]
	List []Item
	// +default={"s": "foo", "i": 5}
	Sub *SubStruct

	//+default=[{"s": "foo1", "i": 1}, {"s": "foo2"}]
	StructList []SubStruct

	//+default=[{"s": "foo1", "i": 1}, {"s": "foo2"}]
	PtrStructList []*SubStruct

	//+default=["foo"]
	StringList []string

	// 默认值被强制为空结构体。
	OtherSub SubStruct

	// +default={"foo": "bar"}
	Map map[string]Item

	// +default={"foo": {"S": "string", "I": 1}}
	StructMap map[string]SubStruct

	// +default={"foo": {"S": "string", "I": 1}}
	PtrStructMap map[string]*SubStruct

	// 此处指定的默认值会覆盖 Item 类型自身的默认值。
	// +default="banana"
	AliasPtr Item
}

// DefaultedOmitempty 与 Defaulted 相同，但所有字段带 json omitempty；
// 用于确认 omitempty 不会改变默认值的应用条件。
type DefaultedOmitempty struct {
	empty.TypeMeta `json:",omitempty"`

	// +default="bar"
	StringDefault string `json:",omitempty"`

	// 默认值被强制为空串，显式指定默认值不会生效。
	// +default=""
	StringEmptyDefault string `json:",omitempty"`

	// 即使不指定 default，非 omitempty 字段仍会按零值默认化。
	StringEmpty string `json:",omitempty"`

	// +default="default"
	StringPointer *string `json:",omitempty"`

	// +default=64
	Int64 *int64 `json:",omitempty"`

	// +default=32
	Int32 *int32 `json:",omitempty"`

	// +default=1
	IntDefault int `json:",omitempty"`

	// +default=0
	IntEmptyDefault int `json:",omitempty"`

	// 默认值被强制为 0。
	IntEmpty int `json:",omitempty"`

	// +default=0.5
	FloatDefault float64 `json:",omitempty"`

	// +default=0.0
	FloatEmptyDefault float64 `json:",omitempty"`

	FloatEmpty float64 `json:",omitempty"`

	// +default=["foo", "bar"]
	List []Item `json:",omitempty"`
	// +default={"s": "foo", "i": 5}
	Sub *SubStruct `json:",omitempty"`

	//+default=[{"s": "foo1", "i": 1}, {"s": "foo2"}]
	StructList []SubStruct `json:",omitempty"`

	//+default=[{"s": "foo1", "i": 1}, {"s": "foo2"}]
	PtrStructList []*SubStruct `json:",omitempty"`

	//+default=["foo"]
	StringList []string `json:",omitempty"`

	// 默认值被强制为空结构体。
	OtherSub SubStruct `json:",omitempty"`

	// +default={"foo": "bar"}
	Map map[string]Item `json:",omitempty"`

	// +default={"foo": {"S": "string", "I": 1}}
	StructMap map[string]SubStruct `json:",omitempty"`

	// +default={"foo": {"S": "string", "I": 1}}
	PtrStructMap map[string]*SubStruct `json:",omitempty"`

	// 此处指定的默认值会覆盖 Item 类型自身的默认值。
	// +default="banana"
	AliasPtr Item `json:",omitempty"`
}

// SomeDefault 被 ref(SomeDefault) 引用，生成代码应输出其常量标识符
// 而不是内联字符串。
const SomeDefault = "ACoolConstant"

// Item 是最简单的默认值示例：指针别名默认指向常量。
// +default="apple"
type Item *string

// ValueItem 是底层为 string 的类型，供常量引用测试使用。
type ValueItem string

// DefaultedValueItem 在类型级声明默认值，只有通过指针间接引用时才会生效。
// +default=ref(SomeValue)
type DefaultedValueItem ValueItem
type PointerValueItem *DefaultedValueItem

// ItemDefaultWiped 演示别名会“抹掉”嵌套类型的默认值。
type ItemDefaultWiped Item

// SomeValue 是被 ref(...) 引用的常量。
const SomeValue ValueItem = "Value"

// SubStruct 的 I 字段带 omitempty，用于验证嵌套结构体默认值的独立性。
type SubStruct struct {
	S string
	// +default=1
	I int `json:"I,omitempty"`
}

// DefaultedWithFunction 在没有显式字段默认值逻辑时仍应输出默认值赋值。
type DefaultedWithFunction struct {
	empty.TypeMeta
	// +default="default_marker"
	S1 string `json:"S1,omitempty"`
	// +default="default_marker"
	S2 string `json:"S2,omitempty"`
}

// DefaultedWithReference 集中测试 ref(...) 符号引用的各种边界：
// 同名包、完全限定名、别名转换与指针构造。
type DefaultedWithReference struct {
	empty.TypeMeta

	// 别名的底层类型是指针时，默认值若可转换为该指针类型，仍可正常使用。
	// +default=ref(SomeValue)
	AliasConvertDefaultPointer PointerValueItem

	// 嵌套类型上的默认值不会穿透别名生效。
	AliasWipedDefault ItemDefaultWiped

	// 定义在指针别名上的默认值可以生效。
	PointerAliasDefault Item

	// 别名可以是指向常量类型的指针。
	// +default=ref(SomeDefault)
	AliasPointerInside Item

	// 可以覆盖别名上指定的默认值。
	// +default=ref(SomeDefault)
	AliasOverride Item

	// 类型级默认值只有在指针场景下才生效。
	AliasNonPointerDefault DefaultedValueItem `json:",omitempty"`

	// 类型级默认值只有在指针场景下才生效。
	AliasPointerDefault *DefaultedValueItem

	// 别名可以是值类型。
	// +default=ref(SomeValue)
	AliasNonPointer ValueItem `json:",omitempty"`

	// 可以有指向别名的指针，而该别名的默认值是非指针值。
	// +default=ref(SomeValue)
	AliasPointer *ValueItem `json:",omitempty"`

	// ref() 的基本用法。
	// +default=ref(SomeDefault)
	SymbolReference string `json:",omitempty"`

	// +default=ref(k8s.io/code-generator/cmd/defaulter-gen/output_tests/marker/external.AConstant)
	SameNamePackageSymbolReference1 string `json:",omitempty"`

	// +default=ref(k8s.io/code-generator/cmd/defaulter-gen/output_tests/marker/external/external.AnotherConstant)
	SameNamePackageSymbolReference2 string `json:",omitempty"`

	// 默认值先由 ValueItem 转成 string，再经 addressOf 与类型转换提升到 B4。
	// +default=ref(SomeValue)
	PointerConversion *B4

	// +default=ref(SomeValue)
	PointerConversionValue B4

	// +default=ref(k8s.io/code-generator/cmd/defaulter-gen/output_tests/marker.SomeValue)
	FullyQualifiedLocalSymbol string

	// 构造 external3.StringPointer 需要导入 external2，验证生成器能正确处理跨包引用。
	// +default=ref(SomeValue)
	ImportFromAliasCast external3.StringPointer
}

// 多层别名与多重指针混合的复杂层级，并包含兄弟类型。
// B0..B4 逐层叠加指针/别名，用于验证生成代码在多层指针转换下的
// 取址与类型转换链。
type B0 *string
type B1 B0
type B2 *B1
type B3 ****B2
type B4 **B3
