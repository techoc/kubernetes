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

// +k8s:deepcopy-gen=package

// // 本包是测试夹具，用于验证生成器的行为。
// 本包用于验证 deepcopy-gen 对 Go 类型定义（type definition）与接口别名的
// 生成行为：别名会在类型系统中被展开，生成器必须为每种写法生成正确的
// DeepCopy 实现。
package aliases

// // 注意：Go 会在类型系统中展平接口别名——接口 I 的别名 J 实际上等价于 I。
// // 要支持这些别名的深拷贝，就必须为接口及其所有别名都实现 DeepCopy 方法。
// an interface I is actually equivalent to I. So support deepcopies of those aliases, we have
// to implement all aliases of that interface.

// Foo 需要同时实现接口本身及其所有别名，否则按接口类型断言时会缺少方法。
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/aliases.Interface
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/aliases.AliasInterface
// +k8s:deepcopy-gen:interfaces=k8s.io/code-generator/cmd/deepcopy-gen/output_tests/aliases.AliasAliasInterface
type Foo struct {
	X int
}

// Interface 列出各层别名接口要求的方法，确保生成器覆盖全部别名组合。
type Interface interface {
	DeepCopyInterface() Interface
	DeepCopyAliasInterface() AliasInterface
	DeepCopyAliasAliasInterface() AliasAliasInterface
}

// 以下几组类型分别覆盖内置类型、切片、指针、结构体与 map 的定义与别名，
// 用于对比生成结果。Generated deepcopy 必须按底层类型逐层复制。
type Builtin int
type Slice []int
type Pointer *int
type PointerAlias *Builtin
type Struct Foo
type Map map[string]int

// Foo 的各类容器别名：验证值类型别名与容器类型的组合。
type FooAlias Foo
type FooSlice []Foo
type FooPointer *Foo
type FooMap map[string]Foo

// 内置类型/容器类型的别名，进一步验证类型定义链。
type AliasBuiltin Builtin
type AliasSlice Slice
type AliasPointer Pointer
type AliasStruct Struct
type AliasMap Map

// 接口别名：由于 Go 会展开接口别名，这些都等价于 Interface，
// 生成器需为其实现同样的 DeepCopy 方法。
type AliasInterface Interface
type AliasAliasInterface AliasInterface
type AliasInterfaceMap map[string]AliasInterface
type AliasInterfaceSlice []AliasInterface

// Aliases
// Ttest 汇总上述所有别名作为字段，一旦生成器对某类别名处理错误，
// 该结构体的 deepcopy 测试就会失败。
type Ttest struct {
	Builtin      Builtin
	Slice        Slice
	Pointer      Pointer
	PointerAlias PointerAlias
	Struct       Struct
	Map          Map
	SliceSlice   []Slice
	MapSlice     map[string]Slice

	FooAlias   FooAlias
	FooSlice   FooSlice
	FooPointer FooPointer
	FooMap     FooMap

	AliasBuiltin AliasBuiltin
	AliasSlice   AliasSlice
	AliasPointer AliasPointer
	AliasStruct  AliasStruct
	AliasMap     AliasMap

	AliasInterface      AliasInterface
	AliasAliasInterface AliasAliasInterface
	AliasInterfaceMap   AliasInterfaceMap
	AliasInterfaceSlice AliasInterfaceSlice
}
