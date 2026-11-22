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

// Package outputtests 驱动 deepcopy-gen 的端到端测试：
// 先运行生成器产出 zz_generated.deepcopy.go，再用 fuzz 输入校验生成结果与
// 基于反射的参考实现结果一致。各子包分别覆盖不同的类型形态。
package outputtests

import (
	"sigs.k8s.io/randfill"

	"k8s.io/code-generator/cmd/deepcopy-gen/output_tests/aliases"
	"k8s.io/code-generator/cmd/deepcopy-gen/output_tests/interfaces"
)

// interfaceFuzzers 中的 fuzzer 会把接口字段置为 nil，因为基于 JSON 的深拷贝
// 无法处理接口值（编码时会丢失具体类型）。
// 这些 fuzzer 必须在全局 fuzz 函数之前注册，从而覆盖默认生成器。
// TODO: test also interface deepcopy
var interfaceFuzzers = []interface{}{
	func(s *aliases.AliasAliasInterface, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			*s = &aliasAliasInterfaceInstance{X: c.Int()}
		}
	},
	func(s *aliases.AliasInterface, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			*s = &aliasAliasInterfaceInstance{X: c.Int()}
		}
	},
	func(s *aliases.Interface, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			*s = &aliasAliasInterfaceInstance{X: c.Int()}
		}
	},
	func(s *aliases.AliasInterfaceMap, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			// 随机生成 0~2 个条目，键值都可能为 nil，以覆盖 map 值接口的两种分支。
			*s = make(aliases.AliasInterfaceMap)
			for i := 0; i < c.Intn(3); i++ {
				if c.Bool() {
					(*s)[c.String(0)] = nil
				} else {
					(*s)[c.String(0)] = &aliasAliasInterfaceInstance{X: c.Int()}
				}
			}
		}

	},
	func(s *aliases.AliasInterfaceSlice, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			// 使用非 nil 的空切片起步，以便同时覆盖空切片与含 nil 元素的切片。
			*s = make(aliases.AliasInterfaceSlice, 0)
			for i := 0; i < c.Intn(3); i++ {
				if c.Bool() {
					*s = append(*s, nil)
				} else {
					*s = append(*s, &aliasAliasInterfaceInstance{X: c.Int()})
				}
			}
		}
	},
	func(s *interfaces.Inner, c randfill.Continue) {
		if c.Bool() {
			*s = nil
		} else {
			*s = &interfacesInnerInstance{X: c.Float64()}
		}
	},
}

// aliasAliasInterfaceInstance 是 aliases 包三个接口共用的唯一实现。
// 同一个类型同时实现多层别名接口，可以验证生成器按接口类型（而非具体类型）
// 选择正确的 DeepCopyXxx 方法。
type aliasAliasInterfaceInstance struct {
	X int
}

// DeepCopyInterface 实现 aliases.Interface 的深拷贝。
func (i *aliasAliasInterfaceInstance) DeepCopyInterface() aliases.Interface {
	if i == nil {
		return nil
	}

	return &aliasAliasInterfaceInstance{X: i.X}
}

// DeepCopyAliasInterface 实现 aliases.AliasInterface 的深拷贝。
func (i *aliasAliasInterfaceInstance) DeepCopyAliasInterface() aliases.AliasInterface {
	if i == nil {
		return nil
	}

	return &aliasAliasInterfaceInstance{X: i.X}
}

// DeepCopyAliasAliasInterface 实现 aliases.AliasAliasInterface 的深拷贝。
func (i *aliasAliasInterfaceInstance) DeepCopyAliasAliasInterface() aliases.AliasAliasInterface {
	if i == nil {
		return nil
	}

	return &aliasAliasInterfaceInstance{X: i.X}
}

// interfacesInnerInstance 是 interfaces.Inner 的实现，含一个 float64 字段。
type interfacesInnerInstance struct {
	X float64
}

// DeepCopyInner 实现 interfaces.Inner 的深拷贝。
func (i *interfacesInnerInstance) DeepCopyInner() interfaces.Inner {
	if i == nil {
		return nil
	}

	return &interfacesInnerInstance{X: i.X}
}

// Function 是 interfaces.Inner 的普通方法，用于确认接口实现完整。
func (i *interfacesInnerInstance) Function() float64 {
	return i.X
}
