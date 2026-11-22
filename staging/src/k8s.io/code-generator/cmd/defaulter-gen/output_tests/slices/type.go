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

package slices

import (
	"k8s.io/code-generator/cmd/defaulter-gen/output_tests/empty"
)

// Only test
// Ttest 覆盖默认值是内嵌 TypeMeta 与指针布尔字段的场景；
// 列表类型分别用值切片和指针切片，用来验证 defaulter 对两种元素形态的处理。
type Ttest struct {
	empty.TypeMeta
	BoolField *bool
}

// TtestList 值切片元素的默认值处理。
type TtestList struct {
	empty.TypeMeta
	Items []Ttest
}

// TtestPointerList 指针切片元素需要先分配再处理默认值。
type TtestPointerList struct {
	empty.TypeMeta
	Items []*Ttest
}
