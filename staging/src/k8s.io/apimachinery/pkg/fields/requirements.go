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

package fields

import "k8s.io/apimachinery/pkg/selection"

// Requirements 是各 Requirement 的逻辑与（AND）：所有条件都满足才算匹配。
type Requirements []Requirement

// Requirement 描述字段选择器中的一个条件：字段、值以及连接二者的操作符。
// 目前用于读取字段选择器的内部选择信息。
type Requirement struct {
	// Operator 是作用在 Field 与 Value 上的比较操作符。
	Operator selection.Operator
	// Field 是条件所针对的字段选择器路径。
	Field string
	// Value 是参与比较的字段值。
	Value string
}
