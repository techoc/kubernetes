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

package selection

// Operator 表示键或字段与一个或多个值之间的关系。
// 具体匹配语义参见 labels.Requirement 和 fields.Requirement。
//
// Operator 的字符串值直接出现在 label selector 语法中（如
// "env in (prod,staging)"），必须与用户的查询表达式保持一致，不能随意改动。
type Operator string

const (
	// DoesNotExist 表示键不存在；不带 value。
	DoesNotExist Operator = "!"
	// Equals 与 DoubleEquals 在语义上等价，都是精确相等。
	Equals       Operator = "="
	DoubleEquals Operator = "=="
	// In 表示键的值属于给定集合。
	In        Operator = "in"
	NotEquals Operator = "!="
	// NotIn 表示键的值不在给定集合中。
	NotIn Operator = "notin"
	// Exists 表示键存在；不带 value。
	Exists      Operator = "exists"
	GreaterThan Operator = "gt"
	LessThan    Operator = "lt"
)
