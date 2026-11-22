/*
Copyright 2019 The Kubernetes Authors.

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

package v1

import (
	"k8s.io/apimachinery/pkg/runtime"
)

// DeepCopy 手写而非生成，因为 TableRow.Cells 是 []interface{}，无法用生成的
// 类型化代码深拷贝；这里借助 runtime.DeepCopyJSONValue 只复制 JSON 兼容的值。
// 其他字段（Conditions、Object）仍复用各自生成的 DeepCopy 逻辑。
func (in *TableRow) DeepCopy() *TableRow {
	if in == nil {
		return nil
	}

	out := new(TableRow)

	if in.Cells != nil {
		out.Cells = make([]interface{}, len(in.Cells))
		for i := range in.Cells {
			// Cells 只允许 JSON 可表示的值；遇到其他类型会 panic，
			// 这是 Table 序列化契约的一部分。
			out.Cells[i] = runtime.DeepCopyJSONValue(in.Cells[i])
		}
	}

	if in.Conditions != nil {
		out.Conditions = make([]TableRowCondition, len(in.Conditions))
		for i := range in.Conditions {
			in.Conditions[i].DeepCopyInto(&out.Conditions[i])
		}
	}

	// RawExtension 的深拷贝由生成代码处理，可能包含任意对象。
	in.Object.DeepCopyInto(&out.Object)
	return out
}
