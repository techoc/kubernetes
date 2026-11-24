/*
Copyright 2015 The Kubernetes Authors.

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

package types

// NamespacedName 表示带命名空间的资源名称，字符串形式为 "<namespace>/<name>"。
// 使用独立类型可以避免 UID、带命名空间的名称和集群级名称在代码中混用。
// 通常 namespace 和 name 已在 API 入口处完成格式校验，因此本类型不重复校验；
// 如果调用场景没有经过入口校验（例如测试代码），可使用本包 testing.go 中的 NamespacedNameOrDie()。

type NamespacedName struct {
	// Namespace 是对象所在的命名空间。
	Namespace string
	Name      string
}

const (
	// Separator 是 String() 中 namespace 与 name 的分隔符。
	Separator = '/'
)

// String 返回通用字符串表示，格式为 namespace/name。
func (n NamespacedName) String() string {
	return n.Namespace + string(Separator) + n.Name
}

// MarshalLog 返回包含 name 和 namespace 键值对的结构体，供结构化日志记录使用。
//
// 实现 logr.Marshaler，使日志中输出结构化的 name/namespace，
// 而不是退化为对整个结构体的 %+v。
func (n NamespacedName) MarshalLog() interface{} {
	return struct {
		Name      string `json:"name"`
		Namespace string `json:"namespace,omitempty"`
	}{
		Name:      n.Name,
		Namespace: n.Namespace,
	}
}
