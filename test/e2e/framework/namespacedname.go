/*
Copyright 2023 The Kubernetes Authors.

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

package framework

// NamespacedName 由资源名称和必需的 namespace 组成，
// 渲染为 "<namespace>/<name>"。它实现了 NamedObject，因此可
// 作为函数参数代替完整的 API 对象。
type NamespacedName struct {
	Namespace string
	Name      string
}

var _ NamedObject = NamespacedName{}

// NamedObject 是 metav1.Object 的子集，提供对对象
// 名称和 namespace 的只读访问。
type NamedObject interface {
	GetNamespace() string
	GetName() string
}

// GetNamespace 实现 NamedObject。
func (n NamespacedName) GetNamespace() string {
	return n.Namespace
}

// GetName 实现 NamedObject。
func (n NamespacedName) GetName() string {
	return n.Name
}

// String 返回通用的字符串表示
func (n NamespacedName) String() string {
	return n.Namespace + "/" + n.Name
}
