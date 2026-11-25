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

package cache

import (
	"k8s.io/apimachinery/pkg/types"
)

// ObjectName 指向某个隐含类型对象的引用（namespace + name）。
//
// 与 types.NamespacedName 字段相同但方法不同，保留本类型是为了让缓存键的
// 编码历史行为保持稳定（见 String 方法）。
type ObjectName struct {
	Namespace string
	Name      string
}

// NewObjectName 构造一个新的 ObjectName。
func NewObjectName(namespace, name string) ObjectName {
	return ObjectName{Namespace: namespace, Name: name}
}

// Parts 是构造函数的逆操作，把 ObjectName 拆回 namespace 和 name。
func (objName ObjectName) Parts() (namespace, name string) {
	return objName.Namespace, objName.Name
}

// String 返回标准的字符串编码，与 MetaNamespaceKeyFunc 的历史行为保持一致；
// 注意该行为与 types.NamespacedName 的 String 方法不同。
//
// 空 namespace 时只返回 name；非空时返回 "namespace/name"。
// 这正是 informer 缓存键的格式，改动会破坏已有缓存与测试的兼容性。
func (objName ObjectName) String() string {
	if len(objName.Namespace) > 0 {
		return objName.Namespace + "/" + objName.Name
	}
	return objName.Name
}

// ParseObjectName 尝试解析标准编码字符串。
//
// 解析规则与 SplitMetaNamespaceKey 一致，保证与 String 互逆。
func ParseObjectName(str string) (ObjectName, error) {
	var objName ObjectName
	var err error
	objName.Namespace, objName.Name, err = SplitMetaNamespaceKey(str)
	return objName, err
}

// NamespacedNameAsObjectName 把给定的 NamespacedName 重新包装为 ObjectName。
func NamespacedNameAsObjectName(nn types.NamespacedName) ObjectName {
	return NewObjectName(nn.Namespace, nn.Name)
}

// AsNamespacedName 把 ObjectName 重新包装为 NamespacedName。
func (objName ObjectName) AsNamespacedName() types.NamespacedName {
	return types.NamespacedName{Namespace: objName.Namespace, Name: objName.Name}
}
