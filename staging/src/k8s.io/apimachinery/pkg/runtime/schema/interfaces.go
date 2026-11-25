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

package schema

// ObjectKind 所有被序列化的对象都会在 Scheme 中编码类型信息。序列化过程通过该接口
// 把 Scheme 中的类型信息写入对象的序列化形式。对于无法序列化或有特殊
// 需求的对象，该接口可以是空实现。
type ObjectKind interface {
	// SetGroupVersionKind 设置或清除对象期望序列化的 kind。
	// 传入 nil 的效果等价于清除当前设置。
	SetGroupVersionKind(kind GroupVersionKind)
	// GroupVersionKind 返回对象存储的 group、version、kind；
	// 若对象不暴露这些字段，则返回空结构体。
	GroupVersionKind() GroupVersionKind
}

// EmptyObjectKind 实现 ObjectKind 接口，所有方法均为空操作。
var EmptyObjectKind = emptyObjectKind{}

type emptyObjectKind struct{}

// SetGroupVersionKind 实现 ObjectKind 接口（空操作）。
func (emptyObjectKind) SetGroupVersionKind(gvk GroupVersionKind) {}

// GroupVersionKind 实现 ObjectKind 接口（始终返回空值）。
func (emptyObjectKind) GroupVersionKind() GroupVersionKind { return GroupVersionKind{} }
