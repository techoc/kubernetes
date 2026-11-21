/*
Copyright 2017 The Kubernetes Authors.

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

//TODO: consider making these methods functions, because we don't want helper
//functions in the k8s.io/api repo.

package core

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// SetGroupVersionKind 把传入的 GVK 分解后写入对象引用的 APIVersion 与 Kind 字段。
func (obj *ObjectReference) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// GroupVersionKind 从对象引用的 APIVersion 与 Kind 字段还原出 GVK。
func (obj *ObjectReference) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}

// GetObjectKind 返回对象引用自身，使其实现 schema.ObjectKind 接口。
func (obj *ObjectReference) GetObjectKind() schema.ObjectKind { return obj }
