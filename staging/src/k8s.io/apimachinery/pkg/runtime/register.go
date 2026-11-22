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

package runtime

import "k8s.io/apimachinery/pkg/runtime/schema"

// SetGroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta
// 把 GVK 拆成 apiVersion 与 kind 写入；这两个字段是 GVK 在序列化格式中的表示。
func (obj *TypeMeta) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// GroupVersionKind satisfies the ObjectKind interface for all objects that embed TypeMeta
// 从 apiVersion 与 kind 重建 GVK；字段为空时得到零值 GVK。
func (obj *TypeMeta) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}

// GetObjectKind 返回自身，使嵌入 TypeMeta 的类型自动获得 ObjectKind 行为。
func (obj *TypeMeta) GetObjectKind() schema.ObjectKind { return obj }
