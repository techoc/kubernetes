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

// 实现 ObjectKind 接口：把 GVK 写入 TypeMeta。GroupVersion 被编码到 apiVersion 字段，Kind 写入 kind 字段。
func (obj *TypeMeta) SetGroupVersionKind(gvk schema.GroupVersionKind) {
	obj.APIVersion, obj.Kind = gvk.ToAPIVersionAndKind()
}

// 实现 ObjectKind 接口：从 TypeMeta 的 apiVersion 与 kind 字段还原 GVK。apiVersion 解析失败或字段缺失时，返回值会体现相应的零值信息。
func (obj *TypeMeta) GroupVersionKind() schema.GroupVersionKind {
	return schema.FromAPIVersionAndKind(obj.APIVersion, obj.Kind)
}

// GetObjectKind 返回自身，使嵌入 TypeMeta 的类型自动获得 ObjectKind 行为。
func (obj *TypeMeta) GetObjectKind() schema.ObjectKind { return obj }
