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

package resolver

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// Combine 把 DefinitionsSchemaResolver 与一个次级 schema 解析器组合起来。
// 对 DefinitionsSchemaResolver 已知的 GVK 使用它解析，其余交给次级解析器。
// 其余交给次级解析器。
//
// 典型用法：内置类型走静态 definitions（快且无需网络），CRD 等动态类型回退到
// 基于发现（discovery）的解析器。
func (d *DefinitionsSchemaResolver) Combine(secondary SchemaResolver) SchemaResolver {
	return &combinedSchemaResolver{definitions: d, secondary: secondary}
}

// combinedSchemaResolver 按“静态优先、动态兜底”的顺序委派解析请求。
type combinedSchemaResolver struct {
	definitions *DefinitionsSchemaResolver
	secondary   SchemaResolver
}

// ResolveSchema 接受一个 GroupVersionKind (GVK)，返回其标识的 OpenAPI schema。
// 若 DefinitionsSchemaResolver 已知该 gvk，则由它解析，否则由次级解析器处理。
//
// 否则由次级解析器处理。
func (r *combinedSchemaResolver) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	// 直接查 gvkToRef 判断是否已知，避免先解析失败再回退带来的额外错误处理路径。
	if _, ok := r.definitions.gvkToRef[gvk]; ok {
		return r.definitions.ResolveSchema(gvk)
	}
	return r.secondary.ResolveSchema(gvk)
}
