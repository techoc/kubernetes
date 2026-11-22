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
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/endpoints/openapi"
	"k8s.io/kube-openapi/pkg/common"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// DefinitionsSchemaResolver resolves the schema of a built-in type
// by looking up the OpenAPI definitions.
//
// gvkToRef 是预建的 GVK→定义名索引；resolver 构建后只读，可并发使用。
// 之所以预建索引，是因为 CEL 编译期可能对同一 GVK 反复解析 schema。
type DefinitionsSchemaResolver struct {
	defs     map[string]common.OpenAPIDefinition
	gvkToRef map[schema.GroupVersionKind]string
}

// NewDefinitionsSchemaResolver creates a new DefinitionsSchemaResolver.
// An example working setup:
// getDefinitions = "k8s.io/kubernetes/pkg/generated/openapi".GetOpenAPIDefinitions
// scheme         = "k8s.io/client-go/kubernetes/scheme".Scheme
//
// 需要传入 scheme，是因为定义名到 GVK 的映射依赖 scheme 中的类型注册；
// 一个定义名可能对应多个 GVK（例如同一 Go 类型被多个组使用）。
func NewDefinitionsSchemaResolver(getDefinitions common.GetOpenAPIDefinitions, schemes ...*runtime.Scheme) *DefinitionsSchemaResolver {
	gvkToRef := make(map[schema.GroupVersionKind]string)
	namer := openapi.NewDefinitionNamer(schemes...)
	defs := getDefinitions(spec.MustCreateRef)
	for name := range defs {
		_, e := namer.GetDefinitionName(name)
		gvks := extensionsToGVKs(e)
		// 同一 GVK 若被多个定义名覆盖，后写入者生效；正常情况下不会冲突。
		for _, gvk := range gvks {
			gvkToRef[gvk] = name
		}
	}
	return &DefinitionsSchemaResolver{
		gvkToRef: gvkToRef,
		defs:     defs,
	}
}

// ResolveSchema 返回给定 GVK 的完整 schema（所有 $ref 已展开）。
// 返回深拷贝的 schema，避免调用方修改包级定义缓存。
func (d *DefinitionsSchemaResolver) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	ref, ok := d.gvkToRef[gvk]
	if !ok {
		return nil, fmt.Errorf("cannot resolve %v: %w", gvk, ErrSchemaNotFound)
	}
	s, err := PopulateRefs(func(ref string) (*spec.Schema, bool) {
		// find the schema by the ref string, and return a deep copy
		def, ok := d.defs[ref]
		if !ok {
			return nil, false
		}
		s := def.Schema
		return &s, true
	}, ref)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// extensionsToGVKs 从定义扩展中解析 x-kubernetes-group-version-kind 列表。
// 任何类型断言失败都返回 nil（视为无 GVK），而不是部分结果，
// 以免把不完整的映射注入索引造成难以排查的错误。
func extensionsToGVKs(extensions spec.Extensions) []schema.GroupVersionKind {
	gvksAny, ok := extensions[extGVK]
	if !ok {
		return nil
	}
	gvks, ok := gvksAny.([]any)
	if !ok {
		return nil
	}
	result := make([]schema.GroupVersionKind, 0, len(gvks))
	for _, gvkAny := range gvks {
		// type check the map and all fields
		gvkMap, ok := gvkAny.(map[string]any)
		if !ok {
			return nil
		}
		g, ok := gvkMap["group"].(string)
		if !ok {
			return nil
		}
		v, ok := gvkMap["version"].(string)
		if !ok {
			return nil
		}
		k, ok := gvkMap["kind"].(string)
		if !ok {
			return nil
		}
		result = append(result, schema.GroupVersionKind{
			Group:   g,
			Version: v,
			Kind:    k,
		})
	}
	return result
}
