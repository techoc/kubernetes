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
	"encoding/json"
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/validation/spec"
)

// ClientDiscoveryResolver 使用 client-go discovery 在运行时解析 schema。
//
// 相比内置定义，它能解析 CRD 等非内置类型，代价是每次解析要走一次
// OpenAPI v3 文档拉取（通常由 discovery 客户端缓存）。
type ClientDiscoveryResolver struct {
	Discovery discovery.DiscoveryInterface
}

var _ SchemaResolver = (*ClientDiscoveryResolver)(nil)

// ResolveSchema 拉取 OpenAPI v3 文档，先按 group/version 定位路径条目，
// 再在 components.schemas 中通过 x-kubernetes-group-version-kind 扩展找到具体 GVK。
func (r *ClientDiscoveryResolver) ResolveSchema(gvk schema.GroupVersionKind) (*spec.Schema, error) {
	p, err := r.Discovery.OpenAPIV3().Paths()
	if err != nil {
		return nil, err
	}
	// OpenAPI v3 以 "api/<version>" 或 "apis/<group>/<version>" 为路径键。
	resourcePath := resourcePathFromGV(gvk.GroupVersion())
	c, ok := p[resourcePath]
	if !ok {
		return nil, fmt.Errorf("cannot resolve group version %q: %w", gvk.GroupVersion(), ErrSchemaNotFound)
	}
	b, err := c.Schema(runtime.ContentTypeJSON)
	if err != nil {
		return nil, err
	}
	resp := new(schemaResponse)
	err = json.Unmarshal(b, resp)
	if err != nil {
		return nil, err
	}
	ref, err := resolveRef(resp, gvk)
	if err != nil {
		return nil, err
	}
	// 以组件内引用为起点展开所有 $ref，得到自包含的 schema。
	s, err := PopulateRefs(func(ref string) (*spec.Schema, bool) {
		s, ok := resp.Components.Schemas[strings.TrimPrefix(ref, refPrefix)]
		return s, ok
	}, ref)
	if err != nil {
		return nil, err
	}
	return s, nil
}

// resolveRef 遍历所有组件 schema，返回 GVK 扩展匹配的第一个引用。
// 找不到时返回包装了 ErrSchemaNotFound 的错误，便于调用方判定是否需要回退。
func resolveRef(resp *schemaResponse, gvk schema.GroupVersionKind) (string, error) {
	for ref, s := range resp.Components.Schemas {
		var gvks []schema.GroupVersionKind
		err := s.Extensions.GetObject(extGVK, &gvks)
		if err != nil {
			return "", err
		}
		for _, g := range gvks {
			if g == gvk {
				return ref, nil
			}
		}
	}
	return "", fmt.Errorf("cannot resolve group version kind %q: %w", gvk, ErrSchemaNotFound)
}

// resourcePathFromGV 生成 OpenAPI v3 中 group/version 对应的路径前缀。
// core 组没有 group 段，因此是 "api/<version>"。
func resourcePathFromGV(gv schema.GroupVersion) string {
	var resourcePath string
	if len(gv.Group) == 0 {
		resourcePath = fmt.Sprintf("api/%s", gv.Version)
	} else {
		resourcePath = fmt.Sprintf("apis/%s/%s", gv.Group, gv.Version)
	}
	return resourcePath
}

// schemaResponse 只解析 OpenAPI v3 文档中需要的 components.schemas 部分，
// 避免把整个文档反序列化成大对象。
type schemaResponse struct {
	Components struct {
		Schemas map[string]*spec.Schema `json:"schemas"`
	} `json:"components"`
}

const refPrefix = "#/components/schemas/"

const extGVK = "x-kubernetes-group-version-kind"
