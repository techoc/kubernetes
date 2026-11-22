/*
Copyright 2021 The Kubernetes Authors.

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

// Package v1 提供 meta/v1 的 apply 配置类型（ApplyConfiguration）。apply 配置是
// 对服务端对象的部分声明式描述的强类型视图，配合 Server-Side Apply 使用。
package v1

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/managedfields"
	"k8s.io/client-go/discovery"
	"k8s.io/kube-openapi/pkg/util/proto"
	"sigs.k8s.io/structured-merge-diff/v6/typed"
)

// openAPISchemaTTL is how frequently we need to check
// whether the open API schema has changed or not.
// 之所以不在每次提取时都重新解析，是因为 OpenAPI 文档可能相当大，而 schema 的变化频率
// 远低于字段提取请求；一分钟的 TTL 是对陈旧度与开销的折中。
const openAPISchemaTTL = time.Minute

// UnstructuredExtractor enables extracting the applied configuration state from object for fieldManager into an
// unstructured object type.
//
// 提取结果是某个字段管理者（fieldManager）当前“拥有”的字段集合，即其 apply 配置的
// 反推视图。提取依赖 OpenAPI schema 来确定哪些字段可原子/粒度的归属，因此需要 discovery 客户端。
type UnstructuredExtractor interface {
	// Extract 返回指定 fieldManager 在对象上的 apply 配置（不含 status 子资源）。
	Extract(object *unstructured.Unstructured, fieldManager string) (*unstructured.Unstructured, error)
	// ExtractStatus 返回指定 fieldManager 在 status 子资源上的 apply 配置。
	ExtractStatus(object *unstructured.Unstructured, fieldManager string) (*unstructured.Unstructured, error)
}

// gvkParserCache caches the GVKParser in order to prevent from having to repeatedly
// parse the models from the open API schema when the schema itself changes infrequently.
//
// 缓存按 TTL 惰性刷新：只有在调用 objectTypeForGVK 且距上次检查超过 openAPISchemaTTL 时
// 才会重新拉取并解析 schema。加锁保证并发提取时不会同时重建解析器。
type gvkParserCache struct {
	// discoveryClient is the client for retrieving the openAPI document and checking
	// whether the document has changed recently
	discoveryClient discovery.DiscoveryInterface
	// mu protects the gvkParser
	// 同时保护 lastChecked，避免并发的 TTL 判断导致重复重建。
	mu sync.Mutex
	// gvkParser retrieves the objectType for a given gvk
	gvkParser *managedfields.GvkParser
	// lastChecked is the last time we checked if the openAPI doc has changed.
	lastChecked time.Time
}

// regenerateGVKParser builds the parser from the raw OpenAPI schema.
//
// 先拉取 OpenAPI 文档，再转换为 proto 模型树，最后构建可按键查找的 GVK 解析器。
// 第二步返回的模型一旦确定，后续提取不再依赖网络。
func regenerateGVKParser(dc discovery.DiscoveryInterface) (*managedfields.GvkParser, error) {
	doc, err := dc.OpenAPISchema()
	if err != nil {
		return nil, err
	}

	models, err := proto.NewOpenAPIData(doc)
	if err != nil {
		return nil, err
	}

	return managedfields.NewGVKParser(models, false)
}

// objectTypeForGVK retrieves the typed.ParseableType for a given gvk from the cache
//
// 这里采用“先更新 lastChecked 再重建”的顺序：即使重建失败，也会等到下一个 TTL 周期
// 再重试，避免在服务端持续不可用时每个请求都打满 discovery 接口。
func (c *gvkParserCache) objectTypeForGVK(gvk schema.GroupVersionKind) (*typed.ParseableType, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// if the ttl on the openAPISchema has expired,
	// regenerate the gvk parser
	if time.Since(c.lastChecked) > openAPISchemaTTL {
		c.lastChecked = time.Now()
		parser, err := regenerateGVKParser(c.discoveryClient)
		if err != nil {
			return nil, err
		}
		c.gvkParser = parser
	}
	return c.gvkParser.Type(gvk), nil
}

// extractor 是 UnstructuredExtractor 的实现，通过共享的解析器缓存避免重复解析 schema。
type extractor struct {
	cache *gvkParserCache
}

// NewUnstructuredExtractor creates the extractor with which you can extract the applied configuration
// for a given manager from an unstructured object.
//
// 构造时立即构建一次解析器，使 schema 拉取失败在创建阶段就暴露出来，而不是等到首次提取。
func NewUnstructuredExtractor(dc discovery.DiscoveryInterface) (UnstructuredExtractor, error) {
	parser, err := regenerateGVKParser(dc)
	if err != nil {
		return nil, fmt.Errorf("failed generating initial GVK Parser: %v", err)
	}
	return &extractor{
		cache: &gvkParserCache{
			gvkParser:       parser,
			discoveryClient: dc,
		},
	}, nil
}

// Extract extracts the applied configuration owned by fieldManager from an unstructured object.
// Note that the apply configuration itself is also an unstructured object.
func (e *extractor) Extract(object *unstructured.Unstructured, fieldManager string) (*unstructured.Unstructured, error) {
	return e.extractUnstructured(object, fieldManager, "")
}

// ExtractStatus is the same as ExtractUnstructured except
// that it extracts the status subresource applied configuration.
// Experimental!
func (e *extractor) ExtractStatus(object *unstructured.Unstructured, fieldManager string) (*unstructured.Unstructured, error) {
	return e.extractUnstructured(object, fieldManager, "status")
}

// extractUnstructured 是 Extract 与 ExtractStatus 的公共实现；subresource 为空表示主资源。
func (e *extractor) extractUnstructured(object *unstructured.Unstructured, fieldManager string, subresource string) (*unstructured.Unstructured, error) {
	// 提取的表单由对象的 GVK 决定，先从缓存中取到对应的 schema 类型。
	gvk := object.GetObjectKind().GroupVersionKind()
	objectType, err := e.cache.objectTypeForGVK(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the objectType: %v", err)
	}
	// 向新对象中回填元数据，使返回的 apply 配置本身可被识别和定位。
	result := &unstructured.Unstructured{}
	err = managedfields.ExtractInto(object, *objectType, fieldManager, result, subresource) //nolint:forbidigo
	if err != nil {
		return nil, fmt.Errorf("failed calling ExtractInto for unstructured: %v", err)
	}
	result.SetName(object.GetName())
	result.SetNamespace(object.GetNamespace())
	result.SetKind(object.GetKind())
	result.SetAPIVersion(object.GetAPIVersion())
	return result, nil
}
