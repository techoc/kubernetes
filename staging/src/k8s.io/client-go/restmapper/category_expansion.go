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

// Package restmapper 将 REST 资源映射到 Go 类型，并支持类别（category）
// 与短名等发现信息的解析。
package restmapper

import (
	"context"
	"iter"
	"slices"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
)

// CategoryExpander maps category strings to GroupResources.
// Categories are classification or 'tag' of a group of resources.
//
// CategoryExpanderWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use CategoryExpanderWithContext instead.
//
// 旧接口无法传递 ctx，新代码应直接使用 CategoryExpanderWithContext。
type CategoryExpander interface {
	Expand(category string) ([]schema.GroupResource, bool)
}

// CategoryExpanderWithContext maps category strings to GroupResources.
// Categories are classification or 'tag' of a group of resources.
//
// 返回的 bool 表示该类别是否被识别，而不是结果是否非空。
type CategoryExpanderWithContext interface {
	ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool)
}

// ToCategoryExpanderWithContext 把旧接口适配为新接口。
// 若入参已经实现新接口则直接复用，避免多层包装。
func ToCategoryExpanderWithContext(c CategoryExpander) CategoryExpanderWithContext {
	if c == nil {
		return nil
	}
	if c, ok := c.(CategoryExpanderWithContext); ok {
		return c
	}
	return &categoryExpanderWrapper{
		delegate: c,
	}
}

// categoryExpanderWrapper 丢弃 ctx，仅为了兼容旧实现。
type categoryExpanderWrapper struct {
	delegate CategoryExpander
}

// ExpandWithContext 忽略 ctx，直接调用底层 Expand。
func (c *categoryExpanderWrapper) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	return c.delegate.Expand(category)
}

// SimpleCategoryExpander implements CategoryExpander and CategoryExpanderWithContext interface
// using a static mapping of categories to GroupResource mapping.
//
// 适合测试或小规模静态映射；真实类别应通过发现客户端动态获取。
type SimpleCategoryExpander struct {
	Expansions map[string][]schema.GroupResource
}

var (
	_ CategoryExpander            = SimpleCategoryExpander{}
	_ CategoryExpanderWithContext = SimpleCategoryExpander{}
)

// Expand fulfills CategoryExpander
func (e SimpleCategoryExpander) Expand(category string) ([]schema.GroupResource, bool) {
	ret, ok := e.Expansions[category]
	return ret, ok
}

// ExpandWithContext fulfills CategoryExpanderWithContext
func (e SimpleCategoryExpander) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	return e.Expand(category)
}

// discoveryCategoryExpander struct lets a REST Client wrapper (discoveryClient) to retrieve list of APIResourceList,
// and then convert to fallbackExpander
//
// 每次展开都实时查询发现信息，未做缓存；调用方需自行控制频率。
type discoveryCategoryExpander struct {
	discoveryClient discovery.DiscoveryInterfaceWithContext
}

// NewDiscoveryCategoryExpander returns a category expander that makes use of the "categories" fields from
// the API, found through the discovery client. In case of any error or no category found (which likely
// means we're at a cluster prior to categories support, fallback to the expander provided.
//
// NewDiscoveryCategoryExpanderWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use NewDiscoveryCategoryExpanderWithContext instead.
func NewDiscoveryCategoryExpander(client discovery.DiscoveryInterface) CategoryExpander {
	return newDiscoveryCategoryExpander(discovery.ToDiscoveryInterfaceWithContext(client))
}

// NewDiscoveryCategoryExpanderWithContext returns a category expander that makes use of the "categories" fields from
// the API, found through the discovery client. In case of any error or no category found (which likely
// means we're at a cluster prior to categories support, fallback to the expander provided.
func NewDiscoveryCategoryExpanderWithContext(client discovery.DiscoveryInterfaceWithContext) CategoryExpanderWithContext {
	return newDiscoveryCategoryExpander(client)
}

// newDiscoveryCategoryExpander 构造基于发现客户端的展开器；client 为 nil 属于编程错误，
// 直接 panic 以便尽早暴露配置问题。
func newDiscoveryCategoryExpander(client discovery.DiscoveryInterfaceWithContext) discoveryCategoryExpander {
	if client == nil {
		panic("Please provide discovery client to shortcut expander")
	}
	return discoveryCategoryExpander{discoveryClient: client}
}

// Expand fulfills CategoryExpander
//
// ExpandWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ExpandWithContext instead.
func (e discoveryCategoryExpander) Expand(category string) ([]schema.GroupResource, bool) {
	return e.ExpandWithContext(context.Background(), category)
}

// ExpandWithContext fulfills CategoryExpanderWithContext
func (e discoveryCategoryExpander) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	// Get all supported resources for groups and versions from server, if no resource found, fallback anyway.
	// 发现请求的错误被有意忽略：调用方约定在无结果时回退到其他展开器。
	_, apiResourceLists, _ := e.discoveryClient.ServerGroupsAndResourcesWithContext(ctx)
	if len(apiResourceLists) == 0 {
		return nil, false
	}

	// 按类别建立索引；同一资源可能属于多个类别，因此是 1:N 关系。
	discoveredExpansions := map[string][]schema.GroupResource{}
	for _, apiResourceList := range apiResourceLists {
		gv, err := schema.ParseGroupVersion(apiResourceList.GroupVersion)
		if err != nil {
			continue
		}
		// Collect GroupVersions by categories
		for _, apiResource := range apiResourceList.APIResources {
			if categories := apiResource.Categories; len(categories) > 0 {
				for _, category := range categories {
					groupResource := schema.GroupResource{
						Group:    gv.Group,
						Resource: apiResource.Name,
					}
					discoveredExpansions[category] = append(discoveredExpansions[category], groupResource)
				}
			}
		}
	}

	ret, ok := discoveredExpansions[category]
	return ret, ok
}

// UnionCategoryExpander implements CategoryExpander interface.
// It maps given category string to union of expansions returned by all the CategoryExpanders in the list.
//
// UnionCategoryExpanderWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use UnionCategoryExpanderWithContext instead.
//
// 展开结果按各展开器的顺序合并并去重。
type UnionCategoryExpander []CategoryExpander

var _ CategoryExpander = UnionCategoryExpander{}

// Expand fulfills CategoryExpander
func (u UnionCategoryExpander) Expand(category string) ([]schema.GroupResource, bool) {
	// 旧接口没有 ctx，只能退化为 Background；同时把每个旧展开器适配成上下文版本。
	return unionExpand(context.Background(), func(yield func(i int, expander CategoryExpanderWithContext) bool) {
		for i, expander := range u {
			if !yield(i, ToCategoryExpanderWithContext(expander)) {
				break
			}
		}
	}, category)
}

// UnionCategoryExpanderWithContext implements CategoryExpanderWithContext interface.
// It maps given category string to union of expansions returned by all the CategoryExpanders in the list.
type UnionCategoryExpanderWithContext []CategoryExpanderWithContext

var _ CategoryExpanderWithContext = UnionCategoryExpanderWithContext{}

// ExpandWithContext fulfills CategoryExpanderWithContext
func (u UnionCategoryExpanderWithContext) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	return unionExpand(ctx, slices.All(u), category)
}

// unionExpand 合并多个展开器的结果：保序、去重，任一展开器识别该类别即返回 true。
// 去重使用线性扫描而非 set，因为结果规模通常很小，避免额外的分配。
func unionExpand(ctx context.Context, expanders iter.Seq2[int, CategoryExpanderWithContext], category string) ([]schema.GroupResource, bool) {
	ret := []schema.GroupResource{}
	ok := false

	// Expand the category for each CategoryExpander in the list and merge/combine the results.
	for _, expansion := range expanders {
		curr, currOk := expansion.ExpandWithContext(ctx, category)

		for _, currGR := range curr {
			found := false
			for _, existing := range ret {
				if existing == currGR {
					found = true
					break
				}
			}
			if !found {
				ret = append(ret, currGR)
			}
		}
		ok = ok || currOk
	}

	return ret, ok
}
