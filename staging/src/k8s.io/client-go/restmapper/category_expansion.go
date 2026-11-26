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

// CategoryExpander 把类别字符串映射到 GroupResource 集合。类别是对一组资源的分类或标签。
//
// CategoryExpanderWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 CategoryExpanderWithContext。
//
// 旧接口无法传递 ctx，新代码应直接使用 CategoryExpanderWithContext。
type CategoryExpander interface {
	Expand(category string) ([]schema.GroupResource, bool)
}

// CategoryExpanderWithContext 把类别字符串映射到 GroupResource 集合。类别是对一组资源的分类或标签。
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

// SimpleCategoryExpander 用静态的类别到 GroupResource 映射实现 CategoryExpander
// 与 CategoryExpanderWithContext 接口。
//
// 适合测试或小规模静态映射；真实类别应通过发现客户端动态获取。
type SimpleCategoryExpander struct {
	Expansions map[string][]schema.GroupResource
}

var (
	_ CategoryExpander            = SimpleCategoryExpander{}
	_ CategoryExpanderWithContext = SimpleCategoryExpander{}
)

// Expand 实现 CategoryExpander。
func (e SimpleCategoryExpander) Expand(category string) ([]schema.GroupResource, bool) {
	ret, ok := e.Expansions[category]
	return ret, ok
}

// ExpandWithContext 实现 CategoryExpanderWithContext。
func (e SimpleCategoryExpander) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	return e.Expand(category)
}

// discoveryCategoryExpander 让 REST Client 包装层（discoveryClient）获取 APIResourceList
// 列表，再转换为 fallbackExpander。
//
// 每次展开都实时查询发现信息，未做缓存；调用方需自行控制频率。
type discoveryCategoryExpander struct {
	discoveryClient discovery.DiscoveryInterfaceWithContext
}

// NewDiscoveryCategoryExpander 返回基于 discovery 客户端中 API "categories" 字段的类别
// 展开器。出错或找不到类别（通常意味着集群版本尚不支持类别）时回退到传入的展开器。
// NewDiscoveryCategoryExpanderWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 NewDiscoveryCategoryExpanderWithContext。
func NewDiscoveryCategoryExpander(client discovery.DiscoveryInterface) CategoryExpander {
	return newDiscoveryCategoryExpander(discovery.ToDiscoveryInterfaceWithContext(client))
}

// NewDiscoveryCategoryExpanderWithContext 返回基于 discovery 客户端中 API "categories" 字段的
// 类别展开器。出错或找不到类别（通常意味着集群版本尚不支持类别）时回退到传入的展开器。
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

// Expand 实现 CategoryExpander。
//
// ExpandWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 ExpandWithContext。
func (e discoveryCategoryExpander) Expand(category string) ([]schema.GroupResource, bool) {
	return e.ExpandWithContext(context.Background(), category)
}

// ExpandWithContext 实现 CategoryExpanderWithContext。
func (e discoveryCategoryExpander) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	// 从服务端拉取所有 group/version 的资源；若没有资源则直接回退。
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
		// 按类别收集 GroupVersion。
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

// UnionCategoryExpander 实现 CategoryExpander 接口，把类别映射为列表中所有
// CategoryExpander 展开结果的并集。
//
// UnionCategoryExpanderWithContext 支持上下文日志与取消，应优先使用。
//
// 上下文日志：请改用 UnionCategoryExpanderWithContext。
//
// 展开结果按各展开器的顺序合并并去重。
type UnionCategoryExpander []CategoryExpander

var _ CategoryExpander = UnionCategoryExpander{}

// Expand 实现 CategoryExpander。
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

// UnionCategoryExpanderWithContext 实现 CategoryExpanderWithContext 接口，把类别映射为
// 列表中所有 CategoryExpander 展开结果的并集。
type UnionCategoryExpanderWithContext []CategoryExpanderWithContext

var _ CategoryExpanderWithContext = UnionCategoryExpanderWithContext{}

// ExpandWithContext 实现 CategoryExpanderWithContext。
func (u UnionCategoryExpanderWithContext) ExpandWithContext(ctx context.Context, category string) ([]schema.GroupResource, bool) {
	return unionExpand(ctx, slices.All(u), category)
}

// unionExpand 合并多个展开器的结果：保序、去重，任一展开器识别该类别即返回 true。
// 去重使用线性扫描而非 set，因为结果规模通常很小，避免额外的分配。
func unionExpand(ctx context.Context, expanders iter.Seq2[int, CategoryExpanderWithContext], category string) ([]schema.GroupResource, bool) {
	ret := []schema.GroupResource{}
	ok := false

	// 依次调用列表中每个展开器并合并结果。
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
