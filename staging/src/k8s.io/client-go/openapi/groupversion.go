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

// Package openapi 提供访问 Kubernetes 服务端 OpenAPI v3 文档的客户端。
//
// 客户端按 group/version 分别获取 schema，每个 GroupVersion 同时暴露一个带 hash 的
// ServerRelativeURL，调用方可将该 URL 作为缓存键，schema 更新时 hash 会随之变化。
package openapi

import (
	"context"
	"net/url"

	"k8s.io/kube-openapi/pkg/handler3"
)

const ContentTypeOpenAPIV3PB = "application/com.github.proto-openapi.spec.v3@v1.0+protobuf"

// GroupVersionWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use GroupVersionWithContext instead.
//
// 中文说明：GroupVersion 的旧接口不接收 context，无法取消请求或传递日志上下文；
// 新代码应优先使用 GroupVersionWithContext。
type GroupVersion interface {
	// Schema 返回指定 content type 的序列化 schema 字节。
	Schema(contentType string) ([]byte, error)

	// ServerRelativeURL. Returns the path and parameters used to fetch the schema.
	// You should use the Schema method to fetch it, but this value can be used
	// to key the current version of the schema in a cache since it contains a
	// hash string which changes upon schema update.
	//
	// 中文：不要直接用该 URL 自行请求，交给 Schema 处理；它更适合做缓存键。
	ServerRelativeURL() string
}

// GroupVersionWithContext 是支持 context 的 GroupVersion 接口。
type GroupVersionWithContext interface {
	// SchemaWithContext 返回指定 content type 的序列化 schema 字节，可被 ctx 取消。
	SchemaWithContext(ctx context.Context, contentType string) ([]byte, error)

	// ServerRelativeURL. 返回获取 schema 所用的路径与查询参数，带有随 schema
	// 更新而变化的 hash，可用作缓存键。实际获取请使用 SchemaWithContext。
	ServerRelativeURL() string
}

// ToGroupVersionWithContext 将旧的 GroupVersion 适配为 GroupVersionWithContext。
//
// 中文说明：若对象已实现新接口则直接返回，避免不必要的包装；旧实现被包装后其
// SchemaWithContext 会忽略 ctx，仅保留兼容性，不接受取消。
func ToGroupVersionWithContext(gv GroupVersion) GroupVersionWithContext {
	if gv == nil {
		return nil
	}
	if gv, ok := gv.(GroupVersionWithContext); ok {
		return gv
	}
	return &groupVersionWrapper{
		GroupVersion: gv,
	}
}

// groupVersionWrapper 是 GroupVersion 到 GroupVersionWithContext 的适配器。
type groupVersionWrapper struct {
	GroupVersion
}

// SchemaWithContext 丢弃 ctx 并委托给旧的 Schema 方法。
//
// 中文：兼容路径无法真正取消请求，调用方不应依赖 ctx 语义。
func (gv *groupVersionWrapper) SchemaWithContext(ctx context.Context, contentType string) ([]byte, error) {
	return gv.Schema(contentType)
}

// groupversion 是 GroupVersion/GroupVersionWithContext 的具体实现。
type groupversion struct {
	client          *client
	item            handler3.OpenAPIV3DiscoveryGroupVersion
	useClientPrefix bool
}

var (
	_ GroupVersion            = &groupversion{}
	_ GroupVersionWithContext = &groupversion{}
)

// newGroupVersion 构造单个 group/version 的 schema 访问器。
func newGroupVersion(client *client, item handler3.OpenAPIV3DiscoveryGroupVersion, useClientPrefix bool) *groupversion {
	return &groupversion{client: client, item: item, useClientPrefix: useClientPrefix}
}

// Schema 是旧接口，使用 Background context 调用 SchemaWithContext。
func (g *groupversion) Schema(contentType string) ([]byte, error) {
	return g.SchemaWithContext(context.Background(), contentType)
}

// SchemaWithContext 通过 REST 客户端获取 schema。
//
// 中文说明：服务端可能返回客户端前缀（client prefix）形式的 URL，两种模式的处理方式不同：
//   - 无前缀：URL 已是相对 restClient 根路径的完整请求 URI，直接 RequestURI。
//   - 有前缀：需要拆分出路径与查询参数，因为 AbsPath 不会保留查询串，需手动逐个添加。
func (g *groupversion) SchemaWithContext(ctx context.Context, contentType string) ([]byte, error) {
	if !g.useClientPrefix {
		return g.client.restClient.Get().
			RequestURI(g.item.ServerRelativeURL).
			SetHeader("Accept", contentType).
			Do(ctx).
			Raw()
	}

	locator, err := url.Parse(g.item.ServerRelativeURL)
	if err != nil {
		return nil, err
	}

	path := g.client.restClient.Get().
		AbsPath(locator.Path).
		SetHeader("Accept", contentType)

	// Other than root endpoints(openapiv3/apis), resources have hash query parameter to support etags.
	// However, absPath does not support handling query parameters internally,
	// so that hash query parameter is added manually
	// 中文：hash 参数用于支持 etag 语义，必须原样保留，否则缓存与条件请求会失效。
	for k, value := range locator.Query() {
		for _, v := range value {
			path.Param(k, v)
		}
	}

	return path.Do(ctx).Raw()
}

// URL used for fetching the schema. The URL includes a hash and can be used
// to key the current version of the schema in a cache.
//
// 中文说明：直接返回 discovery 中记录的 URL 字符串，不做任何改写。
func (g *groupversion) ServerRelativeURL() string {
	return g.item.ServerRelativeURL
}
