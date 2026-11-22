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

package openapi

import (
	"context"
	"encoding/json"
	"strings"

	"k8s.io/client-go/rest"
	"k8s.io/kube-openapi/pkg/handler3"
)

// ClientWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use ClientWithContext instead.
//
// 旧接口不接收 context，无法取消请求或携带请求级日志；新代码应优先使用 ClientWithContext。
type Client interface {
	Paths() (map[string]GroupVersion, error)
}

// ClientWithContext 是支持 context 的 OpenAPI 客户端。
// 每个 GroupVersion 都绑定同一个 context 派生的请求，取消 context 会中止后续文档拉取。
type ClientWithContext interface {
	PathsWithContext(ctx context.Context) (map[string]GroupVersionWithContext, error)
}

// ToClientWithContext 把旧版 Client 适配为 ClientWithContext。
//
// 若实现本身已支持 context 则直接返回，避免多一层包装；nil 输入返回 nil，
// 方便调用方在可选依赖上做空判断。
func ToClientWithContext(c Client) ClientWithContext {
	if c == nil {
		return nil
	}
	if c, ok := c.(ClientWithContext); ok {
		return c
	}
	return &clientWrapper{
		delegate: c,
	}
}

// clientWrapper 用无 context 的旧 Client 模拟 context 版本。
type clientWrapper struct {
	delegate Client
}

// PathsWithContext 忽略 ctx 并委托给旧接口，仅做类型转换。
// 注意：无法真正取消底层请求，这是使用旧实现时不可避免的局限。
func (c *clientWrapper) PathsWithContext(ctx context.Context) (map[string]GroupVersionWithContext, error) {
	resultWithoutContext, err := c.delegate.Paths()
	result := make(map[string]GroupVersionWithContext, len(resultWithoutContext))
	for key, entry := range resultWithoutContext {
		result[key] = ToGroupVersionWithContext(entry)
	}
	return result, err
}

type client struct {
	// URL includes the `hash` query param to take advantage of cache busting
	restClient rest.Interface
}

// NewClientWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use NewClientWithContext instead.
func NewClient(restClient rest.Interface) Client {
	return newClient(restClient)
}

// NewClientWithContext 创建支持 context 的 OpenAPI v3 客户端。
// restClient 应已指向 API server 根路径，构造成本很低，可按需创建。
func NewClientWithContext(restClient rest.Interface) ClientWithContext {
	return newClient(restClient)
}

func newClient(restClient rest.Interface) *client {
	return &client{
		restClient: restClient,
	}
}

// PathsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use PathsWithContext instead.
//
// 旧接口通过 context.Background() 调用新实现，结果再做一次向下转换。
func (c *client) Paths() (map[string]GroupVersion, error) {
	resultWithContext, err := c.PathsWithContext(context.Background())
	result := make(map[string]GroupVersion, len(resultWithContext))
	for key, entry := range resultWithContext {
		// We know that this is a *groupversion which implements GroupVersion.
		result[key] = entry.(GroupVersion)
	}
	return result, err
}

// PathsWithContext 拉取 /openapi/v3 的发现文档，并为每个 group-version 构造可独立取文档的句柄。
//
// 返回的 map 键即发现文档中的路径键（例如 "api/v1"），调用方应原样保留。
func (c *client) PathsWithContext(ctx context.Context) (map[string]GroupVersionWithContext, error) {
	data, err := c.restClient.Get().
		AbsPath("/openapi/v3").
		Do(ctx).
		Raw()

	if err != nil {
		return nil, err
	}

	discoMap := &handler3.OpenAPIV3Discovery{}
	err = json.Unmarshal(data, discoMap)
	if err != nil {
		return nil, err
	}

	// Calculate the client-side prefix for a "root" request
	// 用于剥离服务端返回路径中可能包含的客户端前缀（例如 API server 挂在子路径下）。
	rootPrefix := strings.TrimSuffix(c.restClient.Get().AbsPath("/").URL().Path, "/")
	// Create GroupVersions for each element of the result
	result := make(map[string]GroupVersionWithContext, len(discoMap.Paths))
	for k, v := range discoMap.Paths {
		// Trim off the prefix that will always be added in client-side
		v.ServerRelativeURL = strings.TrimPrefix(v.ServerRelativeURL, rootPrefix)
		// If the server returned a URL rooted at /openapi/v3, preserve any additional client-side prefix.
		// If the server returned a URL not rooted at /openapi/v3, treat it as an actual server-relative URL.
		// See https://github.com/kubernetes/kubernetes/issues/117463 for details
		//
		// 该布尔值决定后续取文档时是拼接客户端前缀还是直接使用服务端给出的绝对路径，
		// 以兼容不同版本 apiserver 对 ServerRelativeURL 的两种约定。
		useClientPrefix := strings.HasPrefix(v.ServerRelativeURL, "/openapi/v3")
		result[k] = newGroupVersion(c, v, useClientPrefix)
	}
	return result, nil
}
