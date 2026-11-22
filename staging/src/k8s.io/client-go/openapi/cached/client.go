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

package cached

import (
	"context"
	"sync"

	"k8s.io/client-go/openapi"
)

// Client 缓存 openapi.Client 的 Paths 结果。
//
// apiserver 的 OpenAPI 路径清单在进程生命周期内基本不变，但每次调用都会产生
// 一次 HTTP 请求；这里用 sync.Once 只取一次并复用，减少对 apiserver 的请求。
type Client struct {
	delegate openapi.ClientWithContext

	// once 保证委托客户端只被调用一次；结果或错误被缓存后所有调用方共享。
	once   sync.Once
	result map[string]openapi.GroupVersionWithContext
	err    error
}

var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ openapi.Client            = &Client{}
	_ openapi.ClientWithContext = &Client{}
)

// NewClientWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use NewClientWithContext instead.
//
// NewClient 包装一个旧版 openapi.Client；新代码应使用 NewClientWithContext。
// 这里把旧接口适配成带 context 的接口，以便统一走缓存逻辑。
func NewClient(other openapi.Client) openapi.Client {
	return newClient(openapi.ToClientWithContext(other))
}

// NewClientWithContext 包装一个带 context 的客户端并返回可缓存的 *Client。
func NewClientWithContext(other openapi.ClientWithContext) *Client {
	return newClient(other)
}

func newClient(other openapi.ClientWithContext) *Client {
	return &Client{
		delegate: other,
	}
}

// PathsWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use PathsWithContext instead.
//
// Paths 返回缓存的 GroupVersion 映射；新代码应使用 PathsWithContext。
func (c *Client) Paths() (map[string]openapi.GroupVersion, error) {
	// For efficiency reasons in the *WithContext case this is a map to
	// openapi.GroupVersionWithContext. But we know that all entries
	// also implement openapi.GroupVersion.
	//
	// 缓存内部只保存 WithContext 版本，避免两份缓存的同步成本；
	// 这里做一次断言转换，因为所有条目同时实现了旧接口。
	resultWithContext, err := c.PathsWithContext(context.Background())
	result := make(map[string]openapi.GroupVersion, len(resultWithContext))
	for key, entry := range resultWithContext {
		result[key] = entry.(openapi.GroupVersion)
	}
	return result, err
}

// PathsWithContext 返回缓存的路径映射，首次调用时向委托客户端拉取并缓存结果。
func (c *Client) PathsWithContext(ctx context.Context) (map[string]openapi.GroupVersionWithContext, error) {
	// 在真正发起请求前检查 context，避免对已取消的调用做无谓的 I/O。
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.once.Do(func() {
		uncached, err := c.delegate.PathsWithContext(ctx)
		if err != nil {
			c.err = err
			return
		}

		// 把每个 GroupVersion 包装成带缓存的版本，后续按路径取 OpenAPI 文档时也走缓存。
		result := make(map[string]openapi.GroupVersionWithContext, len(uncached))
		for k, v := range uncached {
			result[k] = newGroupVersion(v)
		}
		c.result = result
	})
	// 注意：错误只在第一次调用时被设置，后续调用即使 context 已变化也返回同样的缓存错误。
	return c.result, c.err
}
