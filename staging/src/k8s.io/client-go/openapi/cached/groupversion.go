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

// Package cached 为 OpenAPI GroupVersion 文档增加进程内缓存，避免每次 discovery 都重新下载 schema。
package cached

import (
	"context"
	"sync"

	"k8s.io/client-go/openapi"
)

type groupversion struct {
	delegate openapi.GroupVersionWithContext

	// lock 串行化缓存填充；docs 按 content type 缓存下载结果（含错误），
	// 这样同一 content type 不会被重复请求，即使此前失败过。
	lock sync.Mutex
	docs map[string]docInfo
}

var (
	//nolint:staticcheck // Intentionally using the old interface here.
	_ openapi.GroupVersion            = &groupversion{}
	_ openapi.GroupVersionWithContext = &groupversion{}
)

type docInfo struct {
	data []byte
	err  error
}

// newGroupVersion 包装一个真实 GroupVersion；缓存按实例惰性初始化。
func newGroupVersion(delegate openapi.GroupVersionWithContext) *groupversion {
	return &groupversion{
		delegate: delegate,
	}
}

// SchemaWithContext is a better alternative because it supports contextual logging and cancellation.
//
// Contextual logging: Use SchemaWithContext instead.
func (g *groupversion) Schema(contentType string) ([]byte, error) {
	return g.SchemaWithContext(context.Background(), contentType)
}

// SchemaWithContext 返回指定 content type 的 schema，首次调用时从 delegate 拉取并缓存。
func (g *groupversion) SchemaWithContext(ctx context.Context, contentType string) ([]byte, error) {
	// 已取消的上下文不应触发下载，尽早返回。
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	g.lock.Lock()
	defer g.lock.Unlock()

	cachedInfo, ok := g.docs[contentType]
	if !ok {
		if g.docs == nil {
			g.docs = make(map[string]docInfo)
		}

		// 失败结果同样缓存，避免对同一个 content type 反复请求。
		cachedInfo.data, cachedInfo.err = g.delegate.SchemaWithContext(ctx, contentType)
		g.docs[contentType] = cachedInfo
	}

	return cachedInfo.data, cachedInfo.err
}

// ServerRelativeURL 直接透传，缓存层不改变服务端相对路径的语义。
func (c *groupversion) ServerRelativeURL() string {
	return c.delegate.ServerRelativeURL()
}
