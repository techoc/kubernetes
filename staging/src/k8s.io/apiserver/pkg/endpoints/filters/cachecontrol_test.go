/*
Copyright 2019 The Kubernetes Authors.

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

package filters

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestCacheControl 验证 WithCacheControl 对 API 响应强制加上
// "no-cache, private"，同时不覆盖下游已经设置的 Cache-Control。
// 这防止中间代理缓存带有用户上下文的 API 响应。
func TestCacheControl(t *testing.T) {
	tests := []struct {
		name string
		path string

		startingHeader string
		expectedHeader string
	}{
		{
			// 普通 API 资源路径默认被设为 no-cache, private。
			name:           "simple",
			path:           "/api/v1/namespaces",
			expectedHeader: "no-cache, private",
		},
		{
			// OpenAPI 端点同样不能被缓存，避免代理下发过期的 API 描述。
			name:           "openapi",
			path:           "/openapi/v2",
			expectedHeader: "no-cache, private",
		},
		{
			// 下游已显式设置时保留原值，过滤器不覆盖业务决策。
			name:           "already-set",
			path:           "/api/v1/namespaces",
			startingHeader: "nonsense",
			expectedHeader: "nonsense",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := t.Context()
			handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
				// 什么都不做
			})
			wrapped := WithCacheControl(handler)

			testRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, test.path, nil)
			if err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			// 预置起始头以模拟下游 handler 已经写入 Cache-Control 的情况。
			if len(test.startingHeader) > 0 {
				w.Header().Set("Cache-Control", test.startingHeader)
			}

			wrapped.ServeHTTP(w, testRequest)
			actual := w.Header().Get("Cache-Control")

			if actual != test.expectedHeader {
				t.Fatal(actual)
			}
		})
	}

}
