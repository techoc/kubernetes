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

package openapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

// TestGroupVersion 验证 GroupVersion.Schema 在多种客户端前缀（client prefix）配置下
// 都能取到正确的 schema。
// API server 可能被部署在子路径下，discovery 返回的 serverRelativeURL 可能
// 带或不带该前缀，客户端必须两种都支持。测试用 httptest 模拟 /openapi/v3 发现文档与
// 具体 group/version 的 schema 端点，并断言 hash 查询参数被原样保留。
func TestGroupVersion(t *testing.T) {
	tests := []struct {
		name                string
		prefix              string
		serverReturnsPrefix bool
	}{
		{
			// 中文：无反代前缀，最普通的情况。
			name:                "no prefix",
			prefix:              "",
			serverReturnsPrefix: false,
		},
		{
			// 中文：Host 带前缀但 discovery 返回的 URL 不带，走 RequestURI 分支。
			name:                "prefix not in discovery",
			prefix:              "/test-endpoint",
			serverReturnsPrefix: false,
		},
		{
			// 中文：discovery 返回的 URL 自带前缀，需要拆分路径与查询参数后走 AbsPath 分支。
			name:                "prefix in discovery",
			prefix:              "/test-endpoint",
			serverReturnsPrefix: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case r.URL.Path == test.prefix+"/openapi/v3/apis/apps/v1" && r.URL.RawQuery == "hash=014fbff9a07c":
					// 中文：schema 端点要求 hash 参数存在，以此验证客户端保留了查询串。
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					w.Write([]byte(`{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"}}`))
				case r.URL.Path == test.prefix+"/openapi/v3":
					// return root content
					// 中文：发现文档；serverReturnsPrefix 决定 URL 是否包含部署前缀。
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusOK)
					if test.serverReturnsPrefix {
						w.Write([]byte(fmt.Sprintf(`{"paths":{"apis/apps/v1":{"serverRelativeURL":"%s/openapi/v3/apis/apps/v1?hash=014fbff9a07c"}}}`, test.prefix)))
					} else {
						w.Write([]byte(`{"paths":{"apis/apps/v1":{"serverRelativeURL":"/openapi/v3/apis/apps/v1?hash=014fbff9a07c"}}}`))
					}
				default:
					// 中文：任何未预期的请求都直接失败，防止测试掩盖错误的 URL 构造。
					t.Errorf("unexpected request: %s", r.URL.String())
					w.WriteHeader(http.StatusNotFound)
					return
				}
			}))
			defer server.Close()

			// 中文：RESTClient 的 Host 带前缀，模拟 API server 部署在子路径下的场景。
			c, err := rest.RESTClientFor(&rest.Config{
				Host: server.URL + test.prefix,
				ContentConfig: rest.ContentConfig{
					NegotiatedSerializer: scheme.Codecs,
					GroupVersion:         &appsv1.SchemeGroupVersion,
				},
			})

			if err != nil {
				t.Fatalf("unexpected error occurred: %v", err)
			}

			openapiClient := NewClient(c)
			paths, err := openapiClient.Paths()
			if err != nil {
				t.Fatalf("unexpected error occurred: %v", err)
			}
			// 中文：按 discovery 文档中的原始 key 取句柄，键名不做归一化。
			schema, err := paths["apis/apps/v1"].Schema(runtime.ContentTypeJSON)
			if err != nil {
				t.Fatalf("unexpected error occurred: %v", err)
			}
			expectedResult := `{"openapi":"3.0.0","info":{"title":"Kubernetes","version":"unversioned"}}`
			if string(schema) != expectedResult {
				t.Fatalf("unexpected result actual: %s expected: %s", string(schema), expectedResult)
			}
		})
	}
}
