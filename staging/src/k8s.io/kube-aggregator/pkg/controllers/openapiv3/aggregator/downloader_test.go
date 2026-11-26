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

package aggregator

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"k8s.io/kube-openapi/pkg/handler3"
)

// 假后端同时模拟发现文档和带 hash 的具体文档，并把 etag 作为 hash 写进 URL，
// 这样下载器拿到的路径和真实后端返回的发现文档形状一致。
type handlerTest struct {
	etag string
	data []byte
}

// 编译期断言：这个测试替身必须能被当 http.Handler 传给下载器。
var _ http.Handler = handlerTest{}

func (h handlerTest) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 创建一个只处理某个 group/version 的 APIService 及 handler。
	if r.URL.Path == "/openapi/v3" {
		group := &handler3.OpenAPIV3Discovery{
			Paths: map[string]handler3.OpenAPIV3DiscoveryGroupVersion{
				"apis/group/version": {
					ServerRelativeURL: "/openapi/v3/apis/group/version?hash=" + h.etag,
				},
			},
		}

		j, _ := json.Marshal(group)
		w.Write(j)
		return
	}

	if r.URL.Path == "/openapi/v3/apis/group/version" {
		// 带上 If-None-Match 时返回 304，用来验证调用方能否识别"文档没变"。
		if len(h.etag) > 0 {
			w.Header().Add("Etag", h.etag)
		}
		ifNoneMatches := r.Header["If-None-Match"]
		for _, match := range ifNoneMatches {
			if match == h.etag {
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}
		w.Write(h.data)
	}
}

// 只验根发现文档这一步：它能正确解析出 group/version 及其链接，
// 聚合器才有依据决定后续该往哪个路径发请求。
func TestDownloadOpenAPISpec(t *testing.T) {
	s := Downloader{}

	groups, _, err := s.OpenAPIV3Root(
		handlerTest{data: []byte(""), etag: ""})
	assert.NoError(t, err)
	if assert.NotNil(t, groups) {
		assert.Len(t, groups.Paths, 1)
		if assert.Contains(t, groups.Paths, "apis/group/version") {
			assert.NotEmpty(t, groups.Paths["apis/group/version"].ServerRelativeURL)
		}
	}

}
