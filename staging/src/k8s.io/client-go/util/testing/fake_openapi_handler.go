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

// Package testing 为 client-go 的测试提供可复用的假服务器与辅助工具。
package testing

import (
	"encoding/json"
	"io"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"k8s.io/kube-openapi/pkg/handler3"
	"k8s.io/kube-openapi/pkg/spec3"
)

// FakeOpenAPIServer 模拟 API Server 的 OpenAPI V3 端点。
//
// ServedDocuments 保存按 GroupVersion 索引的规范，便于测试断言；
// RequestCounters 按请求路径累计次数，用于验证客户端缓存是否生效。
type FakeOpenAPIServer struct {
	HttpServer      *httptest.Server
	ServedDocuments map[string]*spec3.OpenAPI
	RequestCounters map[string]int
}

// 创建一个模拟 Kubernetes API Server 的 OpenAPI V3 服务端。
// specsPath 指向测试数据的路径，其中每个 GroupVersion 各有一个 OpenAPI V3 JSON 文件，
// 例如 apps/v1beta1 存为 <specsPath>/apps/v1beta1.json。
func NewFakeOpenAPIV3Server(specsPath string) (*FakeOpenAPIServer, error) {
	mux := &testMux{
		counts: map[string]int{},
	}
	server := httptest.NewServer(mux)

	// 复用真实的 handler3 实现，确保客户端看到的发现文档结构与生产一致。
	openAPIVersionedService := handler3.NewOpenAPIService()
	err := openAPIVersionedService.RegisterOpenAPIV3VersionedService("/openapi/v3", mux)
	if err != nil {
		return nil, err
	}

	grouped := make(map[string][]byte)
	var testV3Specs = make(map[string]*spec3.OpenAPI)

	addSpec := func(path string) {
		file, err := os.Open(path)
		if err != nil {
			panic(err)
		}

		defer file.Close()
		vals, err := io.ReadAll(file)
		if err != nil {
			panic(err)
		}

		// 用相对路径去掉扩展名作为 GroupVersion 键，例如 apps/v1beta1。
		rel, err := filepath.Rel(specsPath, path)
		if err == nil {
			grouped[rel[:(len(rel)-len(filepath.Ext(rel)))]] = vals
		}
	}

	filepath.WalkDir(specsPath, func(path string, d fs.DirEntry, err error) error {
		// 只加载 .json 文件，目录本身与无关文件跳过。
		if filepath.Ext(path) != ".json" || d.IsDir() {
			return nil
		}

		addSpec(path)
		return nil
	})

	for gv, jsonSpec := range grouped {
		spec := &spec3.OpenAPI{}
		err = json.Unmarshal(jsonSpec, spec)
		if err != nil {
			return nil, err
		}

		// 同时保留一份解析后的文档供测试断言，并注册到可服务的版本列表中。
		testV3Specs[gv] = spec
		openAPIVersionedService.UpdateGroupVersion(gv, spec)
	}

	return &FakeOpenAPIServer{
		HttpServer:      server,
		ServedDocuments: testV3Specs,
		RequestCounters: mux.counts,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////
// 极简测试用 HTTP 路由器。
////////////////////////////////////////////////////////////////////////////////
// 实现 handler3 注册 OpenAPI handler 所需的 mux 接口。

// testMux 是最小可用的 http 路由：支持精确匹配与最长前缀匹配，并统计请求次数。
type testMux struct {
	lock      sync.Mutex
	prefixMap map[string]http.Handler
	pathMap   map[string]http.Handler
	counts    map[string]int
}

func (t *testMux) Handle(path string, handler http.Handler) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.pathMap == nil {
		t.pathMap = make(map[string]http.Handler)
	}
	t.pathMap[path] = handler
}

func (t *testMux) HandlePrefix(path string, handler http.Handler) {
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.prefixMap == nil {
		t.prefixMap = make(map[string]http.Handler)
	}
	t.prefixMap[path] = handler
}

func (t *testMux) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// 整个分发过程持锁，简化并发测试；计数与 handler 调用因此是原子的。
	t.lock.Lock()
	defer t.lock.Unlock()

	if t.counts == nil {
		t.counts = make(map[string]int)
	}

	if val, exists := t.counts[req.URL.Path]; exists {
		t.counts[req.URL.Path] = val + 1
	} else {
		t.counts[req.URL.Path] = 1
	}

	// 精确匹配优先于前缀匹配，与 net/http.ServeMux 的直觉一致。
	if handler, ok := t.pathMap[req.URL.Path]; ok {
		handler.ServeHTTP(w, req)
		return
	}

	for k, v := range t.prefixMap {
		if strings.HasPrefix(req.URL.Path, k) {
			v.ServeHTTP(w, req)
			return
		}
	}

	w.WriteHeader(http.StatusNotFound)
}
