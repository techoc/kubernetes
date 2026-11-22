/*
Copyright 2014 The Kubernetes Authors.

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

package routes

import (
	"net/http"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/handlers/responsewriters"
	"k8s.io/apiserver/pkg/server/mux"
)

// ListedPathProvider 提供应在 / 处报告的路径。
type ListedPathProvider interface {
	// ListedPaths 返回按字母序排序的、应在 / 处报告的路径列表。
	ListedPaths() []string
}

// ListedPathProviders 是组合多个 ListedPathProvider 的便捷类型。
type ListedPathProviders []ListedPathProvider

// ListedPaths 合并并排序所有包含的路径。
// 使用 set 去重，因为多个 provider 可能报告同一路径（例如 /healthz 与 /readyz 共享前缀处理）。
func (p ListedPathProviders) ListedPaths() []string {
	ret := sets.String{}
	for _, provider := range p {
		for _, path := range provider.ListedPaths() {
			ret.Insert(path)
		}
	}

	return ret.List()
}

// Index 为 http 根路径 / 提供 web 服务，列出所有已知路径。
type Index struct{}

// Install 把 Index web 服务安装到给定 mux。
// 使用 UnlistedHandle：/ 与 /index.html 自身不应出现在根路径列表中。
func (i Index) Install(pathProvider ListedPathProvider, mux *mux.PathRecorderMux) {
	handler := IndexLister{StatusCode: http.StatusOK, PathProvider: pathProvider}

	mux.UnlistedHandle("/", handler)
	mux.UnlistedHandle("/index.html", handler)
}

// IndexLister 以给定状态码列出可用索引。
type IndexLister struct {
	StatusCode   int
	PathProvider ListedPathProvider
}

// ServeHTTP 输出可用路径列表，格式为 metav1.RootPaths 的 JSON。
func (i IndexLister) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	responsewriters.WriteRawJSON(i.StatusCode, metav1.RootPaths{Paths: i.PathProvider.ListedPaths()}, w)
}
