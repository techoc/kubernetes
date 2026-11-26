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
	"net/http/pprof"

	"k8s.io/apiserver/pkg/server/mux"
)

// Profiling 在 /debug/pprof 下添加 pprof 处理器。
// 注意：pprof 端点会暴露调用栈与运行时细节，应由鉴权层（通常仅允许 system:masters 或
// 本地回环地址）保护，因此这里使用 Unlisted 注册，不出现在 API 发现文档中。
type Profiling struct{}

// Install 把 Profiling webservice 添加到给定的 mux。
func (d Profiling) Install(c *mux.PathRecorderMux) {
	// 裸 /debug/pprof 重定向到带斜杠的索引页，保持标准库 pprof 的行为。
	c.UnlistedHandleFunc("/debug/pprof", redirectTo("/debug/pprof/"))
	c.UnlistedHandlePrefix("/debug/pprof/", http.HandlerFunc(pprof.Index))
	c.UnlistedHandleFunc("/debug/pprof/profile", pprof.Profile)
	c.UnlistedHandleFunc("/debug/pprof/symbol", pprof.Symbol)
	c.UnlistedHandleFunc("/debug/pprof/trace", pprof.Trace)
}

// redirectTo 把请求重定向到指定目标。
func redirectTo(to string) func(http.ResponseWriter, *http.Request) {
	return func(rw http.ResponseWriter, req *http.Request) {
		http.Redirect(rw, req, to, http.StatusFound)
	}
}
