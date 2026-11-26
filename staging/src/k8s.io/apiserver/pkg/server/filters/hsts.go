/*
Copyright 2020 The Kubernetes Authors.

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
	"strings"
)

// WithHSTS 是一个简单的 HSTS 实现，包装一个 http Handler。
// hstsDirectives 为空或 nil 时，不启用 HSTS 支持。
//
// 空指令时直接返回原 handler（而非包一层），保持零开销且不改变调用栈。
func WithHSTS(handler http.Handler, hstsDirectives []string) http.Handler {
	if len(hstsDirectives) == 0 {
		return handler
	}
	allDirectives := strings.Join(hstsDirectives, "; ")
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// Chrome 与 Mozilla Firefox 维护 HSTS 预加载列表
		// 问题：golang.org/issue/26162
		// 若尚未设置 Strict-Transport-Security 头，则设置它
		// 不覆盖已有值：下游 handler 可能针对特定路径设置更严格的策略。
		if _, ok := w.Header()["Strict-Transport-Security"]; !ok {
			w.Header().Set("Strict-Transport-Security", allDirectives)
		}
		handler.ServeHTTP(w, req)
	})
}
