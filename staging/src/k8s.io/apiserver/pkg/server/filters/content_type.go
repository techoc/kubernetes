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

import "net/http"

// WithContentType sets both the Content-Type and the X-Content-Type-Options (nosniff) header
//
// 两者必须成对设置：仅设置 Content-Type 时，浏览器可能忽略它并自行嗅探响应内容，
// 在返回用户可控数据的端点上会引入 XSS 风险。
func WithContentType(handler http.Handler, contentType string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("X-Content-Type-Options", "nosniff")
		handler.ServeHTTP(w, r)
	})
}
