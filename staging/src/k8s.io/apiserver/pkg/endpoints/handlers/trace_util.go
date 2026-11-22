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

package handlers

import (
	"net/http"

	"go.opentelemetry.io/otel/attribute"
)

// traceFields 构造 HTTP 请求的 trace 属性集合。
//
// 除 protocol/url 外都通过 Stringer 惰性求值：RequestInfo 等字段需要解析，
// 而大多数请求不会采样到 trace，用延迟求值避免为未采样的请求付出解析开销。
// user-agent 做了截断，防止恶意超长头撑爆 trace 后端。
func traceFields(req *http.Request) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.Stringer("accept", &lazyAccept{req: req}),
		attribute.Stringer("audit-id", &lazyAuditID{req: req}),
		attribute.Stringer("client", &lazyClientIP{req: req}),
		attribute.Stringer("api-group", &lazyAPIGroup{req: req}),
		attribute.Stringer("api-version", &lazyAPIVersion{req: req}),
		attribute.Stringer("name", &lazyName{req: req}),
		attribute.Stringer("subresource", &lazySubresource{req: req}),
		attribute.Stringer("namespace", &lazyNamespace{req: req}),
		attribute.String("protocol", req.Proto),
		attribute.Stringer("resource", &lazyResource{req: req}),
		attribute.Stringer("scope", &lazyScope{req: req}),
		attribute.String("url", req.URL.Path),
		attribute.Stringer("user-agent", &lazyTruncatedUserAgent{req: req}),
		attribute.Stringer("verb", &lazyVerb{req: req}),
	}
}
