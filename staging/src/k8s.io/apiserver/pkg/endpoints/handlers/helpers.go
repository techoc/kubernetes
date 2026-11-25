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

package handlers

import (
	"net/http"

	utilnet "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
)

const (
	// User-Agent 会进入访问日志与审计记录。限制长度是为了防止
	// 超长（甚至恶意构造）的 UA 撑大日志、拖慢日志管道。
	maxUserAgentLength      = 1024
	userAgentTruncateSuffix = "...TRUNCATED"
)

// 下面这些 lazy* 类型都实现 Stringer，在真正需要打印（访问日志、
// 审计）时才读取请求信息。这样在不开日志的路径上不产生额外开销，
// 也避免在日志级别不够时仍构造字符串。

// lazyTruncatedUserAgent 实现 String() string，返回可能被截断的 user-agent。
type lazyTruncatedUserAgent struct {
	req *http.Request
}

// String 返回截断后的 User-Agent；请求为空时返回 "unknown"，
// 保证日志字段始终有值而不会 panic。
func (lazy *lazyTruncatedUserAgent) String() string {
	ua := "unknown"
	if lazy.req != nil {
		ua = utilnet.GetHTTPClient(lazy.req)
		if len(ua) > maxUserAgentLength {
			ua = ua[:maxUserAgentLength] + userAgentTruncateSuffix
		}
	}
	return ua
}

// LazyClientIP 实现 String() string，仅在真正需要时才调用 GetClientIP()。
type lazyClientIP struct {
	req *http.Request
}

// String 返回客户端 IP；GetClientIP 会考虑 X-Forwarded-For 等头，
// 仅在输出日志时才做这项解析。
func (lazy *lazyClientIP) String() string {
	if lazy.req != nil {
		if ip := utilnet.GetClientIP(lazy.req); ip != nil {
			return ip.String()
		}
	}
	return "unknown"
}

// lazyAccept 实现 String() string，仅在需要时调用 http.Request Header.Get()。
type lazyAccept struct {
	req *http.Request
}

// String 返回 Accept 头原值。
func (lazy *lazyAccept) String() string {
	if lazy.req != nil {
		accept := lazy.req.Header.Get("Accept")
		return accept
	}

	return "unknown"
}

// lazyAPIGroup 实现 String() string，按需从 request info 中取得 API 组。
type lazyAPIGroup struct {
	req *http.Request
}

// String 从 RequestInfo 返回 API 组；缺失时返回 "unknown"。
func (lazy *lazyAPIGroup) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.APIGroup
		}
	}

	return "unknown"
}

// lazyAPIVersion 实现 String() string，按需从 request info 中取得 API 版本。
type lazyAPIVersion struct {
	req *http.Request
}

// String 从 RequestInfo 返回 API 版本。
func (lazy *lazyAPIVersion) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.APIVersion
		}
	}

	return "unknown"
}

// lazyName 实现 String() string，按需从 request info 中取得对象名。
type lazyName struct {
	req *http.Request
}

// String 从 RequestInfo 返回对象名。
func (lazy *lazyName) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.Name
		}
	}

	return "unknown"
}

// lazySubresource 实现 String() string，按需从 request info 中取得子资源名。
type lazySubresource struct {
	req *http.Request
}

// String 从 RequestInfo 返回子资源名。
func (lazy *lazySubresource) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.Subresource
		}
	}

	return "unknown"
}

// lazyNamespace 实现 String() string，按需从 request info 中取得命名空间。
type lazyNamespace struct {
	req *http.Request
}

// String 从 RequestInfo 返回命名空间。
func (lazy *lazyNamespace) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.Namespace
		}
	}

	return "unknown"
}

// lazyAuditID 实现 Stringer 接口，按需取出请求关联的审计 ID。
type lazyAuditID struct {
	req *http.Request
}

// String 返回截断后的审计 ID，便于把访问日志与具体审计记录关联，
// 同时避免在日志里暴露完整 ID。
func (lazy *lazyAuditID) String() string {
	if lazy.req != nil {
		return audit.GetAuditIDTruncated(lazy.req.Context())
	}

	return "unknown"
}

// lazyVerb 实现 String() string，按需取得归一化后的 Verb。
type lazyVerb struct {
	req *http.Request
}

// String 返回归一化后的 HTTP 动词（例如把 GET/LIST 区分开），
// 与请求指标使用同一套归一化逻辑，保证日志与指标口径一致。
func (lazy *lazyVerb) String() string {
	if lazy.req == nil {
		return "unknown"
	}
	return metrics.NormalizedVerb(lazy.req)
}

// lazyResource 实现 String() string，按需从 request info 中取得 Resource。
type lazyResource struct {
	req *http.Request
}

// String 从 RequestInfo 返回资源名。
func (lazy *lazyResource) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return requestInfo.Resource
		}
	}

	return "unknown"
}

// lazyScope 实现 String() string，按需从 request info 中取得 Scope。
type lazyScope struct {
	req *http.Request
}

// String 返回清洗后的 scope（resource/namespace/cluster），
// 清洗可避免把高基数命名空间带入日志或指标。
func (lazy *lazyScope) String() string {
	if lazy.req != nil {
		ctx := lazy.req.Context()
		requestInfo, ok := apirequest.RequestInfoFrom(ctx)
		if ok {
			return metrics.CleanScope(requestInfo)
		}
	}

	return "unknown"
}
