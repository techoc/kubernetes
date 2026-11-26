/*
Copyright 2016 The Kubernetes Authors.

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
	"regexp"
	"strings"

	"k8s.io/klog/v2"
)

// TODO: 使用 restful.CrossOriginResourceSharing。
// 参见 github.com/emicklei/go-restful/blob/master/examples/cors/restful-CORS-filter.go 与
// github.com/emicklei/go-restful/blob/master/examples/basicauth/restful-basic-authentication.go。
// 更完整的实现可参考 https://github.com/martini-contrib/cors，
// 或在代理层实现 CORS。

// WithCORS 是一个包装 http Handler 的简单 CORS 实现。
// allowedMethods 与 allowedHeaders 传 nil 时使用默认值；allowedOriginPatterns
// 为空或 nil 时不安装 CORS 支持。
// 安全边界：只有 Origin 命中白名单正则时才回写 CORS 头；未命中时仍转发请求，
// 但浏览器会因缺少允许头而拦截响应。
func WithCORS(handler http.Handler, allowedOriginPatterns []string, allowedMethods []string, allowedHeaders []string, exposedHeaders []string, allowCredentials string) http.Handler {
	if len(allowedOriginPatterns) == 0 {
		// 未配置来源白名单即完全关闭 CORS，而不是默认放行。
		return handler
	}
	allowedOriginPatternsREs := allowedOriginRegexps(allowedOriginPatterns)

	// 未传入 methods 或 headers 时设置默认值；
	// 预先把列表拼成响应头字符串，避免每个请求重复拼接。
	if allowedMethods == nil {
		allowedMethods = []string{"POST", "GET", "OPTIONS", "PUT", "DELETE", "PATCH"}
	}
	allowMethodsResponseHeader := strings.Join(allowedMethods, ", ")

	if allowedHeaders == nil {
		allowedHeaders = []string{"Content-Type", "Content-Length", "Accept-Encoding", "X-CSRF-Token", "Authorization", "X-Requested-With", "If-Modified-Since"}
	}
	allowHeadersResponseHeader := strings.Join(allowedHeaders, ", ")

	if exposedHeaders == nil {
		exposedHeaders = []string{"Date"}
	}
	exposeHeadersResponseHeader := strings.Join(exposedHeaders, ", ")

	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		origin := req.Header.Get("Origin")
		// 非浏览器客户端不带 Origin，直接透传。
		if origin == "" {
			handler.ServeHTTP(w, req)
			return
		}
		if !isOriginAllowed(origin, allowedOriginPatternsREs) {
			handler.ServeHTTP(w, req)
			return
		}

		// 回显具体 Origin 而非 "*"，以便与 Allow-Credentials 共存。
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", allowMethodsResponseHeader)
		w.Header().Set("Access-Control-Allow-Headers", allowHeadersResponseHeader)
		w.Header().Set("Access-Control-Expose-Headers", exposeHeadersResponseHeader)
		w.Header().Set("Access-Control-Allow-Credentials", allowCredentials)

		// 预检请求不进入业务 handler，直接 204 返回。
		if req.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// 分发给下一个 handler。
		handler.ServeHTTP(w, req)
	})
}

// isOriginAllowed 判断请求中给定的 Origin 头是否被 CORS 允许。
//
// 参见 https://www.rfc-editor.org/rfc/rfc6454#page-13
//
//	 a) Origin 头可以包含主机和/或端口
//			serialized-origin   = scheme "://" host [ ":" port ]
//
//	 b) 某些情况下，多个 origin 共同促使用户代理发出 HTTP 请求。
//	 此时用户代理可以在 Origin 头字段中列出所有 origin。例如，
//	 若 HTTP 请求最初由一个 origin 发起，随后被另一个 origin 重定向，
//	 用户代理可以告知服务器有两个 origin 促使其发出了该请求。
//
//			origin-list = serialized-origin *( SP serialized-origin )
func isOriginAllowed(originHeader string, allowedOriginPatternsREs []*regexp.Regexp) bool {
	for _, re := range allowedOriginPatternsREs {
		if re.MatchString(originHeader) {
			return true
		}
	}
	return false
}

// allowedOriginRegexps 把白名单字符串编译为正则；配置非法时直接 Fatal，
// 因为静默忽略会让管理员误以为 CORS 已生效。
func allowedOriginRegexps(allowedOrigins []string) []*regexp.Regexp {
	res, err := compileRegexps(allowedOrigins)
	if err != nil {
		klog.Fatalf("Invalid CORS allowed origin, --cors-allowed-origins flag was set to %v - %v", strings.Join(allowedOrigins, ","), err)
	}
	return res
}

// 接受一个字符串列表并编译为正则列表。
func compileRegexps(regexpStrings []string) ([]*regexp.Regexp, error) {
	regexps := []*regexp.Regexp{}
	for _, regexpStr := range regexpStrings {
		r, err := regexp.Compile(regexpStr)
		if err != nil {
			return []*regexp.Regexp{}, err
		}
		regexps = append(regexps, r)
	}
	return regexps, nil
}
