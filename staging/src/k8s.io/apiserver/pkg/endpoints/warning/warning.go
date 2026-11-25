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

// Package warning 提供向 HTTP 响应附加 Warning 头的路由包装器。
package warning

import (
	restful "github.com/emicklei/go-restful/v3"

	"k8s.io/apiserver/pkg/warning"
)

// AddWarningsHandler 返回一个新的 handler，它会为所有请求附加给定的警告，然后再委托给原 handler。
// AddWarningsHandler 为路由附加固定警告（例如 API 已弃用）。警告通过
// context 传递，由外层过滤器统一写入 Warning 响应头；warnings 为空时
// 直接返回原 handler，避免无意义的包装。
func AddWarningsHandler(handler restful.RouteFunction, warnings []string) restful.RouteFunction {
	if len(warnings) == 0 {
		return handler
	}

	return func(req *restful.Request, res *restful.Response) {
		ctx := req.Request.Context()
		for _, msg := range warnings {
			warning.AddWarning(ctx, "", msg)
		}
		handler(req, res)
	}
}
