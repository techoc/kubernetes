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

	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/utils/clock"
)

// WithRequestReceivedTimestamp 把 ReceivedTimestamp（请求到达 apiserver 的时间）
// 附加到 context。
// 该时间戳用于审计日志与请求超时计算；放在过滤器链最外层，才能最接近请求到达时刻。
func WithRequestReceivedTimestamp(handler http.Handler) http.Handler {
	return withRequestReceivedTimestampWithClock(handler, clock.RealClock{})
}

// clock 作为参数传入，方便单元测试注入假时钟。
func withRequestReceivedTimestampWithClock(handler http.Handler, clock clock.PassiveClock) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		// 只替换 Request 的 context，不修改其他字段，避免影响下游对请求的假设。
		req = req.WithContext(request.WithReceivedTimestamp(ctx, clock.Now()))

		handler.ServeHTTP(w, req)
	})
}
