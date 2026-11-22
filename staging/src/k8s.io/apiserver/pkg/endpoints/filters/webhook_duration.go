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

package filters

import (
	"context"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/endpoints/responsewriter"
)

var (
	// watchVerbs 不跟踪延迟：watch 是长连接，其“响应写入耗时”没有聚合意义，
	// 且会长时间占用 tracker 生命周期。
	watchVerbs = sets.NewString("watch")
)

// WithLatencyTrackers adds a LatencyTrackers instance to the
// context associated with a request so that we can measure latency
// incurred in various components within the apiserver.
//
// 中文说明：必须在 RequestInfo 过滤器之后安装，否则拿不到 verb 也就无法
// 区分 watch；缺失 RequestInfo 属于装配错误，直接 500 而不是静默降级。
func WithLatencyTrackers(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		requestInfo, ok := request.RequestInfoFrom(ctx)
		if !ok {
			handleError(w, req, http.StatusInternalServerError, nil, "no RequestInfo found in context, handler chain must be wrong")
			return
		}

		if watchVerbs.Has(requestInfo.Verb) {
			handler.ServeHTTP(w, req)
			return
		}

		// 包装 ResponseWriter 以捕获实际写响应的耗时；同时把 trackers 放入
		// 上下文，供 webhook 等组件记录各自阶段的延迟。
		req = req.WithContext(request.WithLatencyTrackers(ctx))
		w = responsewriter.WrapForHTTP1Or2(&writeLatencyTracker{
			ResponseWriter: w,
			ctx:            req.Context(),
		})

		handler.ServeHTTP(w, req)
	})
}

var _ http.ResponseWriter = &writeLatencyTracker{}
var _ responsewriter.UserProvidedDecorator = &writeLatencyTracker{}

// writeLatencyTracker 只包装 Write，把单次写入耗时累加到请求上下文中的
// LatencyTrackers。
type writeLatencyTracker struct {
	http.ResponseWriter
	ctx context.Context
}

func (wt *writeLatencyTracker) Unwrap() http.ResponseWriter {
	return wt.ResponseWriter
}

// Write 记录底层写入的耗时后再返回结果；不改变写入语义。
func (wt *writeLatencyTracker) Write(bs []byte) (int, error) {
	startedAt := time.Now()
	n, err := wt.ResponseWriter.Write(bs)
	request.TrackResponseWriteLatency(wt.ctx, time.Since(startedAt))
	return n, err
}
