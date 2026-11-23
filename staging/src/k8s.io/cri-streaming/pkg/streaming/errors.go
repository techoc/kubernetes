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

package streaming

import (
	"net/http"
	"strconv"

	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

// NewErrorTooManyInFlight creates an error for exceeding the maximum number of in-flight requests.
// NewErrorTooManyInFlight 构造一个"进行中请求数超出上限"的错误。
// 它使用 gRPC 的 ResourceExhausted 状态码，便于后续被 WriteError 统一映射为 HTTP 429。
func NewErrorTooManyInFlight() error {
	return grpcstatus.Error(codes.ResourceExhausted, "maximum number of in-flight requests exceeded")
}

// WriteError translates a CRI streaming error into an appropriate HTTP response.
// WriteError 把 gRPC 风格的错误映射为合适的 HTTP 响应，写回给流式请求的客户端。
//
// 映射规则：
//   - codes.NotFound -> 404 Not Found（token 不存在或请求已过期）；
//   - codes.ResourceExhausted -> 429 Too Many Requests，并附带 Retry-After 头；
//   - 其他状态码 -> 500 Internal Server Error。
//
// 响应体为错误消息文本；函数返回写响应体时发生的错误（如果有）。
func WriteError(err error, w http.ResponseWriter) error {
	s, _ := grpcstatus.FromError(err)
	var status int
	switch s.Code() {
	case codes.NotFound:
		status = http.StatusNotFound
	case codes.ResourceExhausted:
		// We only expect to hit this if there is a DoS, so we just wait the full TTL.
		// If this is ever hit in steady-state operations, consider increasing the maxInFlight requests,
		// or plumbing through the time to next expiration.
		// 该状态通常只在遭受 DoS 攻击时出现，因此直接让客户端等待完整的 TTL 再重试。
		// 若在稳态运行中频繁出现，应考虑调大 maxInFlight，或精确计算距离下一次过期的时间。
		w.Header().Set("Retry-After", strconv.Itoa(int(cacheTTL.Seconds())))
		status = http.StatusTooManyRequests
	default:
		// 未知错误统一按服务端内部错误处理，不向客户端暴露细节。
		status = http.StatusInternalServerError
	}
	// 先写状态码，再把错误消息作为响应体写出。
	w.WriteHeader(status)
	_, writeErr := w.Write([]byte(err.Error()))
	return writeErr
}
