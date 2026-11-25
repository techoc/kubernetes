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
)

// muxAndDiscoveryIncompleteKeyType 是未导出类型，避免与其他包的 context
// key 发生碰撞。
type muxAndDiscoveryIncompleteKeyType int

const (
	// muxAndDiscoveryIncompleteKey 是一个 context key，用于在服务端尚未安装完所有已知
	// HTTP 路径时，给在此之前发出的请求打上保护标记，存于请求的 context 中。
	// 该键存在表示请求到达时 HTTP 路由尚未全部安装。值为空串或键缺失
	// 都被视为"已完成"，因此取值后只检查长度即可。
	muxAndDiscoveryIncompleteKey muxAndDiscoveryIncompleteKeyType = iota
)

// NoMuxAndDiscoveryIncompleteKey 检查 context 中是否包含 muxAndDiscoveryIncompleteKey。
// 该键存在说明请求发出时 HTTP 路径尚未安装完成。
// NoMuxAndDiscoveryIncompleteKey 返回 true 表示路由已安装完毕
// （或未安装保护钩子），not found handler 可安全返回 404。
func NoMuxAndDiscoveryIncompleteKey(ctx context.Context) bool {
	muxAndDiscoveryCompleteProtectionKeyValue, _ := ctx.Value(muxAndDiscoveryIncompleteKey).(string)
	return len(muxAndDiscoveryCompleteProtectionKeyValue) == 0
}

// WithMuxAndDiscoveryComplete 如果请求在 muxAndDiscoveryCompleteSignal 就绪之前发出，
// 就往 context 中放入 muxAndDiscoveryIncompleteKey。放入该键可以避免返回 404 而不是 503，
// 这对 GC、命名空间控制器这类会对 404 采取动作的控制器尤其重要：它们会基于 404 做出
// 破坏性动作（例如认为对象不存在而重建），启动竞态下误报 404 会导致错误决策；
// 503 会被客户端按可重试处理。
//
// 该键是否存在由 NotFoundHandler 检查
// （staging/src/k8s.io/apiserver/pkg/util/notfoundhandler/not_found_handler.go）。
//
// 该过滤器存在的主要原因是防止客户端请求到达 NotFoundHandler 与服务端就绪之间的
// 潜在竞态。若没有该保护键，当已注册信号在请求到达新 handler 之前刚好改变状态时，
// 请求仍可能拿到 404。此时该键的存在会让 handler 返回 503 而不是 404。
func WithMuxAndDiscoveryComplete(handler http.Handler, muxAndDiscoveryCompleteSignal <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// 信号为 nil 表示调用方不启用该保护；通道未关闭表示仍在安装路由。
		if muxAndDiscoveryCompleteSignal != nil && !isClosed(muxAndDiscoveryCompleteSignal) {
			req = req.WithContext(context.WithValue(req.Context(), muxAndDiscoveryIncompleteKey, "MuxAndDiscoveryInstallationNotComplete"))
		}
		handler.ServeHTTP(w, req)
	})
}

// isClosed 是一个便捷函数，用非阻塞 select 判断给定 chan 是否已关闭，
// 避免在此处阻塞请求。
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
