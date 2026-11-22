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
	// muxAndDiscoveryIncompleteKey is a key under which a protection signal for all requests made before the server have installed all known HTTP paths is stored in the request's context
	//
	// 该键存在表示请求到达时 HTTP 路由尚未全部安装。值为空串或键缺失
	// 都被视为"已完成"，因此取值后只检查长度即可。
	muxAndDiscoveryIncompleteKey muxAndDiscoveryIncompleteKeyType = iota
)

// NoMuxAndDiscoveryIncompleteKey checks if the context contains muxAndDiscoveryIncompleteKey.
// The presence of the key indicates the request has been made when the HTTP paths weren't installed.
//
// NoMuxAndDiscoveryIncompleteKey 返回 true 表示路由已安装完毕
// （或未安装保护钩子），not found handler 可安全返回 404。
func NoMuxAndDiscoveryIncompleteKey(ctx context.Context) bool {
	muxAndDiscoveryCompleteProtectionKeyValue, _ := ctx.Value(muxAndDiscoveryIncompleteKey).(string)
	return len(muxAndDiscoveryCompleteProtectionKeyValue) == 0
}

// WithMuxAndDiscoveryComplete puts the muxAndDiscoveryIncompleteKey in the context if a request has been made before muxAndDiscoveryCompleteSignal has been ready.
// Putting the key protect us from returning a 404 response instead of a 503.
// It is especially important for controllers like GC and NS since they act on 404s.
//
// The presence of the key is checked in the NotFoundHandler (staging/src/k8s.io/apiserver/pkg/util/notfoundhandler/not_found_handler.go)
//
// The primary reason this filter exists is to protect from a potential race between the client's requests reaching the NotFoundHandler and the server becoming ready.
// Without the protection key a request could still get a 404 response when the registered signals changed their status just slightly before reaching the new handler.
// In that case, the presence of the key will make the handler return a 503 instead of a 404.
//
// WithMuxAndDiscoveryComplete 在路由未安装完成前给请求打标记，使
// not found handler 返回 503 而非 404。原因是垃圾回收器、命名空间控制器
// 等会基于 404 做出破坏性动作（例如认为对象不存在而重建），启动竞态
// 下误报 404 会导致错误决策；503 会被客户端按可重试处理。
func WithMuxAndDiscoveryComplete(handler http.Handler, muxAndDiscoveryCompleteSignal <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		// 信号为 nil 表示调用方不启用该保护；通道未关闭表示仍在安装路由。
		if muxAndDiscoveryCompleteSignal != nil && !isClosed(muxAndDiscoveryCompleteSignal) {
			req = req.WithContext(context.WithValue(req.Context(), muxAndDiscoveryIncompleteKey, "MuxAndDiscoveryInstallationNotComplete"))
		}
		handler.ServeHTTP(w, req)
	})
}

// isClosed is a convenience function that simply check if the given chan has been closed
//
// isClosed 用非阻塞 select 判断通道是否已关闭，避免在此处阻塞请求。
func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
