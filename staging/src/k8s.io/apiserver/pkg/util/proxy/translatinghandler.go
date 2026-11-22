/*
Copyright 2023 The Kubernetes Authors.

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

package proxy

import (
	"net/http"

	"k8s.io/klog/v2"
)

// translatingHandler wraps the delegate handler, implementing the
// http.Handler interface. The delegate handles all requests unless
// the request satisfies the passed "shouldTranslate" function
// (currently only for WebSocket/V5 request), in which case the translator
// handles the request.
//
// 中文说明：按请求形态分流而非按路径，是因为同一代理端点既要兼容旧版
// SPDY/V4 客户端，又要支持新协议；翻译层只处理无法直接透传的请求。
type translatingHandler struct {
	delegate        http.Handler
	translator      http.Handler
	shouldTranslate func(*http.Request) bool
}

// NewTranslatingHandler 构造分流 handler。
//
// 中文说明：shouldTranslate 由调用方注入，便于按 Upgrade 头或子协议判定，
// 而无需在此包内硬编码协议细节。
func NewTranslatingHandler(delegate http.Handler, translator http.Handler, shouldTranslate func(*http.Request) bool) http.Handler {
	return &translatingHandler{
		delegate:        delegate,
		translator:      translator,
		shouldTranslate: shouldTranslate,
	}
}

// ServeHTTP 按需选择翻译器或直接委托。
func (t *translatingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	if t.shouldTranslate(req) {
		// V(4) 级别日志：仅在开启高详细度时记录，避免每个连接都产生日志。
		klog.FromContext(req.Context()).V(4).Info("Request handled by translator proxy")
		t.translator.ServeHTTP(w, req)
		return
	}
	t.delegate.ServeHTTP(w, req)
}
