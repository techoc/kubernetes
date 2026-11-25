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

package healthz

import (
	"net/http"
	"sync"

	"k8s.io/apiserver/pkg/server/healthz"
	"k8s.io/apiserver/pkg/server/mux"
)

// MutableHealthzHandler returns a http.Handler that handles "/healthz"
// following the standard healthz mechanism.
//
// 标准 healthz handler 不允许在创建后再添加检查项。
// MutableHealthzHandler 通过“加检查时重建内部 handler + 读写锁”绕开这个限制，
// 使 controller-manager 能在启动后继续把控制器的健康检查挂到 /healthz 上。
type MutableHealthzHandler struct {
	// handler 是当前实际处理请求的底层 handler；每次新增检查都会被替换成新的 mux。
	handler http.Handler
	// mutex 是一个 RWMutex：读锁用于并发处理健康检查请求，写锁用于安全替换 handler。
	mutex  sync.RWMutex
	checks []healthz.HealthChecker
}

// 处理请求时加读锁：多个健康检查可并行执行，但不会与 handler 替换同时发生。
func (h *MutableHealthzHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	h.mutex.RLock()
	// 持有读锁直到响应结束，确保期间 handler 不会被换掉。
	defer h.mutex.RUnlock()

	h.handler.ServeHTTP(writer, request)
}

// AddHealthChecker 向 handler 追加健康检查项并重建内部 handler。
// 每次调用都要重建 mux，因此建议尽量批量添加，减少重建次数。
func (h *MutableHealthzHandler) AddHealthChecker(checks ...healthz.HealthChecker) {
	h.mutex.Lock()
	// 重建期间独占访问，避免与正在进行的请求/其它添加操作竞争。
	defer h.mutex.Unlock()

	h.checks = append(h.checks, checks...)
	// 把新检查累积到列表中，保留之前添加的全部检查。
	newMux := mux.NewPathRecorderMux("healthz")
	// 创建一个新的 mux，重新安装全部检查（包括新加的）。
	healthz.InstallHandler(newMux, h.checks...)
	// 由 apiserver 的 healthz 包生成 /healthz 及各个子路径的路由。
	h.handler = newMux
	// 原子替换（在写锁保护下）为新的 handler；之后的请求都会走到新路由。
}

// 用初始检查集合构造 handler；统一走 AddHealthChecker 以复用重建逻辑。
func NewMutableHealthzHandler(checks ...healthz.HealthChecker) *MutableHealthzHandler {
	h := &MutableHealthzHandler{}
	h.AddHealthChecker(checks...)

	return h
}
