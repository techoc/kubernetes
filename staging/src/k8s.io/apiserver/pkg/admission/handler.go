/*
Copyright 2015 The Kubernetes Authors.

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

package admission

import (
	"time"

	"k8s.io/apimachinery/pkg/util/sets"
)

const (
	// timeToWaitForReady is the amount of time to wait to let an admission controller to be ready to satisfy a request.
	// this is useful when admission controllers need to warm their caches before letting requests through.
	timeToWaitForReady = 10 * time.Second
)

// ReadyFunc is a function that returns true if the admission controller is ready to handle requests.
//
// ReadyFunc 用于判定准入控制器是否已经完成预热（例如 informer 缓存已同步），
// 只有返回 true 时才会放行请求。
type ReadyFunc func() bool

// Handler is a base for admission control handlers that
// support a predefined set of operations
//
// Handler 保存插件支持的 Operation 集合，并可选地持有一个就绪回调。
// 它可被嵌入到具体插件中，避免每个插件重复实现 Handles 逻辑。
type Handler struct {
	// operations 以字符串形式保存 Operation，方便 sets.Set 去重与快速查找。
	operations sets.Set[string]
	// readyFunc 可为 nil，表示无需等待预热；由 SetReadyFunc 延迟注册。
	readyFunc ReadyFunc
}

// Handles returns true for methods that this handler supports
func (h *Handler) Handles(operation Operation) bool {
	return h.operations.Has(string(operation))
}

// NewHandler creates a new base handler that handles the passed
// in operations
func NewHandler(ops ...Operation) *Handler {
	// 去重后存储，调用方重复传入同一 Operation 不会导致语义变化。
	operations := sets.New[string]()
	for _, op := range ops {
		operations.Insert(string(op))
	}
	return &Handler{
		operations: operations,
	}
}

// SetReadyFunc allows late registration of a ReadyFunc to know if the handler is ready to process requests.
//
// 延迟注册是为了让插件在构造时无需依赖尚未创建的外部组件（如 informer factory）；
// 注意该方法不保证并发安全，应在插件启动前调用。
func (h *Handler) SetReadyFunc(readyFunc ReadyFunc) {
	h.readyFunc = readyFunc
}

// WaitForReady will wait for the readyFunc (if registered) to return ready, and in case of timeout, will return false.
func (h *Handler) WaitForReady() bool {
	// there is no ready func configured, so we return immediately
	if h.readyFunc == nil {
		return true
	}

	// 以固定间隔轮询而非阻塞在通知上：就绪条件由外部组件决定，没有可等待的 channel。
	timeout := time.After(timeToWaitForReady)
	for !h.readyFunc() {
		select {
		case <-time.After(100 * time.Millisecond):
		case <-timeout:
			// 超时后再查一次，缩小最后一次检查与返回之间的竞态窗口。
			return h.readyFunc()
		}
	}
	return true
}
