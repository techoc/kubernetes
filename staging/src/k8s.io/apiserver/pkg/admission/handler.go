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
	// timeToWaitForReady 是给准入控制器预留的就绪等待时长，用于满足请求前的预热。
	// 适用于准入控制器需要在放行请求前先预热缓存的场景。
	timeToWaitForReady = 10 * time.Second
)

// ReadyFunc 用于判定准入控制器是否已经完成预热（例如 informer 缓存已同步），
// 只有返回 true 时才会放行请求。
type ReadyFunc func() bool

// Handler 是支持固定 Operation 集合的准入控制 handler 基类，
// 保存插件支持的 Operation 集合，并可选地持有一个就绪回调。
// 它可被嵌入到具体插件中，避免每个插件重复实现 Handles 逻辑。
type Handler struct {
	// operations 以字符串形式保存 Operation，方便 sets.Set 去重与快速查找。
	operations sets.Set[string]
	// readyFunc 可为 nil，表示无需等待预热；由 SetReadyFunc 延迟注册。
	readyFunc ReadyFunc
}

// Handles 在本 handler 支持的操作上返回 true
func (h *Handler) Handles(operation Operation) bool {
	return h.operations.Has(string(operation))
}

// NewHandler 创建一个新的基础 handler，处理传入的 operations
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

// SetReadyFunc 允许延迟注册一个 ReadyFunc，用于判断 handler 是否已准备好处理请求。
//
// 延迟注册是为了让插件在构造时无需依赖尚未创建的外部组件（如 informer factory）；
// 注意该方法不保证并发安全，应在插件启动前调用。
func (h *Handler) SetReadyFunc(readyFunc ReadyFunc) {
	h.readyFunc = readyFunc
}

// WaitForReady 会等待 readyFunc（若已注册）返回就绪，超时则返回 false。
func (h *Handler) WaitForReady() bool {
	// 未配置 readyFunc，直接返回
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
