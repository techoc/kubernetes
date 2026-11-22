/*
Copyright 2018 The Kubernetes Authors.

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

package fake

import (
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
)

// 编译期断言：Backend 必须始终满足 audit.Backend 接口，接口变更会在此处暴露。
var _ audit.Backend = &Backend{}

// Backend is a fake audit backend for testing purposes.
//
// 仅用于测试：不落盘、不缓冲，收到批次即同步回调，便于用例断言事件内容。
// 非并发安全，回调方需自行保证不跨 goroutine 并发调用。
type Backend struct {
	// OnRequest 在每批事件处理时被调用；为 nil 时静默丢弃，方便只关心“是否被调用”的场景。
	OnRequest func(events []*auditinternal.Event)
}

// Run does nothing.
//
// 实现 audit.Backend 的生命周期接口；fake 后端没有后台协程，故立即返回。
func (b *Backend) Run(stopCh <-chan struct{}) error {
	return nil
}

// Shutdown does nothing.
//
// fake 后端无资源需要释放；保留该方法以满足接口契约。
func (b *Backend) Shutdown() {
	return
}

// ProcessEvents calls a callback on a batch, if present.
//
// 始终返回 true 表示批次已被接收（不重试），让上层审计逻辑按成功路径继续。
func (b *Backend) ProcessEvents(ev ...*auditinternal.Event) bool {
	if b.OnRequest != nil {
		b.OnRequest(ev)
	}
	return true
}

// String 返回空名称；fake 后端不参与审计后端命名与指标标签。
func (b *Backend) String() string {
	return ""
}
