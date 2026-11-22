/*
Copyright 2017 The Kubernetes Authors.

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

package audit

import (
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
)

// Sink 是审计事件接收端的最小接口。所有实现都必须满足下面的并发与所有权约定，
// 因为这些约定是审计管道与后端之间唯一的契约。
type Sink interface {
	// ProcessEvents handles events. Per audit ID it might be that ProcessEvents is called up to three times.
	// Errors might be logged by the sink itself. If an error should be fatal, leading to an internal
	// error, ProcessEvents is supposed to panic. The event must not be mutated and is reused by the caller
	// after the call returns, i.e. the sink has to make a deepcopy to keep a copy around if necessary.
	// Returns true on success, may return false on error.
	//
	// 同一审计 ID 最多会被调用三次，分别对应请求的不同阶段（收到请求、响应头、响应体）。
	// 事件对象在返回后即被调用方复用，sink 若要保留必须自行深拷贝；这是避免每请求多次
	// 分配的关键约定，不要“顺手”改掉。
	ProcessEvents(events ...*auditinternal.Event) bool
}

// Backend 是可运行、可关闭的 Sink，由 apiserver 的生命周期统一管理。
type Backend interface {
	Sink

	// Run will initialize the backend. It must not block, but may run go routines in the background. If
	// stopCh is closed, it is supposed to stop them. Run will be called before the first call to ProcessEvents.
	//
	// 不得阻塞：它在服务启动路径上被同步调用。后台 goroutine 必须在 stopCh 关闭后退出，
	// 否则 Shutdown 期间会泄漏。
	Run(stopCh <-chan struct{}) error

	// Shutdown will synchronously shut down the backend while making sure that all pending
	// events are delivered. It can be assumed that this method is called after
	// the stopCh channel passed to the Run method has been closed.
	//
	// 同步语义是审计完整性的保证：进程退出前必须把缓冲中的事件刷出去。
	Shutdown()

	// Returns the backend PluginName.
	String() string
}
