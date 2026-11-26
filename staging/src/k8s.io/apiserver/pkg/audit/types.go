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
	// ProcessEvents 处理事件。同一个 audit ID  ProcessEvents 最多可能被调用三次。
	// sink 自身可能已记录错误。若某个错误是致命的、应导致内部错误，
	// ProcessEvents 应当 panic。事件不得被修改，调用方会在返回后复用它，
	// 因此 sink 若需保留副本必须深拷贝。
	// 成功返回 true，出错可返回 false。
	//
	// 同一审计 ID 最多会被调用三次，分别对应请求的不同阶段（收到请求、响应头、响应体）。
	// 事件对象在返回后即被调用方复用，sink 若要保留必须自行深拷贝；这是避免每请求多次
	// 分配的关键约定，不要“顺手”改掉。
	ProcessEvents(events ...*auditinternal.Event) bool
}

// Backend 是可运行、可关闭的 Sink，由 apiserver 的生命周期统一管理。
type Backend interface {
	Sink

	// Run 初始化后端。它不得阻塞，但可在后台运行 goroutine。若
	// stopCh 关闭，应停止它们。Run 在首次调用 ProcessEvents 之前被调用。
	//
	// 不得阻塞：它在服务启动路径上被同步调用。后台 goroutine 必须在 stopCh 关闭后退出，
	// 否则 Shutdown 期间会泄漏。
	Run(stopCh <-chan struct{}) error

	// Shutdown 同步关闭后端，同时确保所有待处理
	// 事件都已投递。可假定调用时传给 Run 的 stopCh
	// 已经关闭。
	//
	// 同步语义是审计完整性的保证：进程退出前必须把缓冲中的事件刷出去。
	Shutdown()

	// 返回后端的 PluginName。
	String() string
}
