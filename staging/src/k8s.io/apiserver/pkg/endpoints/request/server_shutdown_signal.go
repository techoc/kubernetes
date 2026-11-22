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

package request

import (
	"context"
)

// The serverShutdownSignalKeyType type is unexported to prevent collisions
//
// 未导出类型确保 context key 不会与其他包的同名 key 冲突。
type serverShutdownSignalKeyType int

// serverShutdownSignalKey is the context key for storing the
// watch termination interface instance for a WATCH request.
//
// serverShutdownSignalKey 是存放 ServerShutdownSignal 的 context 键，
// 仅用于长连接（watch）请求。
const serverShutdownSignalKey serverShutdownSignalKeyType = iota

// ServerShutdownSignal is associated with the request context so
// the request handler logic has access to signals rlated to
// the server shutdown events
//
// ServerShutdownSignal 让长连接处理器感知优雅终止：关闭期间不再接收
// 新请求，watch 需要主动结束，否则会拖住进程退出。
type ServerShutdownSignal interface {
	// Signaled when the apiserver is not receiving any new request
	// 通道关闭表示 apiserver 已停止接收新请求，watch 应尽快结束。
	ShuttingDown() <-chan struct{}
}

// ServerShutdownSignalFrom returns the ServerShutdownSignal instance
// associated with the request context.
// If there is no ServerShutdownSignal asscoaied with the context,
// nil is returned.
//
// ServerShutdownSignalFrom 返回请求上下文中的关闭信号；未设置时返回
// nil，调用方必须做 nil 判断（在测试或非优雅关闭路径下可能出现）。
func ServerShutdownSignalFrom(ctx context.Context) ServerShutdownSignal {
	ev, _ := ctx.Value(serverShutdownSignalKey).(ServerShutdownSignal)
	return ev
}

// WithServerShutdownSignal returns a new context that stores
// the ServerShutdownSignal interface instance.
//
// WithServerShutdownSignal 把关闭信号放入上下文。若父上下文已存在信号
// 则原样返回，避免重复注册或覆盖既有信号。
func WithServerShutdownSignal(parent context.Context, window ServerShutdownSignal) context.Context {
	if ServerShutdownSignalFrom(parent) != nil {
		return parent // Avoid double registering.
	}

	return context.WithValue(parent, serverShutdownSignalKey, window)
}
