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

// serverShutdownSignalKeyType 未导出，确保 context key 不会与其他包的同名 key 冲突。
type serverShutdownSignalKeyType int

// serverShutdownSignalKey 是存放 WATCH 请求终止接口实例的 context 键，
// 仅用于长连接（watch）请求。
const serverShutdownSignalKey serverShutdownSignalKeyType = iota

// ServerShutdownSignal 与请求上下文关联，使请求处理逻辑能够感知服务器关闭事件相关的信号。
// ServerShutdownSignal 让长连接处理器感知优雅终止：关闭期间不再接收
// 新请求，watch 需要主动结束，否则会拖住进程退出。
type ServerShutdownSignal interface {
	// ShuttingDown 在 apiserver 不再接收任何新请求时被触发。
	// 通道关闭表示 apiserver 已停止接收新请求，watch 应尽快结束。
	ShuttingDown() <-chan struct{}
}

// ServerShutdownSignalFrom 返回与请求上下文关联的 ServerShutdownSignal 实例；
// 若上下文中没有该信号，则返回 nil。
// ServerShutdownSignalFrom 返回请求上下文中的关闭信号；未设置时返回
// nil，调用方必须做 nil 判断（在测试或非优雅关闭路径下可能出现）。
func ServerShutdownSignalFrom(ctx context.Context) ServerShutdownSignal {
	ev, _ := ctx.Value(serverShutdownSignalKey).(ServerShutdownSignal)
	return ev
}

// WithServerShutdownSignal 返回存储了 ServerShutdownSignal 接口实例的新 context。
// WithServerShutdownSignal 把关闭信号放入上下文。若父上下文已存在信号
// 则原样返回，避免重复注册或覆盖既有信号。
func WithServerShutdownSignal(parent context.Context, window ServerShutdownSignal) context.Context {
	if ServerShutdownSignalFrom(parent) != nil {
		return parent // 避免重复注册。
	}

	return context.WithValue(parent, serverShutdownSignalKey, window)
}
