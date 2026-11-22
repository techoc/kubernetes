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

package server

import (
	"sync"
)

/*
这里尝试识别 apiserver 生命周期中发生的事件，并为每个事件命名以便引用。

事件：
- ShutdownInitiated：收到 KILL 信号
- AfterShutdownDelayDuration：shutdown 延迟时长已过
- InFlightRequestsDrained：所有在途请求已排空
- HasBeenReady：readyz 端点首次成功时发出

以下是 ShutdownSendRetryAfter = false 时预期的关闭事件序列：

T0: ShutdownInitiated：收到 KILL 信号
	- /readyz 开始返回红色
    - 运行 pre shutdown hooks

T0+70s: AfterShutdownDelayDuration：shutdown 延迟时长已过
	- ShutdownDelayDuration 默认值为 70s
	- 此时开始关闭 HTTP Server，调用 server.Shutdown
	- 因此所有 listener 的 Close 被调用
	- HTTP Server 立即停止监听
	- 任何通过新 TCP socket 到达的新请求都会收到类似 'connection refused' 的网络错误
    - HTTP Server 在 ShutdownTimeout 内（默认 60s）优雅等待现有请求完成
	- 活跃的长运行请求会收到 GOAWAY。

T0+70s: HTTPServerStoppedListening：
	- HTTP Server 停止监听时发出，即紧接在 server.Shutdown 调用之后

T0 + 70s + 最多 60s: InFlightRequestsDrained：在途请求已排空
	- 长运行请求不在此范围内
	- 最多 60s：ShutdownTimeout 默认 60s，即在途请求有 60s 硬超时。
	- 此时应对审计事件调用 'Shutdown'，因为在途请求都已排空。


以下是 ShutdownSendRetryAfter = true 时预期的关闭事件序列：

T0: ShutdownInitiated：收到 KILL 信号
	- /readyz 开始返回红色
    - 运行 pre shutdown hooks

T0+70s: AfterShutdownDelayDuration：shutdown 延迟时长已过
	- ShutdownDelayDuration 默认值为 70s
	- HTTP Server 继续监听
	- apiserver 不再接受新请求
		- 包括新 TCP 连接上的请求，也包括已有 TCP 连接上的新请求
		- 此后到达的新请求返回 429，并带响应头 'Retry-After: 1' 与 'Connection: close'
	- 注意：这些新请求不会出现在审计日志中

T0 + 70s + 最多 60s: InFlightRequestsDrained：在途请求已排空
	- 长运行请求不在此范围内
	- 最多 60s：ShutdownTimeout 默认 60s，即在途请求有 60s 硬超时。
	- 调用 server.Shutdown，HTTP Server 立即停止监听
    - HTTP Server 在 '2s' 内优雅等待现有请求完成（当前为硬编码）
*/

// lifecycleSignal 封装一个具名的 apiserver 事件。
type lifecycleSignal interface {
	// Signal 触发事件，表示事件已发生。
	// Signal 是幂等的：一旦触发，事件保持已触发状态，并立即解除所有等待者的阻塞。
	Signal()

	// Signaled 返回一个通道，底层事件触发后该通道关闭。
	// 多次调用 Signaled 返回同一个值。
	Signaled() <-chan struct{}

	// Name 返回信号名称，便于日志记录。
	Name() string
}

// lifecycleSignals 抽象了 apiserver 生命周期中发生的各类事件，
// 使我们可以编写单元测试来验证预期的优雅终止行为。
//
// GenericAPIServer 可以用它来：
//   - 发出某个终止事件已发生的信号
//   - 等待某个终止事件发生后再执行动作。
type lifecycleSignals struct {
	// ShutdownInitiated 在 apiserver 关闭开始时发出。
	// 当主 goroutine 提供的 `stopCh` 收到 KILL 信号并因此关闭时发出。
	ShutdownInitiated lifecycleSignal

	// AfterShutdownDelayDuration 在 ShutdownInitiated 之后经过
	// ShutdownDelayDuration 时立即发出。
	// ShutdownDelayDuration 允许 apiserver 延迟一段时间再关闭。
	AfterShutdownDelayDuration lifecycleSignal

	// PreShutdownHooksStopped 在所有已注册的 preshutdown hook 执行完毕后发出。
	PreShutdownHooksStopped lifecycleSignal

	// NotAcceptingNewRequest 在服务器不再接受任何新请求时发出，
	// 此后到达的新请求都会收到错误。
	NotAcceptingNewRequest lifecycleSignal

	// InFlightRequestsDrained 在现有在途请求全部完成时发出，
	// 用作关闭审计后端的信号。
	InFlightRequestsDrained lifecycleSignal

	// HTTPServerStoppedListening 终止事件在 HTTP Server 停止监听底层 socket 时发出。
	HTTPServerStoppedListening lifecycleSignal

	// HasBeenReady 在 readyz 端点首次成功时发出。
	HasBeenReady lifecycleSignal

	// MuxAndDiscoveryComplete 在所有已知 HTTP 路径安装完成时发出。
	// 它主要用于避免在资源实际存在、但 handler 路径尚未安装时错误地返回 404。
	// 具体逻辑由使用通用 server 库的 APIServer 实现。
	MuxAndDiscoveryComplete lifecycleSignal
}

// ShuttingDown 返回服务器不再接受新请求时触发的生命周期信号。
// 这是暴露给请求处理逻辑、用于指示服务器正在关闭的生命周期事件。
func (s lifecycleSignals) ShuttingDown() <-chan struct{} {
	return s.NotAcceptingNewRequest.Signaled()
}

// newLifecycleSignals 返回用于协调 apiserver 生命周期的 lifecycleSignals 实例。
func newLifecycleSignals() lifecycleSignals {
	return lifecycleSignals{
		ShutdownInitiated:          newNamedChannelWrapper("ShutdownInitiated"),
		AfterShutdownDelayDuration: newNamedChannelWrapper("AfterShutdownDelayDuration"),
		PreShutdownHooksStopped:    newNamedChannelWrapper("PreShutdownHooksStopped"),
		NotAcceptingNewRequest:     newNamedChannelWrapper("NotAcceptingNewRequest"),
		InFlightRequestsDrained:    newNamedChannelWrapper("InFlightRequestsDrained"),
		HTTPServerStoppedListening: newNamedChannelWrapper("HTTPServerStoppedListening"),
		HasBeenReady:               newNamedChannelWrapper("HasBeenReady"),
		MuxAndDiscoveryComplete:    newNamedChannelWrapper("MuxAndDiscoveryComplete"),
	}
}

// newNamedChannelWrapper 创建一个以关闭通道实现的信号。
func newNamedChannelWrapper(name string) lifecycleSignal {
	return &namedChannelWrapper{
		name: name,
		once: sync.Once{},
		ch:   make(chan struct{}),
	}
}

// namedChannelWrapper 用一次性关闭的 channel 实现 lifecycleSignal。
// 关闭 channel 天然是幂等且并发安全的广播机制。
type namedChannelWrapper struct {
	name string
	once sync.Once
	ch   chan struct{}
}

// Signal 关闭通道；sync.Once 保证只关闭一次，避免重复 close 导致 panic。
func (e *namedChannelWrapper) Signal() {
	e.once.Do(func() {
		close(e.ch)
	})
}

// Signaled 返回只读的已关闭通道。
func (e *namedChannelWrapper) Signaled() <-chan struct{} {
	return e.ch
}

// Name 返回信号名称。
func (e *namedChannelWrapper) Name() string {
	return e.name
}
