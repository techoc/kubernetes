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

package flowcontrol

import (
	"context"
	"sync"
)

// priorityAndFairnessKeyType 是 context key 的私有类型，避免与其他包的 key 冲突。
type priorityAndFairnessKeyType int

const (
	// priorityAndFairnessInitializationSignalKey 是在 context 中存放
	// watch 请求初始化信号函数的 key。
	// 存放在 context 中。
	priorityAndFairnessInitializationSignalKey priorityAndFairnessKeyType = iota
)

// WithInitializationSignal 创建 parent context 的副本，
// 并设置优先级与公平性的初始化信号值。
func WithInitializationSignal(ctx context.Context, signal InitializationSignal) context.Context {
	return context.WithValue(ctx, priorityAndFairnessInitializationSignalKey, signal)
}

// initializationSignalFrom 返回一个初始化信号函数，
// 调用时向优先级与公平性调度器通知 watch 初始化已完成。
// 通知优先级与公平性调度器。
func initializationSignalFrom(ctx context.Context) (InitializationSignal, bool) {
	signal, ok := ctx.Value(priorityAndFairnessInitializationSignalKey).(InitializationSignal)
	return signal, ok && signal != nil
}

// WatchInitialized 向优先级与公平性调度器发送信号，
// 表示给定 watch 请求已完成初始化。
//
// 只有 watch 的首次同步完成才需要通知调度器：在此之前占用的并发份额不能释放，
// 否则会在缓存未就绪时涌入大量请求。
func WatchInitialized(ctx context.Context) {
	if signal, ok := initializationSignalFrom(ctx); ok {
		signal.Signal()
	}
}

// RequestDelegated 通知优先级与公平性调度器：
// 给定请求已被委托给聚合 API server。
// 优先级与公平性未启用时为空操作。
func RequestDelegated(ctx context.Context) {
	// watch 初始化信号不跨越请求边界，因此只要知道请求不会在本机处理，
	// 就大方可触发该信号。对非 watch 请求调用也是安全的。
	//
	// 对非 watch 请求也是安全的。
	WatchInitialized(ctx)
}

// InitializationSignal 是允许发送与处理初始化信号的接口。
// initialization signals.
type InitializationSignal interface {
	// Signal 通知调度器初始化已完成。
	Signal()
	// Wait 等待初始化信号。
	Wait()
}

// initializationSignal 用 once+channel 实现一次性广播：重复 Signal 安全，
// 多个等待者都能被唤醒。
type initializationSignal struct {
	once sync.Once
	done chan struct{}
}

// NewInitializationSignal 返回一个未触发的初始化信号。
func NewInitializationSignal() InitializationSignal {
	return &initializationSignal{
		once: sync.Once{},
		done: make(chan struct{}),
	}
}

// Signal 关闭 channel；once 保证并发重复调用不会 panic。
func (i *initializationSignal) Signal() {
	i.once.Do(func() { close(i.done) })
}

// Wait 阻塞直到 Signal 被调用或 channel 被关闭。
func (i *initializationSignal) Wait() {
	<-i.done
}
