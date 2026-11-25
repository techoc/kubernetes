/*
Copyright 2022 The Kubernetes Authors.

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

// Package synctrack 提供工具，帮助控制器跟踪自己是否已经“同步”，
// 也就是是否已处理完 informer 初始列表中的全部条目。
package synctrack

import (
	"context"
	"sync"
	"sync/atomic"

	"k8s.io/apimachinery/pkg/util/sets"
)

// AsyncTracker 在存在多个工作线程的情况下帮助传播 HasSynced。
// 使用方需要自行监控上游的“已同步”状态，并在其从 false 变为 true 时通知跟踪器。
type AsyncTracker[T comparable] struct {
	// name 描述该实例。
	name string

	// upstreamHasSynced 在调用 UpstreamHasSynced 时从 false（初始值）变为 true。
	upstreamHasSynced atomic.Bool

	lock    sync.Mutex
	waiting sets.Set[T]

	// synced 会在跟踪器与上游都完成同步后被取消。
	// 用 context 实现很方便：它自带 channel 并处理了线程安全。
	synced context.Context
	cancel func()
}

// NewAsyncTracker 创建跟踪器；synced 上下文在满足同步条件时被取消。
func NewAsyncTracker[T comparable](name string) *AsyncTracker[T] {
	t := &AsyncTracker[T]{
		name: name,
	}
	t.synced, t.cancel = context.WithCancel(context.Background())
	return t
}

// Start 应在开始处理某个属于初始列表的 key 之前调用。
func (t *AsyncTracker[T]) Start(key T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.waiting == nil {
		t.waiting = sets.New[T](key)
	} else {
		t.waiting.Insert(key)
	}
}

// Finished 应在处理完某个属于初始列表的 key 之后调用。由于 key 是逐个跟踪的，
// 在没有配对的 Start 调用时直接调用 Finished 也不会造成问题。这让它可以方便地
// 与不方便传递 isInInitialList 布尔值的队列等结构配合使用。
func (t *AsyncTracker[T]) Finished(key T) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if t.waiting != nil {
		t.waiting.Delete(key)
	}

	// 现在可能已经同步了？
	if t.upstreamHasSynced.Load() && len(t.waiting) == 0 {
		// 标记为已同步。
		t.cancel()
	}
}

// 上游的“已同步”变为 true 后，需要尽快至少调用一次 UpstreamHasSynced，
// 告知 AsyncTracker 数据源已经同步。
//
// 必须在把初始列表逐个移交给 Start 之后调用。
func (t *AsyncTracker[T]) UpstreamHasSynced() {
	// 上游已完成，但本端可能还没处理完。
	t.upstreamHasSynced.Store(true)
	t.lock.Lock()
	defer t.lock.Unlock()
	if len(t.waiting) == 0 {
		// 标记为已同步。
		t.cancel()
	}
}

// HasSynced 在数据源已同步且初始列表中的每个 key 都已处理完毕时返回 true。
// 这依赖数据源直到*投递完最后一个 key 的通知之后*才认为自身已同步，
// 且该通知的处理函数必须已经调用过 Start。
func (t *AsyncTracker[T]) HasSynced() bool {
	return t.synced.Err() != nil
}

// Done 返回一个 channel：当数据源已同步且初始列表中的每个 key 都已处理完毕时
// 被关闭。依赖条件与 HasSynced 相同。
func (t *AsyncTracker[T]) Done() <-chan struct{} {
	return t.synced.Done()
}

// Name 返回实例名，用于日志与错误信息。
func (t *AsyncTracker[T]) Name() string {
	return t.name
}

// SingleFileTracker 在事件按序处理（例如经由队列）时帮助传播 HasSynced。
// 使用方需要自行监控上游的“已同步”状态，并在其从 false 变为 true 时通知跟踪器。
type SingleFileTracker struct {
	// name 描述该实例。
	name string

	// 重要：count 通过原子操作访问，必须保持 64 位对齐，否则原子操作会 panic。
	// 把它放在结构体开头，即使在 32 位架构上也能保证对齐。
	// 详见 https://pkg.go.dev/sync/atomic#pkg-note-BUG。
	count int64

	// upstreamHasSynced 在调用 UpstreamHasSynced 时从 false（初始值）变为 true。
	upstreamHasSynced atomic.Bool

	// synced 会在跟踪器与上游都完成同步后被取消。
	// 用 context 实现很方便：它自带 channel 并处理了线程安全。
	synced context.Context
	cancel func()
}

// NewSingleFileTracker 创建顺序处理场景的跟踪器。
func NewSingleFileTracker(name string) *SingleFileTracker {
	t := &SingleFileTracker{
		name: name,
	}
	t.synced, t.cancel = context.WithCancel(context.Background())
	return t
}

// Start 应在开始处理某个属于初始列表的 key 之前调用。
func (t *SingleFileTracker) Start() {
	atomic.AddInt64(&t.count, 1)
}

// Finished 应在处理完某个属于初始列表的 key 之后调用。绝不能在对应的 Start()
// 之前（或脱离 Start()）调用 Finished()，那是可能导致 HasSynced 返回错误值的
// 逻辑错误。为便于发现问题，内部计数器变为负数时 Finished() 会 panic。
func (t *SingleFileTracker) Finished() {
	result := atomic.AddInt64(&t.count, -1)
	if result < 0 {
		panic("synctrack: negative counter; this logic error means HasSynced may return incorrect value")
	}

	// 现在可能已经同步了？
	if result == 0 && t.upstreamHasSynced.Load() {
		// 标记为已同步。
		t.cancel()
	}
}

// 上游的“已同步”变为 true 后，需要尽快至少调用一次 UpstreamHasSynced，
// 告知 SingleFileTracker 数据源已经同步。
//
// 必须在把初始列表逐个移交给 Start 之后调用。
func (t *SingleFileTracker) UpstreamHasSynced() {
	// 上游已完成，但本端可能还没处理完。
	t.upstreamHasSynced.Store(true)
	if atomic.LoadInt64(&t.count) == 0 {
		// 标记为已同步。
		t.cancel()
	}
}

// HasSynced 在数据源已同步且初始列表中的每个 key 都已处理完毕时返回 true。
// 这依赖数据源直到*投递完最后一个 key 的通知之后*才认为自身已同步，
// 且该通知的处理函数必须已经调用过 Start。
func (t *SingleFileTracker) HasSynced() bool {
	return t.synced.Err() != nil
}

// Done 返回一个 channel：当数据源已同步且初始列表中的每个 key 都已处理完毕时
// 被关闭。依赖条件与 HasSynced 相同。
func (t *SingleFileTracker) Done() <-chan struct{} {
	return t.synced.Done()
}

// Name 返回实例名，用于日志与错误信息。
func (t *SingleFileTracker) Name() string {
	return t.name
}
