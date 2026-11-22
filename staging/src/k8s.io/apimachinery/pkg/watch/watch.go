/*
Copyright 2014 The Kubernetes Authors.

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

package watch

import (
	"fmt"
	"sync"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

// Interface 由任何了解如何 watch 并上报变更的实现提供。
type Interface interface {
	// Stop 告知生产者消费者已停止 watch，生产者应停止发送事件并关闭结果通道。
	// 消费者应继续接收事件，直到结果通道被关闭。
	//
	// 某些实现可能在构造时就创建了通道，因此即使消费者尚未调用 ResultChan()，
	// Stop 也必须被调用。
	//
	// 只有消费者应调用 Stop()，生产者不应调用。若生产者出错需要提前结束 watch，
	// 应改为发送一个 Error 事件并关闭结果通道。
	Stop()

	// ResultChan 返回接收事件生产者所发事件的通道。
	// 若发生错误或调用了 Stop()，生产者必须关闭该通道并释放 watch 占用的资源。
	// 通道关闭即表示不会再有事件。
	ResultChan() <-chan Event
}

// EventType 定义事件的可能类型。
type EventType string

const (
	// Added 表示对象被创建。
	Added EventType = "ADDED"
	// Modified 表示对象被更新。
	Modified EventType = "MODIFIED"
	// Deleted 表示对象被删除。
	Deleted EventType = "DELETED"
	// Bookmark 表示仅携带资源版本的书签事件，用于快速恢复 watch。
	Bookmark EventType = "BOOKMARK"
	// Error 表示 watch 过程中发生错误。
	Error EventType = "ERROR"
)

var (
	// DefaultChanSize 是 fake watcher 默认的结果通道缓冲大小。
	DefaultChanSize int32 = 100
)

// Event 表示被 watch 资源上的单个事件。
// +k8s:deepcopy-gen=true
type Event struct {
	Type EventType

	// Object 的语义随 Type 变化：
	//  * Type 为 Added 或 Modified：对象的新状态。
	//  * Type 为 Deleted：对象被删除前的最后状态。
	//  * Type 为 Bookmark：被 watch 类型的一个实例，其中仅设置 ResourceVersion。
	//    从书签的资源版本成功重启 watch 后，客户端保证既不会收到重复事件也不会漏事件。
	//  * Type 为 Error：推荐使用 *api.Status；其他类型也可视上下文而定。
	Object runtime.Object
}

type emptyWatch chan Event

// NewEmptyWatch 返回一个不产生任何结果且已关闭的 watch 接口。
// 适用于“没有可用信息但又不构成错误”的场景，调用方可安全读取并立即得知通道已关闭。
func NewEmptyWatch() Interface {
	ch := make(chan Event)
	close(ch)
	return emptyWatch(ch)
}

// Stop 实现 Interface。
func (w emptyWatch) Stop() {
}

// ResultChan 实现 Interface。
func (w emptyWatch) ResultChan() <-chan Event {
	return chan Event(w)
}

// FakeWatcher 用于测试任何消费 watch.Interface 的代码；线程安全。
// 注意：事件发送是阻塞的，消费者必须及时读取，否则 Add 等方法会阻塞。
type FakeWatcher struct {
	logger  klog.Logger
	result  chan Event
	stopped bool
	sync.Mutex
}

var _ Interface = &FakeWatcher{}

// Contextual logging: NewFakeWithOptions and a logger in the FakeOptions should be used instead in code which supports contextual logging.
// NewFake 创建使用默认选项的 FakeWatcher。
func NewFake() *FakeWatcher {
	return NewFakeWithOptions(FakeOptions{})
}

// Contextual logging: NewFakeWithOptions and a logger in the FakeOptions should be used instead in code which supports contextual logging.
// NewFakeWithChanSize 创建指定通道缓冲大小的 FakeWatcher。
// blocking 参数当前未使用（保留以兼容旧签名）。
func NewFakeWithChanSize(size int, blocking bool) *FakeWatcher {
	return NewFakeWithOptions(FakeOptions{ChannelSize: size})
}

// NewFakeWithOptions 按选项创建 FakeWatcher；未提供 logger 时使用后台 logger。
func NewFakeWithOptions(options FakeOptions) *FakeWatcher {
	return &FakeWatcher{
		logger: ptr.Deref(options.Logger, klog.Background()),
		result: make(chan Event, options.ChannelSize),
	}
}

// FakeOptions 是 FakeWatcher 的构造选项。
type FakeOptions struct {
	Logger      *klog.Logger
	ChannelSize int
}

// Stop 实现 Interface.Stop()。重复调用是安全的，只会关闭通道一次。
func (f *FakeWatcher) Stop() {
	f.Lock()
	defer f.Unlock()
	if !f.stopped {
		f.logger.V(4).Info("Stopping fake watcher")
		close(f.result)
		f.stopped = true
	}
}

// IsStopped 返回是否已经调用过 Stop。
func (f *FakeWatcher) IsStopped() bool {
	f.Lock()
	defer f.Unlock()
	return f.stopped
}

// Reset 重置 watcher 以便复用，并重新创建结果通道。
func (f *FakeWatcher) Reset() {
	f.Lock()
	defer f.Unlock()
	f.stopped = false
	f.result = make(chan Event)
}

func (f *FakeWatcher) ResultChan() <-chan Event {
	return f.result
}

// Add 发送一个 add 事件。
func (f *FakeWatcher) Add(obj runtime.Object) {
	f.result <- Event{Added, obj}
}

// Modify 发送一个 modify 事件。
func (f *FakeWatcher) Modify(obj runtime.Object) {
	f.result <- Event{Modified, obj}
}

// Delete 发送一个 delete 事件。
func (f *FakeWatcher) Delete(lastValue runtime.Object) {
	f.result <- Event{Deleted, lastValue}
}

// Error 发送一个 Error 事件。
func (f *FakeWatcher) Error(errValue runtime.Object) {
	f.result <- Event{Error, errValue}
}

// Action 发送指定类型的事件，便于表驱动测试。
func (f *FakeWatcher) Action(action EventType, obj runtime.Object) {
	f.result <- Event{action, obj}
}

// RaceFreeFakeWatcher 用于测试任何消费 watch.Interface 的代码；线程安全。
// 与 FakeWatcher 不同，它在持锁状态下非阻塞发送：通道满时 panic，
// 从而让测试暴露消费不及时的问题而不是静默丢事件。
type RaceFreeFakeWatcher struct {
	logger  klog.Logger
	result  chan Event
	Stopped bool
	sync.Mutex
}

var _ Interface = &RaceFreeFakeWatcher{}

// Contextual logging: RaceFreeFakeWatcherWithLogger should be used instead of NewRaceFreeFake in code which supports contextual logging.
// NewRaceFreeFake 创建使用后台 logger 的 RaceFreeFakeWatcher。
func NewRaceFreeFake() *RaceFreeFakeWatcher {
	return NewRaceFreeFakeWithLogger(klog.Background())
}

// NewRaceFreeFakeWithLogger 使用指定 logger 创建 RaceFreeFakeWatcher，
// 结果通道容量为 DefaultChanSize。
func NewRaceFreeFakeWithLogger(logger klog.Logger) *RaceFreeFakeWatcher {
	return &RaceFreeFakeWatcher{
		logger: logger,
		result: make(chan Event, DefaultChanSize),
	}
}

// Stop 实现 Interface.Stop()。
func (f *RaceFreeFakeWatcher) Stop() {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		f.logger.V(4).Info("Stopping fake watcher")
		close(f.result)
		f.Stopped = true
	}
}

// IsStopped 返回是否已经调用过 Stop。
func (f *RaceFreeFakeWatcher) IsStopped() bool {
	f.Lock()
	defer f.Unlock()
	return f.Stopped
}

// Reset 重置 watcher 以便复用，并重新创建结果通道。
func (f *RaceFreeFakeWatcher) Reset() {
	f.Lock()
	defer f.Unlock()
	f.Stopped = false
	f.result = make(chan Event, DefaultChanSize)
}

func (f *RaceFreeFakeWatcher) ResultChan() <-chan Event {
	f.Lock()
	defer f.Unlock()
	return f.result
}

// Add 发送一个 add 事件；已停止时静默丢弃，通道满时 panic。
func (f *RaceFreeFakeWatcher) Add(obj runtime.Object) {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		select {
		case f.result <- Event{Added, obj}:
			return
		default:
			panic(fmt.Errorf("channel full"))
		}
	}
}

// Modify 发送一个 modify 事件。
func (f *RaceFreeFakeWatcher) Modify(obj runtime.Object) {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		select {
		case f.result <- Event{Modified, obj}:
			return
		default:
			panic(fmt.Errorf("channel full"))
		}
	}
}

// Delete 发送一个 delete 事件。
func (f *RaceFreeFakeWatcher) Delete(lastValue runtime.Object) {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		select {
		case f.result <- Event{Deleted, lastValue}:
			return
		default:
			panic(fmt.Errorf("channel full"))
		}
	}
}

// Error 发送一个 Error 事件。
func (f *RaceFreeFakeWatcher) Error(errValue runtime.Object) {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		select {
		case f.result <- Event{Error, errValue}:
			return
		default:
			panic(fmt.Errorf("channel full"))
		}
	}
}

// Action 发送指定类型的事件，便于表驱动测试。
func (f *RaceFreeFakeWatcher) Action(action EventType, obj runtime.Object) {
	f.Lock()
	defer f.Unlock()
	if !f.Stopped {
		select {
		case f.result <- Event{action, obj}:
			return
		default:
			panic(fmt.Errorf("channel full"))
		}
	}
}

// ProxyWatcher 把外部通道包装为 watch.Interface；线程安全。
type ProxyWatcher struct {
	result chan Event
	stopCh chan struct{}

	mutex   sync.Mutex
	stopped bool
}

var _ Interface = &ProxyWatcher{}

// NewProxyWatcher 通过包装给定通道创建 ProxyWatcher。
func NewProxyWatcher(ch chan Event) *ProxyWatcher {
	return &ProxyWatcher{
		result:  ch,
		stopCh:  make(chan struct{}),
		stopped: false,
	}
}

// Stop 实现 Interface。关闭 stopCh 通知生产方结束；结果通道由生产方负责关闭。
func (pw *ProxyWatcher) Stop() {
	pw.mutex.Lock()
	defer pw.mutex.Unlock()
	if !pw.stopped {
		pw.stopped = true
		close(pw.stopCh)
	}
}

// Stopping 返回是否已调用过 Stop()。
func (pw *ProxyWatcher) Stopping() bool {
	pw.mutex.Lock()
	defer pw.mutex.Unlock()
	return pw.stopped
}

// ResultChan 实现 Interface。
func (pw *ProxyWatcher) ResultChan() <-chan Event {
	return pw.result
}

// StopChan 返回停止信号通道，生产方可监听它来结束发送。
func (pw *ProxyWatcher) StopChan() <-chan struct{} {
	return pw.stopCh
}

// MockWatcher 用可替换的函数实现 watch.Interface，便于按需定制行为。
type MockWatcher struct {
	StopFunc       func()
	ResultChanFunc func() <-chan Event
}

var _ Interface = &MockWatcher{}

// Stop 调用 StopFunc。
func (mw MockWatcher) Stop() {
	mw.StopFunc()
}

// ResultChan 调用 ResultChanFunc。
func (mw MockWatcher) ResultChan() <-chan Event {
	return mw.ResultChanFunc()
}
