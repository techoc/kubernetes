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
	"sync"
)

// FilterFunc 接收一个事件，可对其进行修改，并返回修改后的事件。
// 若该事件应被忽略，则返回 keep=false。
type FilterFunc func(in Event) (out Event, keep bool)

// Filter 让所有事件先经过 f 再继续传递。
// 由于 Go channel 的工作方式，给 watch 加过滤器不可避免地会让事件 channel 的
// 有效队列长度增加一（多了一层无缓冲转发）。
//
// WARNING: filter 有一个致命缺陷：当某个对象此前不满足过滤条件、现在开始通过时，
// 它无法正确更新事件的 Type 字段（Add/Modified/Deleted）。
// 因此不应依赖过滤后的 Add/Modified 序列重建对象生命周期。
func Filter(w Interface, f FilterFunc) Interface {
	// result 是无缓冲 channel：消费方不读取时会阻塞 loop，从而对上游施加背压。
	fw := &filteredWatch{
		incoming: w,
		result:   make(chan Event),
		f:        f,
	}
	go fw.loop()
	return fw
}

// filteredWatch 是 Filter 返回的 watch 实现，在内部 goroutine 中完成过滤与转发。
type filteredWatch struct {
	incoming Interface
	result   chan Event
	f        FilterFunc
}

// ResultChan 返回接收过滤后事件的 channel。
func (fw *filteredWatch) ResultChan() <-chan Event {
	return fw.result
}

// Stop 停止上游 watch；上游关闭后本 watch 的 loop 也会结束并关闭 result。
func (fw *filteredWatch) Stop() {
	fw.incoming.Stop()
}

// loop 等待新事件、过滤后转发；上游 channel 关闭时关闭 result，通知消费方结束。
func (fw *filteredWatch) loop() {
	defer close(fw.result)
	for event := range fw.incoming.ResultChan() {
		filtered, keep := fw.f(event)
		if keep {
			fw.result <- filtered
		}
	}
}

// Recorder 记录从 watch 收到、直到其关闭为止的所有事件。
// 它通过 Filter 拦截事件，同时仍把事件透传给下游。
type Recorder struct {
	Interface

	lock   sync.Mutex
	events []Event
}

// 编译期断言：Recorder 实现 watch.Interface。
var _ Interface = &Recorder{}

// NewRecorder 包装一个 Interface，并记录其中传递的所有变化。
func NewRecorder(w Interface) *Recorder {
	r := &Recorder{}
	r.Interface = Filter(w, r.record)
	return r
}

// record 是 FilterFunc，记录每个收到的事件，并原样放行。
func (r *Recorder) record(in Event) (Event, bool) {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.events = append(r.events, in)
	return in, true
}

// Events 返回已记录事件的副本。
// 返回副本而非内部切片，避免调用方在锁外修改或读取时与 record 产生数据竞争。
func (r *Recorder) Events() []Event {
	r.lock.Lock()
	defer r.lock.Unlock()
	copied := make([]Event, len(r.events))
	copy(copied, r.events)
	return copied
}
