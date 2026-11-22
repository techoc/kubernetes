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
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

// myType 是最小的 runtime.Object 测试实现，仅携带 ID 与 Value。
type myType struct {
	ID    string
	Value string
}

func (obj *myType) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (obj *myType) DeepCopyObject() runtime.Object {
	if obj == nil {
		return nil
	}
	clone := *obj
	return &clone
}

// TestBroadcaster 验证每个 watcher 都能按顺序收到全部事件，Shutdown 后通道关闭。
func TestBroadcaster(t *testing.T) {
	table := []Event{
		{Type: Added, Object: &myType{"foo", "hello world 1"}},
		{Type: Added, Object: &myType{"bar", "hello world 2"}},
		{Type: Modified, Object: &myType{"foo", "goodbye world 3"}},
		{Type: Deleted, Object: &myType{"bar", "hello world 4"}},
	}

	// The broadcaster we're testing
	// 被测试的 broadcaster
	m := NewBroadcaster(0, WaitIfChannelFull)

	// 添加若干 watcher
	const testWatchers = 2
	wg := sync.WaitGroup{}
	wg.Add(testWatchers)
	for i := 0; i < testWatchers; i++ {
		w, err := m.Watch()
		if err != nil {
			t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
		}
		// 校验每个 watcher 按正确顺序收到事件
		go func(watcher int, w Interface) {
			tableLine := 0
			for {
				event, ok := <-w.ResultChan()
				if !ok {
					break
				}
				if e, a := table[tableLine], event; !reflect.DeepEqual(e, a) {
					t.Errorf("Watcher %v, line %v: Expected (%v, %#v), got (%v, %#v)",
						watcher, tableLine, e.Type, e.Object, a.Type, a.Object)
				} else {
					t.Logf("Got (%v, %#v)", event.Type, event.Object)
				}
				tableLine++
			}
			wg.Done()
		}(i, w)
	}

	for i, item := range table {
		t.Logf("Sending %v", i)
		m.Action(item.Type, item.Object)
	}

	m.Shutdown()

	wg.Wait()
}

// TestBroadcasterWatcherClose 验证 Stop 关闭该 watcher 的通道、Shutdown 关闭其余 watcher 的通道，
// 且重复 Stop 不会 panic。
func TestBroadcasterWatcherClose(t *testing.T) {
	m := NewBroadcaster(0, WaitIfChannelFull)
	w, err := m.Watch()
	if err != nil {
		t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
	}
	w2, err := m.Watch()
	if err != nil {
		t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
	}
	w.Stop()
	m.Shutdown()
	if _, open := <-w.ResultChan(); open {
		t.Errorf("Stop didn't work?")
	}
	if _, open := <-w2.ResultChan(); open {
		t.Errorf("Shutdown didn't work?")
	}
	// 重复 Stop 不会出问题
	w.Stop()
	w2.Stop()
}

// TestBroadcasterWatcherStopDeadlock 验证在 distribute 正尝试向某 watcher 发送事件时
// 停止该 watcher 不会造成死锁。
func TestBroadcasterWatcherStopDeadlock(t *testing.T) {
	done := make(chan bool)
	m := NewBroadcaster(0, WaitIfChannelFull)
	w, err := m.Watch()
	if err != nil {
		t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
	}
	w2, err := m.Watch()
	if err != nil {
		t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
	}
	go func(w0, w1 Interface) {
		// 一旦某个 watcher 收到事件，说明 Broadcaster 正处于 distribute 循环中；
		// 此时停止另一个 watcher，以覆盖 “分发中停止” 的路径。
		select {
		case <-w0.ResultChan():
			w1.Stop()
		case <-w1.ResultChan():
			w0.Stop()
		}
		close(done)
	}(w, w2)
	m.Action(Added, &myType{})
	select {
	case <-time.After(wait.ForeverTestTimeout):
		t.Error("timeout: deadlocked")
	case <-done:
	}
	m.Shutdown()
}

// TestBroadcasterDropIfChannelFull 验证 DropIfChannelFull 模式下，
// 队列容量为 1 的 watcher 只收到第一个事件，第二个事件被丢弃，且不阻塞其他 watcher。
func TestBroadcasterDropIfChannelFull(t *testing.T) {
	m := NewBroadcaster(1, DropIfChannelFull)

	event1 := Event{Type: Added, Object: &myType{"foo", "hello world 1"}}
	event2 := Event{Type: Added, Object: &myType{"bar", "hello world 2"}}

	// Add a couple watchers
	watches := make([]Interface, 2)
	var err error
	for i := range watches {
		watches[i], err = m.Watch()
		if err != nil {
			t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
		}
	}

	// 在关闭广播通道前发送两个事件
	t.Log("Sending event 1")
	m.Action(event1.Type, event1.Object)
	t.Log("Sending event 2")
	m.Action(event2.Type, event2.Object)
	m.Shutdown()

	// 从队列拉取事件
	wg := sync.WaitGroup{}
	wg.Add(len(watches))
	for i := range watches {
		// 校验每个 watcher 只收到第一个事件：其长度为 1 的队列已被首个事件占满。
		go func(watcher int, w Interface) {
			defer wg.Done()
			e1, ok := <-w.ResultChan()
			if !ok {
				t.Errorf("Watcher %v failed to retrieve first event.", watcher)
			}
			if e, a := event1, e1; !reflect.DeepEqual(e, a) {
				t.Errorf("Watcher %v: Expected (%v, %#v), got (%v, %#v)",
					watcher, e.Type, e.Object, a.Type, a.Object)
			}
			t.Logf("Got (%v, %#v)", e1.Type, e1.Object)
			e2, ok := <-w.ResultChan()
			if ok {
				t.Errorf("Watcher %v received second event (%v, %#v) even though it shouldn't have.",
					watcher, e2.Type, e2.Object)
			}
		}(i, watches[i])
	}
	wg.Wait()
}

// BenchmarkBroadCaster 衡量并行调用 Action 的吞吐（每个 goroutine 发送同一事件）。
func BenchmarkBroadCaster(b *testing.B) {
	event1 := Event{Type: Added, Object: &myType{"foo", "hello world 1"}}
	m := NewBroadcaster(0, WaitIfChannelFull)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.Action(event1.Type, event1.Object)
		}
	})
	b.StopTimer()
}

// TestBroadcasterWatchAfterShutdown 验证 Shutdown 后再调用 Watch/WatchWithPrefix 返回错误。
func TestBroadcasterWatchAfterShutdown(t *testing.T) {
	event1 := Event{Type: Added, Object: &myType{"foo", "hello world 1"}}
	event2 := Event{Type: Added, Object: &myType{"bar", "hello world 2"}}

	m := NewBroadcaster(0, WaitIfChannelFull)
	m.Shutdown()

	_, err := m.Watch()
	assert.EqualError(t, err, "broadcaster already stopped", "Watch should report error id broadcaster is shutdown")

	_, err = m.WatchWithPrefix([]Event{event1, event2})
	assert.EqualError(t, err, "broadcaster already stopped", "WatchWithPrefix should report error id broadcaster is shutdown")
}

// TestBroadcasterSendEventAfterShutdown 验证 Shutdown 后 Action 与 ActionOrDrop
// 都返回错误，且 ActionOrDrop 返回 false（不会向已关闭通道发送）。
func TestBroadcasterSendEventAfterShutdown(t *testing.T) {
	m := NewBroadcaster(1, DropIfChannelFull)

	event := Event{Type: Added, Object: &myType{"foo", "hello world"}}

	// Add a couple watchers
	watches := make([]Interface, 2)
	for i := range watches {
		watches[i], _ = m.Watch()
	}
	m.Shutdown()

	// 在关闭广播通道后发送事件
	t.Log("Sending event")

	err := m.Action(event.Type, event.Object)
	assert.EqualError(t, err, "broadcaster already stopped", "ActionOrDrop should report error id broadcaster is shutdown")

	sendOnClosed, err := m.ActionOrDrop(event.Type, event.Object)
	assert.False(t, sendOnClosed, "ActionOrDrop should return false if broadcaster is already shutdown")
	assert.EqualError(t, err, "broadcaster already stopped", "ActionOrDrop should report error id broadcaster is shutdown")
}

// Test this since we see usage patterns where the broadcaster and watchers are
// stopped simultaneously leading to races.
//
// TestBroadcasterShutdownRace 覆盖 broadcaster 与 watcher 同时停止的竞态场景
// （真实用法见 startRecordingEvents）。
func TestBroadcasterShutdownRace(t *testing.T) {
	m := NewBroadcaster(1, WaitIfChannelFull)
	stopCh := make(chan struct{})

	// 添加若干 watcher
	const testWatchers = 2
	for i := 0; i < testWatchers; i++ {
		i := i

		_, err := m.Watch()
		if err != nil {
			t.Fatalf("Unable start event watcher: '%v' (will not retry!)", err)
		}
		// 模拟 watcher 独立于 eventbroadcaster 自行关闭的场景。
		go func() {
			<-stopCh
			t.Log("Stopping Watchers")
			m.stopWatching(int64(i))
		}()
	}

	event := Event{Type: Added, Object: &myType{"foo", "hello world"}}
	err := m.Action(event.Type, event.Object)
	if err != nil {
		t.Fatalf("error sending event: %v", err)
	}

	// 手动模拟 m.Shutdown()，但调整为强制触发竞态：
	// 1. 关闭 watcher 的 stop 通道，让 watcher 独立于 eventBroadcaster 关闭；
	// 2. 先关闭 m.incoming 再关闭 m.stopped，使 watcher 调用 blockQueue 时
	//    能通过 m.stopped 检查，从而命中竞态窗口。
	m.blockQueue(func() {
		close(stopCh)
		close(m.incoming)
		time.Sleep(1 * time.Millisecond)
		close(m.stopped)
	})
	m.distributing.Wait()
}
