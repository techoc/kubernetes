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

package watch_test

import (
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	. "k8s.io/apimachinery/pkg/watch"
)

// testType 是仅实现 runtime.Object 的最小类型，事件内容用字符串即可比较。
type testType string

func (obj testType) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (obj testType) DeepCopyObject() runtime.Object   { return obj }

// TestFake 验证 Fake watcher 按发送顺序投递事件，Stop 后 ResultChan 关闭。
func TestFake(t *testing.T) {
	f := NewFake()

	table := []struct {
		t EventType
		s testType
	}{
		{Added, testType("foo")},
		{Modified, testType("qux")},
		{Modified, testType("bar")},
		{Deleted, testType("bar")},
		{Error, testType("error: blah")},
	}

	// Prove that f implements Interface by phrasing this as a function.
	// 把消费逻辑声明为接收 Interface 的函数，顺带在编译期证明 Fake 实现了接口。
	consumer := func(w Interface) {
		for _, expect := range table {
			got, ok := <-w.ResultChan()
			if !ok {
				t.Fatalf("closed early")
			}
			if e, a := expect.t, got.Type; e != a {
				t.Fatalf("Expected %v, got %v", e, a)
			}
			if a, ok := got.Object.(testType); !ok || a != expect.s {
				t.Fatalf("Expected %v, got %v", expect.s, a)
			}
		}
		// Stop 之后通道必须关闭，读取返回 ok=false。
		_, stillOpen := <-w.ResultChan()
		if stillOpen {
			t.Fatal("Never stopped")
		}
	}

	sender := func() {
		// 混合使用 Add/Action/Modify/Delete/Error，覆盖所有事件类型入口。
		f.Add(testType("foo"))
		f.Action(Modified, testType("qux"))
		f.Modify(testType("bar"))
		f.Delete(testType("bar"))
		f.Error(testType("error: blah"))
		f.Stop()
	}

	go sender()
	consumer(f)
}

// TestRaceFreeFake 与 TestFake 行为相同，但使用带锁的 RaceFreeFake；
// 在 -race 下并发 Stop/发送不应产生数据竞争。
func TestRaceFreeFake(t *testing.T) {
	f := NewRaceFreeFake()

	table := []struct {
		t EventType
		s testType
	}{
		{Added, testType("foo")},
		{Modified, testType("qux")},
		{Modified, testType("bar")},
		{Deleted, testType("bar")},
		{Error, testType("error: blah")},
	}

	// Prove that f implements Interface by phrasing this as a function.
	// 把消费逻辑声明为接收 Interface 的函数，顺带在编译期证明 Fake 实现了接口。
	consumer := func(w Interface) {
		for _, expect := range table {
			got, ok := <-w.ResultChan()
			if !ok {
				t.Fatalf("closed early")
			}
			if e, a := expect.t, got.Type; e != a {
				t.Fatalf("Expected %v, got %v", e, a)
			}
			if a, ok := got.Object.(testType); !ok || a != expect.s {
				t.Fatalf("Expected %v, got %v", expect.s, a)
			}
		}
		// Stop 之后通道必须关闭，读取返回 ok=false。
		_, stillOpen := <-w.ResultChan()
		if stillOpen {
			t.Fatal("Never stopped")
		}
	}

	sender := func() {
		// 混合使用 Add/Action/Modify/Delete/Error，覆盖所有事件类型入口。
		f.Add(testType("foo"))
		f.Action(Modified, testType("qux"))
		f.Modify(testType("bar"))
		f.Delete(testType("bar"))
		f.Error(testType("error: blah"))
		f.Stop()
	}

	go sender()
	consumer(f)
}

// TestEmpty 验证 EmptyWatch 的通道立即关闭，且 Stop 幂等。
func TestEmpty(t *testing.T) {
	w := NewEmptyWatch()
	_, ok := <-w.ResultChan()
	if ok {
		t.Errorf("unexpected result channel result")
	}
	w.Stop()
	_, ok = <-w.ResultChan()
	if ok {
		t.Errorf("unexpected result channel result")
	}
}

// TestProxyWatcher 验证 ProxyWatcher 转发底层通道事件、Stop 关闭 StopChan，
// 并容忍重复 Stop。
func TestProxyWatcher(t *testing.T) {
	events := []Event{
		{Added, testType("foo")},
		{Modified, testType("qux")},
		{Modified, testType("bar")},
		{Deleted, testType("bar")},
		{Error, testType("error: blah")},
	}

	ch := make(chan Event, len(events))
	w := NewProxyWatcher(ch)

	for _, e := range events {
		ch <- e
	}

	for _, e := range events {
		g := <-w.ResultChan()
		if !reflect.DeepEqual(e, g) {
			t.Errorf("Expected %#v, got %#v", e, g)
			continue
		}
	}

	w.Stop()

	select {
	// Closed channel always reads immediately
	case <-w.StopChan():
	default:
		t.Error("Channel isn't closed")
	}

	// Test double close
	// Stop 必须幂等：重复调用不能 panic（close 已关闭通道会 panic）。
	w.Stop()
}
