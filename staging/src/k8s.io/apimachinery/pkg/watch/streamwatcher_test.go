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
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	. "k8s.io/apimachinery/pkg/watch"
)

// fakeDecoder 是可注入事件的假解码器：从通道读取事件，通道关闭时返回 io.EOF。
type fakeDecoder struct {
	items chan Event
	err   error
}

func (f fakeDecoder) Decode() (action EventType, object runtime.Object, err error) {
	if f.err != nil {
		return "", nil, f.err
	}
	item, open := <-f.items
	if !open {
		return action, nil, io.EOF
	}
	return item.Type, item.Object, nil
}

// Close 关闭事件通道，便于让 Decode 返回 EOF 而不泄漏 goroutine。
func (f fakeDecoder) Close() {
	if f.items != nil {
		close(f.items)
	}
}

// fakeReporter 记录传入的错误，并返回空的 unstructured 对象作为错误上报载体。
type fakeReporter struct {
	err error
}

func (f *fakeReporter) AsObject(err error) runtime.Object {
	f.err = err
	return runtime.Unstructured(nil)
}

// TestStreamWatcher 验证事件从解码器透传到结果通道，Stop 后结果通道被关闭。
func TestStreamWatcher(t *testing.T) {
	table := []Event{
		{Type: Added, Object: testType("foo")},
	}

	fd := fakeDecoder{items: make(chan Event, 5)}
	//nolint:logcheck // Intentionally uses the old API.
	sw := NewStreamWatcher(fd, nil)

	for _, item := range table {
		fd.items <- item
		got, open := <-sw.ResultChan()
		if !open {
			t.Errorf("unexpected early close")
		}
		if e, a := item, got; !reflect.DeepEqual(e, a) {
			t.Errorf("expected %v, got %v", e, a)
		}
	}

	sw.Stop()
	_, open := <-sw.ResultChan()
	if open {
		t.Errorf("Unexpected failure to close")
	}
}

// TestStreamWatcherError 验证解码错误被转换为 Error 事件发送一次，随后通道关闭。
func TestStreamWatcherError(t *testing.T) {
	fd := fakeDecoder{err: fmt.Errorf("test error")}
	fr := &fakeReporter{}
	//nolint:logcheck // Intentionally uses the old API.
	sw := NewStreamWatcher(fd, fr)
	evt, ok := <-sw.ResultChan()
	if !ok {
		t.Fatalf("unexpected close")
	}
	if evt.Type != Error || evt.Object != runtime.Unstructured(nil) {
		t.Fatalf("unexpected object: %#v", evt)
	}
	_, ok = <-sw.ResultChan()
	if ok {
		t.Fatalf("unexpected open channel")
	}

	sw.Stop()
	_, ok = <-sw.ResultChan()
	if ok {
		t.Fatalf("unexpected open channel")
	}
}

// TestStreamWatcherRace 验证解码器立即出错与 Stop 并发时，
// 不会向已关闭的结果通道发送事件（避免 panic 或悬挂的 pending send）。
func TestStreamWatcherRace(t *testing.T) {
	fd := fakeDecoder{err: fmt.Errorf("test error")}
	fr := &fakeReporter{}
	//nolint:logcheck // Intentionally uses the old API.
	sw := NewStreamWatcher(fd, fr)
	time.Sleep(10 * time.Millisecond)
	sw.Stop()
	time.Sleep(10 * time.Millisecond)
	_, ok := <-sw.ResultChan()
	if ok {
		t.Fatalf("unexpected pending send")
	}
}
