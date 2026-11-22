/*
Copyright 2015 The Kubernetes Authors.

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

// Package workqueue_test 从外部视角验证 workqueue 的并发、去重、关闭与 drain 语义。
package workqueue_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/workqueue"
)

// traceQueue traces whether items are touched
//
// traceQueue 记录哪些条目被 Touch 过，用于验证“处理中重复入队”是否走 Touch 而非重新排队。
type traceQueue struct {
	workqueue.Queue[any]

	touched map[interface{}]struct{}
}

func (t *traceQueue) Touch(item interface{}) {
	t.Queue.Touch(item)
	if t.touched == nil {
		t.touched = make(map[interface{}]struct{})
	}
	t.touched[item] = struct{}{}
}

var _ workqueue.Queue[any] = &traceQueue{}

// TestBasic 用多生产者/多消费者压测队列：关闭后 Add 应被忽略，
// 已入队条目应被消费完毕，最终 Len 为 0。分别覆盖 ShutDown 与 ShutDownWithDrain。
func TestBasic(t *testing.T) {
	tests := []struct {
		queue         *workqueue.Type
		queueShutDown func(workqueue.Interface)
	}{
		{
			queue:         workqueue.New(),
			queueShutDown: workqueue.Interface.ShutDown,
		},
		{
			queue:         workqueue.New(),
			queueShutDown: workqueue.Interface.ShutDownWithDrain,
		},
	}
	for _, test := range tests {
		// If something is seriously wrong this test will never complete.

		// Start producers
		const producers = 50
		producerWG := sync.WaitGroup{}
		producerWG.Add(producers)
		for i := 0; i < producers; i++ {
			go func(i int) {
				defer producerWG.Done()
				for j := 0; j < 50; j++ {
					test.queue.Add(i)
					time.Sleep(time.Millisecond)
				}
			}(i)
		}

		// Start consumers
		const consumers = 10
		consumerWG := sync.WaitGroup{}
		consumerWG.Add(consumers)
		for i := 0; i < consumers; i++ {
			go func(i int) {
				defer consumerWG.Done()
				for {
					item, quit := test.queue.Get()
					if item == "added after shutdown!" {
						t.Errorf("Got an item added after shutdown.")
					}
					if quit {
						return
					}
					t.Logf("Worker %v: begin processing %v", i, item)
					time.Sleep(3 * time.Millisecond)
					t.Logf("Worker %v: done processing %v", i, item)
					test.queue.Done(item)
				}
			}(i)
		}

		producerWG.Wait()
		test.queueShutDown(test.queue)
		// 模拟“关闭后仍尝试入队”，消费者读到该条目即视为失败。
		test.queue.Add("added after shutdown!")
		consumerWG.Wait()
		if test.queue.Len() != 0 {
			t.Errorf("Expected the queue to be empty, had: %v items", test.queue.Len())
		}
	}
}

// TestAddWhileProcessing 验证“处理中再次 Add”只会把条目标记为 dirty，
// 在 Done 后重新可被消费一次，而不会重复入队多次。
func TestAddWhileProcessing(t *testing.T) {
	tests := []struct {
		queue         *workqueue.Type
		queueShutDown func(workqueue.Interface)
	}{
		{
			queue:         workqueue.New(),
			queueShutDown: workqueue.Interface.ShutDown,
		},
		{
			queue:         workqueue.New(),
			queueShutDown: workqueue.Interface.ShutDownWithDrain,
		},
	}
	for _, test := range tests {

		// Start producers
		const producers = 50
		producerWG := sync.WaitGroup{}
		producerWG.Add(producers)
		for i := 0; i < producers; i++ {
			go func(i int) {
				defer producerWG.Done()
				test.queue.Add(i)
			}(i)
		}

		// Start consumers
		const consumers = 10
		consumerWG := sync.WaitGroup{}
		consumerWG.Add(consumers)
		for i := 0; i < consumers; i++ {
			go func(i int) {
				defer consumerWG.Done()
				// Every worker will re-add every item up to two times.
				// This tests the dirty-while-processing case.
				counters := map[interface{}]int{}
				for {
					item, quit := test.queue.Get()
					if quit {
						return
					}
					counters[item]++
					if counters[item] < 2 {
						test.queue.Add(item)
					}
					test.queue.Done(item)
				}
			}(i)
		}

		producerWG.Wait()
		test.queueShutDown(test.queue)
		consumerWG.Wait()
		if test.queue.Len() != 0 {
			t.Errorf("Expected the queue to be empty, had: %v items", test.queue.Len())
		}
	}
}

// TestLen 验证 Len 反映待处理条目数，且重复 Add 不会增加长度。
func TestLen(t *testing.T) {
	q := workqueue.New()
	q.Add("foo")
	if e, a := 1, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
	q.Add("bar")
	if e, a := 2, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
	q.Add("foo") // should not increase the queue length.
	if e, a := 2, q.Len(); e != a {
		t.Errorf("Expected %v, got %v", e, a)
	}
}

// TestReinsert 验证处理中重新 Add 会在 Done 后再次出队，且最终清空。
func TestReinsert(t *testing.T) {
	q := workqueue.New()
	q.Add("foo")

	// Start processing
	i, _ := q.Get()
	if i != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Add it back while processing
	q.Add(i)

	// Finish it up
	q.Done(i)

	// It should be back on the queue
	i, _ = q.Get()
	if i != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Finish that one up
	q.Done(i)

	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}
}

// TestCollapse 验证重复 Add 同一未处理条目时，底层队列只保留一份（去重）。
func TestCollapse(t *testing.T) {
	tq := &traceQueue{Queue: workqueue.DefaultQueue[any]()}
	q := workqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "",
		Queue: tq,
	})
	// Add a new one twice
	q.Add("bar")
	q.Add("bar")

	// It should get the new one
	i, _ := q.Get()
	if i != "bar" {
		t.Errorf("Expected %v, got %v", "bar", i)
	}

	// Finish that one up
	q.Done(i)

	// There should be no more objects in the queue
	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}

	if _, ok := tq.touched["bar"]; !ok {
		t.Errorf("Expected bar to be Touched")
	}
}

// TestCollapseWhileProcessing 验证“处理中重复 Add”不会被 Touch（不走重新排队路径），
// 且 Done 后条目仍会回到队列被另一 worker 取走。
func TestCollapseWhileProcessing(t *testing.T) {
	tq := &traceQueue{Queue: workqueue.DefaultQueue[any]()}
	q := workqueue.NewWithConfig(workqueue.QueueConfig{
		Name:  "",
		Queue: tq,
	})
	q.Add("foo")

	// Start processing
	i, _ := q.Get()
	if i != "foo" {
		t.Errorf("Expected %v, got %v", "foo", i)
	}

	// Add the same one twice
	q.Add("foo")
	q.Add("foo")

	waitCh := make(chan struct{})
	// simulate another worker consuming the queue
	go func() {
		defer close(waitCh)
		i, _ := q.Get()
		if i != "foo" {
			t.Errorf("Expected %v, got %v", "foo", i)
		}
		// Finish that one up
		q.Done(i)
	}()

	// give the worker some head start to avoid races
	// on the select statement that cause flakiness
	time.Sleep(100 * time.Millisecond)
	// Finish the first one to unblock the other worker
	select {
	case <-waitCh:
		t.Errorf("worker should be blocked until we are done")
	default:
		q.Done("foo")
	}

	// wait for the worker to consume the new object
	// There should be no more objects in the queue
	<-waitCh
	if a := q.Len(); a != 0 {
		t.Errorf("Expected queue to be empty. Has %v items", a)
	}

	if _, ok := tq.touched["foo"]; ok {
		t.Errorf("Unexpected Touch")
	}
}

// TestQueueDrainageUsingShutDownWithDrain 验证 ShutDownWithDrain 会等待所有已取出的条目
// 完成 Done 后才返回，即“排空”语义。
func TestQueueDrainageUsingShutDownWithDrain(t *testing.T) {

	q := workqueue.New()

	q.Add("foo")
	q.Add("bar")

	firstItem, _ := q.Get()
	secondItem, _ := q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// This is done as to simulate a sequence of events where ShutDownWithDrain
	// is called before we start marking all items as done - thus simulating a
	// drain where we wait for all items to finish processing.
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Mark the first two items as done, as to finish up
	q.Done(firstItem)
	q.Done(secondItem)

	finishedWG.Wait()
}

// TestNoQueueDrainageUsingShutDown 验证 ShutDown 立即返回，不等待未 Done 的条目。
func TestNoQueueDrainageUsingShutDown(t *testing.T) {

	q := workqueue.New()

	q.Add("foo")
	q.Add("bar")

	q.Get()
	q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		// Invoke ShutDown: suspending the execution immediately.
		q.ShutDown()
	}()

	// We can now do this and not have the test timeout because we didn't call
	// Done on the first two items before arriving here.
	finishedWG.Wait()
}

// TestForceQueueShutdownUsingShutDown 验证在 ShutDownWithDrain 等待期间调用 ShutDown
// 可强制结束，模拟收到第二次 SIGTERM/SIGINT 时的行为。
func TestForceQueueShutdownUsingShutDown(t *testing.T) {

	q := workqueue.New()

	q.Add("foo")
	q.Add("bar")

	q.Get()
	q.Get()

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// This is done as to simulate a sequence of events where ShutDownWithDrain
	// is called before ShutDown
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Use ShutDown to force the queue to shut down (simulating a caller
	// which can invoke this function on a second SIGTERM/SIGINT)
	q.ShutDown()

	// We can now do this and not have the test timeout because we didn't call
	// done on any of the items before arriving here.
	finishedWG.Wait()
}

// TestQueueDrainageUsingShutDownWithDrainWithDirtyItem 验证存在 dirty 条目时，
// drain 会先让 dirty 条目重新出队并处理完，然后再返回。
func TestQueueDrainageUsingShutDownWithDrainWithDirtyItem(t *testing.T) {
	q := workqueue.New()

	q.Add("foo")
	gotten, _ := q.Get()
	q.Add("foo")

	finishedWG := sync.WaitGroup{}
	finishedWG.Add(1)
	go func() {
		defer finishedWG.Done()
		q.ShutDownWithDrain()
	}()

	// Ensure that ShutDownWithDrain has started and is blocked.
	shuttingDown := false
	for !shuttingDown {
		_, shuttingDown = q.Get()
	}

	// Finish "working".
	q.Done(gotten)

	// `shuttingDown` becomes false because Done caused an item to go back into
	// the queue.
	again, shuttingDown := q.Get()
	if shuttingDown {
		t.Fatalf("should not have been done")
	}
	q.Done(again)

	// Now we are really done.
	_, shuttingDown = q.Get()
	if !shuttingDown {
		t.Fatalf("should have been done")
	}

	finishedWG.Wait()
}

// TestGarbageCollection ensures that objects that are added then removed from the queue are
// able to be garbage collected.
//
// 验证队列处理后不残留对条目的引用（否则会造成内存泄漏）。
func TestGarbageCollection(t *testing.T) {
	type bigObject struct {
		data []byte
	}
	leakQueue := workqueue.New()
	t.Cleanup(func() {
		// Make sure leakQueue doesn't go out of scope too early
		runtime.KeepAlive(leakQueue)
	})
	c := &bigObject{data: []byte("hello")}
	mustGarbageCollect(t, c)
	leakQueue.Add(c)
	o, _ := leakQueue.Get()
	leakQueue.Done(o)
}

// mustGarbageCollect asserts than an object was garbage collected by the end of the test.
// The input must be a pointer to an object.
func mustGarbageCollect(t *testing.T, i interface{}) {
	t.Helper()
	var collected int32 = 0
	runtime.SetFinalizer(i, func(x interface{}) {
		atomic.StoreInt32(&collected, 1)
	})
	t.Cleanup(func() {
		if err := wait.PollImmediate(time.Millisecond*100, wait.ForeverTestTimeout, func() (done bool, err error) {
			// Trigger GC explicitly, otherwise we may need to wait a long time for it to run
			runtime.GC()
			return atomic.LoadInt32(&collected) == 1, nil
		}); err != nil {
			t.Errorf("object was not garbage collected")
		}
	})
}

// BenchmarkQueue 基准测试入队+出队+Done 的单轮开销（每轮 100 个不同 key）。
func BenchmarkQueue(b *testing.B) {
	keys := make([]string, 100)
	for idx := range keys {
		keys[idx] = fmt.Sprintf("key-%d", idx)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		q := workqueue.NewTypedWithConfig(workqueue.TypedQueueConfig[string]{})
		b.StartTimer()
		for j := 0; j < 100; j++ {
			q.Add(keys[j])
			key, _ := q.Get()
			q.Done(key)
		}
	}
}
