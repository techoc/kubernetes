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

package cacher

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/cacher/metrics"
	"k8s.io/apiserver/pkg/storage/cacher/store"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	compbasemetrics "k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/testutil"
	testingclock "k8s.io/utils/clock/testing"

	cachertesting "k8s.io/apiserver/pkg/storage/cacher/testing"
)

// 验证即使写 cacheWatcher.result 被阻塞，
// cacheWatcher.process goroutine 仍能被正确清理。
//
// 把 result 缓冲设为 0 使写入阻塞，再验证 Stop 仍能触发 forget() 两次（不因写阻塞而泄漏协程）。
func TestCacheWatcherCleanupNotBlockedByResult(t *testing.T) {
	var lock sync.RWMutex
	var w *cacheWatcher
	count := 0
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		lock.Lock()
		defer lock.Unlock()
		count++
		// forget() 必须停止 watcher，因为只有停止 watcher
		// 才会触发 process() goroutine 停止，而本测试最终
		// 等待的正是它。
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
	}
	initEvents := []*watchCacheEvent{
		{Object: &v1.Pod{}},
		{Object: &v1.Pod{}},
	}
	// 把 w.result 的缓冲区设为 0，使写入被阻塞。
	//
	w = newCacheWatcher(0, filter, forget, storage.APIObjectVersioner{}, time.Now(), false, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
	go w.processInterval(context.Background(), intervalFromEvents(initEvents), 0)
	w.Stop()
	if err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 2, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called twice, because sendWatchCacheEvent should not be blocked by the result channel: %v", err)
	}
}

// TestCacheWatcherHandlesFiltering 验证字段过滤器与 watch 事件类型的转换语义：
//   - 对象进入/离开过滤集合时，需分别合成 Added/Deleted 事件；
//   - 过滤集合外的变更应被丢弃；
//   - 集合内的连续变更保持 Modified。
func TestCacheWatcherHandlesFiltering(t *testing.T) {
	filter := func(_ string, _ labels.Set, field fields.Set, _ runtime.Object) bool {
		return field["spec.nodeName"] == "host"
	}
	forget := func(bool) {}

	testCases := []struct {
		events   []*watchCacheEvent
		expected []watch.Event
	}{
		// 正确处理先带 filter 启动、再被删除、再重新加入
		{
			events: []*watchCacheEvent{
				{
					Type:            watch.Added,
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
					ObjFields:       fields.Set{"spec.nodeName": "host"},
					ResourceVersion: 1,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
					PrevObjFields:   fields.Set{"spec.nodeName": "host"},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
					ObjFields:       fields.Set{"spec.nodeName": ""},
					ResourceVersion: 2,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
					PrevObjFields:   fields.Set{"spec.nodeName": ""},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "3"}},
					ObjFields:       fields.Set{"spec.nodeName": "host"},
					ResourceVersion: 3,
				},
			},
			expected: []watch.Event{
				{Type: watch.Added, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}}},
				{Type: watch.Deleted, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}}},
				{Type: watch.Added, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "3"}}},
			},
		},
		// 正确处理忽略 filter 之前的变更、再加入、再删除
		{
			events: []*watchCacheEvent{
				{
					Type:            watch.Added,
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
					ObjFields:       fields.Set{"spec.nodeName": ""},
					ResourceVersion: 1,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "1"}},
					PrevObjFields:   fields.Set{"spec.nodeName": ""},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
					ObjFields:       fields.Set{"spec.nodeName": ""},
					ResourceVersion: 2,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "2"}},
					PrevObjFields:   fields.Set{"spec.nodeName": ""},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "3"}},
					ObjFields:       fields.Set{"spec.nodeName": "host"},
					ResourceVersion: 3,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "3"}},
					PrevObjFields:   fields.Set{"spec.nodeName": "host"},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "4"}},
					ObjFields:       fields.Set{"spec.nodeName": "host"},
					ResourceVersion: 4,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "4"}},
					PrevObjFields:   fields.Set{"spec.nodeName": "host"},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}},
					ObjFields:       fields.Set{"spec.nodeName": ""},
					ResourceVersion: 5,
				},
				{
					Type:            watch.Modified,
					PrevObject:      &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}},
					PrevObjFields:   fields.Set{"spec.nodeName": ""},
					Object:          &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "6"}},
					ObjFields:       fields.Set{"spec.nodeName": ""},
					ResourceVersion: 6,
				},
			},
			expected: []watch.Event{
				{Type: watch.Added, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "3"}}},
				{Type: watch.Modified, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "4"}}},
				{Type: watch.Deleted, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "5"}}},
			},
		},
	}

TestCase:
	for i, testCase := range testCases {
		// 把 w.result 的缓冲区设为 0，使写入被阻塞。
		//
		for j := range testCase.events {
			testCase.events[j].ResourceVersion = uint64(j) + 1
		}

		w := newCacheWatcher(0, filter, forget, storage.APIObjectVersioner{}, time.Now(), false, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
		go w.processInterval(context.Background(), intervalFromEvents(testCase.events), 0)

		ch := w.ResultChan()
		for j, event := range testCase.expected {
			e := <-ch
			if !reflect.DeepEqual(event, e) {
				t.Errorf("%d: unexpected event %d: %s", i, j, cmp.Diff(event, e))
				break TestCase
			}
		}
		select {
		case obj, ok := <-ch:
			t.Errorf("%d: unexpected excess event: %#v %t", i, obj, ok)
			break TestCase
		default:
		}
		w.setDrainInputBufferLocked(false)
		w.stopLocked()
	}
}

// TestCacheWatcherStoppedInAnotherGoroutine 回归测试 Stop 与超时定时器并发触发时的死锁/阻塞：
// 重复多轮以放大竞态窗口；每轮都要求 Stop 在 1 秒内完成。
func TestCacheWatcherStoppedInAnotherGoroutine(t *testing.T) {
	var w *cacheWatcher
	done := make(chan struct{})
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
		done <- struct{}{}
	}

	maxRetriesToProduceTheRaceCondition := 1000
	// 把 timeout 设为 0 并并发运行 Stop goroutine，模拟定时器触发与停止同时发生。
	//
	// 确保 watch 不会卡在 Stop 上。
	for i := 0; i < maxRetriesToProduceTheRaceCondition; i++ {
		w = newCacheWatcher(0, filter, forget, storage.APIObjectVersioner{}, time.Now(), false, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
		go w.Stop()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("stop is blocked when the timer is fired concurrently")
		}
	}

	deadline := time.Now().Add(time.Hour)
	// 随后验证 cacheWatcher.process goroutine 正常工作。
	for i := 0; i < maxRetriesToProduceTheRaceCondition; i++ {
		w = newCacheWatcher(2, filter, emptyFunc, storage.APIObjectVersioner{}, deadline, false, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
		w.input <- &watchCacheEvent{Object: &v1.Pod{}, ResourceVersion: uint64(i + 1)}
		ctx, cancel := context.WithDeadline(context.Background(), deadline)
		defer cancel()
		go w.processInterval(ctx, intervalFromEvents(nil), 0)
		select {
		case <-w.ResultChan():
		case <-time.After(time.Second):
			t.Fatal("expected received a event on ResultChan")
		}
		w.setDrainInputBufferLocked(false)
		w.stopLocked()
	}
}

// TestCacheWatcherStoppedOnDestroy 验证 cacher 停止后，所有活跃 watch 的 ResultChan 都会被关闭，
// 避免客户端协程永久挂起。
func TestCacheWatcherStoppedOnDestroy(t *testing.T) {
	backingStorage := &cachertesting.MockStorage{}
	cacher, _, err := newTestCacher(backingStorage)
	if err != nil {
		t.Fatalf("Couldn't create cacher: %v", err)
	}
	defer cacher.Stop()

	// 等待 cacher 初始化完成。
	if err := cacher.ready.wait(context.Background()); err != nil {
		t.Fatalf("unexpected error waiting for the cache to be ready")
	}

	w, err := cacher.Watch(context.Background(), "/pods/ns", storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Failed to create watch: %v", err)
	}

	watchClosed := make(chan struct{})
	go func() {
		defer close(watchClosed)
		for event := range w.ResultChan() {
			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				// 符合预期
			default:
				t.Errorf("unexpected event %#v", event)
			}
		}
	}()

	cacher.Stop()

	select {
	case <-watchClosed:
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("timed out waiting for watch to close")
	}

}

// TestResourceVersionAfterInitEvents 验证初始事件（来自 store 的快照）与通过 input 通道
// 重复投递的同一事件不会造成重复输出：processInterval 应按起始 resourceVersion 去重。
func TestResourceVersionAfterInitEvents(t *testing.T) {
	const numObjects = 10
	store := store.NewIndexer(nil)

	for i := 0; i < numObjects; i++ {
		elem := makeTestStoreElement(makeTestPod(fmt.Sprintf("pod-%d", i), uint64(i)))
		store.Add(elem)
	}

	wci, err := newCacheIntervalFromStore(numObjects, store, "", false)
	if err != nil {
		t.Fatal(err)
	}

	filter := func(_ string, _ labels.Set, _ fields.Set, _ runtime.Object) bool { return true }
	forget := func(_ bool) {}
	deadline := time.Now().Add(time.Minute)
	w := newCacheWatcher(numObjects+1, filter, forget, storage.APIObjectVersioner{}, deadline, true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")

	// 模拟最后一个已在状态中的事件尚未被 cacher 处理，
	// 因而会再次通过 channel 送达。
	//
	event := &watchCacheEvent{
		Type:            watch.Added,
		Object:          makeTestPod(fmt.Sprintf("pod-%d", numObjects-1), uint64(numObjects-1)),
		ResourceVersion: uint64(numObjects - 1),
	}
	if !w.add(event, time.NewTimer(time.Second)) {
		t.Fatalf("failed to add event")
	}
	w.stopLocked()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.processInterval(context.Background(), wci, uint64(numObjects-1))
	}()

	// 期望所有初始化事件都被送达。
	for i := 0; i < numObjects; i++ {
		<-w.ResultChan()
	}
	// 不期望其他事件被送达，因此
	// ResultChan 应被关闭。
	result, ok := <-w.ResultChan()
	if ok {
		t.Errorf("unexpected event: %#v", result)
	}

	wg.Wait()
}

// TestTimeBucketWatchersBasic 验证按截止时间分桶的 watcher 集合：
// 同一时刻的 watcher 归入同一桶，popExpiredWatchers 只返回已过期且每个桶只弹出一次。
func TestTimeBucketWatchersBasic(t *testing.T) {
	filter := func(_ string, _ labels.Set, _ fields.Set, _ runtime.Object) bool {
		return true
	}
	forget := func(bool) {}

	newWatcher := func(deadline time.Time) *cacheWatcher {
		w := newCacheWatcher(0, filter, forget, storage.APIObjectVersioner{}, deadline, true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
		w.setBookmarkAfterResourceVersion(0)
		return w
	}

	clock := testingclock.NewFakeClock(time.Now())
	watchers := newTimeBucketWatchers(clock, defaultBookmarkFrequency)
	now := clock.Now()
	watchers.addWatcherThreadUnsafe(newWatcher(now.Add(10 * time.Second)))
	watchers.addWatcherThreadUnsafe(newWatcher(now.Add(20 * time.Second)))
	watchers.addWatcherThreadUnsafe(newWatcher(now.Add(20 * time.Second)))

	if len(watchers.watchersBuckets) != 2 {
		t.Errorf("unexpected bucket size: %#v", watchers.watchersBuckets)
	}
	watchers0 := watchers.popExpiredWatchersThreadUnsafe()
	if len(watchers0) != 0 {
		t.Errorf("unexpected bucket size: %#v", watchers0)
	}

	clock.Step(10 * time.Second)
	watchers1 := watchers.popExpiredWatchersThreadUnsafe()
	if len(watchers1) != 1 || len(watchers1[0]) != 1 {
		t.Errorf("unexpected bucket size: %v", watchers1)
	}
	watchers1 = watchers.popExpiredWatchersThreadUnsafe()
	if len(watchers1) != 0 {
		t.Errorf("unexpected bucket size: %#v", watchers1)
	}

	clock.Step(12 * time.Second)
	watchers2 := watchers.popExpiredWatchersThreadUnsafe()
	if len(watchers2) != 1 || len(watchers2[0]) != 2 {
		t.Errorf("unexpected bucket size: %#v", watchers2)
	}
}

// makeWatchCacheEvent 构造一个指定 resourceVersion 的 Added 事件，供排空相关用例复用。
func makeWatchCacheEvent(rv uint64) *watchCacheEvent {
	return &watchCacheEvent{
		Type: watch.Added,
		Object: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", rv),
				ResourceVersion: fmt.Sprintf("%d", rv),
			},
		},
		ResourceVersion: rv,
	}
}

// TestCacheWatcherDraining 验证请求 draining 时 cacheWatcher.process goroutine 能被正确清理
//
// 排空（drain）语义：请求停止后仍把已入队的事件全部发送给客户端，然后关闭通道。
func TestCacheWatcherDraining(t *testing.T) {
	var lock sync.RWMutex
	var w *cacheWatcher
	count := 0
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		lock.Lock()
		defer lock.Unlock()
		count++
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
	}
	initEvents := []*watchCacheEvent{
		makeWatchCacheEvent(5),
		makeWatchCacheEvent(6),
	}
	w = newCacheWatcher(1, filter, forget, storage.APIObjectVersioner{}, time.Now(), true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
	go w.processInterval(context.Background(), intervalFromEvents(initEvents), 1)
	if !w.add(makeWatchCacheEvent(7), time.NewTimer(1*time.Second)) {
		t.Fatal("failed adding an even to the watcher")
	}
	forget(true) // 排空 watcher

	eventCount := 0
	for range w.ResultChan() {
		eventCount++
	}
	if eventCount != 3 {
		t.Errorf("Unexpected number of objects received: %d, expected: 3", eventCount)
	}
	if err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 2, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called twice, because processInterval should call Stop(): %v", err)
	}
}

// TestCacheWatcherDrainingRequestedButNotDrained 验证请求 draining 但客户端未取数据时也能正确清理
//
// 客户端不消费时，排空不能无限阻塞，协程仍须退出。
func TestCacheWatcherDrainingRequestedButNotDrained(t *testing.T) {
	var lock sync.RWMutex
	var w *cacheWatcher
	count := 0
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		lock.Lock()
		defer lock.Unlock()
		count++
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
	}
	initEvents := []*watchCacheEvent{
		makeWatchCacheEvent(5),
		makeWatchCacheEvent(6),
	}
	w = newCacheWatcher(1, filter, forget, storage.APIObjectVersioner{}, time.Now(), true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
	go w.processInterval(context.Background(), intervalFromEvents(initEvents), 1)
	if !w.add(makeWatchCacheEvent(7), time.NewTimer(1*time.Second)) {
		t.Fatal("failed adding an even to the watcher")
	}
	forget(true) // 排空 watcher
	w.Stop()     // 客户端断开、超时或 ctx 关闭
	if err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 3, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called three times, because processInterval should call Stop(): %v", err)
	}
}

// TestCacheWatcherDrainingNoBookmarkAfterResourceVersionReceived 验证添加事件超时且尚未收到 bookmark 时 watcher 会被停止
//
// 未收到 bookmark 时不排空：直接停止，避免客户端无限期等待一个永远不会来的书签。
func TestCacheWatcherDrainingNoBookmarkAfterResourceVersionReceived(t *testing.T) {
	var lock sync.RWMutex
	var w *cacheWatcher
	count := 0
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		lock.Lock()
		defer lock.Unlock()
		if drainWatcher {
			t.Fatalf("didn't expect drainWatcher to be set to true")
		}
		count++
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
	}
	initEvents := []*watchCacheEvent{
		{Object: &v1.Pod{}},
		{Object: &v1.Pod{}},
	}
	w = newCacheWatcher(0, filter, forget, storage.APIObjectVersioner{}, time.Now(), true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, "")
	w.setBookmarkAfterResourceVersion(10)
	go w.processInterval(context.Background(), intervalFromEvents(initEvents), 0)

	// 先取得一个事件，以便
	// 确认 w.processInterval 已被调度，
	//
	// 且正阻塞在向 result chan 发送另一事件上
	<-w.ResultChan()

	// 此时已知 processInterval 正在等待，
	// 再加入一个会超时的事件，
	// 触发清理流程。
	if w.add(&watchCacheEvent{Object: &v1.Pod{}}, time.NewTimer(10*time.Millisecond)) {
		t.Fatal("expected the add method to fail")
	}
	if err := wait.PollUntilContextTimeout(context.Background(), 100*time.Millisecond, 5*time.Second, true, func(_ context.Context) (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 2, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called twice, first call from w.add() and then from w.Stop() called from w.processInterval(): %v", err)
	}

	if !w.stopped {
		t.Fatal("expected the watcher to be stopped but it wasn't")
	}
}

// TestCacheWatcherDrainingNoBookmarkAfterResourceVersionSent 检查收到但未发送 bookmark 时，
// watcher 的输入 channel 是否被排空
//
// 已收到但尚未发出 bookmark 时必须排空：保证客户端能看到初始事件结束标记，
// 同时顺带断言分发延迟指标的分阶段统计。
func TestCacheWatcherDrainingNoBookmarkAfterResourceVersionSent(t *testing.T) {
	makePod := func(rv uint64) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:            fmt.Sprintf("pod-%d", rv),
				Namespace:       "ns",
				ResourceVersion: fmt.Sprintf("%d", rv),
				Annotations:     map[string]string{},
			},
		}
	}
	var lock sync.RWMutex
	var w *cacheWatcher
	watchInitializationSignal := utilflowcontrol.NewInitializationSignal()
	ctx := utilflowcontrol.WithInitializationSignal(context.Background(), watchInitializationSignal)
	count := 0
	filter := func(string, labels.Set, fields.Set, runtime.Object) bool { return true }
	forget := func(drainWatcher bool) {
		lock.Lock()
		defer lock.Unlock()
		count++
		w.setDrainInputBufferLocked(drainWatcher)
		w.stopLocked()
	}
	initEvents := []*watchCacheEvent{{Object: makePod(1)}, {Object: makePod(2)}}

	registry := compbasemetrics.NewKubeRegistry()
	if err := registry.Register(metrics.DispatchStageDuration); err != nil {
		t.Fatalf("unexpected error registering metric: %v", err)
	}
	t.Cleanup(func() {
		registry.Reset()
	})

	fakeClock := testingclock.NewFakeClock(time.Now())
	w = newCacheWatcher(2, filter, forget, storage.APIObjectVersioner{}, time.Now(), true, schema.GroupResource{Resource: "pods"}, metrics.NewWatcherMetricsObservers(schema.GroupResource{Resource: "pods"}), fakeClock, "")
	w.setBookmarkAfterResourceVersion(10)
	go w.processInterval(ctx, intervalFromEvents(initEvents), 0)
	watchInitializationSignal.Wait()

	// 注意可添加三个事件，尽管 chanSize 为 2，因为
	// 有一个事件已从输入 channel 取出
	if !w.add(&watchCacheEvent{Object: makePod(5), ResourceVersion: 5, RecordTime: fakeClock.Now().Add(-2 * time.Second), CacheReceived: fakeClock.Now().Add(-1 * time.Second)}, time.NewTimer(1*time.Second)) {
		t.Fatal("failed adding an even to the watcher")
	}
	if !w.nonblockingAdd(&watchCacheEvent{Type: watch.Bookmark, ResourceVersion: 10, Object: &v1.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "10"}}}) {
		t.Fatal("failed adding an even to the watcher")
	}
	if !w.add(&watchCacheEvent{Object: makePod(15), ResourceVersion: 15, RecordTime: fakeClock.Now().Add(-2 * time.Second), CacheReceived: fakeClock.Now().Add(-1 * time.Second)}, time.NewTimer(1*time.Second)) {
		t.Fatal("failed adding an even to the watcher")
	}
	if w.add(&watchCacheEvent{Object: makePod(20), ResourceVersion: 20}, time.NewTimer(1*time.Second)) {
		t.Fatal("expected the add method to fail")
	}
	if err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 1, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called once, just from the w.add() method: %v", err)
	}

	if !w.stopped {
		t.Fatal("expected the watcher to be stopped but it wasn't")
	}
	verifyEvents(t, w, []watch.Event{
		{Type: watch.Added, Object: makePod(1)},
		{Type: watch.Added, Object: makePod(2)},
		{Type: watch.Added, Object: makePod(5)},
		{Type: watch.Bookmark, Object: &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: "10",
				Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
			},
		}},
		{Type: watch.Added, Object: makePod(15)},
	}, true)

	if err := wait.PollImmediate(1*time.Second, 5*time.Second, func() (bool, error) {
		lock.RLock()
		defer lock.RUnlock()
		return count == 2, nil
	}); err != nil {
		t.Fatalf("expected forget() to be called twice, the second call is from w.Stop() method called from  w.processInterval(): %v", err)
	}

	expected := `
# HELP apiserver_watch_events_dispatch_duration_seconds [ALPHA] Histogram of watch event dispatch latency broken by resource type and pipeline stage. The 'total' stage is the end-to-end latency of a delivered event.
# TYPE apiserver_watch_events_dispatch_duration_seconds histogram
apiserver_watch_events_dispatch_duration_seconds_bucket{group="",resource="pods",stage="cache_to_watcher",le="+Inf"} 2
apiserver_watch_events_dispatch_duration_seconds_sum{group="",resource="pods",stage="cache_to_watcher"} 0
apiserver_watch_events_dispatch_duration_seconds_count{group="",resource="pods",stage="cache_to_watcher"} 2
apiserver_watch_events_dispatch_duration_seconds_bucket{group="",resource="pods",stage="storage_to_cache",le="+Inf"} 2
apiserver_watch_events_dispatch_duration_seconds_sum{group="",resource="pods",stage="storage_to_cache"} 2
apiserver_watch_events_dispatch_duration_seconds_count{group="",resource="pods",stage="storage_to_cache"} 2
apiserver_watch_events_dispatch_duration_seconds_bucket{group="",resource="pods",stage="total",le="+Inf"} 2
apiserver_watch_events_dispatch_duration_seconds_sum{group="",resource="pods",stage="total"} 4
apiserver_watch_events_dispatch_duration_seconds_count{group="",resource="pods",stage="total"} 2
`
	if err := testutil.GatherAndCompare(gatherWithoutBuckets(registry), strings.NewReader(expected), "apiserver_watch_events_dispatch_duration_seconds"); err != nil {
		t.Fatal(err)
	}
}

// TestBookmarkAfterResourceVersionWatchers 验证等待初始事件 bookmark 的 watcher 调度策略：
// 在收到 bookmark 前会被立即反复标记为过期以便重试；收到后改为按正常 bookmark 频率调度。
func TestBookmarkAfterResourceVersionWatchers(t *testing.T) {
	newWatcher := func(id string, deadline time.Time) *cacheWatcher {
		w := newCacheWatcher(0, func(_ string, _ labels.Set, _ fields.Set, _ runtime.Object) bool { return true }, func(bool) {}, storage.APIObjectVersioner{}, deadline, true, schema.GroupResource{Resource: "pods"}, metrics.NewNoopWatcherMetricsObservers(), nil, id)
		w.setBookmarkAfterResourceVersion(10)
		return w
	}

	clock := testingclock.NewFakeClock(time.Now())
	target := newTimeBucketWatchers(clock, defaultBookmarkFrequency)
	if !target.addWatcherThreadUnsafe(newWatcher("1", clock.Now().Add(2*time.Minute))) {
		t.Fatal("failed adding an even to the watcher")
	}

	// watcher 立即过期（它在等 bookmark，因此被立即调度）
	ret := target.popExpiredWatchersThreadUnsafe()
	if len(ret) != 1 || len(ret[0]) != 1 {
		t.Fatalf("expected only one watcher to be expired")
	}
	if !target.addWatcherThreadUnsafe(ret[0][0]) {
		t.Fatal("failed adding an even to the watcher")
	}

	// 一秒后 watcher 仍为过期状态
	clock.Step(1 * time.Second)
	ret = target.popExpiredWatchersThreadUnsafe()
	if len(ret) != 1 || len(ret[0]) != 1 {
		t.Fatalf("expected only one watcher to be expired")
	}
	if !target.addWatcherThreadUnsafe(ret[0][0]) {
		t.Fatal("failed adding an even to the watcher")
	}

	// 29 秒后 watcher 仍为过期状态
	clock.Step(29 * time.Second)
	ret = target.popExpiredWatchersThreadUnsafe()
	if len(ret) != 1 || len(ret[0]) != 1 {
		t.Fatalf("expected only one watcher to be expired")
	}

	// 在确认 watcher 未立即过期之后
	ret[0][0].markBookmarkAfterRvAsReceived(&watchCacheEvent{Type: watch.Bookmark, ResourceVersion: 10, Object: &v1.Pod{}})
	if !target.addWatcherThreadUnsafe(ret[0][0]) {
		t.Fatal("failed adding an even to the watcher")
	}
	clock.Step(30 * time.Second)
	ret = target.popExpiredWatchersThreadUnsafe()
	if len(ret) != 0 {
		t.Fatalf("didn't expect any watchers to be expired")
	}

	clock.Step(30 * time.Second)
	ret = target.popExpiredWatchersThreadUnsafe()
	if len(ret) != 1 || len(ret[0]) != 1 {
		t.Fatalf("expected only one watcher to be expired")
	}
}

// gatherWithoutBuckets 返回去掉直方图分桶的采集器，使指标比较不依赖桶边界配置。
func gatherWithoutBuckets(gatherer compbasemetrics.Gatherer) testutil.GathererFunc {
	return func() ([]*testutil.MetricFamily, error) {
		got, err := gatherer.Gather()
		for _, mf := range got {
			for _, m := range mf.Metric {
				if m.Histogram == nil {
					continue
				}
				m.Histogram.Bucket = nil
			}
		}
		return got, err
	}
}
