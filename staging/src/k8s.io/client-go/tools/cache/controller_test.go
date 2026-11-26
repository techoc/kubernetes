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

package cache

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	fcache "k8s.io/client-go/tools/cache/testing"
	"k8s.io/klog/v2/ktesting"

	"sigs.k8s.io/randfill"
)

// Example 演示直接用 Config + RealFIFO 组装一个底层控制器。
//
// 控制器对每个到达的 Pod 先写入下游 Store，再立即从假数据源删除它，
// 因此最终只会观察到删除事件；输出顺序经集合排序以保证稳定。
func Example() {
	// source 模拟 apiserver 的对象端点。
	source := fcache.NewFakeControllerSource()
	defer source.Shutdown()

	// 保存我们观察到的下游状态。
	downstream := NewStore(DeletionHandlingMetaNamespaceKeyFunc)

	// 保存进来的变更。把 downstream 作为 KeyLister 传入，resync 才能产生正确的 update/delete delta。
	fifo := NewRealFIFO(MetaNamespaceKeyFunc, downstream, nil)

	// 用线程安全的输出以便测试结果可预测。
	deletionCounter := make(chan string, 1000)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    source,
		ObjectType:       &v1.Pod{},
		FullResyncPeriod: time.Millisecond * 100,

		// 实现一个简单的控制器：删除所有进来的对象。
		Process: func(obj interface{}, isInInitialList bool) error {
			// Obj 来自上面队列的 Pop。
			newest := obj.(Deltas).Newest()

			if newest.Type != Deleted {
				// 更新下游存储。
				err := downstream.Add(newest.Object)
				if err != nil {
					return err
				}

				// 删除该对象。
				source.Delete(newest.Object.(runtime.Object))
			} else {
				// 更新下游存储。
				err := downstream.Delete(newest.Object)
				if err != nil {
					return err
				}

				// 用 fifo 的 KeyOf 最简单，因为它能处理
				// 处理 DeletedFinalStateUnknown 标记。
				key, err := fifo.keyOf(newest.Object)
				if err != nil {
					return err
				}

				// 记录这次删除。
				deletionCounter <- key
			}
			return nil
		},
	}

	// 创建控制器并运行，直到取消。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go New(cfg).RunWithContext(ctx)

	// 向 source 加入若干对象。
	testIDs := []string{"a-hello", "b-controller", "c-framework"}
	for _, name := range testIDs {
		// 这些 Pod 并不合法——fake source 不做校验。
		source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}

	// 等待控制器处理刚加入的对象。
	outputSet := sets.Set[string]{}
	for i := 0; i < len(testIDs); i++ {
		outputSet.Insert(<-deletionCounter)
	}

	for _, key := range sets.List(outputSet) {
		fmt.Println(key)
	}
	// Output:
	// a-hello
	// b-controller
	// c-framework
}

// ExampleNewInformer 演示 NewInformer 的用法：AddFunc 中立即删除对象，
// DeleteFunc 中记录键，最终输出被删除对象的键。
func ExampleNewInformer() {
	// source 模拟 apiserver 的对象端点。
	source := fcache.NewFakeControllerSource()
	defer source.Shutdown()

	// 用线程安全的输出以便测试结果可预测。
	deletionCounter := make(chan string, 1000)

	// 创建一个立即删除所有新增对象并记录删除的控制器。
	_, controller := NewInformer(
		source,
		&v1.Pod{},
		time.Millisecond*100,
		ResourceEventHandlerDetailedFuncs{
			AddFunc: func(obj interface{}, isInInitialList bool) {
				source.Delete(obj.(runtime.Object))
			},
			DeleteFunc: func(obj interface{}) {
				key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
				if err != nil {
					key = "oops something went wrong with the key"
				}

				// 记录这次删除。
				deletionCounter <- key
			},
		},
	)

	// 运行控制器直至取消。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go controller.RunWithContext(ctx)

	// 向 source 加入若干对象。
	testIDs := []string{"a-hello", "b-controller", "c-framework"}
	for _, name := range testIDs {
		// 这些 Pod 并不合法——fake source 不做校验。
		source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}

	// 等待控制器处理刚加入的对象。
	outputSet := sets.Set[string]{}
	for range testIDs {
		outputSet.Insert(<-deletionCounter)
	}

	for _, key := range sets.List(outputSet) {
		fmt.Println(key)
	}
	// Output:
	// a-hello
	// b-controller
	// c-framework
}

// TestHammerController 并发地增删改大量对象，用来暴露锁与协程错误；
// 失败通常表现为死锁、竞态检测器报错或测试挂起。
func TestHammerController(t *testing.T) {
	// 本测试通过 fake source 与控制器框架执行大量请求，以排查锁/线程问题；
	// 若有问题，测试会永久挂起或触发竞态检测器。

	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	// 用线程安全的输出以便测试结果可预测。
	outputSetLock := sync.Mutex{}
	// key 到该 key 上执行的操作的映射。
	outputSet := map[string][]string{}

	recordFunc := func(eventType string, obj interface{}) {
		key, err := DeletionHandlingMetaNamespaceKeyFunc(obj)
		if err != nil {
			t.Errorf("something wrong with key: %v", err)
			key = "oops something went wrong with the key"
		}

		// 删除条目时记录一些输出。
		outputSetLock.Lock()
		defer outputSetLock.Unlock()
		outputSet[key] = append(outputSet[key], eventType)
	}

	// 创建一个只记录收到的所有变更的控制器。
	_, controller := NewInformer(
		source,
		&v1.Pod{},
		time.Millisecond*100,
		ResourceEventHandlerDetailedFuncs{
			AddFunc:    func(obj interface{}, isInInitialList bool) { recordFunc("add", obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { recordFunc("update", newObj) },
			DeleteFunc: func(obj interface{}) { recordFunc("delete", obj) },
		},
	)

	if controller.HasSynced() {
		t.Errorf("Expected HasSynced() to return false before we started the controller")
	}

	// 运行控制器直至取消。
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	var controllerWG sync.WaitGroup
	controllerWG.Add(1)
	go func() {
		defer controllerWG.Done()
		controller.RunWithContext(ctx)
	}()

	// 等待控制器完成初始同步。
	// 轮询而非阻塞等待，因为此处要验证的是 HasSynced 状态本身。
	wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		return controller.HasSynced(), nil
	})
	if !controller.HasSynced() {
		t.Errorf("Expected HasSynced() to return true after the initial sync")
	}

	wg := sync.WaitGroup{}
	const threads = 3
	wg.Add(threads)
	for i := 0; i < threads; i++ {
		go func() {
			defer wg.Done()
			// 向 source 加入若干对象。
			currentNames := sets.Set[string]{}
			rs := rand.NewSource(rand.Int63())
			f := randfill.New().NilChance(.5).NumElements(0, 2).RandSource(rs)
			for i := 0; i < 100; i++ {
				var name string
				var isNew bool
				if currentNames.Len() == 0 || rand.Intn(3) == 1 {
					f.Fill(&name)
					isNew = true
				} else {
					l := sets.List(currentNames)
					name = l[rand.Intn(len(l))]
				}

				pod := &v1.Pod{}
				f.Fill(pod)
				pod.ObjectMeta.Name = name
				pod.ObjectMeta.Namespace = "default"
				// 随机添加、更新或删除。
				// 这些 Pod 并不合法——fake source 不做校验或其他检查。
				if isNew {
					currentNames.Insert(name)
					source.Add(pod)
					continue
				}
				switch rand.Intn(2) {
				case 0:
					currentNames.Insert(name)
					source.Modify(pod)
				case 1:
					currentNames.Delete(name)
					source.Delete(pod)
				}
			}
		}()
	}
	wg.Wait()

	// 等待控制器处理完刚加入的对象。
	// TODO：检查队列中有多少条目待处理。
	time.Sleep(100 * time.Millisecond)
	cancel()

	// 永久锁定该互斥量前，必须确认控制器已停止运行，此时所有 goroutine 都应已退出。泄漏检查
	// 泄漏检查由 TestMain 完成。
	controllerWG.Wait()

	outputSetLock.Lock()
	t.Logf("got: %#v", outputSet)
}

// TestUpdate 验证各种合法的更新迁移组合。
//
// 由于重同步与 watch 事件异步到达，测试只接受 {from->to, to->to, from->from}
// 三种迁移，其他组合说明事件顺序或缓存状态有误。
func TestUpdate(t *testing.T) {
	// 本测试覆盖导致
	// 触发更新的调用。

	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	const (
		FROM = "from"
		TO   = "to"
	)

	// 以下是预期观察到的状态转换；由于是异步的，存在多种合法可能。
	type pair struct{ from, to string }
	allowedTransitions := map[pair]bool{
		{FROM, TO}: true,

		// 因为 resync 可能发生在已观察到上述变更之后、条目被删除之前。
		{TO, TO}: true,
		// 因为 resync 可能发生在我们观察到更新之前。
		{FROM, FROM}: true,
	}

	pod := func(name, check string, final bool) *v1.Pod {
		p := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:   name,
				Labels: map[string]string{"check": check},
			},
		}
		if final {
			p.Labels["final"] = "true"
		}
		return p
	}
	deletePod := func(p *v1.Pod) bool {
		return p.Labels["final"] == "true"
	}

	tests := []func(string){
		func(name string) {
			name = "a-" + name
			source.Add(pod(name, FROM, false))
			source.Modify(pod(name, TO, true))
		},
	}

	const threads = 3

	var testDoneWG sync.WaitGroup
	testDoneWG.Add(threads * len(tests))

	// 创建一个观察到更新就删除对象的控制器；删除时调用 WaitGroup.Done()，以便判断所有加入的对象是否都已删除。
	watchCh := make(chan struct{})
	_, controller := NewInformer(
		toListWatcherWithUnSupportedWatchListSemantics(&ListWatch{
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				watch, err := source.Watch(options)
				close(watchCh)
				return watch, err
			},
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				return source.List(options)
			},
		}),
		&v1.Pod{},
		0,
		ResourceEventHandlerFuncs{
			UpdateFunc: func(oldObj, newObj interface{}) {
				o, n := oldObj.(*v1.Pod), newObj.(*v1.Pod)
				from, to := o.Labels["check"], n.Labels["check"]
				if !allowedTransitions[pair{from, to}] {
					t.Errorf("observed transition %q -> %q for %v", from, to, n.Name)
				}
				if deletePod(n) {
					source.Delete(n)
				}
			},
			DeleteFunc: func(obj interface{}) {
				testDoneWG.Done()
			},
		},
	)

	// 运行控制器直至取消。
	// 一旦调用 Run()，testDoneWG.Done() 就可能开始，因此所有 Add() 必须在此之前完成。
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go controller.RunWithContext(ctx)
	<-watchCh

	// 并行把每个测试跑几遍。
	var wg sync.WaitGroup
	wg.Add(threads * len(tests))
	for i := 0; i < threads; i++ {
		for j, f := range tests {
			go func(name string, f func(string)) {
				defer wg.Done()
				f(name)
			}(fmt.Sprintf("%v-%v", i, j), f)
		}
	}
	wg.Wait()

	// 等待控制器处理刚加入的对象。
	testDoneWG.Wait()
}

// TestPanicPropagated 验证 handler 中的 panic 不会被控制器吞掉，而是向调用
// RunWithContext 的协程传播，便于上层决定是否崩溃。
func TestPanicPropagated(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	// 创建一个 AddFunc 被调用就 panic 的控制器。
	_, controller := NewInformer(
		source,
		&v1.Pod{},
		time.Millisecond*100,
		ResourceEventHandlerDetailedFuncs{
			AddFunc: func(obj interface{}, isInInitialList bool) {
				// 触发 panic。
				panic("Just panic.")
			},
		},
	)

	// 运行控制器直至取消。
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	propagated := make(chan interface{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				propagated <- r
			}
		}()
		controller.RunWithContext(ctx)
	}()
	// 向 source 加入一个对象，将触发 panic。
	source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "test"}})

	// 检查 panic 是否已向上传播。
	select {
	case p := <-propagated:
		if p == "Just panic." {
			t.Logf("Test Passed")
		} else {
			t.Errorf("unrecognized panic in controller run: %v", p)
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("timeout: the panic failed to propagate from the controller run method!")
	}
}

// TestTransformingInformer 验证 Transform 在对象进入 Store 之前生效，
// 且 handler 收到的是转换后的对象。
func TestTransformingInformer(t *testing.T) {
	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	makePod := func(name, generation string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "namespace",
				Labels:    map[string]string{"generation": generation},
			},
			Spec: v1.PodSpec{
				Hostname:  "hostname",
				Subdomain: "subdomain",
			},
		}
	}
	expectedPod := func(name, generation string) *v1.Pod {
		pod := makePod(name, generation)
		pod.Spec.Hostname = "new-hostname"
		pod.Spec.Subdomain = ""
		pod.Spec.NodeName = "nodename"
		return pod
	}

	source.Add(makePod("pod1", "1"))
	source.Modify(makePod("pod1", "2"))

	type event struct {
		eventType watch.EventType
		previous  interface{}
		current   interface{}
	}
	events := make(chan event, 10)
	recordEvent := func(eventType watch.EventType, previous, current interface{}) {
		events <- event{eventType: eventType, previous: previous, current: current}
	}
	verifyEvent := func(eventType watch.EventType, previous, current interface{}) {
		select {
		case event := <-events:
			if event.eventType != eventType {
				t.Errorf("expected type %v, got %v", eventType, event.eventType)
			}
			if !apiequality.Semantic.DeepEqual(event.previous, previous) {
				t.Errorf("expected previous object %#v, got %#v", previous, event.previous)
			}
			if !apiequality.Semantic.DeepEqual(event.current, current) {
				t.Errorf("expected object %#v, got %#v", current, event.current)
			}
		case <-time.After(wait.ForeverTestTimeout):
			t.Errorf("failed to get event")
		}
	}

	podTransformer := func(obj interface{}) (interface{}, error) {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected object type: %T", obj)
		}
		pod.Spec.Hostname = "new-hostname"
		pod.Spec.Subdomain = ""
		pod.Spec.NodeName = "nodename"

		// 清空 ResourceVersion 以简化比较。
		pod.ResourceVersion = ""

		return pod, nil
	}

	store, controller := NewTransformingInformer(
		source,
		&v1.Pod{},
		0,
		ResourceEventHandlerDetailedFuncs{
			AddFunc:    func(obj interface{}, isInInitialList bool) { recordEvent(watch.Added, nil, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { recordEvent(watch.Modified, oldObj, newObj) },
			DeleteFunc: func(obj interface{}) { recordEvent(watch.Deleted, obj, nil) },
		},
		podTransformer,
	)

	verifyStore := func(expectedItems []interface{}) {
		items := store.List()
		if len(items) != len(expectedItems) {
			t.Errorf("unexpected items %v, expected %v", items, expectedItems)
		}
		for _, expectedItem := range expectedItems {
			found := false
			for _, item := range items {
				if apiequality.Semantic.DeepEqual(item, expectedItem) {
					found = true
				}
			}
			if !found {
				t.Errorf("expected item %v not found in %v", expectedItem, items)
			}
		}
	}

	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go controller.RunWithContext(ctx)

	verifyEvent(watch.Added, nil, expectedPod("pod1", "2"))
	verifyStore([]interface{}{expectedPod("pod1", "2")})

	source.Add(makePod("pod2", "1"))
	verifyEvent(watch.Added, nil, expectedPod("pod2", "1"))
	verifyStore([]interface{}{expectedPod("pod1", "2"), expectedPod("pod2", "1")})

	source.Add(makePod("pod3", "1"))
	verifyEvent(watch.Added, nil, expectedPod("pod3", "1"))

	source.Modify(makePod("pod2", "2"))
	verifyEvent(watch.Modified, expectedPod("pod2", "1"), expectedPod("pod2", "2"))

	source.Delete(makePod("pod1", "2"))
	verifyEvent(watch.Deleted, expectedPod("pod1", "2"), nil)
	verifyStore([]interface{}{expectedPod("pod2", "2"), expectedPod("pod3", "1")})
}

// TestTransformingInformerRace 在重同步期间并发读取 Store，验证转换后的对象
// 不会因共享指针被二次转换或并发修改；badTransform 一旦置位即失败。
func TestTransformingInformerRace(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	// 仅在测试结束时取消。
	testCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 测试过程中也可能取消。
	ctx, cancel = context.WithCancel(ctx)
	defer cancel()

	// source 模拟 apiserver 的对象端点。
	source := newFakeControllerSource(t)

	label := "to-be-transformed"
	makePod := func(name string) *v1.Pod {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "namespace",
				Labels:    map[string]string{label: "true"},
			},
			Spec: v1.PodSpec{
				Hostname: "hostname",
			},
		}
	}

	badTransform := atomic.Bool{}
	podTransformer := func(obj interface{}) (interface{}, error) {
		pod, ok := obj.(*v1.Pod)
		if !ok {
			return nil, fmt.Errorf("unexpected object type: %T", obj)
		}
		if pod.ObjectMeta.Labels[label] != "true" {
			badTransform.Store(true)
			return nil, fmt.Errorf("object already transformed: %#v", obj)
		}
		pod.ObjectMeta.Labels[label] = "false"
		return pod, nil
	}

	numObjs := 5
	for i := 0; i < numObjs; i++ {
		source.Add(makePod(fmt.Sprintf("pod-%d", i)))
	}

	type event struct{}
	events := make(chan event, numObjs)
	recordEvent := func(eventType watch.EventType, previous, current interface{}) {
		select {
		case events <- event{}:
		case <-testCtx.Done():
			// 测试已结束时不要在上面的写入处永久阻塞。
		}
	}
	checkEvents := func(count int) {
		for i := 0; i < count; i++ {
			<-events
		}
	}
	store, controller := NewTransformingInformer(
		source,
		&v1.Pod{},
		5*time.Millisecond,
		ResourceEventHandlerDetailedFuncs{
			AddFunc:    func(obj interface{}, isInInitialList bool) { recordEvent(watch.Added, nil, obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { recordEvent(watch.Modified, oldObj, newObj) },
			DeleteFunc: func(obj interface{}) { recordEvent(watch.Deleted, obj, nil) },
		},
		podTransformer,
	)

	go controller.RunWithContext(ctx)

	checkEvents(numObjs)

	// 周期性读取对象，检测是否存在并发访问竞态。
	wg := sync.WaitGroup{}
	errors := make(chan error, numObjs)
	for i := 0; i < numObjs; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := fmt.Sprintf("namespace/pod-%d", index)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				obj, ok, err := store.GetByKey(key)
				if !ok || err != nil {
					errors <- fmt.Errorf("couldn't get the object for %v", key)
					return
				}
				pod := obj.(*v1.Pod)
				if pod.ObjectMeta.Labels[label] != "false" {
					errors <- fmt.Errorf("unexpected object: %#v", pod)
					return
				}
			}
		}(i)
	}

	// 让 resync 运行一段时间。
	time.Sleep(time.Second)

	cancel()
	wg.Wait()
	close(errors)
	for err := range errors {
		t.Error(err)
	}

	if badTransform.Load() {
		t.Errorf("unexpected transformation happened")
	}
}

// TestDeletionHandlingObjectToName 验证 DeletedFinalStateUnknown 会直接使用
// 其 Key，而不是重新从对象元数据推导。
func TestDeletionHandlingObjectToName(t *testing.T) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname",
			Namespace: "testnamespace",
		},
	}
	stringKey, err := MetaNamespaceKeyFunc(cm)
	if err != nil {
		t.Error(err)
	}
	deleted := DeletedFinalStateUnknown{
		Key: stringKey,
		Obj: cm,
	}
	expected, err := ObjectToName(cm)
	if err != nil {
		t.Error(err)
	}
	actual, err := DeletionHandlingObjectToName(deleted)
	if err != nil {
		t.Error(err)
	}
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}
}

// TestDeletedObjectGetObjectName 覆盖三种来源：正常对象、FinalStateUnknown
// 带对象、FinalStateUnknown 仅带 Key，以及完全零值对象不得 panic。
func TestDeletedObjectGetObjectName(t *testing.T) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname",
			Namespace: "testnamespace",
		},
	}
	expected, err := ObjectToName(cm)
	if err != nil {
		t.Error(err)
	}

	deleted := DeletedObject[*v1.ConfigMap]{OptionalObj: cm}
	actual := deleted.GetObjectName()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	stringKey, err := MetaNamespaceKeyFunc(cm)
	if err != nil {
		t.Error(err)
	}
	deletedUnknown := DeletedObject[*v1.ConfigMap]{
		FinalStateUnknown: &DeletedFinalStateUnknown{
			Key: stringKey,
			Obj: cm,
		},
	}
	actual = deletedUnknown.GetObjectName()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	// informer 从未见过该对象时 FinalStateUnknown.Obj 可能为 nil，但 key 仍然可信。
	deletedUnknownNoObj := DeletedObject[*v1.ConfigMap]{
		FinalStateUnknown: &DeletedFinalStateUnknown{
			Key: stringKey,
			Obj: nil,
		},
	}
	actual = deletedUnknownNoObj.GetObjectName()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	// 完全空的 DeletedObject（无 FinalStateUnknown、OptionalObj 为 nil）不得 panic，应返回零值 ObjectName。
	var empty DeletedObject[*v1.ConfigMap]
	if actual := empty.GetObjectName(); actual != (ObjectName{}) {
		t.Errorf("Expected empty ObjectName, got %#v", actual)
	}
}

// TestDeletedObjectGetKey 验证 GetKey 与 MetaNamespaceKeyFunc 编码一致，
// 并覆盖 FinalStateUnknown 与零值对象。
func TestDeletedObjectGetKey(t *testing.T) {
	cm := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname",
			Namespace: "testnamespace",
		},
	}
	expected, err := MetaNamespaceKeyFunc(cm)
	if err != nil {
		t.Error(err)
	}

	deleted := DeletedObject[*v1.ConfigMap]{OptionalObj: cm}
	actual := deleted.GetKey()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	deletedUnknown := DeletedObject[*v1.ConfigMap]{
		FinalStateUnknown: &DeletedFinalStateUnknown{
			Key: expected,
			Obj: cm,
		},
	}
	actual = deletedUnknown.GetKey()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	// informer 从未见过该对象时 FinalStateUnknown.Obj 可能为 nil，但 key 仍然可信。
	deletedUnknownNoObj := DeletedObject[*v1.ConfigMap]{
		FinalStateUnknown: &DeletedFinalStateUnknown{
			Key: expected,
			Obj: nil,
		},
	}
	actual = deletedUnknownNoObj.GetKey()
	if expected != actual {
		t.Errorf("Expected %#v, got %#v", expected, actual)
	}

	// 完全空的 DeletedObject（无 FinalStateUnknown、OptionalObj 为 nil）不得 panic，应返回空字符串。
	var empty DeletedObject[*v1.ConfigMap]
	if actual := empty.GetKey(); actual != "" {
		t.Errorf("Expected empty key, got %#v", actual)
	}
}

// listWatchWithUnSupportedWatchListSemanticsWrapper 用于模拟不支持 WatchList
// 语义的客户端；嵌入 ListWatch 以保留 List/Watch 行为。
type listWatchWithUnSupportedWatchListSemanticsWrapper struct {
	*ListWatch
}

func (lw listWatchWithUnSupportedWatchListSemanticsWrapper) IsWatchListSemanticsUnSupported() bool {
	return true
}

// toListWatcherWithUnSupportedWatchListSemantics 把普通 ListWatch 包成
// “不支持 WatchList 语义”的变体，用于测试 reflector 的回退路径。
func toListWatcherWithUnSupportedWatchListSemantics(lw *ListWatch) ListerWatcher {
	return listWatchWithUnSupportedWatchListSemanticsWrapper{
		lw,
	}
}

// TestProcessDeltasInBatch 验证批量事务的部分成功语义：
// 只有已提交的对象触发 handler，失败对象既不落盘也不回调。
func TestProcessDeltasInBatch(t *testing.T) {
	cm1 := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname1",
			Namespace: "testnamespace",
		},
	}
	cm2 := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname2",
			Namespace: "testnamespace",
		},
	}
	cm3 := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "testname3",
			Namespace: "testnamespace",
		},
	}
	testDelta1 := Delta{
		Type:   Added,
		Object: cm1,
	}
	testDelta2 := Delta{
		Type:   Added,
		Object: cm2,
	}
	testDelta3 := Delta{
		Type:   Added,
		Object: cm3,
	}
	testCases := []struct {
		name                            string
		deltaList                       []Delta
		succeedingObjects               []runtime.Object
		failingObjects                  []runtime.Object
		expectedListenerReceivedObjects []interface{}
		expectedSuccessCount            int
		assertErr                       func(error) bool
	}{
		{
			name:                            "all transaction succeeding should works",
			deltaList:                       []Delta{testDelta1},
			expectedSuccessCount:            1,
			expectedListenerReceivedObjects: []interface{}{cm1},
		},
		{
			name:                 "all transaction failing should not trigger listener",
			deltaList:            []Delta{testDelta1},
			failingObjects:       []runtime.Object{cm1},
			expectedSuccessCount: 0,
			assertErr: func(err error) bool {
				return assert.Contains(t, err.Error(), "failed to execute (1/1) transactions")
			},
			expectedListenerReceivedObjects: make([]interface{}, 0),
		},
		{
			name:                 "partial transaction failing should only trigger successful events to listener #1",
			deltaList:            []Delta{testDelta1, testDelta2},
			failingObjects:       []runtime.Object{cm2},
			expectedSuccessCount: 1,
			assertErr: func(err error) bool {
				return assert.Contains(t, err.Error(), "failed to execute (1/2) transactions")
			},
			expectedListenerReceivedObjects: []interface{}{cm1},
		},
		{
			name:                 "partial transaction failing should only trigger successful events to listener #2",
			deltaList:            []Delta{testDelta1, testDelta2, testDelta3},
			failingObjects:       []runtime.Object{cm2},
			expectedSuccessCount: 2,
			assertErr: func(err error) bool {
				return assert.Contains(t, err.Error(), "failed to execute (1/3) transactions")
			},
			expectedListenerReceivedObjects: []interface{}{cm1, cm3},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			mockStore := &mockTxnStore{
				Store:       NewStore(MetaNamespaceKeyFunc),
				failingObjs: tc.failingObjects,
			}
			actualListenerReceivedObjects := make([]interface{}, 0)
			dummyListener := ResourceEventHandlerFuncs{
				AddFunc: func(obj interface{}) {
					actualListenerReceivedObjects = append(actualListenerReceivedObjects, obj)
				},
				UpdateFunc: func(oldObj, newObj interface{}) {
					actualListenerReceivedObjects = append(actualListenerReceivedObjects, newObj)
				},
				DeleteFunc: func(obj interface{}) {
					actualListenerReceivedObjects = append(actualListenerReceivedObjects, obj)
				},
			}
			err := processDeltasInBatch(
				logger,
				dummyListener,
				mockStore,
				tc.deltaList,
				true,
				DeletionHandlingMetaNamespaceKeyFunc)
			if tc.assertErr != nil {
				assert.True(t, tc.assertErr(err))
			}
			assert.Equal(t, tc.expectedSuccessCount, mockStore.succeedCount)
			assert.Equal(t, tc.expectedListenerReceivedObjects, actualListenerReceivedObjects)
		})
	}
}

var _ TransactionStore = &mockTxnStore{}

// mockTxnStore 让指定对象在事务中失败，用于构造部分成功场景。
// succeedCount 记录成功提交的数量，供测试断言。
type mockTxnStore struct {
	failingObjs  []runtime.Object
	succeedCount int
	Store
}

func (m *mockTxnStore) Transaction(txns ...Transaction) *TransactionError {
	successfuls := make([]int, 0)
	fails := make([]int, 0)
	errs := make([]error, 0)
	for i := range txns {
		txn := txns[i]
		for _, fail := range m.failingObjs {
			if apiequality.Semantic.DeepEqual(fail, txn.Object) {
				fails = append(fails, i)
				errs = append(errs, errors.New("test error"))
			} else {
				successfuls = append(successfuls, i)
			}
		}
	}
	if len(fails) > 0 {
		m.succeedCount = len(successfuls)
		return &TransactionError{
			TotalTransactions: len(txns),
			SuccessfulIndices: successfuls,
			Errors:            errs,
		}
	}
	m.succeedCount = len(txns)
	return nil
}

// TestReplaceEvents 验证原子与非原子两种 replace 路径产生相同的事件和
// 缓存最终状态；两种模式共享同一组子测试。
func TestReplaceEvents(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// 验证原子与非原子 Replace 在事件和 store 状态上表现一致。
	for _, atomic := range []bool{false, true} {
		source := fcache.NewFakeControllerSource()
		t.Cleanup(func() {
			source.Shutdown()
		})
		store := NewStore(DeletionHandlingMetaNamespaceKeyFunc)
		fifoOptions := RealFIFOOptions{
			AtomicEvents: atomic,
		}
		if !atomic {
			fifoOptions.KnownObjects = store
		}
		fifo := NewRealFIFOWithOptions(fifoOptions)
		recorder := newEventRecorder(store, DeletionHandlingMetaNamespaceKeyFunc)

		cfg := &Config{
			Queue:            fifo,
			ListerWatcher:    source,
			ObjectType:       &v1.Pod{},
			FullResyncPeriod: 0,

			Process: func(obj interface{}, isInInitialList bool) error {
				if deltas, ok := obj.(Deltas); ok {
					return processDeltas(fifo.logger, recorder, store, deltas, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
				}
				return errors.New("object given as Process argument is not Deltas")
			},
			ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
				return processDeltasInBatch(fifo.logger, recorder, store, deltaList, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
			},
		}
		c := New(cfg)
		go c.RunWithContext(ctx)
		if !WaitForCacheSync(ctx.Done(), c.HasSynced) {
			t.Fatal("Timed out waiting for cache sync")
		}
		testReplaceEvents(t, ctx, fifo, recorder, store, atomic)
	}
}

// testReplaceEvents 是 TestReplaceEvents 的共享用例集。
//
// 期望顺序固定为“先删除、再按输入顺序新增/更新”，与 RealFIFO#Replace 一致；
// 删除事件之间无固有顺序，比较前按 key 排序。
func testReplaceEvents(t *testing.T, ctx context.Context, fifo Queue, m *eventRecorder, store Store, atomic bool) {
	tcs := []struct {
		name            string
		initialObjs     []metav1.Object
		replacedItems   []interface{}
		replaceRV       string
		expectedHistory []eventRecord
	}{
		{
			name: "Create with no initial objects",
			replacedItems: []interface{}{
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", ResourceVersion: "1"}},
			},
			expectedHistory: []eventRecord{
				{Action: "add", Key: "default/pod-1", EventRV: "1", StoreRV: "1"},
			},
			replaceRV: "1",
		},
		{
			name: "Delete all objects",
			initialObjs: []metav1.Object{
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", ResourceVersion: "1"}},
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", ResourceVersion: "2"}},
			},
			replacedItems: []interface{}{},
			expectedHistory: []eventRecord{
				{Action: "delete", Key: "default/pod-1", EventRV: "1", StoreRV: ""},
				{Action: "delete", Key: "default/pod-3", EventRV: "2", StoreRV: ""},
			},
			replaceRV: "2",
		},
		{
			name: "Create, update, delete",
			initialObjs: []metav1.Object{
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", ResourceVersion: "1"}},
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Labels: map[string]string{"ver": "1"}, ResourceVersion: "2"}},
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-3", Namespace: "default", ResourceVersion: "3"}},
			},
			replacedItems: []interface{}{
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-1", Namespace: "default", ResourceVersion: "4"}},
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-2", Namespace: "default", Labels: map[string]string{"ver": "2"}, ResourceVersion: "5"}},
				&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod-4", Namespace: "default", ResourceVersion: "6"}},
			},
			expectedHistory: []eventRecord{
				{Action: "delete", Key: "default/pod-3", EventRV: "3", StoreRV: ""},
				{Action: "update", Key: "default/pod-1", EventRV: "4", StoreRV: "4"},
				{Action: "update", Key: "default/pod-2", EventRV: "5", StoreRV: "5"},
				{Action: "add", Key: "default/pod-4", EventRV: "6", StoreRV: "6"},
			},
			replaceRV: "6",
		},
	}

	for _, tc := range tcs {
		testName := tc.name + " atomic: " + strconv.FormatBool(atomic)
		t.Run(testName, func(t *testing.T) {
			// 清空历史，避免计入之前测试的事件。
			m.clearHistory()

			for _, obj := range tc.initialObjs {
				require.NoError(t, fifo.Add(obj), "failed to add")
			}

			// 等待所有创建事件到达。
			require.NoError(t, m.waitForEventCount(ctx, len(tc.initialObjs), 5*time.Second), "Controller failed to receive initial setup all events, got: %v", m.getHistory())

			// 清空历史，只比较 RESYNC/REPLACE 期间的事件。
			m.clearHistory()

			// 执行 Replace。
			require.NoError(t, fifo.Replace(tc.replacedItems, tc.replaceRV), "failed to replace")

			// 等待所有预期事件到达。
			require.NoError(t, m.waitForEventCount(ctx, len(tc.expectedHistory), 5*time.Second), "Controller failed to receive all events, got: %v", m.getHistory())

			items := sortItemsByKey(store.List(), DeletionHandlingMetaNamespaceKeyFunc)
			assert.Equal(t, tc.replacedItems, items)
			// Replace 产生的合成删除事件无序，按 key 排序使测试可稳定断言。
			history := sortDeleteEventsByKey(m.getHistory())
			assert.Equal(t, tc.expectedHistory, history)
		})
	}
}

// TestResetWatch 验证 watch 被服务端重置后，reflector 重新 List 会为已存在
// 对象产生 update、为新对象产生 add，且都不属于初始列表。
func TestResetWatch(t *testing.T) {
	for _, atomic := range []bool{false, true} {
		t.Run("atomic "+strconv.FormatBool(atomic), func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			source := fcache.NewFakeControllerSource()
			pod := &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "pod-1",
					Namespace: "default",
				},
			}
			source.Add(pod)
			t.Cleanup(func() {
				source.Shutdown()
			})

			store := NewStore(DeletionHandlingMetaNamespaceKeyFunc)
			fifoOptions := RealFIFOOptions{
				AtomicEvents: atomic,
			}
			if !atomic {
				fifoOptions.KnownObjects = store
			}
			fifo := NewRealFIFOWithOptions(fifoOptions)
			recorder := newEventRecorder(store, DeletionHandlingMetaNamespaceKeyFunc)

			cfg := &Config{
				Queue:            fifo,
				ListerWatcher:    source,
				ObjectType:       &v1.Pod{},
				FullResyncPeriod: 0,

				Process: func(obj interface{}, isInInitialList bool) error {
					if deltas, ok := obj.(Deltas); ok {
						return processDeltas(fifo.logger, recorder, store, deltas, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
					}
					return errors.New("object given as Process argument is not Deltas")
				},
				ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
					return processDeltasInBatch(fifo.logger, recorder, store, deltaList, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
				},
			}
			c := New(cfg)
			go c.RunWithContext(ctx)
			if !WaitForCacheSync(ctx.Done(), c.HasSynced) {
				t.Fatal("Timed out waiting for cache sync")
			}
			testResetWatch(t, ctx, source, recorder)
		})
	}
}

// testResetWatch 执行重置 watch 的具体断言。
//
// ModifyDropWatch/AddDropWatch 先模拟事件丢失，ResetWatch 再强制一次重新
// List，从而验证增量事件丢失后状态仍能收敛。
func testResetWatch(t *testing.T, ctx context.Context, source *fcache.FakeControllerSource, recorder *eventRecorder) {
	// 首个事件应是初始 Add。
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive initial setup all events, got: %v", recorder.getHistory())
	history := recorder.getHistory()
	assert.Len(t, history, 1)
	assert.Equal(t, "add", history[0].Action)
	assert.Equal(t, "default/pod-1", history[0].Key)
	assert.True(t, history[0].IsInInitialList)

	pod := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
	}
	pod2 := &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-2",
			Namespace: "default",
		},
	}

	recorder.clearHistory()
	source.ModifyDropWatch(pod)
	source.AddDropWatch(pod2)
	source.ResetWatch()

	// Reset 后应收到第二个 Pod 的 Add 与首个 Pod 的 Update。
	require.NoError(t, recorder.waitForEventCount(ctx, 2, 5*time.Second), "Controller failed to receive initial setup all events, got: %v", recorder.getHistory())
	history = recorder.getHistory()
	assert.Len(t, history, 2)

	var addEvent eventRecord
	var updateEvent eventRecord
	for _, event := range history {
		switch event.Action {
		case "update":
			updateEvent = event
		case "add":
			addEvent = event
		}
	}
	assert.Equal(t, "default/pod-1", updateEvent.Key)
	assert.False(t, updateEvent.IsInInitialList)
	assert.Equal(t, "default/pod-2", addEvent.Key)
	assert.False(t, addEvent.IsInInitialList)
}

// TestStoreResourceVersion 验证 Store 会随 update/delete 事件推进
// LastStoreSyncResourceVersion。
func TestStoreResourceVersion(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	source := fcache.NewFakeControllerSource()
	source.Add(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
	})
	t.Cleanup(func() {
		source.Shutdown()
	})

	store := NewStore(DeletionHandlingMetaNamespaceKeyFunc)
	fifo := NewRealFIFOWithOptions(RealFIFOOptions{
		AtomicEvents: true,
	})
	recorder := newEventRecorder(store, DeletionHandlingMetaNamespaceKeyFunc)
	logger, _ := ktesting.NewTestContext(t)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    source,
		ObjectType:       &v1.Pod{},
		FullResyncPeriod: 0,
		Process: func(obj interface{}, isInInitialList bool) error {
			if deltas, ok := obj.(Deltas); ok {
				return processDeltas(logger, recorder, store, deltas, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
		ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
			return processDeltasInBatch(logger, recorder, store, deltaList, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
		},
	}
	c := New(cfg)
	go c.RunWithContext(ctx)
	if !WaitForCacheSync(ctx.Done(), c.HasSynced) {
		t.Fatal("Timed out waiting for cache sync")
	}
	testResourceVersion(t, ctx, source, recorder, store)
}

// testResourceVersion 断言事件携带的 resourceVersion 最终反映到 Store 上。
func testResourceVersion(t *testing.T, ctx context.Context, source *fcache.FakeControllerSource, recorder *eventRecorder, store Store) {
	// 首个事件应是初始 Add。
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive initial setup all events, got: %v", recorder.getHistory())
	history := recorder.getHistory()
	assert.Len(t, history, 1)
	assert.Equal(t, "add", history[0].Action)
	assert.Equal(t, "default/pod-1", history[0].Key)
	assert.True(t, history[0].IsInInitialList)

	recorder.clearHistory()
	// 更新 Pod 标签以提升 resourceVersion。
	source.Modify(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"foo": "bar",
			},
		},
	})
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive update event, got: %v", recorder.getHistory())
	history = recorder.getHistory()
	// The stored resource version should have changed.
	assert.Equal(t, history[0].EventRV, store.LastStoreSyncResourceVersion())

	recorder.clearHistory()
	// Delete the pod.
	source.Delete(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
	})
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive delete event, got: %v", recorder.getHistory())
	history = recorder.getHistory()
	// The stored resource version should have changed.
	assert.Equal(t, history[0].EventRV, store.LastStoreSyncResourceVersion())
}

// TestStoreResourceVersionWithNonMetaTransform 验证 Transform 把对象转换为
// 非元数据类型时，Store 仍能通过 replace 事件推进 resourceVersion，
// 而普通 update 事件因缺少元数据无法推进。
func TestStoreResourceVersionWithNonMetaTransform(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	source := fcache.NewFakeControllerSource()
	source.Add(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
	})
	t.Cleanup(func() {
		source.Shutdown()
	})

	// Create a keyfunc that does not rely on object metadata.
	keyFunc := func(obj interface{}) (string, error) {
		return "pod-1", nil
	}

	store := NewStore(keyFunc)
	fifo := NewRealFIFOWithOptions(RealFIFOOptions{
		Transformer: func(obj interface{}) (interface{}, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return nil, fmt.Errorf("expected *v1.Pod, got %T", obj)
			}
			return pod.Name, nil
		},
		KeyFunction:  keyFunc,
		AtomicEvents: true,
	})
	recorder := newEventRecorder(store, keyFunc)
	logger, _ := ktesting.NewTestContext(t)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    source,
		ObjectType:       &v1.Pod{},
		FullResyncPeriod: 0,

		Process: func(obj interface{}, isInInitialList bool) error {
			if deltas, ok := obj.(Deltas); ok {
				return processDeltas(logger, recorder, store, deltas, isInInitialList, keyFunc)
			}
			return errors.New("object given as Process argument is not Deltas")
		},
		ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
			return processDeltasInBatch(logger, recorder, store, deltaList, isInInitialList, keyFunc)
		},
	}
	c := New(cfg)
	go c.RunWithContext(ctx)
	if !WaitForCacheSync(ctx.Done(), c.HasSynced) {
		t.Fatal("Timed out waiting for cache sync")
	}
	testResourceVersionWithNonMetaTransform(t, ctx, source, recorder, store)
}

// testResourceVersionWithNonMetaTransform 校验非元数据 Transform 下的版本推进规则。
func testResourceVersionWithNonMetaTransform(t *testing.T, ctx context.Context, source *fcache.FakeControllerSource, recorder *eventRecorder, store Store) {
	// 首个事件应是初始 Add。
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive initial setup all events, got: %v", recorder.getHistory())
	history := recorder.getHistory()
	assert.Len(t, history, 1)
	assert.Equal(t, "add", history[0].Action)
	assert.Equal(t, "pod-1", history[0].Key)
	assert.True(t, history[0].IsInInitialList)

	// The stored resource version should stay at the initial value given by the replace event since
	// the update event does not have metadata.
	replaceRV := store.LastStoreSyncResourceVersion()
	recorder.clearHistory()
	// 更新 Pod 标签以提升 resourceVersion。
	source.Modify(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"foo": "bar",
			},
		},
	})
	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive update event, got: %v", recorder.getHistory())
	// The stored resource version should not be changed due to the transform.
	assert.Equal(t, replaceRV, store.LastStoreSyncResourceVersion())

	recorder.clearHistory()
	// Modify the pod but do not send the event.
	source.ModifyDropWatch(&v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
			Labels: map[string]string{
				"foo": "bar2",
			},
		},
	})
	// Trigger a replace by resetting the watch
	source.ResetWatch()

	require.NoError(t, recorder.waitForEventCount(ctx, 1, 5*time.Second), "Controller failed to receive replace event, got: %v", recorder.getHistory())
	// The stored resource version should be updated to the replace events RV.
	assert.Equal(t, "3", store.LastStoreSyncResourceVersion())
}

// eventRecord 是 eventRecorder 为每次回调保存的快照。
//
// StoreRV 记录回调发生时缓存中该对象的 resourceVersion，用于验证
// “先改缓存、再回调”的时序。
type eventRecord struct {
	Action          string
	Key             string
	EventRV         string
	StoreRV         string
	IsInInitialList bool
}

// eventRecorder 是一个线程安全的 ResourceEventHandler，记录事件并提供
// 基于通道的等待接口，避免测试轮询。
type eventRecorder struct {
	historyLock sync.Mutex
	history     []eventRecord
	store       Store
	keyFunc     KeyFunc

	updateCh chan bool
}

// newEventRecorder 创建记录器；updateCh 缓冲 1，通知丢失也不会阻塞记录。
func newEventRecorder(store Store, keyFunc KeyFunc) *eventRecorder {
	return &eventRecorder{
		store:    store,
		updateCh: make(chan bool, 1),
		keyFunc:  keyFunc,
	}
}

func (m *eventRecorder) OnAdd(obj interface{}, isInInitialList bool) {
	m.record("add", obj, isInInitialList)
}
func (m *eventRecorder) OnUpdate(_, obj interface{}) {
	m.record("update", obj, false)
}
func (m *eventRecorder) OnDelete(obj interface{}) {
	m.record("delete", obj, false)
}

// record 保存一条事件快照；完成时以非阻塞方式唤醒等待者。
func (m *eventRecorder) record(action string, obj interface{}, isInInitialList bool) {
	m.historyLock.Lock()
	defer m.historyLock.Unlock()
	key, _ := m.keyFunc(obj)

	if deleted, ok := obj.(DeletedFinalStateUnknown); ok {
		obj = deleted.Obj
	}

	eventRV := ""
	if accessor, err := meta.Accessor(obj); err == nil {
		eventRV = accessor.GetResourceVersion()
	}

	storeRV := ""
	if storeObj, exists, err := m.store.GetByKey(key); err == nil && exists {
		if accessor, err := meta.Accessor(storeObj); err == nil {
			storeRV = accessor.GetResourceVersion()
		}
	}

	m.history = append(m.history, eventRecord{
		Action:          action,
		Key:             key,
		EventRV:         eventRV,
		StoreRV:         storeRV,
		IsInInitialList: isInInitialList,
	})
	select {
	case m.updateCh <- true:
	default:
	}
}

func (m *eventRecorder) clearHistory() {
	m.historyLock.Lock()
	defer m.historyLock.Unlock()
	m.history = []eventRecord{}
}

func (m *eventRecorder) getHistory() []eventRecord {
	m.historyLock.Lock()
	historyCopy := make([]eventRecord, len(m.history))
	copy(historyCopy, m.history)
	m.historyLock.Unlock()

	return historyCopy
}

// waitForEventCount 等待至少 count 条事件，超时或上下文取消时返回错误。
func (m *eventRecorder) waitForEventCount(ctx context.Context, count int, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	for {
		m.historyLock.Lock()
		currentCount := len(m.history)
		m.historyLock.Unlock()

		if currentCount >= count {
			return nil
		}

		select {
		case <-m.updateCh:
			continue
		case <-ctx.Done():
			return fmt.Errorf("context canceled waiting for %d events, currently have %d", count, currentCount)
		}
	}
}

// sortItemsByKey 按 key 排序，消除缓存遍历顺序的不确定性。
func sortItemsByKey(items []interface{}, keyFunc KeyFunc) []interface{} {
	sort.Slice(items, func(i, j int) bool {
		keyI, _ := keyFunc(items[i])
		keyJ, _ := keyFunc(items[j])
		return keyI < keyJ
	})
	return items
}

// sortDeleteEventsByKey 只对删除事件按 key 排序，其他事件保持相对顺序：
// 删除来自集合 diff，本身没有顺序保证；新增/更新则必须忠于输入顺序。
func sortDeleteEventsByKey(events []eventRecord) []eventRecord {
	sort.Slice(events, func(i, j int) bool {
		eventI := events[i]
		eventJ := events[j]
		// sort delete events for different objects by key
		if eventI.Action == "delete" && eventJ.Action == "delete" && eventI.Key != eventJ.Key {
			return eventI.Key < eventJ.Key
		}
		// keep existing order otherwise
		return i < j
	})
	return events
}
