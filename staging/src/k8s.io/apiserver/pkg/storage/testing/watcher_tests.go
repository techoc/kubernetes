/*
Copyright 2016 The Kubernetes Authors.

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

package testing

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
	genericapirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/apiserver/pkg/features"
	"k8s.io/apiserver/pkg/storage"
	"k8s.io/apiserver/pkg/storage/value"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	utilflowcontrol "k8s.io/apiserver/pkg/util/flowcontrol"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/utils/ptr"
)

// RunTestWatch 是存储层 watch 语义的通用一致性测试，分别以非递归/递归两种方式执行。
// 所有 storage.Interface 实现（etcd3、cacher）都应能通过该测试。
func RunTestWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	testWatch(ctx, t, store, false)
	testWatch(ctx, t, store, true)
}

// 它验证以下行为：
// - 对象首次出现时应当通知 Added 事件
// - 更新应当触发 Modified 事件
// - 被过滤条件筛掉的更新应当触发 Deleted 事件
func testWatch(ctx context.Context, t *testing.T, store storage.Interface, recursive bool) {
	basePod := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec:       example.PodSpec{NodeName: ""},
	}
	basePodAssigned := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "foo"},
		Spec:       example.PodSpec{NodeName: "bar"},
	}

	selectedPod := func(pod *example.Pod) *example.Pod {
		result := pod.DeepCopy()
		result.Labels = map[string]string{"select": "true"}
		return result
	}

	tests := []struct {
		name       string
		namespace  string
		key        string
		pred       storage.SelectionPredicate
		watchTests []*testWatchStruct
	}{{
		name:       "create a key",
		namespace:  fmt.Sprintf("test-ns-1-%t", recursive),
		watchTests: []*testWatchStruct{{basePod, true, watch.Added}},
		pred:       storage.Everything,
	}, {
		name:       "key updated to match predicate",
		namespace:  fmt.Sprintf("test-ns-2-%t", recursive),
		watchTests: []*testWatchStruct{{basePod, false, ""}, {basePodAssigned, true, watch.Added}},
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("spec.nodeName=bar"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"spec.nodeName": pod.Spec.NodeName}, nil
			},
		},
	}, {
		name:       "update",
		namespace:  fmt.Sprintf("test-ns-3-%t", recursive),
		watchTests: []*testWatchStruct{{basePod, true, watch.Added}, {basePodAssigned, true, watch.Modified}},
		pred:       storage.Everything,
	}, {
		name:       "delete because of being filtered",
		namespace:  fmt.Sprintf("test-ns-4-%t", recursive),
		watchTests: []*testWatchStruct{{basePod, true, watch.Added}, {basePodAssigned, true, watch.Deleted}},
		pred: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("spec.nodeName!=bar"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"spec.nodeName": pod.Spec.NodeName}, nil
			},
		},
	}, {
		name:      "filtering",
		namespace: fmt.Sprintf("test-ns-5-%t", recursive),
		watchTests: []*testWatchStruct{
			{selectedPod(basePod), true, watch.Added},
			{basePod, true, watch.Deleted},
			{selectedPod(basePod), true, watch.Added},
			{selectedPod(basePodAssigned), true, watch.Modified},
			{nil, true, watch.Deleted},
		},
		pred: storage.SelectionPredicate{
			Label: labels.SelectorFromSet(labels.Set{"select": "true"}),
			Field: fields.Everything(),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return labels.Set(pod.Labels), nil, nil
			},
		},
	}}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			watchKey := fmt.Sprintf("/pods/%s", tt.namespace)
			key := watchKey + "/foo"
			if !recursive {
				watchKey = key
			}

			// 获取当前 RV，从这里开始 watch。
			out := &example.PodList{}
			if err := store.GetList(ctx, watchKey, storage.ListOptions{ResourceVersion: "", Predicate: tt.pred, Recursive: recursive}, out); err != nil {
				t.Fatalf("List failed: %v", err)
			}

			w, err := store.Watch(ctx, watchKey, storage.ListOptions{ResourceVersion: out.ResourceVersion, Predicate: tt.pred, Recursive: recursive})
			if err != nil {
				t.Fatalf("Watch failed: %v", err)
			}

			// 先在不同命名空间创建一个 pod，
			// 确保其对应事件不会被传播过来。
			badKey := fmt.Sprintf("/pods/%s-bad/foo", tt.namespace)
			badOut := &example.Pod{}
			err = store.GuaranteedUpdate(ctx, badKey, badOut, true, nil, storage.SimpleUpdate(
				func(runtime.Object) (runtime.Object, error) {
					obj := basePod.DeepCopy()
					obj.Namespace = fmt.Sprintf("%s-bad", tt.namespace)
					return obj, nil
				}), nil)
			if err != nil {
				t.Fatalf("GuaranteedUpdate of bad pod failed: %v", err)
			}

			var prevObj *example.Pod
			for _, watchTest := range tt.watchTests {
				out := &example.Pod{}
				if watchTest.obj != nil {
					err := store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
						func(runtime.Object) (runtime.Object, error) {
							obj := watchTest.obj.DeepCopy()
							obj.Namespace = tt.namespace
							return obj, nil
						}), nil)
					if err != nil {
						t.Fatalf("GuaranteedUpdate failed: %v", err)
					}
				} else {
					err := store.Delete(ctx, key, out, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{})
					if err != nil {
						t.Fatalf("Delete failed: %v", err)
					}
				}
				if watchTest.expectEvent {
					expectObj := out
					if watchTest.watchType == watch.Deleted {
						expectObj = prevObj
						expectObj.ResourceVersion = out.ResourceVersion
					}
					testCheckResult(t, w, watch.Event{Type: watchTest.watchType, Object: expectObj})
				}
				prevObj = out
			}
			w.Stop()
			testCheckStop(t, w)
		})
	}
}

// RunTestWatchFromZero 验证以下行为：
//   - 从 0 开始 watch 应当完成同步，并拿到此前添加的对象
//   - 用 etcd 测试时，从 0 开始 watch 能够返回那些前一版本已被压缩的对象的事件。
//     用 cacher 测试时，期望 compaction 为 nil。
//
// 该测试同时覆盖“从当前状态重建 watch”与压缩后 410 Gone 的行为。
func RunTestWatchFromZero(ctx context.Context, t *testing.T, store storage.Interface, compaction Compaction) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	testCheckResult(t, w, watch.Event{Type: watch.Added, Object: storedObj})

	// 更新
	out := &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns", Annotations: map[string]string{"a": "1"}}}, nil
		}), nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// 检查我们收到了 Modified watch 事件。该检查同时间接保证了缓存已同步。
	// 这在用 Cacher 测试时很重要：更新传播到 watch cache 可能存在缓慢处理的情况，
	// 这里为此留出了余地。
	testCheckResult(t, w, watch.Event{Type: watch.Modified, Object: out})
	w.Stop()

	// 确保从 0 开始 watch 时收到一个 ADDED 事件
	w, err = store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	testCheckResult(t, w, watch.Event{Type: watch.Added, Object: out})
	w.Stop()

	// 压缩之前的版本
	if compaction == nil {
		t.Skip("compaction callback not provided")
	}

	// 再次更新
	newOut := &example.Pod{}
	err = store.GuaranteedUpdate(ctx, key, newOut, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}, nil
		}), nil)
	if err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// 压缩之前的版本
	compaction(ctx, t, newOut.ResourceVersion)

	// 确保从 0 开始 watch 时仍能收到一个 ADDED 事件
	w, err = store.Watch(ctx, key, storage.ListOptions{ResourceVersion: "0", Predicate: storage.Everything})
	defer w.Stop()
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	testCheckResult(t, w, watch.Event{Type: watch.Added, Object: newOut})

	// 确保从更早的 resource version 开始 watch 已不可能，并得到 "Gone" 错误。
	tooOldWatcher, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: out.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	defer tooOldWatcher.Stop()
	expiredError := errors.NewResourceExpired("").ErrStatus
	// TODO(wojtek-t): etcd 目前似乎会返回另一种错误，
	// 即 Internal error "etcd event received with PrevKv=nil"。
	// 我们临时同时允许两者，但此处应当统一。
	internalError := metav1.Status{
		Status: metav1.StatusFailure,
		Code:   http.StatusInternalServerError,
		Reason: metav1.StatusReasonInternalError,
	}
	testCheckResultFunc(t, tooOldWatcher, func(actualEvent watch.Event) {
		expectNoDiff(t, "incorrect event type", watch.Error, actualEvent.Type)
		if !apiequality.Semantic.DeepDerivative(&expiredError, actualEvent.Object) && !apiequality.Semantic.DeepDerivative(&internalError, actualEvent.Object) {
			t.Errorf("expected: %#v; got %#v", &expiredError, actualEvent.Object)
		}
	})
}

// RunTestDeleteTriggerWatch 验证从对象当前 RV 开始 watch 时，删除会产生 Deleted 事件。
func RunTestDeleteTriggerWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	if err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	testCheckEventType(t, w, watch.Deleted)
}

// RunTestWatchFromNonZero 验证从对象当前 RV 开始 watch 能收到后续 Modified 事件。
func RunTestWatchFromNonZero(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	out := &example.Pod{}
	store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			newObj := storedObj.DeepCopy()
			newObj.Annotations = map[string]string{"version": "2"}
			return newObj, nil
		}), nil)
	testCheckResult(t, w, watch.Event{Type: watch.Modified, Object: out})
}

// RunTestDelayedWatchDelivery 验证消费者处理缓慢时事件不会丢失：
// 缓冲区应至少能容纳实现定义的数量，Stop 后已排队事件仍会送达。
func RunTestDelayedWatchDelivery(ctx context.Context, t *testing.T, store storage.Interface) {
	_, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	startRV := storedObj.ResourceVersion

	watcher, err := store.Watch(ctx, "/pods/test-ns", storage.ListOptions{ResourceVersion: startRV, Predicate: storage.Everything, Recursive: true})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// 取决于具体实现，在阻塞实现（进而迫使 watcher 被关闭，
	// 否则事件就不得不被丢弃）之前，能够产生的、
	// 本应投递给该 watcher 的事件数量各不相同。
	// 就目前而言，Cacher 的该数值最小，为 21。
	totalPods := 21
	for i := 0; i < totalPods; i++ {
		out := &example.Pod{}
		pod := &example.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foo-%d", i), Namespace: "test-ns"},
		}
		err := store.GuaranteedUpdate(ctx, computePodKey(pod), out, true, nil, storage.SimpleUpdate(
			func(runtime.Object) (runtime.Object, error) {
				return pod, nil
			}), nil)
		if err != nil {
			t.Errorf("GuaranteedUpdate failed: %v", err)
		}
	}

	watched := 0
	for {
		event, ok := <-watcher.ResultChan()
		if !ok {
			break
		}
		object := event.Object
		if co, ok := object.(runtime.CacheableObject); ok {
			object = co.GetObject()
		}
		if a, e := object.(*example.Pod).Name, fmt.Sprintf("foo-%d", watched); e != a {
			t.Errorf("Unexpected object watched: %s, expected %s", a, e)
		}
		watched++
		// 在停止 watcher 之前，先等一个事件到达，并给它们一点时间填满队列。
		if watched == 1 {
			time.Sleep(time.Second)
			// 停止 watcher，检查后续事件是否仍会投递。
			watcher.Stop()
		}
	}
	// 取决于实现，我们期望至少投递 N 个事件。
	// 就目前而言，该数值对 Cacher 最小，等于 10（out 缓冲区的大小）。
	outBufferSize := 10
	expectWatched := outBufferSize + 1 // 初始事件
	if watched < expectWatched {
		t.Errorf("Unexpected number of events: %v, expected at least: %v", watched, expectWatched)
	}
}

// RunTestWatchError 通过注入失败的 transformer 触发读取错误，
// 验证 watch 通道会收到 watch.Error 事件而不是静默丢事件。
func RunTestWatchError(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}}
	key := computePodKey(obj)

	// 计算出初始 resource version，后续从它开始 watch。
	list := &example.PodList{}
	storageOpts := storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", storageOpts, list); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if err := store.GuaranteedUpdate(ctx, key, &example.Pod{}, true, nil, storage.SimpleUpdate(
		func(runtime.Object) (runtime.Object, error) {
			return obj, nil
		}), nil); err != nil {
		t.Fatalf("GuaranteedUpdate failed: %v", err)
	}

	// 现在注入一个失败的 transformer 来触发 watch 错误。
	revertTransformer := store.UpdatePrefixTransformer(
		func(previousTransformer *PrefixTransformer) value.Transformer {
			return &failingTransformer{}
		})
	defer revertTransformer()

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: list.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}
	testCheckEventType(t, w, watch.Error)
}

// RunTestWatchWithUnsafeDelete 验证数据损坏时的删除路径：
// 普通删除因解码失败而拒绝，ExpectTransformOrDecodeError 允许绕过并清理坏数据。
func RunTestWatchWithUnsafeDelete(ctx context.Context, t *testing.T, store InterfaceWithTransformerOverride, corruptErr error) {
	obj := &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "foo",
			Namespace: "test-ns",
		},
	}
	key := computePodKey(obj)

	out := &example.Pod{}
	if err := store.Create(ctx, key, obj, out, 0); err != nil {
		t.Fatalf("failed to create object in the store: %v", err)
	}

	// 计算出初始 resource version，后续从它开始 watch。
	list := &example.PodList{}
	storageOpts := storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
		Recursive:       true,
	}
	if err := store.GetList(ctx, "/pods/", storageOpts, list); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// 现在通过让对象 transform 失败来触发 watch 错误。
	revertTransformer := store.UpdateTransformer(newErrInjectingModifier(func(string) error {
		return corruptErr
	}))
	defer revertTransformer()

	w, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: list.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// 普通删除应当失败
	if err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err == nil {
		t.Fatalf("Expected normal Delete to fail")
	}
	if err := store.Delete(ctx, key, &example.Pod{}, nil, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{ExpectTransformOrDecodeError: true}); err != nil {
		t.Fatalf("Expected unsafe Delete to succeed, but got: %v", err)
	}

	testCheckResultFunc(t, w, func(got watch.Event) {
		if want, got := watch.Error, got.Type; want != got {
			t.Errorf("Expected event type: %q, but got: %q", want, got)
		}
		switch v := got.Object.(type) {
		case *metav1.Status:
			if want, got := metav1.StatusReasonStoreReadError, v.Reason; want != got {
				t.Errorf("Expected reason: %q, but got: %q", want, got)
			}
			if want := "saw a DELETED event, but object data is corrupt"; !strings.Contains(v.Message, want) {
				t.Errorf("Expected Message to contain: %q, but got: %q", want, v.Message)
			}
		default:
			t.Errorf("expected an metav1 Status object, but got: %v", got.Object)
		}
	})
}

// RunTestWatchContextCancel 验证已取消的上下文会让 watch 通道立即关闭，而不是报错。
func RunTestWatchContextCancel(ctx context.Context, t *testing.T, store storage.Interface) {
	canceledCtx, cancel := context.WithCancel(ctx)
	cancel()
	// 当我们用已取消的 context 去 watch 时，应当检测到 context canceled。
	// 我们不把它当作错误，同时会关闭 watcher。
	w, err := store.Watch(canceledCtx, "/pods/not-existing", storage.ListOptions{
		ResourceVersion: "0",
		Predicate:       storage.Everything,
	})
	if err != nil {
		t.Fatal(err)
	}

	select {
	case _, ok := <-w.ResultChan():
		if ok {
			t.Error("ResultChan() should be closed")
		}
	case <-time.After(wait.ForeverTestTimeout):
		t.Errorf("timeout after %v", wait.ForeverTestTimeout)
	}
}

// RunTestWatcherTimeout 验证不读取结果的 watcher 不会无限阻塞其他 watcher 或写入。
func RunTestWatcherTimeout(ctx context.Context, t *testing.T, store storage.Interface) {
	// initialRV 用于在世界之初（从头）初始化 watcher。
	podList := example.PodList{}
	options := storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}
	if err := store.GetList(ctx, "/pods/", options, &podList); err != nil {
		t.Fatalf("Failed to list pods: %v", err)
	}
	initialRV := podList.ResourceVersion

	options = storage.ListOptions{
		ResourceVersion: initialRV,
		Predicate:       storage.Everything,
		Recursive:       true,
	}

	// 创建一批从不读取结果的 watcher。
	nonReadingWatchers := 50
	for i := 0; i < nonReadingWatchers; i++ {
		watcher, err := store.Watch(ctx, "/pods/test-ns", options)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer watcher.Stop()
	}

	// 再创建一个会读取结果的 watcher。
	readingWatcher, err := store.Watch(ctx, "/pods/test-ns", options)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer readingWatcher.Stop()

	// 取决于具体实现，在阻塞实现（进而迫使 watcher 被关闭，
	// 否则事件就不得不被丢弃）之前，能够产生的、
	// 本应投递给该 watcher 的事件数量各不相同。
	// 就目前而言，Cacher 的该数值最小，为 21。
	//
	// 制造更多事件，以确保我们不会永远阻塞其他 watcher。
	startTime := time.Now()
	for i := 0; i < 22; i++ {
		out := &example.Pod{}
		pod := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("foo-%d", i), Namespace: "test-ns"}}
		if err := store.Create(ctx, computePodKey(pod), pod, out, 0); err != nil {
			t.Fatalf("Create failed: %v", err)
		}
		testCheckResult(t, readingWatcher, watch.Event{Type: watch.Added, Object: out})
	}
	if time.Since(startTime) > time.Duration(250*nonReadingWatchers)*time.Millisecond {
		t.Errorf("waiting for events took too long: %v", time.Since(startTime))
	}
}

// RunTestWatchDeleteEventObjectHaveLatestRV 验证删除事件的 RV 已更新，
// 客户端因此可以安全地以该 RV 继续 watch。
func RunTestWatchDeleteEventObjectHaveLatestRV(ctx context.Context, t *testing.T, store storage.Interface) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})

	watchCtx, cancel := context.WithTimeout(ctx, wait.ForeverTestTimeout)
	t.Cleanup(cancel)
	w, err := store.Watch(watchCtx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	deletedObj := &example.Pod{}
	if err := store.Delete(ctx, key, deletedObj, &storage.Preconditions{}, storage.ValidateAllObjectFunc, nil, storage.DeleteOptions{}); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 验证删除时 ResourceVersion 发生了变化。
	if storedObj.ResourceVersion == deletedObj.ResourceVersion {
		t.Fatalf("ResourceVersion didn't changed on deletion: %s", deletedObj.ResourceVersion)
	}

	testCheckResult(t, w, watch.Event{Type: watch.Deleted, Object: deletedObj})
}

// RunTestWatchInitializationSignal 验证 watch 建立后会触发初始化信号，
// 供优先级与公平性（APF）在服务恢复时解除请求阻塞。
func RunTestWatchInitializationSignal(ctx context.Context, t *testing.T, store storage.Interface) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	t.Cleanup(cancel)
	initSignal := utilflowcontrol.NewInitializationSignal()
	ctx = utilflowcontrol.WithInitializationSignal(ctx, initSignal)

	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	_, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: storedObj.ResourceVersion, Predicate: storage.Everything})
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	initSignal.Wait()
}

// RunOptionalTestProgressNotify 测试 ListOptions 的 ProgressNotify 特性。
// 鉴于该特性目前并未被 Kubernetes 上层显式使用（更多是由 storage.Interface
// 的包装层用它来实现自身功能），该测试目前被视为可选项。
func RunOptionalTestProgressNotify(ctx context.Context, t *testing.T, store storage.Interface, increaseRV IncreaseRVFunc) {
	input := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "name", Namespace: "test-ns"}}
	key := computePodKey(input)
	out := &example.Pod{}
	if err := store.Create(ctx, key, input, out, 0); err != nil {
		t.Fatalf("Create failed: %v", err)
	}
	validateResourceVersion := resourceVersionNotOlderThan(out.ResourceVersion)
	// 自 etcd v3.6.2 起，由于 https://github.com/etcd-io/etcd/pull/20241，我们需要递增 RV。
	// 必须推进 resource version，确保 etcd revision 越过我们建立的 watch 所在位置，
	// 因为 etcd 不会为指向未来 revision 的 watch 发送进度通知。
	//
	// Kubernetes 的 watch 是排他的（收到的首个事件在给定 RV 之后），这对应于
	// etcd 上 revision+1 处的包含式 watch。若不做这个递增，在没有其他写入时，
	// watch 将指向未来的 revision，导致进度通知无法送达。
	increaseRV(ctx, t)

	opts := storage.ListOptions{
		ResourceVersion: out.ResourceVersion,
		Predicate:       storage.Everything,
		ProgressNotify:  true,
	}
	w, err := store.Watch(ctx, key, opts)
	if err != nil {
		t.Fatalf("Watch failed: %v", err)
	}

	// 当我们发送 bookmark 事件时，客户端期望事件携带正确类型的对象，
	// 但除 resourceVersion 外不设置任何字段
	testCheckResultFunc(t, w, func(actualEvent watch.Event) {
		expectNoDiff(t, "incorrect event type", watch.Bookmark, actualEvent.Type)
		// 首先，检查 resource version 是否正确
		obj, ok := actualEvent.Object.(metav1.Object)
		if !ok {
			t.Fatalf("got %T, not metav1.Object", actualEvent.Object)
		}
		if err := validateResourceVersion(obj.GetResourceVersion()); err != nil {
			t.Fatal(err)
		}

		// 然后，检查类型与内容是否正确
		pod, ok := actualEvent.Object.(*example.Pod)
		if !ok {
			t.Fatalf("got %T, not *example.Pod", actualEvent.Object)
		}
		pod.ResourceVersion = ""
		expectNoDiff(t, "bookmark event should contain an object with no fields set other than resourceVersion", &example.Pod{}, pod)
	})
}

// RunTestClusterScopedWatch 测试集群级（cluster-scoped）资源的 watch。
// 覆盖按名称、field selector、以及 deletecollection 等场景下的事件分发。
func RunTestClusterScopedWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	tests := []struct {
		name string
		// 对 watch 请求而言，对象名称通过 field selector
		// "metadata.name=objectName" 指定。因此在本 watch 测试中，
		// 应当同时设置 requestedName 和 field selector
		// "metadata.name=requestedName"，或者两者都不设置。
		requestedName string
		recursive     bool
		fieldSelector fields.Selector
		indexFields   []string
		watchTests    []*testWatchStruct
	}{
		{
			name:          "cluster-wide watch, request without name, without field selector",
			recursive:     true,
			fieldSelector: fields.Everything(),
			watchTests: []*testWatchStruct{
				{basePod("t1-foo1"), true, watch.Added},
				{basePodUpdated("t1-foo1"), true, watch.Modified},
				{basePodAssigned("t1-foo2", "t1-bar1"), true, watch.Added},
			},
		},
		{
			name:          "cluster-wide watch, request without name, field selector with spec.nodeName",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName=t2-bar1"),
			indexFields:   []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{basePod("t2-foo1"), false, ""},
				{basePodAssigned("t2-foo1", "t2-bar1"), true, watch.Added},
			},
		},
		{
			name:          "cluster-wide watch, request without name, field selector with spec.nodeName to filter out watch",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName!=t3-bar1"),
			indexFields:   []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{basePod("t3-foo1"), true, watch.Added},
				{basePod("t3-foo2"), true, watch.Added},
				{basePodUpdated("t3-foo1"), true, watch.Modified},
				{basePodAssigned("t3-foo1", "t3-bar1"), true, watch.Deleted},
			},
		},
		{
			name:          "cluster-wide watch, request with name, field selector with metadata.name",
			requestedName: "t4-foo1",
			fieldSelector: fields.ParseSelectorOrDie("metadata.name=t4-foo1"),
			watchTests: []*testWatchStruct{
				{basePod("t4-foo1"), true, watch.Added},
				{basePod("t4-foo2"), false, ""},
				{basePodUpdated("t4-foo1"), true, watch.Modified},
				{basePodUpdated("t4-foo2"), false, ""},
			},
		},
		{
			name:          "cluster-wide watch, request with name, field selector with metadata.name and spec.nodeName",
			requestedName: "t5-foo1",
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name": "t5-foo1",
				"spec.nodeName": "t5-bar1",
			}),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{basePod("t5-foo1"), false, ""},
				{basePod("t5-foo2"), false, ""},
				{basePodUpdated("t5-foo1"), false, ""},
				{basePodUpdated("t5-foo2"), false, ""},
				{basePodAssigned("t5-foo1", "t5-bar1"), true, watch.Added},
			},
		},
		{
			name:          "cluster-wide watch, request with name, field selector with metadata.name, and with spec.nodeName to filter out watch",
			requestedName: "t6-foo1",
			fieldSelector: fields.AndSelectors(
				fields.ParseSelectorOrDie("spec.nodeName!=t6-bar1"),
				fields.SelectorFromSet(fields.Set{"metadata.name": "t6-foo1"}),
			),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{basePod("t6-foo1"), true, watch.Added},
				{basePod("t6-foo2"), false, ""},
				{basePodUpdated("t6-foo1"), true, watch.Modified},
				{basePodAssigned("t6-foo1", "t6-bar1"), true, watch.Deleted},
				{basePodAssigned("t6-foo2", "t6-bar1"), false, ""},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestInfo := &genericapirequest.RequestInfo{}
			requestInfo.Name = tt.requestedName
			requestInfo.Namespace = ""
			ctx = genericapirequest.WithRequestInfo(ctx, requestInfo)
			ctx = genericapirequest.WithNamespace(ctx, "")

			watchKey := "/pods/"
			if tt.requestedName != "" {
				watchKey += tt.requestedName
			}

			predicate := CreatePodPredicate(tt.fieldSelector, false, tt.indexFields)

			list := &example.PodList{}
			opts := storage.ListOptions{
				ResourceVersion: "",
				Predicate:       predicate,
				Recursive:       true,
			}
			if err := store.GetList(ctx, "/pods/", opts, list); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			opts.ResourceVersion = list.ResourceVersion
			opts.Recursive = tt.recursive

			w, err := store.Watch(ctx, watchKey, opts)
			if err != nil {
				t.Fatalf("Watch failed: %v", err)
			}

			currentObjs := map[string]*example.Pod{}
			for _, watchTest := range tt.watchTests {
				out := &example.Pod{}
				key := "/pods/" + watchTest.obj.Name
				err := store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
					func(runtime.Object) (runtime.Object, error) {
						obj := watchTest.obj.DeepCopy()
						return obj, nil
					}), nil)
				if err != nil {
					t.Fatalf("GuaranteedUpdate failed: %v", err)
				}

				expectObj := out
				if watchTest.watchType == watch.Deleted {
					expectObj = currentObjs[watchTest.obj.Name]
					expectObj.ResourceVersion = out.ResourceVersion
					delete(currentObjs, watchTest.obj.Name)
				} else {
					currentObjs[watchTest.obj.Name] = out
				}
				if watchTest.expectEvent {
					testCheckResult(t, w, watch.Event{Type: watchTest.watchType, Object: expectObj})
				}
			}
			w.Stop()
			testCheckStop(t, w)
		})
	}
}

// RunTestNamespaceScopedWatch 测试命名空间级（namespace-scoped）资源的 watch。
// 重点验证 /pods/ 与 /pods/ns 前缀的递归 watch 不会串到其他命名空间。
func RunTestNamespaceScopedWatch(ctx context.Context, t *testing.T, store storage.Interface) {
	tests := []struct {
		name string
		// 对 watch 请求而言，对象名称通过 field selector
		// "metadata.name=objectName" 指定。因此在本 watch 测试中，
		// 应当同时设置 requestedName 和 field selector
		// "metadata.name=requestedName"，或者两者都不设置。
		requestedName      string
		requestedNamespace string
		recursive          bool
		fieldSelector      fields.Selector
		indexFields        []string
		watchTests         []*testWatchStruct
	}{
		{
			name:          "namespaced watch, request without name, request without namespace, without field selector",
			recursive:     true,
			fieldSelector: fields.Everything(),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t1-foo1", "t1-ns1"), true, watch.Added},
				{baseNamespacedPod("t1-foo2", "t1-ns2"), true, watch.Added},
				{baseNamespacedPodUpdated("t1-foo1", "t1-ns1"), true, watch.Modified},
				{baseNamespacedPodUpdated("t1-foo2", "t1-ns2"), true, watch.Modified},
			},
		},
		{
			name:          "namespaced watch, request without name, request without namespace, field selector with metadata.namespace",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("metadata.namespace=t2-ns1"),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t2-foo1", "t2-ns1"), true, watch.Added},
				{baseNamespacedPod("t2-foo1", "t2-ns2"), false, ""},
				{baseNamespacedPodUpdated("t2-foo1", "t2-ns1"), true, watch.Modified},
				{baseNamespacedPodUpdated("t2-foo1", "t2-ns2"), false, ""},
			},
		},
		{
			name:          "namespaced watch, request without name, request without namespace, field selector with spec.nodename",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName=t3-bar1"),
			indexFields:   []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t3-foo1", "t3-ns1"), false, ""},
				{baseNamespacedPod("t3-foo2", "t3-ns2"), false, ""},
				{baseNamespacedPodAssigned("t3-foo1", "t3-ns1", "t3-bar1"), true, watch.Added},
				{baseNamespacedPodAssigned("t3-foo2", "t3-ns2", "t3-bar1"), true, watch.Added},
			},
		},
		{
			name:          "namespaced watch, request without name, request without namespace, field selector with spec.nodename to filter out watch",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("spec.nodeName!=t4-bar1"),
			indexFields:   []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t4-foo1", "t4-ns1"), true, watch.Added},
				{baseNamespacedPod("t4-foo2", "t4-ns1"), true, watch.Added},
				{baseNamespacedPodUpdated("t4-foo1", "t4-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t4-foo1", "t4-ns1", "t4-bar1"), true, watch.Deleted},
			},
		},
		{
			name:               "namespaced watch, request without name, request with namespace, without field selector",
			requestedNamespace: "t5-ns1",
			recursive:          true,
			fieldSelector:      fields.Everything(),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t5-foo1", "t5-ns1"), true, watch.Added},
				{baseNamespacedPod("t5-foo1", "t5-ns2"), false, ""},
				{baseNamespacedPod("t5-foo2", "t5-ns1"), true, watch.Added},
				{baseNamespacedPodUpdated("t5-foo1", "t5-ns1"), true, watch.Modified},
				{baseNamespacedPodUpdated("t5-foo1", "t5-ns2"), false, ""},
			},
		},
		{
			name:               "namespaced watch, request without name, request with namespace, field selector with matched metadata.namespace",
			requestedNamespace: "t6-ns1",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("metadata.namespace=t6-ns1"),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t6-foo1", "t6-ns1"), true, watch.Added},
				{baseNamespacedPod("t6-foo1", "t6-ns2"), false, ""},
				{baseNamespacedPodUpdated("t6-foo1", "t6-ns1"), true, watch.Modified},
			},
		},
		{
			name:               "namespaced watch, request without name, request with namespace, field selector with non-matched metadata.namespace",
			requestedNamespace: "t7-ns1",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("metadata.namespace=t7-ns2"),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t7-foo1", "t7-ns1"), false, ""},
				{baseNamespacedPod("t7-foo1", "t7-ns2"), false, ""},
				{baseNamespacedPodUpdated("t7-foo1", "t7-ns1"), false, ""},
				{baseNamespacedPodUpdated("t7-foo1", "t7-ns2"), false, ""},
			},
		},
		{
			name:               "namespaced watch, request without name, request with namespace, field selector with spec.nodename",
			requestedNamespace: "t8-ns1",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("spec.nodeName=t8-bar2"),
			indexFields:        []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t8-foo1", "t8-ns1"), false, ""},
				{baseNamespacedPodAssigned("t8-foo1", "t8-ns1", "t8-bar1"), false, ""},
				{baseNamespacedPodAssigned("t8-foo1", "t8-ns2", "t8-bar2"), false, ""},
				{baseNamespacedPodAssigned("t8-foo1", "t8-ns1", "t8-bar2"), true, watch.Added},
			},
		},
		{
			name:               "namespaced watch, request without name, request with namespace, field selector with spec.nodename to filter out watch",
			requestedNamespace: "t9-ns2",
			recursive:          true,
			fieldSelector:      fields.ParseSelectorOrDie("spec.nodeName!=t9-bar1"),
			indexFields:        []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t9-foo1", "t9-ns1"), false, ""},
				{baseNamespacedPod("t9-foo1", "t9-ns2"), true, watch.Added},
				{baseNamespacedPodAssigned("t9-foo1", "t9-ns2", "t9-bar1"), true, watch.Deleted},
				{baseNamespacedPodAssigned("t9-foo1", "t9-ns2", "t9-bar2"), true, watch.Added},
			},
		},
		{
			name:          "namespaced watch, request with name, request without namespace, field selector with metadata.name",
			requestedName: "t10-foo1",
			recursive:     true,
			fieldSelector: fields.ParseSelectorOrDie("metadata.name=t10-foo1"),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t10-foo1", "t10-ns1"), true, watch.Added},
				{baseNamespacedPod("t10-foo1", "t10-ns2"), true, watch.Added},
				{baseNamespacedPod("t10-foo2", "t10-ns1"), false, ""},
				{baseNamespacedPodUpdated("t10-foo1", "t10-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t10-foo1", "t10-ns1", "t10-bar1"), true, watch.Modified},
			},
		},
		{
			name:          "namespaced watch, request with name, request without namespace, field selector with metadata.name and metadata.namespace",
			requestedName: "t11-foo1",
			recursive:     true,
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "t11-foo1",
				"metadata.namespace": "t11-ns1",
			}),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t11-foo1", "t11-ns1"), true, watch.Added},
				{baseNamespacedPod("t11-foo2", "t11-ns1"), false, ""},
				{baseNamespacedPod("t11-foo1", "t11-ns2"), false, ""},
				{baseNamespacedPodUpdated("t11-foo1", "t11-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t11-foo1", "t11-ns1", "t11-bar1"), true, watch.Modified},
			},
		},
		{
			name:          "namespaced watch, request with name, request without namespace, field selector with metadata.name and spec.nodeName",
			requestedName: "t12-foo1",
			recursive:     true,
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name": "t12-foo1",
				"spec.nodeName": "t12-bar1",
			}),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t12-foo1", "t12-ns1"), false, ""},
				{baseNamespacedPodUpdated("t12-foo1", "t12-ns1"), false, ""},
				{baseNamespacedPodAssigned("t12-foo1", "t12-ns1", "t12-bar1"), true, watch.Added},
			},
		},
		{
			name:          "namespaced watch, request with name, request without namespace, field selector with metadata.name, and with spec.nodeName to filter out watch",
			requestedName: "t15-foo1",
			recursive:     true,
			fieldSelector: fields.AndSelectors(
				fields.ParseSelectorOrDie("spec.nodeName!=t15-bar1"),
				fields.SelectorFromSet(fields.Set{"metadata.name": "t15-foo1"}),
			),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t15-foo1", "t15-ns1"), true, watch.Added},
				{baseNamespacedPod("t15-foo2", "t15-ns1"), false, ""},
				{baseNamespacedPodUpdated("t15-foo1", "t15-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t15-foo1", "t15-ns1", "t15-bar1"), true, watch.Deleted},
				{baseNamespacedPodAssigned("t15-foo1", "t15-ns1", "t15-bar2"), true, watch.Added},
			},
		},
		{
			name:               "namespaced watch, request with name, request with namespace, with field selector metadata.name",
			requestedName:      "t16-foo1",
			requestedNamespace: "t16-ns1",
			fieldSelector:      fields.ParseSelectorOrDie("metadata.name=t16-foo1"),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t16-foo1", "t16-ns1"), true, watch.Added},
				{baseNamespacedPod("t16-foo2", "t16-ns1"), false, ""},
				{baseNamespacedPodUpdated("t16-foo1", "t16-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t16-foo1", "t16-ns1", "t16-bar1"), true, watch.Modified},
			},
		},
		{
			name:               "namespaced watch, request with name, request with namespace, with field selector metadata.name and metadata.namespace",
			requestedName:      "t17-foo2",
			requestedNamespace: "t17-ns1",
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "t17-foo2",
				"metadata.namespace": "t17-ns1",
			}),
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t17-foo1", "t17-ns1"), false, ""},
				{baseNamespacedPod("t17-foo2", "t17-ns1"), true, watch.Added},
				{baseNamespacedPodUpdated("t17-foo1", "t17-ns1"), false, ""},
				{baseNamespacedPodAssigned("t17-foo2", "t17-ns1", "t17-bar1"), true, watch.Modified},
			},
		},
		{
			name:               "namespaced watch, request with name, request with namespace, with field selector metadata.name, metadata.namespace and spec.nodename",
			requestedName:      "t18-foo1",
			requestedNamespace: "t18-ns1",
			fieldSelector: fields.SelectorFromSet(fields.Set{
				"metadata.name":      "t18-foo1",
				"metadata.namespace": "t18-ns1",
				"spec.nodeName":      "t18-bar1",
			}),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t18-foo1", "t18-ns1"), false, ""},
				{baseNamespacedPod("t18-foo2", "t18-ns1"), false, ""},
				{baseNamespacedPod("t18-foo1", "t18-ns2"), false, ""},
				{baseNamespacedPodUpdated("t18-foo1", "t18-ns1"), false, ""},
				{baseNamespacedPodAssigned("t18-foo1", "t18-ns1", "t18-bar1"), true, watch.Added},
			},
		},
		{
			name:               "namespaced watch, request with name, request with namespace, with field selector metadata.name, metadata.namespace, and with spec.nodename to filter out watch",
			requestedName:      "t19-foo2",
			requestedNamespace: "t19-ns1",
			fieldSelector: fields.AndSelectors(
				fields.ParseSelectorOrDie("spec.nodeName!=t19-bar1"),
				fields.SelectorFromSet(fields.Set{"metadata.name": "t19-foo2", "metadata.namespace": "t19-ns1"}),
			),
			indexFields: []string{"spec.nodeName"},
			watchTests: []*testWatchStruct{
				{baseNamespacedPod("t19-foo1", "t19-ns1"), false, ""},
				{baseNamespacedPod("t19-foo2", "t19-ns2"), false, ""},
				{baseNamespacedPod("t19-foo2", "t19-ns1"), true, watch.Added},
				{baseNamespacedPodUpdated("t19-foo2", "t19-ns1"), true, watch.Modified},
				{baseNamespacedPodAssigned("t19-foo2", "t19-ns1", "t19-bar1"), true, watch.Deleted},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			requestInfo := &genericapirequest.RequestInfo{}
			requestInfo.Name = tt.requestedName
			requestInfo.Namespace = tt.requestedNamespace
			ctx = genericapirequest.WithRequestInfo(ctx, requestInfo)
			ctx = genericapirequest.WithNamespace(ctx, tt.requestedNamespace)

			watchKey := "/pods/"
			if tt.requestedNamespace != "" {
				watchKey += tt.requestedNamespace + "/"
				if tt.requestedName != "" {
					watchKey += tt.requestedName
				}
			}

			predicate := CreatePodPredicate(tt.fieldSelector, true, tt.indexFields)

			list := &example.PodList{}
			opts := storage.ListOptions{
				ResourceVersion: "",
				Predicate:       predicate,
				Recursive:       true,
			}
			if err := store.GetList(ctx, "/pods/", opts, list); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}

			opts.ResourceVersion = list.ResourceVersion
			opts.Recursive = tt.recursive

			w, err := store.Watch(ctx, watchKey, opts)
			if err != nil {
				t.Fatalf("Watch failed: %v", err)
			}

			currentObjs := map[string]*example.Pod{}
			for _, watchTest := range tt.watchTests {
				out := &example.Pod{}
				key := "/pods/" + watchTest.obj.Namespace + "/" + watchTest.obj.Name
				err := store.GuaranteedUpdate(ctx, key, out, true, nil, storage.SimpleUpdate(
					func(runtime.Object) (runtime.Object, error) {
						obj := watchTest.obj.DeepCopy()
						return obj, nil
					}), nil)
				if err != nil {
					t.Fatalf("GuaranteedUpdate failed: %v", err)
				}

				expectObj := out
				podIdentifier := watchTest.obj.Namespace + "/" + watchTest.obj.Name
				if watchTest.watchType == watch.Deleted {
					expectObj = currentObjs[podIdentifier]
					expectObj.ResourceVersion = out.ResourceVersion
					delete(currentObjs, podIdentifier)
				} else {
					currentObjs[podIdentifier] = out
				}
				if watchTest.expectEvent {
					testCheckResult(t, w, watch.Event{Type: watchTest.watchType, Object: expectObj})
				}
			}
			w.Stop()
			testCheckStop(t, w)
		})
	}
}

// RunOptionalTestWatchDispatchBookmarkEvents 测试是否发送 bookmark 事件。
// 该特性目前实现在 watch cache 层，因此本测试是可选的。
//
// TODO(#109831): ProgressNotify 特性实际上实现了相同的功能，
//
//	因此我们应当重构该功能，让两者共用同一输入。
//
// RunTestWatchDispatchBookmarkEvents 验证 ListOptions.SendInitialEvents 下
// 是否按实现能力分发 Bookmark 事件（expectedWatchBookmarks 由实现声明）。
func RunTestWatchDispatchBookmarkEvents(ctx context.Context, t *testing.T, store storage.Interface, expectedWatchBookmarks bool) {
	key, storedObj := testPropagateStore(ctx, t, store, &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: "test-ns"}})
	startRV := storedObj.ResourceVersion

	tests := []struct {
		name                string
		timeout             time.Duration
		expected            bool
		allowWatchBookmarks bool
	}{
		{ // 测试旧客户端不会收到 Bookmark 事件
			name:                "allowWatchBookmarks=false",
			timeout:             3 * time.Second,
			expected:            false,
			allowWatchBookmarks: false,
		},
		{
			name:                "allowWatchBookmarks=true",
			timeout:             3 * time.Second,
			expected:            expectedWatchBookmarks,
			allowWatchBookmarks: true,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := storage.Everything
			pred.AllowWatchBookmarks = tt.allowWatchBookmarks
			ctx, cancel := context.WithTimeout(context.Background(), tt.timeout)
			defer cancel()

			watcher, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: startRV, Predicate: pred})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer watcher.Stop()

			// 在不同命名空间创建 pod 事件
			out := &example.Pod{}
			obj := &example.Pod{ObjectMeta: metav1.ObjectMeta{Name: "foo", Namespace: fmt.Sprintf("other-ns-%d", i)}}
			objKey := computePodKey(obj)

			if err := store.Create(ctx, objKey, obj, out, 0); err != nil {
				t.Fatalf("Create failed: %v", err)
			}

			// 现在等待 Bookmark 事件
			select {
			case event, ok := <-watcher.ResultChan():
				if !ok && tt.expected {
					t.Errorf("Unexpected object watched (no objects)")
				}
				if tt.expected && event.Type != watch.Bookmark {
					t.Errorf("Unexpected object watched %#v", event)
				}
			case <-time.After(time.Second * 3):
				if tt.expected {
					t.Errorf("Unexpected object watched (timeout)")
				}
			}
		})
	}
}

// RunOptionalTestWatchBookmarksWithCorrectResourceVersion 测试 bookmark 事件是否
// 携带正确的 resource version 发送。
// 该特性目前实现在 watch cache 层，因此本测试是可选的。
//
// TODO(#109831): ProgressNotify 特性实际上实现了相同的功能，
//
//	因此我们应当重构该功能，让两者共用同一输入。
//
// RunTestOptionalWatchBookmarksWithCorrectResourceVersion 验证 bookmark 事件的 RV
// 不小于此前已分发事件，保证客户端用其续 watch 时不会回退。
func RunTestOptionalWatchBookmarksWithCorrectResourceVersion(ctx context.Context, t *testing.T, store storage.Interface) {
	// 计算初始 resource version。
	list := &example.PodList{}
	storageOpts := storage.ListOptions{
		Predicate: storage.Everything,
		Recursive: true,
	}
	if err := store.GetList(ctx, "/pods/", storageOpts, list); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	startRV := list.ResourceVersion

	key := "/pods/test-ns"
	pred := storage.Everything
	pred.AllowWatchBookmarks = true

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	watcher, err := store.Watch(ctx, key, storage.ListOptions{ResourceVersion: startRV, Predicate: pred, Recursive: true})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer watcher.Stop()

	done := make(chan struct{})
	errc := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Add(1)
	// 必须先等待 waitgroup 退出，之后才能在先前注册的 defer 中终止 cache 或 server。
	defer wg.Wait()
	// 先调用 close，让 goroutine 知道应当退出。
	defer close(done)

	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			select {
			case <-done:
				return
			default:
				out := &example.Pod{}
				pod := &example.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:      fmt.Sprintf("foo-%d", i),
						Namespace: "test-ns",
					},
				}
				podKey := computePodKey(pod)
				if err := store.Create(ctx, podKey, pod, out, 0); err != nil {
					errc <- fmt.Errorf("failed to create pod %v: %v", pod, err)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	bookmarkReceived := false
	lastObservedResourceVersion := uint64(0)

	for {
		select {
		case err := <-errc:
			t.Fatal(err)
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// 确保我们已经收到过一个 bookmark 事件
				if !bookmarkReceived {
					t.Fatalf("Unpexected error, we did not received a bookmark event")
				}
				return
			}
			rv, err := storage.APIObjectVersioner{}.ObjectResourceVersion(event.Object)
			if err != nil {
				t.Fatalf("failed to parse resourceVersion from %#v", event)
			}
			if event.Type == watch.Bookmark {
				bookmarkReceived = true
				// bookmark 事件的 RV 必须大于或等于此前观察到的 RV
				if rv < lastObservedResourceVersion {
					t.Fatalf("Unexpected bookmark resourceVersion %v less than observed %v)", rv, lastObservedResourceVersion)
				}
			} else {
				// 非 bookmark 事件的 RV 必须大于此前所有的 RV
				if rv <= lastObservedResourceVersion {
					t.Fatalf("Unexpected event resourceVersion %v less than or equal to bookmark %v)", rv, lastObservedResourceVersion)
				}
			}
			lastObservedResourceVersion = rv
		}
	}
}

// RunSendInitialEventsBackwardCompatibility 测试各实现设置了 SendInitialEvents 选项时的
// 向后兼容性。向后兼容性定义为 RV = "" 或 RV = "0"，且 AllowWatchBookmarks 设为 false；
// 此时期望 watch 请求能够正常建立。
// 该用例同时验证旧客户端不设置 SendInitialEvents 时，行为保持与以往一致
// （不会突然收到大量初始事件）。
func RunSendInitialEventsBackwardCompatibility(ctx context.Context, t *testing.T, store storage.Interface) {
	opts := storage.ListOptions{Predicate: storage.Everything}
	opts.SendInitialEvents = ptr.To(true)
	w, err := store.Watch(ctx, "/pods/", opts)
	require.NoError(t, err)
	w.Stop()
}

// RunWatchSemantics 测试以下组合：
//
// +-----------------+---------------------+-------------------+
// | ResourceVersion | AllowWatchBookmarks | SendInitialEvents |
// +=================+=====================+===================+
// | Unset           | true/false          | true/false        |
// | 0               | true/false          | true/false        |
// | 1               | true/false          | true/false        |
// | Current         | true/false          | true/false        |
// +-----------------+---------------------+-------------------+
// 其中：
// - false 表示测试用例把该参数设为 "false"
// - true 表示测试用例把该参数设为 "true"
// RunWatchSemantics 覆盖 watch 的核心语义：同名对象多次更新、删除、
// 以及带 SendInitialEvents 时的 Added/Bookmark 顺序。
func RunWatchSemantics(ctx context.Context, t *testing.T, store storage.Interface) {
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)
	trueVal, falseVal := true, false
	addEventsFromCreatedPods := func(createdInitialPods []*example.Pod) []watch.Event {
		var ret []watch.Event
		for _, createdPod := range createdInitialPods {
			ret = append(ret, watch.Event{Type: watch.Added, Object: createdPod})
		}
		return ret
	}
	initialEventsEndFromLastCreatedPod := func(createdInitialPods []*example.Pod) watch.Event {
		return watch.Event{
			Type: watch.Bookmark,
			Object: &example.Pod{
				ObjectMeta: metav1.ObjectMeta{
					ResourceVersion: createdInitialPods[len(createdInitialPods)-1].ResourceVersion,
					Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
				},
			},
		}
	}
	scenarios := []struct {
		name                string
		allowWatchBookmarks bool
		sendInitialEvents   *bool
		resourceVersion     string
		// useCurrentRV 若被设置，则在添加初始 pod 之后从存储获取当前 RV，
		// 并用它来建立新的 watch 请求
		useCurrentRV bool

		initialPods                []*example.Pod
		podsAfterEstablishingWatch []*example.Pod

		expectedInitialEvents                      func(createdInitialPods []*example.Pod) []watch.Event
		expectedInitialEventsBookmarkWithMinimalRV func(createdInitialPods []*example.Pod) watch.Event
		expectedEventsAfterEstablishingWatch       func(createdPodsAfterWatch []*example.Pod) []watch.Event
	}{
		{
			name:                  "allowWatchBookmarks=true, sendInitialEvents=true, RV=unset",
			allowWatchBookmarks:   true,
			sendInitialEvents:     &trueVal,
			initialPods:           []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents: addEventsFromCreatedPods,
			expectedInitialEventsBookmarkWithMinimalRV: initialEventsEndFromLastCreatedPod,
			podsAfterEstablishingWatch:                 []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch:       addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=true, sendInitialEvents=false, RV=unset",
			allowWatchBookmarks:                  true,
			sendInitialEvents:                    &falseVal,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=false, RV=unset",
			sendInitialEvents:                    &falseVal,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=true, RV=unset",
			sendInitialEvents:                    &trueVal,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},

		{
			name:                  "allowWatchBookmarks=true, sendInitialEvents=true, RV=0",
			allowWatchBookmarks:   true,
			sendInitialEvents:     &trueVal,
			resourceVersion:       "0",
			initialPods:           []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents: addEventsFromCreatedPods,
			expectedInitialEventsBookmarkWithMinimalRV: initialEventsEndFromLastCreatedPod,
			podsAfterEstablishingWatch:                 []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch:       addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=true, sendInitialEvents=false, RV=0",
			allowWatchBookmarks:                  true,
			sendInitialEvents:                    &falseVal,
			resourceVersion:                      "0",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=false, RV=0",
			sendInitialEvents:                    &falseVal,
			resourceVersion:                      "0",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=true, RV=0",
			sendInitialEvents:                    &trueVal,
			resourceVersion:                      "0",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},

		{
			name:                  "allowWatchBookmarks=true, sendInitialEvents=true, RV=1",
			allowWatchBookmarks:   true,
			sendInitialEvents:     &trueVal,
			resourceVersion:       "1",
			initialPods:           []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents: addEventsFromCreatedPods,
			expectedInitialEventsBookmarkWithMinimalRV: initialEventsEndFromLastCreatedPod,
			podsAfterEstablishingWatch:                 []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch:       addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=true, sendInitialEvents=false, RV=1",
			allowWatchBookmarks:                  true,
			sendInitialEvents:                    &falseVal,
			resourceVersion:                      "1",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=false, RV=1",
			sendInitialEvents:                    &falseVal,
			resourceVersion:                      "1",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=true, RV=1",
			sendInitialEvents:                    &trueVal,
			resourceVersion:                      "1",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},

		{
			name:                  "allowWatchBookmarks=true, sendInitialEvents=true, RV=useCurrentRV",
			allowWatchBookmarks:   true,
			sendInitialEvents:     &trueVal,
			useCurrentRV:          true,
			initialPods:           []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents: addEventsFromCreatedPods,
			expectedInitialEventsBookmarkWithMinimalRV: initialEventsEndFromLastCreatedPod,
			podsAfterEstablishingWatch:                 []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch:       addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=true, sendInitialEvents=false, RV=useCurrentRV",
			allowWatchBookmarks:                  true,
			sendInitialEvents:                    &falseVal,
			useCurrentRV:                         true,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=false, RV=useCurrentRV",
			sendInitialEvents:                    &falseVal,
			useCurrentRV:                         true,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "allowWatchBookmarks=false, sendInitialEvents=true, RV=useCurrentRV",
			sendInitialEvents:                    &trueVal,
			useCurrentRV:                         true,
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},

		{
			name:                                 "legacy, RV=0",
			resourceVersion:                      "0",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
		{
			name:                                 "legacy, RV=unset",
			initialPods:                          []*example.Pod{makePod("1"), makePod("2"), makePod("3")},
			expectedInitialEvents:                addEventsFromCreatedPods,
			podsAfterEstablishingWatch:           []*example.Pod{makePod("4"), makePod("5")},
			expectedEventsAfterEstablishingWatch: addEventsFromCreatedPods,
		},
	}
	for idx, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			t.Parallel()
			// 准备环境
			if scenario.expectedInitialEvents == nil {
				scenario.expectedInitialEvents = func(_ []*example.Pod) []watch.Event { return nil }
			}
			if scenario.expectedEventsAfterEstablishingWatch == nil {
				scenario.expectedEventsAfterEstablishingWatch = func(_ []*example.Pod) []watch.Event { return nil }
			}

			var createdPods []*example.Pod
			ns := fmt.Sprintf("ns-%v", idx)
			for _, obj := range scenario.initialPods {
				obj.Namespace = ns
				out := &example.Pod{}
				err := store.Create(ctx, computePodKey(obj), obj, out, 0)
				require.NoError(t, err, "failed to add a pod: %v", obj)
				createdPods = append(createdPods, out)
			}

			if len(createdPods) > 0 {
				// 这次 list 调用确保缓存已经看到所创建的 pod，
				// 使下面的 watch 请求成为确定性行为。
				listObject := &example.PodList{}
				opts := storage.ListOptions{
					Predicate:            storage.Everything,
					Recursive:            true,
					ResourceVersion:      createdPods[len(createdPods)-1].ResourceVersion,
					ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
				}
				err := store.GetList(ctx, fmt.Sprintf("/pods/%s", ns), opts, listObject)
				require.NoError(t, err)
				require.Len(t, listObject.Items, len(createdPods))
			}

			if scenario.useCurrentRV {
				currentStorageRV, err := store.GetCurrentResourceVersion(ctx)
				require.NoError(t, err)
				scenario.resourceVersion = fmt.Sprintf("%d", currentStorageRV)
			}

			opts := storage.ListOptions{Predicate: storage.Everything, Recursive: true}
			opts.SendInitialEvents = scenario.sendInitialEvents
			opts.Predicate.AllowWatchBookmarks = scenario.allowWatchBookmarks
			if len(scenario.resourceVersion) > 0 {
				opts.ResourceVersion = scenario.resourceVersion
			}

			w, err := store.Watch(context.Background(), fmt.Sprintf("/pods/%s", ns), opts)
			require.NoError(t, err, "failed to create watch: %v")
			defer w.Stop()

			// 确保我们只收到初始事件
			TestCheckResultsInStrictOrder(t, w, scenario.expectedInitialEvents(createdPods))

			// 确保实际 bookmark 的 RV 至少 >= 期望值
			if scenario.expectedInitialEventsBookmarkWithMinimalRV != nil {
				testCheckResultFunc(t, w, func(actualEvent watch.Event) {
					expectedBookmarkEventWithMinRV := scenario.expectedInitialEventsBookmarkWithMinimalRV(createdPods)
					expectedObj, err := meta.Accessor(expectedBookmarkEventWithMinRV.Object)
					require.NoError(t, err)
					expectedRV, err := storage.APIObjectVersioner{}.ObjectResourceVersion(expectedBookmarkEventWithMinRV.Object)
					require.NoError(t, err)

					actualObj, err := meta.Accessor(actualEvent.Object)
					require.NoError(t, err)
					actualRV, err := storage.APIObjectVersioner{}.ObjectResourceVersion(actualEvent.Object)
					require.NoError(t, err)

					require.GreaterOrEqual(t, actualRV, expectedRV)

					// 确认 RV 至少 >= 期望值之后，
					// 改写它以便比较对象内容
					expectedObj.SetResourceVersion(actualObj.GetResourceVersion())
					expectNoDiff(t, "incorrect event", expectedBookmarkEventWithMinRV, actualEvent)
				})
			}

			createdPods = []*example.Pod{}
			// 添加一个 RV 高于 watch 启动时存储 RV 的 pod
			for _, obj := range scenario.podsAfterEstablishingWatch {
				obj.Namespace = ns
				out := &example.Pod{}
				err = store.Create(ctx, computePodKey(obj), obj, out, 0)
				require.NoError(t, err, "failed to add a pod: %v")
				createdPods = append(createdPods, out)
			}
			ignoreEventsFn := func(event watch.Event) bool { return event.Type == watch.Bookmark }
			testCheckResultWithIgnoreFunc(t, w, scenario.expectedEventsAfterEstablishingWatch(createdPods), ignoreEventsFn)
			TestCheckNoMoreResultsWithIgnoreFunc(t, w, ignoreEventsFn)
		})
	}
}

// RunWatchSemanticInitialEventsExtended 检查标记 list 流结束的 bookmark 事件
// 是否携带全局 RV。
//
// 注意该场景与 RunWatchSemantics 的区别：把 Pod 添加到另一个命名空间以推进全局 RV。
// RunWatchSemanticInitialEventsExtended 是 RunWatchSemantics 的扩展场景，
// 覆盖带 annotation 的对象与初始事件书签的组合。
func RunWatchSemanticInitialEventsExtended(ctx context.Context, t *testing.T, store storage.Interface) {
	trueVal := true
	expectedInitialEventsInStrictOrder := func(initialPods []*example.Pod, globalResourceVersion string) []watch.Event {
		watchEvents := []watch.Event{}
		for _, initialPod := range initialPods {
			watchEvents = append(watchEvents, watch.Event{Type: watch.Added, Object: initialPod})
		}
		watchEvents = append(watchEvents, watch.Event{Type: watch.Bookmark, Object: &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: globalResourceVersion,
				Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
			},
		}})
		return watchEvents
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)

	initialPods := []*example.Pod{}
	ns := "ns-foo"
	for _, initialPod := range []*example.Pod{makePod("1"), makePod("2"), makePod("3"), makePod("4"), makePod("5")} {
		initialPod.Namespace = ns
		out := &example.Pod{}
		err := store.Create(ctx, computePodKey(initialPod), initialPod, out, 0)
		require.NoError(t, err, "failed to add a pod: %v")
		initialPods = append(initialPods, out)
	}

	// 把 pod 添加到另一个命名空间，以推进全局 RV
	pod := makePod("1")
	pod.Namespace = "other-ns-foo"
	otherNsPod := &example.Pod{}
	err := store.Create(ctx, computePodKey(pod), pod, otherNsPod, 0)
	require.NoError(t, err, "failed to add a pod: %v")

	opts := storage.ListOptions{Predicate: storage.Everything, Recursive: true}
	opts.SendInitialEvents = &trueVal
	opts.Predicate.AllowWatchBookmarks = true

	w, err := store.Watch(context.Background(), fmt.Sprintf("/pods/%s", ns), opts)
	require.NoError(t, err, "failed to create watch: %v")
	defer w.Stop()

	// 确保只收到来自第一个命名空间的初始事件，
	// 随后是携带全局 RV 的 bookmark
	TestCheckResultsInStrictOrder(t, w, expectedInitialEventsInStrictOrder(initialPods, otherNsPod.ResourceVersion))
	TestCheckNoMoreResultsWithIgnoreFunc(t, w, nil)
}

// RunWatchListMatchSingle 验证 watch 与 list 使用同一 selector 时结果一致。
func RunWatchListMatchSingle(ctx context.Context, t *testing.T, store storage.Interface) {
	trueVal := true
	expectedInitialEventsInStrictOrder := func(initialPod *example.Pod, globalResourceVersion string) []watch.Event {
		watchEvents := []watch.Event{}
		watchEvents = append(watchEvents, watch.Event{Type: watch.Added, Object: initialPod})
		watchEvents = append(watchEvents, watch.Event{Type: watch.Bookmark, Object: &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				ResourceVersion: globalResourceVersion,
				Annotations:     map[string]string{metav1.InitialEventsAnnotationKey: "true"},
			},
		}})
		return watchEvents
	}
	featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.WatchList, true)

	// 添加将用于构造 field selector 的 pod
	ns := "ns-foo"
	expectedPod := &example.Pod{}
	initialPod := makePod("1")
	initialPod.Namespace = ns
	err := store.Create(ctx, computePodKey(initialPod), initialPod, expectedPod, 0)
	require.NoError(t, err, "failed to add a pod: %v")

	// 再添加若干不匹配该 field selector 的 pod
	lastAddedPod := &example.Pod{}
	for _, otherPod := range []*example.Pod{makePod("2"), makePod("3"), makePod("4"), makePod("5")} {
		otherPod.Namespace = ns
		err = store.Create(ctx, computePodKey(otherPod), otherPod, lastAddedPod, 0)
		require.NoError(t, err, "failed to add a pod: %v")
	}

	opts := storage.ListOptions{
		Predicate: storage.SelectionPredicate{
			Label: labels.Everything(),
			Field: fields.ParseSelectorOrDie("metadata.name=pod-1"),
			GetAttrs: func(obj runtime.Object) (labels.Set, fields.Set, error) {
				pod := obj.(*example.Pod)
				return nil, fields.Set{"metadata.name": pod.Name}, nil
			},
		},
		Recursive: true,
	}
	opts.SendInitialEvents = &trueVal
	opts.Predicate.AllowWatchBookmarks = true

	w, err := store.Watch(context.Background(), "/pods/", opts)
	require.NoError(t, err, "failed to create watch: %v")
	defer w.Stop()

	// 确保只收到一个匹配该 field selector 的 pod，
	// 随后是携带全局 RV 的 bookmark
	TestCheckResultsInStrictOrder(t, w, expectedInitialEventsInStrictOrder(expectedPod, lastAddedPod.ResourceVersion))
	TestCheckNoMoreResultsWithIgnoreFunc(t, w, nil)
}

// RunWatchErrorIsBlockingFurtherEvents 验证某个事件解码失败后，watch 会以错误终止，
// 而不会跳过该事件继续分发后续事件（否则客户端会漏数据）。
func RunWatchErrorIsBlockingFurtherEvents(ctx context.Context, t *testing.T, store InterfaceWithPrefixTransformer) {
	foo := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "foo"}}
	fooKey := fmt.Sprintf("/pods/%s/%s", foo.Namespace, foo.Name)
	fooCreated := &example.Pod{}
	if err := store.Create(context.Background(), fooKey, foo, fooCreated, 0); err != nil {
		t.Errorf("failed to create object: %v", err)
	}
	bar := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "bar"}}
	barKey := fmt.Sprintf("/pods/%s/%s", bar.Namespace, bar.Name)
	barCreated := &example.Pod{}
	if err := store.Create(context.Background(), barKey, bar, barCreated, 0); err != nil {
		t.Errorf("failed to create object: %v", err)
	}

	// 更新 transformer，确保 foo 实际上变成"已损坏"。
	revertTransformer := store.UpdatePrefixTransformer(
		func(transformer *PrefixTransformer) value.Transformer {
			transformer.prefix = []byte("other-prefix")
			return transformer
		})
	defer revertTransformer()

	baz := &example.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "test", Name: "baz"}}
	bazKey := fmt.Sprintf("/pods/%s/%s", baz.Namespace, baz.Name)
	bazCreated := &example.Pod{}
	if err := store.Create(context.Background(), bazKey, baz, bazCreated, 0); err != nil {
		t.Errorf("failed to create object: %v", err)
	}

	opts := storage.ListOptions{
		ResourceVersion: fooCreated.ResourceVersion,
		Predicate:       storage.Everything,
		Recursive:       true,
	}

	// 并发运行 N 个 watch。鉴于异步特性，这样做能提高
	// 至少一个 watch 命中该竞态的概率。
	concurrentWatches := 10
	wg := sync.WaitGroup{}
	for i := 0; i < concurrentWatches; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, err := store.Watch(ctx, "/pods/", opts)
			if err != nil {
				t.Errorf("failed to create watch: %v", err)
				return
			}

			// 我们从对象 bar 的位置发起 watch。
			// 该对象在 TransformFromStorage 阶段失败并产生 ERROR watch 事件。
			// 其后的事件（即 baz 对象的 ADDED 事件）不应当再被发出，
			// 因此我们验证除 ERROR 类型外没有任何事件被发出。
			for {
				event, ok := <-w.ResultChan()
				if !ok {
					break
				}
				if event.Type != watch.Error {
					t.Errorf("unexpected event: %#v", event)
				}
			}
		}()
	}
	wg.Wait()
}

// makePod 构造名为 pod-<prefix> 的测试对象。
func makePod(namePrefix string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("pod-%s", namePrefix),
		},
	}
}

// testWatchStruct 描述一次写入及其期望产生的 watch 事件。
type testWatchStruct struct {
	obj         *example.Pod
	expectEvent bool
	watchType   watch.EventType
}

// basePod 返回无命名空间的基线对象。
func basePod(podName string) *example.Pod {
	return baseNamespacedPod(podName, "")
}

// basePodUpdated 在基线对象上增加 Running 状态，用于触发 Modified。
func basePodUpdated(podName string) *example.Pod {
	return baseNamespacedPodUpdated(podName, "")
}

// basePodAssigned 设置 nodeName，用于验证 field selector 过滤。
func basePodAssigned(podName, nodeName string) *example.Pod {
	return baseNamespacedPodAssigned(podName, "", nodeName)
}

// baseNamespacedPod 返回指定命名空间的基线对象。
func baseNamespacedPod(podName, namespace string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
	}
}

// baseNamespacedPodUpdated 返回带 Running 状态的命名空间对象。
func baseNamespacedPodUpdated(podName, namespace string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Status:     example.PodStatus{Phase: "Running"},
	}
}

// baseNamespacedPodAssigned 返回设置了 nodeName 的命名空间对象。
func baseNamespacedPodAssigned(podName, namespace, nodeName string) *example.Pod {
	return &example.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: namespace},
		Spec:       example.PodSpec{NodeName: nodeName},
	}
}
