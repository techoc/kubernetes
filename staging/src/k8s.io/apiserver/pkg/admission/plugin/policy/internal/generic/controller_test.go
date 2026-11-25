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

package generic_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/admission/plugin/policy/internal/generic"

	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

// testInformer 包装 SharedIndexInformer 并跟踪已注册的事件处理器，
// 以便在测试中断言控制器退出时确实移除了 handler，避免泄漏。
type testInformer struct {
	cache.SharedIndexInformer

	lock          sync.Mutex
	registrations map[interface{}]struct{}
}

// AddEventHandler 注册 handler 并记录其 registration。
func (t *testInformer) AddEventHandler(handler cache.ResourceEventHandler) (cache.ResourceEventHandlerRegistration, error) {
	res, err := t.SharedIndexInformer.AddEventHandler(handler)
	if err != nil {
		return res, err
	}

	func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if t.registrations == nil {
			t.registrations = make(map[interface{}]struct{})
		}
		t.registrations[res] = struct{}{}
	}()

	return res, err
}

// RemoveEventHandler 移除 handler；移除未注册的 handler 视为测试失败。
func (t *testInformer) RemoveEventHandler(registration cache.ResourceEventHandlerRegistration) error {
	func() {
		t.lock.Lock()
		defer t.lock.Unlock()

		if _, ok := t.registrations[registration]; !ok {
			panic("removing unknown event handler?")
		}
		delete(t.registrations, registration)
	}()

	return t.SharedIndexInformer.RemoveEventHandler(registration)
}

var (
	// 用 unstructured 充当任意策略对象，避免为测试注册真实策略类型。
	scheme  *runtime.Scheme             = runtime.NewScheme()
	codecs  serializer.CodecFactory     = serializer.NewCodecFactory(scheme)
	fakeGVR schema.GroupVersionResource = schema.GroupVersionResource{
		Group:    "fake.example.com",
		Version:  "v1",
		Resource: "fakes",
	}
	fakeGVK     schema.GroupVersionKind = fakeGVR.GroupVersion().WithKind("Fake")
	fakeGVKList schema.GroupVersionKind = fakeGVR.GroupVersion().WithKind("FakeList")
)

func init() {
	scheme.AddKnownTypeWithName(fakeGVK, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(fakeGVKList, &unstructured.UnstructuredList{})
}

// setupTest 组装一个基于 fake tracker 的 informer 与通用控制器。
// reconciler 把每次调用的对象副本投递到无缓冲 channel，从而让测试
// 能逐步驱动并观察 reconcile 顺序；channel 无缓冲也使控制器在测试
// 未接收时阻塞，便于断言同步状态。
func setupTest(ctx context.Context, customReconciler func(string, string, runtime.Object) error) (
	tracker clienttesting.ObjectTracker,
	controller generic.Controller[*unstructured.Unstructured],
	informer *testInformer,
	waitForReconcile func(runtime.Object) error,
	verifyNoMoreEvents func() bool,
) {
	tracker = clienttesting.NewObjectTracker(scheme, codecs.UniversalDecoder())
	reconciledObjects := make(chan runtime.Object)

	// 构造返回 mock Policy 定义与 mock policy 绑定的伪 informer
	informer = &testInformer{SharedIndexInformer: cache.NewSharedIndexInformer(cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return tracker.List(fakeGVR, fakeGVK, "")
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return tracker.Watch(fakeGVR, "", options)
		},
	}, tracker), &unstructured.Unstructured{}, 30*time.Second, nil)}

	reconciler := func(namespace, name string, newObj *unstructured.Unstructured) error {
		var err error
		copied := newObj.DeepCopyObject()
		if customReconciler != nil {
			err = customReconciler(namespace, name, newObj)
		}
		select {
		case reconciledObjects <- copied:
		case <-ctx.Done():
			panic("timed out attempting to deliver reconcile event")
		}
		return err
	}

	waitForReconcile = func(obj runtime.Object) error {
		select {
		case reconciledObj := <-reconciledObjects:
			if reflect.DeepEqual(obj, reconciledObj) {
				return nil
			}
			return fmt.Errorf("expected equal objects: %v", cmp.Diff(obj, reconciledObj))
		case <-ctx.Done():
			return fmt.Errorf("context done before reconcile: %w", ctx.Err())
		}
	}

	myController := generic.NewController(
		generic.NewInformer[*unstructured.Unstructured](informer),
		reconciler,
		generic.ControllerOptions{},
	)

	verifyNoMoreEvents = func() bool {
		close(reconciledObjects) // 关闭后若再尝试发送会导致 panic
		for leftover := range reconciledObjects {
			panic(fmt.Errorf("leftover object which was not anticipated by test: %v", leftover))
		}
		// TODO(alexzielenski): 这实际上没有测试到任何内容，因为
		// 控制器在关闭时会丢弃所有待处理事件。
		return true
	}

	return tracker, myController, informer, waitForReconcile, verifyNoMoreEvents
}

// TestReconcile 验证新增、更新、删除事件都会触发 reconcile，
// 且删除事件以 nil 对象回调（表示对象已不存在）。
func TestReconcile(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer testCancel()

	tracker, myController, informer, waitForReconcile, verifyNoMoreEvents := setupTest(testContext, nil)

	// 向 informer 添加对象
	initialObject := &unstructured.Unstructured{}
	initialObject.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "1",
		},
	})
	initialObject.SetGroupVersionKind(fakeGVK)

	require.NoError(t, tracker.Add(initialObject))

	wg := sync.WaitGroup{}

	// 启动 informer
	wg.Add(1)
	go func() {
		defer wg.Done()
		informer.Run(testContext.Done())
	}()

	// 启动控制器
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopReason := myController.Run(testContext)
		if !errors.Is(stopReason, context.Canceled) {
			t.Errorf("expected error to be context.Canceled, but got: %v", stopReason)
		}
	}()

	// 控制器被阻塞：reconcile 函数会向无缓冲 channel 发送数据。
	require.False(t, myController.HasSynced())

	// 等待所有已入队的 reconciliation 完成
	require.NoError(t, waitForReconcile(initialObject))

	// 此时可以安全地等待其 Sync 完成
	require.True(t, cache.WaitForCacheSync(testContext.Done(), myController.HasSynced))

	// 更新对象
	updatedObject := &unstructured.Unstructured{}
	updatedObject.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "2",
		},
		"newKey": "a key",
	})
	updatedObject.SetGroupVersionKind(fakeGVK)
	require.NoError(t, tracker.Update(fakeGVR, updatedObject, ""))

	// 等待所有已入队的 reconciliation 完成
	require.NoError(t, waitForReconcile(updatedObject))
	require.NoError(t, tracker.Delete(fakeGVR, updatedObject.GetNamespace(), updatedObject.GetName()))
	require.NoError(t, waitForReconcile(nil))

	testCancel()
	wg.Wait()

	verifyNoMoreEvents()
}

// TestShutdown 验证上下文取消后控制器与 informer 都能退出，
// 并且事件处理器被移除、不残留注册。
func TestShutdown(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer testCancel()

	_, myController, informer, _, verifyNoMoreEvents := setupTest(testContext, nil)

	wg := sync.WaitGroup{}

	// 启动 informer
	wg.Add(1)
	go func() {
		defer wg.Done()
		informer.Run(testContext.Done())
	}()

	// 启动控制器
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopReason := myController.Run(testContext)
		if !errors.Is(stopReason, context.Canceled) {
			t.Errorf("expected error to be context.Canceled, but got: %v", stopReason)
		}
	}()

	// 等待控制器与 informer 启动
	require.True(t, cache.WaitForCacheSync(testContext.Done(), myController.HasSynced))

	// 停止控制器与 informer
	testCancel()

	// 等待控制器与 informer 退出
	wg.Wait()

	// 确认事件处理器已被清理
	require.Empty(t, informer.registrations)

	verifyNoMoreEvents()
}

// 演示：控制器运行时 informer 尚未启动会抛出错误。
// 验证 informer 未启动时控制器不会永远挂起，而是在 ctx 到期后以
// DeadlineExceeded 停止并清理 handler。
func TestInformerNeverStarts(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer testCancel()

	_, myController, informer, _, verifyNoMoreEvents := setupTest(testContext, nil)

	wg := sync.WaitGroup{}

	// 启动控制器
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopReason := myController.Run(testContext)
		if !errors.Is(stopReason, context.DeadlineExceeded) {
			t.Errorf("expected error to be context.Canceled, but got: %v", stopReason)
		}
	}()

	// 等待截止时间到达，期间缓存不会完成同步
	require.False(t, cache.WaitForCacheSync(testContext.Done(), myController.HasSynced))

	// 等待控制器停止（否则 context 会很快超时）
	wg.Wait()

	// 确认没有残留的事件处理器
	require.Empty(t, informer.registrations)

	verifyNoMoreEvents()
}

// 验证：resourceVersion 未变化时不应重复调用 reconciler，
// 否则周期性 resync 会带来无谓的全量重算。
func TestIgnoredUpdate(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer testCancel()

	tracker, myController, informer, waitForReconcile, verifyNoMoreEvents := setupTest(testContext, nil)

	// 向 informer 添加对象
	initialObject := &unstructured.Unstructured{}
	initialObject.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "1",
		},
	})
	initialObject.SetGroupVersionKind(fakeGVK)

	require.NoError(t, tracker.Add(initialObject))

	wg := sync.WaitGroup{}

	// 启动 informer
	wg.Add(1)
	go func() {
		defer wg.Done()
		informer.Run(testContext.Done())
	}()

	// 启动控制器
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopReason := myController.Run(testContext)
		if !errors.Is(stopReason, context.Canceled) {
			t.Errorf("expected error to be context.Canceled, but got: %v", stopReason)
		}
	}()

	// 控制器被阻塞：reconcile 函数会向无缓冲 channel 发送数据。
	require.False(t, myController.HasSynced())

	// 等待所有已入队的 reconciliation 完成
	require.NoError(t, waitForReconcile(initialObject))

	// 此时可以安全地等待其 Sync 完成
	require.True(t, cache.WaitForCacheSync(testContext.Done(), myController.HasSynced))

	// 用相同对象触发一次更新
	require.NoError(t, tracker.Update(fakeGVR, initialObject, ""))

	// 不等待其被 reconcile

	testCancel()
	wg.Wait()

	// TODO(alexzielenski): 需要找更好的测试方式，因为
	// 控制器在关闭时会丢弃所有待处理事件。
	verifyNoMoreEvents()
}

// 验证：reconciliation 失败的对象会重试，
// 失败的对象应以退避方式快速重试，直到 reconcile 返回成功。
func TestReconcileRetry(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), wait.ForeverTestTimeout)
	defer testCancel()

	calls := atomic.Uint64{}
	success := atomic.Bool{}
	tracker, myController, _, waitForReconcile, verifyNoMoreEvents := setupTest(testContext, func(s1, s2 string, o runtime.Object) error {

		if calls.Add(1) > 2 {
			// 突然开始接受该对象
			success.Store(true)
			return nil
		}
		return errors.New("i dont like this object")
	})

	// 启动 informer
	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		myController.Informer().Run(testContext.Done())
	}()

	// 启动控制器
	wg.Add(1)
	go func() {
		defer wg.Done()
		stopReason := myController.Run(testContext)
		if !errors.Is(stopReason, context.Canceled) {
			t.Errorf("expected error to be context.Canceled, but got: %v", stopReason)
		}
	}()

	// 向 informer 添加对象
	initialObject := &unstructured.Unstructured{}
	initialObject.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "1",
		},
	})
	initialObject.SetGroupVersionKind(fakeGVK)
	require.NoError(t, tracker.Add(initialObject))

	require.NoError(t, waitForReconcile(initialObject), "initial reconcile")
	require.NoError(t, waitForReconcile(initialObject), "previous reconcile failed, should retry quickly")
	require.NoError(t, waitForReconcile(initialObject), "previous reconcile failed, should retry quickly")
	// 上次 reconcile 时 calls > 2，因此不会再重试
	require.True(t, success.Load(), "last call to reconcile should return success")
	testCancel()
	wg.Wait()

	verifyNoMoreEvents()
}

// TestInformerList 验证 informer 包装的 List/Namespaced/Get 语义：
// 增删改后的缓存内容正确，命名空间隔离生效，不存在的对象返回 NotFound。
func TestInformerList(t *testing.T) {
	testContext, testCancel := context.WithTimeout(context.Background(), 2*time.Second)

	tracker, myController, _, _, _ := setupTest(testContext, nil)

	wg := sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
		myController.Informer().Run(testContext.Done())
	}()

	defer func() {
		testCancel()
		wg.Wait()
	}()

	require.True(t, cache.WaitForCacheSync(testContext.Done(), myController.Informer().HasSynced))

	object1 := &unstructured.Unstructured{}
	object1.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "object1",
		},
	})
	object1.SetGroupVersionKind(fakeGVK)

	object1v2 := &unstructured.Unstructured{}
	object1v2.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object1",
			"resourceVersion": "object1v2",
		},
	})
	object1v2.SetGroupVersionKind(fakeGVK)

	object2 := &unstructured.Unstructured{}
	object2.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object2",
			"resourceVersion": "object2",
		},
	})
	object2.SetGroupVersionKind(fakeGVK)

	object3 := &unstructured.Unstructured{}
	object3.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "object3",
			"resourceVersion": "object3",
		},
	})
	object3.SetGroupVersionKind(fakeGVK)

	namespacedObject1 := &unstructured.Unstructured{}
	namespacedObject1.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "namespacedObject1",
			"namespace":       "test",
			"resourceVersion": "namespacedObject1",
		},
	})
	namespacedObject1.SetGroupVersionKind(fakeGVK)

	namespacedObject2 := &unstructured.Unstructured{}
	namespacedObject2.SetUnstructuredContent(map[string]interface{}{
		"metadata": map[string]interface{}{
			"name":            "namespacedObject2",
			"namespace":       "test",
			"resourceVersion": "namespacedObject2",
		},
	})
	namespacedObject2.SetGroupVersionKind(fakeGVK)

	require.NoError(t, tracker.Add(object1))
	require.NoError(t, tracker.Add(object2))

	require.NoError(t, wait.PollUntilContextTimeout(testContext, 100*time.Millisecond, 500*time.Millisecond, false, func(ctx context.Context) (done bool, err error) {
		return myController.Informer().LastSyncResourceVersion() == object2.GetResourceVersion(), nil
	}))

	values, err := myController.Informer().List(labels.Everything())
	require.NoError(t, err)
	require.ElementsMatch(t, []*unstructured.Unstructured{object1, object2}, values)

	require.NoError(t, tracker.Update(fakeGVR, object1v2, object1v2.GetNamespace()))
	require.NoError(t, tracker.Delete(fakeGVR, object2.GetNamespace(), object2.GetName()))
	require.NoError(t, tracker.Add(object3))

	require.NoError(t, wait.PollUntilContextTimeout(testContext, 100*time.Millisecond, 500*time.Millisecond, false, func(ctx context.Context) (done bool, err error) {
		return myController.Informer().LastSyncResourceVersion() == object3.GetResourceVersion(), nil
	}))

	values, err = myController.Informer().List(labels.Everything())
	require.NoError(t, err)
	require.ElementsMatch(t, []*unstructured.Unstructured{object1v2, object3}, values)

	require.NoError(t, tracker.Add(namespacedObject1))
	require.NoError(t, tracker.Add(namespacedObject2))

	require.NoError(t, wait.PollUntilContextTimeout(testContext, 100*time.Millisecond, 500*time.Millisecond, false, func(ctx context.Context) (done bool, err error) {
		return myController.Informer().LastSyncResourceVersion() == namespacedObject2.GetResourceVersion(), nil
	}))
	values, err = myController.Informer().Namespaced(namespacedObject1.GetNamespace()).List(labels.Everything())
	require.NoError(t, err)
	require.ElementsMatch(t, []*unstructured.Unstructured{namespacedObject1, namespacedObject2}, values)

	value, err := myController.Informer().Get(object3.GetName())
	require.NoError(t, err)
	require.Equal(t, value, object3)

	value, err = myController.Informer().Namespaced(namespacedObject1.GetNamespace()).Get(namespacedObject1.GetName())
	require.NoError(t, err)
	require.Equal(t, value, namespacedObject1)

	_, err = myController.Informer().Get("fakeobjectname")
	require.True(t, k8serrors.IsNotFound(err))

	_, err = myController.Informer().Namespaced("test").Get("fakeobjectname")
	require.True(t, k8serrors.IsNotFound(err))

	_, err = myController.Informer().Namespaced("fakenamespace").Get("fakeobjectname")
	require.True(t, k8serrors.IsNotFound(err))
}
