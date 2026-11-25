/*
Copyright 2017 The Kubernetes Authors.

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
	"context"
	"reflect"
	goruntime "runtime"
	"sort"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	fakeclientset "k8s.io/client-go/kubernetes/fake"
	testcore "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/utils/dump"
)

// TestEventProcessorExit：若事件处理器在 stop 后未能退出，本测试预期超时失败。
//
// 两种场景分别让 run 阻塞在“读输入”和“写输出”上，验证 stop 都能打断它；
// 若不能，测试会因等待 exited 而超时失败。
func TestEventProcessorExit(t *testing.T) {
	event := watch.Event{}

	tests := []struct {
		name  string
		write func(e *eventProcessor)
	}{
		{
			name: "exit on blocked read",
			write: func(e *eventProcessor) {
				e.push(event)
			},
		},
		{
			name: "exit on blocked write",
			write: func(e *eventProcessor) {
				e.push(event)
				e.push(event)
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			out := make(chan watch.Event)
			e := newEventProcessor(out)

			test.write(e)

			exited := make(chan struct{})
			go func() {
				e.run()
				close(exited)
			}()

			<-out
			e.stop()
			goruntime.Gosched()
			<-exited
		})
	}
}

// apiInt 是最小的 runtime.Object 替身，借整数把事件序号直接编码进对象，
// 从而能在接收端断言顺序。
type apiInt int

func (apiInt) GetObjectKind() schema.ObjectKind { return nil }
func (apiInt) DeepCopyObject() runtime.Object   { return nil }

// TestEventProcessorOrdersEvents 验证并发 push 时输出顺序与 push 顺序一致（内部队列是 FIFO），
// 并且 1000 个事件全部送达。
func TestEventProcessorOrdersEvents(t *testing.T) {
	out := make(chan watch.Event)
	e := newEventProcessor(out)
	go e.run()

	numProcessed := 0
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	go func() {
		for i := 0; i < 1000; i++ {
			e := <-out
			if got, want := int(e.Object.(apiInt)), i; got != want {
				t.Errorf("unexpected event: got=%d, want=%d", got, want)
			}
			numProcessed++
		}
		cancel()
	}()

	for i := 0; i < 1000; i++ {
		e.push(watch.Event{Object: apiInt(i)})
	}

	<-ctx.Done()
	e.stop()

	if numProcessed != 1000 {
		t.Errorf("unexpected number of events processed: %d", numProcessed)
	}

}

// byEventTypeAndName 为事件提供稳定排序：先按类型，再按 Secret 名。
// 测试末尾据此比较集合，消除 informer 不保证事件顺序带来的抖动。
type byEventTypeAndName []watch.Event

func (a byEventTypeAndName) Len() int      { return len(a) }
func (a byEventTypeAndName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
func (a byEventTypeAndName) Less(i, j int) bool {
	if a[i].Type < a[j].Type {
		return true
	}

	if a[i].Type > a[j].Type {
		return false
	}

	return a[i].Object.(*corev1.Secret).Name < a[j].Object.(*corev1.Secret).Name
}

// newTestSecret 构造仅含名称与一个数据项的 Secret，足够区分不同事件。
func newTestSecret(name, key, value string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		StringData: map[string]string{
			key: value,
		},
	}
}

// TestNewInformerWatcher 对比 WatchListClient 特性开关关闭/开启时，watcher 对外的事件序列差异：
// 关闭时由 reflector 为 list 结果合成 ADDED 事件；开启时服务端直接推送初始事件，且结束标记
// bookmark 不会透传给客户端。
func TestNewInformerWatcher(t *testing.T) {
	// 确保不会在同一 secret 上出现两个同类型事件，否则测试可能不稳定。
	tt := []struct {
		name                    string
		watchListFeatureEnabled bool
		objects                 []runtime.Object
		inputEvents             []watch.Event
		outputEvents            []watch.Event
	}{
		{
			name:                    "WatchListClient feature disabled",
			watchListFeatureEnabled: false,
			objects: []runtime.Object{
				newTestSecret("pod-1", "foo-1", "initial"),
				newTestSecret("pod-2", "foo-2", "initial"),
				newTestSecret("pod-3", "foo-3", "initial"),
			},
			inputEvents: []watch.Event{
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-4", "foo-4", "initial"),
				},
				{
					Type:   watch.Modified,
					Object: newTestSecret("pod-2", "foo-2", "new"),
				},
				{
					Type:   watch.Deleted,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
			},
			outputEvents: []watch.Event{
				// WatchListClient 关闭时，ListAndWatch 会为每个列出的对象
				// 合成假的 ADDED 事件。
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-1", "foo-1", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-2", "foo-2", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
				// 其后是常规事件。
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-4", "foo-4", "initial"),
				},
				{
					Type:   watch.Modified,
					Object: newTestSecret("pod-2", "foo-2", "new"),
				},
				{
					Type:   watch.Deleted,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
			},
		},
		{
			name:                    "WatchListClient feature enabled",
			watchListFeatureEnabled: true,
			objects: []runtime.Object{
				newTestSecret("pod-1", "foo-1", "initial"),
				newTestSecret("pod-2", "foo-2", "initial"),
				newTestSecret("pod-3", "foo-3", "initial"),
			},
			inputEvents: []watch.Event{
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-1", "foo-1", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-2", "foo-2", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
				// ListWatch 的 bookmark 表示初始列举已完成
				{
					Type: watch.Bookmark,
					Object: &corev1.Secret{
						ObjectMeta: metav1.ObjectMeta{
							Annotations: map[string]string{
								metav1.InitialEventsAnnotationKey: "true",
							},
						},
					},
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-4", "foo-4", "initial"),
				},
				{
					Type:   watch.Modified,
					Object: newTestSecret("pod-2", "foo-2", "new"),
				},
				{
					Type:   watch.Deleted,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
			},
			outputEvents: []watch.Event{
				// WatchListClient 开启时，WatchList 直接从服务端接收
				// 每个已存在对象的 ADDED 事件。
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-1", "foo-1", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-2", "foo-2", "initial"),
				},
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
				// 列举结束时的 bookmark 事件不会发送给客户端。
				// 其后是常规事件。
				{
					Type:   watch.Added,
					Object: newTestSecret("pod-4", "foo-4", "initial"),
				},
				{
					Type:   watch.Modified,
					Object: newTestSecret("pod-2", "foo-2", "new"),
				},
				{
					Type:   watch.Deleted,
					Object: newTestSecret("pod-3", "foo-3", "initial"),
				},
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, tc.watchListFeatureEnabled)

			fake := fakeclientset.NewSimpleClientset(tc.objects...)
			inputCh := make(chan watch.Event)
			inputWatcher := watch.NewProxyWatcher(inputCh)
			// 输出 watcher 停止时 Indexer 应当也会停止输入 watcher。
			// 但为保险起见，测试结束时再显式停止一次。
			defer inputWatcher.Stop()
			inputStopCh := inputWatcher.StopChan()
			// 用 watch reactor 替换真实 watch，把用例中的 inputEvents 原样注入 reflector。
			fake.PrependWatchReactor("secrets", testcore.DefaultWatchReactor(inputWatcher, nil))
			// 发送事件后关闭 done channel
			inputDoneCh := make(chan struct{})
			go func() {
				defer close(inputDoneCh)
				for _, e := range tc.inputEvents {
					select {
					case inputCh <- e:
					case <-inputStopCh:
						return
					}
				}
			}()

			lw := &cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					return fake.CoreV1().Secrets("").List(context.TODO(), options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return fake.CoreV1().Secrets("").Watch(context.TODO(), options)
				},
			}
			//nolint:logcheck // 有意使用较旧的 API。
			_, _, outputWatcher, informerDoneCh := NewIndexerInformerWatcher(lw, &corev1.Secret{})
			outputCh := outputWatcher.ResultChan()
			timeoutCh := time.After(wait.ForeverTestTimeout)
			var result []watch.Event
			// 只收齐期望数量的事件即提前退出，随后用排序消除顺序差异。
		loop:
			for {
				select {
				case event, ok := <-outputCh:
					if !ok {
						t.Errorf("Output result channel closed prematurely")
						break loop
					}
					result = append(result, *event.DeepCopy())
					if len(result) >= len(tc.outputEvents) {
						break loop
					}
				case <-timeoutCh:
					t.Error("Timed out waiting for events")
					break loop
				}
			}

			// Informer 不保证事件顺序，需先排序再比较两个数组。
			sort.Sort(byEventTypeAndName(tc.outputEvents))
			sort.Sort(byEventTypeAndName(result))

			if !reflect.DeepEqual(tc.outputEvents, result) {
				t.Errorf("\nexpected: %s,\ngot:      %s,\ndiff: %s", dump.Pretty(tc.outputEvents), dump.Pretty(result), cmp.Diff(tc.outputEvents, result))
				return
			}

			// 再发送一些事件，但不再读取。
			// 消费者停止 watcher 时应停止生产事件。
			go func() {
				defer close(inputCh)
				for _, e := range tc.inputEvents {
					select {
					case inputCh <- e:
					case <-inputStopCh:
						return
					}
				}
			}()

			// 在读完所有数据前停止，确保 informer 能正确处理已关闭的 channel
			outputWatcher.Stop()

			select {
			case <-informerDoneCh:
			case <-time.After(wait.ForeverTestTimeout):
				t.Error("Timed out waiting for informer to cleanup")
			}
		})
	}

}

// TestInformerWatcherDeletedFinalStateUnknown 测试 `NewIndexerInformerWatcher` 中
// `DeleteFunc` 从底层 `DeltaFIFO` 收到 `cache.DeletedFinalStateUnknown` 对象的代码路径。
// 触发条件见
// https://github.com/kubernetes/kubernetes/blob/dc39ab2417bfddcec37be4011131c59921fdbe98/staging/src/k8s.io/client-go/tools/cache/delta_fifo.go#L736-L739。
//
// 代码来自 @liggitt
//
// 场景：首次 list 得到一个 secret，随后 watch 立刻收到 Gone 错误并触发重新 list；
// 第二次 list 为空，于是 DeltaFIFO 用 DeletedFinalStateUnknown 合成删除事件。
// watcher 必须把它正确转换为 Deleted 事件，并保留原始对象（含 RV）。
func TestInformerWatcherDeletedFinalStateUnknown(t *testing.T) {
	listCalls := 0
	watchCalls := 0
	lw := toListWatcherWithUnSupportedWatchListSemantics(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			retval := &corev1.SecretList{}
			if listCalls == 0 {
				// 返回一个非空列表
				retval.ResourceVersion = "1"
				retval.Items = []corev1.Secret{{ObjectMeta: metav1.ObjectMeta{Name: "secret1", Namespace: "ns1", ResourceVersion: "123"}}}
			} else {
				// 首次调用之后返回空列表
				retval.ResourceVersion = "2"
			}
			listCalls++
			return retval, nil
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			w := watch.NewRaceFreeFake()
			if options.ResourceVersion == "1" {
				go func() {
					// 尝试从第一次 list 的 RV 启动 watch 时以 "Gone" 错误关闭
					w.Error(&apierrors.NewGone("gone").ErrStatus)
					w.Stop()
				}()
			}
			watchCalls++
			return w, nil
		},
	})
	//nolint:logcheck // 有意使用较旧的 API。
	_, _, w, done := NewIndexerInformerWatcher(lw, &corev1.Secret{})
	defer w.Stop()

	// 期望收到 secret 的 add 事件
	select {
	case event, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("unexpected close")
		}
		if event.Type != watch.Added {
			t.Fatalf("expected Added event, got %#v", event)
		}
		if event.Object.(*corev1.Secret).ResourceVersion != "123" {
			t.Fatalf("expected added Secret with rv=123, got %#v", event.Object)
		}
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	// 期望收到 secret 的 delete 事件，因为重新 list 时已不含该 secret
	select {
	case event, ok := <-w.ResultChan():
		if !ok {
			t.Fatal("unexpected close")
		}
		if event.Type != watch.Deleted {
			t.Fatalf("expected Deleted event, got %#v", event)
		}
		if event.Object.(*corev1.Secret).ResourceVersion != "123" {
			t.Fatalf("expected deleted Secret with rv=123, got %#v", event.Object)
		}
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	w.Stop()
	select {
	case <-done:
	case <-time.After(time.Second * 10):
		t.Fatal("timeout")
	}

	if listCalls < 2 {
		t.Fatalf("expected at least 2 list calls, got %d", listCalls)
	}
	if watchCalls < 1 {
		t.Fatalf("expected at least 1 watch call, got %d", watchCalls)
	}
}

// listWatchWithUnSupportedWatchListSemanticsWrapper 显式声明不支持 WatchList 语义，
// 强制 reflector 走传统 list+watch 路径，从而构造出上面的 Gone/重列场景。
type listWatchWithUnSupportedWatchListSemanticsWrapper struct {
	*cache.ListWatch
}

func (lw listWatchWithUnSupportedWatchListSemanticsWrapper) IsWatchListSemanticsUnSupported() bool {
	return true
}

func toListWatcherWithUnSupportedWatchListSemantics(lw *cache.ListWatch) cache.ListerWatcher {
	return listWatchWithUnSupportedWatchListSemanticsWrapper{
		lw,
	}
}
