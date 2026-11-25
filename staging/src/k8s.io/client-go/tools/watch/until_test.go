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

package watch

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	fakeclient "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
)

// fakePod 是最小的 runtime.Object 实现，仅用于在 watch 事件中携带一个可断言的类型。
// DeepCopy 故意 panic，以暴露测试中不该发生的深拷贝。
type fakePod struct {
}

func (obj *fakePod) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (obj *fakePod) DeepCopyObject() runtime.Object   { panic("DeepCopyObject not supported by fakePod") }

// TestUntil 验证条件按顺序消费事件：Add 满足第一个条件、Modify 满足第二个，
// UntilWithoutRetry 返回最后一条已处理事件。
func TestUntil(t *testing.T) {
	fw := watch.NewFake()
	go func() {
		var obj *fakePod
		fw.Add(obj)
		fw.Modify(obj)
	}()
	conditions := []ConditionFunc{
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
		func(event watch.Event) (bool, error) { return event.Type == watch.Modified, nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	lastEvent, err := UntilWithoutRetry(ctx, fw, conditions...)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if lastEvent == nil {
		t.Fatal("expected an event")
	}
	if lastEvent.Type != watch.Modified {
		t.Fatalf("expected MODIFIED event type, got %v", lastEvent.Type)
	}
	if got, isPod := lastEvent.Object.(*fakePod); !isPod {
		t.Fatalf("expected a pod event, got %#v", got)
	}
}

// TestUntilMultipleConditions 验证同一事件需依次满足所有条件，
// 单条 Add 即可让两个条件都成立。
func TestUntilMultipleConditions(t *testing.T) {
	fw := watch.NewFake()
	go func() {
		var obj *fakePod
		fw.Add(obj)
	}()
	conditions := []ConditionFunc{
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	lastEvent, err := UntilWithoutRetry(ctx, fw, conditions...)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if lastEvent == nil {
		t.Fatal("expected an event")
	}
	if lastEvent.Type != watch.Added {
		t.Fatalf("expected MODIFIED event type, got %v", lastEvent.Type)
	}
	if got, isPod := lastEvent.Object.(*fakePod); !isPod {
		t.Fatalf("expected a pod event, got %#v", got)
	}
}

// TestUntilMultipleConditionsFail 验证条件链无法全部满足时，
// 等待会在超时后以 ErrWaitTimeout 结束，并返回最后观察到的事件。
func TestUntilMultipleConditionsFail(t *testing.T) {
	fw := watch.NewFake()
	go func() {
		var obj *fakePod
		fw.Add(obj)
	}()
	conditions := []ConditionFunc{
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
		func(event watch.Event) (bool, error) { return event.Type == watch.Deleted, nil },
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	lastEvent, err := UntilWithoutRetry(ctx, fw, conditions...)
	if !wait.Interrupted(err) {
		t.Fatalf("expected ErrWaitTimeout error, got %#v", err)
	}
	if lastEvent == nil {
		t.Fatal("expected an event")
	}
	if lastEvent.Type != watch.Added {
		t.Fatalf("expected ADDED event type, got %v", lastEvent.Type)
	}
	if got, isPod := lastEvent.Object.(*fakePod); !isPod {
		t.Fatalf("expected a pod event, got %#v", got)
	}
}

// TestUntilTimeout 使用 Background context（不会超时），
// 验证条件满足后立即返回而不是等待 watch 关闭。
func TestUntilTimeout(t *testing.T) {
	fw := watch.NewFake()
	go func() {
		var obj *fakePod
		fw.Add(obj)
		fw.Modify(obj)
	}()
	conditions := []ConditionFunc{
		func(event watch.Event) (bool, error) {
			return event.Type == watch.Added, nil
		},
		func(event watch.Event) (bool, error) {
			return event.Type == watch.Modified, nil
		},
	}

	lastEvent, err := UntilWithoutRetry(context.Background(), fw, conditions...)
	if err != nil {
		t.Fatalf("expected nil error, got %#v", err)
	}
	if lastEvent == nil {
		t.Fatal("expected an event")
	}
	if lastEvent.Type != watch.Modified {
		t.Fatalf("expected MODIFIED event type, got %v", lastEvent.Type)
	}
	if got, isPod := lastEvent.Object.(*fakePod); !isPod {
		t.Fatalf("expected a pod event, got %#v", got)
	}
}

// TestUntilErrorCondition 验证条件函数返回的错误会原样传出，
// 而不是被包装成超时错误。
func TestUntilErrorCondition(t *testing.T) {
	fw := watch.NewFake()
	go func() {
		var obj *fakePod
		fw.Add(obj)
	}()
	expected := "something bad"
	conditions := []ConditionFunc{
		func(event watch.Event) (bool, error) { return event.Type == watch.Added, nil },
		func(event watch.Event) (bool, error) { return false, errors.New(expected) },
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	_, err := UntilWithoutRetry(ctx, fw, conditions...)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), expected) {
		t.Fatalf("expected %q in error string, got %q", expected, err.Error())
	}
}

// TestUntilWithSync 覆盖 UntilWithSync 的四种组合：
// 无 precondition 时不等缓存同步、precondition 在无法同步时阻塞、
// precondition 可提前结束循环，以及 precondition 放行后转入常规条件判断。
func TestUntilWithSync(t *testing.T) {
	// FIXME: 测试前置条件
	tt := []struct {
		name             string
		lw               cache.ListerWatcher
		preconditionFunc PreconditionFunc
		conditionFunc    ConditionFunc
		expectedErr      error
		expectedEvent    *watch.Event
	}{
		{
			name: "doesn't wait for sync with no precondition",
			lw: toListWatcherWithUnSupportedWatchListSemantics(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					select {}
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					select {}
				},
			}),
			preconditionFunc: nil,
			conditionFunc: func(e watch.Event) (bool, error) {
				return true, nil
			},
			expectedErr:   wait.ErrorInterrupted(nil),
			expectedEvent: nil,
		},
		{
			name: "waits indefinitely with precondition if it can't sync",
			lw: toListWatcherWithUnSupportedWatchListSemantics(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					select {}
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					select {}
				},
			}),
			preconditionFunc: func(store cache.Store) (bool, error) {
				return true, nil
			},
			conditionFunc: func(e watch.Event) (bool, error) {
				return true, nil
			},
			expectedErr:   fmt.Errorf("UntilWithSync: unable to sync caches: %w", context.DeadlineExceeded),
			expectedEvent: nil,
		},
		{
			name: "precondition can stop the loop",
			lw: func() cache.ListerWatcher {
				fakeclient := fakeclient.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "first"}})

				return toListWatcherWithUnSupportedWatchListSemantics(&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						return fakeclient.CoreV1().Secrets("").List(context.TODO(), options)
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return fakeclient.CoreV1().Secrets("").Watch(context.TODO(), options)
					},
				})
			}(),
			preconditionFunc: func(store cache.Store) (bool, error) {
				_, exists, err := store.Get(&metav1.ObjectMeta{Namespace: "", Name: "first"})
				if err != nil {
					return true, err
				}
				if exists {
					return true, nil
				}
				return false, nil
			},
			conditionFunc: func(e watch.Event) (bool, error) {
				return true, errors.New("should never reach this")
			},
			expectedErr:   nil,
			expectedEvent: nil,
		},
		{
			name: "precondition lets it proceed to regular condition",
			lw: func() cache.ListerWatcher {
				fakeclient := fakeclient.NewSimpleClientset(&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "first"}})

				return toListWatcherWithUnSupportedWatchListSemantics(&cache.ListWatch{
					ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
						return fakeclient.CoreV1().Secrets("").List(context.TODO(), options)
					},
					WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
						return fakeclient.CoreV1().Secrets("").Watch(context.TODO(), options)
					},
				})
			}(),
			preconditionFunc: func(store cache.Store) (bool, error) {
				return false, nil
			},
			conditionFunc: func(e watch.Event) (bool, error) {
				if e.Type == watch.Added {
					return true, nil
				}
				panic("no other events are expected")
			},
			expectedErr:   nil,
			expectedEvent: &watch.Event{Type: watch.Added, Object: &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "first"}}},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			// Informer 以 100ms 间隔轮询等待缓存同步，
			// 因此超时要留足余量
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()

			event, err := UntilWithSync(ctx, tc.lw, &corev1.Secret{}, tc.preconditionFunc, tc.conditionFunc)

			if !reflect.DeepEqual(err, tc.expectedErr) {
				t.Errorf("expected error %#v, got %#v", tc.expectedErr, err)
			}

			if !reflect.DeepEqual(event, tc.expectedEvent) {
				t.Errorf("expected event %#v, got %#v", tc.expectedEvent, event)
			}
		})
	}
}
