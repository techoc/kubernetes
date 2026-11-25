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
	"errors"
	"flag"
	"fmt"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	"k8s.io/utils/dump"
)

func init() {
	// 启用依赖包中使用的 klog
	// 依赖链中会打日志；打开详细日志便于失败时定位。
	klog.InitFlags(nil)
	flag.Set("logtostderr", "true")
	flag.Set("v", "9")
}

// testObject 是最小化的 runtime.Object，只携带 resourceVersion 供测试使用。
type testObject struct {
	resourceVersion string
}

func (o testObject) GetObjectKind() schema.ObjectKind { return schema.EmptyObjectKind }
func (o testObject) DeepCopyObject() runtime.Object   { return o }
func (o testObject) GetResourceVersion() string       { return o.resourceVersion }

// withCounter 包装 Watcher 并统计 Watch 被调用次数，用来断言重连行为。
func withCounter(w cache.Watcher) (*uint32, cache.WatcherWithContext) {
	var counter uint32
	return &counter, &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			atomic.AddUint32(&counter, 1)
			return w.Watch(options)
		},
	}
}

// makeTestEvent 构造一个带指定 resourceVersion 的 Added 事件。
func makeTestEvent(rv int) watch.Event {
	return watch.Event{
		Type: watch.Added,
		Object: testObject{
			resourceVersion: fmt.Sprintf("%d", rv),
		},
	}
}

// arrayToChannel 把事件切片转为带缓冲 channel；ProxyWatcher 从其关闭推断 watch 结束。
func arrayToChannel(array []watch.Event) chan watch.Event {
	ch := make(chan watch.Event, len(array))

	for _, event := range array {
		ch <- event
	}

	return ch
}

// parseResourceVersionOrDie 仅供测试：这是模拟服务端的代码，因此可以自行解析 resourceVersion
func parseResourceVersionOrDie(resourceVersion string) uint64 {
	// 由于导入限制，无法使用 etcdstorage.Versioner.ParseResourceVersion()

	if resourceVersion == "" {
		return 0
	}
	version, err := strconv.ParseUint(resourceVersion, 10, 64)
	if err != nil {
		panic(fmt.Errorf("failed to parse resourceVersion %q", resourceVersion))
	}
	return version
}

// fromRV 模拟服务端行为：过滤掉 resourceVersion 不大于请求值的事件，
// 错误事件在首次有效事件之前被丢弃。
func fromRV(resourceVersion string, array []watch.Event) []watch.Event {
	var result []watch.Event
	rv := parseResourceVersionOrDie(resourceVersion)
	for _, event := range array {
		if event.Type == watch.Error {
			if len(result) == 0 {
				// 跳过错误事件，直到找到满足 RV 要求的对象
				continue
			}
		} else {
			rvGetter, ok := event.Object.(resourceVersionGetter)
			if ok {
				if parseResourceVersionOrDie(rvGetter.GetResourceVersion()) <= rv {
					continue
				}
			}
		}

		result = append(result, event)
	}

	return result
}

// closeAfterN 返回一个在转发 n 个事件后自行关闭的 channel，
// 用于模拟服务端周期性断开 watch 连接。
func closeAfterN(n int, source chan watch.Event) chan watch.Event {
	result := make(chan watch.Event, 0)
	go func() {
		defer close(result)
		defer close(source)
		for i := 0; i < n; i++ {
			result <- <-source
		}
	}()
	return result
}

// unexpectedError 模拟返回了非 Status 的未知错误对象，用于验证 RetryWatcher
// 不会因类型断言失败而崩溃。
type unexpectedError struct {
	// 继承任意实现 runtime.Object 接口的结构体都可以。
	metav1.Status
}

var _ runtime.Object = &unexpectedError{}

// TestNewRetryWatcher 验证空 resourceVersion 与 "0" 在构造阶段即被拒绝：
// 底层 WATCH 对这两个值语义不可靠，提前失败比运行期静默错误更好。
func TestNewRetryWatcher(t *testing.T) {
	tt := []struct {
		name      string
		initialRV string
		err       error
	}{
		{
			name:      "empty RV should fail",
			initialRV: "",
			err:       errors.New("initial RV \"\" is not supported due to issues with underlying WATCH"),
		},
		{
			name:      "RV \"0\" should fail",
			initialRV: "0",
			err:       errors.New("initial RV \"0\" is not supported due to issues with underlying WATCH"),
		},
	}
	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewRetryWatcher(tc.initialRV, nil)
			if !reflect.DeepEqual(err, tc.err) {
				t.Errorf("Expected error: %v, got: %v", tc.err, err)
			}
		})
	}
}

// TestRetryWatcher 是核心表驱动测试：覆盖连接错误、410 Gone、超时/5xx、
// 未知错误码/类型、watch 正常关闭、以及从任意 resourceVersion 续传等情形。
// 每个用例断言收到的事件序列与底层 Watch 的调用次数。
func TestRetryWatcher(t *testing.T) {
	tt := []struct {
		name        string
		initialRV   string
		watchClient cache.Watcher
		watchCount  uint32
		expected    []watch.Event
	}{
		{
			name:      "recovers if watchClient returns error",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func() func(options metav1.ListOptions) (watch.Interface, error) {
					firstRun := true
					return func(options metav1.ListOptions) (watch.Interface, error) {
						if firstRun {
							firstRun = false
							return nil, fmt.Errorf("test error")
						}

						return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
							makeTestEvent(2),
						}))), nil
					}
				}(),
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(2),
			},
		},
		{
			name:      "recovers from 410 Gone error on watch establishment",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func() func(options metav1.ListOptions) (watch.Interface, error) {
					firstRun := true
					return func(options metav1.ListOptions) (watch.Interface, error) {
						if firstRun {
							firstRun = false
							return nil, apierrors.NewResourceExpired("")
						}

						return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
							makeTestEvent(2),
						}))), nil
					}
				}(),
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(2),
			},
		},
		{
			name:      "recovers if watchClient returns nil watcher",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func() func(options metav1.ListOptions) (watch.Interface, error) {
					firstRun := true
					return func(options metav1.ListOptions) (watch.Interface, error) {
						if firstRun {
							firstRun = false
							return nil, nil
						}

						return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
							makeTestEvent(2),
						}))), nil
					}
				}(),
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(2),
			},
		},
		{
			name:      "works with empty initialRV",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(2),
					}))), nil
				},
			},
			watchCount: 1,
			expected: []watch.Event{
				makeTestEvent(2),
			},
		},
		{
			name:      "works with initialRV set, skipping the preceding items but reading those directly following",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(1),
						makeTestEvent(2),
					}))), nil
				},
			},
			watchCount: 1,
			expected: []watch.Event{
				makeTestEvent(2),
			},
		},
		{
			name:      "works with initialRV set, skipping the preceding items with none following",
			initialRV: "3",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(2),
					}))), nil
				},
			},
			watchCount: 1,
			expected:   nil,
		},
		{
			name:      "fails on Gone (RV too old error)",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(5),
						makeTestEvent(6),
						{Type: watch.Error, Object: &apierrors.NewGone("").ErrStatus},
						makeTestEvent(7),
						makeTestEvent(8),
					}))), nil
				},
			},
			watchCount: 1,
			expected: []watch.Event{
				makeTestEvent(6),
				{
					Type:   watch.Error,
					Object: &apierrors.NewGone("").ErrStatus,
				},
			},
		},
		{
			name:      "fails on Forbidden",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func() func(options metav1.ListOptions) (watch.Interface, error) {
					return func(options metav1.ListOptions) (watch.Interface, error) {
						return nil, apierrors.NewForbidden(schema.GroupResource{}, "", errors.New("unknown"))
					}
				}(),
			},
			watchCount: 1,
			expected: []watch.Event{
				{
					Type:   watch.Error,
					Object: &apierrors.NewForbidden(schema.GroupResource{}, "", errors.New("unknown")).ErrStatus,
				},
			},
		},
		{
			name:      "fails on Unauthorized",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func() func(options metav1.ListOptions) (watch.Interface, error) {
					return func(options metav1.ListOptions) (watch.Interface, error) {
						return nil, apierrors.NewUnauthorized("")
					}
				}(),
			},
			watchCount: 1,
			expected: []watch.Event{
				{
					Type:   watch.Error,
					Object: &apierrors.NewUnauthorized("").ErrStatus,
				},
			},
		},
		{
			name:      "recovers from timeout error",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(6),
						{
							Type:   watch.Error,
							Object: &apierrors.NewTimeoutError("", 0).ErrStatus,
						},
						makeTestEvent(7),
					}))), nil
				},
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(6),
				makeTestEvent(7),
			},
		},
		{
			name:      "recovers from internal server error",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(6),
						{
							Type:   watch.Error,
							Object: &apierrors.NewInternalError(errors.New("")).ErrStatus,
						},
						makeTestEvent(7),
					}))), nil
				},
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(6),
				makeTestEvent(7),
			},
		},
		{
			name:      "recovers from unexpected error code",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(6),
						{
							Type: watch.Error,
							Object: &metav1.Status{
								Code: 666,
							},
						},
						makeTestEvent(7),
					}))), nil
				},
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(6),
				makeTestEvent(7),
			},
		},
		{
			name:      "recovers from unexpected error type",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(6),
						{
							Type:   watch.Error,
							Object: &unexpectedError{},
						},
						makeTestEvent(7),
					}))), nil
				},
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(6),
				makeTestEvent(7),
			},
		},
		{
			name:      "survives 1 closed watch and reads 1 item",
			initialRV: "5",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(6),
					})))), nil
				},
			},
			watchCount: 2,
			expected: []watch.Event{
				makeTestEvent(6),
			},
		},
		{
			name:      "survives 2 closed watches and reads 2 items",
			initialRV: "4",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(5),
						makeTestEvent(6),
					})))), nil
				},
			},
			watchCount: 3,
			expected: []watch.Event{
				makeTestEvent(5),
				makeTestEvent(6),
			},
		},
		{
			name:      "survives 2 closed watches and reads 2 items for nonconsecutive RVs",
			initialRV: "4",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(5),
						makeTestEvent(7),
					})))), nil
				},
			},
			watchCount: 3,
			expected: []watch.Event{
				makeTestEvent(5),
				makeTestEvent(7),
			},
		},
		{
			name:      "survives 2 closed watches and reads 2 items for nonconsecutive RVs starting at much lower RV",
			initialRV: "2",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(5),
						makeTestEvent(7),
					})))), nil
				},
			},
			watchCount: 3,
			expected: []watch.Event{
				makeTestEvent(5),
				makeTestEvent(7),
			},
		},
		{
			name:      "survives 4 closed watches and reads 4 items for nonconsecutive, spread RVs",
			initialRV: "2",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(5),
						makeTestEvent(6),
						makeTestEvent(7),
						makeTestEvent(11),
					})))), nil
				},
			},
			watchCount: 5,
			expected: []watch.Event{
				makeTestEvent(5),
				makeTestEvent(6),
				makeTestEvent(7),
				makeTestEvent(11),
			},
		},
		{
			name:      "survives 4 closed watches and reads 4 items for nonconsecutive, spread RVs and skips those with lower or equal RV",
			initialRV: "2",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(1, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(1),
						makeTestEvent(2),
						makeTestEvent(5),
						makeTestEvent(6),
						makeTestEvent(7),
						makeTestEvent(11),
					})))), nil
				},
			},
			watchCount: 5,
			expected: []watch.Event{
				makeTestEvent(5),
				makeTestEvent(6),
				makeTestEvent(7),
				makeTestEvent(11),
			},
		},
		{
			name:      "survives 2 closed watches and reads 2+2+1 items skipping those with equal RV",
			initialRV: "1",
			watchClient: &cache.ListWatch{
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return watch.NewProxyWatcher(closeAfterN(2, arrayToChannel(fromRV(options.ResourceVersion, []watch.Event{
						makeTestEvent(1),
						makeTestEvent(2),
						makeTestEvent(5),
						makeTestEvent(6),
						makeTestEvent(7),
						makeTestEvent(11),
					})))), nil
				},
			},
			watchCount: 3,
			expected: []watch.Event{
				makeTestEvent(2),
				makeTestEvent(5),
				makeTestEvent(6),
				makeTestEvent(7),
				makeTestEvent(11),
			},
		},
	}

	for _, tc := range tt {
		t.Run(tc.name, func(t *testing.T) {
			_, ctx := ktesting.NewTestContext(t)
			t.Parallel()

			atomicCounter, watchFunc := withCounter(tc.watchClient)
			watcher, err := newRetryWatcher(ctx, tc.initialRV, watchFunc, time.Duration(0))
			if err != nil {
				t.Fatalf("failed to create a RetryWatcher: %v", err)
			}
			defer func() {
				watcher.Stop()
				t.Log("Waiting on RetryWatcher to stop...")
				<-watcher.Done()
			}()

			var got []watch.Event
			for i := 0; i < len(tc.expected); i++ {
				event, ok := <-watcher.ResultChan()
				if !ok {
					t.Errorf("expected event %s, but channel is closed", dump.Pretty(tc.expected[i]))
					break
				}

				got = append(got, event)
			}

			// （合理性检查，尽力而为）确认没有多余事件可收。
			// RetryWatcher 代理了源 channel，不能立即去读源，需要容忍一定延迟；
			// 既然是尽力而为的检测，可以用较短的等待时长。
			// 这同时确保在事件数为 0 时 watchFunc 有被调用的时间。
			select {
			case event, ok := <-watcher.ResultChan():
				if ok {
					t.Errorf("Unexpected event received after reading all the expected ones: %s", dump.Pretty(event))
				}
			case <-time.After(10 * time.Millisecond):
				break
			}

			var counter uint32
			// 最后一次 watch 重建几乎必然发生但仍存在竞争，我们总是把它计入预期。
			// 等待最后一次 watch 重建完成以避免该竞争。
			err = wait.PollImmediate(10*time.Millisecond, 10*time.Second, func() (done bool, err error) {
				counter = atomic.LoadUint32(atomicCounter)
				return counter == tc.watchCount, nil
			})
			if wait.Interrupted(err) {
				t.Errorf("expected %d watcher starts, but it has started %d times", tc.watchCount, counter)
			} else if err != nil {
				t.Fatal(err)
			}

			if !reflect.DeepEqual(tc.expected, got) {
				t.Fatalf("expected %s, got %s;\ndiff: %s", dump.Pretty(tc.expected), dump.Pretty(got), cmp.Diff(tc.expected, got))
			}
		})
	}
}

// TestRetryWatcherToFinishWithUnreadEvents 验证 Stop 在仍有未读事件时也能
// 及时关闭 Done 与 ResultChan，且重复 Stop 不会 panic。
func TestRetryWatcherToFinishWithUnreadEvents(t *testing.T) {
	watcher, err := NewRetryWatcher("1", &cache.ListWatch{
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return watch.NewProxyWatcher(arrayToChannel([]watch.Event{
				makeTestEvent(2),
			})), nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create a RetryWatcher: %v", err)
	}

	// 给 watcher 机会进入发送事件（阻塞）状态
	time.Sleep(10 * time.Millisecond)

	watcher.Stop()
	// 校验第二次 Stop 不会引发 panic
	watcher.Stop()

	maxTime := time.Second
	select {
	case <-watcher.Done():
		break
	case <-time.After(maxTime):
		t.Errorf("The watcher failed to be closed in %s", maxTime)
	}

	// RetryWatcher 的结果 channel 应当已关闭
	_, ok := <-watcher.ResultChan()
	if ok {
		t.Error("ResultChan is not closed")
	}
}
