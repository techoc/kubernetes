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

package scheduling

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/e2e/framework"
)

func scheduleSuccessEvent(ns, podName, nodeName string) func(*v1.Event) bool {
	return func(e *v1.Event) bool {
		return e.Type == v1.EventTypeNormal &&
			e.Reason == "Scheduled" &&
			strings.HasPrefix(e.Name, podName) &&
			strings.Contains(e.Message, fmt.Sprintf("Successfully assigned %v/%v to %v", ns, podName, nodeName))
	}
}

func scheduleFailureEvent(podName string) func(*v1.Event) bool {
	return func(e *v1.Event) bool {
		return strings.HasPrefix(e.Name, podName) &&
			e.Type == "Warning" &&
			e.Reason == "FailedScheduling"
	}
}

// Action 是要由系统执行的函数。
type Action func(ctx context.Context) error

// 如果执行给定 Action 后系统发出了匹配 predicate 的事件，
// observeEventAfterAction 返回 true。
func observeEventAfterAction(ctx context.Context, c clientset.Interface, ns string, eventPredicate func(*v1.Event) bool, action Action) (bool, error) {
	// TODO (pohly): 添加 context 支持
	observedMatchingEvent := false
	informerStartedChan := make(chan struct{})
	var informerStartedGuard sync.Once

	// 创建一个 informer，list/watch 测试框架命名空间中的事件。
	_, controller := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				ls, err := c.CoreV1().Events(ns).List(ctx, options)
				return ls, err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				// 通知父 goroutine 监控已开始。
				defer informerStartedGuard.Do(func() { close(informerStartedChan) })
				w, err := c.CoreV1().Events(ns).Watch(ctx, options)
				return w, err
			},
		},
		&v1.Event{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				e, ok := obj.(*v1.Event)
				if !ok {
					framework.Failf("Expected *v1.Event, got %T %v", obj, obj)
				}
				ginkgo.By(fmt.Sprintf("Considering event: \nType = [%s], Name = [%s], Reason = [%s], Message = [%s]", e.Type, e.Name, e.Reason, e.Message))
				if eventPredicate(e) {
					observedMatchingEvent = true
				}
			},
		},
	)

	// 启动 informer，并阻塞此 goroutine 等待启动信号。
	informerStopChan := make(chan struct{})
	defer func() { close(informerStopChan) }()
	go controller.Run(informerStopChan)
	<-informerStartedChan

	// 调用 action 函数。
	err := action(ctx)
	if err != nil {
		return false, err
	}

	// 带超时轮询 informer 是否找到匹配的事件。
	// 最多等待 2 分钟，每秒轮询一次。
	timeout := 2 * time.Minute
	interval := 1 * time.Second
	err = wait.PollUntilContextTimeout(ctx, interval, timeout, false, func(ctx context.Context) (bool, error) {
		return observedMatchingEvent, nil
	})
	return err == nil, err
}
