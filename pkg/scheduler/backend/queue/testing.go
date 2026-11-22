/*
Copyright 2021 The Kubernetes Authors.

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

package queue

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// NewTestQueue 创建一个使用空 informer factory 的优先级队列。
func NewTestQueue(ctx context.Context, lessFn fwk.LessFunc, opts ...Option) *PriorityQueue {
	return NewTestQueueWithObjects(ctx, lessFn, nil, opts...)
}

// NewTestQueueWithObjects 创建一个优先级队列，其 informer factory 预先填充了给定对象。
func NewTestQueueWithObjects(
	ctx context.Context,
	lessFn fwk.LessFunc,
	objs []runtime.Object,
	opts ...Option,
) *PriorityQueue {
	informerFactory := informers.NewSharedInformerFactory(fake.NewClientset(objs...), 0)

	// Pop 等主要函数依赖 metric recorder，因此这里总是先设置一个，
	// 避免调用方忘记注入导致空指针。
	recorder := metrics.NewMetricsAsyncRecorder(10, 20*time.Microsecond, ctx.Done())
	// 放在用户 options 之前，这样用户仍可通过自己的 option 覆盖它。
	opts = append([]Option{WithMetricsRecorder(recorder)}, opts...)
	pq := NewTestQueueWithInformerFactory(ctx, lessFn, informerFactory, opts...)
	return pq
}

// NewTestQueueWithInformerFactory 基于给定的 informer factory 创建优先级队列，
// 并启动 factory、等待缓存同步完成后返回。
func NewTestQueueWithInformerFactory(
	ctx context.Context,
	lessFn fwk.LessFunc,
	informerFactory informers.SharedInformerFactory,
	opts ...Option,
) *PriorityQueue {
	pq := NewPriorityQueue(lessFn, informerFactory, opts...)
	informerFactory.Start(ctx.Done())
	informerFactory.WaitForCacheSync(ctx.Done())
	return pq
}
