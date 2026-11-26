/*
Copyright 2025 The Kubernetes Authors.

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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/watch"
)

var benchmarkNamespace = "default"

// BenchmarkSharedIndexInformer 在不同缓存规模与并发读者数下测量 informer
// 处理写入和 ListAllByNamespace 读取的吞吐，用于发现锁竞争与索引退化。
func BenchmarkSharedIndexInformer(b *testing.B) {
	// 规模跨越 1.6 万到 16 万，按 2 倍递增，便于观察复杂度趋势。
	for _, podCount := range []int{10_000, 20_000, 40_000, 80_000, 160_000} {
		b.Run(fmt.Sprintf("podCount=%d", podCount), func(b *testing.B) {
			pods := createPods(podCount, benchmarkNamespace)
			for _, readers := range []int{0, 1, 10, 20, 40, 80} {
				b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
					watcher := watch.NewFakeWithChanSize(1, false)
					informer, stop := setupSharedIndexInformer(watcher, pods)
					defer stop()
					queuedEvents := 10
					benchmarkSharedIndexInformer(b, readers, watcher, informer, pods, queuedEvents)
				})
			}
		})
	}
}

// createPods 生成指定数量的 Pod，名字随机以接近真实分布。
func createPods(count int, namespace string) []corev1.Pod {
	pods := []corev1.Pod{}
	for i := 0; i < count; i++ {
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rand.String(20),
				Namespace: namespace,
			},
		}
		pods = append(pods, *pod)
	}
	return pods
}

// setupSharedIndexInformer 用内存 List + 假 watcher 启动 informer，并忙等到缓存同步，
// 返回的清理函数负责关闭 informer 并等待其 goroutine 退出。
func setupSharedIndexInformer(watcher watch.Interface, pods []corev1.Pod) (SharedIndexInformer, func()) {
	podInformer := NewSharedIndexInformerWithOptions(
		&ListWatch{
			ListWithContextFunc: func(ctx context.Context, options metav1.ListOptions) (runtime.Object, error) {
				return &corev1.PodList{
					Items: pods,
				}, nil
			},
			WatchFuncWithContext: func(ctx context.Context, options metav1.ListOptions) (watch.Interface, error) {
				return watcher, nil
			},
		},
		&corev1.Pod{},
		SharedIndexInformerOptions{
			ResyncPeriod: time.Second * 60,
			Indexers:     Indexers{NamespaceIndex: MetaNamespaceIndexFunc},
		},
	)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		podInformer.Run(stop)
	}()
	for !podInformer.HasSynced() {
		time.Sleep(time.Millisecond)
	}
	return podInformer, func() {
		close(stop)
		wg.Wait()
	}
}

func benchmarkSharedIndexInformer(b *testing.B, readers int, watcher *watch.FakeWatcher, podInformer SharedIndexInformer, pods []corev1.Pod, queuedEvents int) {
	var writes atomic.Int64
	got := make(chan struct{}, queuedEvents)

	_, err := podInformer.AddEventHandler(ResourceEventHandlerDetailedFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			writes.Add(1)
			select {
			case <-got:
			default:
			}
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	pod := pods[rand.Intn(len(pods))]
	watcher.Modify(&pod)
	// 等待修改事件到达，确认初始 Pod 已被处理。
	for writes.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	var reads atomic.Int64
	// readers=0 表示纯写入基线；其余并发读者持续执行全量 namespace 列表，
	// 制造读锁与写锁的竞争。
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				err := ListAllByNamespace(podInformer.GetIndexer(), benchmarkNamespace, labels.Everything(), func(obj interface{}) {
				})
				if err != nil {
					panic(err)
				}
				reads.Add(1)
			}
		}()
	}

	writes.Store(0)
	reads.Store(0)
	// got 是有界信号量：队列满时阻塞，模拟处理速度跟不上事件速率时的背压。
	for b.Loop() {
		pod := pods[rand.Intn(len(pods))]
		watcher.Modify(&pod)
		got <- struct{}{}
	}
	b.ReportMetric(float64(reads.Load())/b.Elapsed().Seconds(), "reads/s")
	b.ReportMetric(float64(writes.Load())/b.Elapsed().Seconds(), "writes/s")
	close(stop)
	wg.Wait()
}
