/*
Copyright 2018 The Kubernetes Authors.

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

// Package debugger 提供调度器缓存的调试工具，可在运行时对比并导出缓存内容。
package debugger

import (
	"context"
	"os"
	"os/signal"

	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"
	internalcache "k8s.io/kubernetes/pkg/scheduler/backend/cache"
	internalqueue "k8s.io/kubernetes/pkg/scheduler/backend/queue"
)

// CacheDebugger 聚合缓存对比器与导出器，用于排查调度器缓存与集群实际状态的不一致。
type CacheDebugger struct {
	Comparer CacheComparer
	Dumper   CacheDumper
}

// New 创建一个 CacheDebugger，用 informer lister、调度缓存和调度队列初始化对比器与导出器。
func New(
	nodeLister corelisters.NodeLister,
	podLister corelisters.PodLister,
	cache internalcache.Cache,
	podQueue internalqueue.SchedulingQueue,
) *CacheDebugger {
	return &CacheDebugger{
		Comparer: CacheComparer{
			NodeLister: nodeLister,
			PodLister:  podLister,
			Cache:      cache,
			PodQueue:   podQueue,
		},
		Dumper: CacheDumper{
			cache:    cache,
			podQueue: podQueue,
		},
	}
}

// ListenForSignal 启动一个 goroutine，当进程收到 SIGINT（Windows）或 SIGUSR2（非 Windows）时，
// 触发缓存对比与导出。用单独 goroutine 监听，避免阻塞调用方；ctx 结束时自动退出。
func (d *CacheDebugger) ListenForSignal(ctx context.Context) {
	logger := klog.FromContext(ctx)
	stopCh := ctx.Done()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, compareSignal)

	go func() {
		for {
			select {
			case <-stopCh:
				return
			case <-ch:
				d.Comparer.Compare(logger)
				d.Dumper.DumpAll(logger)
			}
		}
	}()
}
