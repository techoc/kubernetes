/*
Copyright The Kubernetes Authors.

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
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fcache "k8s.io/client-go/tools/cache/testing"
	"k8s.io/klog/v2/ktesting"
)

// handlerWaitTime 模拟慢事件处理器，使处理阶段持续占用队列。
const handlerWaitTime = time.Millisecond

// BenchmarkAddWithSlowHandlers 测量事件处理器很慢时向队列添加对象的开销，
// 并对比"处理期间持锁"与"处理期间解锁"两种模式的差异。
func BenchmarkAddWithSlowHandlers(b *testing.B) {
	// unlockWhileProcessing=true 时使用 AtomicEvents 模式，处理期间允许生产者并发入队。
	for _, unlockWhileProcessing := range []bool{false, true} {
		b.Run(fmt.Sprintf("unlockWhileProcessing=%t", unlockWhileProcessing), func(b *testing.B) {
			logger, ctx := ktesting.NewTestContext(b)
			ctx, cancel := context.WithCancel(ctx)
			source := fcache.NewFakeControllerSource()
			b.Cleanup(func() {
				cancel()
				source.Shutdown()
			})
			testIDs := []string{"a-hello"}
			source.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: testIDs[0]}})

			store := NewStore(DeletionHandlingMetaNamespaceKeyFunc)
			fifoOptions := RealFIFOOptions{}
			// 两种模式互斥：AtomicEvents 要求不提供 KnownObjects，
			// 而 UnlockWhileProcessing 只能在 AtomicEvents 下开启。
			if unlockWhileProcessing {
				fifoOptions.UnlockWhileProcessing = true
				fifoOptions.AtomicEvents = true
			} else {
				fifoOptions.KnownObjects = store
			}
			fifo := NewRealFIFOWithOptions(fifoOptions)
			handler := ResourceEventHandlerFuncs{
				UpdateFunc: func(oldObj, newObj interface{}) {
					time.Sleep(handlerWaitTime)
				},
			}

			cfg := &Config{
				Queue:            fifo,
				ListerWatcher:    source,
				ObjectType:       &v1.Pod{},
				FullResyncPeriod: 0,

				Process: func(obj interface{}, isInInitialList bool) error {
					if deltas, ok := obj.(Deltas); ok {
						return processDeltas(logger, handler, store, deltas, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
					}
					return errors.New("object given as Process argument is not Deltas")
				},
				ProcessBatch: func(deltaList []Delta, isInInitialList bool) error {
					return processDeltasInBatch(logger, handler, store, deltaList, isInInitialList, DeletionHandlingMetaNamespaceKeyFunc)
				},
			}
			c := New(cfg)
			go c.RunWithContext(ctx)
			// 等首次同步完成后开始计时，避免初始化噪声影响基准结果。
			if !WaitForCacheSync(ctx.Done(), c.HasSynced) {
				b.Fatal("Timed out waiting for cache sync")
			}

			// Producer：以 handler 能处理的最快速度修改对象，确保 process func 始终在运行。
			// 生产者与慢处理器同频修改对象，保证队列中始终有事件待处理。
			go func() {
				// benchmark context 取消时停止。
				ticker := time.NewTicker(handlerWaitTime)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						source.Modify(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: testIDs[0]}})
					}
				}
			}()
			benchmarkReflectorWithSlowHandlers(b, fifo)
		})
	}
}

// benchmarkReflectorWithSlowHandlers 在控制器持续处理事件的同时，
// 测量向 FIFO 添加新对象的耗时（b.N 次）。
func benchmarkReflectorWithSlowHandlers(b *testing.B, fifo *RealFIFO) {
	// 重置计时，排除上面的准备与同步阶段。
	b.ResetTimer()
	// 在控制器处理其他事件时尝试向队列加入对象。
	for i := 0; i < b.N; i++ {
		if err := fifo.Add(&v1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "b-hello"}}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
