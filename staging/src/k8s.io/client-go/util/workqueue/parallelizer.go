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

package workqueue

import (
	"context"
	"sync"

	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// DoWorkPieceFunc 处理单个工作单元；piece 取值 [0, pieces)。
// 同一 piece 至多被一个 worker 处理，但调用顺序不保证。
type DoWorkPieceFunc func(piece int)

// options 保存 ParallelizeUntil 的可选配置。
type options struct {
	chunkSize int
}

// Options 是 ParallelizeUntil 的配置函数，参见 WithChunkSize。
type Options func(*options)

// WithChunkSize allows to set chunks of work items to the workers, rather than
// processing one by one.
// It is recommended to use this option if the number of pieces significantly
// higher than the number of workers and the work done for each item is small.
//
// 增大 chunkSize 可摊薄每个 worker 抢占 channel 的开销，但会降低负载均衡粒度，
// 并让取消响应变慢（一个 chunk 内不会中途停下）。
func WithChunkSize(c int) func(*options) {
	return func(o *options) {
		o.chunkSize = c
	}
}

// ParallelizeUntil is a framework that allows for parallelizing N
// independent pieces of work until done or the context is canceled.
//
// ctx 为 nil 时表示不可取消（仅依赖 pieces 全部完成）；函数会等待所有
// worker 退出后才返回，因此返回即代表“已完成或被取消”。
func ParallelizeUntil(ctx context.Context, workers, pieces int, doWorkPiece DoWorkPieceFunc, opts ...Options) {
	if pieces == 0 {
		return
	}
	o := options{}
	for _, opt := range opts {
		opt(&o)
	}
	// 非法 chunkSize 归一为 1，避免除零与空 chunk。
	chunkSize := o.chunkSize
	if chunkSize < 1 {
		chunkSize = 1
	}

	// chunk 编号预先写入带缓冲的 channel 并关闭，worker 直接 range 消费，
	// 无需额外的分发 goroutine，也无需关闭信号。
	chunks := ceilDiv(pieces, chunkSize)
	toProcess := make(chan int, chunks)
	for i := 0; i < chunks; i++ {
		toProcess <- i
	}
	close(toProcess)

	// ctx 为 nil 时 stop 为 nil channel，select 永远不会在其上就绪，
	// 从而保持“不可取消”的语义。
	var stop <-chan struct{}
	if ctx != nil {
		stop = ctx.Done()
	}
	if chunks < workers {
		// 不为空闲 worker 启动 goroutine，避免无意义的调度开销。
		workers = chunks
	}
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer utilruntime.HandleCrashWithContext(ctx)
			defer wg.Done()
			for chunk := range toProcess {
				start := chunk * chunkSize
				end := start + chunkSize
				if end > pieces {
					// 最后一个 chunk 通常不满，需要截断以免越界。
					end = pieces
				}
				for p := start; p < end; p++ {
					// 每个工作项前检查取消，使取消的粒度是单项而不是整块。
					select {
					case <-stop:
						return
					default:
						doWorkPiece(p)
					}
				}
			}
		}()
	}
	wg.Wait()
}

// ceilDiv 返回 a 除以 b 的上取整，b 必须为正。
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}
