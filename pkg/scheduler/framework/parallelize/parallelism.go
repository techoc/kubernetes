/*
Copyright 2020 The Kubernetes Authors.

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

// Package parallelize 为调度框架提供并行执行能力。
package parallelize

import (
	"context"
	"math"

	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/scheduler/metrics"
)

// DefaultParallelism 是调度器使用的默认并行度。
const DefaultParallelism int = 16

// Parallelizer 实现 k8s.io/kube-scheduler/framework.Parallelizer，
// 在可行处把调度操作分块并行执行，以提升性能和 CPU 利用率。
// 它在 k8s.io/client-go/util/workqueue 之上封装了多 worker 执行逻辑。
type Parallelizer struct {
	parallelism int
}

// NewParallelizer 返回持有给定并行度（worker 数量）的对象。
func NewParallelizer(p int) Parallelizer {
	return Parallelizer{parallelism: p}
}

// chunkSizeFor 根据待处理项数计算并行任务的块大小，目标是获得良好的 CPU 利用率。
// 返回 max(1, min(sqrt(n), n/Parallelism))。
func chunkSizeFor(n, parallelism int) int {
	s := int(math.Sqrt(float64(n)))

	if r := n/parallelism + 1; s > r {
		s = r
	} else if s < 1 {
		s = 1
	}
	return s
}

// numWorkersForChunkSize 返回在给定 parallelism、pieces 和 chunkSize 下，
// workqueue.ParallelizeUntil 将创建的 worker（goroutine）数量。
func numWorkersForChunkSize(parallelism, pieces, chunkSize int) int {
	chunks := (pieces + chunkSize - 1) / chunkSize
	if chunks < parallelism {
		return chunks
	}
	return parallelism
}

// Until 是 workqueue.ParallelizeUntil 的包装，用于调度算法。
// operation 会作为标签记录在 goroutine 指标中。
func (p Parallelizer) Until(ctx context.Context, pieces int, doWorkPiece workqueue.DoWorkPieceFunc, operation string) {
	chunkSize := chunkSizeFor(pieces, p.parallelism)
	workers := numWorkersForChunkSize(p.parallelism, pieces, chunkSize)

	goroutinesMetric := metrics.Goroutines.WithLabelValues(operation)
	// 一次按 worker 数量 Add，比为每个工作项调用 Inc/Dec 更高效。
	// 这能改善部分插件（亲和性、拓扑分布）以及抢占的性能。
	goroutinesMetric.Add(float64(workers))
	defer goroutinesMetric.Add(float64(-workers))

	workqueue.ParallelizeUntil(ctx, p.parallelism, pieces, doWorkPiece, workqueue.WithChunkSize(chunkSize))
}
