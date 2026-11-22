/*
Copyright 2019 The Kubernetes Authors.

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

package parallelize

import "context"

// ResultChannel 支持非阻塞的结果发送与接收。
// 通道内最多保留一个结果：先到的结果被接收后，后续结果才有机会写入；
// 通道非空时的其余发送都会被丢弃。这样发送方永远不会因为结果没人读而被卡住。
type ResultChannel[T comparable] struct {
	ch chan T
}

// Send 非阻塞地发送一个结果；通道已满时直接丢弃，避免拖慢调用方。
func (e *ResultChannel[T]) Send(result T) {
	select {
	case e.ch <- result:
	default:
	}
}

// SendWithCancel 非阻塞地发送结果并调用 cancel 取消上下文。
// 用于并行任务中：一旦有 goroutine 得到结果，就通知其他 goroutine 停止。
func (e *ResultChannel[T]) SendWithCancel(result T, cancel context.CancelFunc) {
	e.Send(result)
	cancel()
}

// Receive 非阻塞地接收一个结果；通道为空时返回零值，
// 调用方可用零值区分“尚无结果”与“已有结果”。
func (e *ResultChannel[T]) Receive() T {
	select {
	case result := <-e.ch:
		return result
	default:
		var zeroValue T
		return zeroValue
	}
}

// NewResultChannel 创建一个容量为 1 的 ResultChannel。
func NewResultChannel[T comparable]() *ResultChannel[T] {
	return &ResultChannel[T]{
		ch: make(chan T, 1),
	}
}
