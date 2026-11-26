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

// Package counter 定义 APF 内部用于统计活跃 goroutine 数量的抽象。
// 生产实现基于原子计数，测试实现则让测试代码得以等待“所有就绪工作”完成。
package counter

// GoRoutineCounter 跟踪正在为某事工作的活跃 goroutine 数量，这是一项让此类代码
// 更可测试的工具：被测代码通过它向测试代码报告活跃 goroutine 数，测试代码据此
// 当且仅当被测代码完成当前所有就绪工作时才推进假时钟。
// 计数的语义是“可能还会做事的 goroutine 数”，而非物理线程数；
// 等待/阻塞前减一、唤醒对方前加一，测试据此判断何时推进虚拟时钟。
type GoRoutineCounter interface {
	// Add 把给定增量加到活跃 goroutine 计数上。
	// fork goroutine 前调用 Add(1)，该 goroutine 结束时调用 Add(-1)。
	// 在等待来自另一个 goroutine 的某物之前调用 Add(-1)（例如
	// 在 `select` 之前）。
	// 在做某件会解除另一个 goroutine 阻塞的事情之前调用 Add(1)。
	//
	Add(delta int)
}
