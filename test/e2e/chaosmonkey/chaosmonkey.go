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

package chaosmonkey

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
)

// Disruption 是构造 Chaosmonkey 时使用的类型；详情见 Do。
type Disruption func(ctx context.Context)

// Test 是注册到 Chaosmonkey 的类型。测试会在 Chaosmonkey 执行 Disruption 期间异步运行。
// Test 接收一个 Semaphore 参数：准备好开始故障注入时调用 sem.Ready()，然后等待 sem.StopCh
// （类型为 <-chan struct{}）关闭，表示故障注入结束；随后清理并返回。详情见 Do 和 Semaphore。
type Test func(ctx context.Context, sem *Semaphore)

// 若不想直接处理 Semaphore，可以实现 Interface：定义包含 Setup、Test、Teardown 三个方法的
// 结构体并通过 RegisterInterface 注册。详情见 RegisterInterface。
type Interface interface {
	Setup()
	Test(stopCh <-chan struct{})
	Teardown()
}

// Chaosmonkey 保存 chaosmonkey 测试所需的内容
type Chaosmonkey struct {
	disruption Disruption
	tests      []Test
}

// New 创建并返回一个 Chaosmonkey，调用方应通过它注册 Test 并调用 Do。详情见 Do。
func New(disruption Disruption) *Chaosmonkey {
	return &Chaosmonkey{
		disruption,
		[]Test{},
	}
}

// Register 将给定的 Test 注册到 Chaosmonkey，使其在执行 Disruption 期间运行。
func (cm *Chaosmonkey) Register(test Test) {
	cm.tests = append(cm.tests, test)
}

// RegisterInterface 将给定的 Interface 注册到 Chaosmonkey，Chaosmonkey 会依次调用
// Setup、Test 和 Teardown。Test 可通过 stopCh 关闭判断 Disruption 已结束。
func (cm *Chaosmonkey) RegisterInterface(in Interface) {
	cm.Register(func(ctx context.Context, sem *Semaphore) {
		in.Setup()
		sem.Ready()
		in.Test(sem.StopCh)
		in.Teardown()
	})
}

// Do 在执行 Disruption 的同时运行所有已注册的 Test。调用方注册完所有 Test 后调用 Do。
// Do 异步启动每个 Test，等待它们通过 sem.Ready() 报告就绪，然后执行 Disruption；完成后关闭
// sem.StopCh 通知各 Test 故障注入已结束，并等待所有 Test 返回。
func (cm *Chaosmonkey) Do(ctx context.Context) {
	sems := []*Semaphore{}
	// 所有 semaphore 共享同一个 StopCh。
	stopCh := make(chan struct{})

	for _, test := range cm.tests {
		sem := newSemaphore(stopCh)
		sems = append(sems, sem)
		go func() {
			defer ginkgo.GinkgoRecover()
			defer sem.done()
			test(ctx, sem)
		}()
	}

	fmt.Println("Waiting for all async tests to be ready")
	for _, sem := range sems {
		// 等待测试就绪。必须同时等待 ready 或 done，因为测试可能在报告就绪前 panic，
		// 此时不能一直阻塞。上面已 defer sem.done()，因此测试 panic 时也会被标记为 done。
		sem.waitForReadyOrDone()
	}

	defer func() {
		close(stopCh)
		fmt.Println("Waiting for async validations to complete")
		for _, sem := range sems {
			sem.waitForDone()
		}
	}()

	fmt.Println("Starting disruption")
	cm.disruption(ctx)
	fmt.Println("Disruption complete; stopping async validations")
}

// Semaphore 由 Test 使用，提供 Ready()（测试准备好开始故障注入时调用）和 StopCh
// （关闭时表示故障注入已结束）。
type Semaphore struct {
	readyCh chan struct{}
	StopCh  <-chan struct{}
	doneCh  chan struct{}
}

func newSemaphore(stopCh <-chan struct{}) *Semaphore {
	// 避免在 Ready() 或 done() 上阻塞
	return &Semaphore{
		make(chan struct{}, 1),
		stopCh,
		make(chan struct{}, 1),
	}
}

// Test 调用 Ready 表示已准备好开始故障注入。
func (sem *Semaphore) Ready() {
	close(sem.readyCh)
}

// done 是供 defer 调用的内部方法，既用于等待所有测试返回，也用于感知测试在调用
// Ready 之前是否 panic。见 waitForReadyOrDone。
func (sem *Semaphore) done() {
	close(sem.doneCh)
}

// 理想情况下只需检查所有测试是否就绪，但测试失败时（Ginkgo 以 panic 实现）可能未调用
// Ready()。因此同时检查 done：若函数已返回，就不必关心是否就绪，直接继续。
func (sem *Semaphore) waitForReadyOrDone() {
	select {
	case <-sem.readyCh:
	case <-sem.doneCh:
	}
}

// waitForDone 是用于等待所有 Test 返回的内部方法。
func (sem *Semaphore) waitForDone() {
	<-sem.doneCh
}
