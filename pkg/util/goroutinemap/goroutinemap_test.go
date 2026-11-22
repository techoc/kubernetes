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

package goroutinemap

import (
	"fmt"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	// testTimeout 是等待 goroutine 完成的超时时间。理论上只是一次“上下文切换”，
	// 几毫秒就够，但 Clayton 提醒过：曾有人假设 15s 足够而遇到测试不稳定。
	testTimeout time.Duration = 1 * time.Minute

	// initialOperationWaitTimeShort 是测试等待操作完成的初始时间，
	// 每次失败后按指数退避递增。
	initialOperationWaitTimeShort time.Duration = 20 * time.Millisecond

	// initialOperationWaitTimeLong 是测试等待操作完成的初始时间（较长），
	// 每次失败后按指数退避递增。
	initialOperationWaitTimeLong time.Duration = 500 * time.Millisecond
)

// Test_NewGoRoutineMap_Positive_SingleOp 验证单个操作能成功启动并完成。
func Test_NewGoRoutineMap_Positive_SingleOp(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation := func() error { return nil }

	// 执行
	err := grm.Run(operationName, operation)

	// 断言
	if err != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err)
	}
}

// Test_NewGoRoutineMap_Positive_TwoOps 验证两个不同名的操作能同时成功启动。
func Test_NewGoRoutineMap_Positive_TwoOps(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operation1Name := "operation1-name"
	operation2Name := "operation2-name"
	operation := func() error { return nil }

	// 执行
	err1 := grm.Run(operation1Name, operation)
	err2 := grm.Run(operation2Name, operation)

	// 断言
	if err1 != nil {
		t.Fatalf("NewGoRoutine %q failed. Expected: <no error> Actual: <%v>", operation1Name, err1)
	}

	if err2 != nil {
		t.Fatalf("NewGoRoutine %q failed. Expected: <no error> Actual: <%v>", operation2Name, err2)
	}
}

// Test_NewGoRoutineMap_Positive_SingleOpWithExpBackoff 验证启用指数退避时，单个操作仍能成功启动并完成。
func Test_NewGoRoutineMap_Positive_SingleOpWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation := func() error { return nil }

	// 执行
	err := grm.Run(operationName, operation)

	// 断言
	if err != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err)
	}
}

// Test_NewGoRoutineMap_Positive_SecondOpAfterFirstCompletes 验证第一个操作完成后，可用同名启动第二个操作。
func Test_NewGoRoutineMap_Positive_SecondOpAfterFirstCompletes(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateCallbackFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()
	<-operation1DoneCh // 让 operation1 完成，释放同名限制

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(operationName, operation2)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NewGoRoutineMap_Positive_SecondOpAfterFirstCompletesWithExpBackoff 验证启用指数退避时，第一个操作完成后同名操作仍可重建。
func Test_NewGoRoutineMap_Positive_SecondOpAfterFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateCallbackFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()
	<-operation1DoneCh // 让 operation1 完成，释放同名限制

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(operationName, operation2)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NewGoRoutineMap_Positive_SecondOpAfterFirstPanics 验证第一个操作 panic 后，同名操作仍可重建。
func Test_NewGoRoutineMap_Positive_SecondOpAfterFirstPanics(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1 := generatePanicFunc()
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(operationName, operation2)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NewGoRoutineMap_Positive_SecondOpAfterFirstPanicsWithExpBackoff 验证启用指数退避时，第一个操作 panic 后同名操作仍可重建。
func Test_NewGoRoutineMap_Positive_SecondOpAfterFirstPanicsWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1 := generatePanicFunc()
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()

	// 执行
	err2 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeLong), // 退避会拉长等待时间，因此初始值更大
		func() (bool, error) {
			err := grm.Run(operationName, operation2)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err2 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err2)
	}
}

// Test_NewGoRoutineMap_Negative_SecondOpBeforeFirstCompletes 验证第一个操作未完成时，同名操作会收到 AlreadyExists 错误。
func Test_NewGoRoutineMap_Negative_SecondOpBeforeFirstCompletes(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()

	// 执行
	err2 := grm.Run(operationName, operation2)

	// 断言
	if err2 == nil {
		t.Fatalf("NewGoRoutine did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", operationName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NewGoRoutine did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NewGoRoutineMap_Negative_SecondOpBeforeFirstCompletesWithExpBackoff 验证启用指数退避时，同名操作在运行中仍会收到 AlreadyExists 错误。
func Test_NewGoRoutineMap_Negative_SecondOpBeforeFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()

	// 执行
	err2 := grm.Run(operationName, operation2)

	// 断言
	if err2 == nil {
		t.Fatalf("NewGoRoutine did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", operationName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NewGoRoutine did not return alreadyExistsError, got: %v", err2)
	}
}

// Test_NewGoRoutineMap_Positive_ThirdOpAfterFirstCompletes 验证第二个操作被拒绝、第一个操作完成后，第三个同名操作能启动。
func Test_NewGoRoutineMap_Positive_ThirdOpAfterFirstCompletes(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()
	operation3 := generateNoopFunc()

	// 执行
	err2 := grm.Run(operationName, operation2)

	// 断言
	if err2 == nil {
		t.Fatalf("NewGoRoutine did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", operationName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NewGoRoutine did not return alreadyExistsError, got: %v", err2)
	}

	// 执行
	operation1DoneCh <- true // 让 operation1 完成，释放同名限制
	err3 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(operationName, operation3)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err3 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err3)
	}
}

// Test_NewGoRoutineMap_Positive_ThirdOpAfterFirstCompletesWithExpBackoff 验证启用指数退避时，第三个同名操作在第一个完成后能启动。
func Test_NewGoRoutineMap_Positive_ThirdOpAfterFirstCompletesWithExpBackoff(t *testing.T) {
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err1 := grm.Run(operationName, operation1)
	if err1 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err1)
	}
	operation2 := generateNoopFunc()
	operation3 := generateNoopFunc()

	// 执行
	err2 := grm.Run(operationName, operation2)

	// 断言
	if err2 == nil {
		t.Fatalf("NewGoRoutine did not fail. Expected: <Failed to create operation with name \"%s\". An operation with that name already exists.> Actual: <no error>", operationName)
	}
	if !IsAlreadyExists(err2) {
		t.Fatalf("NewGoRoutine did not return alreadyExistsError, got: %v", err2)
	}

	// 执行
	operation1DoneCh <- true // 让 operation1 完成，释放同名限制
	err3 := retryWithExponentialBackOff(
		time.Duration(initialOperationWaitTimeShort),
		func() (bool, error) {
			err := grm.Run(operationName, operation3)
			if err != nil {
				t.Logf("Warning: NewGoRoutine failed with %v. Will retry.", err)
				return false, nil
			}
			return true, nil
		},
	)

	// 断言
	if err3 != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err3)
	}
}

// Test_NewGoRoutineMap_Positive_WaitEmpty 验证空映射上 Wait() 不阻塞。
func Test_NewGoRoutineMap_Positive_WaitEmpty(t *testing.T) {
	// 验证空 GoRoutineMap 上 Wait() 能立即返回，不会阻塞
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 断言
	err := waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Errorf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NewGoRoutineMap_Positive_WaitEmptyWithExpBackoff 验证启用指数退避时，空映射上 Wait() 不阻塞。
func Test_NewGoRoutineMap_Positive_WaitEmptyWithExpBackoff(t *testing.T) {
	// 验证空 GoRoutineMap 上 Wait() 能立即返回，不会阻塞
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 断言
	err := waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Errorf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NewGoRoutineMap_Positive_Wait 验证 Wait() 会阻塞到最后一个操作完成。
func Test_NewGoRoutineMap_Positive_Wait(t *testing.T) {
	// 验证 Wait() 会一直阻塞到最后一个操作完成
	// 准备
	grm := NewGoRoutineMap(false /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err := grm.Run(operationName, operation1)
	if err != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 让操作结束
	operation1DoneCh <- true

	// 断言
	err = waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Fatalf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NewGoRoutineMap_Positive_WaitWithExpBackoff 验证启用指数退避时，Wait() 同样会阻塞到操作完成。
func Test_NewGoRoutineMap_Positive_WaitWithExpBackoff(t *testing.T) {
	// 验证 Wait() 会一直阻塞到最后一个操作完成
	// 准备
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-name"
	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateWaitFunc(operation1DoneCh)
	err := grm.Run(operationName, operation1)
	if err != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.Wait()
		waitDoneCh <- true
	}()

	// 让操作结束
	operation1DoneCh <- true

	// 断言
	err = waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Fatalf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// Test_NewGoRoutineMap_WaitForCompletionWithExpBackoff 验证启用指数退避时，操作报错后 WaitForCompletion 仍会返回。
func Test_NewGoRoutineMap_WaitForCompletionWithExpBackoff(t *testing.T) {
	grm := NewGoRoutineMap(true /* 出错时指数退避 */)
	operationName := "operation-err"

	operation1DoneCh := make(chan interface{}, 0 /* 缓冲区大小 */)
	operation1 := generateErrorFunc(operation1DoneCh)
	err := grm.Run(operationName, operation1)
	if err != nil {
		t.Fatalf("NewGoRoutine failed. Expected: <no error> Actual: <%v>", err)
	}

	// 执行
	waitDoneCh := make(chan interface{}, 1)
	go func() {
		grm.WaitForCompletion()
		waitDoneCh <- true
	}()

	// 让操作结束
	operation1DoneCh <- true

	// 断言即使操作报错，WaitForCompletion 也会返回
	err = waitChannelWithTimeout(waitDoneCh, testTimeout)
	if err != nil {
		t.Fatalf("Error waiting for GoRoutineMap.Wait: %v", err)
	}
}

// generateCallbackFunc 生成一个完成时向 done 通道发信号的操作函数。
func generateCallbackFunc(done chan<- interface{}) func() error {
	return func() error {
		done <- true
		return nil
	}
}

// generateErrorFunc 生成一个等待 done 通道后返回错误的函数。
func generateErrorFunc(done <-chan interface{}) func() error {
	return func() error {
		<-done
		return fmt.Errorf("Generic error")
	}
}

// generateWaitFunc 生成一个一直等待 done 通道的函数，用于模拟运行中的操作。
func generateWaitFunc(done <-chan interface{}) func() error {
	return func() error {
		<-done
		return nil
	}
}

// generatePanicFunc 生成一个必定 panic 的函数。
func generatePanicFunc() func() error {
	return func() error {
		panic("testing panic")
	}
}

// generateNoopFunc 生成一个立即返回 nil 的空操作函数。
func generateNoopFunc() func() error {
	return func() error { return nil }
}

// retryWithExponentialBackOff 以指数退避方式重试 fn，直到返回 true 或用尽重试次数。
func retryWithExponentialBackOff(initialDuration time.Duration, fn wait.ConditionFunc) error {
	backoff := wait.Backoff{
		Duration: initialDuration,
		Factor:   3,
		Jitter:   0,
		Steps:    4,
	}
	return wait.ExponentialBackoff(backoff, fn)
}

// waitChannelWithTimeout 在指定超时时间内等待通道信号，超时返回错误。
func waitChannelWithTimeout(ch <-chan interface{}, timeout time.Duration) error {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ch:
		// 通道收到信号，等待成功
		return nil
	case <-timer.C:
		return fmt.Errorf("timeout after %v", timeout)
	}
}
