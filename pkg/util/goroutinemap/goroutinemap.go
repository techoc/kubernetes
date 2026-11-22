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

/*
Package goroutinemap 提供按名称管理 goroutine 的数据结构。
当同名的 goroutine 已存在时，阻止创建新的 goroutine。
*/
package goroutinemap

import (
	"fmt"
	"sync"

	k8sRuntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
)

// GoRoutineMap 管理具名 goroutine 的运行与状态。
// 它禁止同名 goroutine 并行存在，并可在上一个同名 goroutine 结束后的一段退避时间内阻止重建。
type GoRoutineMap interface {
	// Run 将 operationName 加入运行列表，并启动一个新的 goroutine 执行操作。
	// 若同名操作已存在，返回 AlreadyExists 或 ExponentialBackoff 错误。
	// 操作完成后，goroutine 退出，名称从执行列表移除，
	// 此后可用同一名称无错地启动新操作。
	Run(operationName string, operationFunc func() error) error

	// Wait 阻塞直到操作映射为空。测试通常需要等待所有操作结束后再检查结果。
	Wait()

	// WaitForCompletion 阻塞直到所有操作要么成功完成，要么已失败但不再处于待处理状态。
	WaitForCompletion()

	// IsOperationPending 判断操作是否正在运行。
	IsOperationPending(operationName string) bool
}

// NewGoRoutineMap 创建一个新的 GoRoutineMap 实例。
// exponentialBackOffOnError 为 true 时，操作失败后重建同名操作需要指数退避。
func NewGoRoutineMap(exponentialBackOffOnError bool) GoRoutineMap {
	g := &goRoutineMap{
		operations:                make(map[string]operation),
		exponentialBackOffOnError: exponentialBackOffOnError,
	}

	g.cond = sync.NewCond(&g.lock)
	return g
}

// goRoutineMap 是 GoRoutineMap 的并发安全实现，
// 通过 cond 在操作完成时唤醒等待的 Wait/WaitForCompletion 调用方。
type goRoutineMap struct {
	operations                map[string]operation
	exponentialBackOffOnError bool
	cond                      *sync.Cond
	lock                      sync.RWMutex
}

// operation 记录单个具名 goroutine 的状态。
type operation struct {
	operationPending bool
	expBackoff       exponentialbackoff.ExponentialBackoff
}

// Run 实现 GoRoutineMap.Run。
func (grm *goRoutineMap) Run(
	operationName string,
	operationFunc func() error) error {
	grm.lock.Lock()
	defer grm.lock.Unlock()

	existingOp, exists := grm.operations[operationName]
	if exists {
		// 同名操作已存在，需要判断是否正在运行或处于退避期。
		if existingOp.operationPending {
			return NewAlreadyExistsError(operationName)
		}

		if err := existingOp.expBackoff.SafeToRetry(operationName); err != nil {
			return err
		}
	}

	grm.operations[operationName] = operation{
		operationPending: true,
		expBackoff:       existingOp.expBackoff,
	}
	go func() (err error) {
		// 三个 defer 按 LIFO 执行：先把 panic 转成错误，
		// 再记录完成状态，最后由 HandleCrash 兜底极端情况。
		// 处理未捕获的 panic（极少发生）
		defer k8sRuntime.HandleCrash()
		// 记录 operationFunc() 的完成状态及其错误（如有）
		defer grm.operationComplete(operationName, &err)
		// 捕获 operationFunc() 的 panic（如有）并转成错误
		defer k8sRuntime.RecoverFromPanic(&err)
		return operationFunc()
	}()

	return nil
}

// operationComplete 处理 goRoutineMap 中一个 goroutine 的完成：
// 成功时移除操作记录，失败且启用退避时保留记录并更新退避状态。
func (grm *goRoutineMap) operationComplete(
	operationName string, err *error) {
	// defer 按后进先出执行：先获取锁，方法结束时先释放锁再 Signal，
	// 避免唤醒等待者时它因拿不到锁而立即再次阻塞。
	defer grm.cond.Signal()
	grm.lock.Lock()
	defer grm.lock.Unlock()

	if *err == nil || !grm.exponentialBackOffOnError {
		// 操作成功完成，或未启用失败退避：直接移除记录。
		delete(grm.operations, operationName)
		if *err != nil {
			// 仍需记录失败原因，便于排查。
			klog.Errorf("operation for %q failed with: %v",
				operationName,
				*err)
		}
	} else {
		// 操作失败且启用了退避：保留记录，阻止同名操作立即重建。
		existingOp := grm.operations[operationName]
		existingOp.expBackoff.Update(err)
		existingOp.operationPending = false
		grm.operations[operationName] = existingOp

		// 记录退避信息，说明何时允许重建操作。
		klog.Errorf("%v",
			existingOp.expBackoff.GenerateNoRetriesPermittedMsg(operationName))
	}
}

// IsOperationPending 实现 GoRoutineMap.IsOperationPending。
func (grm *goRoutineMap) IsOperationPending(operationName string) bool {
	grm.lock.RLock()
	defer grm.lock.RUnlock()
	existingOp, exists := grm.operations[operationName]
	if exists && existingOp.operationPending {
		return true
	}
	return false
}

// Wait 实现 GoRoutineMap.Wait。
func (grm *goRoutineMap) Wait() {
	grm.lock.Lock()
	defer grm.lock.Unlock()

	for len(grm.operations) > 0 {
		grm.cond.Wait()
	}
}

// WaitForCompletion 实现 GoRoutineMap.WaitForCompletion。
func (grm *goRoutineMap) WaitForCompletion() {
	grm.lock.Lock()
	defer grm.lock.Unlock()

	for {
		if len(grm.operations) == 0 || grm.nothingPending() {
			break
		} else {
			grm.cond.Wait()
		}
	}
}

// nothingPending 判断是否没有待处理操作。
// 调用方必须已持有必要的锁。
func (grm *goRoutineMap) nothingPending() bool {
	nothingIsPending := true
	for _, operation := range grm.operations {
		if operation.operationPending {
			nothingIsPending = false
			break
		}
	}
	return nothingIsPending
}

// NewAlreadyExistsError 返回一个 AlreadyExists 错误实例。
func NewAlreadyExistsError(operationName string) error {
	return alreadyExistsError{operationName}
}

// IsAlreadyExists 判断 GoRoutineMap 返回的错误是否表示：
// 同名操作正在执行，无法启动新操作。
func IsAlreadyExists(err error) bool {
	switch err.(type) {
	case alreadyExistsError:
		return true
	default:
		return false
	}
}

// alreadyExistsError 是 GoRoutineMap 在同名操作已在执行、
// 无法启动新操作时返回的错误。
type alreadyExistsError struct {
	operationName string
}

var _ error = alreadyExistsError{}

// Error 实现 error 接口，返回描述同名冲突的错误信息。
func (err alreadyExistsError) Error() string {
	return fmt.Sprintf(
		"Failed to create operation with name %q. An operation with that name is already executing.",
		err.operationName)
}
