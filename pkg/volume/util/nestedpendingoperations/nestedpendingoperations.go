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
Package nestedpendingoperations 是 pkg/util/goroutinemap 的改造版本。
它提供按卷名/Pod 名管理 goroutine 的数据结构：若该卷已有 goroutine 在运行，则不再创建新的；
同时允许同一卷上的多个操作在不同 Pod 上并行执行。
*/
package nestedpendingoperations

import (
	"fmt"
	"sync"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	k8sRuntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	volumetypes "k8s.io/kubernetes/pkg/volume/util/types"
)

const (
	// EmptyUniquePodName 表示空字符串的 UniquePodName。
	EmptyUniquePodName volumetypes.UniquePodName = volumetypes.UniquePodName("")

	// EmptyUniqueVolumeName 表示空字符串的 UniqueVolumeName。
	EmptyUniqueVolumeName v1.UniqueVolumeName = v1.UniqueVolumeName("")

	// EmptyNodeName 表示空字符串的 NodeName。
	EmptyNodeName types.NodeName = types.NodeName("")
)

// NestedPendingOperations 定义所支持的操作集合。
type NestedPendingOperations interface {

	// Run 把 volumeName、podName 和 nodeName 的组合加入运行中的操作列表，
	// 并启动一个新的 goroutine 执行 generatedOperations。

	// volumeName、podName 和 nodeName 共同构成操作键。支持以下键形式
	// （需要串行化两个键的操作时，称这两个键“匹配”）：
	// - volumeName 为空，podName 和 nodeName 任意：
	//   该键不与任何键匹配。
	// - volumeName 非空，podName 为空，nodeName 为空：
	//   该键与所有 volumeName 相同的键匹配。
	// - volumeName 非空，podName 非空，nodeName 为空：
	//   该键与以下键匹配：
	//   - volumeName 和 podName 均相同
	//   - volumeName 相同但 podName 为空
	// - volumeName 非空，podName 为空，nodeName 非空：
	//   该键与以下键匹配：
	//   - volumeName 和 nodeName 均相同
	//   - volumeName 相同但 nodeName 为空

	// 若不存在匹配的键，则允许操作继续。
	// 若存在匹配的键且前一个操作仍在运行，则返回 AlreadyExists 错误。
	// 若存在匹配的键且前一个操作失败：
	// - 若前一个操作与 generatedOperations.operationName 同名：
	//   - 已满足完整的指数退避周期时，允许操作继续；
	//   - 否则返回 ExponentialBackoff 错误。
	// - 否则重置指数退避，允许操作继续。

	// 操作完成后 goroutine 即终止。若操作成功，其键会从执行中的操作列表移除，
	// 之后可用该键无错地启动新操作；若失败，键会保留并更新指数退避状态。
	Run(
		volumeName v1.UniqueVolumeName,
		podName volumetypes.UniquePodName,
		nodeName types.NodeName,
		generatedOperations volumetypes.GeneratedOperations) error

	// Wait 阻塞直到所有操作完成。测试通常需要调用它：
	// 等待全部操作结束后再断言结果。
	Wait()

	// IsOperationPending 若给定 volumeName 与 podName 或 nodeName 的操作仍在进行则返回 true，
	// 否则返回 false。
	IsOperationPending(
		volumeName v1.UniqueVolumeName,
		podName volumetypes.UniquePodName,
		nodeName types.NodeName) bool

	// IsOperationSafeToRetry 若给定 volumeName 与 podName 或 nodeName 的操作正在进行
	// 或处于指数退避中则返回 false，否则返回 true。
	IsOperationSafeToRetry(
		volumeName v1.UniqueVolumeName,
		podName volumetypes.UniquePodName,
		nodeName types.NodeName, operationName string) bool
}

// NewNestedPendingOperations 返回一个新的 NestedPendingOperations 实例。
func NewNestedPendingOperations(exponentialBackOffOnError bool) NestedPendingOperations {
	g := &nestedPendingOperations{
		operations:                []operation{},
		exponentialBackOffOnError: exponentialBackOffOnError,
	}
	g.cond = sync.NewCond(&g.lock)
	return g
}

// nestedPendingOperations 维护操作列表及条件变量，实现操作的串行化与指数退避。
type nestedPendingOperations struct {
	operations                []operation
	exponentialBackOffOnError bool
	cond                      *sync.Cond
	lock                      sync.RWMutex
}

// operation 记录一个操作键的挂起状态、名称与退避信息。
type operation struct {
	key              operationKey
	operationName    string
	operationPending bool
	expBackoff       exponentialbackoff.ExponentialBackoff
}

// Run 登记操作并启动 goroutine；同名操作未结束时按退避规则拒绝或放行。
func (grm *nestedPendingOperations) Run(
	volumeName v1.UniqueVolumeName,
	podName volumetypes.UniquePodName,
	nodeName types.NodeName,
	generatedOperations volumetypes.GeneratedOperations) error {
	grm.lock.Lock()
	defer grm.lock.Unlock()

	opKey := operationKey{volumeName, podName, nodeName}

	opExists, previousOpIndex := grm.isOperationExists(opKey)
	if opExists {
		previousOp := grm.operations[previousOpIndex]
		// 已有相同键的操作记录。
		if previousOp.operationPending {
			// 上一个操作尚未完成，不能并发启动。
			return NewAlreadyExistsError(opKey)
		}

		backOffErr := previousOp.expBackoff.SafeToRetry(fmt.Sprintf("%+v", opKey))
		if backOffErr != nil {
			if previousOp.operationName == generatedOperations.OperationName {
				return backOffErr
			}
			// 前后是两个不同操作，退避状态不应沿用。
			grm.operations[previousOpIndex].operationName = generatedOperations.OperationName
			grm.operations[previousOpIndex].expBackoff = exponentialbackoff.ExponentialBackoff{}
		}

		// 复用已有记录，标记为挂起。
		grm.operations[previousOpIndex].operationPending = true
		grm.operations[previousOpIndex].key = opKey
	} else {
		// 首次出现该键，新建记录。
		grm.operations = append(grm.operations,
			operation{
				key:              opKey,
				operationPending: true,
				operationName:    generatedOperations.OperationName,
				expBackoff:       exponentialbackoff.ExponentialBackoff{},
			})
	}

	go func() (eventErr, detailedErr error) {
		// 兜底处理逃逸的 panic（概率很低）。
		defer k8sRuntime.HandleCrash()
		// 无论成功失败都要清理挂起状态，否则该键会被永久占用。
		defer grm.operationComplete(opKey, &detailedErr)
		return generatedOperations.Run()
	}()

	return nil
}

// IsOperationSafeToRetry 判断同名操作是否可以安全重试。
func (grm *nestedPendingOperations) IsOperationSafeToRetry(
	volumeName v1.UniqueVolumeName,
	podName volumetypes.UniquePodName,
	nodeName types.NodeName,
	operationName string) bool {

	grm.lock.RLock()
	defer grm.lock.RUnlock()

	opKey := operationKey{volumeName, podName, nodeName}
	exist, previousOpIndex := grm.isOperationExists(opKey)
	if !exist {
		return true
	}
	previousOp := grm.operations[previousOpIndex]
	if previousOp.operationPending {
		return false
	}
	backOffErr := previousOp.expBackoff.SafeToRetry(fmt.Sprintf("%+v", opKey))
	if backOffErr != nil {
		if previousOp.operationName == operationName {
			return false
		}
	}

	return true
}

// IsOperationPending 判断匹配的操作是否仍在执行。
func (grm *nestedPendingOperations) IsOperationPending(
	volumeName v1.UniqueVolumeName,
	podName volumetypes.UniquePodName,
	nodeName types.NodeName) bool {

	grm.lock.RLock()
	defer grm.lock.RUnlock()

	opKey := operationKey{volumeName, podName, nodeName}
	exist, previousOpIndex := grm.isOperationExists(opKey)
	if exist && grm.operations[previousOpIndex].operationPending {
		return true
	}
	return false
}

// isOperationExists 是内部函数，调用方需自行加锁/解锁。
func (grm *nestedPendingOperations) isOperationExists(key operationKey) (bool, int) {

	// volumeName 为空表示操作互不关联，可以并发执行。
	if key.volumeName == EmptyUniqueVolumeName {
		return false, -1
	}

	opIndex := -1
	for previousOpIndex, previousOp := range grm.operations {
		volumeNameMatch := previousOp.key.volumeName == key.volumeName

		podNameMatch := previousOp.key.podName == EmptyUniquePodName ||
			key.podName == EmptyUniquePodName ||
			previousOp.key.podName == key.podName

		podNameExactMatch := previousOp.key.podName == key.podName

		nodeNameMatch := previousOp.key.nodeName == EmptyNodeName ||
			key.nodeName == EmptyNodeName ||
			previousOp.key.nodeName == key.nodeName

		nodeNameExactMatch := previousOp.key.nodeName == key.nodeName

		if volumeNameMatch && podNameMatch && nodeNameMatch {
			// 挂起的非精确匹配优先：已有操作在跑就必须串行化。
			if previousOp.operationPending {
				return true, previousOpIndex
			}
			// 无挂起时先记下第一个非精确匹配；精确匹配可覆盖它。
			if opIndex == -1 || (podNameExactMatch && nodeNameExactMatch) {
				opIndex = previousOpIndex
			}
		}
	}
	return opIndex != -1, opIndex

}

// getOperation 查找键完全相同的操作，返回其下标。
func (grm *nestedPendingOperations) getOperation(key operationKey) (uint, error) {
	// 假定调用方已加锁。

	for i, op := range grm.operations {
		if op.key.volumeName == key.volumeName &&
			op.key.podName == key.podName &&
			op.key.nodeName == key.nodeName {
			return uint(i), nil
		}
	}

	return 0, fmt.Errorf("operation %+v not found", key)
}

// deleteOperation 删除键完全相同的操作记录。
func (grm *nestedPendingOperations) deleteOperation(key operationKey) {
	// 假定调用方已加锁。

	opIndex := -1
	for i, op := range grm.operations {
		if op.key.volumeName == key.volumeName &&
			op.key.podName == key.podName &&
			op.key.nodeName == key.nodeName {
			opIndex = i
			break
		}
	}

	if opIndex < 0 {
		return
	}

	// 用末尾元素覆盖待删元素即可，列表本身无序。
	grm.operations[opIndex] = grm.operations[len(grm.operations)-1]
	grm.operations = grm.operations[:len(grm.operations)-1]
}

// operationComplete 在操作结束时清理挂起标记；失败时按配置保留键并更新退避状态。
func (grm *nestedPendingOperations) operationComplete(key operationKey, err *error) {
	// defer 后进先出：Signal 在锁释放之后执行，才能唤醒 Wait 中的 goroutine。
	defer grm.cond.Signal()
	grm.lock.Lock()
	defer grm.lock.Unlock()

	if *err == nil || !grm.exponentialBackOffOnError {
		// 操作成功，或未启用失败退避：直接删除记录。
		grm.deleteOperation(key)
		if *err != nil {
			// 记录被忽略的错误，便于排查。
			klog.Errorf("operation %+v failed with: %v", key, *err)
		}
		return
	}

	// 操作失败且启用了退避：保留记录以限制重试频率。
	existingOpIndex, getOpErr := grm.getOperation(key)
	if getOpErr != nil {
		// 记录异常：需要更新退避却找不到原操作。
		klog.Errorf("Operation %+v completed. error: %v. exponentialBackOffOnError is enabled, but failed to get operation to update.",
			key,
			*err)
		return
	}

	grm.operations[existingOpIndex].expBackoff.Update(err)
	grm.operations[existingOpIndex].operationPending = false

	// 输出禁止重试的剩余时间，方便判断何时可重试。
	klog.Errorf("%v", grm.operations[existingOpIndex].expBackoff.
		GenerateNoRetriesPermittedMsg(fmt.Sprintf("%+v", key)))
}

// Wait 阻塞直到操作列表清空。
func (grm *nestedPendingOperations) Wait() {
	grm.lock.Lock()
	defer grm.lock.Unlock()

	for len(grm.operations) > 0 {
		grm.cond.Wait()
	}
}

// operationKey 由卷名、Pod 名和节点名组成，决定操作的匹配与串行化范围。
type operationKey struct {
	volumeName v1.UniqueVolumeName
	podName    volumetypes.UniquePodName
	nodeName   types.NodeName
}

// NewAlreadyExistsError 返回一个 AlreadyExists 错误实例。
func NewAlreadyExistsError(key operationKey) error {
	return alreadyExistsError{key}
}

// IsAlreadyExists 判断错误是否表示同名操作已在执行，因而无法启动新操作。
func IsAlreadyExists(err error) bool {
	switch err.(type) {
	case alreadyExistsError:
		return true
	default:
		return false
	}
}

// alreadyExistsError 表示因同名操作已在执行而无法启动新操作。
type alreadyExistsError struct {
	operationKey operationKey
}

var _ error = alreadyExistsError{}

// Error 返回错误描述。
func (err alreadyExistsError) Error() string {
	return fmt.Sprintf(
		"Failed to create operation with name %+v. An operation with that name is already executing.",
		err.operationKey)
}
