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

// Package types 定义仅由卷组件使用的类型。
package types

import (
	"errors"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/mount-utils"
)

var (
	// NodeExpansionNotRequired 是标注节点扩张非必需的注解键。
	NodeExpansionNotRequired = "volume.kubernetes.io/node-expansion-not-required"
)

// UniquePodName 定义用于标识 Pod 的键类型。
type UniquePodName types.UID

// UniquePVCName 定义用于标识 PVC 的键类型。
type UniquePVCName types.UID

// GeneratedOperations 包含待执行的操作及 operation executor 所需的辅助函数。
type GeneratedOperations struct {
	// 操作名称，可用于重置共享的指数退避。
	OperationName     string
	OperationFunc     func() (context OperationContext)
	EventRecorderFunc func(*error)
	CompleteFunc      func(CompleteFuncParam)
}

// OperationContext 承载操作执行的结果，包括事件错误、详细错误及迁移标记。
type OperationContext struct {
	EventErr    error
	DetailedErr error
	Migrated    bool
}

// NewOperationContext 构造一个 OperationContext。
func NewOperationContext(eventErr, detailedErr error, migrated bool) OperationContext {
	return OperationContext{
		EventErr:    eventErr,
		DetailedErr: detailedErr,
		Migrated:    migrated,
	}
}

// CompleteFuncParam 在 GeneratedOperations 完成时回传错误与迁移标记。
type CompleteFuncParam struct {
	Err      *error
	Migrated *bool
}

// Run 执行操作及其辅助函数。
func (o *GeneratedOperations) Run() (eventErr, detailedErr error) {
	var context OperationContext
	if o.CompleteFunc != nil {
		c := CompleteFuncParam{
			Err:      &context.DetailedErr,
			Migrated: &context.Migrated,
		}
		defer o.CompleteFunc(c)
	}
	if o.EventRecorderFunc != nil {
		defer o.EventRecorderFunc(&eventErr)
	}
	// 捕获 OperationFunc 中的 panic，避免整个 kubelet 崩溃。
	defer runtime.RecoverFromPanic(&detailedErr)

	context = o.OperationFunc()
	return context.EventErr, context.DetailedErr
}

// FailedPrecondition 表示 CSI 操作返回了 failed precondition 错误。
type FailedPrecondition struct {
	msg string
}

// Error 返回错误信息。
func (err *FailedPrecondition) Error() string {
	return err.msg
}

// NewFailedPreconditionError 创建一个 FailedPrecondition 错误实例。
func NewFailedPreconditionError(msg string) *FailedPrecondition {
	return &FailedPrecondition{msg: msg}
}

// IsFailedPreconditionError 判断给定错误是否表示操作因前置条件不满足而失败。
func IsFailedPreconditionError(err error) bool {
	var failedPreconditionError *FailedPrecondition
	return errors.As(err, &failedPreconditionError)
}

// InfeasibleError 属于 OperationFinished（最终错误）的子集。对 CSI 而言，通常表示在当前状态和参数下操作不可行。
type InfeasibleError struct {
	msg string
}

// Error 返回错误信息。
func (err *InfeasibleError) Error() string {
	return err.msg
}

// NewInfeasibleError 创建一个 InfeasibleError 实例。
func NewInfeasibleError(msg string) *InfeasibleError {
	return &InfeasibleError{msg: msg}
}

// IsInfeasibleError 判断给定错误是否为 InfeasibleError。
func IsInfeasibleError(err error) bool {
	var infeasibleError *InfeasibleError
	return errors.As(err, &infeasibleError)
}

// OperationNotSupported 表示请求的操作不受支持。
type OperationNotSupported struct {
	msg string
}

// Error 返回错误信息。
func (err *OperationNotSupported) Error() string {
	return err.msg
}

// NewOperationNotSupportedError 创建一个 OperationNotSupported 错误实例。
func NewOperationNotSupportedError(msg string) *OperationNotSupported {
	return &OperationNotSupported{msg: msg}
}

// IsOperationNotSupportedError 判断给定错误是否为 OperationNotSupported。
func IsOperationNotSupportedError(err error) bool {
	var operationNotSupportedError *OperationNotSupported
	return errors.As(err, &operationNotSupportedError)
}

// TransientOperationFailure 表示操作因临时错误失败，重试后可能自行恢复。
type TransientOperationFailure struct {
	msg string
}

// Error 返回错误信息。
func (err *TransientOperationFailure) Error() string {
	return err.msg
}

// NewTransientOperationFailure 创建一个 TransientOperationFailure 错误实例。
func NewTransientOperationFailure(msg string) *TransientOperationFailure {
	return &TransientOperationFailure{msg: msg}
}

// UncertainProgressError 表示操作因非最终错误失败，且操作可能仍在后台进行。
type UncertainProgressError struct {
	msg string
}

// Error 返回错误信息。
func (err *UncertainProgressError) Error() string {
	return err.msg
}

// NewUncertainProgressError 创建一个 UncertainProgressError 实例。
func NewUncertainProgressError(msg string) *UncertainProgressError {
	return &UncertainProgressError{msg: msg}
}

// IsOperationFinishedError 判断给定错误是否表示操作已以最终错误结束。
func IsOperationFinishedError(err error) bool {
	if _, ok := err.(*UncertainProgressError); ok {
		return false
	}
	if _, ok := err.(*TransientOperationFailure); ok {
		return false
	}
	return true
}

// IsFilesystemMismatchError 判断挂载失败是否因 PVC 请求的文件系统与磁盘实际文件系统不一致。
func IsFilesystemMismatchError(err error) bool {
	mountError := mount.MountError{}
	return errors.As(err, &mountError) && mountError.Type == mount.FilesystemMismatch
}

// IsUncertainProgressError 判断给定错误是否表示操作可能仍在后台进行。
func IsUncertainProgressError(err error) bool {
	if _, ok := err.(*UncertainProgressError); ok {
		return true
	}
	return false
}

const (
	// VolumeResizerKey 是用于记录负责 PVC 扩容的 resizer 的注解键。生成的键值对会作为注解添加到 PVC 上。
	VolumeResizerKey = "volume.kubernetes.io/storage-resizer"
)
