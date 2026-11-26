/*
Copyright 2017 The Kubernetes Authors.

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

// Package errors 定义云提供商卷操作中可被调用方识别的错误类型。
// 这些错误用于在重试/删除等路径上区分可恢复与不可恢复情况。
package errors

import (
	k8stypes "k8s.io/apimachinery/pkg/types"
)

// NewDeletedVolumeInUseError 返回一个新的 DeletedVolumeInUseError 实例。
// 表示卷已被删除但仍有挂载引用，调用方通常应继续等待而不是立即失败。
func NewDeletedVolumeInUseError(message string) error {
	return deletedVolumeInUseError(message)
}

// deletedVolumeInUseError 使用字符串底层类型，方便直接携带错误消息且不可变。
type deletedVolumeInUseError string

// 编译期断言：确保该类型实现 error 接口。
var _ error = deletedVolumeInUseError("")

// IsDeletedVolumeInUse 判断 Delete() 返回的错误是否为 DeletedVolumeInUseError。
// 仅匹配具体类型而非错误文本，避免误判。
func IsDeletedVolumeInUse(err error) bool {
	switch err.(type) {
	case deletedVolumeInUseError:
		return true
	default:
		return false
	}
}

// Error 原样返回构造时传入的消息，满足 error 接口。
func (err deletedVolumeInUseError) Error() string {
	return string(err)
}

// DanglingAttachError 表示卷挂载在与预期不同的节点上。
// 携带实际挂载节点与设备路径，便于 attach/detach 逻辑修正状态而不是盲目重试。
type DanglingAttachError struct {
	msg         string
	CurrentNode k8stypes.NodeName
	DevicePath  string
}

// Error 返回构造时传入的消息；附加字段仅用于调用方修正挂载状态。
func (err *DanglingAttachError) Error() string {
	return err.msg
}

// NewDanglingError 创建一个新的悬挂挂载错误。
// 返回指针类型，以匹配 IsDanglingError 的类型判断。
func NewDanglingError(msg string, node k8stypes.NodeName, devicePath string) error {
	return &DanglingAttachError{
		msg:         msg,
		CurrentNode: node,
		DevicePath:  devicePath,
	}
}

// IsDanglingError 判断错误是否为 DanglingAttachError。
// 只识别 *DanglingAttachError；值类型不会实现 error 接口，因此不会误匹配。
func IsDanglingError(err error) bool {
	switch err.(type) {
	case *DanglingAttachError:
		return true
	default:
		return false
	}
}
