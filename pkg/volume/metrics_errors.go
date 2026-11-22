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

package volume

import (
	"fmt"
)

const (
	// ErrCodeNotSupported 表示“不支持”类错误。
	ErrCodeNotSupported int = iota + 1

	// ErrCodeNoPathDefined 表示“未定义路径”类错误。
	ErrCodeNoPathDefined

	// ErrCodeFsInfoFailed 表示获取文件系统信息失败类错误。
	ErrCodeFsInfoFailed
)

// NewNotSupportedError 创建一个错误码为 NotSupported 的 MetricsError。
func NewNotSupportedError() *MetricsError {
	return &MetricsError{
		Code: ErrCodeNotSupported,
		Msg:  "metrics are not supported for MetricsNil Volumes",
	}
}

// NewNotImplementedError 创建一个错误码为 NotSupported 的 MetricsError，并附带未实现的原因。
func NewNotImplementedError(reason string) *MetricsError {
	return &MetricsError{
		Code: ErrCodeNotSupported,
		Msg:  fmt.Sprintf("metrics support is not implemented: %s", reason),
	}
}

// NewNotSupportedErrorWithDriverName 创建一个错误码为 NotSupported 的 MetricsError，
// 并在错误消息中附上驱动名。
func NewNotSupportedErrorWithDriverName(name string) *MetricsError {
	return &MetricsError{
		Code: ErrCodeNotSupported,
		Msg:  fmt.Sprintf("metrics are not supported for %s volumes", name),
	}
}

// NewNoPathDefinedError 创建一个错误码为 NoPathDefined 的 MetricsError。
func NewNoPathDefinedError() *MetricsError {
	return &MetricsError{
		Code: ErrCodeNoPathDefined,
		Msg:  "no path defined for disk usage metrics.",
	}
}

// NewFsInfoFailedError 创建一个错误码为 FsInfoFailed 的 MetricsError，
// 消息中包含底层错误。
func NewFsInfoFailedError(err error) *MetricsError {
	return &MetricsError{
		Code: ErrCodeFsInfoFailed,
		Msg:  fmt.Sprintf("failed to get FsInfo due to error %v", err),
	}
}

// MetricsError 用于区分不同类别的 Metrics 错误。
type MetricsError struct {
	Code int
	Msg  string
}

// Error 返回错误的文本消息。
func (e *MetricsError) Error() string {
	return e.Msg
}

// IsNotSupported 判断 err 是否为“不支持”类错误。
func IsNotSupported(err error) bool {
	return isErrCode(err, ErrCodeNotSupported)
}

// isErrCode 判断 err 是否为指定错误码的 MetricsError。
func isErrCode(err error, code int) bool {
	if err == nil {
		return false
	}
	if e, ok := err.(*MetricsError); ok {
		return e.Code == code
	}
	return false
}
