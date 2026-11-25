/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"errors"
	"fmt"

	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
)

const (
	// 准入错误原因标识。
	ErrorReasonUnexpected = "UnexpectedAdmissionError"
	// ErrorReasonEmptyPodSharedPool 表示 Pod 共享池为空。
	ErrorReasonEmptyPodSharedPool = "EmptyPodSharedPoolError"
)

// Error 是带有错误原因类型的准入错误接口。
type Error interface {
	// Error 返回错误消息。
	Error() string
	// Type 返回错误原因标识。
	Type() string
}

// unexpectedAdmissionError 包装未预期到的准入错误。
type unexpectedAdmissionError struct{ Err error }

// 编译期断言：unexpectedAdmissionError 实现了 Error 接口。
var _ Error = (*unexpectedAdmissionError)(nil)

// Error 返回带原始错误的描述。
func (e *unexpectedAdmissionError) Error() string {
	return fmt.Sprintf("Allocate failed due to %v, which is unexpected", e.Err)
}

// Type 返回 UnexpectedAdmissionError。
func (e *unexpectedAdmissionError) Type() string {
	return ErrorReasonUnexpected
}

// EmptyPodSharedPoolError 表示 Topology Manager 作用域为 pod 时，
// Pod 规格产生了空的共享池、但仍有容器需要它而导致的错误。
type EmptyPodSharedPoolError struct {
	// message 是错误消息。
	message string
}

// NewEmptyPodSharedPoolError 创建 EmptyPodSharedPoolError。
func NewEmptyPodSharedPoolError(err error) *EmptyPodSharedPoolError {
	return &EmptyPodSharedPoolError{message: err.Error()}
}

// Error 返回错误消息。
func (e *EmptyPodSharedPoolError) Error() string {
	return e.message
}

// Type 返回 EmptyPodSharedPoolError。
func (e *EmptyPodSharedPoolError) Type() string {
	return ErrorReasonEmptyPodSharedPool
}

// GetPodAdmitResult 把错误转换为 Pod 准入结果。
func GetPodAdmitResult(err error) lifecycle.PodAdmitResult {
	// nil 表示准入通过。
	if err == nil {
		return lifecycle.PodAdmitResult{Admit: true}
	}

	var admissionErr Error
	// 非准入错误统一包装为 unexpectedAdmissionError，避免丢失信息。
	if !errors.As(err, &admissionErr) {
		admissionErr = &unexpectedAdmissionError{err}
	}

	return lifecycle.PodAdmitResult{
		Message: admissionErr.Error(),
		Reason:  admissionErr.Type(),
		Admit:   false,
	}
}
