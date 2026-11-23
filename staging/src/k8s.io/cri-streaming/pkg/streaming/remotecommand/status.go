/*
Copyright The Kubernetes Authors.

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

package remotecommand

import (
	"fmt"
	"net/http"
)

const (
	// statusSuccess 与 statusFailure 是状态字段的两种取值。
	statusSuccess = "Success"
	statusFailure = "Failure"

	// statusReasonInternalError 表示服务端内部错误。
	statusReasonInternalError = "InternalError"
)

// streamStatusError carries status details written to the error stream.
// streamStatusError 携带写入 error 流的状态信息；ErrStatus 是其中的状态对象。
type streamStatusError struct {
	ErrStatus streamStatus
}

// Error 返回状态中的消息，使 streamStatusError 满足 error 接口。
func (e *streamStatusError) Error() string {
	return e.ErrStatus.Message
}

// status 返回内部的状态对象，供序列化写入 error 流时使用。
func (e *streamStatusError) status() streamStatus {
	return e.ErrStatus
}

// newInternalError 根据给定的错误构造 InternalError 状态，HTTP code 为 500。
func newInternalError(err error) *streamStatusError {
	// 把原始错误拼进消息，方便客户端定位服务端失败的原因。
	return &streamStatusError{
		ErrStatus: streamStatus{
			Status:  statusFailure,
			Reason:  statusReasonInternalError,
			Message: fmt.Sprintf("Internal error occurred: %v", err),
			Code:    http.StatusInternalServerError,
		},
	}
}

// streamStatus 是写入 error 流的状态对象：Status 表示成功或失败，Message 是描述信息，
// Reason 给出错误原因，Details 携带更细的 causes，Code 是 HTTP 风格的状态码。
type streamStatus struct {
	Status  string               `json:"status,omitempty"`
	Message string               `json:"message,omitempty"`
	Reason  string               `json:"reason,omitempty"`
	Details *streamStatusDetails `json:"details,omitempty"`
	Code    int32                `json:"code,omitempty"`
}

// streamStatusDetails 包装状态的原因列表 Causes。
type streamStatusDetails struct {
	Causes []streamStatusCause `json:"causes,omitempty"`
}

// streamStatusCause uses "reason" on the wire to match metav1.StatusCause.
// streamStatusCause 描述一条具体原因：Type 在线上序列化为 "reason" 以兼容 metav1.StatusCause，
// Message 是该原因的说明；命令退出码就是通过 Type 为 ExitCode 的 cause 传递的。
type streamStatusCause struct {
	Type    string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}
