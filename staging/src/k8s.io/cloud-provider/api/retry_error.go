/*
Copyright 2023 The Kubernetes Authors.

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

// Package api 汇总 cloud-controller-manager 与云提供商实现之间共享的约定：
// 节点污点键、节点注解键，以及可被控制器识别的错误类型。
package api

import (
	"time"
)

// RetryError indicates that a service reconciliation should be retried after a
// fixed duration (as opposed to backing off exponentially).
//
// 中文说明：云厂商在负载均衡等资源尚未就绪时，往往能给出精确的重试间隔（例如
// 等待后端安全组规则生效）。用固定间隔重试可以避免指数退避把重试拖到很久之后，
// 从而缩短 Service 从 Pending 到可用状态的时间。调用方通过 errors.As 识别该类型，
// 并读取 RetryAfter() 决定入队延迟。
type RetryError struct {
	msg        string        // 暴露给用户的错误描述，Error() 原样返回，通常包含云厂商给出的原因。
	retryAfter time.Duration // 建议的固定重试间隔，必须为正数才有意义。
}

// NewRetryError returns a RetryError.
// 中文说明：msg 应说明“为什么需要等待”，方便从事件或日志中直接定位云侧问题。
func NewRetryError(msg string, retryAfter time.Duration) *RetryError {
	return &RetryError{
		msg:        msg,
		retryAfter: retryAfter,
	}
}

// Error shows the details of the retry reason.
// 中文说明：实现 error 接口；消息面向最终用户，不应包含敏感信息。
func (re *RetryError) Error() string {
	return re.msg
}

// RetryAfter returns the defined retry-after duration.
// 中文说明：调用方应在该时长后重新入队，而不是走默认的指数退避。
func (re *RetryError) RetryAfter() time.Duration {
	return re.retryAfter
}
