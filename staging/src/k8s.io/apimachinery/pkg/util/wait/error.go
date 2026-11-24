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

package wait

import (
	"context"
	"errors"
)

// ErrWaitTimeout 表示等待条件在允许的时间或尝试次数内始终没有满足。
// 已弃用：请使用 Interrupted 判断等待是否被中断，或使用 ErrorInterrupted 包装具体原因。
// 直接比较该错误可能漏掉 context 取消、截止时间到达等其他中断原因。
var ErrWaitTimeout = ErrorInterrupted(errors.New("timed out waiting for the condition"))

// Interrupted 判断等待循环是否因条件未成功且未返回自身错误而提前结束。
// context 被取消、达到截止时间，或退避次数达到上限，都属于中断。
//
// 调用方应使用该方法，而不是直接将错误与 ErrWaitTimeout 比较；
// 使用 context 的接口可能返回 context.Canceled 或 context.DeadlineExceeded。
//
// 不推荐：
//
//	err := wait.Poll(...)
//	if err == wait.ErrWaitTimeout {
//	    log.Infof("等待操作超时")
//	} else ...
//
// 推荐：
//
//	err := wait.Poll(...)
//	if wait.Interrupted(err) {
//	    log.Infof("等待操作超时或被中断")
//	} else ...
//
// 中文补充：同时识别 errInterrupted、context.Canceled 与 context.DeadlineExceeded，
// 因为轮询循环可能直接返回 context 错误而不是包装后的超时错误。
func Interrupted(err error) bool {
	switch {
	case errors.Is(err, errWaitTimeout),
		errors.Is(err, context.Canceled),
		errors.Is(err, context.DeadlineExceeded):
		return true
	default:
		return false
	}
}

// errInterrupted 是表示“等待被提前终止”的内部错误类型，
// 通过 Unwrap 暴露原因、通过 Is 与 errWaitTimeout 哨兵值匹配。
type errInterrupted struct {
	cause error
}

// ErrorInterrupted 创建一个表示等待提前结束的错误，并保留导致结束的具体原因。
// cause 为 nil 时使用通用错误；建议尽量传入真实原因，以便排查问题。
func ErrorInterrupted(cause error) error {
	switch cause.(type) {
	case errInterrupted:
		// errInterrupted 在错误链中只需要出现一次，已经包装过就直接返回，避免重复嵌套。
		return cause
	default:
		return errInterrupted{cause}
	}
}

// errWaitTimeout 是 ErrWaitTimeout 的内部哨兵版本。将其设为私有可避免调用方直接比较；
// 调用方应使用 ErrorInterrupted 包装错误，并通过 Interrupted 判断。
var errWaitTimeout = errInterrupted{}

// Unwrap 返回底层原因，使 errors.Is/As 能穿透到真正的错误。
func (e errInterrupted) Unwrap() error { return e.cause }

// Is 让所有 errInterrupted 都能与 errWaitTimeout 匹配，从而支持 errors.Is 判断。
func (e errInterrupted) Is(target error) bool { return target == errWaitTimeout }

// Error 在没有原因时沿用历史文案，保持与旧版本错误消息兼容。
func (e errInterrupted) Error() string {
	if e.cause == nil {
		// 原因为空时保持历史错误文本，避免改变依赖该文本的既有行为。
		return "timed out waiting for the condition"
	}
	return e.cause.Error()
}
