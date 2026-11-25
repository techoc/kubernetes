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

package ktesting

import (
	"context"
	"time"

	"k8s.io/klog/v2"
)

// WithCancel 在 [TContext.Cleanup] 回调中设置取消机制，
// 并构造一个新的 TContext，其中 [TContext.Cancel]
// 只取消新的 context。
func (tCtx TContext) WithCancel() TContext {
	ctx, cancel := context.WithCancelCause(tCtx)

	tCtx.Context = ctx
	tCtx.cancel = func(cause string) {
		var cancelCause error
		if cause != "" {
			cancelCause = canceledError(cause)
		}
		cancel(cancelCause)
	}
	return tCtx
}

// WithoutCancel 使返回的 context 忽略其父级的取消。
// 调用 Cancel 只会取消新的 context。
// 这与 [context.WithoutCancel] 一致。
func (tCtx TContext) WithoutCancel() TContext {
	ctx := context.WithoutCancel(tCtx)

	tCtx.Context = ctx
	tCtx = tCtx.WithCancel() // 重新创建可取消的 TContext。
	return tCtx
}

// WithTimeout 设置一个带超时机制的新 context。取消超时
// 会注册到清理回调中。[TContext.Cancel] 只取消
// 新的 context。当到达超时时间时，cause 用作
// context 被取消的原因。它可以为空，此时使用通常的
// "context canceled" 错误。
func (tCtx TContext) WithTimeout(timeout time.Duration, timeoutCause string) TContext {
	ctx, cancel := withTimeout(tCtx, tCtx.TB(), timeout, timeoutCause)

	tCtx.Context = ctx
	tCtx.cancel = cancel
	return tCtx
}

// WithLogger 使用另一个 logger 构造新的 context。
func (tCtx TContext) WithLogger(logger klog.Logger) TContext {
	ctx := klog.NewContext(tCtx, logger)

	tCtx.Context = ctx
	return tCtx
}
