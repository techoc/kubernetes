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

package finisher

import (
	"context"
	"fmt"
	"net/http"
	goruntime "runtime"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apiserver/pkg/endpoints/metrics"
	"k8s.io/klog/v2"
)

// ResultFunc 是返回 REST 结果的函数，可以在 goroutine 中运行。
type ResultFunc func() (runtime.Object, error)

// result 保存 ResultFunc 的返回值或 panic 信息。
type result struct {
	// object 记录 ResultFunc 返回的响应。
	object runtime.Object
	// err 记录 ResultFunc 返回的错误。
	err error
	// reason 记录 ResultFunc 抛出的 panic 原因。
	reason interface{}
}

// Return 处理 ResultFunc 返回的结果。
//
// panic 的优先级高于 error：panic 意味着执行者状态未知，
// 不能让一个“看似成功”的返回值掩盖它。
func (r *result) Return() (runtime.Object, error) {
	switch {
	case r.reason != nil:
		// panic 的优先级更高：执行 ResultFunc 的 goroutine 已经 panic，
		// 因此向调用方传播 panic。
		panic(r.reason)
	case r.err != nil:
		return nil, r.err
	default:
		// 走到这里说明既没有 panic，也没有错误。
		if status, ok := r.object.(*metav1.Status); ok {
			// status 不为 success 的 api.Status 对象被视为“错误”，会中断正常响应流程。
			// 这是 Kubernetes API 的约定：handler 可以用 Status 对象表达失败，
			// 而不必返回 error。
			if status.Status != metav1.StatusSuccess {
				return nil, errors.FromObject(status)
			}
		}
		return r.object, nil
	}
}

// PostTimeoutLoggerFunc 用于在请求已超时之后记录 ResultFunc 返回的结果。
// timedOutAt 是请求被判定超时的时刻；r 是子 goroutine 返回的结果。
type PostTimeoutLoggerFunc func(timedOutAt time.Time, r *result)

const (
	// 请求超时后，超时后的接收方 goroutine 等待发送方（执行 ResultFunc 的子 goroutine）
	// 发出结果的最长时间。
	//
	// 上限而非无限等待，防止一个卡死的 handler 让负责收尾的 goroutine 永久驻留。
	postTimeoutLoggerWait = 5 * time.Minute
)

// FinishRequest 把给定的 ResultFunc 变成异步执行，并处理响应 writer 返回的错误。
//
// 异步执行的意义：请求超时后 HTTP 处理函数可以立即返回，而实际存储操作
// 可能仍在进行；这里负责把两者的结果与 panic 边界衔接起来。
func FinishRequest(ctx context.Context, fn ResultFunc) (runtime.Object, error) {
	return finishRequest(ctx, fn, postTimeoutLoggerWait, logPostTimeoutResult)
}

func finishRequest(ctx context.Context, fn ResultFunc, postTimeoutWait time.Duration, postTimeoutLogger PostTimeoutLoggerFunc) (runtime.Object, error) {
	// 该 channel 必须带缓冲：超时后的接收方 goroutine 最长会等待 5 分钟，
	// 等待子 goroutine 返回。
	// 缓冲为 1 可保证发送方不会因接收方已放弃而阻塞泄漏。
	resultCh := make(chan *result, 1)

	go func() {
		result := &result{}

		// panic 不会跨 goroutine 边界传播，因此需要自行处理。
		defer func() {
			reason := recover()
			if reason != nil {
				// 不要包装哨兵值 ErrAbortHandler 的 panic。
				// ErrAbortHandler 是 net/http 用于静默中止响应的哨兵值，包装会破坏其语义。
				if reason != http.ErrAbortHandler {
					// 与标准库 http server 代码一致：手动分配栈追踪缓冲区大小，
					// 避免日志过大。
					const size = 64 << 10
					buf := make([]byte, size)
					buf = buf[:goruntime.Stack(buf, false)]
					reason = fmt.Sprintf("%v\n%s", reason, buf)
				}

				// 把 panic 原因存入 result。
				result.reason = reason
			}

			// 把结果传递给父 goroutine。
			resultCh <- result
		}()

		if object, err := fn(); err != nil {
			result.err = err
		} else {
			result.object = object
		}
	}()

	select {
	case result := <-resultCh:
		return result.Return()
	case <-ctx.Done():
		// 我们即将向调用方发送超时响应，但异步 goroutine（发送方）仍在执行 ResultFunc。
		// 这里再起一个 goroutine（接收方）等待发送方把结果发来，然后记录结果的详细信息。
		//
		// 这个收尾 goroutine 只做观测与指标记录，不影响已返回给客户端的超时响应。
		defer func() {
			go func() {
				timedOutAt := time.Now()

				var result *result
				select {
				case result = <-resultCh:
				case <-time.After(postTimeoutWait):
					// 我们不会无限等待：走到这里说明某些发送方 goroutine
					// 耗时超过了 postTimeoutWait。
				}
				postTimeoutLogger(timedOutAt, result)
			}()
		}()
		return nil, errors.NewTimeoutError(fmt.Sprintf("request did not complete within requested timeout - %s", ctx.Err()), 0)
	}
}

// logPostTimeoutResult 记录发送方（执行 ResultFunc 的 goroutine）在请求超时之后
// 发送给接收方的 panic 或错误。timedOutAt 是请求被判定超时的时刻。
func logPostTimeoutResult(timedOutAt time.Time, r *result) {
	if r == nil {
		// 用 r == nil 表示子 goroutine 从未返回结果。
		metrics.RecordRequestPostTimeout(metrics.PostTimeoutSourceRestHandler, metrics.PostTimeoutHandlerPending)
		klog.Errorf("FinishRequest: post-timeout activity, waited for %s, child goroutine has not returned yet", time.Since(timedOutAt))
		return
	}

	var status string
	switch {
	case r.reason != nil:
		// result 中 reason 非空说明发生过 panic。
		status = metrics.PostTimeoutHandlerPanic
	case r.err != nil:
		status = metrics.PostTimeoutHandlerError
	default:
		status = metrics.PostTimeoutHandlerOK
	}

	// 无论结果如何都记录一条错误日志：请求已超时，后续完成都值得关注。
	metrics.RecordRequestPostTimeout(metrics.PostTimeoutSourceRestHandler, status)
	err := fmt.Errorf("FinishRequest: post-timeout activity - time-elapsed: %s, panicked: %t, err: %v, panic-reason: %v",
		time.Since(timedOutAt), r.reason != nil, r.err, r.reason)
	utilruntime.HandleError(err)
}
