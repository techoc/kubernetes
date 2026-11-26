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

package rest

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"k8s.io/klog/v2"
)

// IsRetryableErrorFunc 允许调用方提供自定义函数，判断服务端返回的错误是否可重试。
//
// request：发给服务端的原始请求；err：服务端返回的错误。
//
// 函数返回 true 表示错误可重试、请求可重新发送。通信有 Stream、Watch、Do、DoRaw
// 四种模式，该函数让调用方分别定制各自的重试判定。
//
// IsRetryableErrorFunc 允许调用方自定义错误是否可重试。
// 四种通信模式（Stream/Watch/Do/DoRaw）的重试语义各不相同，因此用回调注入。
type IsRetryableErrorFunc func(request *http.Request, err error) bool

// IsErrorRetryable 调用回调判断错误是否可重试。
func (r IsRetryableErrorFunc) IsErrorRetryable(request *http.Request, err error) bool {
	return r(request, err)
}

// neverRetryError 是恒不重试的策略。
var neverRetryError = IsRetryableErrorFunc(func(_ *http.Request, _ error) bool {
	return false
})

// WithRetry 支持把请求重试若干次。注意：没有额外加锁或协调时，WithRetry
// 不支持多个 goroutine 并发使用。
//
// WithRetry 负责一次请求的重试计数与退避。
// 注意：单个 WithRetry 实例不能被多个 goroutine 并发使用，除非外层另有同步。
type WithRetry interface {
	// IsNextRetry 推进重试计数，并返回是否应重试。
	//   - 已达到最大重试次数。
	//   - 错误不属于可重试类别。
	//   - 服务端没有返回 429 或 5xx，且未设置 'Retry-After' 响应头。
	//   - 发起下次重试前需要 seek 回请求体开头，若失败应记录错误并返回 false。
	//
	// restReq：关联的 rest.Request。
	// httpReq：发给服务端的 HTTP Request。
	// resp：服务端返回的响应，仅在 err 为 nil 时设置。err：服务端返回的错误，
	// 设置时 resp 为 nil。f：调用方提供的 IsRetryableErrorFunc，用于判断可重试性。
	// f：调用方提供的 IsRetryableErrorFunc，用于判断服务端错误是否可重试。
	//
	// IsNextRetry 推进重试计数，并返回是否应重试。
	// 达到上限、错误不可重试、服务端未给出 429/5xx + Retry-After、或请求体无法重置时返回 false。
	IsNextRetry(ctx context.Context, restReq *Request, httpReq *http.Request, resp *http.Response, err error, f IsRetryableErrorFunc) bool

	// Before 在每次尝试（含第一次）之前调用；返回错误则立即终止请求。它还负责为下次
	// 重试准备请求，例如在请求体已被读取后重置它。
	//
	// Before 在每次尝试（含第一次）之前调用；返回错误则立即终止请求。
	// 它也负责为重试准备请求（例如重置已被读取的请求体）。
	Before(ctx context.Context, r *Request) error

	// After 在每次尝试之后立即调用。
	//
	// After 在每次尝试之后立即调用，用于更新指标与退避状态。
	After(ctx context.Context, r *Request, resp *http.Response, err error)

	// WrapPreviousError 把之前尝试的错误包装进 finalErr，为调用方提供更多失败上下文。
	// 例如请求多次重试后超时，只会看到泛化的 context.Canceled 或
	// context.DeadlineExceeded，不利于排查。此函数可包装之前尝试的错误。返回的 err
	// 必须满足以下条件：
	// following conditions:
	//  a: errors.Unwrap(err) = errors.Unwrap(finalErr) if finalErr
	//     implements Unwrap
	//  b: errors.Unwrap(err) = finalErr if finalErr does not
	//     implements Unwrap
	//  c: errors.Is(err, otherErr) = errors.Is(finalErr, otherErr)
	//
	// WrapPreviousError 把上一次尝试的错误附加到最终错误上，便于定位失败原因，
	// 同时保持错误链的 Unwrap/Is 语义不变。
	WrapPreviousError(finalErr error) (err error)
}

// RetryAfter 描述下一次重试的相关参数。
//
// RetryAfter 描述下一次重试的相关参数。
type RetryAfter struct {
	// Wait 是服务端要求下次重试前等待的时长，取自 'Retry-After' 响应头的秒数。
	Wait time.Duration

	// Attempt 是收到可重试错误或 'Retry-After' 响应头时是第几次尝试。
	Attempt int

	// Reason 描述重试原因。
	Reason string
}

// withRetry 是 WithRetry 的默认实现。
type withRetry struct {
	maxRetries int
	attempts   int

	// 即将进行的尝试对应的 retry after 参数，供 Before/After 引用。
	//   - 第一次尝试时总为 nil。
	//   - 后续尝试时非 nil，保存下次尝试的重试参数。
	retryAfter *RetryAfter

	// 跟踪最近两次错误：若最近一次尝试记为 'N'，则：
	//   - currentErr 是尝试 N 返回的错误，尝试 N 未出错时可为 nil。
	//   - previousErr 是某次更早尝试 'M' 的错误：
	//       - 对尝试序列 1..n（n>1），存在某次 k（k<n）返回了错误。
	previousErr, currentErr error
}

// trackPreviousError 记录最近两次错误，供最终错误包装使用。
func (r *withRetry) trackPreviousError(err error) {
	// 跟踪最近两次错误。
	if r.currentErr != nil {
		r.previousErr = r.currentErr
	}
	r.currentErr = err
}

// IsNextRetry 推进重试计数并判断是否应重试；具体规则见接口注释。
func (r *withRetry) IsNextRetry(ctx context.Context, restReq *Request, httpReq *http.Request, resp *http.Response, err error, f IsRetryableErrorFunc) bool {
	defer r.trackPreviousError(err)

	if httpReq == nil || (resp == nil && err == nil) {
		// 输入非法，不做处理。
		return false
	}

	if restReq.body != nil {
		// 请求体是不透明（opaque）且不可重置的 reader，无法安全重放，因此不重试。
		return false
	}

	r.attempts++
	r.retryAfter = &RetryAfter{Attempt: r.attempts}
	if r.attempts > r.maxRetries {
		return false
	}

	// 服务端返回错误时以它为准，优先于 HTTP 响应。
	var errIsRetryable bool
	if f != nil && err != nil && f.IsErrorRetryable(httpReq, err) {
		errIsRetryable = true
		// 对可重试错误伪造一个 500 + Retry-After: 1 的响应，
		// 以便复用统一的等待逻辑。
		resp = retryAfterResponse()
	}
	if err != nil && !errIsRetryable {
		return false
	}

	// 执行到这里，a 或 b 至少有一个非空。
	//  a: 有可重试错误，且已构造人工 "Retry-After" 响应。
	//  b: 有服务端响应，需要检查它是否可重试。
	seconds, wait := checkWait(resp)
	if !wait {
		return false
	}

	r.retryAfter.Wait = time.Duration(seconds) * time.Second
	r.retryAfter.Reason = getRetryReason(r.attempts, seconds, resp, err)

	return true
}

// Before 在每次尝试前执行退避与限流；首次尝试也执行退避（保持既有行为）。
func (r *withRetry) Before(ctx context.Context, request *Request) error {
	// 请求 context 已取消时无需重试。
	if ctx.Err() != nil {
		r.trackPreviousError(ctx.Err())
		return ctx.Err()
	}

	url := request.URL()
	// r.retryAfter 是根据上次尝试的 (响应, 错误) 计算的重试参数，Before 可在
	// 下次尝试前应用这些参数。
	// 'r.retryAfter == nil' 表示这是第一次尝试。
	if r.retryAfter == nil {
		// 首次尝试前也会做退避睡眠（保持既有行为）。
		// (preserving current behavior).
		if request.backoff != nil {
			request.backoff.SleepWithContext(ctx, request.backoff.CalculateBackoffWithContext(ctx, url))
		}
		return nil
	}

	// 走到这里说明此前至少已经尝试过一次。
	// 重试时取客户端退避与 Retry-After 的较大值，避免早于服务端要求的时间重试。
	if request.backoff != nil {
		delay := request.backoff.CalculateBackoffWithContext(ctx, url)
		if r.retryAfter.Wait > delay {
			delay = r.retryAfter.Wait
		}
		request.backoff.SleepWithContext(ctx, delay)
	}

	// 这是对至少已发往 apiserver 一次的请求进行重试，也应经过客户端内部限流器。
	if err := request.tryThrottleWithInfo(ctx, r.retryAfter.Reason); err != nil {
		r.trackPreviousError(ctx.Err())
		return err
	}

	klog.FromContext(ctx).V(4).Info("Got a Retry-After response", "delay", r.retryAfter.Wait, "attempt", r.retryAfter.Attempt, "url", request.URL())
	return nil
}

// After 在每次尝试后更新指标与退避；首次尝试记为 retry=2。
func (r *withRetry) After(ctx context.Context, request *Request, resp *http.Response, err error) {
	// 'After' 在每次尝试后立即调用，这里把尝试记为：
	// 刚完成的尝试记为 'N'；当前 r.retryAfter 来自尝试 N-1 的 (响应, 错误)，
	// 已经过时，这里不应再引用。
	isRetry := r.retryAfter != nil
	r.retryAfter = nil

	// 客户端在 N 次尝试（1..N）后结束单次请求。
	//   - 所有尝试（1..N）都计入 rest_client_requests_total。
	//    metric (current behavior).
	//   - 首次之后的每次尝试（2..N）都计入 rest_client_request_retries_total 指标。
	updateRequestResultMetric(ctx, request, resp, err)
	if isRetry {
		// 这是第 2 次或更晚的尝试。
		updateRequestRetryMetric(ctx, request, resp, err)
	}

	if request.c.base != nil {
		if err != nil {
			request.backoff.UpdateBackoffWithContext(ctx, request.URL(), err, 0)
		} else {
			request.backoff.UpdateBackoffWithContext(ctx, request.URL(), err, resp.StatusCode)
		}
	}
}

// WrapPreviousError 用 wrapPreviousError 把上一次尝试的错误附加到当前错误。
func (r *withRetry) WrapPreviousError(currentErr error) error {
	if currentErr == nil || r.previousErr == nil {
		return currentErr
	}

	// 若上一次与当前错误表示同一问题，则无需包装上一次错误。
	if currentErr.Error() == r.previousErr.Error() {
		return currentErr
	}

	previousErr := r.previousErr
	// net/http 用 url.Error 包装底层错误；如果服务端返回错误，它会优先于 HTTP 响应。
	// 上一次错误对象是 url.Error 时，可展开取出内部错误，避免出现类似如下消息：
	//  Error: Get "http://foo.bar/api/v1": context deadline exceeded - error \
	//  from a previous attempt: Error: Get "http://foo.bar/api/v1": EOF
	//
	// 去掉重复的 url 前缀，让“上一次尝试的错误”更易读。
	if urlErr, ok := r.previousErr.(*url.Error); ok && urlErr != nil {
		if urlErr.Unwrap() != nil {
			previousErr = urlErr.Unwrap()
		}
	}

	return &wrapPreviousError{
		currentErr:    currentErr,
		previousError: previousErr,
	}
}

// wrapPreviousError 组合当前错误与上一次尝试的错误，Unwrap 只暴露当前错误。
type wrapPreviousError struct {
	currentErr, previousError error
}

func (w *wrapPreviousError) Unwrap() error { return w.currentErr }
func (w *wrapPreviousError) Error() string {
	return fmt.Sprintf("%s - error from a previous attempt: %s", w.currentErr.Error(), w.previousError.Error())
}

// 若服务端要求重试前等待，checkWait 返回 true 以及秒数。
//
// checkWait 只对 429 与 5xx 检查 Retry-After；其他状态码不触发等待。
func checkWait(resp *http.Response) (int, bool) {
	switch r := resp.StatusCode; {
	// 任何 5xx 错误和 429 都可触发等待。
	case r == http.StatusTooManyRequests, r >= 500:
	default:
		return 0, false
	}
	i, ok := retryAfterSeconds(resp)
	return i, ok
}

// getRetryReason 生成人类可读的重试原因，用于限流日志与指标。
func getRetryReason(retries, seconds int, resp *http.Response, err error) string {
	// priority and fairness 通过以下响应头设置请求关联的 FlowSchema UID。
	const responseHeaderMatchedFlowSchemaUID = "X-Kubernetes-PF-FlowSchema-UID"

	message := fmt.Sprintf("retries: %d, retry-after: %ds", retries, seconds)

	switch {
	case resp.StatusCode == http.StatusTooManyRequests:
		// 这是 priority and fairness 触发的服务端限流。
		flowSchemaUID := resp.Header.Get(responseHeaderMatchedFlowSchemaUID)
		return fmt.Sprintf("%s - retry-reason: due to server-side throttling, FlowSchema UID: %q", message, flowSchemaUID)
	case err != nil:
		// 这是可重试错误。
		return fmt.Sprintf("%s - retry-reason: due to retryable error, error: %v", message, err)
	default:
		return fmt.Sprintf("%s - retry-reason: %d", message, resp.StatusCode)
	}
}

// readAndCloseResponseBody 在重连前读完并关闭响应体。
func readAndCloseResponseBody(resp *http.Response) {
	if resp == nil {
		return
	}

	// 重连前读完并关闭响应体，以便复用同一 TCP 连接。
	const maxBodySlurpSize = 2 << 10
	defer resp.Body.Close()

	// 只丢弃有限大小的内容，避免为复用连接而读取超大响应体。
	if resp.ContentLength <= maxBodySlurpSize {
		io.Copy(io.Discard, &io.LimitedReader{R: resp.Body, N: maxBodySlurpSize})
	}
}

// retryAfterResponse 构造 Retry-After: 1 的伪造响应（用于可重试错误）。
func retryAfterResponse() *http.Response {
	return retryAfterResponseWithDelay("1")
}

// retryAfterResponseWithDelay 构造指定 Retry-After 秒数的 500 响应，供测试使用。
func retryAfterResponseWithDelay(delay string) *http.Response {
	return retryAfterResponseWithCodeAndDelay(http.StatusInternalServerError, delay)
}

// retryAfterResponseWithCodeAndDelay 构造指定状态码与 Retry-After 的响应。
func retryAfterResponseWithCodeAndDelay(code int, delay string) *http.Response {
	return &http.Response{
		StatusCode: code,
		Header:     http.Header{"Retry-After": []string{delay}},
	}
}
