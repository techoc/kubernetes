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
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

// alwaysRetryError 让所有错误都被判定为可重试，用于构造重试耗尽的场景。
var alwaysRetryError = IsRetryableErrorFunc(func(_ *http.Request, _ error) bool {
	return true
})

// TestIsNextRetry 覆盖 withRetry 的重试判定与退避计算：
// 只有可重试错误或 429/5xx 才会重试，重试次数用尽后停止，并记录 Retry-After 信息。
func TestIsNextRetry(t *testing.T) {
	fakeError := errors.New("fake error")
	tests := []struct {
		name               string
		attempts           int
		maxRetries         int
		request            *http.Request
		response           *http.Response
		err                error
		retryableErrFunc   IsRetryableErrorFunc
		retryExpected      []bool
		retryAfterExpected []*RetryAfter
	}{
		{
			// response 和 err 都为 nil 属于非法输入，不应重试。
			name:               "bad input, response and err are nil",
			maxRetries:         2,
			attempts:           1,
			request:            &http.Request{},
			response:           nil,
			err:                nil,
			retryExpected:      []bool{false},
			retryAfterExpected: []*RetryAfter{nil},
		},
		{
			// maxRetries=0 表示禁用重试，即使响应带 Retry-After 也不重试。
			name:          "zero maximum retry",
			maxRetries:    0,
			attempts:      1,
			request:       &http.Request{},
			response:      retryAfterResponse(),
			err:           nil,
			retryExpected: []bool{false},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
				},
			},
		},
		{
			name:       "server returned a retryable error",
			maxRetries: 3,
			attempts:   1,
			request:    &http.Request{},
			response:   nil,
			err:        fakeError,
			retryableErrFunc: func(_ *http.Request, err error) bool {
				if err == fakeError {
					return true
				}
				return false
			},
			retryExpected: []bool{true},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
					Wait:    time.Second,
					Reason:  "retries: 1, retry-after: 1s - retry-reason: due to retryable error, error: fake error",
				},
			},
		},
		{
			// 429 且带 Retry-After 时，等待时长取服务端建议值，并记录 FlowSchema UID 便于定位限流来源。
			name:       "server returned a retryable HTTP 429 response",
			maxRetries: 3,
			attempts:   1,
			request:    &http.Request{},
			response: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header: http.Header{
					"Retry-After":                    []string{"2"},
					"X-Kubernetes-Pf-Flowschema-Uid": []string{"fs-1"},
				},
			},
			err:           nil,
			retryExpected: []bool{true},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
					Wait:    2 * time.Second,
					Reason:  `retries: 1, retry-after: 2s - retry-reason: due to server-side throttling, FlowSchema UID: "fs-1"`,
				},
			},
		},
		{
			// 5xx 默认可重试，Retry-After 同样生效。
			name:       "server returned a retryable HTTP 5xx response",
			maxRetries: 3,
			attempts:   1,
			request:    &http.Request{},
			response: &http.Response{
				StatusCode: http.StatusServiceUnavailable,
				Header: http.Header{
					"Retry-After": []string{"3"},
				},
			},
			err:           nil,
			retryExpected: []bool{true},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
					Wait:    3 * time.Second,
					Reason:  "retries: 1, retry-after: 3s - retry-reason: 503",
				},
			},
		},
		{
			// 没有 Retry-After 头时无法确定等待时间，不发起重试。
			name:       "server returned a non response without without a Retry-After header",
			maxRetries: 1,
			attempts:   1,
			request:    &http.Request{},
			response: &http.Response{
				StatusCode: http.StatusTooManyRequests,
				Header:     http.Header{},
			},
			err:           nil,
			retryExpected: []bool{false},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
				},
			},
		},
		{
			// err 与 response 同时存在时，err 优先，按可重试错误处理。
			name:       "both response and err are set, err takes precedence",
			maxRetries: 1,
			attempts:   1,
			request:    &http.Request{},
			response:   retryAfterResponse(),
			err:        fakeError,
			retryableErrFunc: func(_ *http.Request, err error) bool {
				if err == fakeError {
					return true
				}
				return false
			},
			retryExpected: []bool{true},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
					Wait:    time.Second,
					Reason:  "retries: 1, retry-after: 1s - retry-reason: due to retryable error, error: fake error",
				},
			},
		},
		{
			// attempts=4、maxRetries=3：前三次返回 true，第四次耗尽后返回 false。
			name:             "all retries are exhausted",
			maxRetries:       3,
			attempts:         4,
			request:          &http.Request{},
			response:         nil,
			err:              fakeError,
			retryableErrFunc: alwaysRetryError,
			retryExpected:    []bool{true, true, true, false},
			retryAfterExpected: []*RetryAfter{
				{
					Attempt: 1,
					Wait:    time.Second,
					Reason:  "retries: 1, retry-after: 1s - retry-reason: due to retryable error, error: fake error",
				},
				{
					Attempt: 2,
					Wait:    time.Second,
					Reason:  "retries: 2, retry-after: 1s - retry-reason: due to retryable error, error: fake error",
				},
				{
					Attempt: 3,
					Wait:    time.Second,
					Reason:  "retries: 3, retry-after: 1s - retry-reason: due to retryable error, error: fake error",
				},
				{
					Attempt: 4,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restReq := &Request{
				bodyBytes: []byte{},
				c: &RESTClient{
					base: &url.URL{},
				},
			}
			// 同一个 withRetry 上连续调用，模拟真实请求的多次尝试。
			r := &withRetry{maxRetries: test.maxRetries}

			retryGot := make([]bool, 0)
			retryAfterGot := make([]*RetryAfter, 0)
			for i := 0; i < test.attempts; i++ {
				retry := r.IsNextRetry(context.TODO(), restReq, test.request, test.response, test.err, test.retryableErrFunc)
				retryGot = append(retryGot, retry)
				retryAfterGot = append(retryAfterGot, r.retryAfter)
			}

			if !reflect.DeepEqual(test.retryExpected, retryGot) {
				t.Errorf("Expected retry: %t, but got: %t", test.retryExpected, retryGot)
			}
			if !reflect.DeepEqual(test.retryAfterExpected, retryAfterGot) {
				t.Errorf("Expected retry-after parameters to match, but got: %s", cmp.Diff(test.retryAfterExpected, retryAfterGot))
			}
		})
	}
}

// TestWrapPreviousError 验证重试失败后如何包装错误：
// 当前错误应保持可用 errors.Is/As 判定的身份，同时错误消息中附加上一次尝试的错误。
func TestWrapPreviousError(t *testing.T) {
	const (
		attempt                = 2
		previousAttempt        = 1
		containsFormatExpected = "- error from a previous attempt: %s"
	)
	var (
		wrappedCtxDeadlineExceededErr = &url.Error{
			Op:  "GET",
			URL: "http://foo.bar",
			Err: context.DeadlineExceeded,
		}
		wrappedCtxCanceledErr = &url.Error{
			Op:  "GET",
			URL: "http://foo.bar",
			Err: context.Canceled,
		}
		urlEOFErr = &url.Error{
			Op:  "GET",
			URL: "http://foo.bar",
			Err: io.EOF,
		}
	)

	tests := []struct {
		name        string
		previousErr error
		currentErr  error
		expectedErr error
		wrapped     bool
		contains    string
	}{
		{
			name: "current error is nil, previous error is nil",
		},
		{
			name:        "current error is nil",
			previousErr: errors.New("error from a previous attempt"),
		},
		{
			name:        "previous error is nil",
			currentErr:  urlEOFErr,
			expectedErr: urlEOFErr,
			wrapped:     false,
		},
		{
			name:        "both current and previous errors represent the same error",
			currentErr:  urlEOFErr,
			previousErr: &url.Error{Op: "GET", URL: "http://foo.bar", Err: io.EOF},
			expectedErr: urlEOFErr,
		},
		{
			name:        "current and previous errors are not same",
			currentErr:  urlEOFErr,
			previousErr: errors.New("unknown error"),
			expectedErr: urlEOFErr,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, "unknown error"),
		},
		{
			name:        "current error is context.Canceled",
			currentErr:  context.Canceled,
			previousErr: io.EOF,
			expectedErr: context.Canceled,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, io.EOF.Error()),
		},
		{
			name:        "current error is context.DeadlineExceeded",
			currentErr:  context.DeadlineExceeded,
			previousErr: io.EOF,
			expectedErr: context.DeadlineExceeded,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, io.EOF.Error()),
		},
		{
			name:        "current error is a wrapped context.DeadlineExceeded",
			currentErr:  wrappedCtxDeadlineExceededErr,
			previousErr: io.EOF,
			expectedErr: wrappedCtxDeadlineExceededErr,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, io.EOF.Error()),
		},
		{
			name:        "current error is a wrapped context.Canceled",
			currentErr:  wrappedCtxCanceledErr,
			previousErr: io.EOF,
			expectedErr: wrappedCtxCanceledErr,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, io.EOF.Error()),
		},
		{
			// url.Error 内部通常已经包含底层错误，这里只取其 Err 字段避免重复或信息丢失。
			name:        "previous error should be unwrapped if it is url.Error",
			currentErr:  urlEOFErr,
			previousErr: &url.Error{Err: io.ErrUnexpectedEOF},
			expectedErr: urlEOFErr,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, io.ErrUnexpectedEOF.Error()),
		},
		{
			name:        "previous error should not be unwrapped if it is not url.Error",
			currentErr:  urlEOFErr,
			previousErr: fmt.Errorf("should be included in error message - %w", io.EOF),
			expectedErr: urlEOFErr,
			wrapped:     true,
			contains:    fmt.Sprintf(containsFormatExpected, "should be included in error message - EOF"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry := &withRetry{
				previousErr: test.previousErr,
			}

			err := retry.WrapPreviousError(test.currentErr)
			switch {
			case test.expectedErr == nil:
				if err != nil {
					t.Errorf("Expected a nil error, but got: %v", err)
					return
				}
			case test.expectedErr != nil:
				// 确认返回错误的消息中包含上一次重试的错误信息。
				if !strings.Contains(err.Error(), test.contains) {
					t.Errorf("Expected error message to contain %q, but got: %v", test.contains, err)
				}

				// 包装后应能用 errors.Unwrap 取回原始当前错误。
				currentErrGot := err
				if test.wrapped {
					currentErrGot = errors.Unwrap(err)
				}
				if test.expectedErr != currentErrGot {
					t.Errorf("Expected current error %v, but got: %v", test.expectedErr, currentErrGot)
				}
			}
		})
	}

	t.Run("Before should track previous error", func(t *testing.T) {
		// Before 在请求开始前检查 ctx：上下文已取消时应立即返回错误，
		// 并把上一次的 currentErr 移到 previousErr，供后续错误包装使用。
		retry := &withRetry{
			currentErr: io.EOF,
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		// 这里传入零值 Request，因为预期 Before 一开始就检查 context 错误。
		err := retry.Before(ctx, &Request{})
		if err != context.Canceled {
			t.Errorf("Expected error: %v, but got: %v", context.Canceled, err)
		}
		if retry.currentErr != context.Canceled {
			t.Errorf("Expected current error: %v, but got: %v", context.Canceled, retry.currentErr)
		}
		if retry.previousErr != io.EOF {
			t.Errorf("Expected previous error: %v, but got: %v", io.EOF, retry.previousErr)
		}
	})
}
