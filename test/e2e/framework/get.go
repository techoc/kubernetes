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

package framework

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetFunc 是获取某个对象的函数。
type GetFunc[T any] func(ctx context.Context) (T, error)

// APIGetFunc 是 client-go 中使用的 get 函数。
type APIGetFunc[T any] func(ctx context.Context, name string, getOptions metav1.GetOptions) (T, error)

// APIListFunc 是 client-go 中使用的 list 函数。
type APIListFunc[T any] func(ctx context.Context, listOptions metav1.ListOptions) (T, error)

// GetObject 接受一个类似 clientset.CoreV1().Pods(ns).Get 的 get 函数
// 及其参数，返回一个在 [gomega.Eventually] 或 [gomega.Consistently] 中
// 执行该 get 操作的函数。
//
// 延迟和重试由 [HandleRetry] 处理。"not found" 错误是
// 致命错误，会立即停止轮询。如果不想这样，
// 就用 [IgnoreNotFound] 包装结果。
func GetObject[T any](get APIGetFunc[T], name string, getOptions metav1.GetOptions) GetFunc[T] {
	return HandleRetry(func(ctx context.Context) (T, error) {
		return get(ctx, name, getOptions)
	})
}

// ListObjects 接受一个类似 clientset.CoreV1().Pods(ns).List 的 list 函数
// 及其参数，返回一个执行该 list 操作的函数。
// 执行该 get 操作的函数。
//
// 延迟和重试由 [HandleRetry] 处理。
func ListObjects[T any](list APIListFunc[T], listOptions metav1.ListOptions) GetFunc[T] {
	return HandleRetry(func(ctx context.Context) (T, error) {
		return list(ctx, listOptions)
	})
}

// HandleRetry 包装任意 get 函数。当被包装的函数
// 返回错误时，HandleGetError 决定该调用是否
// 需要重试，并在需要时先休眠。
//
// 这旨在用于 [gomega.Eventually] 或 [gomega.Consistently] 内部。
func HandleRetry[T any](get GetFunc[T]) GetFunc[T] {
	return func(ctx context.Context) (T, error) {
		t, err := get(ctx)
		if err != nil {
			if retry, delay := ShouldRetry(err); retry {
				if delay > 0 {
					// 我们本可以返回
					// 这里的 gomega.TryAgainAfter(delay)，
					// 但那样就需要把这个
					// 错误穿过所有其他
					// 包装器。直接等待更简单。
					ctx, cancel := context.WithTimeout(ctx, delay)
					defer cancel()
					<-ctx.Done()
				}
				return t, err
			}
			// 立即放弃轮询。
			var null T
			return t, gomega.StopTrying(fmt.Sprintf("Unexpected final error while getting %T", null)).Wrap(err)
		}
		return t, nil
	}
}

// ShouldRetry 决定是否重试 API 请求，并可返回
// 重试前的延迟。
func ShouldRetry(err error) (retry bool, retryAfter time.Duration) {
	// 如果错误带有 Retry-After header，我们将其视为应重试的明确信号。
	if delay, shouldRetry := apierrors.SuggestsClientDelay(err); shouldRetry {
		return shouldRetry, time.Duration(delay) * time.Second
	}

	// 这些错误表示应重试的瞬态错误。
	if apierrors.IsTimeout(err) ||
		apierrors.IsTooManyRequests(err) ||
		apierrors.IsServiceUnavailable(err) ||
		errors.Is(err, io.EOF) ||
		errors.As(err, &transientError{}) {
		return true, 0
	}

	return false, 0
}

// RetryNotFound 包装任意 get 函数。当被包装的函数
// 遇到 "not found" 错误时，将其视为瞬态问题
// 并继续轮询。
//
// 这旨在用于 [gomega.Eventually] 或 [gomega.Consistently] 内部。
func RetryNotFound[T any](get GetFunc[T]) GetFunc[T] {
	return func(ctx context.Context) (T, error) {
		t, err := get(ctx)
		if apierrors.IsNotFound(err) {
			// 如果包装的是 HandleRetry，错误将是
			// gomega.StopTrying。需要去掉它，
			// 否则 gomega.Eventually 会停止。
			var stopTryingErr gomega.PollingSignalError
			if errors.As(err, &stopTryingErr) {
				if wrappedErr := errors.Unwrap(stopTryingErr); wrappedErr != nil {
					err = wrappedErr
				}
			}

			// 将错误标记为瞬态，以防被
			// HandleRetry 包装。
			err = transientError{error: err}
		}
		return t, err
	}
}

// transientError 包装另一个错误，表示
// 该错误是可能消失的瞬态错误。
type transientError struct {
	error
}

func (err transientError) Unwrap() error {
	return err.error
}
