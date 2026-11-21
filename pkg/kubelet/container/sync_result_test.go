/*
Copyright 2015 The Kubernetes Authors.

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

package container

import (
	"errors"
	"fmt"
	"testing"
	"time"

	utilerrors "k8s.io/apimachinery/pkg/util/errors"
)

// TestPodSyncResult 验证 PodSyncResult 在成功、失败与嵌套结果下的错误聚合行为。
func TestPodSyncResult(t *testing.T) {
	// 构造成功与失败两组结果，供后续断言使用。
	okResults := []*SyncResult{
		NewSyncResult(StartContainer, "container_0"),
		NewSyncResult(CreatePodSandbox, "pod"),
	}
	errResults := []*SyncResult{
		NewSyncResult(KillContainer, "container_1"),
		NewSyncResult(KillPodSandbox, "pod"),
	}
	errResults[0].Fail(errors.New("error_0"), "message_0")
	errResults[1].Fail(errors.New("error_1"), "message_1")

	// 不含错误结果时不应产生错误。
	result := PodSyncResult{}
	result.AddSyncResult(okResults...)
	if result.Error() != nil {
		t.Errorf("PodSyncResult should not be error: %v", result)
	}

	// 含错误结果时应返回错误。
	result = PodSyncResult{}
	result.AddSyncResult(okResults...)
	result.AddSyncResult(errResults...)
	if result.Error() == nil {
		t.Errorf("PodSyncResult should be error: %v", result)
	}

	// 显式标记失败时应返回错误。
	result = PodSyncResult{}
	result.AddSyncResult(okResults...)
	result.Fail(errors.New("error"))
	if result.Error() == nil {
		t.Errorf("PodSyncResult should be error: %v", result)
	}

	// 合并含错误的 PodSyncResult 后应返回错误。
	errResult := PodSyncResult{}
	errResult.AddSyncResult(errResults...)
	result = PodSyncResult{}
	result.AddSyncResult(okResults...)
	result.AddPodSyncResult(errResult)
	if result.Error() == nil {
		t.Errorf("PodSyncResult should be error: %v", result)
	}
}

// myCustomError 是测试用的自定义错误类型。
type myCustomError struct {
	msg string
}

// Error 返回自定义错误消息。
func (e *myCustomError) Error() string {
	return e.msg
}

// TestPodSyncResultPreservesOriginalErrorType 验证聚合错误保留原始错误类型，
// 便于通过 errors.As 识别。
func TestPodSyncResultPreservesOriginalErrorType(t *testing.T) {
	t.Run("with custom error in SyncResult", func(t *testing.T) {
		customErr := &myCustomError{msg: "a special error"}
		syncResult := NewSyncResult(KillContainer, "container_1")
		syncResult.Fail(customErr, "a special message")
		result := &PodSyncResult{
			SyncResults: []*SyncResult{syncResult},
		}

		aggErr := result.Error()
		if aggErr == nil {
			t.Fatal("expected an aggregate error, but got nil")
		}

		// 聚合错误应可通过 errors.As 还原为 utilerrors.Aggregate。
		var ae utilerrors.Aggregate
		if !errors.As(aggErr, &ae) {
			t.Fatalf("expected an aggregate error, but got %q", aggErr)
		}
		errs := ae.Errors()
		// 逐个检查聚合错误中的元素，确认原始自定义错误仍在其中。
		foundCustomErr := false
		for _, err := range errs {
			var ce *myCustomError
			if errors.As(err, &ce) && ce == customErr {
				foundCustomErr = true
			}
		}
		if !foundCustomErr {
			t.Errorf("expected custom error not found in aggregate error. got %q, want %q", aggErr, customErr)
		}
	})

	t.Run("with custom error in SyncError", func(t *testing.T) {
		customErr := &myCustomError{msg: "a special sync error"}
		result := &PodSyncResult{}
		result.Fail(customErr)

		aggErr := result.Error()
		if aggErr == nil {
			t.Fatal("expected an aggregate error for SyncError, but got nil")
		}

		// SyncError 路径同样应保留原始错误类型。
		var ae utilerrors.Aggregate
		if !errors.As(aggErr, &ae) {
			t.Fatalf("expected an aggregate error, but got %q", aggErr)
		}
		errs := ae.Errors()
		// 逐个检查聚合错误中的元素，确认原始自定义错误仍在其中。
		foundCustomErr := false
		for _, err := range errs {
			var ce *myCustomError
			if errors.As(err, &ce) && ce == customErr {
				foundCustomErr = true
			}
		}
		if !foundCustomErr {
			t.Errorf("expected custom error not found in aggregate error. got %q, want %q", aggErr, customErr)
		}
	})
}

// TestMinBackoffExpiration 验证从各种错误形态中提取最小退避时间的逻辑。
func TestMinBackoffExpiration(t *testing.T) {
	now := time.Now()
	// 覆盖 nil、普通错误、包装错误、聚合与嵌套聚合等场景。
	testCases := []struct {
		name            string
		err             error
		expectedBackoff time.Time
		expectedFound   bool
	}{
		{
			name:            "nil error",
			err:             nil,
			expectedBackoff: time.Time{},
			expectedFound:   false,
		},
		{
			name:            "simple error",
			err:             errors.New("generic error"),
			expectedBackoff: time.Time{},
			expectedFound:   false,
		},
		{
			name:            "BackoffError",
			err:             NewBackoffError(errors.New("backoff"), now.Add(5*time.Second)),
			expectedBackoff: now.Add(5 * time.Second),
			expectedFound:   true,
		},
		{
			name:            "wrapped BackoffError",
			err:             fmt.Errorf("wrapped: %w", NewBackoffError(errors.New("backoff"), now.Add(3*time.Second))),
			expectedBackoff: now.Add(3 * time.Second),
			expectedFound:   true,
		},
		{
			name:            "aggregate with no BackoffError",
			err:             utilerrors.NewAggregate([]error{errors.New("err1"), errors.New("err2")}),
			expectedBackoff: time.Time{},
			expectedFound:   false,
		},
		{
			name: "aggregate with one BackoffError",
			err: utilerrors.NewAggregate([]error{
				errors.New("err1"),
				NewBackoffError(errors.New("backoff"), now.Add(7*time.Second)),
			}),
			expectedBackoff: now.Add(7 * time.Second),
			expectedFound:   true,
		},
		{
			name: "aggregate with multiple BackoffErrors, returns minimum",
			err: utilerrors.NewAggregate([]error{
				NewBackoffError(errors.New("backoff1"), now.Add(10*time.Second)),
				NewBackoffError(errors.New("backoff2"), now.Add(3*time.Second)),
				errors.New("err1"),
				NewBackoffError(errors.New("backoff3"), now.Add(5*time.Second)),
			}),
			expectedBackoff: now.Add(3 * time.Second),
			expectedFound:   true,
		},
		{
			name: "wrapped aggregate with BackoffError",
			err: fmt.Errorf("wrapped: %w", utilerrors.NewAggregate([]error{
				NewBackoffError(errors.New("backoff1"), now.Add(10*time.Second)),
				NewBackoffError(errors.New("backoff2"), now.Add(3*time.Second)),
			})),
			expectedBackoff: now.Add(3 * time.Second),
			expectedFound:   true,
		},
		{
			name: "nested aggregate with BackoffError",
			err: utilerrors.NewAggregate([]error{
				errors.New("err1"),
				utilerrors.NewAggregate([]error{
					NewBackoffError(errors.New("backoff nested"), now.Add(2*time.Second)),
				}),
				NewBackoffError(errors.New("backoff outer"), now.Add(4*time.Second)),
			}),
			expectedBackoff: now.Add(2 * time.Second),
			expectedFound:   true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// 校验是否找到退避时间，以及退避时间点是否一致。
			backoff, found := MinBackoffExpiration(tc.err)
			if found != tc.expectedFound {
				t.Errorf("expected found=%t, got %t", tc.expectedFound, found)
			}
			if !backoff.Equal(tc.expectedBackoff) {
				t.Errorf("expected backoff=%v, got %v", tc.expectedBackoff, backoff)
			}
		})
	}
}
