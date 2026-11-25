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
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/apis/example"

	"github.com/google/go-cmp/cmp"
)

// TestFinishRequest 验证 FinishRequest 的返回值语义：
// 成功状态对象原样返回，失败状态对象转为 StatusError，panic 向上传播；
// http.ErrAbortHandler 特殊处理——不附加堆栈，保持原值以便 net/http 识别。
func TestFinishRequest(t *testing.T) {
	exampleObj := &example.Pod{}
	exampleErr := fmt.Errorf("error")
	successStatusObj := &metav1.Status{Status: metav1.StatusSuccess, Message: "success message"}
	errorStatusObj := &metav1.Status{Status: metav1.StatusFailure, Message: "error message"}
	timeoutFunc := func() (context.Context, context.CancelFunc) {
		return context.WithTimeout(context.TODO(), time.Second)
	}

	testcases := []struct {
		name          string
		timeout       func() (context.Context, context.CancelFunc)
		fn            ResultFunc
		expectedObj   runtime.Object
		expectedErr   error
		expectedPanic string

		expectedPanicObj interface{}
	}{
		{
			name:    "Expected obj is returned",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				return exampleObj, nil
			},
			expectedObj: exampleObj,
			expectedErr: nil,
		},
		{
			name:    "Expected error is returned",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				return nil, exampleErr
			},
			expectedObj: nil,
			expectedErr: exampleErr,
		},
		{
			name:    "No expected error or object or panic",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				return nil, nil
			},
		},
		{
			name:    "Successful status object is returned as expected",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				return successStatusObj, nil
			},
			expectedObj: successStatusObj,
			expectedErr: nil,
		},
		{
			name:    "Error status object is converted to StatusError",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				return errorStatusObj, nil
			},
			expectedObj: nil,
			expectedErr: apierrors.FromObject(errorStatusObj),
		},
		{
			name:    "Panic is propagated up",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				panic("my panic")
			},
			expectedObj:   nil,
			expectedErr:   nil,
			expectedPanic: "my panic",
		},
		{
			name:    "Panic is propagated with stack",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				panic("my panic")
			},
			expectedObj:   nil,
			expectedErr:   nil,
			expectedPanic: "finisher_test.go",
		},
		{
			name:    "http.ErrAbortHandler panic is propagated without wrapping with stack",
			timeout: timeoutFunc,
			fn: func() (runtime.Object, error) {
				panic(http.ErrAbortHandler)
			},
			expectedObj:      nil,
			expectedErr:      nil,
			expectedPanic:    http.ErrAbortHandler.Error(),
			expectedPanicObj: http.ErrAbortHandler,
		},
	}
	for i, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := tc.timeout()
			defer func() {
				cancel()

				r := recover()
				switch {
				case r == nil && len(tc.expectedPanic) > 0:
					t.Errorf("expected panic containing '%s', got none", tc.expectedPanic)
				case r != nil && len(tc.expectedPanic) == 0:
					t.Errorf("unexpected panic: %v", r)
				case r != nil && len(tc.expectedPanic) > 0 && !strings.Contains(fmt.Sprintf("%v", r), tc.expectedPanic):
					t.Errorf("expected panic containing '%s', got '%v'", tc.expectedPanic, r)
				}

				if tc.expectedPanicObj != nil && !reflect.DeepEqual(tc.expectedPanicObj, r) {
					t.Errorf("expected panic obj %#v, got %#v", tc.expectedPanicObj, r)
				}
			}()
			obj, err := FinishRequest(ctx, tc.fn)
			if (err == nil && tc.expectedErr != nil) || (err != nil && tc.expectedErr == nil) || (err != nil && tc.expectedErr != nil && err.Error() != tc.expectedErr.Error()) {
				t.Errorf("%d: unexpected err. expected: %v, got: %v", i, tc.expectedErr, err)
			}
			if !apiequality.Semantic.DeepEqual(obj, tc.expectedObj) {
				t.Errorf("%d: unexpected obj. expected %#v, got %#v", i, tc.expectedObj, obj)
			}
		})
	}
}

// TestFinishRequestWithPostTimeoutTracker 验证请求超时后 ResultFunc 的收尾行为：
// 父 goroutine 立即返回超时错误，但后台仍等待 ResultFunc 结束并把结果交给
// post-timeout logger；若 ResultFunc 永不返回，父 goroutine 在 postTimeoutWait
// 之后放弃，避免 goroutine 永久泄漏。
func TestFinishRequestWithPostTimeoutTracker(t *testing.T) {
	tests := []struct {
		name                       string
		object                     runtime.Object
		postTimeoutWait            time.Duration
		childGoroutineNeverReturns bool
		err                        error
		reason                     string
	}{
		{
			name:            "ResultFunc function returns a result after the request had timed out",
			object:          &example.Pod{},
			postTimeoutWait: 5 * time.Minute,
		},
		{
			name:            "ResultFunc function returns an error after the request had timed out",
			err:             errors.New("my error"),
			postTimeoutWait: 5 * time.Minute,
		},
		{
			name:            "ResultFunc function panics after the request had timed out",
			reason:          "my panic",
			postTimeoutWait: 5 * time.Minute,
		},
		{
			name:                       "ResultFunc function never returns, parent gives up after postTimeoutWait",
			postTimeoutWait:            1 * time.Second,
			childGoroutineNeverReturns: true,
		},
	}

	expectedTimeoutErr := apierrors.NewTimeoutError(fmt.Sprintf("request did not complete within requested timeout - %s",
		context.DeadlineExceeded), 0)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// 立即超时的 context：确保 FinishRequest 走超时分支。
			ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond)
			defer cancel()

			timeoutAsDesignedCh, resultFuncDoneCh := make(chan struct{}), make(chan struct{})
			resultFn := func() (runtime.Object, error) {
				defer func() {
					if test.childGoroutineNeverReturns {
						// 比 test.postTimeoutWait 多睡一会儿，
						// 让 post-timeout 监控放弃等待。
						time.Sleep(test.postTimeoutWait + time.Second)
					}
					close(resultFuncDoneCh)
				}()

				// 在这里阻塞
				<-timeoutAsDesignedCh

				if len(test.reason) > 0 {
					panic(test.reason)
				}
				if test.err != nil && test.object != nil {
					t.Fatal("both result and err are set, wrong test setup")
				}

				return test.object, test.err
			}

			var resultGot *result
			postTimeoutLoggerCompletedCh := make(chan struct{})
			decoratedPostTimeoutLogger := func(timedOutAt time.Time, r *result) {
				defer func() {
					resultGot = r
					close(postTimeoutLoggerCompletedCh)
				}()

				logPostTimeoutResult(timedOutAt, r)
			}

			_, err := finishRequest(ctx, resultFn, test.postTimeoutWait, decoratedPostTimeoutLogger)
			if err == nil || err.Error() != expectedTimeoutErr.Error() {
				t.Errorf("expected timeout error: %v, but got: %v", expectedTimeoutErr, err)
			}

			// 其余的 ResultFunc 仍在运行，解除阻塞让它完成
			close(timeoutAsDesignedCh)

			t.Log("waiting for the ResultFunc rest function to finish")
			<-resultFuncDoneCh

			t.Log("waiting for the post-timeout logger to return")
			<-postTimeoutLoggerCompletedCh

			switch {
			case test.childGoroutineNeverReturns && resultGot != nil:
				t.Fatal("expected the result for the post-timeout logger to be nil")
			case test.childGoroutineNeverReturns:
				// resultGot 为 nil，无需再验证
				return
			case !test.childGoroutineNeverReturns && resultGot == nil:
				t.Fatal("expected a result for the post-timeout logger, but got nil")
			}

			if test.object != resultGot.object {
				t.Errorf("expected object to match, diff: %s", cmp.Diff(test.object, resultGot.object))
			}
			if test.err != resultGot.err {
				t.Errorf("expected err: %v, but got: %v", test.err, resultGot.err)
			}

			switch {
			case len(test.reason) == 0:
				if resultGot.reason != nil {
					t.Errorf("unexpected panic: %v", resultGot.reason)
				}
			case !strings.Contains(fmt.Sprintf("%v", resultGot.reason), test.reason):
				t.Errorf("expected panic to contain: %q, but got: %v", test.reason, resultGot.reason)
			}
		})
	}
}
