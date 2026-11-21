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

package admission

import (
	"errors"
	"testing"
)

// TestAdmissionError 是测试用的准入错误类型。
type TestAdmissionError struct {
	// message 是错误消息。
	message string
	// reason 是错误原因。
	reason string
}

// Error 返回错误消息。
func (e *TestAdmissionError) Error() string {
	return e.message
}

// Type 返回错误原因。
func (e *TestAdmissionError) Type() string {
	return e.reason
}

// TestAdmissionErrors 验证 GetPodAdmitResult 对各类错误的转换结果。
func TestAdmissionErrors(t *testing.T) {
	testCases := []struct {
		Error                  error
		expectedAdmissionError bool
	}{
		{
			nil,
			false,
		},
		{
			errors.New("Not an AdmissionError error"),
			false,
		},
		{
			&TestAdmissionError{
				"Is an AdmissionError error",
				"TestAdmissionError",
			},
			true,
		},
	}

	// 覆盖 nil、普通错误与实现 AdmissionError 接口的错误。
	for _, tc := range testCases {
		// nil 错误应返回准入成功。
		h := GetPodAdmitResult(tc.Error)
		if tc.Error == nil {
			if !h.Admit {
				t.Errorf("expected PodAdmitResult.Admit = true")
			}
			continue
		}

		if h.Admit {
			t.Errorf("expected PodAdmitResult.Admit = false")
		}

		if tc.expectedAdmissionError {
			err, ok := tc.Error.(*TestAdmissionError)
			if !ok {
				t.Errorf("expected TestAdmissionError")
			}
			if h.Reason != err.reason {
				t.Errorf("expected PodAdmitResult.Reason = %v, got %v", err.reason, h.Reason)
			}
			continue
		}

		if h.Reason != ErrorReasonUnexpected {
			t.Errorf("expected PodAdmitResult.Reason = %v, got %v", ErrorReasonUnexpected, h.Reason)
		}
	}
}
