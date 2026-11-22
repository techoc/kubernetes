/*
Copyright 2017 The Kubernetes Authors.

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

package errors

import (
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestToStatusErr 验证 webhook 返回的 metav1.Status 如何被转换为 StatusError。
// 关键不变量：无论 webhook 返回什么，最终对客户端都是失败状态，且状态码
// 只接受 4xx/5xx；缺失信息时补上 "without explanation" 以保持可读性。
func TestToStatusErr(t *testing.T) {
	hookName := "foo"
	deniedBy := fmt.Sprintf("admission webhook %q denied the request", hookName)
	tests := []struct {
		name           string
		result         *metav1.Status
		expectedError  string
		expectedCode   int32
		expectedStatus string
	}{
		{
			// 完全没有 Result 时也必须给出可读的拒绝原因。
			"nil result",
			nil,
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			// Message 优先于 Reason 用于拼装错误文本。
			"only message",
			&metav1.Status{
				Message: "you shall not pass",
			},
			deniedBy + ": you shall not pass",
			400,
			metav1.StatusFailure,
		},
		{
			"only reason",
			&metav1.Status{
				Reason: metav1.StatusReasonForbidden,
			},
			deniedBy + ": Forbidden",
			400,
			metav1.StatusFailure,
		},
		{
			"message and reason",
			&metav1.Status{
				Message: "you shall not pass",
				Reason:  metav1.StatusReasonForbidden,
			},
			deniedBy + ": you shall not pass",
			400,
			metav1.StatusFailure,
		},
		{
			"no message, no reason",
			&metav1.Status{},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			"custom 4xx status code",
			&metav1.Status{Code: 401},
			deniedBy + " without explanation",
			401,
			metav1.StatusFailure,
		},
		{
			"custom 5xx status code",
			&metav1.Status{Code: 500},
			deniedBy + " without explanation",
			500,
			metav1.StatusFailure,
		},
		{
			// 2xx/3xx 对拒绝语义没有意义，统一回落为 400，避免出现
			// “200 表示拒绝”这类自相矛盾的响应。
			"200 status code",
			&metav1.Status{Code: 200},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			"300 status code",
			&metav1.Status{Code: 300},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			"399 status code",
			&metav1.Status{Code: 399},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			"missing status",
			&metav1.Status{},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			// Status 字段为 Success 与拒绝语义冲突，必须覆盖为 Failure。
			"success status overridden",
			&metav1.Status{Status: metav1.StatusSuccess},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			"failure status preserved",
			&metav1.Status{Status: metav1.StatusFailure},
			deniedBy + " without explanation",
			400,
			metav1.StatusFailure,
		},
		{
			// 非标准 Status 值（如 "custom"）原样保留，便于 webhook 作者
			// 表达自定义语义。
			"custom status preserved",
			&metav1.Status{Status: "custom"},
			deniedBy + " without explanation",
			400,
			"custom",
		},
	}
	for _, test := range tests {
		err := ToStatusErr(hookName, test.result)
		// 三处断言分别覆盖错误文本、HTTP 状态码与 Status 字段：任何一处
		// 回退都会改变客户端可见的拒绝行为。
		if err == nil || err.Error() != test.expectedError {
			t.Errorf("%s: expected an error saying %q, but got %v", test.name, test.expectedError, err)
		}
		if err.ErrStatus.Code != test.expectedCode {
			t.Errorf("%s: expected code %d, got %d", test.name, test.expectedCode, err.ErrStatus.Code)
		}
		if err.ErrStatus.Status != test.expectedStatus {
			t.Errorf("%s: expected code %q, got %q", test.name, test.expectedStatus, err.ErrStatus.Status)
		}
	}
}
