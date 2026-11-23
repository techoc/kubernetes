/*
Copyright 2019 The Kubernetes Authors.

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
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"

	"k8s.io/apimachinery/pkg/util/sets"
)

// 共享的 Status 实例，用于验证“同一个指针”以及错误链包装后的相等性判断。
var errorStatus = NewStatus(Error, "internal error")
var statusWithErr = AsStatus(errors.New("internal error"))

// Status 是插件之间传递调度结果的主要载体，零值 nil 被约定为成功。
// 测试覆盖每种 Code 及其对应的判定方法，重点锁住 nil 与 Success 等价这一隐式约定。
func TestStatus(t *testing.T) {
	tests := []struct {
		name              string
		status            *Status
		expectedCode      Code
		expectedMessage   string
		expectedIsSuccess bool
		expectedIsWait    bool
		expectedIsSkip    bool
		expectedAsError   error
	}{
		{
			name:              "success status",
			status:            NewStatus(Success, ""),
			expectedCode:      Success,
			expectedMessage:   "",
			expectedIsSuccess: true,
			expectedIsWait:    false,
			expectedIsSkip:    false,
			expectedAsError:   nil,
		},
		{
			name:              "wait status",
			status:            NewStatus(Wait, ""),
			expectedCode:      Wait,
			expectedMessage:   "",
			expectedIsSuccess: false,
			expectedIsWait:    true,
			expectedIsSkip:    false,
			expectedAsError:   nil,
		},
		{
			name:              "error status",
			status:            NewStatus(Error, "unknown error"),
			expectedCode:      Error,
			expectedMessage:   "unknown error",
			expectedIsSuccess: false,
			expectedIsWait:    false,
			expectedIsSkip:    false,
			expectedAsError:   errors.New("unknown error"),
		},
		{
			name:              "skip status",
			status:            NewStatus(Skip, ""),
			expectedCode:      Skip,
			expectedMessage:   "",
			expectedIsSuccess: false,
			expectedIsWait:    false,
			expectedIsSkip:    true,
			expectedAsError:   nil,
		},
		{
			name:              "nil status",
			status:            nil,
			expectedCode:      Success,
			expectedMessage:   "",
			expectedIsSuccess: true,
			expectedIsSkip:    false,
			expectedAsError:   nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.status.Code() != test.expectedCode {
				t.Errorf("expect status.Code() returns %v, but %v", test.expectedCode, test.status.Code())
			}

			if test.status.Message() != test.expectedMessage {
				t.Errorf("expect status.Message() returns %v, but %v", test.expectedMessage, test.status.Message())
			}

			if test.status.IsSuccess() != test.expectedIsSuccess {
				t.Errorf("expect status.IsSuccess() returns %v, but %v", test.expectedIsSuccess, test.status.IsSuccess())
			}

			if test.status.IsWait() != test.expectedIsWait {
				t.Errorf("status.IsWait() returns %v, but want %v", test.status.IsWait(), test.expectedIsWait)
			}

			if test.status.IsSkip() != test.expectedIsSkip {
				t.Errorf("status.IsSkip() returns %v, but want %v", test.status.IsSkip(), test.expectedIsSkip)
			}

			if errors.Is(test.status.AsError(), test.expectedAsError) {
				return
			}

			if test.status.AsError().Error() != test.expectedAsError.Error() {
				t.Errorf("expect status.AsError() returns %v, but %v", test.expectedAsError, test.status.AsError())
			}
		})
	}
}

// PreFilterResult.Merge 对节点名集合取交集：每个插件的过滤结果都会进一步缩小候选集。
// 表中特意覆盖 nil 与空集合的不同组合，防止把“未设置”误解为“无限制”而放大候选集。
func TestPreFilterResultMerge(t *testing.T) {
	tests := map[string]struct {
		receiver *PreFilterResult
		in       *PreFilterResult
		want     *PreFilterResult
	}{
		"all nil": {},
		"nil receiver empty input": {
			in:   &PreFilterResult{NodeNames: sets.New[string]()},
			want: &PreFilterResult{NodeNames: sets.New[string]()},
		},
		"empty receiver nil input": {
			receiver: &PreFilterResult{NodeNames: sets.New[string]()},
			want:     &PreFilterResult{NodeNames: sets.New[string]()},
		},
		"empty receiver empty input": {
			receiver: &PreFilterResult{NodeNames: sets.New[string]()},
			in:       &PreFilterResult{NodeNames: sets.New[string]()},
			want:     &PreFilterResult{NodeNames: sets.New[string]()},
		},
		"nil receiver populated input": {
			in:   &PreFilterResult{NodeNames: sets.New("node1")},
			want: &PreFilterResult{NodeNames: sets.New("node1")},
		},
		"empty receiver populated input": {
			receiver: &PreFilterResult{NodeNames: sets.New[string]()},
			in:       &PreFilterResult{NodeNames: sets.New("node1")},
			want:     &PreFilterResult{NodeNames: sets.New[string]()},
		},

		"populated receiver nil input": {
			receiver: &PreFilterResult{NodeNames: sets.New("node1")},
			want:     &PreFilterResult{NodeNames: sets.New("node1")},
		},
		"populated receiver empty input": {
			receiver: &PreFilterResult{NodeNames: sets.New("node1")},
			in:       &PreFilterResult{NodeNames: sets.New[string]()},
			want:     &PreFilterResult{NodeNames: sets.New[string]()},
		},
		"populated receiver and input": {
			receiver: &PreFilterResult{NodeNames: sets.New("node1", "node2")},
			in:       &PreFilterResult{NodeNames: sets.New("node2", "node3")},
			want:     &PreFilterResult{NodeNames: sets.New("node2")},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.receiver.Merge(test.in)
			if diff := cmp.Diff(test.want, got); diff != "" {
				t.Errorf("unexpected diff (-want, +got):\n%s", diff)
			}
		})
	}
}

// Equal 用于比较插件前后两次调度结果，决定是否要把 Pod 重新入队。
// 实践中容易被错误链包装，因此这里专门验证包装后的 Error 状态仍被视为相等，
// 而两个仅消息相同的独立 error 不相等，避免把偶发错误误判成持续状态。
func TestIsStatusEqual(t *testing.T) {
	tests := []struct {
		name string
		x, y *Status
		want bool
	}{
		{
			name: "two nil should be equal",
			x:    nil,
			y:    nil,
			want: true,
		},
		{
			name: "nil should be equal to success status",
			x:    nil,
			y:    NewStatus(Success),
			want: true,
		},
		{
			name: "nil should not be equal with status except success",
			x:    nil,
			y:    NewStatus(Error, "internal error"),
			want: false,
		},
		{
			name: "one status should be equal to itself",
			x:    errorStatus,
			y:    errorStatus,
			want: true,
		},
		{
			name: "same type statuses without reasons should be equal",
			x:    NewStatus(Success),
			y:    NewStatus(Success),
			want: true,
		},
		{
			name: "statuses with same message should be equal",
			x:    NewStatus(Unschedulable, "unschedulable"),
			y:    NewStatus(Unschedulable, "unschedulable"),
			want: true,
		},
		{
			name: "error statuses with same message should be equal",
			x:    NewStatus(Error, "error"),
			y:    NewStatus(Error, "error"),
			want: true,
		},
		{
			name: "statuses with different reasons should not be equal",
			x:    NewStatus(Unschedulable, "unschedulable"),
			y:    NewStatus(Unschedulable, "unschedulable", "injected filter status"),
			want: false,
		},
		{
			name: "statuses with different codes should not be equal",
			x:    NewStatus(Error, "internal error"),
			y:    NewStatus(Unschedulable, "internal error"),
			want: false,
		},
		{
			name: "wrap error status should be equal with original one",
			x:    statusWithErr,
			y:    AsStatus(fmt.Errorf("error: %w", statusWithErr.AsError())),
			want: true,
		},
		{
			name: "statues with different errors that have the same message shouldn't be equal",
			x:    AsStatus(errors.New("error")),
			y:    AsStatus(errors.New("error")),
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.x.Equal(tt.y); got != tt.want {
				t.Errorf("cmp.Equal() = %v, want %v", got, tt.want)
			}
		})
	}
}
