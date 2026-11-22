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

package routine

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"k8s.io/apiserver/pkg/endpoints/request"
)

// TestExecutionWithRoutine 验证长时请求路径下，handler 设置的任务会在
// 请求结束后被执行（锁释放等收尾逻辑依赖此行为）。
func TestExecutionWithRoutine(t *testing.T) {
	var executed bool
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t := TaskFrom(r.Context())
		t.Func = func() {
			executed = true
		}
	})
	ts := httptest.NewServer(WithRoutine(handler, func(_ *http.Request, _ *request.RequestInfo) bool { return true }))
	defer ts.Close()

	_, err := http.Get(ts.URL)
	if err != nil {
		t.Errorf("got unexpected error on request: %v", err)
	}
	if !executed {
		t.Error("expected to execute")
	}
}

// TestAppendTask 验证 AppendTask 只在上下文已有 Task 时生效：
// 没有 Task 时返回 false 且回调不得执行，调用方据此决定是否自行处理。
func TestAppendTask(t *testing.T) {
	tests := []struct {
		name          string
		existingTask  bool
		taskAppended  bool
		shouldExecute bool
	}{
		{
			name:          "append task when existing",
			existingTask:  true,
			taskAppended:  true,
			shouldExecute: true,
		},
		{
			name:          "not append task when no existing tasks",
			existingTask:  false,
			taskAppended:  false,
			shouldExecute: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var executed, appended bool
			taskToAppend := func() {
				executed = true
			}
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				if test.existingTask {
					t := TaskFrom(ctx)
					t.Func = func() {}
				}

				// 关键断言：返回值的语义是“是否成功追加”，而不是“是否执行”。
				appended = AppendTask(ctx, &Task{taskToAppend})
			})
			ts := httptest.NewServer(WithRoutine(handler, func(_ *http.Request, _ *request.RequestInfo) bool { return true }))
			defer ts.Close()

			_, err := http.Get(ts.URL)
			if err != nil {
				t.Errorf("got unexpected error on request: %v", err)
			}

			if test.taskAppended != appended {
				t.Errorf("expected taskAppended: %t, got: %t", test.taskAppended, executed)
			}

			if test.shouldExecute != executed {
				t.Errorf("expected shouldExecute: %t, got: %t", test.shouldExecute, executed)
			}
		})
	}
}
