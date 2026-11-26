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

// Package routine 为长时请求（watch）提供“准备阶段与流式阶段分离”的辅助：
// 准备阶段挂载的收尾任务会在请求处理结束后执行，从而可以尽早释放准备阶段
// 占用的栈与资源。
package routine

import (
	"context"
	"net/http"

	"k8s.io/apiserver/pkg/endpoints/request"
)

// taskKeyType 是私有的 context key 类型，避免与其他包的 key 冲突。
type taskKeyType int

const taskKey taskKeyType = iota

// Task 携带一个在长时请求准备阶段结束后执行的回调。
// 典型用途：请求已经切换为 watch 流后，释放准备阶段占用的资源（如锁）。
type Task struct {
	Func func()
}

// WithTask 返回携带 t 的 context，供中间件在准备阶段挂载收尾任务。
func WithTask(parent context.Context, t *Task) context.Context {
	return request.WithValue(parent, taskKey, t)
}

// AppendTask 在当前任务完成后追加一个任务；当前没有任务时为空操作。
//
// 返回 false 表示当前上下文未经过 WithRoutine，调用方需自行处理任务，
// 否则会静默丢弃收尾逻辑（例如锁永远不释放）。
func AppendTask(ctx context.Context, t *Task) bool {
	if existTask := TaskFrom(ctx); existTask != nil && existTask.Func != nil {
		existFunc := existTask.Func
		existTask.Func = func() {
			existFunc()
			t.Func()
		}
		return true
	}
	return false
}

// TaskFrom 返回 ctx 上挂载的任务，没有则返回 nil。
func TaskFrom(ctx context.Context) *Task {
	t, _ := ctx.Value(taskKey).(*Task)
	return t
}

// WithRoutine 返回的 handler 在独立 goroutine 中完成长请求（如 watch）的准备工作，
// 再由主 goroutine 处理该请求。这样准备阶段占用的栈内存可以及时释放，降低内存占用。
//
// 注意：整个长时请求在 goroutine 中处理，主 goroutine 阻塞等待其结束；
// goroutine 内的 panic 会被重新抛到主 goroutine，保证 net/http 的 panic
// 恢复机制仍然生效。
func WithRoutine(handler http.Handler, longRunning request.LongRunningRequestCheck) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx := req.Context()
		requestInfo, _ := request.RequestInfoFrom(ctx)
		if !longRunning(req, requestInfo) {
			handler.ServeHTTP(w, req)
			return
		}

		// 为本次请求挂一个空 Task；后续中间件可通过 AppendTask 追加收尾动作。
		req = req.WithContext(WithTask(ctx, &Task{}))
		// 带缓冲的 channel：即使主 goroutine 已继续执行，panic 值也不会阻塞发送。
		panicCh := make(chan any, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					panicCh <- r
				}
				close(panicCh)
			}()
			handler.ServeHTTP(w, req)
		}()

		// 准备阶段完成（或发生 panic）。panic 必须在主 goroutine 重新抛出，
		// 否则 net/http 的 recover 无法捕获，进程会崩溃。
		if p, ok := <-panicCh; ok {
			panic(p)
		}

		// 长时请求处理结束后执行收尾任务（AppendTask 追加的回调）。
		ctx = req.Context()
		if t := TaskFrom(ctx); t != nil && t.Func != nil {
			t.Func()
		}

	})
}
