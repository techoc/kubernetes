/*
Copyright The Kubernetes Authors.

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

// Package filesystem 提供带轮询兜底的文件监听工具：当平台不支持或不允许
// inotify/kqueue 时，仍能以固定间隔触发回调。
package filesystem

import (
	"context"
	"fmt"
	"time"

	"github.com/fsnotify/fsnotify"
)

// watchAddRemover 抽象 fsnotify.Watcher 的增删监听能力，便于替换为 no-op。
type watchAddRemover interface {
	Add(path string) error
	Remove(path string) error
}

// noopWatcher 在无法创建真实 watcher 时使用，使调用方退化为纯轮询。
type noopWatcher struct{}

func (noopWatcher) Add(path string) error    { return nil }
func (noopWatcher) Remove(path string) error { return nil }

// WatchUntil watches the specified path for changes and blocks until ctx is canceled.
// eventHandler() must be non-nil, and pollInterval must be greater than 0.
// eventHandler() is invoked whenever a change event is observed or pollInterval elapses.
// errorHandler() is invoked (if non-nil) whenever an error occurs initializing or watching the specified path.
//
// If path is a directory, only the directory and immediate children are watched.
//
// If path does not exist or cannot be watched, an error is passed to errorHandler() and eventHandler() is called at pollInterval.
//
// Multiple observed events may collapse to a single invocation of eventHandler().
//
// eventHandler() is invoked immediately after successful initialization of the filesystem watch,
// in case the path changed concurrent with calling WatchUntil().
//
// 设计取舍：事件与轮询双通道。事件提供低延迟，轮询保证在事件丢失（如目录被
// 整体替换）时仍能收敛；因此 eventHandler 必须幂等，可能被重复调用。
func WatchUntil(ctx context.Context, pollInterval time.Duration, path string, eventHandler func(), errorHandler func(err error)) {
	if pollInterval <= 0 {
		panic(fmt.Errorf("pollInterval must be > 0"))
	}
	if eventHandler == nil {
		panic(fmt.Errorf("eventHandler must be non-nil"))
	}
	if errorHandler == nil {
		errorHandler = func(err error) {}
	}

	// Initialize watcher, fall back to no-op.
	// 创建 watcher 失败不应致命：调用方通常依赖此回调来加载证书等关键数据，
	// 宁可退化为较慢的轮询，也不要直接退出。
	var (
		eventsCh chan fsnotify.Event
		errorCh  chan error
		watcher  watchAddRemover
	)
	if w, err := fsnotify.NewWatcher(); err != nil {
		errorHandler(fmt.Errorf("error creating file watcher, falling back to poll at interval %s: %w", pollInterval, err))
		watcher = noopWatcher{}
	} else {
		watcher = w
		eventsCh = w.Events
		errorCh = w.Errors
		defer func() {
			_ = w.Close()
		}()
	}

	// Initialize background poll
	// 即使 watcher 正常，轮询也始终运行：作为事件通道的兜底。
	t := time.NewTicker(pollInterval)
	defer t.Stop()

	attemptPeriodicRewatch := false

	// Start watching the path
	// 首次 Add 失败时不在 select 循环里立即重试，而是交给周期性 tick，
	// 避免路径长期不存在时形成忙等。
	if err := watcher.Add(path); err != nil {
		errorHandler(err)
		attemptPeriodicRewatch = true
	} else {
		// Invoke handle() at least once after successfully registering the listener,
		// in case the file changed concurrent with calling WatchUntil.
		eventHandler()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-t.C:
			// Prioritize exiting if context is canceled
			// ctx.Done 与 t.C 可能同时就绪，显式检查避免取消后仍触发一次回调。
			if ctx.Err() != nil {
				return
			}

			// Try to re-establish the watcher if we previously got a watch error
			// 先 Remove 再 Add：某些平台对重复 Add 同一路径返回错误。
			if attemptPeriodicRewatch {
				_ = watcher.Remove(path)
				if err := watcher.Add(path); err != nil {
					errorHandler(err)
				} else {
					attemptPeriodicRewatch = false
				}
			}

			// Handle
			eventHandler()

		case e := <-eventsCh:
			// Prioritize exiting if context is canceled
			if ctx.Err() != nil {
				return
			}

			// Try to re-establish the watcher for events which dropped the existing watch
			// 编辑器常以“写临时文件 + rename”方式原子替换，被监听的 inode 会消失，
			// 必须重新 Add 才能继续收到后续事件。
			if e.Name == path && (e.Has(fsnotify.Remove) || e.Has(fsnotify.Rename)) {
				_ = watcher.Remove(path)
				if err := watcher.Add(path); err != nil {
					errorHandler(err)
					attemptPeriodicRewatch = true
				}
			}

			// Handle
			eventHandler()

		case err := <-errorCh:
			// Prioritize exiting if context is canceled
			if ctx.Err() != nil {
				return
			}

			// If the error occurs in response to calling watcher.Add, re-adding here could hot-loop.
			// The periodic poll will attempt to re-establish the watch.
			errorHandler(err)
			attemptPeriodicRewatch = true
		}
	}
}
