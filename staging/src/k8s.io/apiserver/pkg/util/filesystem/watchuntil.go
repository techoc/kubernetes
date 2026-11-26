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

// WatchUntil 监听指定路径的变化，阻塞至 ctx 取消。
// eventHandler() 不能为 nil，pollInterval 必须大于 0。
// 观察到变化事件或 pollInterval 到时调用 eventHandler()。
// 初始化或监听路径出错时调用 errorHandler()（若非 nil）。
//
// path 为目录时只监听目录本身及其直接子项。
//
// 路径不存在或无法监听时，错误传给 errorHandler()，并按 pollInterval 调用 eventHandler()。
//
// 多个事件可能合并为一次 eventHandler() 调用。
//
// 监听初始化成功后立即调用 eventHandler()，
// 以防调用 WatchUntil() 期间路径恰好发生变化。
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

	// 即使 watcher 正常，轮询也始终运行：作为事件通道的兜底。
	t := time.NewTicker(pollInterval)
	defer t.Stop()

	attemptPeriodicRewatch := false

	// 首次 Add 失败时不在 select 循环里立即重试，而是交给周期性 tick，
	// 避免路径长期不存在时形成忙等。
	if err := watcher.Add(path); err != nil {
		errorHandler(err)
		attemptPeriodicRewatch = true
	} else {
		// 成功注册监听后至少调用一次 handle()，
		// 以防调用 WatchUntil 期间文件恰好变化。
		eventHandler()
	}

	for {
		select {
		case <-ctx.Done():
			return

		case <-t.C:
			// ctx.Done 与 t.C 可能同时就绪，显式检查避免取消后仍触发一次回调。
			if ctx.Err() != nil {
				return
			}

			// 先 Remove 再 Add：某些平台对重复 Add 同一路径返回错误。
			if attemptPeriodicRewatch {
				_ = watcher.Remove(path)
				if err := watcher.Add(path); err != nil {
					errorHandler(err)
				} else {
					attemptPeriodicRewatch = false
				}
			}

			// 调用 eventHandler
			eventHandler()

		case e := <-eventsCh:
			// ctx 取消时优先退出
			if ctx.Err() != nil {
				return
			}

			// 编辑器常以“写临时文件 + rename”方式原子替换，被监听的 inode 会消失，
			// 必须重新 Add 才能继续收到后续事件。
			if e.Name == path && (e.Has(fsnotify.Remove) || e.Has(fsnotify.Rename)) {
				_ = watcher.Remove(path)
				if err := watcher.Add(path); err != nil {
					errorHandler(err)
					attemptPeriodicRewatch = true
				}
			}

			// 调用 eventHandler
			eventHandler()

		case err := <-errorCh:
			// ctx 取消时优先退出
			if ctx.Err() != nil {
				return
			}

			// 若错误来自 watcher.Add，此处重新添加可能导致忙循环。
			// 定期轮询会尝试重建监听。
			errorHandler(err)
			attemptPeriodicRewatch = true
		}
	}
}
