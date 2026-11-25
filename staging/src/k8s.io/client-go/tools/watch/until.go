/*
Copyright 2016 The Kubernetes Authors.

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

// Package watch 提供在 watch 流上等待条件的辅助函数。
// 相比裸 watch，它处理连接中断、API 超时与 resourceVersion 过期等常见故障。
package watch

import (
	"context"
	"errors"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

// PreconditionFunc 在条件已达成时返回 true，尚未达成时返回 false，
// 条件失败或检测到错误状态时返回错误。
type PreconditionFunc func(store cache.Store) (bool, error)

// ConditionFunc 在条件已达成时返回 true，尚未达成时返回 false；
// 若条件无法检查且应终止则返回错误。通常应优先定义水平（level driven）条件
// 而非边缘（edge driven）条件（Pod ready=true，而不是 Pod 被修改且 ready 由 false 变 true）。
type ConditionFunc func(event watch.Event) (bool, error)

// ErrWatchClosed 在 UntilWithoutRetry 超时之前 watch channel 已关闭时返回。
var ErrWatchClosed = errors.New("watch closed before UntilWithoutRetry timeout")

// UntilWithoutRetry 从 watch 中读取条目，直到每个给定的条件依次成功，然后返回
// 最后遇到的事件。第一个返回错误的条件会终止 watch（同时返回该事件）。
// 若未收到任何事件，返回的 event 为 nil。
// 条件按顺序依次满足，以便作为上层组合的有用原语。
// 会等待到 context 截止时间或 context 被取消。
//
// 警告：除非有非常特定的用途（大概是一个特殊的 Watcher），否则不要使用本函数！
// 警告：例如在 API 超时和/或 'too old resource version' 错误时它会失败。
// 警告：你大概率想找的是下面的 Until 或 UntilWithSync 函数，
// 警告：它们能解决这类问题。
// TODO: 当代码库中其他使用点消失后，考虑把本函数改为私有以防误用。
//
// 本函数不做重连；一旦 watch 关闭或 RV 过期就直接失败。
func UntilWithoutRetry(ctx context.Context, watcher watch.Interface, conditions ...ConditionFunc) (*watch.Event, error) {
	ch := watcher.ResultChan()
	defer watcher.Stop()
	var lastEvent *watch.Event
	for _, condition := range conditions {
		// 先用上一个事件检查下一个条件，命中则短路跳过等待新的 watch
		// 同一事件可能同时满足多个条件，因此先复用 lastEvent，避免不必要的等待。
		if lastEvent != nil {
			done, err := condition(*lastEvent)
			if err != nil {
				return lastEvent, err
			}
			if done {
				continue
			}
		}
	ConditionSucceeded:
		for {
			select {
			case event, ok := <-ch:
				if !ok {
					return lastEvent, ErrWatchClosed
				}
				lastEvent = &event

				done, err := condition(event)
				if err != nil {
					return lastEvent, err
				}
				if done {
					break ConditionSucceeded
				}

			case <-ctx.Done():
				return lastEvent, wait.ErrorInterrupted(nil)
			}
		}
	}
	return lastEvent, nil
}

// Until 用 RetryWatcher 包装 watcherClient 的 watch 函数，确保出错时 watcher 会被重启。
// initialResourceVersion 会在首次调用时传给 watch 方法；受底层 WATCH 调用的
// 问题影响（#74022），它不能是 "" 或 "0"。
// 其余行为与 UntilWithoutRetry 完全相同（见上文）。
// Until 能处理 API 超时和连接丢失。
// 它保证你能看到所有事件，且顺序与发生顺序一致。
// 正因为这一保证，它无法处理 'Resource version too old' 错误，此时会失败。
// （若希望包括 RV too old 在内的所有错误都能通过重新 LIST 恢复，
// 请参考 `UntilWithSync`。普通代码应关注水平驱动，不必在意漏看某些边缘事件。）
//
// Until 最常见的用法是测试中需要验证事件的精确顺序（“边缘”）。
//
// 通过 RetryWatcher 自动重连，但 RV 过期仍会失败，因为那需要重新 LIST。
func Until(ctx context.Context, initialResourceVersion string, watcherClient cache.Watcher, conditions ...ConditionFunc) (*watch.Event, error) {
	w, err := NewRetryWatcherWithContext(ctx, initialResourceVersion, cache.ToWatcherWithContext(watcherClient))
	if err != nil {
		return nil, err
	}

	return UntilWithoutRetry(ctx, w, conditions...)
}

// UntilWithSync 从 lw 创建 informer，可选地在 store 同步后检查前置条件，
// 并监听输出直到每个给定条件成功，方式与 UntilWithoutRetry 相同（见上文）。
// UntilWithSync 能处理所有错误，如 API 超时、连接丢失和 'Resource version too old'。
// 它是唯一能从 'Resource version too old' 中恢复的函数；Until 和 UntilWithoutRetry
// 在这种情况下只会失败。另一方面，它无法提供像 Until 配合简单 Watch 方法那样强的保证：
// watch 失败时它可能跳过一些中间事件，但会通过重新 LIST 恢复，恢复后只要
// 有过变化就一定能收到事件。
// 此外，基于 DeltaFIFO 的当前实现只保证同一对象的接收顺序，
// 即使是同一资源，不同对象之间的顺序也不保证。
// 最常见的用法是需要观察“世界状态”且不应失败的命令，例如：
// 等待对象达到某个状态、“小型”控制器等。
//
// 借助 informer 与重新 LIST 从 RV 过期中恢复，代价是可能漏掉中间事件。
func UntilWithSync(ctx context.Context, lw cache.ListerWatcher, objType runtime.Object, precondition PreconditionFunc, conditions ...ConditionFunc) (*watch.Event, error) {
	indexer, informer, watcher, done := NewIndexerInformerWatcherWithLogger(klog.FromContext(ctx), lw, objType)
	// 需要等待内部 informer 完全停止，这样更容易推理，
	// 且对非线程安全的客户端也适用。
	defer func() { <-done }()
	// 代理 watcher 可以多次 Stop，因此这里用 defer 覆盖各个分支，
	// 并让 UntilWithoutRetry 来停止它
	defer watcher.Stop()

	if precondition != nil {
		if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
			return nil, fmt.Errorf("UntilWithSync: unable to sync caches: %w", ctx.Err())
		}

		done, err := precondition(indexer)
		if err != nil {
			return nil, err
		}

		if done {
			return nil, nil
		}
	}

	return UntilWithoutRetry(ctx, watcher, conditions...)
}

// ContextWithOptionalTimeout 包装 context.WithTimeout，并把 0 时长视为无限超时。
// 0 表示无限等待，因此退化为可取消的 WithCancel；负值视为非法配置并修正为 0。
func ContextWithOptionalTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout < 0 {
		// 这种情况本应在参数校验阶段处理
		klog.FromContext(parent).Error(nil, "Timeout for context shall not be negative")
		timeout = 0
	}

	if timeout == 0 {
		return context.WithCancel(parent)
	}

	return context.WithTimeout(parent, timeout)
}
