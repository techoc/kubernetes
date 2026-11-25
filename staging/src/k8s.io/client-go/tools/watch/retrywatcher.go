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

package watch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
	"k8s.io/utils/dump"
)

// resourceVersionGetter 是用于从事件获取 resource version 的接口。
// 不能复用 meta 中的接口，否则会造成循环依赖；这里只需要这一个方法。
type resourceVersionGetter interface {
	GetResourceVersion() string
}

// RetryWatcher 确保底层 watcher 关闭时（如 API 超时或 etcd 超时），
// 会从上一个位置无感重启，消费者甚至感知不到。
// 它通过检查事件并跟踪 resourceVersion 来实现。
// 特别适合配合 watch.UntilWithoutRetry 使用，避免其过早终止导致的问题和抖动。
// 注意：它无法应对 etcd 缓存中 resource version 已不存在的情况——
// 那种场景需要使用 Informer。
type RetryWatcher struct {
	cancel func(error)
	// lastResourceVersion 是下一条 watch 的起点；每个事件（包括 bookmark）都会推进它。
	lastResourceVersion string
	watcherClient       cache.WatcherWithContext
	// resultChan 无缓冲：消费者不读取时会反压到 watch 循环。
	resultChan      chan watch.Event
	doneChan        chan struct{}
	minRestartDelay time.Duration
}

// NewRetryWatcher 创建一个新的 RetryWatcher。
// 它会确保 watch 在可恢复错误时自动重启。
// initialResourceVersion 会在首次调用时传给 watch 方法。
//
// Deprecated: 请改用 NewRetryWatcherWithContext。旧构造函数没有 ctx，无法在启动前取消。
func NewRetryWatcher(initialResourceVersion string, watcherClient cache.Watcher) (*RetryWatcher, error) {
	return NewRetryWatcherWithContext(context.Background(), initialResourceVersion, cache.ToWatcherWithContext(watcherClient))
}

// NewRetryWatcherWithContext 创建一个新的 RetryWatcher。
// 它会确保 watch 在可恢复错误时自动重启。
// initialResourceVersion 会在首次调用时传给 watch 方法。
func NewRetryWatcherWithContext(ctx context.Context, initialResourceVersion string, watcherClient cache.WatcherWithContext) (*RetryWatcher, error) {
	return newRetryWatcher(ctx, initialResourceVersion, watcherClient, 1*time.Second)
}

func newRetryWatcher(ctx context.Context, initialResourceVersion string, watcherClient cache.WatcherWithContext, minRestartDelay time.Duration) (*RetryWatcher, error) {
	// 空 RV 或 "0" 会让服务端先做一次隐式 list，期间可能漏掉事件；
	// RetryWatcher 依赖严格从指定 RV 继续，因此拒绝这两种输入。
	switch initialResourceVersion {
	case "", "0":
		// TODO: 如果将来支持 WATCH v2（表示“从现在开始”，且开头
		//       不再合成一次对象 list，见 #74022），需要重新审视这里的处理。
		return nil, fmt.Errorf("initial RV %q is not supported due to issues with underlying WATCH", initialResourceVersion)
	default:
		break
	}

	// 使用 CancelCause，便于 Stop 与内部错误区分退出原因。
	ctx, cancel := context.WithCancelCause(ctx)

	rw := &RetryWatcher{
		cancel:              cancel,
		lastResourceVersion: initialResourceVersion,
		watcherClient:       watcherClient,
		doneChan:            make(chan struct{}),
		resultChan:          make(chan watch.Event, 0),
		minRestartDelay:     minRestartDelay,
	}

	go rw.receive(ctx)
	return rw, nil
}

func (rw *RetryWatcher) send(ctx context.Context, event watch.Event) bool {
	// 向无缓冲 channel 写入是阻塞操作，
	// 期间需要检查是否已被要求停止。
	// 返回 false 表示 ctx 已取消，调用方应终止接收循环。
	select {
	case rw.resultChan <- event:
		return true
	case <-ctx.Done():
		return false
	}
}

// doReceive 完成时返回 true，否则返回 false。
// 未完成时，第二个返回值表示再次调用前需要等待的时长。
//
// done=false 表示可恢复，调用方会用 lastResourceVersion 重建 watch；
// done=true 表示不可继续（ctx 取消、权限错误或数据不一致）。
func (rw *RetryWatcher) doReceive(ctx context.Context) (bool, time.Duration) {
	watcher, err := rw.watcherClient.WatchWithContext(ctx, metav1.ListOptions{
		ResourceVersion:     rw.lastResourceVersion,
		AllowWatchBookmarks: true,
	})
	// 这里几乎不会遇到 EOF（调用刚建立），但也可能碰上 apiserver
	// 正在关闭（如重启中）。处理方式与 informer 保持一致。
	switch err {
	case nil:
		break

	case io.EOF:
		// watch 正常关闭
		return false, 0

	case io.ErrUnexpectedEOF:
		klog.FromContext(ctx).V(1).Info("Watch closed with unexpected EOF", "err", err)
		return false, 0

	default:
		msg := "Watch failed"
		if net.IsProbableEOF(err) || net.IsTimeout(err) {
			klog.FromContext(ctx).V(5).Info(msg, "err", err)
			// 重试
			return false, 0
		}

		// 检查 watch 是否因客户端没有监听该资源的权限、或凭据无效（如 token 过期）而失败。
		if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
			// Kubernetes API 返回的 forbidden 信息只有 "unknown"，这里补充更多细节。
			klog.FromContext(ctx).Error(err, msg+": ensure the client has valid credentials and watch permissions on the resource")

			if apiStatus, ok := err.(apierrors.APIStatus); ok {
				statusErr := apiStatus.Status()

				sent := rw.send(ctx, watch.Event{
					Type:   watch.Error,
					Object: &statusErr,
				})
				if !sent {
					// 这通常意味着 RetryWatcher 正在停止，但返回 false，
					// 让 doReceive 的调用方可以确认并可能重试。
					klog.FromContext(ctx).Error(nil, "Failed to send the Unauthorized or Forbidden watch event")

					return false, 0
				}
			} else {
				// apierrors 只处理 apierrors.APIStatus，本不该走到这里。
				// 但这仍属于不可恢复错误，因此下面依旧返回 true。
				klog.FromContext(ctx).Error(err, msg+": encountered an unexpected Unauthorized or Forbidden error type")
			}

			return true, 0
		}

		// Resource expired 或 410 Gone 错误（如 resource version 过旧）
		// 是预期内且可恢复的，因此用更低的日志级别而非 ERROR。
		// 这类错误可通过重新 list（informer）或调用方兜底恢复；本组件本身无法补数据。
		if apierrors.IsResourceExpired(err) || apierrors.IsGone(err) {
			klog.FromContext(ctx).V(4).Info(msg, "err", err)
			return false, 0
		}

		klog.FromContext(ctx).Error(err, msg)
		// 重试
		return false, 0
	}

	if watcher == nil {
		klog.FromContext(ctx).Error(nil, "Watch returned nil watcher")
		// 重试
		return false, 0
	}

	ch := watcher.ResultChan()
	defer watcher.Stop()

	for {
		select {
		case <-ctx.Done():
			klog.FromContext(ctx).V(4).Info("Stopping RetryWatcher")
			return true, 0
		case event, ok := <-ch:
			if !ok {
				klog.FromContext(ctx).V(4).Info("Failed to get event - re-creating the watcher", "resourceVersion", rw.lastResourceVersion)
				return false, 0
			}

			// 需要检查事件并从中取出 ResourceVersion
			switch event.Type {
			case watch.Added, watch.Modified, watch.Deleted, watch.Bookmark:
				metaObject, ok := event.Object.(resourceVersionGetter)
				if !ok {
					_ = rw.send(ctx, watch.Event{
						Type:   watch.Error,
						Object: &apierrors.NewInternalError(errors.New("retryWatcher: doesn't support resourceVersion")).ErrStatus,
					})
					// 必须在此中止：跳过可能带有有效数据的 RV 会导致 lastResourceVersion 不一致！
					return true, 0
				}

				resourceVersion := metaObject.GetResourceVersion()
				if resourceVersion == "" {
					_ = rw.send(ctx, watch.Event{
						Type:   watch.Error,
						Object: &apierrors.NewInternalError(fmt.Errorf("retryWatcher: object %#v doesn't support resourceVersion", event.Object)).ErrStatus,
					})
					// 必须在此中止：跳过可能带有有效数据的 RV 会导致 lastResourceVersion 不一致！
					return true, 0
				}

				// 一切正常；发送非 bookmark 事件并更新 resource version。
				// bookmark 只用于推进 RV，不向消费者投递，以免破坏既有事件语义。
				if event.Type != watch.Bookmark {
					ok = rw.send(ctx, event)
					if !ok {
						return true, 0
					}
				}
				rw.lastResourceVersion = resourceVersion

				continue

			case watch.Error:
				// 经过这一轮转换，我们可以处理非结构化的 status
				errObject := apierrors.FromObject(event.Object)
				statusErr, ok := errObject.(*apierrors.StatusError)
				if !ok {
					klog.FromContext(ctx).Error(nil, "Received an error which is not *metav1.Status", "errorObject", dump.Pretty(event.Object))
					// 未知错误则重试
					return false, 0
				}

				status := statusErr.ErrStatus

				statusDelay := time.Duration(0)
				if status.Details != nil {
					statusDelay = time.Duration(status.Details.RetryAfterSeconds) * time.Second
				}

				switch status.Code {
				case http.StatusGone:
					// RV too old 错误绝不重试
					// 410 无法从当前位置续接，必须交给上层重新 list。
					_ = rw.send(ctx, event)
					return true, 0

				case http.StatusGatewayTimeout, http.StatusInternalServerError:
					// 重试
					return false, statusDelay

				default:
					// 默认重试。除非 RetryWatcher 确定无法继续，否则应尽量前行；
					// 不确定时继续重试，由使用者自行决定超时。

					// 在此记录日志，留下遇到意外错误的痕迹；
					// 如果遗漏了某些预期内的错误码，可以据此加入白名单。
					klog.FromContext(ctx).V(5).Info("Retrying after unexpected error", "errorObject", dump.Pretty(event.Object))

					// 重试
					return false, statusDelay
				}

			default:
				klog.FromContext(ctx).Error(nil, "Failed to recognize event", "type", event.Type)
				_ = rw.send(ctx, watch.Event{
					Type:   watch.Error,
					Object: &apierrors.NewInternalError(fmt.Errorf("retryWatcher failed to recognize Event type %q", event.Type)).ErrStatus,
				})
				// 无法重启 watch，必须停止循环，否则跳过可能带有有效数据的 RV 会导致 lastResourceVersion 不一致！
				return true, 0
			}
		}
	}
}

// receive 从 watcher 读取结果，必要时重启 watcher。
func (rw *RetryWatcher) receive(ctx context.Context) {
	// 退出时两个 channel 都会被关闭：消费者先观察到 resultChan 关闭，
	// 再通过 Done 确认后台循环已结束。
	defer close(rw.doneChan)
	defer close(rw.resultChan)

	logger := klog.FromContext(ctx)
	logger.V(4).Info("Starting RetryWatcher")
	defer logger.V(4).Info("Stopping RetryWatcher")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 使用非滑动的 until：WATCH 调用超时或被关闭后需要重建时，
	// 正常路径不会引入额外延迟，同时也避免热循环。
	// 非滑动间隔：正常路径不会被周期推迟，同时 minRestartDelay 防止忙等。
	wait.NonSlidingUntilWithContext(ctx, func(ctx context.Context) {
		done, retryAfter := rw.doReceive(ctx)
		if done {
			cancel()
			return
		}

		timer := time.NewTimer(retryAfter)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		logger.V(4).Info("Restarting RetryWatcher", "resourceVersion", rw.lastResourceVersion)
	}, rw.minRestartDelay)
}

// ResultChan 实现 Interface 接口。
// resultChan 在 receive 退出时关闭，消费者应处理 channel 关闭。
func (rw *RetryWatcher) ResultChan() <-chan watch.Event {
	return rw.resultChan
}

// Stop 实现 Interface 接口。
func (rw *RetryWatcher) Stop() {
	rw.cancel(errors.New("asked to stop"))
}

// Done 让调用方能够获知 RetryWatcher 何时停止。
func (rw *RetryWatcher) Done() <-chan struct{} {
	return rw.doneChan
}
