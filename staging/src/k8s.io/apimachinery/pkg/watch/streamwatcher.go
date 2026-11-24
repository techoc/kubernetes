/*
Copyright 2014 The Kubernetes Authors.

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
	"fmt"
	"io"
	"sync"

	"k8s.io/klog/v2"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/net"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

// Decoder 让 StreamWatcher 能监听任何可写出 Decoder 的流。
type Decoder interface {
	// Decode 返回事件类型、解码后的对象或错误。发生错误时 StreamWatcher 会调用 Close()。
	// Decode 应阻塞等待，直到读取到数据或发生错误。
	//
	// Decode 必须阻塞直到有数据或出错；返回错误会终止监听。
	Decode() (action EventType, object runtime.Object, err error)

	// Close 关闭底层 io.Reader，通知流的提供方当前不再被 watch。
	// Close() 必须确保所有正在阻塞的 Decode() 调用都能返回某种错误。
	//
	// Close 必须让阻塞中的 Decode 尽快返回错误，否则 Stop 无法真正结束监听。
	Close()
}

// Reporter 隐藏把错误转换为 runtime.Object 的细节，
// 因为本包不能依赖更高层的错误报告类型（避免循环依赖）。
type Reporter interface {
	// AsObject 必须将 err 转换为可在 watch 流中传输的有效 runtime.Object。
	AsObject(err error) runtime.Object
}

// StreamWatcher 把任意可解码的流转换为 watch.Interface。
// 内部单 goroutine 读取并转发事件，Stop 通过 done 通道解除可能的阻塞。
type StreamWatcher struct {
	logger klog.Logger
	// 内嵌互斥锁用于保护 Stop 的幂等关闭。
	sync.Mutex
	source   Decoder
	reporter Reporter
	result   chan Event
	done     chan struct{}
}

// NewStreamWatcher 使用给定的 decoder 创建 StreamWatcher。
// 支持上下文日志的代码应优先使用 NewStreamWatcherWithLogger，以便传入关联的 logger。
func NewStreamWatcher(d Decoder, r Reporter) *StreamWatcher {
	return NewStreamWatcherWithLogger(klog.Background(), d, r)
}

// NewStreamWatcherWithLogger 用给定 decoder 与 logger 创建 StreamWatcher，并启动接收 goroutine。
func NewStreamWatcherWithLogger(logger klog.Logger, d Decoder, r Reporter) *StreamWatcher {
	sw := &StreamWatcher{
		logger:   logger,
		source:   d,
		reporter: r,
		// 消费者可以很容易地通过额外的 goroutine/channel 增加缓冲，但无法移除生产者预先添加的缓冲，
		// 因此这里使用无缓冲通道，将缓冲策略交给消费者决定。
		result: make(chan Event),
		// 如果 watcher 被外部停止，可能已没有消费者继续接收 result；此时向 result 发送事件（尤其是错误事件）
		// 可能永久阻塞。因此使用独立的 done 通道，让发送操作能够通过 select 响应停止信号。
		// done 与 result 的发送操作在 select 中二选一，从而避免消费者退出后永久阻塞。
		done: make(chan struct{}),
	}
	go sw.receive()
	return sw
}

// ResultChan 实现 watch.Interface，返回事件通道；通道在监听结束时关闭。
func (sw *StreamWatcher) ResultChan() <-chan Event {
	return sw.result
}

// Stop 实现 watch.Interface，幂等地停止监听。
func (sw *StreamWatcher) Stop() {
	// 通过互斥锁和 done 状态确保 Close() 只调用一次。
	sw.Lock()
	defer sw.Unlock()
	// 重复关闭 channel 会触发 panic，因此关闭前必须先检查状态。
	select {
	case <-sw.done:
	default:
		close(sw.done)
		sw.source.Close()
	}
}

// receive 循环从 decoder 读取事件并发送到 result 通道，直到出错或被停止。
func (sw *StreamWatcher) receive() {
	defer utilruntime.HandleCrashWithLogger(sw.logger)
	defer close(sw.result)
	defer sw.Stop()
	for {
		action, obj, err := sw.source.Decode()
		if err != nil {
			switch err {
			case io.EOF:
				// watch 流正常结束。
			case io.ErrUnexpectedEOF:
				// 截断属于异常但可恢复，降级为低级别日志，不向消费者报错事件。
				sw.logger.V(1).Info("Unexpected EOF during watch stream event decoding", "err", err)
			default:
				// 超时或“疑似 EOF”多为网络抖动，记录后直接结束；
				// 其它错误则作为 Error 事件交给消费者，且发送时也要能被 Stop 打断。
				if net.IsProbableEOF(err) || net.IsTimeout(err) {
					sw.logger.V(5).Info("Unable to decode an event from the watch stream", "err", err)
				} else {
					select {
					case <-sw.done:
					case sw.result <- Event{
						Type:   Error,
						Object: sw.reporter.AsObject(fmt.Errorf("unable to decode an event from the watch stream: %v", err)),
					}:
					}
				}
			}
			return
		}
		select {
		case <-sw.done:
			return
		case sw.result <- Event{
			Type:   action,
			Object: obj,
		}:
		}
	}
}
