/*
Copyright 2018 The Kubernetes Authors.

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

package buffered

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/client-go/util/flowcontrol"
)

// PluginName 是错误指标中上报的名称。
const PluginName = "buffered"

// BatchConfig 表示委托审计后端（delegate backend）的批量处理配置。
// BufferSize 是内存上限——队列满时 ProcessEvents 会丢弃并报错，
// 因此需要在“降低下游调用频率”和“丢审计事件”之间取舍。
type BatchConfig struct {
	// BufferSize 定义缓冲队列的大小。
	BufferSize int
	// MaxBatchSize 定义一批的最大长度。
	MaxBatchSize int
	// MaxBatchWait 表示两批之间的最大间隔。
	MaxBatchWait time.Duration

	// ThrottleEnable 定义是否对批处理过程限速。
	ThrottleEnable bool
	// ThrottleQPS 定义每秒向委托后端发送批次的允许速率。
	ThrottleQPS float32
	// ThrottleBurst 定义在 ThrottleQPS 容量未用尽时，
	// 同一时刻可向委托后端发送请求的最大数量。
	ThrottleBurst int

	// 是否异步调用委托后端。
	AsyncDelegate bool
}

// bufferedBackend 把逐个事件写改为批量写。buffer 与 shutdownCh 的生命周期严格由 Run/Shutdown 控制：
// buffer 只在 processIncomingEvents 结束时关闭，shutdownCh 只在后台 routine 退出时关闭。
type bufferedBackend struct {
	// 真正导出事件的委托后端。
	delegateBackend audit.Backend

	// 在发送给委托后端之前缓冲事件的 channel。
	buffer chan *auditinternal.Event
	// 发送给委托后端的一批中事件的最大数量。
	maxBatchSize int
	// 向委托后端发送一批后，到发送下一批前的等待时间。
	//
	// 无论过了多久，收到 maxBatchSize 个事件总会触发发送一批。
	maxBatchWait time.Duration

	// 是否异步调用委托后端。
	asyncDelegate bool

	// 用于通知批处理 goroutine 已处理完所有剩余事件并退出的 channel。
	// 一旦 `shutdownCh` 关闭，就不再向委托后端发送新事件。
	shutdownCh chan struct{}

	// 控制向委托后端发送批次的并发度的 WaitGroup。
	// worker goroutine 在发送批次前调用 Add，
	// 再启动一个 goroutine 在委托后端处理完后调用 Done。
	// 关闭审计后端前，用该 WaitGroup 等待所有发送 goroutine 完成。
	wg sync.WaitGroup

	// 限制每秒向委托后端发送的批次数。
	throttle flowcontrol.RateLimiter
}

var _ audit.Backend = &bufferedBackend{}

// NewBackend 返回包装 delegate 后端的缓冲型审计后端。缓冲后端会自动运行并关闭 delegate 后端。
// delegate 的生命周期由本包装器托管，调用方不要自行 Run/Shutdown，
// 否则会出现重复启动或提前关闭。
func NewBackend(delegate audit.Backend, config BatchConfig) audit.Backend {
	var throttle flowcontrol.RateLimiter
	if config.ThrottleEnable {
		throttle = flowcontrol.NewTokenBucketRateLimiter(config.ThrottleQPS, config.ThrottleBurst)
	}
	return &bufferedBackend{
		delegateBackend: delegate,
		buffer:          make(chan *auditinternal.Event, config.BufferSize),
		maxBatchSize:    config.MaxBatchSize,
		maxBatchWait:    config.MaxBatchWait,
		asyncDelegate:   config.AsyncDelegate,
		shutdownCh:      make(chan struct{}),
		wg:              sync.WaitGroup{},
		throttle:        throttle,
	}
}

// Run 启动后台批处理 goroutine，并同步运行 delegate。
// stopCh 关闭后仍需尽量排空 buffer；排空循环用 panic 恢复保证
// 单个事件处理失败不会丢掉剩余事件。
func (b *bufferedBackend) Run(stopCh <-chan struct{}) error {
	go func() {
		// 通知 worker goroutine 已退出。
		defer close(b.shutdownCh)

		b.processIncomingEvents(stopCh)

		// 处理上次清空缓冲后到本行之前收到的事件。由于缓冲已关闭，
		//
		// 不会再有新事件进来。
		allEventsProcessed := false
		timer := make(chan time.Time)
		for !allEventsProcessed {
			allEventsProcessed = func() bool {
				// 从任何 panic 中恢复，尽量处理完所有剩余事件。
				// 注意 panic 时返回值为 false，循环会
				// 继续执行。
				defer runtime.HandleCrash()

				events := b.collectEvents(timer, wait.NeverStop)
				b.processEvents(events)
				return len(events) == 0
			}()
		}
	}()
	return b.delegateBackend.Run(stopCh)
}

// Shutdown 阻塞直到传给 Run 的 stopCh 关闭，且此前加入的所有事件
// 都已分批发送给委托后端。
func (b *bufferedBackend) Shutdown() {
	// 等待 Run 方法中启动的 goroutine 退出。
	<-b.shutdownCh

	// 等待所有发送 goroutine 退出。
	//
	// - b.shutdownCh 关闭意味着 Run 中的 goroutine 已终止。
	// - 这说明 processIncomingEvents 已终止。
	// - 也就是说 b.buffer 已关闭，不能再接收新事件。
	// - 由于 processEvents 由 Run 的 goroutine 同步调用，waitgroup 已取到最终值。
	// 因此 wg.Wait 不会漏掉任何后续的批次。
	b.wg.Wait()

	b.delegateBackend.Shutdown()
}

// processIncomingEvents 循环从缓冲中收集事件。当
// b.stopCh 关闭时，它停止并关闭缓冲。
func (b *bufferedBackend) processIncomingEvents(stopCh <-chan struct{}) {
	defer close(b.buffer)

	var (
		maxWaitChan  <-chan time.Time
		maxWaitTimer *time.Timer
	)
	// 只有启用批处理时才使用最大等待时间分批。
	if b.maxBatchSize > 1 {
		maxWaitTimer = time.NewTimer(b.maxBatchWait)
		maxWaitChan = maxWaitTimer.C
		defer maxWaitTimer.Stop()
	}

	for {
		func() {
			// 从本函数引起的任何 panic 中恢复，避免
			// goroutine 中的 panic 拖垮主 goroutine。
			defer runtime.HandleCrash()

			if b.maxBatchSize > 1 {
				maxWaitTimer.Reset(b.maxBatchWait)
			}
			b.processEvents(b.collectEvents(maxWaitChan, stopCh))
		}()

		select {
		case <-stopCh:
			return
		default:
		}
	}
}

// collectEvents 尝试收集一批事件。以下任一情况都会让 collectEvents 停止并返回事件列表：
//
//   - 一批达到事件数上限。
//   - 计时器超时。
//   - 缓冲 channel 已关闭且为空。
//   - stopCh 被关闭。
//
// 返回空切片是正常结果（例如超时但无事件），调用方据此结束循环。
func (b *bufferedBackend) collectEvents(timer <-chan time.Time, stopCh <-chan struct{}) []*auditinternal.Event {
	var events []*auditinternal.Event

L:
	for i := 0; i < b.maxBatchSize; i++ {
		select {
		case ev, ok := <-b.buffer:
			// 缓冲 channel 已关闭，不会再有新事件。
			if !ok {
				break L
			}
			events = append(events, ev)
		case <-timer:
			// 计时器到期，发送当前累积的批次。
			break L
		case <-stopCh:
			// 后端已停止，发送当前累积的批次。
			break L
		}
	}

	return events
}

// processEvents 在 goroutine 中用委托后端的 ProcessEvents 处理批次事件。
func (b *bufferedBackend) processEvents(events []*auditinternal.Event) {
	if len(events) == 0 {
		return
	}

	// TODO(audit)：应控制活跃 goroutine 的数量：
	// 若一个 goroutine 耗时 5 秒，goroutine 数可达 5 * defaultBatchThrottleQPS
	// throttle 在同步路径上阻塞，因此限流会反压到采集循环——这是有意的背压。
	if b.throttle != nil {
		b.throttle.Accept()
	}

	if b.asyncDelegate {
		b.wg.Add(1)
		go func() {
			defer b.wg.Done()
			defer runtime.HandleCrash()

			// 在 goroutine 中执行真正的处理以免阻塞，
			// 让批处理 goroutine 能立即继续清空队列。
			b.delegateBackend.ProcessEvents(events...)
		}()
	} else {
		func() {
			defer runtime.HandleCrash()

			// 在 goroutine 中执行真正的处理以免阻塞，
			// 让批处理 goroutine 能立即继续清空队列。
			b.delegateBackend.ProcessEvents(events...)
		}()
	}
}

// ProcessEvents 把事件深拷贝后放入缓冲队列。审计后端接口约定调用方会复用事件对象，因此必须 DeepCopy；
// 队列满时丢弃并记录错误，但返回 true 以免拖慢请求处理路径。
func (b *bufferedBackend) ProcessEvents(ev ...*auditinternal.Event) bool {
	// 以下机制用于支持后端停止后仍有审计事件
	// 继续到来的情况。
	var sendErr error
	var evIndex int

	// 若委托后端已关闭且缓冲 channel 已关闭，向其中添加事件
	// 会导致 panic，这里需要
	// 恢复。
	defer func() {
		if err := recover(); err != nil {
			sendErr = fmt.Errorf("audit backend shut down")
		}
		if sendErr != nil {
			audit.HandlePluginError(PluginName, sendErr, ev[evIndex:]...)
		}
	}()

	for i, e := range ev {
		evIndex = i
		// 按 audit.Backend 接口约定，这些事件发给 Sink 后会被复用。
		// 因此先深拷贝，再把副本放入队列。
		event := e.DeepCopy()

		select {
		case b.buffer <- event:
		default:
			sendErr = fmt.Errorf("audit buffer queue blocked")
			return true
		}
	}
	return true
}

func (b *bufferedBackend) String() string {
	return fmt.Sprintf("%s<%s>", PluginName, b.delegateBackend)
}
