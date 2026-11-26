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

// 本文件测试缓冲审计后端的分批与关停语义：
// 满批立即投递、不足一批等待超时、停止信号触发最后一次投递，
// 以及关闭后不再接收事件、缓冲区有界、Shutdown 等待在途委托调用完成。

package buffered

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/util/wait"
	auditinternal "k8s.io/apiserver/pkg/apis/audit"
	"k8s.io/apiserver/plugin/pkg/audit/fake"
)

var (
	// 永不触发的计时通道：让 collectEvents 只依赖满批或 stop 信号。
	infiniteTimeCh <-chan time.Time
)

// newEvents 生成指定数量的事件；内容不重要，测试只关心数量与顺序。
func newEvents(number int) []*auditinternal.Event {
	events := make([]*auditinternal.Event, number)
	for i := range events {
		events[i] = &auditinternal.Event{}
	}

	return events
}

// testBatchConfig 返回一组便于观测的批处理参数：小批次、长等待（由测试显式触发超时）。
func testBatchConfig() BatchConfig {
	return BatchConfig{
		BufferSize:     100,
		MaxBatchSize:   10,
		MaxBatchWait:   wait.ForeverTestTimeout,
		ThrottleEnable: false,
		AsyncDelegate:  true,
	}
}

// TestBatchedBackendCollectEvents 覆盖 collectEvents 的三种退出条件：
// 满批、计时器到期、stop 通道关闭。
func TestBatchedBackendCollectEvents(t *testing.T) {
	config := testBatchConfig()
	batchSize := config.MaxBatchSize
	backend := NewBackend(&fake.Backend{}, config).(*bufferedBackend)

	t.Log("Max batch size encountered.")
	backend.ProcessEvents(newEvents(batchSize + 1)...)
	batch := backend.collectEvents(nil, nil)
	assert.Len(t, batch, batchSize, "Expected full batch")

	t.Log("Partial batch should hang until timer expires.")
	backend.ProcessEvents(newEvents(1)...)
	tc := make(chan time.Time)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch = backend.collectEvents(tc, nil)
	}()
	// 事件先被 ProcessEvents 搬入内部缓冲，因此轮询缓冲清空来确认收集协程已拿到事件，
	// 再手动触发计时器，避免依赖真实时间。
	err := wait.Poll(time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		return len(backend.buffer) == 0, nil
	})
	require.NoError(t, err)

	tc <- time.Now() // 触发 "timeout"
	wg.Wait()
	assert.Len(t, batch, 2, "Expected partial batch")

	t.Log("Collected events should be delivered when stop channel is closed.")
	backend.ProcessEvents(newEvents(3)...)
	stopCh := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch = backend.collectEvents(nil, stopCh)
	}()
	// 等待队列中的事件被收集。
	err = wait.Poll(time.Second, wait.ForeverTestTimeout, func() (bool, error) {
		return len(backend.buffer) == 0, nil
	})
	require.NoError(t, err)

	close(stopCh)
	wg.Wait()
	assert.Len(t, batch, 3, "Expected partial batch")
}

// TestUnbatchedBackendCollectEvents 验证 MaxBatchSize=1 时退化为逐条投递，
// 队列被持续排空，且 stop 关闭后不再返回任何事件。
func TestUnbatchedBackendCollectEvents(t *testing.T) {
	config := testBatchConfig()
	config.MaxBatchSize = 1 // 不做批处理。
	backend := NewBackend(&fake.Backend{}, config).(*bufferedBackend)

	t.Log("Max batch size encountered.")
	backend.ProcessEvents(newEvents(3)...)
	batch := backend.collectEvents(nil, nil)
	assert.Len(t, batch, 1, "Expected single event")

	t.Log("Queue should always be drained.")
	for len(backend.buffer) > 0 {
		batch = backend.collectEvents(nil, nil)
		assert.Len(t, batch, 1, "Expected single event")
	}

	t.Log("Collection should hault when stop channel is closed.")
	stopCh := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		batch = backend.collectEvents(nil, stopCh)
	}()
	close(stopCh)
	wg.Wait()
	assert.Empty(t, batch, "Empty final batch")
}

// TestBufferedBackendProcessEventsAfterStop 验证后端停止后到达的事件被直接丢弃，
// 不会在 Shutdown 之后又产生新的委托调用。
func TestBufferedBackendProcessEventsAfterStop(t *testing.T) {
	t.Parallel()

	backend := NewBackend(&fake.Backend{}, testBatchConfig()).(*bufferedBackend)

	closedStopCh := make(chan struct{})
	close(closedStopCh)
	backend.Run(closedStopCh)
	backend.Shutdown()
	backend.ProcessEvents(newEvents(1)...)
	batch := backend.collectEvents(infiniteTimeCh, wait.NeverStop)

	require.Empty(t, batch, "processed events after the backed has been stopped")
}

// TestBufferedBackendProcessEventsBufferFull 验证缓冲区有界：
// 超出容量的事件被丢弃而不是无限堆积，避免审计后端故障拖垮 apiserver 内存。
func TestBufferedBackendProcessEventsBufferFull(t *testing.T) {
	t.Parallel()

	config := testBatchConfig()
	config.BufferSize = 1
	backend := NewBackend(&fake.Backend{}, config).(*bufferedBackend)

	backend.ProcessEvents(newEvents(2)...)

	require.Len(t, backend.buffer, 1, "buffed contains more elements than it should")
}

// TestBufferedBackendShutdownWaitsForDelegatedCalls 验证优雅关停：
// 在途的委托调用未返回前 Shutdown 必须阻塞，否则会丢失已确认接收的审计事件。
func TestBufferedBackendShutdownWaitsForDelegatedCalls(t *testing.T) {
	t.Parallel()

	delegatedCallStartCh := make(chan struct{})
	delegatedCallEndCh := make(chan struct{})
	delegateBackend := &fake.Backend{
		OnRequest: func(_ []*auditinternal.Event) {
			close(delegatedCallStartCh)
			<-delegatedCallEndCh
		},
	}
	config := testBatchConfig()
	backend := NewBackend(delegateBackend, config)

	// 运行后端、处理事件，等待批处理与委托调用开始。
	stopCh := make(chan struct{})
	backend.Run(stopCh)
	backend.ProcessEvents(newEvents(config.MaxBatchSize)...)
	<-delegatedCallStartCh

	// 启动关停流程。
	shutdownEndCh := make(chan struct{})
	go func() {
		close(stopCh)
		backend.Shutdown()
		close(shutdownEndCh)
	}()

	// 等待一段时间后检查 Shutdown 是否已退出。可能误报，
	// 但不会漏报。
	time.Sleep(100 * time.Millisecond)
	select {
	case <-shutdownEndCh:
		t.Fatalf("Shutdown exited before delegated call ended")
	default:
	}

	// 等待委托调用退出后 Shutdown 退出。
	close(delegatedCallEndCh)
	<-shutdownEndCh
}

// TestDelegateProcessEvents 验证同步与异步两种委托模式下，
// 每批事件都完整、按批大小交付给底层后端。
func TestDelegateProcessEvents(t *testing.T) {
	for _, async := range []bool{true, false} {
		t.Run(fmt.Sprintf("async:%t", async), func(t *testing.T) {
			config := testBatchConfig()
			config.AsyncDelegate = async
			wg := sync.WaitGroup{}
			delegate := &fake.Backend{
				OnRequest: func(events []*auditinternal.Event) {
					assert.Len(t, events, config.MaxBatchSize, "Unexpected batch")
					wg.Done()
				},
			}
			b := NewBackend(delegate, config).(*bufferedBackend)
			wg.Add(5)
			for i := 0; i < 5; i++ {
				b.processEvents(newEvents(config.MaxBatchSize))
			}
			wg.Wait()
		})
	}
}
