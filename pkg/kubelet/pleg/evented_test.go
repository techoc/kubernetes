/*
Copyright 2015 The Kubernetes Authors.

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

package pleg

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	internalapi "k8s.io/cri-api/pkg/apis"
	v1 "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
)

// 编译期断言：fakePodRelister 实现 podRelister 接口。
var _ podRelister = (*fakePodRelister)(nil)

// fakePodRelister 记录收到的重新列举请求，便于测试断言。
type fakePodRelister struct {
	relistRequests []types.UID
}

// newFakePodRelister 创建空的假 relister。
func newFakePodRelister() *fakePodRelister {
	return &fakePodRelister{}
}

// RequestRelist 记录一次重新列举请求。
func (f *fakePodRelister) RequestRelist(logger klog.Logger, podUID types.UID) {
	f.relistRequests = append(f.relistRequests, podUID)
}

// newTestEventedPLEG 创建使用假 relister 的 EventedPLEG 实例。
func newTestEventedPLEG(podRelister *fakePodRelister) *EventedPLEG {
	return NewEventedPLEG(nil, podRelister, 5)
}

// erroringRuntimeService 按顺序返回预设错误，用于模拟事件流失败。
type erroringRuntimeService struct {
	internalapi.RuntimeService
	streamErrors []error
	calls        int
}

// GetContainerEvents 依次返回预设的错误序列，并统计调用次数。
func (f *erroringRuntimeService) GetContainerEvents(context.Context, chan *v1.ContainerEventResponse, func(v1.RuntimeService_GetContainerEventsClient)) error {
	err := f.streamErrors[f.calls]
	f.calls++
	return err
}

// TestEventedPLEGRetryExhaustionClosesEventChannelAndLogsLastError 验证重试耗尽后
// 关闭事件通道，并记录最后一次错误。
func TestEventedPLEGRetryExhaustionClosesEventChannelAndLogsLastError(t *testing.T) {
	firstErr := errors.New("first stream failure")
	lastErr := errors.New("last stream failure")
	runtimeService := &erroringRuntimeService{streamErrors: []error{firstErr, lastErr}}
	pleg := newTestEventedPLEG(newFakePodRelister())
	pleg.runtimeService = runtimeService
	// 让重试次数恰好等于预设的错误数量。
	pleg.eventedPlegMaxStreamRetries = len(runtimeService.streamErrors)

	// 使用缓冲日志，便于断言最终日志内容。
	logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true), ktesting.Verbosity(0)))
	ctx := klog.NewContext(t.Context(), logger)
	done := make(chan struct{})
	// 在后台运行事件监听，主 goroutine 等待其退出。
	go func() {
		defer close(done)
		pleg.watchEventsChannel(ctx)
	}()

	// 重试耗尽后监听循环应在测试超时前退出。
	select {
	case <-done:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("EventedPLEG did not stop after exhausting stream retries")
	}

	// 每次重试都应调用一次运行时服务。
	assert.Equal(t, len(runtimeService.streamErrors), runtimeService.calls)

	// 终态日志应恰好出现一次，并携带最后一次错误与重试次数。
	var terminalLogs int
	for _, entry := range logger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
		if entry.Message != "Evented PLEG: disabling container-termination fast path after exhausting event stream retries" {
			continue
		}
		terminalLogs++
		require.ErrorIs(t, entry.Err, lastErr)
		assert.Equal(t, []any{"retries", len(runtimeService.streamErrors)}, entry.ParameterKVList)
	}
	assert.Equal(t, 1, terminalLogs)
}

// TestEventedPLEGWatcherRetriesWhenStreamClosesWithoutError 验证事件流无错误关闭时
// 仍会重试，并在耗尽后记录流关闭错误。
func TestEventedPLEGWatcherRetriesWhenStreamClosesWithoutError(t *testing.T) {
	// 运行时服务返回 nil 错误，模拟流正常关闭。
	runtimeService := &erroringRuntimeService{streamErrors: []error{nil}}
	pleg := newTestEventedPLEG(newFakePodRelister())
	pleg.runtimeService = runtimeService
	// 重试次数设为 1，验证无错误关闭也会触发重试。
	pleg.eventedPlegMaxStreamRetries = len(runtimeService.streamErrors)

	logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true), ktesting.Verbosity(0)))
	ctx := klog.NewContext(t.Context(), logger)
	done := make(chan struct{})
	// 在后台运行事件监听。
	go func() {
		defer close(done)
		pleg.watchEventsChannel(ctx)
	}()

	// 无错误关闭耗尽重试后监听循环应退出。
	select {
	case <-done:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("EventedPLEG did not stop after the event stream closed without an error")
	}

	// 流关闭后应恰好重试一次。
	assert.Equal(t, 1, runtimeService.calls)
	// 终态日志应携带 errEventedPLEGStreamClosed。
	var terminalLogs int
	for _, entry := range logger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
		if entry.Message != "Evented PLEG: disabling container-termination fast path after exhausting event stream retries" {
			continue
		}
		terminalLogs++
		require.ErrorIs(t, entry.Err, errEventedPLEGStreamClosed)
	}
	assert.Equal(t, 1, terminalLogs)
}

// blockingRuntimeService 在 GetContainerEvents 中阻塞，直到上下文取消。
type blockingRuntimeService struct {
	internalapi.RuntimeService
	started chan struct{}
}

// GetContainerEvents 通知已启动后阻塞，直到上下文取消。
func (f *blockingRuntimeService) GetContainerEvents(ctx context.Context, _ chan *v1.ContainerEventResponse, _ func(v1.RuntimeService_GetContainerEventsClient)) error {
	close(f.started)
	<-ctx.Done()
	return ctx.Err()
}

// TestEventedPLEGWatcherStopsWhenContextIsCanceled 验证上下文取消后事件监听能够退出，
// 且不会记录重试耗尽的终态日志。
func TestEventedPLEGWatcherStopsWhenContextIsCanceled(t *testing.T) {
	runtimeService := &blockingRuntimeService{started: make(chan struct{})}
	pleg := newTestEventedPLEG(newFakePodRelister())
	pleg.runtimeService = runtimeService

	logger := ktesting.NewLogger(t, ktesting.NewConfig(ktesting.BufferLogs(true), ktesting.Verbosity(0)))
	ctx, cancel := context.WithCancel(klog.NewContext(t.Context(), logger))
	done := make(chan struct{})
	// 在后台运行事件监听。
	go func() {
		defer close(done)
		pleg.watchEventsChannel(ctx)
	}()

	select {
	// 等待监听循环真正启动后再取消上下文。
	case <-runtimeService.started:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("EventedPLEG did not start watching container events")
	}
	cancel()

	// 取消后监听循环应退出。
	select {
	case <-done:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("EventedPLEG did not stop after its context was canceled")
	}

	// 主动取消不应产生重试耗尽的终态日志。
	for _, entry := range logger.GetSink().(ktesting.Underlier).GetBuffer().Data() {
		assert.NotEqual(t, "Evented PLEG: disabling container-termination fast path after exhausting event stream retries", entry.Message)
	}
}

// TestProcessCRIEventsRequestsRelistOnlyForStoppedEvents 验证只有容器停止事件
// 会立即触发 Pod 重新列举。
func TestProcessCRIEventsRequestsRelistOnlyForStoppedEvents(t *testing.T) {
	// 每个用例检查处理事件后请求重新列举的 Pod 列表。
	tests := []struct {
		name       string
		event      *v1.ContainerEventResponse
		wantRelist []types.UID
	}{
		{
			name: "stopped non-zero exit requests relist",
			event: newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_STOPPED_EVENT, &v1.ContainerStatus{
				Id:       "container1",
				State:    v1.ContainerState_CONTAINER_EXITED,
				ExitCode: 2,
			}),
			wantRelist: []types.UID{"pod1"},
		},
		{
			name: "stopped OOMKilled requests relist",
			event: newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_STOPPED_EVENT, &v1.ContainerStatus{
				Id:       "container1",
				State:    v1.ContainerState_CONTAINER_EXITED,
				ExitCode: 0,
				Reason:   "OOMKilled",
			}),
			wantRelist: []types.UID{"pod1"},
		},
		{
			name: "stopped clean exit requests relist",
			event: newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_STOPPED_EVENT, &v1.ContainerStatus{
				Id:       "container1",
				State:    v1.ContainerState_CONTAINER_EXITED,
				ExitCode: 0,
			}),
			wantRelist: []types.UID{"pod1"},
		},
		{
			name: "started event waits for generic relist",
			event: newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_STARTED_EVENT, &v1.ContainerStatus{
				Id:       "container1",
				State:    v1.ContainerState_CONTAINER_RUNNING,
				ExitCode: 2,
			}),
		},
		{
			name: "deleted event waits for generic relist",
			event: newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_DELETED_EVENT, &v1.ContainerStatus{
				Id:       "container1",
				State:    v1.ContainerState_CONTAINER_EXITED,
				ExitCode: 2,
			}),
		},
		{
			name:       "stopped event without container status requests relist",
			event:      newContainerEvent("pod1", "container1", v1.ContainerEventType_CONTAINER_STOPPED_EVENT),
			wantRelist: []types.UID{"pod1"},
		},
		{
			name: "missing sandbox metadata waits for generic relist",
			event: &v1.ContainerEventResponse{
				ContainerId:        "container1",
				ContainerEventType: v1.ContainerEventType_CONTAINER_STOPPED_EVENT,
				CreatedAt:          time.Now().UnixNano(),
				ContainersStatuses: []*v1.ContainerStatus{{
					Id:       "container1",
					State:    v1.ContainerState_CONTAINER_EXITED,
					ExitCode: 2,
				}},
			},
		},
	}

	// 逐个处理事件，并校验重新列举请求。
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			podRelister := newFakePodRelister()
			eventedPleg := newTestEventedPLEG(podRelister)
			logger := ktesting.NewLogger(t, ktesting.DefaultConfig)
			eventsCh := make(chan *v1.ContainerEventResponse, 1)
			eventsCh <- test.event
			close(eventsCh)

			eventedPleg.processCRIEvents(logger, eventsCh)

			assert.Equal(t, test.wantRelist, podRelister.relistRequests)
		})
	}
}

// newContainerEvent 构造带沙箱元数据的容器事件响应。
func newContainerEvent(podUID types.UID, containerID string, eventType v1.ContainerEventType, statuses ...*v1.ContainerStatus) *v1.ContainerEventResponse {
	return &v1.ContainerEventResponse{
		ContainerId:        containerID,
		ContainerEventType: eventType,
		CreatedAt:          time.Now().UnixNano(),
		PodSandboxStatus: &v1.PodSandboxStatus{
			Metadata: &v1.PodSandboxMetadata{
				Uid: string(podUID),
			},
		},
		ContainersStatuses: statuses,
	}
}
