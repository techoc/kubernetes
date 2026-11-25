/*
Copyright 2022 The Kubernetes Authors.

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
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/types"
	internalapi "k8s.io/cri-api/pkg/apis"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/metrics"
)

// eventedPLEGContainerEventsChannelCapacity 是事件通道的缓冲长度，用于吸收突发事件。
const eventedPLEGContainerEventsChannelCapacity = 1000

// errEventedPLEGStreamClosed 表示事件流在无错误的情况下被关闭，需要重连。
var errEventedPLEGStreamClosed = errors.New("CRI container event stream closed without an error")

// podRelister 抽象出请求 GenericPLEG 重新列举 Pod 的能力。
type podRelister interface {
	// RequestRelist 请求尽快重新列举指定 Pod。
	RequestRelist(logger klog.Logger, podUID types.UID)
}

// EventedPLEG 监听 CRI 容器事件并触发低延迟的 Pod 重新列举。
// 运行时状态观测、缓存更新和 Pod 生命周期事件仍由 GenericPLEG 负责。
type EventedPLEG struct {
	runtimeService internalapi.RuntimeService
	podRelister    podRelister
	startOnce      sync.Once

	eventedPlegMaxStreamRetries int
}

// NewEventedPLEG 创建 CRI 事件监听器，用于触发低延迟的 Pod 重新列举。
func NewEventedPLEG(runtimeService internalapi.RuntimeService, relister podRelister,
	eventedPlegMaxStreamRetries int) *EventedPLEG {
	return &EventedPLEG{
		runtimeService:              runtimeService,
		podRelister:                 relister,
		eventedPlegMaxStreamRetries: eventedPlegMaxStreamRetries,
	}
}

// Start 启动 CRI 容器事件监听，直到上下文取消或流重试次数耗尽。
func (e *EventedPLEG) Start(ctx context.Context) {
	e.startOnce.Do(func() {
		go e.watchEventsChannel(ctx)
	})
}

// watchEventsChannel 启动事件拉取协程，并在当前协程消费事件流。
func (e *EventedPLEG) watchEventsChannel(ctx context.Context) {
	logger := klog.FromContext(ctx)
	containerEventsResponseCh := make(chan *runtimeapi.ContainerEventResponse, eventedPLEGContainerEventsChannelCapacity)

	// 从运行时拉取容器事件。
	go func() {
		defer close(containerEventsResponseCh)

		numAttempts := 0
		var lastStreamErr error
		for {
			// 重试次数耗尽后放弃事件快路径，回退到 GenericPLEG 的周期轮询。
			if numAttempts >= e.eventedPlegMaxStreamRetries {
				logger.Error(lastStreamErr, "Evented PLEG: disabling container-termination fast path after exhausting event stream retries", "retries", numAttempts)
				return
			}

			err := e.runtimeService.GetContainerEvents(ctx, containerEventsResponseCh, func(runtimeapi.RuntimeService_GetContainerEventsClient) {
				metrics.EventedPLEGConn.Inc()
			})
			if ctx.Err() != nil {
				return
			}
			// 流正常结束也视为异常，需要重连。
			if err == nil {
				err = errEventedPLEGStreamClosed
			}
			metrics.EventedPLEGConnErr.Inc()
			numAttempts++
			lastStreamErr = err
			logger.V(4).Info("Evented PLEG: failed to get container events, retrying", "err", err)
		}
	}()

	e.processCRIEvents(logger, containerEventsResponseCh)
}

// processCRIEvents 消费 CRI 容器事件流。
// 它只是快路径提示：容器终止会触发 GenericPLEG 立即重新列举。
// Pod 缓存更新和生命周期事件仍只由 GenericPLEG 产生。
func (e *EventedPLEG) processCRIEvents(logger klog.Logger, containerEventsResponseCh <-chan *runtimeapi.ContainerEventResponse) {
	for event := range containerEventsResponseCh {
		// PodSandboxStatus 为 nil 时忽略该事件。
		// 这可能发生在 podSandbox 已被删除的竞态场景：运行时生成事件时
		// 已找不到容器所属的 podSandbox。忽略是安全的：
		// a) sandbox 删除本身会产生事件；
		// b) 最坏情况下后续 relist 也会同步 Pod 状态。
		// TODO(#114371): 设法处理这种情况，而不是直接忽略。
		if event.PodSandboxStatus == nil || event.PodSandboxStatus.Metadata == nil {
			logger.Error(nil, "Evented PLEG: received ContainerEventResponse with nil PodSandboxStatus or PodSandboxStatus.Metadata", "containerEventResponse", event)
			continue
		}

		// 记录事件从产生到被处理的时间差。
		e.updateLatencyMetric(event)

		// 只有容器停止事件才需要触发即时 relist。
		if event.ContainerEventType != runtimeapi.ContainerEventType_CONTAINER_STOPPED_EVENT {
			continue
		}

		podID := types.UID(event.PodSandboxStatus.Metadata.Uid)
		// 请求 GenericPLEG 立即重新列举该 Pod。
		e.podRelister.RequestRelist(logger, podID)

		logger.V(4).Info("Evented PLEG: requested pod relist after container termination",
			"podUID", podID,
			"containerID", event.ContainerId,
			"eventType", event.ContainerEventType)
	}
}

// updateLatencyMetric 观测 CRI 事件的处理延迟。
func (e *EventedPLEG) updateLatencyMetric(event *runtimeapi.ContainerEventResponse) {
	duration := time.Duration(time.Now().UnixNano()-event.CreatedAt) * time.Nanosecond
	metrics.EventedPLEGConnLatency.Observe(duration.Seconds())
}
