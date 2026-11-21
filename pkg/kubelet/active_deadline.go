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

package kubelet

import (
	"fmt"
	"time"

	"k8s.io/api/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/status"
	"k8s.io/utils/clock"
)

const (
	// 活动截止时间驱逐使用的事件原因与消息。
	reason  = "DeadlineExceeded"
	message = "Pod was active on the node longer than the specified deadline"
)

// activeDeadlineHandler 负责执行 Pod 的活动截止时间检查。
type activeDeadlineHandler struct {
	// 用于计算截止时间的时钟。
	clock clock.Clock
	// Pod 状态提供者。
	podStatusProvider status.PodStatusProvider
	// 发现 Pod 超过活动截止时间时用于记录事件。
	recorder record.EventRecorder
}

// newActiveDeadlineHandler 创建可执行 Pod 活动截止时间检查的处理器。
func newActiveDeadlineHandler(
	podStatusProvider status.PodStatusProvider,
	recorder record.EventRecorder,
	clock clock.Clock,
) (*activeDeadlineHandler, error) {

	// 检查所有必需字段。
	// 三个依赖缺一不可，否则无法执行检查。
	if clock == nil || podStatusProvider == nil || recorder == nil {
		return nil, fmt.Errorf("required arguments must not be nil: %v, %v, %v", clock, podStatusProvider, recorder)
	}
	return &activeDeadlineHandler{
		clock:             clock,
		podStatusProvider: podStatusProvider,
		recorder:          recorder,
	}, nil
}

// ShouldSync 返回 Pod 是否已超过活动截止时间。
func (m *activeDeadlineHandler) ShouldSync(pod *v1.Pod) bool {
	return m.pastActiveDeadline(pod)
}

// ShouldEvict 返回 Pod 是否已超过活动截止时间。
// 若已超期，会记录一条应驱逐 Pod 的事件。
func (m *activeDeadlineHandler) ShouldEvict(pod *v1.Pod) lifecycle.ShouldEvictResponse {
	if !m.pastActiveDeadline(pod) {
		return lifecycle.ShouldEvictResponse{Evict: false}
	}
	// 记录事件，便于用户感知驱逐原因。
	m.recorder.Eventf(pod, v1.EventTypeNormal, reason, message)
	return lifecycle.ShouldEvictResponse{Evict: true, Reason: reason, Message: message}
}

// pastActiveDeadline 判断 Pod 的活动时长是否已超过 ActiveDeadlineSeconds。
func (m *activeDeadlineHandler) pastActiveDeadline(pod *v1.Pod) bool {
	// 未设置活动截止时间。
	if pod.Spec.ActiveDeadlineSeconds == nil {
		return false
	}
	// 获取最新状态以判断 Pod 是否已启动。
	// 优先使用状态提供者的最新状态，取不到时退回 Pod 自身状态。
	podStatus, ok := m.podStatusProvider.GetPodStatus(pod.UID)
	if !ok {
		podStatus = pod.Status
	}
	// 没有启动时间，无法判断是否超期。
	if podStatus.StartTime.IsZero() {
		return false
	}
	// 比较已运行时长与允许的截止时长。
	start := podStatus.StartTime.Time
	duration := m.clock.Since(start)
	allowedDuration := time.Duration(*pod.Spec.ActiveDeadlineSeconds) * time.Second
	return duration >= allowedDuration
}
