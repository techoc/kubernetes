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
	"time"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/klog/v2"
)

// PodLifeCycleEventType 定义 Pod 生命周期事件的类型。
type PodLifeCycleEventType string

// RelistDuration 定义 relist 的周期与健康判定阈值。
type RelistDuration struct {
	// RelistPeriod 是 relist 的周期。
	RelistPeriod time.Duration
	// RelistThreshold 需要大于 relist 周期加上 relist 耗时（后者波动可能很大）。
	// 应设置保守的阈值，避免健康状态在正常与异常之间反复抖动。
	RelistThreshold time.Duration
}

const (
	// ContainerStarted 表示容器新状态为 running。
	ContainerStarted PodLifeCycleEventType = "ContainerStarted"
	// ContainerDied 表示容器新状态为 exited。
	ContainerDied PodLifeCycleEventType = "ContainerDied"
	// ContainerRemoved 表示容器旧状态已 exited（容器被移除）。
	ContainerRemoved PodLifeCycleEventType = "ContainerRemoved"
	// PodSync 用于在观察到的 Pod 状态变化无法由上述单个事件表达时触发 Pod 同步。
	PodSync PodLifeCycleEventType = "PodSync"
	// ContainerChanged 表示容器新状态未知。
	ContainerChanged PodLifeCycleEventType = "ContainerChanged"
)

// PodLifecycleEvent 表示 Pod 状态变化事件。
type PodLifecycleEvent struct {
	// ID 是 Pod 的 UID。
	ID types.UID
	// Type 是事件类型。
	Type PodLifeCycleEventType
	// Data 是随事件类型变化的附带数据：
	//   - ContainerStarted/ContainerStopped：容器名（string）。
	//   - 其他事件类型：未使用。
	Data interface{}
}

// PodLifecycleEventGenerator 定义生成 Pod 生命周期事件的方法。
type PodLifecycleEventGenerator interface {
	// Start 启动事件生成循环。
	Start(ctx context.Context)
	// Watch 返回接收 Pod 生命周期事件的 channel。
	Watch() chan *PodLifecycleEvent
	// Healthy 返回 PLEG 是否健康。
	Healthy() (bool, error)
	// RequestReinspect 标记该 Pod 在下一轮 Relist 时重新检查。
	RequestReinspect(podUID types.UID)
	// RequestRelist 把该 Pod 加入按需 relist 队列。
	RequestRelist(logger klog.Logger, podUID types.UID)
}
