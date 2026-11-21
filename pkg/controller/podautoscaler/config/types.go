/*
Copyright 2019 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// HPAControllerConfiguration 包含描述 HPAController 的各项配置。
type HPAControllerConfiguration struct {
	// ConcurrentHorizontalPodAutoscalerSyncs 允许并发同步的 HPA 对象数量。
	// 数值越大，HPA 处理响应越及时，但也会带来更高的 CPU（和网络）负载。
	ConcurrentHorizontalPodAutoscalerSyncs int32
	// HorizontalPodAutoscalerSyncPeriod 是水平 Pod 自动扩缩容器同步 Pod 数量的周期。
	HorizontalPodAutoscalerSyncPeriod metav1.Duration
	// HorizontalPodAutoscalerDownscaleStabilizationWindow 是自动扩缩容器回看的时间窗口：
	// 在该窗口内，不会把 Pod 数量降到低于期间任意一次推荐值，以避免缩容抖动。
	HorizontalPodAutoscalerDownscaleStabilizationWindow metav1.Duration
	// HorizontalPodAutoscalerTolerance 是资源使用率触发扩容/缩容判断的容差。
	HorizontalPodAutoscalerTolerance float64
	// HorizontalPodAutoscalerCPUInitializationPeriod 是 Pod 启动后的一段时期，
	// 期间采集到的 CPU 样本可能会被忽略，因为启动阶段的数据尚不稳定。
	HorizontalPodAutoscalerCPUInitializationPeriod metav1.Duration
	// HorizontalPodAutoscalerInitialReadinessDelay 是 Pod 启动后的一段时间，
	// 期间就绪状态的变化被视为首次设置就绪。其唯一效果是：
	// HPA 会忽略在该时间段内最后一次发生就绪状态变化的未就绪 Pod 的 CPU 样本。
	HorizontalPodAutoscalerInitialReadinessDelay metav1.Duration
}
