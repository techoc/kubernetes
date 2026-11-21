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

// NodeLifecycleControllerConfiguration 包含描述 NodeLifecycleController（节点生命周期控制器）的各项配置。
type NodeLifecycleControllerConfiguration struct {
	// nodeEvictionRate 表示可用区健康时，节点发生故障后每秒驱逐 Pod 的节点数量。
	NodeEvictionRate float32
	// secondaryNodeEvictionRate 表示可用区不健康时，节点发生故障后每秒驱逐 Pod 的节点数量。
	SecondaryNodeEvictionRate float32
	// nodeStartupGracePeriod 表示节点启动后，在将其标记为不健康之前
	// 允许其无响应的时长。
	NodeStartupGracePeriod metav1.Duration
	// NodeMonitorGracePeriod 表示正在运行的节点在标记为不健康之前
	// 允许其无响应的时长。该值必须是 kubelet 的 nodeStatusUpdateFrequency 的 N 倍，
	// 其中 N 为允许 kubelet 上报节点状态的重试次数。该值还应大于
	// HTTP2_PING_TIMEOUT_SECONDS 与 HTTP2_READ_IDLE_TIMEOUT_SECONDS 之和，
	// 以免 HTTP/2 连接空闲超时被误判为节点失联。
	NodeMonitorGracePeriod metav1.Duration
	// 对于规模小于或等于 largeClusterSizeThreshold 的集群，
	// secondaryNodeEvictionRate 会被隐式覆盖为 0。
	LargeClusterSizeThreshold int32
	// 当某个可用区中处于 NotReady 状态的节点数至少达到
	// unhealthyZoneThreshold（不小于 3）时，该可用区在 nodeEvictionRate
	// 和 secondaryNodeEvictionRate 的计算中被视为不健康。
	UnhealthyZoneThreshold float32
	// NodeMonitorPeriod 表示 NodeLifecycleController 同步 NodeStatus 的周期。
	NodeMonitorPeriod metav1.Duration
}
