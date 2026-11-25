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

package service

import (
	"time"
)

const (
	// RespondingTimeout 是等待 Service 开始响应所需的时间。
	RespondingTimeout = 2 * time.Minute

	// MaxNodesForEndpointsTests 是测试 endpoints 时允许的最大节点数。
	// 不要用超过 3 个节点进行测试。
	// 很多测试会为每个节点创建一个 endpoint，在大规模集群中，这非常消耗资源和时间。
	MaxNodesForEndpointsTests = 3

	// KubeProxyLagTimeout 是节点上的 kube-proxy 守护进程被允许
	// 未察觉 Service 更新（如 type=NodePort）的最长时间。
	// TODO: 这个超时应该是 O(10s) 量级，观测到的值是 O(1m) 量级，2m 太宽松了。
	// 修复进度见 #20567。
	KubeProxyLagTimeout = 2 * time.Minute

	// KubeProxyEndpointLagTimeout 是节点上的 kube-proxy 守护进程被允许
	// 未察觉 Endpoint 更新的最长时间。
	KubeProxyEndpointLagTimeout = 30 * time.Second

	// LoadBalancerLagTimeoutDefault 是 load balancer 创建后
	// 被允许不响应的最长时间。
	LoadBalancerLagTimeoutDefault = 2 * time.Minute

	// LoadBalancerLagTimeoutAWS 是在 AWS 上从 ELB 创建到开始承载流量之间的延迟。
	// 通常需要几分钟，所以取 10m。
	LoadBalancerLagTimeoutAWS = 10 * time.Minute

	// LoadBalancerCreateTimeoutDefault 是等待 load balancer 创建/修改的默认时间。
	// TODO: 等支持工单 21807001 解决后，把这个超时调回合理值。
	// 已隐藏 - 请改用 GetServiceLoadBalancerCreateTimeout 函数。
	loadBalancerCreateTimeoutDefault = 15 * time.Minute
	// LoadBalancerCreateTimeoutLarge 是等待 load balancer 创建/修改的最长时间。
	// 已隐藏 - 请改用 GetServiceLoadBalancerCreateTimeout 函数。
	loadBalancerCreateTimeoutLarge = 60 * time.Minute

	// LoadBalancerPropagationTimeoutDefault 是等待 Pod 被 load balancer 选中作为后端的默认时间。
	// 已隐藏 - 请改用 GetServiceLoadBalancerPropagationTimeout 函数。
	loadBalancerPropagationTimeoutDefault = 10 * time.Minute
	// LoadBalancerPropagationTimeoutLarge 是等待 Pod 被 load balancer 选中作为后端的最长时间。
	// 已隐藏 - 请改用 GetServiceLoadBalancerPropagationTimeout 函数。
	loadBalancerPropagationTimeoutLarge = time.Hour

	// LoadBalancerCleanupTimeout 是 loadbalancer 清理所需的时间，与 numApps/Ing 成正比。
	// 等 b/33588344 解决后，把清理超时降回 5m。
	LoadBalancerCleanupTimeout = 15 * time.Minute

	// LoadBalancerPollInterval 是 loadbalancer 轮询的时间间隔。
	LoadBalancerPollInterval = 30 * time.Second

	// LargeClusterMinNodesNumber 是构成大规模集群的节点数。
	LargeClusterMinNodesNumber = 100

	// TestTimeout 用于大多数轮询/等待操作。
	TestTimeout = 60 * time.Second

	// ServiceEndpointsTimeout 是创建 Service 的 endpoints 所允许的最长时间。
	ServiceEndpointsTimeout = 2 * time.Minute

	// ServiceReachabilityShortPollTimeout 是轮询期间 Service 必须可达的最长时间。
	ServiceReachabilityShortPollTimeout = 2 * time.Minute
)
