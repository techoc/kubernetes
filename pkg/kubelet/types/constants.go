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

package types

const (
	// ResolvConfDefault 是系统默认的 DNS 解析器配置路径。
	ResolvConfDefault = "/etc/resolv.conf"
)

// 用于管理节点可分配资源约束的、对用户可见的 key。
const (
	NodeAllocatableEnforcementKey            = "pods"
	SystemReservedEnforcementKey             = "system-reserved"
	SystemReservedCompressibleEnforcementKey = "system-reserved-compressible"
	KubeReservedEnforcementKey               = "kube-reserved"
	KubeReservedCompressibleEnforcementKey   = "kube-reserved-compressible"
	NodeAllocatableNoneKey                   = "none"
)

// SwapBehavior 定义交换行为类型。
type SwapBehavior string

const (
	// LimitedSwap 允许容器使用部分交换空间（受内存限制约束）。
	LimitedSwap SwapBehavior = "LimitedSwap"
	// NoSwap 表示容器不使用交换空间。
	NoSwap SwapBehavior = "NoSwap"
)

// Pod 状态 condition 的原因与消息。
const (
	// PodReadyToStartContainers condition 的原因。
	PodSandboxNotReadyReason = "PodSandboxNotReady"

	// PodReadyToStartContainers condition 的消息。
	PodSandboxNotReadyMsgNoPodSandbox         = "no pod sandbox exists"
	PodSandboxNotReadyMsgMultipleSandboxes    = "multiple pod sandboxes are ready and need to be reconciled"
	PodSandboxNotReadyMsgSandboxNotReady      = "pod sandbox is not ready"
	PodSandboxNotReadyMsgNetworkNamespaceMode = "network namespace mode changed and sandbox needs to be recreated"
	PodSandboxNotReadyMsgNoIPAddress          = "pod sandbox has no IP address"
)
