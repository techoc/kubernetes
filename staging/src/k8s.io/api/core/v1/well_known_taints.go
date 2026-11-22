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

package v1

const (
	// TaintNodeNotReady 在节点未就绪时添加，节点恢复就绪时移除。
	TaintNodeNotReady = "node.kubernetes.io/not-ready"

	// TaintNodeUnreachable 在节点变得不可达时添加（对应 NodeReady 状态 ConditionUnknown），
	// 在节点恢复可达时移除（NodeReady 状态 ConditionTrue）。
	TaintNodeUnreachable = "node.kubernetes.io/unreachable"

	// TaintNodeUnschedulable 在节点变为不可调度时添加，节点恢复可调度时移除。
	TaintNodeUnschedulable = "node.kubernetes.io/unschedulable"

	// TaintNodeMemoryPressure 在节点出现内存压力时添加，内存充足时移除。
	TaintNodeMemoryPressure = "node.kubernetes.io/memory-pressure"

	// TaintNodeDiskPressure 在节点出现磁盘压力时添加，磁盘空间充足时移除。
	TaintNodeDiskPressure = "node.kubernetes.io/disk-pressure"

	// TaintNodeNetworkUnavailable 在节点网络不可用时添加，网络就绪时移除。
	TaintNodeNetworkUnavailable = "node.kubernetes.io/network-unavailable"

	// TaintNodePIDPressure 在节点出现 PID 压力时添加，PID 充足时移除。
	TaintNodePIDPressure = "node.kubernetes.io/pid-pressure"

	// TaintNodeOutOfService 可在节点非正常关闭导致停止服务时添加。
	TaintNodeOutOfService = "node.kubernetes.io/out-of-service"
)
