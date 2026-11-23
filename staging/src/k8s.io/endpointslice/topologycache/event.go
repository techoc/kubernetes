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

package topologycache

// TopologyAwareHints events messages list.
// TopologyAwareHints 事件消息列表。
const (
	// NoZoneSpecified 表示一个或多个端点没有指定可用区。
	// NoAllocatedHintsForZones 表示没有任何可用区被分配到提示。
	// TopologyAwareHintsEnabled 表示拓扑感知提示已被启用。
	// TopologyAwareHintsDisabled 表示拓扑感知提示已被禁用。
	// InsufficientNodeInfo 表示节点信息不足：某些节点未指定可分配 CPU 或可用区。
	// NodesReadyInOneZoneOnly 表示只有单一可用区中存在就绪节点。
	// InsufficientNumberOfEndpoints 表示端点数量不足。
	// MinAllocationExceedsOverloadThreshold 表示无法在不超出过载阈值的前提下为每个可用区分配最少数量的端点。
	NoZoneSpecified                       = "One or more endpoints do not have a zone specified"
	NoAllocatedHintsForZones              = "No hints allocated for zones"
	TopologyAwareHintsEnabled             = "Topology Aware Hints has been enabled"
	TopologyAwareHintsDisabled            = "Topology Aware Hints has been disabled"
	InsufficientNodeInfo                  = "Insufficient Node information: allocatable CPU or zone not specified on one or more nodes"
	NodesReadyInOneZoneOnly               = "Nodes only ready in one zone"
	InsufficientNumberOfEndpoints         = "Insufficient number of endpoints"
	MinAllocationExceedsOverloadThreshold = "Unable to allocate minimum required endpoints to each zone without exceeding overload threshold"
)

// EventBuilder let's us construct events in the code.
// We use it to build events and return them from a function instead of publishing them from within it.
// EventType, Reason, and Message fields are equivalent to the v1.Event fields - https://pkg.go.dev/k8s.io/api/core/v1#Event.
// EventBuilder 用于在代码中构造事件：函数只构建并返回事件，而不在函数内部发布事件。
// 其 EventType、Reason、Message 字段与 v1.Event 的同名字段含义一致。
type EventBuilder struct {
	// EventType 表示事件类型（如 Normal 或 Warning）。
	// Reason 表示事件原因。
	// Message 表示事件的可读描述。
	EventType string
	Reason    string
	Message   string
}
