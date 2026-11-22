/*
Copyright 2021 The Kubernetes Authors.

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

// Package names 定义调度器框架各插件的名称常量。
// 这些字符串用于配置中引用插件，以及插件注册表里的键。
package names

const (
	// PrioritySort 按优先级对 Pod 排序的队列排序插件。
	PrioritySort = "PrioritySort"
	// DefaultBinder 将 Pod 绑定到选定节点的默认绑定插件。
	DefaultBinder = "DefaultBinder"
	// DefaultPreemption 默认抢占插件，为高优先级 Pod 寻找可抢占的牺牲者。
	DefaultPreemption = "DefaultPreemption"
	// DynamicResources 处理动态资源声明的插件。
	DynamicResources = "DynamicResources"
	// GangScheduling gang（PodGroup）调度插件，保证一组 Pod 同时满足调度条件。
	GangScheduling = "GangScheduling"
	// ImageLocality 根据节点上已有镜像情况打分的插件。
	ImageLocality = "ImageLocality"
	// InterPodAffinity 处理 Pod 间亲和与反亲和的插件。
	InterPodAffinity = "InterPodAffinity"
	// NodeAffinity 处理 Pod 与节点亲和性的插件。
	NodeAffinity = "NodeAffinity"
	// NodeDeclaredFeatures 根据节点声明的特性进行过滤的插件。
	NodeDeclaredFeatures = "NodeDeclaredFeatures"
	// NodeName 处理指定了 spec.nodeName 的 Pod 的插件。
	NodeName = "NodeName"
	// NodePorts 检查节点上端口是否可用的插件。
	NodePorts = "NodePorts"
	// NodeResourcesBalancedAllocation 按资源使用均衡度打分的插件。
	NodeResourcesBalancedAllocation = "NodeResourcesBalancedAllocation"
	// NodeResourcesFit 检查节点资源是否满足 Pod 请求的插件。
	NodeResourcesFit = "NodeResourcesFit"
	// NodeUnschedulable 过滤不可调度节点的插件。
	NodeUnschedulable = "NodeUnschedulable"
	// NodeVolumeLimits 检查节点卷数量限制的插件。
	NodeVolumeLimits = "NodeVolumeLimits"
	// PodTopologySpread 按拓扑域分散 Pod 的插件。
	PodTopologySpread = "PodTopologySpread"
	// DeferredPodScheduling 处理延迟调度 Pod 的插件。
	DeferredPodScheduling = "DeferredPodScheduling"
	// SchedulingGates 处理带调度门禁的 Pod 的插件。
	SchedulingGates = "SchedulingGates"
	// TaintToleration 处理节点污点与 Pod 容忍的插件。
	TaintToleration = "TaintToleration"
	// VolumeBinding 处理卷绑定与卷拓扑的插件。
	VolumeBinding = "VolumeBinding"
	// VolumeRestrictions 检查卷相关限制（如单节点卷数量）的插件。
	VolumeRestrictions = "VolumeRestrictions"
	// VolumeZone 检查卷所在可用区的插件。
	VolumeZone = "VolumeZone"
	// TopologyPlacementGenerator 生成拓扑放置决策的插件。
	TopologyPlacementGenerator = "TopologyPlacementGenerator"
	// PodGroupPodsCount 按 PodGroup 中的 Pod 数量执行约束的插件。
	PodGroupPodsCount = "PodGroupPodsCount"
)
