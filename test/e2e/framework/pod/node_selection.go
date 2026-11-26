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

package pod

import (
	v1 "k8s.io/api/core/v1"
)

// NodeSelection 用固定节点名、node selector 和/或 affinity 的组合指定 Pod 的运行位置。
type NodeSelection struct {
	Name     string
	Selector map[string]string
	Affinity *v1.Affinity
}

// setNodeAffinityRequirement 给 nodeSelection 设置针对 nodeName、使用指定 operator 的 affinity。
func setNodeAffinityRequirement(nodeSelection *NodeSelection, operator v1.NodeSelectorOperator, nodeName string) {
	// 添加 node anti-affinity。
	if nodeSelection.Affinity == nil {
		nodeSelection.Affinity = &v1.Affinity{}
	}
	if nodeSelection.Affinity.NodeAffinity == nil {
		nodeSelection.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	if nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
		v1.NodeSelectorTerm{
			MatchFields: []v1.NodeSelectorRequirement{
				{Key: "metadata.name", Operator: operator, Values: []string{nodeName}},
			},
		})
}

// SetNodeAffinityTopologyRequirement 设置针对指定拓扑的 node affinity
func SetNodeAffinityTopologyRequirement(nodeSelection *NodeSelection, topology map[string]string) {
	if nodeSelection.Affinity == nil {
		nodeSelection.Affinity = &v1.Affinity{}
	}
	if nodeSelection.Affinity.NodeAffinity == nil {
		nodeSelection.Affinity.NodeAffinity = &v1.NodeAffinity{}
	}
	if nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = &v1.NodeSelector{}
	}
	for k, v := range topology {
		nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = append(nodeSelection.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms,
			v1.NodeSelectorTerm{
				MatchExpressions: []v1.NodeSelectorRequirement{
					{Key: k, Operator: v1.NodeSelectorOpIn, Values: []string{v}},
				},
			})

	}
}

// SetAffinity 给 nodeSelection 设置针对 nodeName 的 affinity
func SetAffinity(nodeSelection *NodeSelection, nodeName string) {
	setNodeAffinityRequirement(nodeSelection, v1.NodeSelectorOpIn, nodeName)
}

// SetAntiAffinity 给 nodeSelection 设置针对 nodeName 的 anti-affinity
func SetAntiAffinity(nodeSelection *NodeSelection, nodeName string) {
	setNodeAffinityRequirement(nodeSelection, v1.NodeSelectorOpNotIn, nodeName)
}

// SetNodeAffinity 修改给定 Pod 对象，
// 为其设置指向给定节点名的 NodeAffinity。
func SetNodeAffinity(podSpec *v1.PodSpec, nodeName string) {
	nodeSelection := &NodeSelection{}
	SetAffinity(nodeSelection, nodeName)
	podSpec.Affinity = nodeSelection.Affinity
}

// SetNodeSelection 按指定的 NodeSelection 修改给定 Pod 对象
func SetNodeSelection(podSpec *v1.PodSpec, nodeSelection NodeSelection) {
	podSpec.NodeSelector = nodeSelection.Selector
	podSpec.Affinity = nodeSelection.Affinity
	// 不直接设置 pod.Spec.NodeName，因为那样会绕过 scheduler，
	// 节点资源不足时 kubelet 可能立即让 Pod 失败。
	// 我们希望 Pod 保持 pending，由 scheduler 等待
	// 节点释放资源后再调度。
	//
	//
	if nodeSelection.Name != "" {
		SetNodeAffinity(podSpec, nodeSelection.Name)
	}
}
