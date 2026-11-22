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

package defaults

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
)

// PluginsV1 是 v1 版本的默认插件集合（MultiPoint 扩展之前）。
var PluginsV1 = &config.Plugins{
	MultiPoint: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.SchedulingGates},
			{Name: names.PrioritySort},
			{Name: names.NodeName},
			{Name: names.NodeUnschedulable},
			{Name: names.TaintToleration, Weight: 3},
			{Name: names.NodeAffinity, Weight: 2},
			{Name: names.NodePorts},
			{Name: names.NodeResourcesFit, Weight: 1},
			{Name: names.VolumeRestrictions},
			{Name: names.NodeVolumeLimits},
			{Name: names.VolumeBinding},
			{Name: names.VolumeZone},
			{Name: names.PodTopologySpread, Weight: 2},
			{Name: names.InterPodAffinity, Weight: 2},
			{Name: names.DynamicResources, Weight: 2},
			{Name: names.DefaultPreemption},
			{Name: names.NodeResourcesBalancedAllocation, Weight: 1},
			{Name: names.ImageLocality, Weight: 1},
			{Name: names.DefaultBinder},
			{Name: names.NodeDeclaredFeatures},
		},
	},
}

// ExpandedPluginsV1 是 v1 版本 MultiPoint 展开后的默认插件集合。
var ExpandedPluginsV1 = &config.Plugins{
	PreEnqueue: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.SchedulingGates},
			{Name: names.DynamicResources},
			{Name: names.DefaultPreemption},
		},
	},
	QueueSort: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.PrioritySort},
		},
	},
	PreFilter: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.NodeName},
			{Name: names.NodeUnschedulable},
			{Name: names.TaintToleration},
			{Name: names.NodeAffinity},
			{Name: names.NodePorts},
			{Name: names.NodeResourcesFit},
			{Name: names.VolumeRestrictions},
			{Name: names.NodeVolumeLimits},
			{Name: names.VolumeBinding},
			{Name: names.VolumeZone},
			{Name: names.PodTopologySpread},
			{Name: names.InterPodAffinity},
			{Name: names.DynamicResources},
			{Name: names.NodeDeclaredFeatures},
		},
	},
	Filter: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.NodeName},
			{Name: names.NodeUnschedulable},
			{Name: names.TaintToleration},
			{Name: names.NodeAffinity},
			{Name: names.NodePorts},
			{Name: names.NodeResourcesFit},
			{Name: names.VolumeRestrictions},
			{Name: names.NodeVolumeLimits},
			{Name: names.VolumeBinding},
			{Name: names.VolumeZone},
			{Name: names.PodTopologySpread},
			{Name: names.InterPodAffinity},
			{Name: names.DynamicResources},
			{Name: names.NodeDeclaredFeatures},
		},
	},
	PostFilter: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.DynamicResources},
			{Name: names.DefaultPreemption},
		},
	},
	PodGroupPostFilter: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.DynamicResources},
			{Name: names.DefaultPreemption},
		},
	},
	PreScore: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.TaintToleration},
			{Name: names.NodeAffinity},
			{Name: names.NodeResourcesFit},
			{Name: names.VolumeBinding},
			{Name: names.PodTopologySpread},
			{Name: names.InterPodAffinity},
			{Name: names.NodeResourcesBalancedAllocation},
		},
	},
	Score: config.PluginSet{
		Enabled: []config.Plugin{
			// 权重设为三倍：该分数来自用户偏好，且用污点给节点分组的做法越来越常见。
			{Name: names.TaintToleration, Weight: 3},
			// 权重翻倍：该分数来自用户偏好。
			{Name: names.NodeAffinity, Weight: 2},
			{Name: names.NodeResourcesFit, Weight: 1},
			{Name: names.VolumeBinding, Weight: 1},
			// 权重翻倍：来自用户偏好，并使其信号强度与 NodeResourcesLeastAllocated 相当。
			{Name: names.PodTopologySpread, Weight: 2},
			// 权重翻倍：该分数来自用户偏好。
			{Name: names.InterPodAffinity, Weight: 2},
			// 权重翻倍：该分数来自用户偏好。
			{Name: names.DynamicResources, Weight: 2},
			{Name: names.NodeResourcesBalancedAllocation, Weight: 1},
			{Name: names.ImageLocality, Weight: 1},
		},
	},
	Reserve: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.VolumeBinding},
			{Name: names.DynamicResources},
		},
	},
	PreBind: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.VolumeBinding},
			{Name: names.DynamicResources},
		},
	},
	Bind: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.DefaultBinder},
		},
	},
	PlacementScore: config.PluginSet{
		Enabled: []config.Plugin{
			{Name: names.NodeResourcesFit, Weight: 1},
		},
	},
}

// PluginConfigsV1 是 v1 版本的默认插件参数配置。
var PluginConfigsV1 = []config.PluginConfig{
	{
		Name: "DefaultPreemption",
		Args: &config.DefaultPreemptionArgs{
			MinCandidateNodesPercentage: 10,
			MinCandidateNodesAbsolute:   100,
		},
	},
	{
		Name: "DynamicResources",
		Args: &config.DynamicResourcesArgs{
			FilterTimeout:  &metav1.Duration{Duration: 10 * time.Second},
			BindingTimeout: &metav1.Duration{Duration: 10 * time.Minute},
		},
	},
	{
		Name: "InterPodAffinity",
		Args: &config.InterPodAffinityArgs{
			HardPodAffinityWeight: 1,
		},
	},
	{
		Name: "NodeAffinity",
		Args: &config.NodeAffinityArgs{},
	},
	{
		Name: "NodeResourcesBalancedAllocation",
		Args: &config.NodeResourcesBalancedAllocationArgs{
			Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
		},
	},
	{
		Name: "NodeResourcesFit",
		Args: &config.NodeResourcesFitArgs{
			ScoringStrategy: &config.ScoringStrategy{
				Type:      config.LeastAllocated,
				Resources: []config.ResourceSpec{{Name: "cpu", Weight: 1}, {Name: "memory", Weight: 1}},
			},
		},
	},
	{
		Name: "PodTopologySpread",
		Args: &config.PodTopologySpreadArgs{
			DefaultingType: config.SystemDefaulting,
		},
	},
	{
		Name: "VolumeBinding",
		Args: &config.VolumeBindingArgs{
			BindTimeoutSeconds: 600,
			Shape: []config.UtilizationShapePoint{
				{Utilization: 0, Score: 10},
				{Utilization: 100, Score: 0},
			},
		},
	},
}
