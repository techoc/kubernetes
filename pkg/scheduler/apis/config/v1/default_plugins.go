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

package v1

import (
	"slices"

	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/klog/v2"
	v1 "k8s.io/kube-scheduler/config/v1"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	"k8s.io/utils/ptr"
)

// getDefaultPlugins 返回默认的插件集合。
// 权重较低的插件在 Score 阶段影响较小，顺序本身不决定执行优先级，框架按扩展点归类执行。
func getDefaultPlugins() *v1.Plugins {
	plugins := &v1.Plugins{
		MultiPoint: v1.PluginSet{
			Enabled: []v1.Plugin{
				{Name: names.SchedulingGates},
				{Name: names.PrioritySort},
				{Name: names.NodeName},
				{Name: names.NodeUnschedulable},
				{Name: names.TaintToleration, Weight: ptr.To[int32](3)},
				{Name: names.NodeAffinity, Weight: ptr.To[int32](2)},
				{Name: names.NodePorts},
				{Name: names.NodeResourcesFit, Weight: ptr.To[int32](1)},
				{Name: names.VolumeRestrictions},
				{Name: names.NodeVolumeLimits},
				{Name: names.VolumeBinding},
				{Name: names.VolumeZone},
				{Name: names.PodTopologySpread, Weight: ptr.To[int32](2)},
				{Name: names.InterPodAffinity, Weight: ptr.To[int32](2)},
				{Name: names.DefaultPreemption},
				{Name: names.NodeResourcesBalancedAllocation, Weight: ptr.To[int32](1)},
				{Name: names.ImageLocality, Weight: ptr.To[int32](1)},
				{Name: names.DefaultBinder},
			},
		},
	}
	// 根据 feature gate 叠加实验性插件，默认列表本身保持与开关无关。
	applyFeatureGates(plugins)

	return plugins
}

// applyFeatureGates 根据启用的 feature gate 向配置中追加或插入对应的插件。
func applyFeatureGates(config *v1.Plugins) {
	if utilfeature.DefaultFeatureGate.Enabled(features.NodeDeclaredFeatures) {
		config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.NodeDeclaredFeatures})
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.InPlacePodVerticalScalingSchedulerPreemption) {
		applyDeferredPodScheduling(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.DynamicResourceAllocation) {
		applyDynamicResources(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.GenericWorkload) {
		applyGangScheduling(config)
	}
	if utilfeature.DefaultFeatureGate.Enabled(features.TopologyAwareWorkloadScheduling) {
		applyTopologyAwareWorkloadScheduling(config)
	}
}

// applyDynamicResources 将 DynamicResources 插件插入到默认插件列表中。
func applyDynamicResources(config *v1.Plugins) {
	// 该插件必须排在 DefaultPreemption 之前：当 Pod 出问题而触发 PostFilter 时，
	// 先释放空闲的 ResourceClaim 比驱逐一个可能仍在干活的 Pod 代价更小。
	for i := range config.MultiPoint.Enabled {
		if config.MultiPoint.Enabled[i].Name == names.DefaultPreemption {
			extended := make([]v1.Plugin, 0, len(config.MultiPoint.Enabled)+1)
			extended = append(extended, config.MultiPoint.Enabled[:i]...)
			extended = append(extended, v1.Plugin{Name: names.DynamicResources, Weight: ptr.To[int32](2)})
			extended = append(extended, config.MultiPoint.Enabled[i:]...)
			config.MultiPoint.Enabled = extended
			break
		}
	}
}

// applyGangScheduling 追加 GangScheduling 插件以支持 gang 调度。
func applyGangScheduling(config *v1.Plugins) {
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.GangScheduling})
}

// applyTopologyAwareWorkloadScheduling 追加拓扑感知调度所需的插件。
func applyTopologyAwareWorkloadScheduling(config *v1.Plugins) {
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.TopologyPlacementGenerator})
	config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.PodGroupPodsCount, Weight: ptr.To[int32](1)})
}

// mergePlugins 将自定义插件集合合并到给定的默认集合中，并处理禁用集合。
func mergePlugins(logger klog.Logger, defaultPlugins, customPlugins *v1.Plugins) *v1.Plugins {
	if customPlugins == nil {
		return defaultPlugins
	}

	defaultPlugins.MultiPoint = mergePluginSet(logger, defaultPlugins.MultiPoint, customPlugins.MultiPoint)
	defaultPlugins.PreEnqueue = mergePluginSet(logger, defaultPlugins.PreEnqueue, customPlugins.PreEnqueue)
	defaultPlugins.QueueSort = mergePluginSet(logger, defaultPlugins.QueueSort, customPlugins.QueueSort)
	defaultPlugins.PreFilter = mergePluginSet(logger, defaultPlugins.PreFilter, customPlugins.PreFilter)
	defaultPlugins.Filter = mergePluginSet(logger, defaultPlugins.Filter, customPlugins.Filter)
	defaultPlugins.PostFilter = mergePluginSet(logger, defaultPlugins.PostFilter, customPlugins.PostFilter)
	defaultPlugins.PreScore = mergePluginSet(logger, defaultPlugins.PreScore, customPlugins.PreScore)
	defaultPlugins.Score = mergePluginSet(logger, defaultPlugins.Score, customPlugins.Score)
	defaultPlugins.Reserve = mergePluginSet(logger, defaultPlugins.Reserve, customPlugins.Reserve)
	defaultPlugins.Permit = mergePluginSet(logger, defaultPlugins.Permit, customPlugins.Permit)
	defaultPlugins.PreBind = mergePluginSet(logger, defaultPlugins.PreBind, customPlugins.PreBind)
	defaultPlugins.Bind = mergePluginSet(logger, defaultPlugins.Bind, customPlugins.Bind)
	defaultPlugins.PostBind = mergePluginSet(logger, defaultPlugins.PostBind, customPlugins.PostBind)
	defaultPlugins.PlacementGenerate = mergePluginSet(logger, defaultPlugins.PlacementGenerate, customPlugins.PlacementGenerate)
	defaultPlugins.PlacementScore = mergePluginSet(logger, defaultPlugins.PlacementScore, customPlugins.PlacementScore)
	defaultPlugins.PodGroupPostFilter = mergePluginSet(logger, defaultPlugins.PodGroupPostFilter, customPlugins.PodGroupPostFilter)
	return defaultPlugins
}

// pluginIndex 记录插件在自定义列表中的位置及其定义，用于判断它是否替换了默认插件。
type pluginIndex struct {
	index  int
	plugin v1.Plugin
}

// mergePluginSet 合并单个扩展点的默认插件集合与自定义插件集合。
func mergePluginSet(logger klog.Logger, defaultPluginSet, customPluginSet v1.PluginSet) v1.PluginSet {
	disabledPlugins := sets.New[string]()
	enabledCustomPlugins := make(map[string]pluginIndex)
	// replacedPluginIndex 记录已替换默认插件的自定义插件在列表中的下标。
	replacedPluginIndex := sets.New[int]()
	var disabled []v1.Plugin
	for _, disabledPlugin := range customPluginSet.Disabled {
		// 用户为某个扩展点手动禁用（或用 "*" 禁用全部）默认插件时，必须记录下来，
		// 这样 framework 中的 MultiPoint 逻辑才知道不要把未显式指定的默认插件补回该扩展点。
		disabled = append(disabled, v1.Plugin{Name: disabledPlugin.Name})
		disabledPlugins.Insert(disabledPlugin.Name)
	}

	// 引入 MultiPoint 后，默认配置中也可能存在被禁用的插件。
	// 例如通过 MultiPoint 同时启用了插件 X 的 Filter 与 Score，但默认禁用了它的 Score。
	for _, disabledPlugin := range defaultPluginSet.Disabled {
		disabled = append(disabled, v1.Plugin{Name: disabledPlugin.Name})
		disabledPlugins.Insert(disabledPlugin.Name)
	}

	for index, enabledPlugin := range customPluginSet.Enabled {
		enabledCustomPlugins[enabledPlugin.Name] = pluginIndex{index, enabledPlugin}
	}
	var enabledPlugins []v1.Plugin
	if !disabledPlugins.Has("*") {
		for _, defaultEnabledPlugin := range defaultPluginSet.Enabled {
			if disabledPlugins.Has(defaultEnabledPlugin.Name) {
				continue
			}
			// 默认插件被显式重新配置时，用自定义配置覆盖它。
			if customPlugin, ok := enabledCustomPlugins[defaultEnabledPlugin.Name]; ok {
				logger.Info("Default plugin is explicitly re-configured; overriding", "plugin", defaultEnabledPlugin.Name)
				// 原位替换以保留默认插件的相对顺序。
				defaultEnabledPlugin = customPlugin.plugin
				replacedPluginIndex.Insert(customPlugin.index)
			}
			enabledPlugins = append(enabledPlugins, defaultEnabledPlugin)
		}
	}

	// 追加所有没有替换默认插件的自定义插件。
	// 注意：重复的自定义插件仍会被追加到这里，
	// 此时 scheduler framework 的实例化会检测到重复并直接终止。
	for index, plugin := range customPluginSet.Enabled {
		if !replacedPluginIndex.Has(index) {
			enabledPlugins = append(enabledPlugins, plugin)
		}
	}
	return v1.PluginSet{Enabled: enabledPlugins, Disabled: disabled}
}

// applyDeferredPodScheduling 插入 DeferredPodScheduling 插件。
// 它必须位于 NodeResourcesFit 之前：这样当某节点禁用了抢占时，Pod 会先在
// DeferredPodScheduling 失败，从而被记录为失败插件，便于其 Queueing Hint 在策略启用后触发重试。
// 同时它必须排在 NodeName 等基础过滤器之后，避免对无关的节点事件做无用的重试。
func applyDeferredPodScheduling(config *v1.Plugins) {
	idx := slices.IndexFunc(config.MultiPoint.Enabled, func(p v1.Plugin) bool {
		return p.Name == names.NodeResourcesFit
	})
	if idx != -1 {
		config.MultiPoint.Enabled = slices.Insert(config.MultiPoint.Enabled, idx, v1.Plugin{Name: names.DeferredPodScheduling})
	} else {
		config.MultiPoint.Enabled = append(config.MultiPoint.Enabled, v1.Plugin{Name: names.DeferredPodScheduling})
	}
}
