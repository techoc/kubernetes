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

package framework

import (
	"sync"

	"k8s.io/apimachinery/pkg/util/sets"
	fwk "k8s.io/kube-scheduler/framework"
)

// 注意：CycleState 用 sync.Map 作为底层存储，因为它天然线程安全。这样优化的是"一次写入、多次读取"的场景。
// 内置插件都遵循这一模式：在 PreFilter/PreScore 中写入插件专属状态，随后在 Filter/Score 中多次读取。
type CycleState struct {
	// storage 以 StateKey 为键、StateData 为值。
	storage sync.Map
	// recordPluginMetrics 为 true 时，本次调度周期会记录 metrics.PluginExecutionDuration。
	recordPluginMetrics bool
	// skipFilterPlugins 是在 Filter 扩展点要跳过的插件集合。
	skipFilterPlugins sets.Set[string]
	// skipScorePlugins 是在 Score 扩展点要跳过的插件集合。
	skipScorePlugins sets.Set[string]
	// skipPreBindPlugins 是在 PreBind 扩展点要跳过的插件集合。
	skipPreBindPlugins sets.Set[string]
	// skipAllPostFilterPlugins 表示是否跳过 PostFilter 扩展点的全部插件。
	skipAllPostFilterPlugins bool
	// parallelPreBindPlugins 保存可在 PreBind 扩展点与其他插件并行运行的插件。
	parallelPreBindPlugins sets.Set[string]
	// podGroupCycleState 存放该 Pod 所属 PodGroup 的 CycleState。
	// 若为 nil，表示引用此 CycleState 的 Pod 已通过 PodGroup 调度周期，或根本不属于任何 PodGroup。
	// 只有启用 GenericWorkload 特性开关时，该字段才可能非 nil。
	podGroupCycleState fwk.PodGroupCycleState
	// placementCycleState 存放当前正在评估的 Placement 的 CycleState。
	// 若为 nil，表示该 Pod 不在 placement 上下文中调度。
	// 只有启用 GenericWorkload 特性开关时，该字段才可能非 nil。
	placementCycleState fwk.PlacementCycleState
}

// NewCycleState 初始化一个新的 CycleState 并返回其指针。
func NewCycleState() *CycleState {
	return &CycleState{}
}

// ShouldRecordPluginMetrics 返回是否应记录 metrics.PluginExecutionDuration 指标。
func (c *CycleState) ShouldRecordPluginMetrics() bool {
	if c == nil {
		return false
	}
	return c.recordPluginMetrics
}

// SetRecordPluginMetrics 设置是否记录插件执行指标。
func (c *CycleState) SetRecordPluginMetrics(flag bool) {
	if c == nil {
		return
	}
	c.recordPluginMetrics = flag
}

// SetSkipFilterPlugins 设置 Filter 扩展点要跳过的插件集合。
func (c *CycleState) SetSkipFilterPlugins(plugins sets.Set[string]) {
	c.skipFilterPlugins = plugins
}

// GetSkipFilterPlugins 返回 Filter 扩展点要跳过的插件集合。
func (c *CycleState) GetSkipFilterPlugins() sets.Set[string] {
	return c.skipFilterPlugins
}

// SetSkipScorePlugins 设置 Score 扩展点要跳过的插件集合。
func (c *CycleState) SetSkipScorePlugins(plugins sets.Set[string]) {
	c.skipScorePlugins = plugins
}

// GetSkipScorePlugins 返回 Score 扩展点要跳过的插件集合。
func (c *CycleState) GetSkipScorePlugins() sets.Set[string] {
	return c.skipScorePlugins
}

// SetSkipPreBindPlugins 设置 PreBind 扩展点要跳过的插件集合。
func (c *CycleState) SetSkipPreBindPlugins(plugins sets.Set[string]) {
	c.skipPreBindPlugins = plugins
}

// GetSkipPreBindPlugins 返回 PreBind 扩展点要跳过的插件集合。
func (c *CycleState) GetSkipPreBindPlugins() sets.Set[string] {
	return c.skipPreBindPlugins
}

// SetParallelPreBindPlugins 设置 PreBind 扩展点可并行运行的插件集合。
func (c *CycleState) SetParallelPreBindPlugins(plugins sets.Set[string]) {
	c.parallelPreBindPlugins = plugins
}

// GetParallelPreBindPlugins 返回 PreBind 扩展点可并行运行的插件集合。
func (c *CycleState) GetParallelPreBindPlugins() sets.Set[string] {
	return c.parallelPreBindPlugins
}

// IsPodGroupSchedulingCycle 返回当前是否处于 PodGroup 调度周期中。
func (c *CycleState) IsPodGroupSchedulingCycle() bool {
	return c.podGroupCycleState != nil
}

// SetPodGroupSchedulingCycle 设置 PodGroup 调度周期状态。
func (c *CycleState) SetPodGroupSchedulingCycle(podGroupCycleState fwk.PodGroupCycleState) {
	c.podGroupCycleState = podGroupCycleState
}

// GetPodGroupSchedulingCycle 返回 PodGroup 调度周期状态。
func (c *CycleState) GetPodGroupSchedulingCycle() fwk.PodGroupCycleState {
	return c.podGroupCycleState
}

// GetPlacementCycleState 返回当前 Placement 的调度状态。
func (c *CycleState) GetPlacementCycleState() fwk.PlacementCycleState {
	return c.placementCycleState
}

// GetParentPlacementCycleState 实现 [fwk.PodGroupCycleState.GetParentPlacementCycleState]。
// 可以复用与 GetPlacementCycleState 相同的 placementCycleState 字段，因为二者实现的是不同接口，且互斥。
func (c *CycleState) GetParentPlacementCycleState() fwk.PlacementCycleState {
	return c.placementCycleState
}

// SetPlacementCycleState 设置当前 Placement 的调度状态。
func (c *CycleState) SetPlacementCycleState(placementCycleState fwk.PlacementCycleState) {
	c.placementCycleState = placementCycleState
}

// SetSkipAllPostFilterPlugins 设置是否跳过 PostFilter 扩展点的全部插件。
func (c *CycleState) SetSkipAllPostFilterPlugins(flag bool) {
	c.skipAllPostFilterPlugins = flag
}

// ShouldSkipAllPostFilterPlugins 返回是否应跳过 PostFilter 扩展点的全部插件。
func (c *CycleState) ShouldSkipAllPostFilterPlugins() bool {
	return c.skipAllPostFilterPlugins
}

// Clone 复制 CycleState 并返回其指针。若被克隆的上下文为 nil，Clone 返回 nil。
func (c *CycleState) Clone() fwk.CycleState {
	if c == nil {
		return nil
	}
	copy := NewCycleState()
	// storage 可能被覆盖，因此需要安全复制。
	c.storage.Range(func(k, v interface{}) bool {
		copy.storage.Store(k, v.(fwk.StateData).Clone())
		return true
	})
	// 以下字段不会被原地修改，不必做安全复制。
	copy.recordPluginMetrics = c.recordPluginMetrics
	copy.skipFilterPlugins = c.skipFilterPlugins
	copy.skipScorePlugins = c.skipScorePlugins
	copy.skipPreBindPlugins = c.skipPreBindPlugins
	copy.parallelPreBindPlugins = c.parallelPreBindPlugins
	copy.podGroupCycleState = c.podGroupCycleState
	copy.placementCycleState = c.placementCycleState
	copy.skipAllPostFilterPlugins = c.skipAllPostFilterPlugins

	return copy
}

// Read 从 CycleState 中读取给定 "key" 的数据。若 key 不存在，返回 ErrNotFound。
//
// 并发相关说明见 CycleState。
func (c *CycleState) Read(key fwk.StateKey) (fwk.StateData, error) {
	if v, ok := c.storage.Load(key); ok {
		return v.(fwk.StateData), nil
	}
	return nil, fwk.ErrNotFound
}

// Write 以给定 "key" 把给定 "val" 存入 CycleState。
//
// 并发相关说明见 CycleState。
func (c *CycleState) Write(key fwk.StateKey, val fwk.StateData) {
	c.storage.Store(key, val)
}

// Delete 从 CycleState 中删除给定 key 的数据。
//
// 并发相关说明见 CycleState。
func (c *CycleState) Delete(key fwk.StateKey) {
	c.storage.Delete(key)
}
