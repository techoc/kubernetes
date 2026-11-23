/*
Copyright 2025 The Kubernetes Authors.

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
	"errors"

	"k8s.io/apimachinery/pkg/util/sets"
)

var (
	// ErrNotFound is the not found error message.
	//
	// 单例 error 让调用方可以用 errors.Is 直接比较，不必构造带相同文本的新 error；
	// 文本本身只是给人看的。
	ErrNotFound = errors.New("not found")
)

// 强制实现 Clone 而不是让各插件自行决定何时复制，是因为 PreFilter 的增量 AddPod/RemovePod
// 会在克隆出的 state 上反复试探同一份预计算结果：深拷贝太贵，不拷贝又会在试探间串改数据，
// 所以接口约定「该浅的浅、该独立的独立」，把取舍交给最懂数据的插件。
// StateData is a generic type for arbitrary data stored in CycleState.
type StateData interface {
	// Clone is an interface to make a copy of StateData. For performance reasons,
	// clone should make shallow copies for members (e.g., slices or maps) that are not
	// impacted by PreFilter's optional AddPod/RemovePod methods.
	Clone() StateData
}

// StateKey is the type of keys stored in CycleState.
type StateKey string

// CycleState provides a mechanism for plugins to store and retrieve arbitrary data.
// StateData stored by one plugin can be read, altered, or deleted by another plugin.
//
// 不做锁保护是刻意的性能取舍：调度周期内 state 基本是单线程顺序访问，
// 加互斥只会让每个 Read/Write 都付出无谓代价；信任插件是这套设计的前提。
// CycleState does not provide any data protection, as all plugins are assumed to be
// trusted.
type CycleState interface {
	// ShouldRecordPluginMetrics returns whether metrics.PluginExecutionDuration metrics
	// should be recorded.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	ShouldRecordPluginMetrics() bool
	// GetSkipFilterPlugins returns plugins that will be skipped in the Filter extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	GetSkipFilterPlugins() sets.Set[string]
	// SetSkipFilterPlugins sets plugins that should be skipped in the Filter extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	SetSkipFilterPlugins(plugins sets.Set[string])
	// GetSkipScorePlugins returns plugins that will be skipped in the Score extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	GetSkipScorePlugins() sets.Set[string]
	// SetSkipScorePlugins sets plugins that should be skipped in the Score extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	SetSkipScorePlugins(plugins sets.Set[string])
	// GetSkipPreBindPlugins returns plugins that will be skipped in the PreBind extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	GetSkipPreBindPlugins() sets.Set[string]
	// SetSkipPreBindPlugins sets plugins that should be skipped in the PerBind extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	SetSkipPreBindPlugins(plugins sets.Set[string])
	// GetParallelPreBindPlugins returns plugins that can be run in parallel with other plugins
	// in the PreBind extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	GetParallelPreBindPlugins() sets.Set[string]
	// GetParallelPreBindPlugins returns plugins that can be run in parallel with other plugins
	// in the PreBind extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	SetParallelPreBindPlugins(plugins sets.Set[string])
	// ShouldSkipAllPostFilterPlugins returns whether all plugins should be skipped in the PostFilter extension point.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	ShouldSkipAllPostFilterPlugins() bool

	// Read retrieves data with the given "key" from CycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// See CycleState for notes on concurrency.
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in CycleState with the given "key".
	//
	// See CycleState for notes on concurrency.
	Write(key StateKey, val StateData)
	// Delete deletes data with the given key from CycleState.
	//
	// See CycleState for notes on concurrency.
	Delete(key StateKey)
	// Clone creates a copy of CycleState and returns its pointer. Clone returns
	// nil if the context being cloned is nil.
	//
	// 克隆整个 state 是为了支撑抢占等场景：在不污染原始状态的前提下，
	// 反复把候选 Pod 放上/移出节点做可行性试探。
	Clone() CycleState
	// IsPodGroupSchedulingCycle returns true if this cycle is a pod group scheduling cycle.
	// If set to false, it means that the pod referencing this CycleState either passed the pod group cycle
	// or doesn't belong to any pod group.
	// This field can only be true when GenericWorkload feature flag is enabled.
	IsPodGroupSchedulingCycle() bool
	// GetPodGroupSchedulingCycle gets the cycle state of the PodGroup for a Pod.
	// This should be only used when GenericWorkload feature flag is enabled.
	GetPodGroupSchedulingCycle() PodGroupCycleState
	// GetPlacementCycleState gets the cycle state of the current Placement for a Pod.
	// Returns nil if this pod is not being scheduled within a placement context.
	// This should be only used when GenericWorkload feature flag is enabled.
	GetPlacementCycleState() PlacementCycleState
}

// PlacementCycleState provides a mechanism for plugins to store and retrieve arbitrary data
// scoped to a single placement candidate within a pod group scheduling cycle.
// Data stored in PlacementCycleState is shared across all pods scheduled within the same
// placement iteration and is passed to ScorePlacement for that placement.
//
// 与 CycleState 同理，这里也不加锁：placement 维度的数据在同一个调度周期内按候选逐个处理，
// 并发读写并不是常态。它单独存在而非复用 CycleState，是为了让数据的生命周期
// 精确对齐「同一个 placement 的所有 Pod」——跨 placement 自动隔离，省去插件自己带 placement 前缀。
// PlacementCycleState does not provide any data protection, as all plugins are assumed to be
// trusted.
type PlacementCycleState interface {
	// ShouldRecordPluginMetrics returns whether metrics.PluginExecutionDuration metrics
	// should be recorded.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	ShouldRecordPluginMetrics() bool
	// Read retrieves data with the given "key" from PlacementCycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// See PlacementCycleState for notes on concurrency.
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in PlacementCycleState with the given "key".
	//
	// See PlacementCycleState for notes on concurrency.
	Write(key StateKey, val StateData)
	// Delete deletes data with the given key from PlacementCycleState.
	//
	// See PlacementCycleState for notes on concurrency.
	Delete(key StateKey)
	// GetPodGroupSchedulingCycle gets the cycle state of the PodGroup for this Placement.
	// This should be only used when GenericWorkload feature flag is enabled.
	GetPodGroupSchedulingCycle() PodGroupCycleState
}

// PodGroupCycleState provides a mechanism for plugins that operate on pod groups to store and retrieve arbitrary data.
// StateData stored by one plugin can be read, altered, or deleted by another plugin that operates on a pod group.
//
// pod group 状态同样不做保护：只有处理同一组的插件会访问它。
// 它比 PlacementCycleState 再高一层的意义在于让组级插件共享一份跨 placement 的结论，
// 而不用塞进每个 Pod 各自的 CycleState 重复计算。
// PodGroupCycleState does not provide any data protection, as all plugins are assumed to be
// trusted.
type PodGroupCycleState interface {
	// ShouldRecordPluginMetrics returns whether metrics.PluginExecutionDuration metrics
	// should be recorded.
	// This function is mostly for the scheduling framework runtime, plugins usually don't have to use it.
	ShouldRecordPluginMetrics() bool
	// Read retrieves data with the given "key" from PodGroupCycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// See PodGroupCycleState for notes on concurrency.
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in PodGroupCycleState with the given "key".
	//
	// See PodGroupCycleState for notes on concurrency.
	Write(key StateKey, val StateData)
	// Delete deletes data with the given key from PodGroupCycleState.
	//
	// See PodGroupCycleState for notes on concurrency.
	Delete(key StateKey)

	// GetParentPlacementCycleState returns PlacementCycleState of the parent composite pod group.
	// If there is no parent, returns nil.
	//
	// 复合 pod group 的组可能是嵌套的，子组做决策时需要借用父 placement 的上下文；
	// 没有父组就返回 nil，调用方应当先判空再用。
	GetParentPlacementCycleState() PlacementCycleState
}
