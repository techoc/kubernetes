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
	// ErrNotFound 表示数据不存在。
	//
	// 单例 error 让调用方可以用 errors.Is 直接比较，不必构造带相同文本的新 error；
	// 文本本身只是给人看的。
	ErrNotFound = errors.New("not found")
)

// 强制实现 Clone 而不是让各插件自行决定何时复制，是因为 PreFilter 的增量 AddPod/RemovePod
// 会在克隆出的 state 上反复试探同一份预计算结果：深拷贝太贵，不拷贝又会在试探间串改数据，
// 所以接口约定「该浅的浅、该独立的独立」，把取舍交给最懂数据的插件。
// StateData 是 CycleState 中任意数据的泛型类型。
type StateData interface {
	// Clone 用于复制 StateData。出于性能考虑，
	// 对不受 PreFilter 的 AddPod/RemovePod 影响的成员（如 slice、map）
	// 应做浅拷贝。
	Clone() StateData
}

// StateKey 是 CycleState 中键的类型。
type StateKey string

// CycleState 让插件在调度周期内存取任意数据。
// 一个插件存入的 StateData 可被另一插件读取、修改或删除。
//
// 不做锁保护是刻意的性能取舍：调度周期内 state 基本是单线程顺序访问，
// 加互斥只会让每个 Read/Write 都付出无谓代价；信任插件是这套设计的前提。
// CycleState 不做数据保护，默认所有插件协同工作、
// 互信的。
type CycleState interface {
	// ShouldRecordPluginMetrics 判断是否应记录 PluginExecutionDuration 指标；
	// 才记录该指标。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	ShouldRecordPluginMetrics() bool
	// GetSkipFilterPlugins 返回 Filter 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	GetSkipFilterPlugins() sets.Set[string]
	// SetSkipFilterPlugins 设置 Filter 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	SetSkipFilterPlugins(plugins sets.Set[string])
	// GetSkipScorePlugins 返回 Score 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	GetSkipScorePlugins() sets.Set[string]
	// SetSkipScorePlugins 设置 Score 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	SetSkipScorePlugins(plugins sets.Set[string])
	// GetSkipPreBindPlugins 返回 PreBind 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	GetSkipPreBindPlugins() sets.Set[string]
	// SetSkipPreBindPlugins 设置 PreBind 扩展点中要跳过的插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	SetSkipPreBindPlugins(plugins sets.Set[string])
	// GetParallelPreBindPlugins 返回可与其它插件并行执行的插件；
	// in the PreBind extension point.
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	GetParallelPreBindPlugins() sets.Set[string]
	// GetParallelPreBindPlugins 返回可与其它插件并行执行的插件；
	// in the PreBind extension point.
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	SetParallelPreBindPlugins(plugins sets.Set[string])
	// ShouldSkipAllPostFilterPlugins 判断 PostFilter 扩展点是否应跳过全部插件。
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	ShouldSkipAllPostFilterPlugins() bool

	// Read retrieves data with the given "key" from CycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// 并发约定见 CycleState。
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in CycleState with the given "key".
	//
	// 并发约定见 CycleState。
	Write(key StateKey, val StateData)
	// Delete 从 CycleState 中删除指定 key 的数据。
	//
	// 并发约定见 CycleState。
	Delete(key StateKey)
	// Clone 复制 CycleState 并返回指针；待克隆的 context 为 nil 时
	// 返回 nil。
	//
	// 克隆整个 state 是为了支撑抢占等场景：在不污染原始状态的前提下，
	// 反复把候选 Pod 放上/移出节点做可行性试探。
	Clone() CycleState
	// IsPodGroupSchedulingCycle 判断当前是否为 PodGroup 调度周期。
	// 为 false 表示该 Pod 要么已走过 PodGroup 周期，
	// 要么不属于任何 PodGroup。
	// 只有启用 GenericWorkload 特性开关时才可能为 true。
	IsPodGroupSchedulingCycle() bool
	// GetPodGroupSchedulingCycle 获取某 Pod 所属 PodGroup 的周期状态。
	// 仅限启用 GenericWorkload 时使用。
	GetPodGroupSchedulingCycle() PodGroupCycleState
	// GetPlacementCycleState 获取某 Pod 当前 Placement 的周期状态。
	// Pod 不在 Placement 上下文中调度时返回 nil。
	// 仅限启用 GenericWorkload 时使用。
	GetPlacementCycleState() PlacementCycleState
}

// PlacementCycleState 让插件存取单次 Placement 粒度的数据，
// 作用于 PodGroup 调度周期内的一个候选项。
// 其中的数据在同一轮 Placement 内所有 Pod 间共享，
// 并传给该 Placement 的 ScorePlacement。
//
// 与 CycleState 同理，这里也不加锁：placement 维度的数据在同一个调度周期内按候选逐个处理，
// 并发读写并不是常态。它单独存在而非复用 CycleState，是为了让数据的生命周期
// 精确对齐「同一个 placement 的所有 Pod」——跨 placement 自动隔离，省去插件自己带 placement 前缀。
// PlacementCycleState 同样不做数据保护。
// trusted.
type PlacementCycleState interface {
	// ShouldRecordPluginMetrics 判断是否应记录 PluginExecutionDuration 指标；
	// should be recorded.
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	ShouldRecordPluginMetrics() bool
	// Read retrieves data with the given "key" from PlacementCycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// 并发约定见 PlacementCycleState。
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in PlacementCycleState with the given "key".
	//
	// 并发约定见 PlacementCycleState。
	Write(key StateKey, val StateData)
	// Delete 从 PlacementCycleState 中删除指定 key 的数据。
	//
	// 并发约定见 PlacementCycleState。
	Delete(key StateKey)
	// GetPodGroupSchedulingCycle 获取该 Placement 所属 PodGroup 的周期状态。
	// 仅限启用 GenericWorkload 时使用。
	GetPodGroupSchedulingCycle() PodGroupCycleState
}

// PodGroupCycleState 让处理 PodGroup 的插件存取任意数据。
// 一个插件存入的状态可被其它处理 PodGroup 的插件读取、修改或删除。
//
// pod group 状态同样不做保护：只有处理同一组的插件会访问它。
// 它比 PlacementCycleState 再高一层的意义在于让组级插件共享一份跨 placement 的结论，
// 而不用塞进每个 Pod 各自的 CycleState 重复计算。
// PodGroupCycleState 不做数据保护。
// trusted.
type PodGroupCycleState interface {
	// ShouldRecordPluginMetrics 判断是否应记录 PluginExecutionDuration 指标；
	// should be recorded.
	// 该函数主要供调度框架运行时使用，插件通常无需调用。
	ShouldRecordPluginMetrics() bool
	// Read retrieves data with the given "key" from PodGroupCycleState. If the key is not
	// present, ErrNotFound is returned.
	//
	// 并发约定见 PodGroupCycleState。
	Read(key StateKey) (StateData, error)
	// Write stores the given "val" in PodGroupCycleState with the given "key".
	//
	// 并发约定见 PodGroupCycleState。
	Write(key StateKey, val StateData)
	// Delete 从 PodGroupCycleState 中删除指定 key 的数据。
	//
	// 并发约定见 PodGroupCycleState。
	Delete(key StateKey)

	// GetParentPlacementCycleState 返回父级组合 PodGroup 的 PlacementCycleState。
	// If there is no parent, returns nil.
	//
	// 复合 pod group 的组可能是嵌套的，子组做决策时需要借用父 placement 的上下文；
	// 没有父组就返回 nil，调用方应当先判空再用。
	GetParentPlacementCycleState() PlacementCycleState
}
