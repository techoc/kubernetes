/*
Copyright 2020 The Kubernetes Authors.

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
	v1 "k8s.io/api/core/v1"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
)

// Extender 是外部进程影响 Kubernetes 调度决策的接口，
// 主要用于处理调度器无法直接感知的资源。
// 由 Kubernetes 管理。
//
// 这套接口是 HTTP extender 的抽象：调度器把远程调用包成本地方法，
// 上层调度流程就不必关心对端是进程内插件还是另一个服务。
// 各 Is* 方法让调度器在每次循环里跳过未实现的回调，省掉无谓的远程请求。
type Extender interface {
	// Name 返回标识该扩展的唯一名字。
	Name() string

	// 按扩展实现的 predicate 过滤节点，返回值应是
	// 传入列表的子集。
	// failedNodes 与 failedAndUnresolvableNodes 可携带失败节点及原因；
	// 后者的节点被视为无法通过其它扩展补救。
	// 不可补救的节点。
	Filter(pod *v1.Pod, nodes []NodeInfo) (filteredNodes []NodeInfo, failedNodesMap extenderv1.FailedNodesMap, failedAndUnresolvable extenderv1.FailedNodesMap, err error)

	// 按扩展实现的优先级函数打分；返回的分数与权重
	// 计算出该扩展的加权分，并与调度器自身打分相加，
	// 最终总分用于选节点。
	Prioritize(pod *v1.Pod, nodes []NodeInfo) (hostPriorities *extenderv1.HostPriorityList, weight int64, err error)

	// Bind 把 Pod 绑定到节点的动作委托给扩展。
	Bind(binding *v1.Binding) error

	// IsBinder 判断该扩展是否配置了 Bind。
	IsBinder() bool

	// Pod 请求的资源中至少有一种由该扩展管理时
	// 返回 true。
	IsInterested(pod *v1.Pod) bool

	// IsPrioritizer 判断该扩展是否配置了 Prioritize。
	IsPrioritizer() bool

	// IsFilter 判断该扩展是否配置了 Filter。
	IsFilter() bool

	// ProcessPreemption 让扩展基于以下信息重新处理节点与受害者：
	// 如下：
	//   1. Pod to schedule
	//   2. 上一轮调度生成的候选节点及受害者（nodeNameToVictims）。
	// 扩展可做的调整包括：
	//   1. 抢占后从给定候选节点中取子集。
	//   2. 为每个候选节点返回不同的受害者集合。
	// 抢占结果先由调度器算出，再交给 extender 复核：外部资源不在调度器视图里，
	// 只有 extender 知道抢掉这些 Pod 是否真的能腾出它管理的资源。
	ProcessPreemption(
		pod *v1.Pod,
		nodeNameToVictims map[string]*extenderv1.Victims,
		nodeInfos NodeInfoLister,
	) (map[string]*extenderv1.Victims, error)

	// SupportsPreemption 判断该扩展是否支持抢占。
	SupportsPreemption() bool

	// IsIgnorable 返回 true 表示扩展不可用时不应让调度失败，
	// 让调度器既能快速失败，也能容忍非关键扩展。
	// Filter 与 Bind 两种动作都支持。
	// 把“可忽略”做成开关，是为了区分关键扩展和非关键扩展：
	// 前者挂了必须中止调度，后者挂了直接跳过，避免一个旁路服务拖垮整个集群的调度。
	IsIgnorable() bool
}
