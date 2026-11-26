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

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// 分值边界供外部实现对齐：超出范围的分数会被调度器夹住，
// 约定清楚边界可以避免 extender 误以为打分越高就一定能压过内置插件。
const (
	// MinExtenderPriority 是 extender 分数的最小合法值。
	MinExtenderPriority int64 = 0

	// MaxExtenderPriority 是 extender 分数的最大合法值。
	MaxExtenderPriority int64 = 10
)

// ExtenderPreemptionResult 是 extender 抢占阶段的返回结果。
type ExtenderPreemptionResult struct {
	NodeNameToMetaVictims map[string]*MetaVictims
}

// ExtenderPreemptionArgs 是 extender 抢占节点上 Pod 所需的参数。
// 抢占参数要同时支持“全量节点对象”和“只有节点名”两种形态，
// 由 NodeCacheCapable 决定填哪一份，避免 extender 已缓存节点信息时还要重复传输。
type ExtenderPreemptionArgs struct {
	// 正在调度的 Pod。
	Pod *v1.Pod
	// 调度器抢占阶段生成的 Victims 映射；
	// 只有 Extender.NodeCacheCapable 为 true 时才填 NodeNameToMetaVictims，否则只填 NodeNameToVictims。
	NodeNameToVictims     map[string]*Victims
	NodeNameToMetaVictims map[string]*MetaVictims
}

// Victims 表示：
//
// pods: 一组待抢占的 Pod。
// numPDBViolations: 违反 PodDisruptionBudget 的次数。
//
// PDB 违规数单独记一份，是因为调度器要在“抢到足够资源”和“少破坏可用性”之间权衡，
// 光看被抢的 Pod 列表算不出这个代价。
type Victims struct {
	Pods             []*v1.Pod
	NumPDBViolations int64
}

// MetaPod 只保存 v1.Pod 的身份信息。
//
// 只传 UID 而不传整个 Pod：extender 声明自己缓存了集群状态，就该按 UID 自己查，
// 省下的序列化开销在大规模抢占时很可观。
type MetaPod struct {
	UID string
}

// MetaVictims 表示：
//
// pods: 一组待抢占的 Pod。
// 只发送 Pod 标识，由使用方自行取回完整的 v1.Pod。
// numPDBViolations: 违反 PodDisruptionBudget 的次数。
type MetaVictims struct {
	Pods             []*MetaPod
	NumPDBViolations int64
}

// ExtenderArgs 是 extender 执行过滤/打分所需的参数。
// 的节点。
//
// 过滤和打分共用同一套参数：两个阶段关心的是同一个 Pod 与候选节点集合，
// 分开定义只会让 extender 多写一遍解析逻辑。
type ExtenderArgs struct {
	// Pod being scheduled
	Pod *v1.Pod
	// 可调度该 Pod 的候选节点列表；
	// only if Extender.NodeCacheCapable == false
	Nodes *v1.NodeList
	// 可调度该 Pod 的候选节点名列表；
	// 仅在 Extender.NodeCacheCapable 为 true 时填写。
	NodeNames *[]string
}

// FailedNodesMap 是被过滤掉的节点及其失败原因。
//
// 用 map 而不是列表：同一批节点可能既被过滤又被判为不可解，
// 调度器还要按节点名快速查原因，map 正好满足这两个用途。
type FailedNodesMap map[string]string

// ExtenderFilterResult 是 extender 过滤调用的返回结果。
//
// 失败节点拆成“可抢占挽回”和“不可解”两类：前者还能走抢占流程，
// 后者直接放弃，免得对注定放不下的节点白做一轮抢占。
type ExtenderFilterResult struct {
	// 过滤后可调度的节点集合；
	// only if Extender.NodeCacheCapable == false
	Nodes *v1.NodeList
	// 过滤后可调度的节点集合；
	// only if Extender.NodeCacheCapable == true
	NodeNames *[]string
	// 被过滤掉的节点及其失败原因；
	FailedNodes FailedNodesMap
	// 被过滤掉且抢占也无法改变结果的节点，
	// 其值与 FailedNodes 中的失败原因相同。
	// Nodes specified here takes precedence over FailedNodes.
	FailedAndUnresolvableNodes FailedNodesMap
	// Error message indicating failure
	Error string
}

// ExtenderBindingArgs 是让 extender 把 Pod 绑定到节点时传入的参数。
//
// 这里带的是 PodUID 而不是 Pod 对象：绑定时刻的权威状态在 API server，
// extender 凭名字加 UID 去操作，能挡住旧 Pod 重名导致的误绑定。
type ExtenderBindingArgs struct {
	// PodName 是被绑定 Pod 的名字；
	PodName string
	// PodNamespace 是被绑定 Pod 的命名空间；
	PodNamespace string
	// PodUID 是被绑定 Pod 的 UID。
	PodUID types.UID
	// Node selected by the scheduler
	Node string
}

// ExtenderBindingResult 是 extender 返回的 Pod 绑定结果。
type ExtenderBindingResult struct {
	// Error message indicating failure
	Error string
}

// HostPriority 表示调度到某节点的优先级，数值越大越优。
//
// 分数与权重分开传：调度器先把 extender 分数乘上权重再与内置插件得分相加，
// 这样管理员调权重就能改变 extender 在总排名里的话语权，而不用改外部实现。
type HostPriority struct {
	// Name of the host
	Host string
	// 该节点对应的分数。
	Score int64
}

// HostPriorityList declares a []HostPriority type.
type HostPriorityList []HostPriority
