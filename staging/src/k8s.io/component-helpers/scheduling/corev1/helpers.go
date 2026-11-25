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

package corev1

import (
	"encoding/json"

	v1 "k8s.io/api/core/v1"
	"k8s.io/component-helpers/scheduling/corev1/nodeaffinity"
	"k8s.io/klog/v2"
)

// PodPriority 返回给定 Pod 的优先级。
//
// pod 是待查询的 Pod。当 pod.Spec.Priority 非空时直接解引用返回其指向的值；
// 老版本创建的 Pod 可能没有 priority 字段（为 nil），此时统一当作 0 处理，
// 让调用方不必到处写 nil 判断。0 是这里的静态兜底值，不读取集群全局默认优先级类。
func PodPriority(pod *v1.Pod) int32 {
	if pod.Spec.Priority != nil {
		return *pod.Spec.Priority
	}
	// 运行中的 Pod 若 priority 为 nil，说明它创建于尚无全局默认优先级类、
	// 且该 Pod 的优先级类名为空的年代，因此这里回退到静态默认优先级。
	return 0
}

// MatchNodeSelectorTerms 判断节点的标签与字段是否匹配节点选择器
// （多个 NodeSelectorTerm 之间是“或”的关系，任一 term 命中即返回匹配）。
//
// node 是待匹配的节点，nodeSelector 是要匹配的节点选择器；
// 返回值 bool 表示是否匹配，error 返回匹配过程中（如选择器非法）出现的错误。
// nil 或空的 term 不匹配任何对象。
//
// 该函数是对 nodeaffinity 惰性求值选择器的薄包装，让调用方只需传入 Node 对象，
// 不必自己构造选择器上下文；node 为 nil 时直接判定不匹配，省得下游对空对象求值。
func MatchNodeSelectorTerms(
	node *v1.Node,
	nodeSelector *v1.NodeSelector,
) (bool, error) {
	if node == nil {
		return false, nil
	}
	return nodeaffinity.NewLazyErrorNodeSelector(nodeSelector).Match(node)
}

// GetAvoidPodsFromNodeAnnotations 扫描节点的注解，解析出该节点上调度时应尽量避免的 Pod 列表。
//
// annotations 是节点的注解键值对，函数只读取 v1.PreferAvoidPodsAnnotationKey 对应的值；
// 返回值 v1.AvoidPods 描述需要避让的 Pod 特征，error 仅在注解内容确实无法解析（JSON 反序列化失败）时返回。
//
// 注解缺失或为空时返回零值而非报错：旧集群或未开启该特性的节点本来就不会写这个注解，
// 当成错误会让调度路径无谓失败；只有注解内容确实不合法才值得抛错。
func GetAvoidPodsFromNodeAnnotations(annotations map[string]string) (v1.AvoidPods, error) {
	var avoidPods v1.AvoidPods
	if len(annotations) > 0 && annotations[v1.PreferAvoidPodsAnnotationKey] != "" {
		err := json.Unmarshal([]byte(annotations[v1.PreferAvoidPodsAnnotationKey]), &avoidPods)
		if err != nil {
			return avoidPods, err
		}
	}
	return avoidPods, nil
}

// TolerationsTolerateTaint 判断给定的污点（taint）是否被容忍列表中的任意一项所容忍。
//
// logger 用于日志输出；tolerations 是 Pod 的容忍列表；taint 是待判定的污点；
// enableComparisonOperators 控制是否启用 Gt/Lt 等数值比较操作符，由调用方按当前版本能力传入。
// 只要有一项容忍匹配即返回 true，全部不匹配才返回 false。
//
// 容忍判定委托给 v1.Toleration 的方法，是为了让 operator 比较等语义与 API 层保持一致。
func TolerationsTolerateTaint(logger klog.Logger, tolerations []v1.Toleration, taint *v1.Taint, enableComparisonOperators bool) bool {
	for i := range tolerations {
		if tolerations[i].ToleratesTaint(logger, taint, enableComparisonOperators) {
			return true
		}
	}
	return false
}

// taintsFilterFunc 是判断一条污点是否纳入考察的过滤函数：
// 入参是一条污点，返回 true 表示保留（纳入后续容忍判定），false 表示排除。
//
// 该类型不导出，说明它只是本包内部拆出来的中间步骤，
// 调用方通过 FindMatchingUntoleratedTaint 的参数传入 predicate 即可。
type taintsFilterFunc func(*v1.Taint) bool

// FindMatchingUntoleratedTaint 检查给定的容忍列表能否容忍过滤后的所有污点，
// 并返回第一条没有对应容忍项的污点。
//
// logger 用于日志输出；taints 是节点上的污点列表；tolerations 是 Pod 的容忍列表；
// inclusionFilter 用于筛选需要考察的污点，为 nil 时表示不过滤；
// enableComparisonOperators 控制是否启用数值比较操作符。
// 返回值中，v1.Taint 是找到的第一条未被容忍的污点（全部被容忍时为零值），
// bool 为 true 表示存在未被容忍的污点，false 表示全部已被容忍。
//
// 先过滤再找第一个未容忍项，是因为调用方通常只关心“Node 是否可用”及原因，
// 无须遍历全部污点；用函数参数注入过滤条件，让同一逻辑服务不同调度阶段。
func FindMatchingUntoleratedTaint(logger klog.Logger, taints []v1.Taint, tolerations []v1.Toleration, inclusionFilter taintsFilterFunc, enableComparisonOperators bool) (v1.Taint, bool) {
	filteredTaints := getFilteredTaints(taints, inclusionFilter)
	for _, taint := range filteredTaints {
		if !TolerationsTolerateTaint(logger, tolerations, &taint, enableComparisonOperators) {
			return taint, true
		}
	}
	return v1.Taint{}, false
}

// getFilteredTaints 返回满足过滤条件的污点列表。
//
// taints 是原始污点列表；inclusionFilter 为过滤谓词，为 nil 时表示不过滤、直接返回原切片。
// 没有过滤器时直接返回原切片，省去一次分配；这是常见调用路径，值得单独走快速分支。
func getFilteredTaints(taints []v1.Taint, inclusionFilter taintsFilterFunc) []v1.Taint {
	if inclusionFilter == nil {
		return taints
	}
	filteredTaints := []v1.Taint{}
	for _, taint := range taints {
		if !inclusionFilter(&taint) {
			continue
		}
		filteredTaints = append(filteredTaints, taint)
	}
	return filteredTaints
}
