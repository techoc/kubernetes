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

package noderesources

import (
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
)

// leastResourceScorer 倾向于给已请求资源更少的节点打分更高。
// 它计算调度到节点上的 Pod 所请求的内存、CPU 及其他资源占容量的百分比，
// 并以"请求量/容量"比例的平均值中的最小值作为打分依据。
//
// 计算细节：
// (cpu((capacity-requested)*MaxNodeScore*cpuWeight/capacity) + memory((capacity-requested)*MaxNodeScore*memoryWeight/capacity) + ...)/weightSum
func leastResourceScorer(resources []config.ResourceSpec) func([]int64, []int64, []int64) int64 {
	return func(requested, _, allocable []int64) int64 {
		var nodeScore, weightSum int64
		for i := range requested {
			// 容量为 0 的资源无法参与比例计算，跳过以免除零。
			if allocable[i] == 0 {
				continue
			}
			weight := resources[i].Weight
			resourceScore := leastRequestedScore(requested[i], allocable[i])
			nodeScore += resourceScore * weight
			weightSum += weight
		}
		// 所有资源容量都为 0 时没有有效信息，保守地给 0 分。
		if weightSum == 0 {
			return 0
		}
		return nodeScore / weightSum
	}
}

// leastRequestedScore 将未使用的容量映射到 0-MaxNodeScore 区间。
// 0 表示优先级最低，`MaxNodeScore` 表示最高。
// 剩余资源越多，得分越高。
func leastRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		return 0
	}

	return ((capacity - requested) * fwk.MaxNodeScore) / capacity
}
