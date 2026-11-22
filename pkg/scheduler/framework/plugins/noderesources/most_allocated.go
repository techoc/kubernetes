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

// mostResourceScorer 偏好已请求资源最多的节点。
// 它计算节点上已调度 Pod 请求的 CPU 与内存占比，并按各资源请求量与容量之比
// 的加权平均值排序。
//
// 具体公式：
// (cpu(MaxNodeScore * requested * cpuWeight / capacity) + memory(MaxNodeScore * requested * memoryWeight / capacity) + ...) / weightSum
func mostResourceScorer(resources []config.ResourceSpec) func(requested, _, allocable []int64) int64 {
	return func(requested, _, allocable []int64) int64 {
		var nodeScore, weightSum int64
		for i := range requested {
			// 容量为 0 时占比无意义，直接跳过，避免除零。
			if allocable[i] == 0 {
				continue
			}
			weight := resources[i].Weight
			resourceScore := mostRequestedScore(requested[i], allocable[i])
			nodeScore += resourceScore * weight
			weightSum += weight
		}
		// 所有资源都被跳过说明没有可用容量，给最低分。
		if weightSum == 0 {
			return 0
		}
		return nodeScore / weightSum
	}
}

// mostRequestedScore 把已用容量换算到 0-MaxNodeScore 区间（MaxNodeScore 常量为 100），
// 0 为最低优先级、100 为最高。资源使用越多得分越高，
// 基本是 noderesources.leastRequestedScore 的反向版本。
func mostRequestedScore(requested, capacity int64) int64 {
	if capacity == 0 {
		return 0
	}
	if requested > capacity {
		// 未声明 requests 的 Pod 会被赋予最小值，但某些情况下 requested 仍可能超过 capacity，
		// 因此截断到 capacity，保证得分不超过上限。
		requested = capacity
	}

	return (requested * fwk.MaxNodeScore) / capacity
}
