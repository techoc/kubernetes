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
	"math"

	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/helper"
)

const maxUtilization = 100

// buildRequestedToCapacityRatioScorerFunction 构造按“请求量/容量”打分的函数，
// 可用于 CPU、内存等核心资源，也可用于加速器之类的扩展资源。
func buildRequestedToCapacityRatioScorerFunction(scoringFunctionShape helper.FunctionShape, resources []config.ResourceSpec) func([]int64, []int64, []int64) int64 {
	rawScoringFunction := helper.BuildBrokenLinearFunction(scoringFunctionShape)
	resourceScoringFunction := func(requested, capacity int64) int64 {
		// 容量为 0 或请求超过容量时，视为 100% 利用率，由形状函数给出最差档位的分数。
		if capacity == 0 || requested > capacity {
			return rawScoringFunction(maxUtilization)
		}

		return rawScoringFunction(requested * maxUtilization / capacity)
	}
	return func(requested, _, allocable []int64) int64 {
		var nodeScore, weightSum int64
		for i := range requested {
			// 可分配量为 0 的资源不参与打分，也不能计入权重，否则会拉低均值。
			if allocable[i] == 0 {
				continue
			}
			weight := resources[i].Weight
			resourceScore := resourceScoringFunction(requested[i], allocable[i])
			// 只累加正分，避免某些资源形状函数给出负值时把其他资源的分数抵消掉。
			if resourceScore > 0 {
				nodeScore += resourceScore * weight
				weightSum += weight
			}
		}
		// 没有任何资源贡献分数时返回 0，避免除以零。
		if weightSum == 0 {
			return 0
		}
		return int64(math.Round(float64(nodeScore) / float64(weightSum)))
	}
}

// requestedToCapacityRatioScorer 根据配置的资源权重和利用率曲线构造打分函数。
func requestedToCapacityRatioScorer(resources []config.ResourceSpec, shape []config.UtilizationShapePoint) func([]int64, []int64, []int64) int64 {
	shapes := make([]helper.FunctionShapePoint, 0, len(shape))
	for _, point := range shape {
		shapes = append(shapes, helper.FunctionShapePoint{
			Utilization: int64(point.Utilization),
			// 自定义优先级的最大分可能与调度器使用的 MaxNodeScore 不同，
			// 因此要把“请求/容量比”的分数缩放到调度器使用的分值区间。
			Score: int64(point.Score) * (fwk.MaxNodeScore / config.MaxCustomPriorityScore),
		})
	}

	return buildRequestedToCapacityRatioScorerFunction(shapes, resources)
}
